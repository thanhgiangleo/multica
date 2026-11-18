//! Workspace child counts from the frozen Go ChildIssueProgress SQL contract.
use super::{ApiError, ApiState, GoJson, catalog::WorkspaceId, error};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
};
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
struct Entry {
    parent_issue_id: Uuid,
    total: i64,
    done: i64,
}

pub(super) async fn get(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
) -> Result<GoJson<impl Serialize>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to resolve status categories",
        )
    })?;
    // ExpandCategories includes archived custom keys and the built-in keys even
    // if the catalog has not been seeded. A failed catalog read must not turn
    // completed custom statuses into unfinished children.
    let statuses = db
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND category = ANY($2::text[])",
            &[&ws, &vec!["done", "closed"]],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve status categories",
            )
        })?;
    let mut terminal: Vec<String> = statuses.iter().map(|row| row.get(0)).collect();
    for key in ["done", "cancelled"] {
        if !terminal.iter().any(|stored| stored == key) {
            terminal.push(key.to_owned());
        }
    }
    let rows = db.query(
        "SELECT parent_issue_id, COUNT(*)::bigint AS total, \
         COUNT(*) FILTER (WHERE status = ANY($2::text[]))::bigint AS done \
         FROM issue WHERE workspace_id = $1 AND parent_issue_id IS NOT NULL GROUP BY parent_issue_id",
        &[&ws, &terminal],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get child issue progress"))?;
    let progress: Vec<_> = rows
        .iter()
        .map(|row| Entry {
            parent_issue_id: row.get("parent_issue_id"),
            total: row.get("total"),
            done: row.get("done"),
        })
        .collect();
    Ok(GoJson(serde_json::json!({"progress": progress})))
}
