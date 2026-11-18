//! Issue-bound reads, with the selected workspace checked before any issue lookup.
use super::{
    ApiError, ApiState, GoJson, catalog::WorkspaceId, error, squad::legacy_prefix, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/labels", get(labels))
        .route("/api/issues/{id}/metadata", get(metadata))
        .route("/api/issues/{id}/subscribers", get(subscribers))
        .route("/api/issues/{id}/usage", get(usage))
        .route("/api/issues/limit-usage", get(limit_usage))
}

/// Go `GetIssueLimitUsage`. A nil entitlement provider, an unresolved policy
/// and a non-enforcing policy all answer 204: Cloud failures degrade to "off"
/// rather than surfacing as an error. The count is bounded at `limit + 1` so a
/// full workspace never scans its whole issue table, and the reported usage is
/// capped at the limit because the exact number no longer matters.
async fn limit_usage(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
) -> Result<Response, ApiError> {
    let Some(runtime) = state.issue_create_runtime.as_ref() else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let Ok(policy) = runtime.count_policy(ws).await else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    // Go normalizes an enforcing policy without a positive limit back to off.
    let Some(limit) = policy.limit.filter(|limit| *limit > 0) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load issue limit usage",
        )
    })?;
    let sampled = db
        .query_one(
            "SELECT COUNT(*)::bigint FROM (SELECT 1 FROM issue WHERE workspace_id=$1 LIMIT $2) bounded",
            &[&ws, &limit.saturating_add(1)],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load issue limit usage",
            )
        })?
        .get::<_, i64>(0);
    Ok(GoJson(json!({"used": sampled.min(limit), "limit": limit})).into_response())
}

// Go accepts both a UUID and PREFIX-NUMBER. The prefix must be the selected
// workspace's effective prefix; never resolve a number in a different tenant.
pub(super) async fn load_issue(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    raw: &str,
) -> Result<Row, ApiError> {
    if let Some((prefix, number)) = raw.rsplit_once('-')
        && !prefix.is_empty()
        && let Ok(number) = number.parse::<i32>()
        && number > 0
        && let Ok(Some(workspace)) = db
            .query_opt(
                "SELECT name, issue_prefix FROM workspace WHERE id=$1",
                &[&ws],
            )
            .await
    {
        let stored: String = workspace.get("issue_prefix");
        let expected = if stored.is_empty() {
            legacy_prefix(workspace.get("name"))
        } else {
            stored
        };
        if prefix.eq_ignore_ascii_case(&expected)
            && let Ok(Some(row)) = db
                .query_opt(
                    "SELECT id, revision, metadata FROM issue WHERE workspace_id=$1 AND number=$2",
                    &[&ws, &number],
                )
                .await
        {
            return Ok(row);
        }
    }
    let id = Uuid::parse_str(raw).map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    db.query_opt(
        "SELECT id, revision, metadata FROM issue WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .ok()
    .flatten()
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))
}

async fn labels(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let rows = db.query(
        "SELECT l.id, l.workspace_id, l.resource_type, l.name, l.description, l.color, l.created_at, l.updated_at \
         FROM issue_label l JOIN issue_to_label il ON il.label_id=l.id \
         WHERE il.issue_id=$1 AND l.workspace_id=$2 AND l.resource_type='issue' ORDER BY lower(l.name) ASC",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list labels"))?;
    let labels: Vec<_> = rows.iter().map(|row| json!({
        "id": row.get::<_, Uuid>("id"), "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "resource_type": row.get::<_, String>("resource_type"),
        "name": row.get::<_, String>("name"), "description": row.get::<_, String>("description"),
        "color": row.get::<_, String>("color"), "usage_count": 0,
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        "updated_at": timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
    })).collect();
    Ok(GoJson(
        json!({"labels": labels, "issue_revision": issue.get::<_, i64>("revision")}),
    ))
}

async fn metadata(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let metadata: Value = issue.get("metadata");
    Ok(GoJson(
        json!({"metadata": super::issue_write::metadata_projection(metadata)}),
    ))
}

async fn subscribers(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let rows = db
        .query(
            "SELECT issue_id, user_type, user_id, reason, created_at FROM issue_subscriber \
         WHERE issue_id=$1 AND unsubscribed_at IS NULL ORDER BY created_at",
            &[&id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list subscribers",
            )
        })?;
    let subscribers: Vec<_> = rows.iter().map(|row| json!({
        "issue_id": row.get::<_, Uuid>("issue_id"), "user_type": row.get::<_, String>("user_type"),
        "user_id": row.get::<_, Uuid>("user_id"), "reason": row.get::<_, String>("reason"),
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
    })).collect();
    Ok(GoJson(json!(subscribers)))
}

// Frozen Go GetIssueUsageSummary SQL counts metered terminal runs separately
// from legacy task_count, which includes nonterminal tasks with usage rows.
const ISSUE_USAGE_SQL: &str = include_str!("issue_usage.sql");

async fn usage(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get issue usage",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let issue = load_issue(&db, ws, &raw).await?;
    let issue_id: Uuid = issue.get("id");
    let row = db
        .query_one(ISSUE_USAGE_SQL, &[&issue_id])
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(json!({
        "total_input_tokens": row.get::<_, i64>("total_input_tokens"),
        "total_output_tokens": row.get::<_, i64>("total_output_tokens"),
        "total_cache_read_tokens": row.get::<_, i64>("total_cache_read_tokens"),
        "total_cache_write_tokens": row.get::<_, i64>("total_cache_write_tokens"),
        "cost_usd_ticks": row.get::<_, i64>("total_cost_usd_ticks"),
        "uncosted_input_tokens": row.get::<_, i64>("uncosted_input_tokens"),
        "uncosted_output_tokens": row.get::<_, i64>("uncosted_output_tokens"),
        "uncosted_cache_read_tokens": row.get::<_, i64>("uncosted_cache_read_tokens"),
        "uncosted_cache_write_tokens": row.get::<_, i64>("uncosted_cache_write_tokens"),
        "task_count": row.get::<_, i32>("task_count"),
        "terminal_task_count": row.get::<_, i32>("terminal_task_count"),
        "metered_task_count": row.get::<_, i32>("metered_task_count"),
        "unreported_task_count": row.get::<_, i32>("unreported_task_count"),
    })))
}

#[cfg(test)]
mod tests {
    #[test]
    fn identifier_numbers_must_fit_go_int32() {
        assert!("2147483648".parse::<i32>().is_err());
    }
}
