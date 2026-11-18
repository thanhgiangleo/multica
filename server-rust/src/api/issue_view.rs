//! Saved issue-view reads with owner/private visibility and workspace scoping.
use super::{
    ApiError, ApiState, GoJson, Identity,
    account_catalog_write::view_preference::parse_go_uuid,
    catalog::{WorkspaceId, parse_params},
    error, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issue-views", get(list))
        .route("/api/issue-views/", get(list))
        .route("/api/issue-views/{id}", get(detail))
        .route("/api/issue-views/{id}/", get(detail))
}

#[derive(Serialize)]
struct IssueView {
    id: Uuid,
    workspace_id: Uuid,
    owner_id: Uuid,
    name: String,
    scope_type: String,
    scope_id: Option<Uuid>,
    scope_variant: Option<String>,
    visibility: String,
    definition_version: i32,
    query: Value,
    display: Value,
    revision: i32,
    created_at: String,
    updated_at: String,
}

fn view(row: &Row) -> IssueView {
    IssueView {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        scope_type: row.get("scope_type"),
        scope_id: row.get("scope_id"),
        scope_variant: row.get("scope_variant"),
        visibility: row.get("visibility"),
        definition_version: row.get("definition_version"),
        query: row.get("query"),
        display: row.get("display"),
        revision: row.get("revision"),
        created_at: timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        updated_at: timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
    }
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Vec<IssueView>>, ApiError> {
    let params = parse_params(raw.as_deref());
    let scope = params.get("scope_type").map(String::as_str).unwrap_or("");
    if !matches!(scope, "workspace" | "my" | "project") {
        return Err(error(StatusCode::BAD_REQUEST, "invalid scope_type"));
    }
    let scope_id = params
        .get("scope_id")
        .filter(|id| !id.is_empty())
        .map(|id| {
            parse_go_uuid(id).ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid scope_id"))
        })
        .transpose()?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list views");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query(
            "SELECT * FROM issue_view WHERE workspace_id=$1 AND scope_type=$2 \
         AND scope_id IS NOT DISTINCT FROM $3::uuid \
         AND (owner_id=$4 OR visibility='workspace') ORDER BY created_at ASC LIMIT 200",
            &[&ws, &scope, &scope_id, &user],
        )
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(rows.iter().map(view).collect()))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<GoJson<IssueView>, ApiError> {
    let id =
        parse_go_uuid(&raw).ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid view id"))?;
    // Frozen Go deliberately hides both SQL errors and unauthorized private
    // views behind the same 404, rather than revealing their existence.
    let not_found = || error(StatusCode::NOT_FOUND, "view not found");
    let db = state.pool.get().await.map_err(|_| not_found())?;
    let row = db
        .query_opt(
            "SELECT * FROM issue_view WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    if row.get::<_, Uuid>("owner_id") != user && row.get::<_, String>("visibility") != "workspace" {
        return Err(not_found());
    }
    Ok(GoJson(view(&row)))
}
