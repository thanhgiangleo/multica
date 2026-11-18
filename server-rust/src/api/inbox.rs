//! Member-only inbox reads from the frozen Go inbox SQL and response contract.
use super::{ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, State},
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
        .route("/api/inbox", get(list))
        .route("/api/inbox/", get(list))
        .route("/api/inbox/archived", get(list_archived))
        .route("/api/inbox/unread-count", get(unread_count))
        .route("/api/inbox/unread-summary", get(unread_summary))
}

#[derive(Serialize)]
pub(super) struct InboxItem {
    pub(super) id: Uuid,
    pub(super) workspace_id: Uuid,
    pub(super) recipient_type: String,
    pub(super) recipient_id: Uuid,
    #[serde(rename = "type")]
    pub(super) kind: String,
    pub(super) severity: String,
    pub(super) issue_id: Option<Uuid>,
    pub(super) title: String,
    pub(super) body: Option<String>,
    pub(super) read: bool,
    pub(super) archived: bool,
    pub(super) created_at: String,
    pub(super) issue_status: Option<String>,
    pub(super) issue_priority: Option<String>,
    pub(super) actor_type: Option<String>,
    pub(super) actor_id: Option<Uuid>,
    pub(super) details: Value,
}

pub(super) fn list_body(
    kind: &str,
    issue_id: Option<Uuid>,
    body: Option<String>,
) -> Option<String> {
    let full = body?;
    if kind != "new_comment" || issue_id.is_none() {
        return Some(full);
    }
    // Go's preview is 200 code points including its final ellipsis.
    let mut chars = full.chars();
    let preview: String = chars.by_ref().take(199).collect();
    if chars.next().is_some() && chars.next().is_some() {
        Some(format!("{preview}…"))
    } else {
        Some(full)
    }
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<InboxItem>>, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list inbox");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query(
            "SELECT i.*, iss.status AS issue_status, iss.priority AS issue_priority \
         FROM inbox_item i LEFT JOIN issue iss ON iss.id=i.issue_id \
         WHERE i.workspace_id=$1 AND i.recipient_type='member' AND i.recipient_id=$2 \
         AND i.archived=false ORDER BY i.created_at DESC",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(rows.iter().map(row_to_item).collect()))
}

fn row_to_item(row: &Row) -> InboxItem {
    let kind: String = row.get("type");
    let issue_id: Option<Uuid> = row.get("issue_id");
    InboxItem {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        recipient_type: row.get("recipient_type"),
        recipient_id: row.get("recipient_id"),
        kind: kind.clone(),
        severity: row.get("severity"),
        issue_id,
        title: row.get("title"),
        body: list_body(&kind, issue_id, row.get("body")),
        read: row.get("read"),
        archived: row.get("archived"),
        created_at: timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        issue_status: row.get("issue_status"),
        issue_priority: row.get("issue_priority"),
        actor_type: row.get("actor_type"),
        actor_id: row.get("actor_id"),
        details: row
            .get::<_, Option<Value>>("details")
            .unwrap_or(Value::Null),
    }
}

async fn list_archived(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<InboxItem>>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list archived inbox",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query(
            include_str!("archived_legacy.sql"),
            &[&ws, &"member", &user],
        )
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(rows.iter().map(row_to_item).collect()))
}

async fn unread_count(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to count unread inbox",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM inbox_item WHERE workspace_id=$1 AND recipient_type='member' \
         AND recipient_id=$2 AND read=false AND archived=false",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    Ok(GoJson(serde_json::json!({"count":count})))
}

#[derive(Serialize)]
struct WorkspaceUnread {
    workspace_id: Uuid,
    count: i64,
}

async fn unread_summary(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<WorkspaceUnread>>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to summarize unread inbox",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    // Unlike the raw unread count, each issue group contributes only when its newest
    // active item is unread; revoked memberships contribute nothing.
    let rows = db
        .query(
            "SELECT newest.workspace_id, count(*) AS count FROM ( \
           SELECT DISTINCT ON (i.workspace_id, COALESCE(i.issue_id, i.id)) \
             i.workspace_id, i.read FROM inbox_item i \
           JOIN member m ON m.workspace_id=i.workspace_id AND m.user_id=i.recipient_id \
           WHERE i.recipient_type='member' AND i.recipient_id=$1 AND i.archived=false \
           ORDER BY i.workspace_id, COALESCE(i.issue_id, i.id), i.created_at DESC \
         ) newest WHERE newest.read=false GROUP BY newest.workspace_id",
            &[&user],
        )
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(
        rows.iter()
            .map(|row| WorkspaceUnread {
                workspace_id: row.get("workspace_id"),
                count: row.get("count"),
            })
            .collect(),
    ))
}
