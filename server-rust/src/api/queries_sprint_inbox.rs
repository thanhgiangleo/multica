//! Workspace-scoped inbox mutations. The integrator supplies the user event bus.
use super::{
    ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, inbox::InboxItem, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

/// Recipient-only events, published after the SQL mutation succeeds.
#[derive(Clone, Debug, PartialEq)]
pub struct InboxEvent {
    pub kind: InboxEventKind,
    pub workspace_id: Uuid,
    pub recipient_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InboxEventKind {
    Read,
    Unread,
    Archived,
    Unarchived,
    BatchRead,
    BatchArchived,
}

impl InboxEventKind {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Read => "inbox:read",
            Self::Unread => "inbox:unread",
            Self::Archived => "inbox:archived",
            Self::Unarchived => "inbox:unarchived",
            Self::BatchRead => "inbox:batch-read",
            Self::BatchArchived => "inbox:batch-archived",
        }
    }
}

pub trait InboxEventSink: Send + Sync {
    fn publish(&self, event: InboxEvent);
}

pub type SharedInboxEventSink = Arc<dyn InboxEventSink>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/inbox/mark-all-read", post(mark_all_read))
        .route("/api/inbox/archive-all", post(archive_all))
        .route("/api/inbox/archive-all-read", post(archive_all_read))
        .route("/api/inbox/archive-completed", post(archive_completed))
        .route("/api/inbox/{id}/read", post(mark_read))
        .route("/api/inbox/{id}/unread", post(mark_unread))
        .route("/api/inbox/{id}/archive", post(archive))
        .route("/api/inbox/{id}/unarchive", post(unarchive))
}

fn sink(state: &ApiState) -> Result<&SharedInboxEventSink, ApiError> {
    state.inbox_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "inbox event delivery is unavailable",
        )
    })
}

fn item(row: &Row) -> InboxItem {
    InboxItem {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        recipient_type: row.get("recipient_type"),
        recipient_id: row.get("recipient_id"),
        kind: row.get("type"),
        severity: row.get("severity"),
        issue_id: row.get("issue_id"),
        title: row.get("title"),
        body: row.get("body"),
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

const ITEM_SQL: &str = "WITH target AS (\
    SELECT id, workspace_id, recipient_id, issue_id FROM inbox_item \
    WHERE id=$1 AND workspace_id=$2 AND recipient_type='member' AND recipient_id=$3\
), changed AS (\
    UPDATE inbox_item i SET read=$4 \
    FROM target t WHERE i.id=t.id AND i.workspace_id=$2 \
      AND i.recipient_type='member' AND i.recipient_id=$3 RETURNING i.*\
) SELECT changed.*, iss.status AS issue_status, iss.priority AS issue_priority \
  FROM changed LEFT JOIN issue iss ON iss.id=changed.issue_id AND iss.workspace_id=changed.workspace_id";

// One statement updates the selected item and its issue siblings atomically. A
// trigger failure rolls back every sibling, unlike Go's unchecked second UPDATE.
const GROUP_SQL: &str = "WITH target AS MATERIALIZED (\
    SELECT id, workspace_id, recipient_id, issue_id FROM inbox_item \
    WHERE id=$1 AND workspace_id=$2 AND recipient_type='member' AND recipient_id=$3\
), changed AS (\
    UPDATE inbox_item i SET archived=$4 \
    FROM target t WHERE i.workspace_id=t.workspace_id \
      AND i.recipient_type='member' AND i.recipient_id=t.recipient_id \
      AND (i.id=t.id OR (t.issue_id IS NOT NULL AND i.issue_id=t.issue_id)) \
    RETURNING i.*\
) SELECT changed.*, iss.status AS issue_status, iss.priority AS issue_priority \
  FROM changed LEFT JOIN issue iss ON iss.id=changed.issue_id AND iss.workspace_id=changed.workspace_id \
  WHERE changed.id=$1";

async fn change_item(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    raw_id: String,
    kind: InboxEventKind,
) -> Result<GoJson<InboxItem>, ApiError> {
    let publisher = sink(&state)?;
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid inbox item id"))?;
    let (sql, value, message) = match kind {
        InboxEventKind::Read => (ITEM_SQL, true, "failed to mark read"),
        InboxEventKind::Unread => (ITEM_SQL, false, "failed to mark unread"),
        InboxEventKind::Archived => (GROUP_SQL, true, "failed to archive"),
        InboxEventKind::Unarchived => (GROUP_SQL, false, "failed to unarchive"),
        _ => unreachable!(),
    };
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, message);
    let db = state.pool.get().await.map_err(|_| fail())?;
    let row = db
        .query_opt(sql, &[&id, &ws, &user, &value])
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "inbox item not found"))?;
    let response = item(&row);
    publisher.publish(InboxEvent {
        kind,
        workspace_id: ws,
        recipient_id: user,
        payload: match kind {
            InboxEventKind::Archived | InboxEventKind::Unarchived => json!({
                "item_id": id, "issue_id": response.issue_id, "recipient_id": user
            }),
            _ => json!({"item_id": id, "recipient_id": user}),
        },
    });
    Ok(GoJson(response))
}

async fn mark_read(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<GoJson<InboxItem>, ApiError> {
    change_item(state, ws, user, id, InboxEventKind::Read).await
}

async fn mark_unread(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<GoJson<InboxItem>, ApiError> {
    change_item(state, ws, user, id, InboxEventKind::Unread).await
}

async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<GoJson<InboxItem>, ApiError> {
    change_item(state, ws, user, id, InboxEventKind::Archived).await
}

async fn unarchive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<GoJson<InboxItem>, ApiError> {
    change_item(state, ws, user, id, InboxEventKind::Unarchived).await
}

async fn change_batch(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    sql: &'static str,
    kind: InboxEventKind,
    message: &'static str,
) -> Result<GoJson<Value>, ApiError> {
    let publisher = sink(&state)?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, message);
    let db = state.pool.get().await.map_err(|_| fail())?;
    let count = db.execute(sql, &[&ws, &user]).await.map_err(|_| fail())?;
    publisher.publish(InboxEvent {
        kind,
        workspace_id: ws,
        recipient_id: user,
        payload: json!({"recipient_id": user, "count": count}),
    });
    Ok(GoJson(json!({"count": count})))
}

const MARK_ALL_READ_SQL: &str = "UPDATE inbox_item SET read=true WHERE workspace_id=$1 \
    AND recipient_type='member' AND recipient_id=$2 AND archived=false AND read=false";
const ARCHIVE_ALL_SQL: &str = "UPDATE inbox_item SET archived=true WHERE workspace_id=$1 \
    AND recipient_type='member' AND recipient_id=$2 AND archived=false";
const ARCHIVE_ALL_READ_SQL: &str = "WITH newest AS (\
    SELECT DISTINCT ON (COALESCE(issue_id,id)) COALESCE(issue_id,id) AS group_id, read \
    FROM inbox_item WHERE workspace_id=$1 AND recipient_type='member' AND recipient_id=$2 \
      AND archived=false ORDER BY COALESCE(issue_id,id),created_at DESC,id DESC\
), read_groups AS (SELECT group_id FROM newest WHERE read=true) \
    UPDATE inbox_item i SET archived=true FROM read_groups selected \
    WHERE i.workspace_id=$1 AND i.recipient_type='member' AND i.recipient_id=$2 \
      AND i.archived=false AND COALESCE(i.issue_id,i.id)=selected.group_id";
const ARCHIVE_COMPLETED_SQL: &str = "UPDATE inbox_item i SET archived=true \
    WHERE i.workspace_id=$1 AND i.recipient_type='member' AND i.recipient_id=$2 \
      AND i.archived=false AND EXISTS (\
        SELECT 1 FROM issue iss WHERE iss.id=i.issue_id AND iss.workspace_id=$1 \
        AND (iss.status IN ('done','cancelled') OR EXISTS (\
          SELECT 1 FROM issue_status st WHERE st.workspace_id=$1 AND st.key=iss.status \
            AND st.category IN ('done','closed')))\
      )";

async fn mark_all_read(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    change_batch(
        state,
        ws,
        user,
        MARK_ALL_READ_SQL,
        InboxEventKind::BatchRead,
        "failed to mark all inbox read",
    )
    .await
}

async fn archive_all(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    change_batch(
        state,
        ws,
        user,
        ARCHIVE_ALL_SQL,
        InboxEventKind::BatchArchived,
        "failed to archive all inbox",
    )
    .await
}

async fn archive_all_read(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    change_batch(
        state,
        ws,
        user,
        ARCHIVE_ALL_READ_SQL,
        InboxEventKind::BatchArchived,
        "failed to archive all read inbox",
    )
    .await
}

async fn archive_completed(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    change_batch(
        state,
        ws,
        user,
        ARCHIVE_COMPLETED_SQL,
        InboxEventKind::BatchArchived,
        "failed to archive completed inbox",
    )
    .await
}
