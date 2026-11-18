//! Resolve a single thread comment atomically, including displaced resolutions.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_write::{IssueCommentResolutionEvent, write_actor},
    timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::{Client, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/comments/{commentId}/resolve",
        post(resolve).delete(unresolve),
    )
}

async fn resolve(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    mutate(state, ws, user, actor, raw, true).await
}

async fn unresolve(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    mutate(state, ws, user, actor, raw, false).await
}

/// Frozen Go tombstones and newly created comments have no reaction projection.
pub(super) fn base_response(row: &Row) -> Value {
    let resolved_at: Option<DateTime<Utc>> = row.get("resolved_at");
    let mut result = json!({
        "id":row.get::<_, Uuid>("id"), "issue_id":row.get::<_, Uuid>("issue_id"),
        "author_type":row.get::<_, String>("author_type"), "author_id":row.get::<_, Uuid>("author_id"),
        "content":row.get::<_, String>("content"), "type":row.get::<_, String>("type"),
        "parent_id":row.get::<_, Option<Uuid>>("parent_id"),
        "created_at":timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
        "revision":row.get::<_, i64>("revision"),
        "resolved_at":resolved_at.map(timestamp),
        "resolved_by_type":row.get::<_, Option<String>>("resolved_by_type"),
        "resolved_by_id":row.get::<_, Option<Uuid>>("resolved_by_id"),
        "reactions":[], "attachments":[],
    });
    for field in ["source_task_id", "quick_action_id"] {
        if let Some(id) = row.get::<_, Option<Uuid>>(field) {
            result[field] = json!(id);
        }
    }
    if let Some(deleted) = row.get::<_, Option<DateTime<Utc>>>("deleted_at") {
        result["deleted_at"] = json!(timestamp(deleted));
    }
    result
}

/// Run only after commit. Go's groupAttachments swallows read errors; an
/// unavailable projection neither rolls back the write nor suppresses events.
pub(super) async fn with_attachments(db: &Client, row: &Row, ws: Uuid) -> Value {
    let id: Uuid = row.get("id");
    let attachments = db.query(
        "SELECT id,workspace_id,issue_id,comment_id,chat_session_id,chat_message_id,uploader_type,uploader_id,filename,url,content_type,size_bytes,created_at FROM attachment WHERE comment_id=$1 AND workspace_id=$2 ORDER BY created_at,id",
        &[&id, &ws],
    ).await.unwrap_or_default().iter().map(|a| {
        let aid: Uuid = a.get("id");
        let download_url = format!("/api/attachments/{aid}/download");
        json!({
            "id":aid, "workspace_id":a.get::<_, Uuid>("workspace_id"),
            "issue_id":a.get::<_, Option<Uuid>>("issue_id"), "comment_id":a.get::<_, Option<Uuid>>("comment_id"),
            "chat_session_id":a.get::<_, Option<Uuid>>("chat_session_id"), "chat_message_id":a.get::<_, Option<Uuid>>("chat_message_id"),
            "uploader_type":a.get::<_, String>("uploader_type"), "uploader_id":a.get::<_, Uuid>("uploader_id"),
            "filename":a.get::<_, String>("filename"), "url":a.get::<_, String>("url"),
            "download_url":download_url, "markdown_url":download_url,
            "content_type":a.get::<_, String>("content_type"), "size_bytes":a.get::<_, i64>("size_bytes"),
            "created_at":timestamp(a.get::<_, DateTime<Utc>>("created_at")),
        })
    }).collect::<Vec<_>>();
    let mut result = base_response(row);
    result["attachments"] = json!(attachments);
    result
}

/// Attachment events need a complete projection of the just-written row.
/// Run these reads in the mutation transaction; a failed query must not publish
/// a partial comment projection as if it were complete.
pub(super) async fn response_in_transaction(
    tx: &deadpool_postgres::Transaction<'_>,
    row: &Row,
    ws: Uuid,
) -> Result<Value, ApiError> {
    let id: Uuid = row.get("id");
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load comment projection",
        )
    };
    let attachments = tx.query(
        "SELECT id,workspace_id,issue_id,comment_id,chat_session_id,chat_message_id,uploader_type,uploader_id,filename,url,content_type,size_bytes,created_at FROM attachment WHERE comment_id=$1 AND workspace_id=$2 ORDER BY created_at,id",
        &[&id, &ws],
    ).await.map_err(|_| fail())?.iter().map(|a| {
        let aid: Uuid = a.get("id");
        let download_url = format!("/api/attachments/{aid}/download");
        json!({
            "id":aid, "workspace_id":a.get::<_, Uuid>("workspace_id"),
            "issue_id":a.get::<_, Option<Uuid>>("issue_id"), "comment_id":a.get::<_, Option<Uuid>>("comment_id"),
            "chat_session_id":a.get::<_, Option<Uuid>>("chat_session_id"), "chat_message_id":a.get::<_, Option<Uuid>>("chat_message_id"),
            "uploader_type":a.get::<_, String>("uploader_type"), "uploader_id":a.get::<_, Uuid>("uploader_id"),
            "filename":a.get::<_, String>("filename"), "url":a.get::<_, String>("url"),
            "download_url":download_url, "markdown_url":download_url,
            "content_type":a.get::<_, String>("content_type"), "size_bytes":a.get::<_, i64>("size_bytes"),
            "created_at":timestamp(a.get::<_, DateTime<Utc>>("created_at")),
        })
    }).collect::<Vec<_>>();
    let reactions = tx.query(
        "SELECT id, comment_id, actor_type, actor_id, emoji, created_at FROM comment_reaction WHERE comment_id=$1 ORDER BY created_at,id",
        &[&id],
    ).await.map_err(|_| fail())?.iter().map(|r| json!({
        "id":r.get::<_, Uuid>("id"), "comment_id":r.get::<_, Uuid>("comment_id"),
        "actor_type":r.get::<_, String>("actor_type"), "actor_id":r.get::<_, Uuid>("actor_id"),
        "emoji":r.get::<_, String>("emoji"), "created_at":timestamp(r.get::<_, DateTime<Utc>>("created_at")),
    })).collect::<Vec<_>>();
    let mut result = base_response(row);
    result["attachments"] = json!(attachments);
    result["reactions"] = json!(reactions);
    Ok(result)
}

/// The two best-effort reads are independent, outside the write transaction.
pub(super) async fn response(db: &Client, row: &Row, ws: Uuid) -> Value {
    let id: Uuid = row.get("id");
    let reactions = db.query(
        "SELECT id, comment_id, actor_type, actor_id, emoji, created_at FROM comment_reaction WHERE comment_id=$1 ORDER BY created_at,id",
        &[&id],
    ).await.unwrap_or_default().iter().map(|r| json!({
        "id":r.get::<_, Uuid>("id"), "comment_id":r.get::<_, Uuid>("comment_id"),
        "actor_type":r.get::<_, String>("actor_type"), "actor_id":r.get::<_, Uuid>("actor_id"),
        "emoji":r.get::<_, String>("emoji"), "created_at":timestamp(r.get::<_, DateTime<Utc>>("created_at")),
    })).collect::<Vec<_>>();
    let mut result = with_attachments(db, row, ws).await;
    result["reactions"] = json!(reactions);
    result
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    resolving: bool,
) -> Result<GoJson<Value>, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let (actor_type, actor_id, task_id) = write_actor(actor, user)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            if resolving {
                "failed to resolve comment"
            } else {
                "failed to unresolve comment"
            },
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    // Lock the owning issue first: all resolution replacements in a thread and
    // comment deletes share this fence. A stale pre-lock lookup is never used.
    let issue = db.query_opt(
        "SELECT i.id FROM issue i JOIN comment c ON c.issue_id=i.id AND c.workspace_id=i.workspace_id WHERE c.id=$1 AND c.workspace_id=$2",
        &[&id, &ws],
    ).await.map_err(|_| fail())?.ok_or_else(|| error(StatusCode::NOT_FOUND, "comment not found"))?;
    let issue_id: Uuid = issue.get("id");
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true), set_config('multica.actor_id',$2,true), set_config('multica.source_task_id',$3,true)",
        &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_| fail())?;
    tx.query_one(
        "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
        &[&issue_id, &ws],
    )
    .await
    .map_err(|_| fail())?;
    let current = tx.query_opt("SELECT * FROM comment WHERE id=$1 AND issue_id=$2 AND workspace_id=$3 AND deleted_at IS NULL FOR NO KEY UPDATE", &[&id,&issue_id,&ws]).await.map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "comment not found"))?;
    let changed = current
        .get::<_, Option<DateTime<Utc>>>("resolved_at")
        .is_some()
        != resolving;
    let cleared = if resolving {
        // Frozen Go ClearOtherThreadResolutions, scoped to this issue + workspace.
        tx.query(
            include_str!("issue_write_resolution_clear.sql"),
            &[&id, &issue_id, &ws],
        )
        .await
        .map_err(|_| fail())?
    } else {
        vec![]
    };
    let updated = if resolving {
        tx.query_opt("UPDATE comment SET resolved_at=COALESCE(resolved_at,now()),resolved_by_type=COALESCE(resolved_by_type,$2),resolved_by_id=COALESCE(resolved_by_id,$3),revision=revision+CASE WHEN resolved_at IS NULL THEN 1 ELSE 0 END,updated_at=CASE WHEN resolved_at IS NULL THEN now() ELSE updated_at END WHERE id=$1 AND issue_id=$4 AND workspace_id=$5 AND deleted_at IS NULL RETURNING *",
            &[&id,&actor_type,&actor_id,&issue_id,&ws]).await.map_err(|_| fail())?
    } else {
        tx.query_opt("UPDATE comment SET resolved_at=NULL,resolved_by_type=NULL,resolved_by_id=NULL,revision=revision+CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END,updated_at=CASE WHEN resolved_at IS NOT NULL THEN now() ELSE updated_at END WHERE id=$1 AND issue_id=$2 AND workspace_id=$3 AND deleted_at IS NULL RETURNING *",
            &[&id,&issue_id,&ws]).await.map_err(|_| fail())?
    }.ok_or_else(|| error(StatusCode::NOT_FOUND, "comment not found"))?;
    tx.commit().await.map_err(|_| fail())?;
    for row in cleared {
        let comment = response(&db, &row, ws).await;
        sink.publish_comment_resolution(IssueCommentResolutionEvent {
            event_type: "comment:unresolved",
            workspace_id: ws,
            actor_type,
            actor_id,
            payload: json!({"comment":comment}),
        });
    }
    let result = response(&db, &updated, ws).await;
    if changed {
        sink.publish_comment_resolution(IssueCommentResolutionEvent {
            event_type: if resolving {
                "comment:resolved"
            } else {
                "comment:unresolved"
            },
            workspace_id: ws,
            actor_type,
            actor_id,
            payload: json!({"comment":result}),
        });
    }
    Ok(GoJson(result))
}
