//! Creator-only deferred-cancellation drafts from the staged Go chat handler.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/chat/sessions/{sessionId}/draft-restores", get(list))
}

#[derive(Clone, Serialize)]
struct Attachment {
    id: Uuid,
    workspace_id: Uuid,
    issue_id: Option<Uuid>,
    comment_id: Option<Uuid>,
    chat_session_id: Option<Uuid>,
    chat_message_id: Option<Uuid>,
    uploader_type: String,
    uploader_id: Uuid,
    filename: String,
    url: String,
    download_url: String,
    markdown_url: String,
    content_type: String,
    size_bytes: i64,
    created_at: String,
}

#[derive(Serialize)]
struct Restore {
    id: Uuid,
    chat_session_id: Uuid,
    task_id: Uuid,
    content: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<Attachment>,
    created_at: String,
}

#[derive(Serialize)]
struct Restores {
    restores: Vec<Restore>,
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Restores>, ApiError> {
    let session = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    // The draft endpoint uses loadChatSessionForUser, not the private-agent or
    // public-projection gates: a creator must recover a draft after losing access.
    let row = db
        .query_opt(
            "SELECT cs.creator_id, cs.agent_id, cs.zbs_has_guest_history, a.owner_id \
         FROM chat_session cs LEFT JOIN agent a ON a.id=cs.agent_id \
         WHERE cs.id=$1 AND cs.workspace_id=$2",
            &[&session, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if row.get::<_, Uuid>("creator_id") != user {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(task) = &actor {
        let bound = db
            .query_opt(
                "SELECT chat_session_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2",
                &[&task.task, &task.agent],
            )
            .await
            .ok()
            .flatten()
            .and_then(|r| r.get::<_, Option<Uuid>>(0));
        if bound != Some(session) {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    if row.get::<_, bool>("zbs_has_guest_history")
        && (!matches!(actor, Actor::Human) || row.get::<_, Option<Uuid>>("owner_id") != Some(user))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    let rows = db
        .query(
            "SELECT id, chat_session_id, task_id, content, attachment_ids, created_at \
         FROM chat_draft_restore WHERE chat_session_id=$1 ORDER BY created_at ASC",
            &[&session],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list draft restores",
            )
        })?;
    let ids: Vec<Uuid> = rows
        .iter()
        .flat_map(|r| r.get::<_, Vec<Uuid>>("attachment_ids"))
        .collect();
    let mut attachments = HashMap::new();
    if !ids.is_empty() {
        let found = db.query(
            "SELECT id, workspace_id, issue_id, comment_id, chat_session_id, chat_message_id, \
             uploader_type, uploader_id, filename, url, content_type, size_bytes, created_at \
             FROM attachment WHERE id = ANY($1::uuid[]) AND workspace_id=$2 ORDER BY created_at ASC",
            &[&ids, &ws],
        ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load draft restore attachments"))?;
        for r in found {
            let id: Uuid = r.get("id");
            let path = format!("/api/attachments/{id}/download");
            // Stable-path local-storage mode. Other Go URL policies (signed CDN,
            // public base URL) need deployment-aware attachment integration.
            attachments.insert(
                id,
                Attachment {
                    id,
                    workspace_id: r.get("workspace_id"),
                    issue_id: r.get("issue_id"),
                    comment_id: r.get("comment_id"),
                    chat_session_id: r.get("chat_session_id"),
                    chat_message_id: r.get("chat_message_id"),
                    uploader_type: r.get("uploader_type"),
                    uploader_id: r.get("uploader_id"),
                    filename: r.get("filename"),
                    url: r.get("url"),
                    download_url: path.clone(),
                    markdown_url: path,
                    content_type: r.get("content_type"),
                    size_bytes: r.get("size_bytes"),
                    created_at: r
                        .get::<_, DateTime<Utc>>("created_at")
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                },
            );
        }
    }
    Ok(GoJson(Restores {
        restores: rows
            .iter()
            .map(|r| Restore {
                id: r.get("id"),
                chat_session_id: r.get("chat_session_id"),
                task_id: r.get("task_id"),
                content: r.get("content"),
                created_at: timestamp(r.get::<_, DateTime<Utc>>("created_at")),
                attachments: r
                    .get::<_, Vec<Uuid>>("attachment_ids")
                    .iter()
                    .filter_map(|id| attachments.get(id).cloned())
                    .collect(),
            })
            .collect(),
    }))
}
