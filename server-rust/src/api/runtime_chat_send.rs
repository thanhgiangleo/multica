//! Direct sends commit task-owned input, attachments and session metadata together.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Json,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Default, Deserialize)]
pub(super) struct SendRequest {
    #[serde(default, deserialize_with = "null_string")]
    content: String,
    attachment_ids: Option<Vec<String>>,
}

fn null_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Serialize)]
pub(super) struct Sent {
    message_id: Uuid,
    task_id: Uuid,
    supports_queue: bool,
    queued: bool,
    attachment_ids: Option<Vec<Uuid>>,
    created_at: String,
}

pub(super) struct SendError(Response);
impl From<ApiError> for SendError {
    fn from(error: ApiError) -> Self {
        Self(error.into_response())
    }
}
impl IntoResponse for SendError {
    fn into_response(self) -> Response {
        self.0
    }
}
fn blocked(status: StatusCode, reason: &'static str, message: &'static str) -> SendError {
    SendError(
        (
            status,
            GoJson(serde_json::json!({"error":message,"reason_code":reason})),
        )
            .into_response(),
    )
}
fn failed(_: tokio_postgres::Error) -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to send chat message",
    )
}

async fn readiness(db: &impl GenericClient, agent: &tokio_postgres::Row) -> Result<(), SendError> {
    let runtime: Option<Uuid> = agent.get("runtime_id");
    let Some(runtime) = runtime else {
        return Err(blocked(
            StatusCode::CONFLICT,
            "agent_runtime_required",
            "the target needs a runtime",
        ));
    };
    // Match Go: a failed readiness lookup is not an offline-runtime refusal.
    let Ok(Some(row)) = db
        .query_opt(
            "SELECT owner_id,visibility,status,metadata,updated_at FROM agent_runtime WHERE id=$1",
            &[&runtime],
        )
        .await
    else {
        return Ok(());
    };
    if row.get::<_, String>("visibility") == "private"
        && row.get::<_, Option<Uuid>>("owner_id").is_some()
        && row.get::<_, Option<Uuid>>("owner_id") != agent.get::<_, Option<Uuid>>("owner_id")
    {
        return Err(blocked(
            StatusCode::CONFLICT,
            "runtime_access_denied",
            "the target cannot run on this runtime",
        ));
    }
    if row.get::<_, String>("status") != "online" {
        let metadata: Option<serde_json::Value> = row.get("metadata");
        let reason = metadata.as_ref().and_then(|m| m.get("offline_reason"));
        let code = reason.and_then(|r| r.get("code")).and_then(|c| c.as_str());
        if code == Some("not_executable") {
            return Err(blocked(
                StatusCode::CONFLICT,
                "runtime_unusable",
                "the target's agent CLI cannot run on its machine",
            ));
        }
        let installing = reason
            .and_then(|r| r.get("installing"))
            .and_then(|v| v.as_bool())
            == Some(true);
        let fresh = row
            .get::<_, Option<DateTime<Utc>>>("updated_at")
            .is_some_and(|t| Utc::now().signed_duration_since(t).num_seconds() < 300);
        if code == Some("dsh_profile") && !(installing && fresh) {
            return Err(blocked(
                StatusCode::CONFLICT,
                "runtime_profile_missing",
                "the target's agent CLI is missing a runtime profile on its machine",
            ));
        }
    }
    Ok(())
}

pub(super) async fn send(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Option<SendRequest>>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, GoJson<Sent>), SendError> {
    let Json(req) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req = req.unwrap_or_default();
    if req.content.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "content is required").into());
    }
    let requested: Vec<Uuid> = req
        .attachment_ids
        .unwrap_or_default()
        .iter()
        .map(|id| {
            Uuid::parse_str(id)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid attachment_ids"))
        })
        .collect::<Result<_, _>>()?;
    let id = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to send chat message",
        )
    })?;
    let preflight = super::runtime_chat_archive::check_access(&db, ws, user, &actor, id).await?;
    if db.query_opt("SELECT 1 FROM chat_session cs WHERE cs.id=$1 AND (cs.explicitly_created_at IS NOT NULL OR EXISTS(SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'))", &[&id]).await.map_err(failed)?.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found").into());
    }
    if preflight.get::<_, String>("status") != "active" {
        return Err(error(StatusCode::BAD_REQUEST, "chat session is archived").into());
    }
    let tx = db.transaction().await.map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM chat_session WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(failed)?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let session = super::runtime_chat_archive::check_access(&tx, ws, user, &actor, id).await?;
    let public = tx.query_opt("SELECT 1 FROM chat_session cs WHERE cs.id=$1 AND (cs.explicitly_created_at IS NOT NULL OR EXISTS(SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'))", &[&id]).await.map_err(failed)?;
    if public.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found").into());
    }
    if session.get::<_, String>("status") != "active" {
        return Err(error(StatusCode::CONFLICT, "chat session is archived").into());
    }
    let agent: Uuid = session.get("agent_id");
    let carrier = tx
        .query_opt(
            "SELECT * FROM agent WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&agent, &ws],
        )
        .await
        .map_err(failed)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    // Recheck after the agent lock: ownership and invoke targets may have changed while waiting.
    super::runtime_chat_archive::check_access(&tx, ws, user, &actor, id).await?;
    if carrier
        .get::<_, Option<DateTime<Utc>>>("archived_at")
        .is_some()
    {
        return Err(error(StatusCode::CONFLICT, "chat agent is archived").into());
    }
    readiness(&tx, &carrier).await?;
    if !super::runtime_chat::can_invoke(
        &*tx,
        ws,
        user,
        &actor,
        agent,
        carrier.get("owner_id"),
        carrier.get("permission_mode"),
    )
    .await?
    {
        return Err(blocked(
            StatusCode::FORBIDDEN,
            "invocation_not_allowed",
            "you don't have permission to use this target",
        ));
    }
    let runtime: Option<Uuid> = carrier.get("runtime_id");
    let queued: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE chat_session_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') AND regenerate_quick_actions_for IS NULL)", &[&id]).await.map_err(failed)?.get(0);
    let task = Uuid::now_v7();
    let task_row = tx
        .query_one(
            include_str!("runtime_chat_send.sql"),
            &[&task, &agent, &runtime, &id, &user],
        )
        .await
        .map_err(failed)?;
    tx.execute("UPDATE chat_message SET task_id=$2 WHERE chat_session_id=$1 AND role='user' AND message_kind='onboarding_kickoff' AND task_id IS NULL", &[&id,&task]).await.map_err(failed)?;
    let title = derive_title(&req.content);
    let mut initialized_title = None;
    if !title.is_empty() {
        initialized_title = tx.query_opt("UPDATE chat_session cs SET title=$2 WHERE cs.id=$1 AND cs.title='' AND NOT EXISTS(SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.role='user' AND m.message_kind!='channel_command') RETURNING title", &[&id,&title]).await.map_err(failed)?.map(|r| r.get::<_,String>(0));
    }
    let message = Uuid::now_v7();
    let message_row = tx.query_one("INSERT INTO chat_message(id,chat_session_id,role,content,task_id,message_kind) VALUES ($1,$2,'user',$3,$4,'message') RETURNING created_at", &[&message,&id,&req.content,&task]).await.map_err(failed)?;
    let attachment_ids = if requested.is_empty() {
        None
    } else {
        let (kind, uploader) = match &actor {
            Actor::Task(task) => ("agent", task.agent),
            _ => ("member", user),
        };
        let rows = tx.query("UPDATE attachment SET chat_message_id=$1,chat_session_id=$2 WHERE workspace_id=$3 AND uploader_type=$4 AND uploader_id=$5 AND id=ANY($6::uuid[]) AND issue_id IS NULL AND comment_id IS NULL AND chat_message_id IS NULL AND source_context_id IS NULL AND (chat_session_id IS NULL OR chat_session_id=$2) RETURNING id", &[&message,&id,&ws,&kind,&uploader,&requested]).await.map_err(failed)?;
        let bound: Vec<Uuid> = rows.iter().map(|r| r.get(0)).collect();
        if initialized_title.is_none() && req.content.trim().is_empty() && !bound.is_empty() {
            let file = tx.query_one("SELECT filename FROM attachment WHERE chat_message_id=$1 AND workspace_id=$2 ORDER BY created_at ASC LIMIT 1", &[&message,&ws]).await.map_err(failed)?;
            let media_title = derive_title(file.get("filename"));
            if !media_title.is_empty() {
                initialized_title = tx.query_opt("UPDATE chat_session cs SET title=$3 WHERE cs.id=$1 AND cs.title='' AND NOT EXISTS(SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.role='user' AND m.message_kind!='channel_command' AND m.id!=$2) RETURNING title", &[&id,&message,&media_title]).await.map_err(failed)?.map(|r| r.get::<_,String>(0));
            }
        }
        Some(bound)
    };
    let updated: DateTime<Utc> = tx
        .query_one(
            "UPDATE chat_session SET updated_at=now() WHERE id=$1 RETURNING updated_at",
            &[&id],
        )
        .await
        .map_err(failed)?
        .get(0);
    let created_at = timestamp(task_row.get("created_at"));
    let message_created_at = timestamp(message_row.get("created_at"));
    let creator: Uuid = session.get("creator_id");
    let guest_history: bool = session.get("zbs_has_guest_history");
    let channel: Option<String> = session.get("zbs_source_channel_type");
    let agent_owner: Option<Uuid> = carrier.get("owner_id");
    tx.commit().await.map_err(failed)?;
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::task_queued(ws, user, id, task, agent),
        creator,
        guest_history,
        channel.as_deref(),
        agent_owner,
    );
    state.wake_daemon(runtime, Some(task));
    if let Some(title) = initialized_title {
        state.events.publish_chat_audience(
            super::runtime_chat_events::Event::chat_session_updated(
                ws,
                user,
                id,
                &title,
                &timestamp(updated),
                None,
                None,
            ),
            creator,
            guest_history,
            channel.as_deref(),
            agent_owner,
        );
    }
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::chat_message(
            ws,
            user,
            id,
            message,
            task,
            &req.content,
            &message_created_at,
        ),
        creator,
        guest_history,
        channel.as_deref(),
        agent_owner,
    );
    Ok((
        StatusCode::CREATED,
        GoJson(Sent {
            message_id: message,
            task_id: task,
            supports_queue: true,
            queued,
            attachment_ids,
            created_at,
        }),
    ))
}

fn derive_title(body: &str) -> String {
    let mut line = body
        .split('\n')
        .find(|s| !s.trim().is_empty())
        .unwrap_or_default()
        .to_owned();
    while let Some(start) = line.find("```") {
        let Some(end) = line[start + 3..].find("```") else {
            break;
        };
        line.replace_range(start..start + end + 6, " ");
    }
    line.retain(|c| !"#*`>~_".contains(c));
    let mut offset = 0;
    while let Some(start) = line[offset..].find('[').map(|n| n + offset) {
        let Some(close) = line[start + 1..].find(']').map(|n| n + start + 1) else {
            break;
        };
        if line[close + 1..].starts_with('(')
            && let Some(end) = line[close + 2..].find(')').map(|n| n + close + 2)
        {
            let label = line[start + 1..close].to_owned();
            let begin = if start > 0 && line.as_bytes()[start - 1] == b'!' {
                start - 1
            } else {
                start
            };
            line.replace_range(begin..end + 1, &label);
            offset = begin + label.len();
            continue;
        }
        offset = start + 1;
    }
    // Go's POSIX whitespace class collapses ASCII whitespace only; TrimSpace is Unicode.
    let normalized = line
        .split(|c: char| c.is_ascii_whitespace() || c == '\u{b}')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let normalized = normalized.trim();
    if normalized.chars().count() <= 30 {
        normalized.to_owned()
    } else {
        format!(
            "{}…",
            normalized.chars().take(29).collect::<String>().trim_end()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::derive_title;
    #[test]
    fn titles_preserve_go_markdown_and_unicode_rules() {
        for (body, want) in [
            ("\n # **Hello** [world](https://example.org)", "Hello world"),
            ("```skip``` Keep `this`", "Keep this"),
            ("![Photo](url)\t title", "Photo title"),
            ("   ", ""),
            (
                "01234567890123456789012345678901234",
                "01234567890123456789012345678…",
            ),
            ("你好世界", "你好世界"),
        ] {
            assert_eq!(derive_title(body), want);
        }
    }
}
