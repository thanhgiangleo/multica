//! Workspace task presence: active tasks and one latest completed/failed outcome per visible agent.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, agent_activity::allowed_agents,
    catalog::WorkspaceId, error, runtime_chat_task,
};
use axum::{
    Router,
    extract::{Extension, State},
    http::StatusCode,
    routing::get,
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/agent-task-snapshot", get(snapshot))
}

const SNAPSHOT: &str = "SELECT atq.* FROM agent_task_queue atq \
JOIN agent a ON a.id=atq.agent_id WHERE a.workspace_id=$1 \
AND (atq.status IN ('queued','dispatched','running','waiting_local_directory') \
OR (atq.status='deferred' AND atq.context->>'wakeup_id' IS NOT NULL)) \
UNION ALL SELECT latest.* FROM agent a JOIN LATERAL ( \
SELECT atq.* FROM agent_task_queue atq WHERE atq.agent_id=a.id \
AND atq.status IN ('completed','failed') \
ORDER BY atq.completed_at DESC NULLS LAST, atq.created_at DESC, atq.id DESC LIMIT 1 \
) latest ON TRUE WHERE a.workspace_id=$1";

fn redact_chat_content(task: &mut Value) {
    if task["chat_session_id"].as_str().is_none_or(str::is_empty) {
        return;
    }
    task["result"] = Value::Null;
    task["error"] = Value::Null;
    for field in [
        "trigger_summary",
        "trigger_comment_content",
        "handoff_note",
        "wakeup_joined",
        "work_dir",
        "relative_work_dir",
        "durable_work_dir",
        "relative_durable_work_dir",
        "branch_name",
        "chat_message",
        "chat_message_attachments",
        "attribution",
    ] {
        task.as_object_mut().unwrap().remove(field);
    }
}

async fn snapshot(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Vec<Value>>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list agent task snapshot",
        )
    })?;
    let rows = db.query(SNAPSHOT, &[&ws]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list agent task snapshot",
        )
    })?;
    let allowed: HashSet<_> = allowed_agents(&db, ws, user, &actor)
        .await?
        .into_iter()
        .collect();
    let mut tasks = Vec::with_capacity(rows.len());
    let mut attribution_users = HashSet::new();
    for row in &rows {
        if !allowed.contains(&row.get::<_, Uuid>("agent_id")) {
            continue;
        }
        let mut task = runtime_chat_task::response(row, ws);
        redact_chat_content(&mut task);
        for field in ["initiator", "originator"] {
            if let Some(id) = task["attribution"][field]["id"].as_str()
                && let Ok(id) = Uuid::parse_str(id)
            {
                attribution_users.insert(id);
            }
        }
        tasks.push(task);
    }
    if !attribution_users.is_empty() {
        let ids: Vec<_> = attribution_users.into_iter().collect();
        if let Ok(users) = db
            .query(
                "SELECT id,name,email,avatar_url FROM \"user\" WHERE id=ANY($1::uuid[])",
                &[&ids],
            )
            .await
        {
            let by_id: HashMap<Uuid, Value> = users
                .iter()
                .map(|row| {
                    let id: Uuid = row.get("id");
                    let email: String = row.get("email");
                    let mut display = json!({"name":row.get::<_,String>("name")});
                    if !email
                        .trim()
                        .to_ascii_lowercase()
                        .ends_with("@channel.invalid")
                    {
                        display["email"] = json!(email);
                    }
                    if let Some(avatar) = row.get::<_, Option<String>>("avatar_url")
                        && !avatar.is_empty()
                    {
                        // Stored avatar references may be private object keys or expiring URLs.
                        // Never return one verbatim without the storage owner's resolver.
                        let policy = state.timeline_avatar_urls.as_ref().ok_or_else(|| {
                            error(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "avatar URL policy unavailable",
                            )
                        })?;
                        let url = policy
                            .resolve(&avatar)
                            .filter(|url| !url.is_empty())
                            .ok_or_else(|| {
                                error(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "avatar URL policy unavailable",
                                )
                            })?;
                        display["avatar_url"] = json!(url);
                    }
                    Ok((id, display))
                })
                .collect::<Result<_, ApiError>>()?;
            for task in &mut tasks {
                for field in ["initiator", "originator"] {
                    if let Some(id) = task["attribution"][field]["id"].as_str()
                        && let Ok(id) = Uuid::parse_str(id)
                        && let Some(display) = by_id.get(&id)
                    {
                        for (key, value) in display.as_object().unwrap() {
                            task["attribution"][field][key] = value.clone();
                        }
                    }
                }
            }
        }
    }
    Ok(GoJson(tasks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_content_is_removed_without_erasing_presence() {
        let mut task = json!({
            "id":"task", "status":"running", "kind":"chat", "chat_session_id":"session",
            "result":{"message":"private"}, "error":"provider detail", "trigger_summary":"user chat",
            "wakeup_joined":"private", "work_dir":"/home/user/private", "relative_work_dir":"private",
            "durable_work_dir":"/home/user/private", "relative_durable_work_dir":"private",
            "handoff_note":"private", "branch_name":"private", "chat_message":"private",
            "chat_message_attachments":[{"name":"private"}], "attribution":{"source":"direct_human"}
        });
        redact_chat_content(&mut task);
        assert_eq!(task["status"], "running");
        assert_eq!(task["kind"], "chat");
        assert_eq!(task["chat_session_id"], "session");
        assert!(task["result"].is_null());
        assert!(task["error"].is_null());
        assert!(!task.to_string().contains("private"));
        assert!(!task.to_string().contains("provider detail"));
        assert!(!task.to_string().contains("direct_human"));
    }
}
