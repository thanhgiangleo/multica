//! Queue reordering uses the same agent lock as a daemon claim, then atomically
//! demotes older priorities and promotes the selected follow-up.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use serde::Serialize;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/chat/sessions/{sessionId}/queued-tasks/{taskId}/prioritize",
            post(prioritize),
        )
        .route(
            "/api/chat/sessions/{sessionId}/queued-tasks",
            axum::routing::delete(clear_queued),
        )
}

#[derive(Serialize)]
struct Prioritized {
    task_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_task_id: Option<Uuid>,
}

const PRIORITIZE: &str = include_str!("runtime_chat_queue.sql");

async fn prioritize(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_session, raw_task)): Path<(String, String)>,
) -> Result<GoJson<Prioritized>, ApiError> {
    let session = Uuid::parse_str(&raw_session)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let task = Uuid::parse_str(&raw_task)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start prioritize transaction",
        )
    })?;
    let row = db.query_opt(
        "SELECT cs.creator_id,cs.agent_id,cs.zbs_has_guest_history,a.owner_id,a.permission_mode, \
         (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m \
          WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command')) AS public \
         FROM chat_session cs JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id \
         WHERE cs.id=$1 AND cs.workspace_id=$2",
        &[&session, &ws],
    ).await.map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if row.get::<_, Uuid>("creator_id") != user
        || (row.get::<_, bool>("zbs_has_guest_history")
            && (!matches!(actor, Actor::Human)
                || row.get::<_, Option<Uuid>>("owner_id") != Some(user)))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(ref bound) = actor {
        let valid = db
            .query_opt(
                "SELECT 1 FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND workspace_id=$3 \
             AND chat_session_id=$4 AND status NOT IN ('completed','failed','cancelled')",
                &[&bound.task, &bound.agent, &ws, &session],
            )
            .await
            .map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if valid.is_none() {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    if row.get::<_, Option<Uuid>>("owner_id") != Some(user) {
        let role: String = db
            .query_one(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to resolve agent access",
                )
            })?
            .get(0);
        if role != "owner" && role != "admin" {
            let agent: Uuid = row.get("agent_id");
            let allowed = row.get::<_, String>("permission_mode") == "public_to"
                && db
                    .query_opt(
                        "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND \
                     ((target_type='workspace' AND target_id=$2) OR \
                      (target_type='member' AND target_id=$3)) LIMIT 1",
                        &[&agent, &ws, &user],
                    )
                    .await
                    .map_err(|_| {
                        error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "failed to resolve agent access",
                        )
                    })?
                    .is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
        }
    }
    if !row.get::<_, bool>("public") {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }

    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start prioritize transaction",
        )
    })?;
    let agent: Uuid = row.get("agent_id");
    let locked = tx
        .query_one(
            "SELECT owner_id,permission_mode FROM agent WHERE id=$1 FOR UPDATE",
            &[&agent],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to lock chat agent",
            )
        })?;
    // A changed owner or guest-history bit must not inherit an earlier gate.
    let current = tx
        .query_opt(
            "SELECT cs.zbs_has_guest_history,cs.zbs_source_channel_type, \
             (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m \
             WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command')) AS public \
             FROM chat_session cs WHERE cs.id=$1 AND cs.workspace_id=$2 \
             AND cs.agent_id=$3 AND cs.creator_id=$4",
            &[&session, &ws, &agent, &user],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load chat session",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if current.get::<_, bool>("zbs_has_guest_history")
        && (!matches!(actor, Actor::Human)
            || locked.get::<_, Option<Uuid>>("owner_id") != Some(user))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(ref bound) = actor {
        let bound_task = tx
            .query_opt(
                "SELECT 1 FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND workspace_id=$3 \
             AND chat_session_id=$4 AND status NOT IN ('completed','failed','cancelled')",
                &[&bound.task, &bound.agent, &ws, &session],
            )
            .await
            .map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if bound_task.is_none() {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    if locked.get::<_, Option<Uuid>>("owner_id") != Some(user) {
        let role: String = tx
            .query_one(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to resolve agent access",
                )
            })?
            .get(0);
        if role != "owner" && role != "admin" {
            let allowed = locked.get::<_, String>("permission_mode") == "public_to"
                && tx
                    .query_opt(
                        "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND \
                     ((target_type='workspace' AND target_id=$2) OR \
                      (target_type='member' AND target_id=$3)) LIMIT 1",
                        &[&agent, &ws, &user],
                    )
                    .await
                    .map_err(|_| {
                        error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "failed to resolve agent access",
                        )
                    })?
                    .is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
        }
    }
    if !current.get::<_, bool>("public") {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }
    let updated = tx
        .query_opt(PRIORITIZE, &[&task, &session])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to prioritize queued task",
            )
        })?;
    let Some(updated) = updated else {
        let queued = tx.query_opt(
            "SELECT 1 FROM agent_task_queue WHERE id=$1 AND chat_session_id=$2 AND status='queued'",
            &[&task, &session],
        ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load queued task"))?;
        return Err(error(
            StatusCode::CONFLICT,
            if queued.is_some() {
                "there is no active reply to replace"
            } else {
                "task is no longer queued"
            },
        ));
    };
    let result = Prioritized {
        task_id: updated.get("task_id"),
        active_task_id: updated.get("active_task_id"),
    };
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit queued task priority",
        )
    })?;
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::task_queued(ws, user, session, task, agent),
        user,
        current.get("zbs_has_guest_history"),
        current
            .get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref(),
        locked.get("owner_id"),
    );
    Ok(GoJson(result))
}

// Lock session before agent: send, delete and claim share this lock order.
async fn clear_queued(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to clear queued tasks",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to clear queued tasks",
        )
    })?;
    tx.query_opt(
        "SELECT id FROM chat_session WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to lock chat session",
        )
    })?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let row = super::runtime_chat_archive::check_access(&tx, ws, user, &actor, id).await?;
    let public = tx.query_opt(
        "SELECT 1 FROM chat_session cs WHERE cs.id=$1 AND (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'))",
        &[&id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load chat session"))?;
    if public.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }
    let agent: Uuid = row.get("agent_id");
    tx.query_one("SELECT id FROM agent WHERE id=$1 FOR UPDATE", &[&agent])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to lock chat agent",
            )
        })?;
    let tasks = tx
        .query(include_str!("runtime_chat_clear_queue.sql"), &[&id])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to clear queued tasks",
            )
        })?;
    for task in &tasks {
        let task_id: Uuid = task.get("id");
        let input_owner: Uuid = task
            .get::<_, Option<Uuid>>("chat_input_task_id")
            .unwrap_or(task_id);
        // Cancelled queued inputs are never restored into a composer. A channel
        // sender has no composer, so retain the input and add a stopped reply.
        let channel: bool = tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM chat_message WHERE task_id=$1 AND role='user' AND channel_ingested)",
            &[&input_owner],
        ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued chat input"))?.get(0);
        if channel {
            tx.execute(
                "INSERT INTO chat_message(chat_session_id,role,content,task_id) VALUES ($1,'assistant','Stopped.',$2)",
                &[&id, &task_id],
            ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued chat input"))?;
        } else {
            tx.execute(
                "UPDATE chat_message SET task_id=(SELECT successor.id FROM agent_task_queue successor WHERE successor.chat_session_id=chat_message.chat_session_id AND successor.status='queued' AND successor.chat_input_task_id=successor.id AND successor.regenerate_quick_actions_for IS NULL AND successor.id<>$1 ORDER BY successor.priority DESC,successor.created_at,successor.id LIMIT 1) WHERE task_id=$1 AND role='user' AND message_kind='onboarding_kickoff'",
                &[&input_owner],
            ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued chat input"))?;
            tx.execute(
                "DELETE FROM chat_message WHERE task_id=$1 AND role='user' AND message_kind<>'onboarding_kickoff'",
                &[&input_owner],
            ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued chat input"))?;
        }
        tx.execute("UPDATE task_supplement SET status='failed',failure_reason='turn_ended',updated_at=now() WHERE task_id=$1 AND status IN ('pending','delivering')", &[&task_id])
            .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued task"))?;
        tx.execute("UPDATE comment SET recovery_settled_at=now() WHERE id=ANY($1::uuid[]) AND author_type='system' AND type='progress_update' AND source_task_id IS NOT NULL AND recovery_settled_at IS NULL", &[&task.get::<_, Vec<Uuid>>("delivered_comment_ids")])
            .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle queued task"))?;
        tx.execute("DELETE FROM task_token WHERE task_id=$1", &[&task_id])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to revoke task token",
                )
            })?;
    }
    if !tasks.is_empty() {
        tx.execute("UPDATE agent SET status=CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue q WHERE q.agent_id=$1 AND q.status IN ('dispatched','running')) THEN 'working' ELSE 'idle' END,updated_at=now() WHERE id=$1", &[&agent])
            .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to reconcile agent status"))?;
    }
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to clear queued tasks",
        )
    })?;
    let creator: Uuid = row.get("creator_id");
    let guest_history: bool = row.get("zbs_has_guest_history");
    let channel: Option<String> = row.get("zbs_source_channel_type");
    let owner: Option<Uuid> = row.get("owner_id");
    for task in &tasks {
        state.events.publish_chat_audience(
            super::runtime_chat_events::Event::task_cancelled(ws, user, id, task.get("id"), agent),
            creator,
            guest_history,
            channel.as_deref(),
            owner,
        );
    }
    // Go's CancelQueuedChatTasks ends with notifyTasksFinished, which coalesces
    // one terminal wake per runtime after every broadcast.
    let mut woken = std::collections::HashSet::new();
    for runtime in tasks
        .iter()
        .filter_map(|task| task.get::<_, Option<Uuid>>("runtime_id"))
        .filter(|runtime| woken.insert(*runtime))
    {
        state.wake_daemon(Some(runtime), None);
    }
    Ok(StatusCode::NO_CONTENT)
}
