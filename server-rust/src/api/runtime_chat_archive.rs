//! Archive and unarchive a creator-owned chat, cancelling channel turns before retiring its route.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error,
    runtime_chat::Session, runtime_chat::session,
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::patch,
};
use deadpool_postgres::GenericClient;
use serde::Deserialize;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/chat/sessions/{sessionId}/archive", patch(archive))
}

#[derive(Deserialize)]
struct ArchiveRequest {
    #[serde(default)]
    archived: bool,
}

pub(super) async fn check_access(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    id: Uuid,
) -> Result<Row, ApiError> {
    let row = db
        .query_opt(
            "SELECT cs.*,a.owner_id,a.permission_mode FROM chat_session cs \
         JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id \
         WHERE cs.id=$1 AND cs.workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load chat session",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let owner: Option<Uuid> = row.get("owner_id");
    if row.get::<_, Uuid>("creator_id") != user
        || (row.get::<_, bool>("zbs_has_guest_history")
            && (!matches!(actor, Actor::Human) || owner != Some(user)))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(task) = actor {
        let bound = db.query_opt(
            "SELECT 1 FROM agent_task_queue WHERE id=$1 AND workspace_id=$2 \
             AND agent_id=$3 AND chat_session_id=$4 AND status NOT IN ('completed','failed','cancelled')",
            &[&task.task, &ws, &task.agent, &id],
        ).await.map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if bound.is_none() {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    if matches!(actor, Actor::Daemon) {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if owner != Some(user) {
        let role = db
            .query_opt(
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
            .ok_or_else(|| {
                error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                )
            })?
            .get::<_, String>(0);
        if role != "owner" && role != "admin" {
            let agent: Uuid = row.get("agent_id");
            let allowed = row.get::<_, String>("permission_mode") == "public_to"
                && db.query_opt(
                    "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND \
                     ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1",
                    &[&agent, &ws, &user],
                ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve agent access"))?.is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
        }
    }
    Ok(row)
}

/// A task this session cancellation retired. The runtime is carried out of the
/// transaction so the caller can wake it once per runtime after commit:
/// cancelling can clear a capacity or serialization slot that a queued
/// successor is waiting on.
pub(super) struct Cancelled {
    pub(super) task: Uuid,
    pub(super) runtime: Option<Uuid>,
}

pub(super) async fn cancel_tasks(
    tx: &deadpool_postgres::Transaction<'_>,
    id: Uuid,
    agent: Uuid,
) -> Result<Vec<Cancelled>, ApiError> {
    let rows = tx.query(
    "UPDATE agent_task_queue SET status='cancelled',completed_at=now(),prepare_lease_expires_at=NULL, \
     cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL \
     WHERE chat_session_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') \
     RETURNING id,runtime_id,delivered_comment_ids",
    &[&id],
).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to cancel chat session tasks"))?;
    let cancelled: Vec<Uuid> = rows.iter().map(|r| r.get::<_, Uuid>("id")).collect();
    if !cancelled.is_empty() {
        tx.execute(
        "UPDATE task_supplement AS s SET status='failed',failure_reason='turn_ended',updated_at=now() \
         FROM agent_task_queue AS t WHERE s.task_id=t.id AND t.id=ANY($1) \
         AND t.status IN ('completed','failed','cancelled') AND s.status IN ('pending','delivering')",
        &[&cancelled],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle terminal task state"))?;
        for task in rows
            .iter()
            .filter(|r| !r.get::<_, Vec<Uuid>>("delivered_comment_ids").is_empty())
        {
            let task_id: Uuid = task.get("id");
            tx.execute(
            "UPDATE comment AS recovery SET recovery_settled_at=now() FROM agent_task_queue AS t \
             WHERE t.id=$1 AND t.status IN ('completed','failed','cancelled') \
             AND recovery.id=ANY(t.delivered_comment_ids) AND recovery.author_type='system' \
             AND recovery.type='progress_update' AND recovery.source_task_id IS NOT NULL \
             AND recovery.recovery_settled_at IS NULL",
            &[&task_id],
        ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to settle terminal task state"))?;
        }
        tx.execute(
            "DELETE FROM task_token WHERE task_id=ANY($1)",
            &[&cancelled],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to revoke task tokens",
            )
        })?;
        tx.execute(
        "UPDATE agent AS a SET status=CASE WHEN EXISTS (SELECT 1 FROM agent_task_queue q WHERE q.agent_id=a.id \
         AND q.status IN ('dispatched','running')) THEN 'working' ELSE 'idle' END, updated_at=now() WHERE a.id=$1",
        &[&agent],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to reconcile agent status"))?;
    }
    Ok(rows
        .iter()
        .map(|row| Cancelled {
            task: row.get("id"),
            runtime: row.get("runtime_id"),
        })
        .collect())
}

pub(super) async fn delete(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    let row = check_access(&db, ws, user, &actor, id).await?;
    if matches!(
        row.get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref(),
        Some("telegram" | "zalo")
    ) {
        return Err(error(
            StatusCode::CONFLICT,
            "Channel conversations must be archived and cannot be permanently deleted",
        ));
    }
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    // Exclusive session lock blocks a concurrent append's FK validation until
    // the delete commits. Agent -> task ordering matches send and claim.
    let exists = tx
        .query_opt(
            "SELECT id FROM chat_session WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to lock chat session",
            )
        })?;
    if exists.is_none() {
        return Ok(StatusCode::NO_CONTENT);
    }
    let row = check_access(&tx, ws, user, &actor, id).await?;
    if matches!(
        row.get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref(),
        Some("telegram" | "zalo")
    ) {
        return Err(error(
            StatusCode::CONFLICT,
            "Channel conversations must be archived and cannot be permanently deleted",
        ));
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
    let cancelled = cancel_tasks(&tx, id, agent).await?;
    tx.execute(include_str!("runtime_chat_archive.sql"), &[&id])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete chat session binding",
            )
        })?;
    for (query, message) in [
        (
            "DELETE FROM channel_outbound_card_message WHERE chat_session_id=$1",
            "failed to delete chat session outbound cards",
        ),
        (
            "DELETE FROM chat_draft_restore WHERE chat_session_id=$1",
            "failed to delete chat session draft restores",
        ),
        (
            "DELETE FROM agent_builder_draft WHERE chat_session_id=$1",
            "failed to delete chat session builder draft",
        ),
    ] {
        tx.execute(query, &[&id])
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, message))?;
    }
    tx.execute(
        "DELETE FROM chat_session WHERE id=$1 AND workspace_id=$2 AND creator_id=$3",
        &[&id, &ws, &user],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete chat session",
        )
    })?;
    tx.execute("DELETE FROM agent_to_label WHERE agent_id=$1 AND EXISTS (SELECT 1 FROM agent WHERE id=$1 AND kind='system' AND system_key LIKE 'agent_builder:%')", &[&agent])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to remove chat session agent label assignments"))?;
    tx.execute(
        "DELETE FROM agent WHERE id=$1 AND kind='system' AND system_key LIKE 'agent_builder:%'",
        &[&agent],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to clean up chat session agent",
        )
    })?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit chat session delete",
        )
    })?;
    for task in &cancelled {
        state
            .events
            .publish(super::runtime_chat_events::Event::task_cancelled(
                ws, user, id, task.task, agent,
            ));
    }
    // Go publishes every task:cancelled inside BroadcastCancelledTasks and only
    // then calls notifyTasksFinished, which coalesces one wake per runtime.
    let mut woken = std::collections::HashSet::new();
    for runtime in cancelled
        .iter()
        .filter_map(|task| task.runtime)
        .filter(|runtime| woken.insert(*runtime))
    {
        state.wake_daemon(Some(runtime), None);
    }
    state
        .events
        .publish(super::runtime_chat_events::Event::chat_session_deleted(
            ws, user, id,
        ));
    Ok(StatusCode::NO_CONTENT)
}

async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    body: Result<Json<ArchiveRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Session>, ApiError> {
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    check_access(&db, ws, user, &actor, id).await?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    // A concurrent send must not enqueue a task after this archive. The session
    // lock is taken before the agent lock, matching the direct-send path.
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
    let row = check_access(&tx, ws, user, &actor, id).await?;
    let agent: Uuid = row.get("agent_id");
    tx.query_one("SELECT id FROM agent WHERE id=$1 FOR UPDATE", &[&agent])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to lock chat agent",
            )
        })?;
    let updated = tx.query_one(
        "UPDATE chat_session SET status=CASE WHEN $3 THEN 'archived' ELSE 'active' END, updated_at=now() \
         WHERE id=$1 AND workspace_id=$2 AND creator_id=$4 RETURNING *",
        &[&id, &ws, &body.archived, &user],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update chat session"))?;
    let mut cancelled = Vec::new();
    if body.archived {
        let bound = tx
            .query_opt(
                "SELECT id FROM channel_chat_session_binding WHERE chat_session_id=$1",
                &[&id],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to read chat session channel binding",
                )
            })?
            .is_some();
        if bound {
            cancelled = cancel_tasks(&tx, id, agent).await?;
        }
        tx.execute(include_str!("runtime_chat_archive.sql"), &[&id])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to clear chat session channel binding",
                )
            })?;
    }
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit chat session update",
        )
    })?;
    let updated = session(&updated);
    let creator: Uuid = row.get("creator_id");
    let guest_history: bool = row.get("zbs_has_guest_history");
    let channel: Option<String> = row.get("zbs_source_channel_type");
    let owner: Option<Uuid> = row.get("owner_id");
    for task in &cancelled {
        state.events.publish_chat_audience(
            super::runtime_chat_events::Event::task_cancelled(ws, user, id, task.task, agent),
            creator,
            guest_history,
            channel.as_deref(),
            owner,
        );
    }
    // Post-commit, after the broadcasts, exactly as Go's BroadcastCancelledTasks
    // does: one wake per runtime so a queued successor is claimable now.
    let mut woken = std::collections::HashSet::new();
    for runtime in cancelled
        .iter()
        .filter_map(|task| task.runtime)
        .filter(|runtime| woken.insert(*runtime))
    {
        state.wake_daemon(Some(runtime), None);
    }
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::chat_session_archived(
            ws,
            user,
            id,
            &updated.title,
            &updated.status,
            &updated.updated_at,
        ),
        creator,
        guest_history,
        channel.as_deref(),
        owner,
    );
    Ok(GoJson(updated))
}
