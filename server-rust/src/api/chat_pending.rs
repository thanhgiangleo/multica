//! Pending chat tasks visible to the current creator and chat reader.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/chat/pending-tasks", get(list))
        .route("/api/chat/pending-tasks/has-any", get(has_any))
        .route(
            "/api/chat/sessions/{sessionId}/pending-task",
            get(session_pending),
        )
}

#[derive(Serialize)]
struct PendingTask {
    task_id: Uuid,
    status: String,
    chat_session_id: Uuid,
}

#[derive(Serialize)]
struct PendingTasks {
    tasks: Vec<PendingTask>,
}

#[derive(Serialize)]
struct HasPending {
    has_pending: bool,
}

// ListAllAgents only includes user agents. An authenticated task actor sees all
// such agents; human/cloud members follow owner/admin and invocation targets.
async fn accessible_agents(
    db: &tokio_postgres::Client,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<Vec<Uuid>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to resolve agent access",
        )
    };
    let role: String = db
        .query_one(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    let unrestricted = matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin";
    let rows = db
        .query(
            "SELECT a.id FROM agent a WHERE a.workspace_id=$1 AND a.kind='user' AND (\
             $3::bool OR a.owner_id=$2 OR (a.permission_mode='public_to' AND EXISTS (\
             SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (\
             t.target_type='workspace' OR (t.target_type='member' AND t.target_id=$2)))))",
            &[&ws, &user, &unrestricted],
        )
        .await
        .map_err(|_| fail())?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

// This is the staged Go ListPendingChatTasksByCreator predicate, including the
// quick-action regeneration exclusion. Visibility is checked separately so the
// SQL does not accidentally relax task-session or guest-history fences.
const PENDING_SQL: &str = "SELECT atq.id AS task_id, atq.status, cs.id AS chat_session_id, \
    cs.agent_id, cs.zbs_has_guest_history, a.owner_id \
    FROM agent_task_queue atq JOIN chat_session cs ON cs.id=atq.chat_session_id \
    JOIN agent a ON a.id=cs.agent_id \
    WHERE atq.chat_session_id IS NOT NULL \
    AND atq.status IN ('queued','dispatched','running','waiting_local_directory','deferred') \
    AND atq.regenerate_quick_actions_for IS NULL \
    AND cs.workspace_id=$1 AND cs.creator_id=$2 ORDER BY atq.created_at DESC";

fn visible(
    row: &Row,
    allowed: &[Uuid],
    actor: &Actor,
    user: Uuid,
    bound_session: Option<Uuid>,
) -> bool {
    if !allowed.contains(&row.get::<_, Uuid>("agent_id")) {
        return false;
    }
    let session: Uuid = row.get("chat_session_id");
    let guest: bool = row.get("zbs_has_guest_history");
    match actor {
        Actor::Task(_) => bound_session == Some(session) && !guest,
        Actor::Cloud => !guest,
        Actor::Human => !guest || row.get::<_, Uuid>("owner_id") == user,
        Actor::Daemon => false,
    }
}

async fn pending(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    any: bool,
) -> Result<Vec<PendingTask>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            if any {
                "failed to check pending chat tasks"
            } else {
                "failed to list pending chat tasks"
            },
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let allowed = accessible_agents(&db, ws, user, actor).await?;
    if allowed.is_empty() {
        return Ok(Vec::new());
    }
    let bound_session = if let Actor::Task(task) = actor {
        let row = db
            .query_opt(
                "SELECT chat_session_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2",
                &[&task.task, &task.agent],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::FORBIDDEN,
                    "chat tasks are not available to this actor",
                )
            })?
            .ok_or_else(|| {
                error(
                    StatusCode::FORBIDDEN,
                    "chat tasks are not available to this actor",
                )
            })?;
        row.get::<_, Option<Uuid>>(0)
    } else {
        None
    };
    let rows = db
        .query(PENDING_SQL, &[&ws, &user])
        .await
        .map_err(|_| fail())?;
    Ok(rows
        .iter()
        .filter(|row| visible(row, &allowed, actor, user, bound_session))
        .take(if any { 1 } else { usize::MAX })
        .map(|row| PendingTask {
            task_id: row.get("task_id"),
            status: row.get("status"),
            chat_session_id: row.get("chat_session_id"),
        })
        .collect())
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<PendingTasks>, ApiError> {
    Ok(GoJson(PendingTasks {
        tasks: pending(&state, ws, user, &actor, false).await?,
    }))
}

async fn has_any(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<HasPending>, ApiError> {
    Ok(GoJson(HasPending {
        has_pending: !pending(&state, ws, user, &actor, true).await?.is_empty(),
    }))
}

#[derive(Serialize)]
struct QueuedTask {
    task_id: Uuid,
    status: String,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_id: Option<Uuid>,
    #[serde(skip_serializing_if = "String::is_empty")]
    content: String,
}

#[derive(Serialize)]
struct SessionPending {
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_reason: Option<String>,
    supports_queue: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    queued_tasks: Vec<QueuedTask>,
}

const SESSION_PENDING_SQL: &str = include_str!("chat_session_pending.sql");

async fn session_pending(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<SessionPending>, ApiError> {
    let session = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let row = db
        .query_opt(
            "SELECT cs.creator_id, cs.agent_id, cs.zbs_has_guest_history, a.owner_id, \
         (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m \
         WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command')) AS public \
         FROM chat_session cs JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id \
         WHERE cs.id=$1 AND cs.workspace_id=$2",
            &[&session, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if row.get::<_, Uuid>("creator_id") != user {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    let task_bound = if let Actor::Task(task) = &actor {
        db.query_opt(
            "SELECT chat_session_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2",
            &[&task.task, &task.agent],
        )
        .await
        .ok()
        .flatten()
        .and_then(|r| r.get::<_, Option<Uuid>>(0))
            == Some(session)
    } else {
        true
    };
    if !task_bound
        || (row.get::<_, bool>("zbs_has_guest_history")
            && (!matches!(actor, Actor::Human) || row.get::<_, Uuid>("owner_id") != user))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if !accessible_agents(&db, ws, user, &actor)
        .await?
        .contains(&row.get::<_, Uuid>("agent_id"))
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you do not have access to this agent",
        ));
    }
    if !row.get::<_, bool>("public") {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }
    let tasks = db
        .query(SESSION_PENDING_SQL, &[&session])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list pending chat tasks",
            )
        })?;
    let mut tasks = tasks.iter();
    let Some(head) = tasks.next() else {
        return Ok(GoJson(SessionPending {
            task_id: None,
            status: None,
            created_at: None,
            wait_reason: None,
            supports_queue: true,
            queued_tasks: Vec::new(),
        }));
    };
    let status: String = head.get("status");
    let wait_reason = if status == "waiting_local_directory" {
        head.get::<_, Option<String>>("wait_reason")
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    } else {
        None
    };
    Ok(GoJson(SessionPending {
        task_id: Some(head.get("id")),
        status: Some(status),
        created_at: Some(timestamp(head.get::<_, DateTime<Utc>>("created_at"))),
        wait_reason,
        supports_queue: true,
        queued_tasks: tasks
            .filter(|r| r.get::<_, String>("status") == "queued")
            .map(|r| QueuedTask {
                task_id: r.get("id"),
                status: r.get("status"),
                created_at: timestamp(r.get::<_, DateTime<Utc>>("created_at")),
                message_id: r.get("message_id"),
                content: r.get("content"),
            })
            .collect(),
    }))
}
