//! Chat creation against the workspace-scoped, request-authenticated actor.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/chat/sessions", post(create).get(list))
        .route("/api/chat/sessions/", post(create).get(list))
        .route(
            "/api/chat/sessions/{sessionId}",
            get(get_session)
                .patch(update)
                .delete(super::runtime_chat_archive::delete),
        )
        .route(
            "/api/chat/sessions/{sessionId}/",
            get(get_session)
                .patch(update)
                .delete(super::runtime_chat_archive::delete),
        )
        .route(
            "/api/chat/sessions/{sessionId}/draft-restores/{restoreId}",
            axum::routing::delete(consume_draft_restore),
        )
        .route(
            "/api/chat/sessions/{sessionId}/messages",
            get(messages).post(super::runtime_chat_send::send),
        )
        .route(
            "/api/chat/sessions/{sessionId}/messages/page",
            get(messages_page),
        )
        .route("/api/chat/sessions/{sessionId}/read", post(mark_read))
        .merge(super::runtime_chat_archive::routes())
        .route(
            "/api/chat/sessions/{sessionId}/pin",
            axum::routing::patch(pin),
        )
}

#[derive(Deserialize)]
struct CreateSession {
    #[serde(default)]
    agent_id: String,
    #[serde(default)]
    title: String,
    project_id: Option<String>,
}

#[derive(Serialize)]
pub(super) struct Session {
    pub(super) id: Uuid,
    workspace_id: Uuid,
    agent_id: Uuid,
    creator_id: Uuid,
    project_id: Option<Uuid>,
    pub(super) title: String,
    pub(super) status: String,
    access_mode: &'static str,
    read_only: bool,
    has_unread: bool,
    unread_count: i32,
    last_message: Option<serde_json::Value>,
    pinned: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    source_channel_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    source_channel_chat_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_source: Option<ChannelSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_current_channel_route: Option<bool>,
    created_at: String,
    pub(super) updated_at: String,
}

#[derive(Serialize)]
struct ChannelSource {
    channel_type: String,
    installation_id: Uuid,
    route_revision: i64,
}

pub(super) fn session(row: &Row) -> Session {
    Session {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        agent_id: row.get("agent_id"),
        creator_id: row.get("creator_id"),
        project_id: row.get("project_id"),
        title: row.get("title"),
        status: row.get("status"),
        access_mode: "owner",
        read_only: false,
        has_unread: false,
        unread_count: 0,
        last_message: None,
        pinned: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("pinned_at")
            .is_some(),
        source_channel_type: row
            .get::<_, Option<String>>("zbs_source_channel_type")
            .unwrap_or_default(),
        source_channel_chat_type: row
            .get::<_, Option<String>>("zbs_source_channel_chat_type")
            .unwrap_or_default(),
        channel_source: None,
        is_current_channel_route: None,
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
    }
}

// Invocation is not a view permission: an admin cannot start another member's
// private agent. A task actor uses its verified task's originator, never the
// runtime owner's or a caller-supplied identity header.
pub(super) async fn can_invoke(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    agent: Uuid,
    owner: Option<Uuid>,
    mode: &str,
) -> Result<bool, ApiError> {
    let (effective, broad) = match actor {
        Actor::Task(task) => {
            let row = db.query_opt("SELECT originator_user_id FROM agent_task_queue WHERE id=$1 AND workspace_id=$2 AND agent_id=$3 AND status NOT IN ('completed','failed','cancelled')", &[&task.task, &ws, &task.agent]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve agent access"))?;
            let Some(row) = row else { return Ok(false) };
            (row.get::<_, Option<Uuid>>(0), true)
        }
        Actor::Human | Actor::Cloud => (Some(user), false),
        Actor::Daemon => return Ok(false),
    };
    if effective.is_some() && effective == owner {
        return Ok(true);
    }
    if mode != "public_to" {
        return Ok(false);
    }
    let is_member = if let Some(effective) = effective {
        db.query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &effective],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve agent access",
            )
        })?
        .is_some()
    } else {
        false
    };
    let allowed = db.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2 AND $4::bool) OR (target_type='member' AND target_id=$3)) LIMIT 1", &[&agent, &ws, &effective, &(broad || is_member)]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve agent access"))?;
    Ok(allowed.is_some())
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<CreateSession>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, GoJson<Session>), ApiError> {
    let Json(req) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if req.agent_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "agent_id is required"));
    }
    let agent = Uuid::parse_str(&req.agent_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent_id"))?;
    let project = match req
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(raw) => Some(
            Uuid::parse_str(raw)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid project_id"))?,
        ),
        None => None,
    };
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create chat session",
        )
    })?;
    let agent_row = db.query_opt("SELECT owner_id, permission_mode, archived_at FROM agent WHERE id=$1 AND workspace_id=$2", &[&agent, &ws]).await.map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if agent_row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(error(StatusCode::BAD_REQUEST, "agent is archived"));
    }
    if !can_invoke(
        &**db,
        ws,
        user,
        &actor,
        agent,
        agent_row.get("owner_id"),
        agent_row.get("permission_mode"),
    )
    .await?
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you do not have access to this agent",
        ));
    }
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    let locked = tx
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to lock workspace",
            )
        })?;
    if locked.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    if let Some(project) = project {
        let locked = tx
            .query_opt(
                "SELECT id FROM project WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
                &[&project, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock project"))?;
        if locked.is_none() {
            return Err(error(StatusCode::NOT_FOUND, "project not found"));
        }
    }
    let id = Uuid::now_v7();
    let row = tx.query_one("INSERT INTO chat_session (id,workspace_id,agent_id,creator_id,title,runtime_id,project_id,explicitly_created_at) VALUES ($1,$2,$3,$4,$5,(SELECT runtime_id FROM agent WHERE id=$3),$6,now()) RETURNING *", &[&id, &ws, &agent, &user, &req.title, &project]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create chat session"))?;
    let response = session(&row);
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit chat session create",
        )
    })?;
    Ok((StatusCode::CREATED, GoJson(response)))
}

// Return a public chat only after enforcing current workspace membership,
// creator/guest fences, task-session binding and the current agent owner gate.
async fn get_session(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Session>, ApiError> {
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load chat session",
        )
    })?;
    let row = db
        .query_opt(
            "SELECT * FROM chat_session WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let mut response = session(&row);
    let agent = db
        .query_opt(
            "SELECT owner_id,permission_mode FROM agent WHERE id=$1",
            &[&response.agent_id],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let owner: Option<Uuid> = agent.get("owner_id");
    let human = matches!(actor, Actor::Human);
    if let Actor::Task(ref task) = actor {
        let task_session = db.query_opt("SELECT chat_session_id FROM agent_task_queue WHERE id=$1 AND workspace_id=$2 AND agent_id=$3", &[&task.task, &ws, &task.agent]).await.map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if task_session.and_then(|row| row.get::<_, Option<Uuid>>(0)) != Some(id) {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    let owner_human = human && owner == Some(user);
    let creator = response.creator_id == user
        && (!row.get::<_, bool>("zbs_has_guest_history") || owner_human);
    let owner_view =
        owner_human && matches!(response.source_channel_type.as_str(), "telegram" | "zalo");
    if !creator && !owner_view {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if creator && !matches!(actor, Actor::Task(_)) && owner != Some(user) {
        let role: String = db
            .query_one(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                )
            })?
            .get(0);
        if role != "owner" && role != "admin" {
            let mode: String = agent.get("permission_mode");
            let allowed = mode == "public_to" && db.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1", &[&response.agent_id,&ws,&user]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve agent access"))?.is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
        }
    }
    let public = db.query_opt("SELECT 1 FROM chat_session cs WHERE cs.id=$1 AND cs.workspace_id=$2 AND (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'))", &[&id,&ws]).await.map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if public.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }
    response.read_only = response.creator_id != user;
    response.access_mode = if response.read_only {
        "viewer"
    } else {
        "owner"
    };
    if let Some(binding) = db.query_opt("SELECT channel_type,installation_id,route_revision,retired_at IS NULL AS current FROM channel_chat_session_binding WHERE chat_session_id=$1", &[&id]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load chat channel metadata"))? {
        response.channel_source = Some(ChannelSource {
            channel_type: binding.get("channel_type"), installation_id: binding.get("installation_id"), route_revision: binding.get("route_revision"),
        });
        response.is_current_channel_route = Some(binding.get("current"));
    }
    Ok(GoJson(response))
}

#[derive(Default, Deserialize)]
struct ListFilter {
    status: Option<String>,
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Query(filter): Query<ListFilter>,
) -> Result<GoJson<Vec<Session>>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list chat sessions",
        )
    })?;
    let human = matches!(actor, Actor::Human);
    let task_session = if let Actor::Task(ref task) = actor {
        db.query_opt("SELECT chat_session_id FROM agent_task_queue WHERE id=$1 AND workspace_id=$2 AND agent_id=$3", &[&task.task,&ws,&task.agent]).await.map_err(|_| error(StatusCode::FORBIDDEN, "chat sessions are not available to this actor"))?
            .and_then(|row| row.get::<_, Option<Uuid>>(0))
    } else {
        None
    };
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
    let unrestricted = matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin";
    let rows = db
        .query(
            include_str!("runtime_chat_list.sql"),
            &[
                &ws,
                &user,
                &(filter.status.as_deref() == Some("all")),
                &human,
                &task_session,
                &unrestricted,
            ],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list chat sessions",
            )
        })?;
    let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    let bindings = db.query("SELECT chat_session_id,channel_type,installation_id,route_revision,retired_at IS NULL AS current FROM channel_chat_session_binding WHERE chat_session_id=ANY($1)", &[&ids]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load chat channel metadata"))?;
    let bindings: HashMap<Uuid, Row> = bindings
        .into_iter()
        .map(|row| (row.get("chat_session_id"), row))
        .collect();
    let response = rows
        .iter()
        .map(|row| {
            let mut item = session(row);
            item.read_only = row.get("read_only");
            item.access_mode = if item.read_only { "viewer" } else { "owner" };
            item.unread_count = row.get("unread_count");
            item.has_unread = item.unread_count > 0;
            if let Some(at) = row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_message_at")
                && row.get::<_, String>("last_message_kind") != "onboarding_kickoff"
            {
                let kind: String = row.get("last_message_kind");
                item.last_message = Some(serde_json::json!({
                    "content": row.get::<_, String>("last_message_content"),
                    "role": row.get::<_, String>("last_message_role"),
                    "created_at": timestamp(at),
                    "failure_reason": row.get::<_, Option<String>>("last_message_failure_reason"),
                    "message_kind": if kind.is_empty() {"message"} else {kind.as_str()},
                }));
            }
            if let Some(binding) = bindings.get(&item.id) {
                item.channel_source = Some(ChannelSource {
                    channel_type: binding.get("channel_type"),
                    installation_id: binding.get("installation_id"),
                    route_revision: binding.get("route_revision"),
                });
                item.is_current_channel_route = Some(binding.get("current"));
            }
            item
        })
        .collect();
    Ok(GoJson(response))
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    body: Result<Json<serde_json::Value>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Session>, ApiError> {
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let fields = body
        .as_object()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    // A null title is absent, but a null project is an explicit request to clear it.
    let title = match fields.get("title") {
        Some(serde_json::Value::String(title)) => Some(title.as_str()),
        None | Some(serde_json::Value::Null) => None,
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid request body")),
    };
    let project = fields.get("project_id");
    if title.is_some() == project.is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "exactly one of title or project_id is required",
        ));
    }
    let GoJson(visible) = get_session(
        State(state.clone()),
        Extension(WorkspaceId(ws)),
        Extension(Identity(user)),
        Extension(actor),
        Path(raw_id),
    )
    .await?;
    if visible.read_only {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update chat session",
        )
    })?;
    let row = if let Some(title) = title {
        let title = title.trim();
        if title.is_empty() {
            return Err(error(StatusCode::BAD_REQUEST, "title is required"));
        }
        if title.chars().count() > 200 {
            return Err(error(StatusCode::BAD_REQUEST, "title is too long"));
        }
        db.query_one("UPDATE chat_session SET title=$3, updated_at=now() WHERE id=$1 AND workspace_id=$2 AND creator_id=$4 RETURNING *, (SELECT owner_id FROM agent WHERE id=chat_session.agent_id) AS agent_owner_id", &[&visible.id, &ws, &title, &user])
            .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update chat session"))?
    } else {
        let project = match project.unwrap() {
            serde_json::Value::Null => None,
            serde_json::Value::String(raw) if !raw.trim().is_empty() => Some(
                Uuid::parse_str(raw.trim())
                    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid project_id"))?,
            ),
            _ => {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "project_id must be a UUID or null",
                ));
            }
        };
        let tx = db.transaction().await.map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to start transaction",
            )
        })?;
        if let Some(project) = project
            && tx
                .query_opt(
                    "SELECT id FROM project WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
                    &[&project, &ws],
                )
                .await
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock project"))?
                .is_none()
        {
            return Err(error(StatusCode::NOT_FOUND, "project not found"));
        }
        let row = tx.query_one("UPDATE chat_session SET project_id=$3 WHERE id=$1 AND workspace_id=$2 AND creator_id=$4 RETURNING *, (SELECT owner_id FROM agent WHERE id=chat_session.agent_id) AS agent_owner_id", &[&visible.id, &ws, &project, &user])
            .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update chat session"))?;
        tx.commit().await.map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update chat session",
            )
        })?;
        row
    };
    let updated = session(&row);
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::chat_session_updated(
            ws,
            user,
            updated.id,
            &updated.title,
            &updated.updated_at,
            project.is_some().then_some(updated.project_id),
            None,
        ),
        row.get("creator_id"),
        row.get("zbs_has_guest_history"),
        row.get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref(),
        row.get("agent_owner_id"),
    );
    Ok(GoJson(updated))
}

#[derive(Deserialize)]
struct PinRequest {
    pinned: bool,
}

async fn pin(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    body: Result<Json<PinRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Session>, ApiError> {
    let Json(req) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let GoJson(visible) = get_session(
        State(state.clone()),
        Extension(WorkspaceId(ws)),
        Extension(Identity(user)),
        Extension(actor),
        Path(raw_id),
    )
    .await?;
    if visible.read_only {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update chat session",
        )
    })?;
    let row = db.query_one("UPDATE chat_session SET pinned_at=CASE WHEN $4::bool THEN COALESCE(pinned_at,now()) ELSE NULL END WHERE id=$1 AND workspace_id=$2 AND creator_id=$3 RETURNING *, (SELECT owner_id FROM agent WHERE id=chat_session.agent_id) AS agent_owner_id", &[&visible.id, &ws, &user, &req.pinned]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update chat session"))?;
    let updated = session(&row);
    state.events.publish_chat_audience(
        super::runtime_chat_events::Event::chat_session_updated(
            ws,
            user,
            updated.id,
            &updated.title,
            &updated.updated_at,
            None,
            Some(updated.pinned),
        ),
        row.get("creator_id"),
        row.get("zbs_has_guest_history"),
        row.get::<_, Option<String>>("zbs_source_channel_type")
            .as_deref(),
        row.get("agent_owner_id"),
    );
    Ok(GoJson(updated))
}

#[derive(Serialize)]
struct ChatAttachment {
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
struct ChatMessage {
    id: Uuid,
    chat_session_id: Uuid,
    role: String,
    content: String,
    task_id: Option<Uuid>,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_user_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_external_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_display_name: Option<String>,
    failure_reason: Option<String>,
    elapsed_ms: Option<i64>,
    message_kind: String,
    quick_actions: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<ChatAttachment>,
}

#[derive(Serialize)]
struct MessagesPage {
    messages: Vec<ChatMessage>,
    limit: usize,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<MessageCursor>,
}

#[derive(Serialize)]
struct MessageCursor {
    created_at: String,
    id: Uuid,
}

#[derive(Default, Deserialize)]
struct MessagesFilter {
    limit: Option<String>,
    before_created_at: Option<String>,
    before_id: Option<String>,
}

// Keep the visible-head predicate identical to frozen Go's ListChatMessages
// and ListChatMessagesPage: queued follow-ups are not conversation history.
fn message_sql(page: bool) -> &'static str {
    let (list, page_sql) = include_str!("runtime_chat_messages.sql")
        .split_once("-- PAGE")
        .expect("both chat projections are present");
    // SQL constants must have a static lifetime; avoid allocating on every read.
    if page { page_sql } else { list }
}

async fn chat_messages(
    db: &impl GenericClient,
    ws: Uuid,
    rows: Vec<Row>,
) -> Result<Vec<ChatMessage>, ApiError> {
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let mut attachments: HashMap<Uuid, Vec<ChatAttachment>> = HashMap::new();
    if !ids.is_empty() {
        let found = db.query(
            "SELECT id,workspace_id,issue_id,comment_id,chat_session_id,chat_message_id,\
             uploader_type,uploader_id,filename,url,content_type,size_bytes,created_at \
             FROM attachment WHERE workspace_id=$1 AND chat_message_id=ANY($2::uuid[]) ORDER BY created_at ASC",
            &[&ws, &ids],
        ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list chat messages"))?;
        for r in found {
            let id: Uuid = r.get("id");
            let path = format!("/api/attachments/{id}/download");
            attachments
                .entry(r.get("chat_message_id"))
                .or_default()
                .push(ChatAttachment {
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
                    created_at: timestamp(r.get("created_at")),
                });
        }
    }
    Ok(rows
        .into_iter()
        .filter(|r| r.get::<_, String>("message_kind") != "onboarding_kickoff")
        .map(|r| {
            let id: Uuid = r.get("id");
            let actions: serde_json::Value = r.get("quick_actions");
            ChatMessage {
                id,
                chat_session_id: r.get("chat_session_id"),
                role: r.get("role"),
                content: r.get("content"),
                task_id: r.get("task_id"),
                created_at: timestamp(r.get("created_at")),
                sender_user_id: r.get("zbs_sender_user_id"),
                sender_external_id: r.get("zbs_sender_external_id"),
                sender_display_name: r.get("zbs_sender_display_name"),
                failure_reason: r.get("failure_reason"),
                elapsed_ms: r.get("elapsed_ms"),
                message_kind: r.get("message_kind"),
                quick_actions: actions.as_array().cloned().unwrap_or_default(),
                attachments: attachments.remove(&id).unwrap_or_default(),
            }
        })
        .collect())
}

async fn messages(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Vec<ChatMessage>>, ApiError> {
    let GoJson(session) = get_session(
        State(state.clone()),
        Extension(WorkspaceId(ws)),
        Extension(Identity(user)),
        Extension(actor),
        Path(raw_id),
    )
    .await?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list chat messages",
        )
    })?;
    let rows = db
        .query(message_sql(false), &[&session.id])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list chat messages",
            )
        })?;
    Ok(GoJson(chat_messages(&**db, ws, rows).await?))
}

async fn messages_page(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    Query(filter): Query<MessagesFilter>,
) -> Result<GoJson<MessagesPage>, ApiError> {
    let GoJson(session) = get_session(
        State(state.clone()),
        Extension(WorkspaceId(ws)),
        Extension(Identity(user)),
        Extension(actor),
        Path(raw_id),
    )
    .await?;
    let limit = match filter.limit.as_deref().filter(|v| !v.is_empty()) {
        None => 50,
        Some(raw) => raw
            .parse::<usize>()
            .ok()
            .filter(|v| (1..=100).contains(v))
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid limit"))?,
    };
    let cursor = match (
        filter
            .before_created_at
            .as_deref()
            .filter(|v| !v.is_empty()),
        filter.before_id.as_deref().filter(|v| !v.is_empty()),
    ) {
        (None, None) => None,
        (Some(at), Some(id)) => Some((
            DateTime::parse_from_rfc3339(at)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid cursor"))?
                .with_timezone(&Utc),
            Uuid::parse_str(id).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid cursor"))?,
        )),
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid cursor")),
    };
    let (before_at, before_id) = cursor.unzip();
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list chat messages",
        )
    })?;
    // One extra for lookahead and one for the hidden onboarding kickoff.
    let rows = db
        .query(
            message_sql(true),
            &[&session.id, &((limit + 2) as i64), &before_at, &before_id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list chat messages",
            )
        })?;
    let mut visible: Vec<Row> = rows
        .into_iter()
        .filter(|r| r.get::<_, String>("message_kind") != "onboarding_kickoff")
        .collect();
    let has_more = visible.len() > limit;
    visible.truncate(limit);
    let next_cursor = if has_more {
        visible.last().map(|r| MessageCursor {
            created_at: r
                .get::<_, DateTime<Utc>>("created_at")
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            id: r.get("id"),
        })
    } else {
        None
    };
    visible.reverse();
    Ok(GoJson(MessagesPage {
        messages: chat_messages(&**db, ws, visible).await?,
        limit,
        has_more,
        next_cursor,
    }))
}

// A read cursor is private to the session creator. The regular session detail
// permits channel-owner viewer access, which must not be reused for this write.
async fn mark_read(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load chat session",
        )
    })?;
    let row = db.query_opt(
        "SELECT cs.creator_id,cs.zbs_has_guest_history,a.owner_id,a.permission_mode,cs.agent_id \
         FROM chat_session cs JOIN agent a ON a.id=cs.agent_id \
         WHERE cs.id=$1 AND cs.workspace_id=$2",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if row.get::<_, Uuid>("creator_id") != user
        || (row.get::<_, bool>("zbs_has_guest_history")
            && (!matches!(actor, Actor::Human)
                || row.get::<_, Option<Uuid>>("owner_id") != Some(user)))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(ref task) = actor {
        let bound = db.query_opt(
            "SELECT 1 FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND workspace_id=$3 AND chat_session_id=$4",
            &[&task.task, &task.agent, &ws, &id],
        ).await.map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if bound.is_none() {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    let agent_id: Uuid = row.get("agent_id");
    if row.get::<_, Option<Uuid>>("owner_id") != Some(user) {
        let role: String = db
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
            .get(0);
        if role != "owner" && role != "admin" {
            let mode: String = row.get("permission_mode");
            let allowed = mode == "public_to" && db.query_opt(
                "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1",
                &[&agent_id, &ws, &user],
            ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve agent access"))?.is_some();
            if !allowed {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
        }
    }
    let public = db.query_opt(
        "SELECT 1 FROM chat_session cs WHERE cs.id=$1 AND cs.workspace_id=$2 AND (cs.explicitly_created_at IS NOT NULL OR EXISTS (SELECT 1 FROM chat_message m WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'))",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if public.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "chat session not found"));
    }
    db.execute(
        "UPDATE chat_session SET last_read_at=now() WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to mark session read",
        )
    })?;
    state
        .events
        .publish(super::runtime_chat_events::Event::chat_session_read(
            ws, user, id,
        ));
    Ok(StatusCode::NO_CONTENT)
}

async fn consume_draft_restore(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((session, restore)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let session = Uuid::parse_str(&session)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid chat session id"))?;
    let restore = Uuid::parse_str(&restore)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid restore id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let row = db
        .query_opt(
            "SELECT cs.creator_id,cs.zbs_has_guest_history,a.owner_id FROM chat_session cs \
         LEFT JOIN agent a ON a.id=cs.agent_id WHERE cs.id=$1 AND cs.workspace_id=$2",
            &[&session, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "chat session not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    if row.get::<_, Uuid>("creator_id") != user {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if row.get::<_, bool>("zbs_has_guest_history")
        && (!matches!(actor, Actor::Human) || row.get::<_, Option<Uuid>>("owner_id") != Some(user))
    {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if let Actor::Task(task) = actor {
        let bound = db.query_opt(
            "SELECT 1 FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND workspace_id=$3 AND chat_session_id=$4",
            &[&task.task, &task.agent, &ws, &session],
        ).await.map_err(|_| error(StatusCode::FORBIDDEN, "not your chat session"))?;
        if bound.is_none() {
            return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
        }
    }
    db.execute(
        "DELETE FROM chat_draft_restore WHERE id=$1 AND chat_session_id=$2",
        &[&restore, &session],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to consume draft restore",
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod transcript_tests {
    #[test]
    fn cursor_preserves_fractional_seconds() {
        let time = chrono::DateTime::parse_from_rfc3339("2025-01-01T01:02:03.123456Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let encoded = time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
        assert_eq!(encoded, "2025-01-01T01:02:03.123456Z");
    }
}
