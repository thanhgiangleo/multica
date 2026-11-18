//! Quick-chat agent pins scoped to the authenticated user and workspace.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{delete, post},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/chat/pinned-agents", post(pin))
        .route("/api/chat/pinned-agents/{agentId}", delete(unpin))
}

#[derive(Deserialize)]
struct PinRequest {
    agent_id: String,
}

#[derive(Serialize)]
struct PinResponse {
    agent_id: Uuid,
    position: f64,
}

async fn pin(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<PinRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<PinResponse>, ApiError> {
    let Json(request) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let agent = Uuid::parse_str(&request.agent_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent_id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to pin agent"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to pin agent"))?;
    // Serialize the cap and append position for concurrent devices of this user.
    tx.query_one("SELECT id FROM \"user\" WHERE id=$1 FOR UPDATE", &[&user])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to pin agent"))?;
    let role: String = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let unrestricted = matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin";
    let allowed = tx
        .query_opt(
            "SELECT 1 FROM agent a WHERE a.id=$1 AND a.workspace_id=$2 AND a.kind='user' \
         AND ($4::bool OR a.owner_id=$3 OR (a.permission_mode='public_to' AND EXISTS ( \
         SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND ( \
         t.target_type='workspace' OR (t.target_type='member' AND t.target_id=$3)))))",
            &[&agent, &ws, &user, &unrestricted],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve agent access",
            )
        })?;
    if allowed.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let rows = tx
        .query(
            "SELECT agent_id,position FROM chat_pinned_agent WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list pinned agents",
            )
        })?;
    if !rows.iter().any(|r| r.get::<_, Uuid>("agent_id") == agent) && rows.len() >= 5 {
        return Err(error(StatusCode::BAD_REQUEST, "pinned agent limit reached"));
    }
    let position = rows
        .iter()
        .map(|r| r.get::<_, f64>("position"))
        .fold(0f64, f64::max)
        + 1.0;
    let row = tx.query_one(
        "INSERT INTO chat_pinned_agent(workspace_id,user_id,agent_id,position) VALUES ($1,$2,$3,$4) \
         ON CONFLICT (workspace_id,user_id,agent_id) DO UPDATE SET position=chat_pinned_agent.position \
         RETURNING agent_id,position",
        &[&ws, &user, &agent, &position],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to pin agent"))?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to pin agent"))?;
    Ok(GoJson(PinResponse {
        agent_id: row.get("agent_id"),
        position: row.get("position"),
    }))
}

async fn unpin(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agentId"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to unpin agent"))?;
    db.execute(
        "DELETE FROM chat_pinned_agent WHERE workspace_id=$1 AND user_id=$2 AND agent_id=$3",
        &[&ws, &user, &id],
    )
    .await
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to unpin agent"))?;
    Ok(StatusCode::NO_CONTENT)
}
