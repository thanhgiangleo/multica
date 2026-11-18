//! Per-user quick-chat agent pins, filtered against the current agent view policy.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error};
use axum::{
    Router,
    extract::{Extension, State},
    http::StatusCode,
    routing::get,
};
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/chat/pinned-agents", get(list))
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role: String = db
        .query_one(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    // Go's ListAllAgents includes archived user agents. A task actor sees the
    // entire user-agent inventory; a member follows owner/admin and target rules.
    // The pin row has no foreign keys, so join the agent under the selected
    // workspace before evaluating any permission or returning its identifier.
    let unrestricted = matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin";
    let rows = db
        .query(
            "SELECT p.agent_id, p.position FROM chat_pinned_agent p \
         JOIN agent a ON a.id=p.agent_id AND a.workspace_id=p.workspace_id AND a.kind='user' \
         WHERE p.workspace_id=$1 AND p.user_id=$2 AND (\
             $3::bool OR a.owner_id=$2 OR (a.permission_mode='public_to' AND EXISTS (\
               SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id \
               AND (t.target_type='workspace' OR (t.target_type='member' AND t.target_id=$2))\
             ))\
         ) ORDER BY p.position ASC, p.created_at ASC",
            &[&ws, &user, &unrestricted],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list pinned agents",
            )
        })?;
    Ok(GoJson(json!(
        rows.iter()
            .map(|row| json!({
                "agent_id": row.get::<_, Uuid>("agent_id"),
                "position": row.get::<_, f64>("position"),
            }))
            .collect::<Vec<_>>()
    )))
}
