//! Board cues for paused rules on open issues, from frozen wakeup.sql.
use super::{ApiError, ApiState, GoJson, catalog::WorkspaceId, error};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
};
use serde::Serialize;
use uuid::Uuid;

const PAUSED: &str = r#"SELECT w.issue_id,w.id,w.agent_id,w.paused_reason
FROM issue_wakeup w JOIN issue i ON i.id=w.issue_id AND i.workspace_id=w.workspace_id
WHERE w.workspace_id=$1 AND NOT w.enabled AND w.paused_reason IN ('loop','rate','max_fires')
 AND i.status NOT IN ('done','cancelled')
 AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=i.status AND s.category IN ('done','closed'))
ORDER BY w.updated_at DESC LIMIT 200"#;

#[derive(Serialize)]
pub(super) struct Paused {
    issue_id: Uuid,
    id: Uuid,
    agent_id: Uuid,
    paused_reason: String,
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
) -> Result<GoJson<Vec<Paused>>, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db.query(PAUSED, &[&workspace]).await.map_err(|_| fail())?;
    Ok(GoJson(
        rows.into_iter()
            .map(|row| Paused {
                issue_id: row.get("issue_id"),
                id: row.get("id"),
                agent_id: row.get("agent_id"),
                paused_reason: row.get("paused_reason"),
            })
            .collect(),
    ))
}
