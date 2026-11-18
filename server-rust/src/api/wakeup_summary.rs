//! Frozen-Go workspace wakeup board summary: three previews per issue, exact counts.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, agent_activity::allowed_agents,
    catalog::WorkspaceId, error, timestamp,
};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Serialize)]
pub(super) struct Summary {
    issue_id: Uuid,
    id: Uuid,
    agent_id: Uuid,
    agent_name: String,
    kind: String,
    mode: String,
    event_types: Vec<String>,
    filter_actor_type: Option<String>,
    filter_actor_id: Option<Uuid>,
    filter_actor_name: String,
    filter_task_id: Option<Uuid>,
    filter_agent_name: Option<String>,
    interval_seconds: Option<i64>,
    cron_expression: Option<String>,
    timezone: String,
    next_fire_at: Option<String>,
    condition: Option<Value>,
    active_count: i64,
    event_count: i64,
}

fn summary(row: Row) -> Summary {
    Summary {
        issue_id: row.get("issue_id"),
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_name: row.get("agent_name"),
        kind: row.get("kind"),
        mode: row.get("mode"),
        event_types: row.get("event_types"),
        filter_actor_type: row.get("filter_actor_type"),
        filter_actor_id: row.get("filter_actor_id"),
        filter_actor_name: row.get("filter_actor_name"),
        filter_task_id: row.get("filter_task_id"),
        filter_agent_name: row.get("filter_agent_name"),
        interval_seconds: row.get("interval_seconds"),
        cron_expression: row.get("cron_expression"),
        timezone: row.get("timezone"),
        next_fire_at: row
            .get::<_, Option<DateTime<Utc>>>("next_fire_at")
            .map(timestamp),
        condition: row.get("condition"),
        active_count: row.get("active_count"),
        event_count: row.get("event_count"),
    }
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Vec<Summary>>, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let ids = allowed_agents(&db, ws, user, &actor).await?;
    let rows = db
        .query(include_str!("wakeup_summary.sql"), &[&ids, &ws])
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(rows.into_iter().map(summary).collect()))
}
