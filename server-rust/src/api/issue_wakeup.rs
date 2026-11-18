//! Issue-bound wakeup reads using the frozen Go query and agent redaction scope.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, agent_activity::allowed_agents,
    catalog::WorkspaceId, error, issue_read::load_issue, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/wakeups", get(list))
        .route(
            "/api/issues/{id}/system-wakeups",
            get(super::issue_system_wakeup::list),
        )
        .route(
            "/api/issues/{id}/system-wakeups/{rule}",
            axum::routing::put(super::issue_system_wakeup::put),
        )
        .route("/api/issues/{id}/wakeups/{wakeup_id}/runs", get(runs))
}

fn date(row: &Row, key: &str) -> Option<String> {
    row.get::<_, Option<DateTime<Utc>>>(key).map(timestamp)
}

fn wakeup(row: Row) -> Value {
    let mut out = serde_json::Map::new();
    macro_rules! put {
        ($key:literal, $value:expr) => {
            out.insert($key.into(), json!($value));
        };
    }
    put!("id", row.get::<_, Uuid>("id"));
    put!("workspace_id", row.get::<_, Uuid>("workspace_id"));
    put!("issue_id", row.get::<_, Uuid>("issue_id"));
    put!("agent_id", row.get::<_, Uuid>("agent_id"));
    put!("created_by", row.get::<_, Uuid>("created_by"));
    put!(
        "source_task_id",
        row.get::<_, Option<Uuid>>("source_task_id")
    );
    put!(
        "parent_comment_id",
        row.get::<_, Option<Uuid>>("parent_comment_id")
    );
    put!("instruction", row.get::<_, String>("instruction"));
    put!("kind", row.get::<_, String>("kind"));
    put!("mode", row.get::<_, String>("mode"));
    put!("event_types", row.get::<_, Vec<String>>("event_types"));
    put!(
        "filter_actor_type",
        row.get::<_, Option<String>>("filter_actor_type")
    );
    put!(
        "filter_actor_id",
        row.get::<_, Option<Uuid>>("filter_actor_id")
    );
    put!(
        "filter_actor_name",
        row.get::<_, String>("filter_actor_name")
    );
    put!(
        "filter_agent_id",
        row.get::<_, Option<Uuid>>("filter_agent_id")
    );
    put!(
        "filter_task_id",
        row.get::<_, Option<Uuid>>("filter_task_id")
    );
    put!(
        "interval_seconds",
        row.get::<_, Option<i64>>("interval_seconds")
    );
    put!(
        "cron_expression",
        row.get::<_, Option<String>>("cron_expression")
    );
    put!("timezone", row.get::<_, String>("timezone"));
    put!("next_fire_at", date(&row, "next_fire_at"));
    put!("enabled", row.get::<_, bool>("enabled"));
    put!("disabled_at", date(&row, "disabled_at"));
    put!("revision", row.get::<_, i64>("revision"));
    put!("last_task_id", row.get::<_, Option<Uuid>>("last_task_id"));
    put!("last_error", row.get::<_, Option<String>>("last_error"));
    put!("created_at", date(&row, "created_at"));
    put!("updated_at", date(&row, "updated_at"));
    put!("agent_name", row.get::<_, String>("agent_name"));
    put!(
        "filter_agent_name",
        row.get::<_, Option<String>>("filter_agent_name")
    );
    put!(
        "last_task_status",
        row.get::<_, Option<String>>("last_task_status")
    );
    put!("expires_at", date(&row, "expires_at"));
    put!(
        "expiry_seconds",
        row.get::<_, Option<i64>>("expiry_seconds")
    );
    put!("on_timeout", row.get::<_, Option<String>>("on_timeout"));
    put!("timed_out_at", date(&row, "timed_out_at"));
    put!("condition", row.get::<_, Option<Value>>("condition"));
    put!("max_fires", row.get::<_, Option<i32>>("max_fires"));
    put!("fire_count", row.get::<_, i32>("fire_count"));
    put!(
        "paused_reason",
        row.get::<_, Option<String>>("paused_reason")
    );
    put!("created_by_agent", row.get::<_, bool>("created_by_agent"));
    put!(
        "created_by_name",
        row.get::<_, Option<String>>("created_by_name")
    );
    put!(
        "source_agent_id",
        row.get::<_, Option<Uuid>>("source_agent_id")
    );
    put!(
        "source_agent_name",
        row.get::<_, Option<String>>("source_agent_name")
    );
    Value::Object(out)
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Vec<Value>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let ids = allowed_agents(&db, ws, user, &actor).await?;
    let rows = db
        .query(
            include_str!("issue_wakeup.sql"),
            &[&ids, &ws, &issue.get::<_, Uuid>("id")],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    Ok(GoJson(rows.into_iter().map(wakeup).collect()))
}

// Keep the rule lookup scoped to the selected issue before querying task history.
async fn runs(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path((issue_id, wakeup_id)): Path<(String, String)>,
) -> Result<GoJson<Vec<Value>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let issue = load_issue(&db, ws, &issue_id).await?;
    let id = Uuid::parse_str(&wakeup_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup id"))?;
    let issue_id: Uuid = issue.get("id");
    let rule = db
        .query_opt(
            "SELECT issue_id FROM issue_wakeup WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .ok()
        .flatten();
    if rule.is_none_or(|row| row.get::<_, Uuid>("issue_id") != issue_id) {
        return Err(error(StatusCode::NOT_FOUND, "wakeup not found"));
    }
    let rows = db
        .query(include_str!("wakeup_runs.sql"), &[&wakeup_id, &issue_id])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    Ok(GoJson(
        rows.into_iter()
            .map(|row| {
                json!({
                    "id": row.get::<_, Uuid>("id"),
                    "status": row.get::<_, String>("status"),
                    "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
                    "started_at": date(&row, "started_at"),
                    "completed_at": date(&row, "completed_at"),
                    "checkin_note": row.get::<_, String>("checkin_note"),
                    "triggers": row.get::<_, Vec<String>>("triggers"),
                    "commented": row.get::<_, bool>("commented"),
                })
            })
            .collect(),
    ))
}
