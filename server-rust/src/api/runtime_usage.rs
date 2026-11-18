//! Runtime reports require machine-owner consent, never governance-role override.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::parse_params, error};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

const DAILY: &str = include_str!("runtime_usage_daily.sql");
const BY_AGENT: &str = include_str!("runtime_usage_by_agent.sql");
const BY_HOUR: &str = include_str!("runtime_usage_by_hour.sql");
const ACTIVITY: &str = "SELECT EXTRACT(HOUR FROM started_at AT TIME ZONE $2::text)::int AS hour, COUNT(*)::int AS count FROM agent_task_queue WHERE runtime_id=$1 AND started_at IS NOT NULL GROUP BY hour ORDER BY hour";

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/runtimes/{runtime_id}/usage", get(daily))
        .route("/api/runtimes/{runtime_id}/usage/by-agent", get(by_agent))
        .route("/api/runtimes/{runtime_id}/usage/by-hour", get(by_hour))
        .route("/api/runtimes/{runtime_id}/activity", get(activity))
}

async fn read_access(
    db: &deadpool_postgres::Client,
    raw: &str,
    user: Uuid,
    actor: &Actor,
) -> Result<Uuid, ApiError> {
    let runtime =
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_id"))?;
    let not_found = || error(StatusCode::NOT_FOUND, "runtime not found");
    let row = db
        .query_opt(
            "SELECT workspace_id,owner_id,visibility,shared_with FROM agent_runtime WHERE id=$1",
            &[&runtime],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    let workspace: Uuid = row.get("workspace_id");
    if actor.workspace().is_some_and(|bound| bound != workspace) {
        return Err(not_found());
    }
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| not_found())?
        .is_none()
    {
        return Err(not_found());
    }
    let owner: Option<Uuid> = row.get("owner_id");
    let visibility: &str = row.get("visibility");
    let shared: Vec<Option<Uuid>> = row.get("shared_with");
    if owner.is_none()
        || !(owner == Some(user)
            || visibility == "public"
            || (visibility == "private" && shared.contains(&Some(user))))
    {
        return Err(not_found());
    }
    Ok(runtime)
}

async fn viewing_timezone(db: &deadpool_postgres::Client, user: Uuid, raw: Option<&str>) -> String {
    let params = parse_params(raw);
    if let Some(tz) = params
        .get("tz")
        .map(|v| v.trim())
        .filter(|v| super::dashboard_daily::valid_timezone(v))
    {
        return tz.to_owned();
    }
    db.query_opt("SELECT timezone FROM \"user\" WHERE id=$1", &[&user])
        .await
        .ok()
        .flatten()
        .and_then(|row| row.get::<_, Option<String>>(0))
        .map(|tz| tz.trim().to_owned())
        .filter(|tz| super::dashboard_daily::valid_timezone(tz))
        .unwrap_or_else(|| "UTC".into())
}

async fn cutoff(
    db: &deadpool_postgres::Client,
    tz: &str,
    raw: Option<&str>,
    default_days: i32,
) -> Result<DateTime<Utc>, ApiError> {
    let params = parse_params(raw);
    let days = params
        .get("days")
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| (1..=365).contains(v))
        .unwrap_or(default_days);
    // Frozen runtime endpoints all retain N+1 local calendar buckets, including
    // by-agent/by-hour. Do not reuse the dashboard's exact-N aggregate cutoff.
    db.query_one(
        "SELECT (((now() AT TIME ZONE $1::text)::date - $2::int)::timestamp AT TIME ZONE $1::text)",
        &[&tz, &days],
    )
    .await
    .map(|row| row.get(0))
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to determine runtime window",
        )
    })
}

fn tokens(row: &Row) -> Value {
    let mut value = serde_json::Map::new();
    for name in [
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "cost_usd_ticks",
        "uncosted_input_tokens",
        "uncosted_output_tokens",
        "uncosted_cache_read_tokens",
        "uncosted_cache_write_tokens",
    ] {
        value.insert(name.into(), json!(row.get::<_, i64>(name)));
    }
    Value::Object(value)
}

async fn daily(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let runtime = read_access(&db, &raw_id, user, &actor).await?;
    let tz = viewing_timezone(&db, user, raw.as_deref()).await;
    let since = cutoff(&db, &tz, raw.as_deref(), 90).await?;
    let rows = db
        .query(DAILY, &[&runtime, &tz, &since])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list usage"))?;
    Ok(GoJson(json!(
        rows.iter()
            .map(|row| {
                let mut value = tokens(row);
                value["runtime_id"] = json!(runtime);
                value["date"] = json!(row.get::<_, NaiveDate>("date").to_string());
                value["provider"] = json!(row.get::<_, String>("provider"));
                value["model"] = json!(row.get::<_, String>("model"));
                value
            })
            .collect::<Vec<_>>()
    )))
}

async fn by_agent(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let runtime = read_access(&db, &raw_id, user, &actor).await?;
    let tz = viewing_timezone(&db, user, raw.as_deref()).await;
    let since = cutoff(&db, &tz, raw.as_deref(), 30).await?;
    let rows = db.query(BY_AGENT, &[&runtime, &since]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list usage by agent",
        )
    })?;
    Ok(GoJson(json!(
        rows.iter()
            .map(|row| {
                let mut value = tokens(row);
                value["agent_id"] = json!(row.get::<_, Uuid>("agent_id"));
                value["provider"] = json!(row.get::<_, String>("provider"));
                value["model"] = json!(row.get::<_, String>("model"));
                value["task_count"] = json!(row.get::<_, i32>("task_count"));
                value
            })
            .collect::<Vec<_>>()
    )))
}

async fn by_hour(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let runtime = read_access(&db, &raw_id, user, &actor).await?;
    let tz = viewing_timezone(&db, user, raw.as_deref()).await;
    let since = cutoff(&db, &tz, raw.as_deref(), 30).await?;
    let rows = db
        .query(BY_HOUR, &[&runtime, &tz, &since])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to get usage by hour",
            )
        })?;
    Ok(GoJson(json!(
        rows.iter()
            .map(|row| {
                let mut value = tokens(row);
                value["hour"] = json!(row.get::<_, i32>("hour"));
                value["model"] = json!(row.get::<_, String>("model"));
                value["task_count"] = json!(row.get::<_, i32>("task_count"));
                value
            })
            .collect::<Vec<_>>()
    )))
}

async fn activity(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let runtime = read_access(&db, &raw_id, user, &actor).await?;
    let tz = viewing_timezone(&db, user, raw.as_deref()).await;
    let rows = db.query(ACTIVITY, &[&runtime, &tz]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get task activity",
        )
    })?;
    Ok(GoJson(json!(
        rows.iter()
            .map(|row| json!({"hour": row.get::<_,i32>("hour"),"count": row.get::<_,i32>("count")}))
            .collect::<Vec<_>>()
    )))
}
