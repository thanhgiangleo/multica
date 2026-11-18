//! Workspace-level daily aggregates from the frozen staged Go dashboard queries.
use super::{
    ApiError, ApiState, GoJson, Identity,
    catalog::{WorkspaceId, parse_params},
    error,
};
use axum::{
    Router,
    extract::{Extension, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use std::path::Path;
use uuid::Uuid;

const USAGE: &str = include_str!("dashboard_usage_daily.sql");
const FAILURES: &str = include_str!("dashboard_failures_daily.sql");
const RUNTIME: &str = include_str!("dashboard_runtime_daily.sql");

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/dashboard/usage/daily", get(usage))
        .route("/api/dashboard/failures/daily", get(failures))
        .route("/api/dashboard/runtime/daily", get(runtime))
        .merge(super::dashboard_agents::routes())
}

pub(super) fn valid_timezone(name: &str) -> bool {
    if name.is_empty() || name.starts_with('/') || name.contains('\0') {
        return false;
    }
    let root = Path::new("/usr/share/zoneinfo");
    let Ok(path) = root.join(name).canonicalize() else {
        return false;
    };
    path.starts_with(root) && path.is_file()
}

pub(super) async fn options(
    db: &deadpool_postgres::Client,
    user: Uuid,
    raw: Option<&str>,
    exact: bool,
) -> Result<(String, DateTime<Utc>, Option<Uuid>), ApiError> {
    let params = parse_params(raw);
    let project = match params.get("project_id").map(String::as_str).unwrap_or("") {
        "" => None,
        raw => Some(
            Uuid::parse_str(raw)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid project_id"))?,
        ),
    };
    let tz = if let Some(tz) = params
        .get("tz")
        .map(|s| s.trim())
        .filter(|s| valid_timezone(s))
    {
        tz.to_owned()
    } else {
        let stored = db
            .query_opt("SELECT timezone FROM \"user\" WHERE id=$1", &[&user])
            .await
            .ok()
            .flatten()
            .and_then(|r| r.get::<_, Option<String>>(0));
        stored
            .map(|tz| tz.trim().to_owned())
            .filter(|tz| valid_timezone(tz))
            .unwrap_or_else(|| "UTC".into())
    };
    let days = params
        .get("days")
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|v| (1..=365).contains(v))
        .unwrap_or(30)
        - i32::from(exact);
    // Date-bucketed reads retain N+1 days; agent rollups have no date for
    // client-side trimming and must use exactly N local calendar days.
    // PostgreSQL resolves the local calendar midnight including DST boundaries;
    // never truncate an already UTC-converted timestamp in the server timezone.
    let since = db
        .query_one(
            "SELECT ((((now() AT TIME ZONE $1::text)::date - $2::int)::timestamp AT TIME ZONE $1::text))",
            &[&tz, &days],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to determine dashboard window",
            )
        })?
        .get(0);
    Ok((tz, since, project))
}

async fn usage(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list usage");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (tz, since, project) = options(&db, user, raw.as_deref(), false).await?;
    let rows = db
        .query(USAGE, &[&ws, &tz, &since, &project])
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(Value::Array(rows.iter().map(|row| json!({
        "date": row.get::<_, NaiveDate>("date").to_string(),
        "provider": row.get::<_, String>("provider"), "model": row.get::<_, String>("model"),
        "input_tokens": row.get::<_, i64>("input_tokens"), "output_tokens": row.get::<_, i64>("output_tokens"),
        "cache_read_tokens": row.get::<_, i64>("cache_read_tokens"), "cache_write_tokens": row.get::<_, i64>("cache_write_tokens"),
        "cost_usd_ticks": row.get::<_, i64>("cost_usd_ticks"),
        "uncosted_input_tokens": row.get::<_, i64>("uncosted_input_tokens"), "uncosted_output_tokens": row.get::<_, i64>("uncosted_output_tokens"),
        "uncosted_cache_read_tokens": row.get::<_, i64>("uncosted_cache_read_tokens"), "uncosted_cache_write_tokens": row.get::<_, i64>("uncosted_cache_write_tokens"),
        "task_count": row.get::<_, i32>("task_count"),
    })).collect())))
}

async fn failures(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list daily failures",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (tz, since, project) = options(&db, user, raw.as_deref(), false).await?;
    let rows = db
        .query(FAILURES, &[&ws, &tz, &since, &project])
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "date": row.get::<_, NaiveDate>("date").to_string(),
                    "failure_reason": row.get::<_, String>("failure_reason"),
                    "task_count": row.get::<_, i32>("task_count"),
                })
            })
            .collect(),
    )))
}

async fn runtime(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list daily runtime",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (tz, since, project) = options(&db, user, raw.as_deref(), false).await?;
    let rows = db
        .query(RUNTIME, &[&ws, &tz, &since, &project])
        .await
        .map_err(|_| fail())?;
    Ok(GoJson(Value::Array(
        rows.iter()
            .map(|row| {
                json!({
                    "date": row.get::<_, NaiveDate>("date").to_string(),
                    "total_seconds": row.get::<_, i64>("total_seconds"),
                    "task_count": row.get::<_, i32>("task_count"),
                    "failed_count": row.get::<_, i32>("failed_count"),
                    "cancelled_count": row.get::<_, i32>("cancelled_count"),
                })
            })
            .collect(),
    )))
}

#[cfg(test)]
mod tests {
    use super::valid_timezone;
    #[test]
    fn timezone_does_not_escape_zoneinfo() {
        assert!(valid_timezone("UTC"));
        assert!(valid_timezone("Asia/Ho_Chi_Minh"));
        assert!(!valid_timezone("../../etc/passwd"));
        assert!(!valid_timezone("nonsense"));
    }
}
