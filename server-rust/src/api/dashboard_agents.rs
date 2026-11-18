//! Agent dashboard rollups retain totals while folding restricted live agents.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, agent_activity::allowed_agents,
    catalog::WorkspaceId, dashboard_daily::options, error,
};
use axum::{
    Router,
    extract::{Extension, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

const RESTRICTED: &str = "__restricted_agents__";
const USAGE: &str = include_str!("dashboard_usage_by_agent.sql");
const RUNTIME: &str = include_str!("dashboard_agent_runtime.sql");
const FAILURES: &str = include_str!("dashboard_failures_by_agent.sql");

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/dashboard/usage/by-agent", get(usage))
        .route("/api/dashboard/agent-runtime", get(runtime))
        .route("/api/dashboard/failures/by-agent", get(failures))
}

async fn restricted_agents(
    db: &tokio_postgres::Client,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<HashSet<Uuid>, ApiError> {
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
    let rows = db
        .query("SELECT id,kind FROM agent WHERE workspace_id=$1", &[&ws])
        .await
        .map_err(|_| fail())?;
    // Go skips invocation-target reads for owner/admin and agent actors, but
    // still hides all non-user kinds. Keep this fail-closed distinction.
    if matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin" {
        return Ok(rows
            .iter()
            .filter(|r| r.get::<_, String>("kind") != "user")
            .map(|r| r.get(0))
            .collect());
    }
    let allowed: HashSet<_> = allowed_agents(db, ws, user, actor)
        .await?
        .into_iter()
        .collect();
    // Only existing agents are restricted. Hard-deleted usage IDs must retain
    // their deleted-agent identity; all non-user kinds stay hidden, even to admin.
    Ok(rows
        .iter()
        .map(|r| r.get(0))
        .filter(|id| !allowed.contains(id))
        .collect())
}

// Preserve SQL row order and place each merged bucket at its first hidden row.
// Rows contain only agent_id, string dimensions and integer additive measures.
fn fold_restricted(
    rows: Vec<Value>,
    restricted: &HashSet<Uuid>,
    dimensions: &[&str],
) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    let mut positions: HashMap<Vec<String>, usize> = HashMap::new();
    for mut row in rows {
        let hidden = row["agent_id"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .is_some_and(|id| restricted.contains(&id));
        if !hidden {
            out.push(row);
            continue;
        }
        row["agent_id"] = json!(RESTRICTED);
        let key: Vec<String> = dimensions
            .iter()
            .map(|name| row[*name].as_str().unwrap().to_owned())
            .collect();
        if let Some(&index) = positions.get(&key) {
            for (name, value) in row.as_object().unwrap() {
                if let Some(value) = value.as_i64() {
                    let total = out[index][name].as_i64().unwrap().wrapping_add(value);
                    // Go's task/count fields are int32; preserve its wire arithmetic.
                    out[index][name] = if name.ends_with("count") {
                        json!(total as i32)
                    } else {
                        json!(total)
                    };
                }
            }
        } else {
            positions.insert(key, out.len());
            out.push(row);
        }
    }
    out
}

async fn usage(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list usage by agent",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (_, since, project) = options(&db, user, raw.as_deref(), true).await?;
    let restricted = restricted_agents(&db, ws, user, &actor).await?;
    let rows = db
        .query(USAGE, &[&ws, &since, &project])
        .await
        .map_err(|_| fail())?;
    let rows = rows.iter().map(|row| json!({
        "agent_id": row.get::<_, Uuid>("agent_id"),
        "provider": row.get::<_, String>("provider"), "model": row.get::<_, String>("model"),
        "input_tokens": row.get::<_, i64>("input_tokens"), "output_tokens": row.get::<_, i64>("output_tokens"),
        "cache_read_tokens": row.get::<_, i64>("cache_read_tokens"), "cache_write_tokens": row.get::<_, i64>("cache_write_tokens"),
        "cost_usd_ticks": row.get::<_, i64>("cost_usd_ticks"),
        "uncosted_input_tokens": row.get::<_, i64>("uncosted_input_tokens"), "uncosted_output_tokens": row.get::<_, i64>("uncosted_output_tokens"),
        "uncosted_cache_read_tokens": row.get::<_, i64>("uncosted_cache_read_tokens"), "uncosted_cache_write_tokens": row.get::<_, i64>("uncosted_cache_write_tokens"),
        "task_count": row.get::<_, i32>("task_count"),
    })).collect();
    Ok(GoJson(Value::Array(fold_restricted(
        rows,
        &restricted,
        &["provider", "model"],
    ))))
}

async fn runtime(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list agent runtime",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (_, since, project) = options(&db, user, raw.as_deref(), true).await?;
    let restricted = restricted_agents(&db, ws, user, &actor).await?;
    let rows = db
        .query(RUNTIME, &[&ws, &since, &project])
        .await
        .map_err(|_| fail())?;
    let rows = rows
        .iter()
        .map(|row| {
            json!({
                "agent_id": row.get::<_, Uuid>("agent_id"),
                "total_seconds": row.get::<_, i64>("total_seconds"),
                "task_count": row.get::<_, i32>("task_count"),
                "metered_task_count": row.get::<_, i32>("metered_task_count"),
                "failed_count": row.get::<_, i32>("failed_count"),
                "cancelled_count": row.get::<_, i32>("cancelled_count"),
            })
        })
        .collect();
    Ok(GoJson(Value::Array(fold_restricted(
        rows,
        &restricted,
        &[],
    ))))
}

async fn failures(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list failures by agent",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let (_, since, project) = options(&db, user, raw.as_deref(), true).await?;
    let restricted = restricted_agents(&db, ws, user, &actor).await?;
    let rows = db
        .query(FAILURES, &[&ws, &since, &project])
        .await
        .map_err(|_| fail())?;
    let rows = rows
        .iter()
        .map(|row| {
            json!({
                "agent_id": row.get::<_, Uuid>("agent_id"),
                "failure_reason": row.get::<_, String>("failure_reason"),
                "task_count": row.get::<_, i32>("task_count"),
            })
        })
        .collect();
    Ok(GoJson(Value::Array(fold_restricted(
        rows,
        &restricted,
        &["failure_reason"],
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn folds_by_remaining_dimensions_without_naming_hidden_agents() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let visible = Uuid::new_v4();
        let rows = vec![
            json!({"agent_id":a,"provider":"p","model":"m","task_count":2,"input_tokens":5}),
            json!({"agent_id":visible,"provider":"p","model":"m","task_count":1,"input_tokens":7}),
            json!({"agent_id":b,"provider":"p","model":"m","task_count":3,"input_tokens":11}),
            json!({"agent_id":b,"provider":"q","model":"m","task_count":4,"input_tokens":13}),
        ];
        let out = fold_restricted(rows, &HashSet::from([a, b]), &["provider", "model"]);
        assert_eq!(
            out,
            vec![
                json!({"agent_id":RESTRICTED,"provider":"p","model":"m","task_count":5,"input_tokens":16}),
                json!({"agent_id":visible,"provider":"p","model":"m","task_count":1,"input_tokens":7}),
                json!({"agent_id":RESTRICTED,"provider":"q","model":"m","task_count":4,"input_tokens":13}),
            ]
        );
    }
}
