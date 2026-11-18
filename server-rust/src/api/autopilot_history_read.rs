//! Autopilot run and delivery history, matching the frozen Go read contract.
use super::{
    ApiError, ApiState, GoJson,
    catalog::{WorkspaceId, parse_params},
    error, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/autopilots/{id}/runs", get(runs))
        .route("/api/autopilots/{id}/runs/{runId}", get(run))
        .route("/api/autopilots/{id}/deliveries", get(deliveries))
        .route(
            "/api/autopilots/{id}/deliveries/{deliveryId}",
            get(delivery),
        )
}

fn bad_id(raw: &str, message: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, message))
}

async fn load_autopilot(
    state: &ApiState,
    workspace: Uuid,
    raw: &str,
) -> Result<(deadpool_postgres::Client, Uuid), ApiError> {
    let id = bad_id(raw, "invalid autopilot id")?;
    // Frozen loadAutopilotInWorkspace masks DB errors as not-found.
    let missing = || error(StatusCode::NOT_FOUND, "autopilot not found");
    let db = state.pool.get().await.map_err(|_| missing())?;
    db.query_opt(
        "SELECT id FROM autopilot WHERE id=$1 AND workspace_id=$2",
        &[&id, &workspace],
    )
    .await
    .map_err(|_| missing())?
    .ok_or_else(missing)?;
    Ok((db, id))
}

fn pagination(query: Option<&str>) -> (i32, i32) {
    let params = parse_params(query);
    // Go parses a native int before narrowing to int32, including wraparound.
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .map(|v| v as i32)
        .unwrap_or(20)
        .min(100);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .map(|v| v as i32)
        .unwrap_or(0);
    (limit, offset)
}

fn time(row: &Row, column: &str) -> Option<String> {
    row.get::<_, Option<DateTime<Utc>>>(column).map(timestamp)
}

fn run_response(row: &Row, detail: bool) -> Value {
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "autopilot_id":row.get::<_,Uuid>("autopilot_id"),
        "trigger_id":row.get::<_,Option<Uuid>>("trigger_id"), "source":row.get::<_,String>("source"),
        "status":row.get::<_,String>("status"), "issue_id":row.get::<_,Option<Uuid>>("issue_id"),
        "task_id":row.get::<_,Option<Uuid>>("task_id"), "triggered_at":time(row,"triggered_at").unwrap_or_default(),
        "completed_at":time(row,"completed_at"), "failure_reason":row.get::<_,Option<String>>("failure_reason"),
        "trigger_payload":if detail { row.get::<_,Option<Value>>("trigger_payload") } else { None },
        "result":row.get::<_,Option<Value>>("result"), "created_at":time(row,"created_at").unwrap_or_default(),
    });
    if let Some(code) = row.get::<_, Option<String>>("reason_code") {
        value["reason_code"] = json!(code);
    }
    value
}

async fn runs(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let (db, id) = load_autopilot(&state, workspace, &raw).await?;
    let (limit, offset) = pagination(query.as_deref());
    let rows = db
        .query(
            "SELECT * FROM autopilot_run WHERE autopilot_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            &[&id, &(limit as i64), &(offset as i64)],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list runs"))?;
    let runs: Vec<_> = rows.iter().map(|row| run_response(row, false)).collect();
    Ok(GoJson(json!({"total":runs.len(), "runs":runs})))
}

async fn run(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Path((raw, run)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    let (db, id) = load_autopilot(&state, workspace, &raw).await?;
    let run = bad_id(&run, "invalid run id")?;
    let row = db
        .query_opt("SELECT * FROM autopilot_run WHERE id=$1", &[&run])
        .await
        .ok()
        .flatten()
        .filter(|r| r.get::<_, Uuid>("autopilot_id") == id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "run not found"))?;
    Ok(GoJson(run_response(&row, true)))
}

fn delivery_response(row: &Row, detail: bool) -> Value {
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "autopilot_id":row.get::<_,Uuid>("autopilot_id"), "trigger_id":row.get::<_,Uuid>("trigger_id"),
        "provider":row.get::<_,String>("provider"), "event":row.get::<_,String>("event"),
        "dedupe_key":row.get::<_,Option<String>>("dedupe_key"), "dedupe_source":row.get::<_,Option<String>>("dedupe_source"),
        "signature_status":row.get::<_,String>("signature_status"), "status":row.get::<_,String>("status"),
        "attempt_count":row.get::<_,i32>("attempt_count"), "dispatch_attempts":row.get::<_,i32>("dispatch_attempts"),
        "available_at":time(row,"available_at").unwrap_or_default(), "content_type":row.get::<_,Option<String>>("content_type"),
        "response_status":row.get::<_,Option<i32>>("response_status"), "autopilot_run_id":row.get::<_,Option<Uuid>>("autopilot_run_id"),
        "replayed_from_delivery_id":row.get::<_,Option<Uuid>>("replayed_from_delivery_id"), "error":row.get::<_,Option<String>>("error"),
        "reason_code":row.get::<_,Option<String>>("reason_code"), "replay_idempotency_key":row.get::<_,Option<String>>("replay_idempotency_key"),
        "received_at":time(row,"received_at").unwrap_or_default(), "last_attempt_at":time(row,"last_attempt_at").unwrap_or_default(),
        "created_at":time(row,"created_at").unwrap_or_default(),
    });
    if detail {
        if let Some(headers) = row.get::<_, Option<Value>>("selected_headers") {
            value["selected_headers"] = headers;
        }
        if let Some(body) = row
            .get::<_, Option<Vec<u8>>>("raw_body")
            .filter(|b| !b.is_empty())
        {
            value["raw_body"] = json!(String::from_utf8_lossy(&body));
        }
        if let Some(body) = row.get::<_, Option<String>>("response_body") {
            value["response_body"] = json!(body);
        }
    }
    value
}

async fn deliveries(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let (db, id) = load_autopilot(&state, workspace, &raw).await?;
    let (limit, offset) = pagination(query.as_deref());
    // Keep the frozen slim projection: raw body and headers never leave DB on lists.
    // Extra delivery workspace filter rejects malformed cross-tenant autopilot links.
    let rows = db.query(
        "SELECT d.id,d.workspace_id,d.autopilot_id,d.trigger_id,d.provider,d.event,d.dedupe_key,d.dedupe_source,d.signature_status,d.status,d.attempt_count,d.content_type,d.response_status,d.autopilot_run_id,d.replayed_from_delivery_id,d.error,d.received_at,d.last_attempt_at,d.created_at,d.available_at,d.dispatch_attempts,d.reason_code,d.replay_idempotency_key FROM webhook_delivery d JOIN autopilot a ON a.id=d.autopilot_id WHERE d.autopilot_id=$1 AND a.workspace_id=$2 AND d.workspace_id=$2 ORDER BY d.created_at DESC LIMIT $3 OFFSET $4",
        &[&id,&workspace,&(limit as i64),&(offset as i64)],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list deliveries"))?;
    let deliveries: Vec<_> = rows
        .iter()
        .map(|row| delivery_response(row, false))
        .collect();
    Ok(GoJson(
        json!({"total":deliveries.len(), "deliveries":deliveries}),
    ))
}

async fn delivery(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Path((raw, delivery)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    let (db, id) = load_autopilot(&state, workspace, &raw).await?;
    let delivery = bad_id(&delivery, "invalid delivery id")?;
    let row = db
        .query_opt(
            "SELECT * FROM webhook_delivery WHERE id=$1 AND workspace_id=$2",
            &[&delivery, &workspace],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load delivery"))?
        .filter(|r| r.get::<_, Uuid>("autopilot_id") == id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "delivery not found"))?;
    Ok(GoJson(delivery_response(&row, true)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_pagination_first_value_defaults_and_narrowing() {
        for (query, expected) in [
            (None, (20, 0)),
            (Some("limit=0&offset=-1"), (20, 0)),
            (Some("limit=200&offset=3"), (100, 3)),
            (Some("limit=2&limit=9&offset=4&offset=8"), (2, 4)),
            (Some("limit=%2B7&offset=%2B2"), (7, 2)),
            (Some("limit=2147483648&offset=4294967295"), (i32::MIN, -1)),
            (Some("limit=4294967296&offset=4294967296"), (0, 0)),
            (Some("limit=9223372036854775808&offset=invalid"), (20, 0)),
            (Some("limit=+1&offset=+2"), (20, 0)),
        ] {
            assert_eq!(pagination(query), expected, "{query:?}");
        }
    }
}
