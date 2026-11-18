//! Model discovery RPC. Real daemon heartbeat delivery is required before registration.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::{WorkspaceId, parse_params},
    daemon::DaemonIdentity,
    error,
    runtime_skills_sprint_control::{daemon_access, human_access, runtime_id},
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Row, Transaction};
use uuid::Uuid;

pub(super) fn user_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/runtimes/{runtimeId}/models", post(initiate))
        .route("/api/runtimes/{runtimeId}/models/{requestId}", get(poll))
}
pub(super) fn daemon_routes() -> Router<ApiState> {
    Router::new().route(
        "/api/daemon/runtimes/{runtimeId}/models/{requestId}/result",
        post(report),
    )
}
fn failure(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct Model {
    #[serde(deserialize_with = "null_default")]
    id: String,
    #[serde(deserialize_with = "null_default")]
    label: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    provider: String,
    #[serde(deserialize_with = "null_default", skip_serializing_if = "is_false")]
    default: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<Thinking>,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    service_tiers: Vec<ServiceTier>,
    #[serde(deserialize_with = "null_default", skip_serializing_if = "is_false")]
    supports_explicit_standard_service_tier: bool,
}
fn is_false(value: &bool) -> bool {
    !value
}
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct Thinking {
    supported_levels: Option<Vec<ThinkingLevel>>,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    default_level: String,
}
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct ThinkingLevel {
    #[serde(deserialize_with = "null_default")]
    value: String,
    #[serde(deserialize_with = "null_default")]
    label: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    description: String,
}
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct ServiceTier {
    #[serde(deserialize_with = "null_default")]
    id: String,
    #[serde(deserialize_with = "null_default")]
    name: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    description: String,
}
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct Unavailable {
    #[serde(deserialize_with = "null_default")]
    id: String,
    #[serde(deserialize_with = "null_default")]
    label: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    reason: String,
}
#[derive(Default)]
struct Report {
    status: String,
    models: Vec<Model>,
    unavailable: Vec<Unavailable>,
    supported: Option<bool>,
    error: String,
    fallback: bool,
}
fn decode(body: &[u8]) -> Result<Report, ApiError> {
    struct Seed;
    impl<'de> DeserializeSeed<'de> for Seed {
        type Value = Report;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Report, D::Error> {
            d.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Seed {
        type Value = Report;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("object or null")
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Report, E> {
            Ok(Report::default())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Report, M::Error> {
            let mut r = Report::default();
            while let Some(key) = map.next_key::<String>()? {
                match key.to_ascii_lowercase().as_str() {
                    "status" => {
                        if let Some(v) = map.next_value::<Option<String>>()? {
                            r.status = v;
                        }
                    }
                    "error" => {
                        if let Some(v) = map.next_value::<Option<String>>()? {
                            r.error = v;
                        }
                    }
                    "fallback" => {
                        if let Some(v) = map.next_value::<Option<bool>>()? {
                            r.fallback = v;
                        }
                    }
                    "supported" => r.supported = map.next_value()?,
                    "models" => {
                        r.models = map.next_value::<Option<Vec<Model>>>()?.unwrap_or_default()
                    }
                    "unavailable_models" => {
                        r.unavailable = map
                            .next_value::<Option<Vec<Unavailable>>>()?
                            .unwrap_or_default()
                    }
                    _ => {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
            }
            Ok(r)
        }
    }
    Seed.deserialize(&mut serde_json::Deserializer::from_slice(body))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}
// Sanitize every daemon-supplied string, including nested advisory display copy.
fn scrub(value: &mut Value) {
    match value {
        Value::String(text) => *text = super::runtime_chat_redact::text(text),
        Value::Array(values) => {
            for value in values {
                scrub(value);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                scrub(value);
            }
        }
        _ => {}
    }
}
const COLUMNS: &str =
    "id,runtime_id,status,models,unavailable_models,supported,error,created_at,updated_at";
const TIMEOUTS: &str = "UPDATE runtime_rpc_model_list SET status='timeout',error=CASE WHEN status='pending' THEN 'daemon did not respond within 30 seconds' ELSE 'daemon did not finish within 60 seconds' END,updated_at=now() WHERE runtime_id=$1 AND ((status='pending' AND created_at < now()-interval '30 seconds') OR (status='running' AND started_at < now()-interval '60 seconds'))";
fn projection(row: &Row) -> Value {
    let mut value = json!({
        "id": row.get::<_, Uuid>("id"), "runtime_id": row.get::<_, Uuid>("runtime_id"),
        "status": row.get::<_, String>("status"), "supported": row.get::<_, bool>("supported"),
        "created_at": timestamp(row.get("created_at")), "updated_at": timestamp(row.get("updated_at")),
    });
    for key in ["models", "unavailable_models"] {
        if let Some(v) = row.get::<_, Option<Value>>(key)
            && v.as_array().is_some_and(|v| !v.is_empty())
        {
            value[key] = v;
        }
    }
    let message: String = row.get("error");
    if !message.is_empty() {
        value["error"] = json!(message);
    }
    value
}
async fn enqueue(db: &impl GenericClient, runtime: Uuid) -> Result<Value, ApiError> {
    db.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to enqueue model list request"))?;
    db.execute(
        "DELETE FROM runtime_rpc_model_list WHERE created_at < now()-interval '2 minutes'",
        &[],
    )
    .await
    .map_err(|_| failure("failed to enqueue model list request"))?;
    let sql = format!(
        "INSERT INTO runtime_rpc_model_list(id,runtime_id,status) VALUES($1,$2,'pending') RETURNING {COLUMNS}"
    );
    let row = db
        .query_one(&sql, &[&Uuid::new_v4(), &runtime])
        .await
        .map_err(|_| failure("failed to enqueue model list request"))?;
    Ok(projection(&row))
}
/// Caller must already authorize the daemon against this runtime. Atomic across API nodes.
pub async fn claim_pending_model_list(
    db: &impl GenericClient,
    runtime: Uuid,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    db.execute(TIMEOUTS, &[&runtime]).await?;
    let row = db.query_opt("WITH next AS (SELECT id FROM runtime_rpc_model_list WHERE runtime_id=$1 AND status='pending' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE runtime_rpc_model_list r SET status='running',started_at=now(),updated_at=now() FROM next WHERE r.id=next.id RETURNING r.id", &[&runtime]).await?;
    Ok(row.map(|r| r.get(0)))
}
async fn cached(tx: &Transaction<'_>, runtime: Uuid) -> Result<Option<Row>, ApiError> {
    tx.batch_execute("SAVEPOINT model_cache_read")
        .await
        .map_err(|_| failure("failed to load request"))?;
    let result = tx.query_opt("SELECT models,unavailable_models,supported,stored_at,stored_at <= now()-interval '60 seconds' AS stale FROM runtime_rpc_model_catalog WHERE runtime_id=$1 AND stored_at > now()-interval '24 hours' AND supported=true AND jsonb_array_length(models)>0", &[&runtime]).await;
    if result.is_err() {
        tx.batch_execute("ROLLBACK TO SAVEPOINT model_cache_read")
            .await
            .map_err(|_| failure("failed to load request"))?;
    }
    tx.batch_execute("RELEASE SAVEPOINT model_cache_read")
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(result.ok().flatten())
}
async fn revalidate(tx: &Transaction<'_>, runtime: Uuid) -> Result<bool, ApiError> {
    tx.batch_execute("SAVEPOINT model_revalidate")
        .await
        .map_err(|_| failure("failed to load request"))?;
    let result = async {
        tx.execute(TIMEOUTS, &[&runtime]).await?;
        tx.execute("DELETE FROM runtime_rpc_model_list WHERE created_at < now()-interval '2 minutes'", &[]).await?;
        tx.execute("INSERT INTO runtime_rpc_model_list(id,runtime_id,status) SELECT $1,$2,'pending' WHERE NOT EXISTS(SELECT 1 FROM runtime_rpc_model_list WHERE runtime_id=$2 AND status='pending')", &[&Uuid::new_v4(), &runtime]).await
    }.await;
    if result.is_err() {
        tx.batch_execute("ROLLBACK TO SAVEPOINT model_revalidate")
            .await
            .map_err(|_| failure("failed to load request"))?;
    }
    tx.batch_execute("RELEASE SAVEPOINT model_revalidate")
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(result.is_ok_and(|count| count > 0))
}
async fn initiate(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let rt = human_access(&*tx, runtime, workspace, user).await?;
    if rt.status != "online" {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "runtime is offline"));
    }
    let mut notify = false;
    let cache = if parse_params(query.as_deref())
        .get("force")
        .map(String::as_str)
        != Some("true")
    {
        cached(&tx, runtime).await?
    } else {
        None
    };
    let result = if let Some(row) = cache {
        if row.get::<_, bool>("stale") {
            notify = revalidate(&tx, runtime).await?;
        }
        let at: DateTime<Utc> = row.get("stored_at");
        let mut value = json!({"id":Uuid::new_v4(),"runtime_id":runtime,"status":"completed","supported":true,"models":row.get::<_, Value>("models"),"created_at":timestamp(at),"updated_at":timestamp(at),"cached":true,"cached_at":timestamp(at)});
        let unavailable: Value = row.get("unavailable_models");
        if unavailable.as_array().is_some_and(|v| !v.is_empty()) {
            value["unavailable_models"] = unavailable;
        }
        value
    } else {
        notify = true;
        enqueue(&*tx, runtime).await?
    };
    tx.commit()
        .await
        .map_err(|_| failure("failed to enqueue model list request"))?;
    if notify && let Some(t) = &state.runtime_rpc_transport {
        t.notify_model_list_pending(runtime);
    }
    Ok(GoJson(result))
}
async fn poll(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, request)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    human_access(&*tx, runtime, workspace, user).await?;
    tx.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load request"))?;
    let request =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    let sql = format!("SELECT {COLUMNS} FROM runtime_rpc_model_list WHERE id=$1 AND runtime_id=$2");
    let row = tx
        .query_opt(&sql, &[&request, &runtime])
        .await
        .map_err(|_| failure("failed to load request"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    tx.commit()
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(GoJson(projection(&row)))
}
async fn report(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((raw, request)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    daemon_access(&*tx, &identity, runtime).await?;
    tx.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load request"))?;
    let request =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    let row = tx
        .query_opt(
            "SELECT status FROM runtime_rpc_model_list WHERE id=$1 AND runtime_id=$2 FOR UPDATE",
            &[&request, &runtime],
        )
        .await
        .map_err(|_| failure("failed to load request"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    let status: String = row.get(0);
    if !matches!(status.as_str(), "completed" | "failed" | "timeout") {
        let report = decode(&body)?;
        if report.status == "completed" {
            let supported = report.supported.unwrap_or(true);
            let mut models = json!(report.models);
            let mut unavailable = json!(report.unavailable);
            scrub(&mut models);
            scrub(&mut unavailable);
            tx.execute("UPDATE runtime_rpc_model_list SET status='completed',models=$2,unavailable_models=$3,supported=$4,updated_at=now() WHERE id=$1", &[&request,&models,&unavailable,&supported]).await.map_err(|_| failure("failed to persist completion"))?;
            if !report.fallback {
                tx.batch_execute("SAVEPOINT model_cache_write")
                    .await
                    .map_err(|_| failure("failed to persist completion"))?;
                let cache_result = if supported && models.as_array().is_some_and(|v| !v.is_empty())
                {
                    tx.execute("INSERT INTO runtime_rpc_model_catalog(runtime_id,models,unavailable_models,supported,stored_at) VALUES($1,$2,$3,$4,now()) ON CONFLICT(runtime_id) DO UPDATE SET models=EXCLUDED.models,unavailable_models=EXCLUDED.unavailable_models,supported=EXCLUDED.supported,stored_at=EXCLUDED.stored_at", &[&runtime,&models,&unavailable,&supported]).await
                } else {
                    tx.execute(
                        "DELETE FROM runtime_rpc_model_catalog WHERE runtime_id=$1",
                        &[&runtime],
                    )
                    .await
                };
                if cache_result.is_err() {
                    tx.batch_execute("ROLLBACK TO SAVEPOINT model_cache_write")
                        .await
                        .map_err(|_| failure("failed to persist completion"))?;
                }
                tx.batch_execute("RELEASE SAVEPOINT model_cache_write")
                    .await
                    .map_err(|_| failure("failed to persist completion"))?;
            }
        } else {
            let message = super::runtime_chat_redact::text(&report.error);
            tx.execute("UPDATE runtime_rpc_model_list SET status='failed',error=$2,updated_at=now() WHERE id=$1", &[&request,&message]).await.map_err(|_| failure("failed to persist failure"))?;
        }
    }
    tx.commit()
        .await
        .map_err(|_| failure("failed to persist model result"))?;
    Ok(GoJson(json!({"status":"ok"})))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_report_wire_projection_and_decoder() {
        let report = decode(br#"{"STATUS":"completed","supported":null,"models":[{"id":"m","label":"Model","default":true,"thinking":{"supported_levels":[{"value":"high","label":"High"}]},"unknown":"secret"}],"unavailable_models":[{"id":"later","reason":"upgrade"}]} trailing"#).unwrap();
        assert_eq!(report.status, "completed");
        assert!(report.supported.is_none());
        let models = json!(report.models);
        assert_eq!(models[0]["default"], true);
        assert!(models[0].get("unknown").is_none());
        assert_eq!(
            models[0]["thinking"]["supported_levels"][0]["value"],
            "high"
        );
        assert_eq!(report.unavailable[0].id, "later");
        assert!(decode(br#"{"models":[{"id":5}]}"#).is_err());
        assert!(decode(br#"{"supported":"yes"}"#).is_err());
        assert!(decode(b"[]").is_err());
        assert_eq!(decode(b"null").unwrap().status, "");
    }
}
