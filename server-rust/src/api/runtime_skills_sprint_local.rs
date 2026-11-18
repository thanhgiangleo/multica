//! Local capability inventory. Enable only with real authenticated heartbeat delivery.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    daemon::DaemonIdentity,
    error,
    runtime_skills_sprint_control::{daemon_access, human_access, runtime_id},
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn user_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/runtimes/{runtimeId}/local-skills", post(initiate))
        .route(
            "/api/runtimes/{runtimeId}/local-skills/{requestId}",
            get(poll),
        )
}
pub(super) fn daemon_routes() -> Router<ApiState> {
    Router::new().route(
        "/api/daemon/runtimes/{runtimeId}/local-skills/{requestId}/result",
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
fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct Skill {
    #[serde(deserialize_with = "null_default")]
    key: String,
    #[serde(deserialize_with = "null_default")]
    name: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    description: String,
    #[serde(deserialize_with = "null_default")]
    source_path: String,
    #[serde(deserialize_with = "null_default")]
    provider: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    root: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    plugin: String,
    #[serde(deserialize_with = "null_default", skip_serializing_if = "is_false")]
    can_disable: bool,
    #[serde(deserialize_with = "null_default")]
    file_count: i64,
}
// Deliberately exclude commands, arguments, URLs, environment and headers.
#[derive(Default, Deserialize, Serialize)]
#[serde(default)]
struct McpServer {
    #[serde(deserialize_with = "null_default")]
    name: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    transport: String,
    #[serde(
        deserialize_with = "null_default",
        skip_serializing_if = "String::is_empty"
    )]
    source: String,
    #[serde(deserialize_with = "null_default")]
    enabled: bool,
}
#[derive(Default)]
struct Report {
    status: String,
    skills: Vec<Skill>,
    supported: Option<bool>,
    mcp_servers: Vec<McpServer>,
    mcp_supported: Option<bool>,
    error: String,
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
                    "skills" => {
                        r.skills = map.next_value::<Option<Vec<Skill>>>()?.unwrap_or_default()
                    }
                    "supported" => r.supported = map.next_value()?,
                    "mcp_servers" => {
                        r.mcp_servers = map
                            .next_value::<Option<Vec<McpServer>>>()?
                            .unwrap_or_default()
                    }
                    "mcp_supported" => r.mcp_supported = map.next_value()?,
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
fn scrub(value: &mut Value) {
    match value {
        Value::String(text) => *text = super::runtime_chat_redact::text(text),
        Value::Array(values) => {
            for v in values {
                scrub(v);
            }
        }
        Value::Object(values) => {
            for v in values.values_mut() {
                scrub(v);
            }
        }
        _ => {}
    }
}
const COLUMNS: &str =
    "id,runtime_id,status,skills,supported,mcp_servers,mcp_supported,error,created_at,updated_at";
const TIMEOUTS: &str = "UPDATE runtime_rpc_local_skill_list SET status='timeout',error=CASE WHEN status='pending' THEN 'daemon did not respond within 3 minutes' ELSE 'daemon did not finish within 60 seconds' END,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE runtime_id=$1 AND expires_at>now() AND ((status='pending' AND created_at < now()-interval '3 minutes') OR (status='running' AND started_at < now()-interval '60 seconds'))";
fn projection(row: &Row) -> Value {
    let mut value = json!({
        "id": row.get::<_, Uuid>("id"), "runtime_id": row.get::<_, Uuid>("runtime_id"),
        "status": row.get::<_, String>("status"), "supported": row.get::<_, bool>("supported"),
        "mcp_supported": row.get::<_, bool>("mcp_supported"),
        "created_at": timestamp(row.get("created_at")), "updated_at": timestamp(row.get("updated_at")),
    });
    for key in ["skills", "mcp_servers"] {
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
async fn expire(db: &impl GenericClient, runtime: Uuid) -> Result<(), ApiError> {
    db.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(())
}

/// Heartbeat-only primitive. Caller must authenticate/bind the daemon and verify
/// runtime ownership before calling, then deliver the returned ID in pending_local_skills.
/// Never invoke as a speculative probe: this durably starts the running timeout.
pub async fn claim_pending_local_skill_list(
    db: &impl GenericClient,
    runtime: Uuid,
) -> Result<Option<Uuid>, tokio_postgres::Error> {
    db.execute(TIMEOUTS, &[&runtime]).await?;
    let row = db.query_opt(
        "WITH next AS (SELECT id FROM runtime_rpc_local_skill_list WHERE runtime_id=$1 AND status='pending' AND expires_at>now() ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE runtime_rpc_local_skill_list r SET status='running',started_at=now(),updated_at=now(),expires_at=now()+interval '5 minutes' FROM next WHERE r.id=next.id RETURNING r.id", &[&runtime]
    ).await?;
    Ok(row.map(|r| r.get(0)))
}
/// Read-only heartbeat hint. False means no unexpired pending work; true can race a claim.
pub async fn has_pending_local_skill_list(
    db: &impl GenericClient,
    runtime: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    Ok(db.query_one("SELECT EXISTS(SELECT 1 FROM runtime_rpc_local_skill_list WHERE runtime_id=$1 AND status='pending' AND expires_at>now() AND created_at>=now()-interval '3 minutes')", &[&runtime]).await?.get(0))
}
async fn initiate(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to enqueue local skills request"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to enqueue local skills request"))?;
    let rt = human_access(&*tx, runtime, workspace, user).await?;
    if rt.status != "online" {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "runtime is offline"));
    }
    expire(&*tx, runtime).await?;
    tx.execute(
        "DELETE FROM runtime_rpc_local_skill_list WHERE runtime_id=$1 AND expires_at<=now()",
        &[&runtime],
    )
    .await
    .map_err(|_| failure("failed to enqueue local skills request"))?;
    let sql = format!(
        "INSERT INTO runtime_rpc_local_skill_list(id,runtime_id) VALUES($1,$2) RETURNING {COLUMNS}"
    );
    let row = tx
        .query_one(&sql, &[&Uuid::new_v4(), &runtime])
        .await
        .map_err(|_| failure("failed to enqueue local skills request"))?;
    let value = projection(&row);
    tx.commit()
        .await
        .map_err(|_| failure("failed to enqueue local skills request"))?;
    drop(db);
    state
        .runtime_rpc_transport
        .as_ref()
        .expect("gated routes")
        .notify_local_skill_list_pending(runtime);
    Ok(GoJson(value))
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
        .map_err(|_| failure("failed to load request"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    human_access(&*tx, runtime, workspace, user).await?;
    let request =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    expire(&*tx, runtime).await?;
    let sql = format!(
        "SELECT {COLUMNS} FROM runtime_rpc_local_skill_list WHERE runtime_id=$1 AND id=$2 AND expires_at>now()"
    );
    let row = tx
        .query_opt(&sql, &[&runtime, &request])
        .await
        .map_err(|_| failure("failed to load request"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    let value = projection(&row);
    tx.commit()
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(GoJson(value))
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
        .map_err(|_| failure("failed to load request"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    daemon_access(&*tx, &identity, runtime).await?;
    let request =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    expire(&*tx, runtime).await?;
    let row = tx.query_opt("SELECT status FROM runtime_rpc_local_skill_list WHERE runtime_id=$1 AND id=$2 AND expires_at>now() FOR UPDATE", &[&runtime,&request]).await.map_err(|_| failure("failed to load request"))?.ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    let status: String = row.get(0);
    if status != "pending" && status != "running" {
        tx.commit()
            .await
            .map_err(|_| failure("failed to load request"))?;
        return Ok(GoJson(json!({"status":"ok"})));
    }
    let report = decode(&body)?;
    if report.status == "completed" {
        let mut skills = serde_json::to_value(report.skills).expect("serializable inventory");
        let mut servers = serde_json::to_value(report.mcp_servers).expect("serializable inventory");
        scrub(&mut skills);
        scrub(&mut servers);
        let supported = report.supported.unwrap_or(true);
        let mcp_supported = report.mcp_supported.unwrap_or(false);
        tx.execute("UPDATE runtime_rpc_local_skill_list SET status='completed',skills=$3,supported=$4,mcp_servers=$5,mcp_supported=$6,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE runtime_id=$1 AND id=$2", &[&runtime,&request,&skills,&supported,&servers,&mcp_supported]).await.map_err(|_| failure("failed to persist completion"))?;
    } else {
        let message = super::runtime_chat_redact::text(&report.error);
        tx.execute("UPDATE runtime_rpc_local_skill_list SET status='failed',error=$3,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE runtime_id=$1 AND id=$2", &[&runtime,&request,&message]).await.map_err(|_| failure("failed to persist failure"))?;
    }
    tx.commit()
        .await
        .map_err(|_| failure("failed to persist result"))?;
    Ok(GoJson(json!({"status":"ok"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_inventory_decode_defaults_null_and_wire_order() {
        let r = decode(br#"{"STATUS":"completed","status":null,"supported":false,"skills":[{"key":"s","description":null,"file_count":-1}],"mcp_servers":[{"name":"m","enabled":true}]} trailing"#).unwrap();
        assert_eq!(r.status, "completed");
        assert_eq!(r.supported, Some(false));
        assert_eq!(r.skills[0].file_count, -1);
        let s = serde_json::to_value(&r.skills[0]).unwrap();
        assert_eq!(s["source_path"], "");
        assert!(s.get("description").is_none());
        assert!(s.get("can_disable").is_none());
        assert!(decode(b"null").is_ok());
        assert!(decode(b"[]").is_err());
        assert!(decode(br#"{"supported":1}"#).is_err());
        assert!(decode(br#"{"skills":[{"file_count":1.5}]}"#).is_err());
    }
    #[test]
    fn local_inventory_projection_never_keeps_unknown_secret_fields() {
        let r = decode(br#"{"status":"completed","mcp_servers":[{"name":"TOKEN=machine-secret","enabled":true,"command":"secret-command","url":"secret-url","headers":{"authorization":"secret"},"env":{"SECRET":"secret"}}],"skills":[{"name":"token=skill-secret","content":"secret-body","files":["secret-file"]}]}"#).unwrap();
        let mut v = json!({"skills":r.skills,"mcp_servers":r.mcp_servers});
        scrub(&mut v);
        let text = v.to_string();
        for secret in [
            "secret-command",
            "secret-url",
            "authorization",
            "SECRET",
            "secret-body",
            "secret-file",
            "machine-secret",
            "skill-secret",
        ] {
            assert!(!text.contains(secret), "{text}");
        }
        assert_eq!(v["mcp_servers"][0]["enabled"], true);
    }
}
