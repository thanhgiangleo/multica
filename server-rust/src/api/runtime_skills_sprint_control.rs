//! CLI update requests. Registration requires a real daemon heartbeat adapter.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    daemon::{self, DaemonIdentity},
    error, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::de::{DeserializeSeed, MapAccess, Visitor};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::{GenericClient, Row, error::SqlState};
use uuid::Uuid;

/// The real heartbeat must claim pending commands before it sends them to the daemon.
/// A notifier is only a latency hint; the polling heartbeat remains the delivery path.
pub trait RuntimeRpcTransport: Send + Sync {
    fn notify_update_pending(&self, runtime_id: Uuid);
    fn notify_model_list_pending(&self, runtime_id: Uuid);
    fn notify_local_skill_list_pending(&self, runtime_id: Uuid);
    fn notify_local_skill_import_pending(&self, runtime_id: Uuid);
}
pub type SharedRuntimeRpcTransport = Arc<dyn RuntimeRpcTransport>;

pub(super) fn user_routes() -> Router<ApiState> {
    Router::new()
        .merge(super::runtime_skills_sprint_models::user_routes())
        .merge(super::runtime_skills_sprint_local::user_routes())
        .route("/api/runtimes/{runtimeId}/update", post(initiate))
        .route(
            "/api/runtimes/{runtimeId}/update/{updateId}",
            get(get_update),
        )
}

pub(super) fn daemon_routes() -> Router<ApiState> {
    Router::new()
        .merge(super::runtime_skills_sprint_models::daemon_routes())
        .merge(super::runtime_skills_sprint_local::daemon_routes())
        .route(
            "/api/daemon/runtimes/{runtimeId}/update/{updateId}/result",
            post(report),
        )
}

fn failure(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

// Decode the first JSON value, as Go's Decoder.Decode does. Duplicate spelling
// wins in wire order; null never clears a non-pointer string; unknown keys are ignored.
fn string_fields(body: &[u8], fields: &[&str]) -> Result<HashMap<String, String>, ApiError> {
    struct Fields<'a>(&'a [&'a str]);
    impl<'de> DeserializeSeed<'de> for Fields<'_> {
        type Value = HashMap<String, String>;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            decoder: D,
        ) -> Result<Self::Value, D::Error> {
            decoder.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Fields<'_> {
        type Value = HashMap<String, String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("object or null")
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(HashMap::new())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut result = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                if let Some(field) = self.0.iter().find(|field| field.eq_ignore_ascii_case(&key)) {
                    if let Some(text) = map.next_value::<Option<String>>()? {
                        result.insert((*field).to_owned(), text);
                    }
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(result)
        }
    }
    Fields(fields)
        .deserialize(&mut serde_json::Deserializer::from_slice(body))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

pub(super) fn runtime_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_id"))
}

#[derive(Clone)]
pub(super) struct RuntimeAccess {
    pub(super) status: String,
    pub(super) workspace: Uuid,
    pub(super) owner: Option<Uuid>,
    role: String,
    visibility: String,
    shared_with: Vec<Option<Uuid>>,
    daemon: Option<String>,
}

async fn load_runtime(db: &impl GenericClient, id: Uuid) -> Result<RuntimeAccess, ApiError> {
    let row = db
        .query_opt("SELECT workspace_id,owner_id,visibility,shared_with,daemon_id,status FROM agent_runtime WHERE id=$1 FOR SHARE", &[&id])
        .await
        .map_err(|_| failure("failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    Ok(RuntimeAccess {
        status: row.get("status"),
        workspace: row.get("workspace_id"),
        owner: row.get("owner_id"),
        role: String::new(),
        visibility: row.get("visibility"),
        shared_with: row.get("shared_with"),
        daemon: row.get("daemon_id"),
    })
}

pub(super) async fn human_access(
    db: &impl GenericClient,
    runtime: Uuid,
    workspace: Uuid,
    user: Uuid,
) -> Result<RuntimeAccess, ApiError> {
    let mut rt = load_runtime(db, runtime).await?;
    if rt.workspace != workspace {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| failure("failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    rt.role = role.get("role");
    if rt.owner.is_none()
        || (rt.owner != Some(user)
            && rt.visibility != "public"
            && !(rt.visibility == "private" && rt.shared_with.contains(&Some(user))))
    {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    Ok(rt)
}

pub(super) async fn daemon_access(
    db: &impl GenericClient,
    identity: &DaemonIdentity,
    runtime: Uuid,
) -> Result<RuntimeAccess, ApiError> {
    let rt = load_runtime(db, runtime).await?;
    daemon::workspace_access(db, identity, &rt.workspace.to_string()).await?;
    let bound = match identity {
        DaemonIdentity::User(user) => rt.owner == Some(*user),
        DaemonIdentity::Bound { daemon, owner, .. } => {
            !daemon.is_empty()
                && rt.daemon.as_deref() == Some(daemon)
                && (daemon.starts_with("qoder-managed-") || owner.is_some() && *owner == rt.owner)
        }
    };
    if !bound {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    Ok(rt)
}

#[derive(Clone)]
struct Update {
    id: Uuid,
    runtime: Uuid,
    initiator: Uuid,
    status: String,
    target: String,
    output: String,
    error: String,
    created: DateTime<Utc>,
    updated: DateTime<Utc>,
}
impl Update {
    fn from_row(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            runtime: row.get("runtime_id"),
            initiator: row.get("initiator_user_id"),
            status: row.get("status"),
            target: row.get("target_version"),
            output: row.get("output"),
            error: row.get("error"),
            created: row.get("created_at"),
            updated: row.get("updated_at"),
        }
    }
    fn json(&self) -> Value {
        let mut result = json!({
            "id": self.id,
            "runtime_id": self.runtime,
            "status": self.status,
            "target_version": self.target,
            "created_at": timestamp(self.created),
            "updated_at": timestamp(self.updated),
        });
        if !self.output.is_empty() {
            result["output"] = json!(self.output);
        }
        if !self.error.is_empty() {
            result["error"] = json!(self.error);
        }
        result
    }
}

const COLUMNS: &str =
    "id,runtime_id,initiator_user_id,status,target_version,output,error,created_at,updated_at";
const TIMEOUTS: &str = "UPDATE runtime_rpc_update SET status='timeout',error=CASE WHEN status='pending' THEN 'daemon did not respond within 120 seconds' ELSE 'update did not complete within 150 seconds' END,updated_at=now() WHERE runtime_id=$1 AND ((status='pending' AND created_at < now()-interval '120 seconds') OR (status='running' AND started_at < now()-interval '150 seconds'))";

async fn expire(db: &impl GenericClient, runtime: Uuid) -> Result<(), ApiError> {
    db.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load update"))?;
    Ok(())
}

/// Called by the authenticated daemon heartbeat once per addressed runtime. Atomic
/// claim works across API replicas; never call from a browser route or unauthenticated client.
pub async fn claim_pending_update(
    db: &impl GenericClient,
    runtime: Uuid,
) -> Result<Option<(Uuid, String)>, tokio_postgres::Error> {
    db.execute(TIMEOUTS, &[&runtime]).await?;
    let row = db.query_opt(
        "WITH next AS (SELECT id FROM runtime_rpc_update WHERE runtime_id=$1 AND status='pending' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE runtime_rpc_update r SET status='running',started_at=now(),updated_at=now() FROM next WHERE r.id=next.id RETURNING r.id,r.target_version",
        &[&runtime],
    ).await?;
    Ok(row.map(|r| (r.get(0), r.get(1))))
}

async fn initiate(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let id = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to start the update"))?;
    let rt = human_access(&*tx, id, workspace, user).await?;
    if !matches!(rt.role.as_str(), "owner" | "admin") && rt.owner != Some(user) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only runtime owners and workspace admins can update runtimes",
        ));
    }
    let mut request = string_fields(&body, &["target_version"])?;
    let target = request.remove("target_version").unwrap_or_default();
    if target.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "target_version is required"));
    }
    expire(&*tx, id)
        .await
        .map_err(|_| failure("failed to start the update"))?;
    tx.execute(
        "DELETE FROM runtime_rpc_update WHERE created_at < now()-interval '5 minutes'",
        &[],
    )
    .await
    .map_err(|_| failure("failed to start the update"))?;
    let update = Uuid::new_v4();
    let sql = format!(
        "INSERT INTO runtime_rpc_update (id,runtime_id,initiator_user_id,status,target_version,output,error) VALUES ($1,$2,$3,'pending',$4,'','') RETURNING {COLUMNS}"
    );
    let row = tx
        .query_one(&sql, &[&update, &id, &user, &target])
        .await
        .map_err(|e| {
            if e.code() == Some(&SqlState::UNIQUE_VIOLATION)
                && e.as_db_error().and_then(|e| e.constraint())
                    == Some("runtime_rpc_update_active_idx")
            {
                error(
                    StatusCode::CONFLICT,
                    "an update is already in progress for this runtime",
                )
            } else {
                failure("failed to start the update")
            }
        })?;
    tx.commit()
        .await
        .map_err(|_| failure("failed to start the update"))?;
    if let Some(transport) = state.runtime_rpc_transport.as_ref() {
        transport.notify_update_pending(id);
    }
    Ok(GoJson(Update::from_row(&row).json()))
}

async fn get_update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, update_id)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let id = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load update"))?;
    let rt = human_access(&*tx, id, workspace, user).await?;
    let request = Uuid::parse_str(&update_id).ok();
    expire(&*tx, id).await?;
    let sql = format!("SELECT {COLUMNS} FROM runtime_rpc_update WHERE id=$1 AND runtime_id=$2");
    let row = if let Some(request) = request {
        tx.query_opt(&sql, &[&request, &id])
            .await
            .map_err(|_| failure("failed to load update"))?
    } else {
        None
    };
    let update = row
        .as_ref()
        .map(Update::from_row)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "update not found"))?;
    if !matches!(rt.role.as_str(), "owner" | "admin")
        && rt.owner != Some(user)
        && update.initiator != user
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only runtime owners, workspace admins, and the update initiator can view this update",
        ));
    }
    tx.commit()
        .await
        .map_err(|_| failure("failed to load update"))?;
    Ok(GoJson(update.json()))
}

async fn report(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((raw, update_id)): Path<(String, String)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let id = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load runtime"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load update"))?;
    daemon_access(&*tx, &identity, id).await?;
    let update_id = Uuid::parse_str(&update_id).ok();
    expire(&*tx, id).await?;
    let row = if let Some(update_id) = update_id {
        tx.query_opt(
            "SELECT status FROM runtime_rpc_update WHERE id=$1 AND runtime_id=$2 FOR UPDATE",
            &[&update_id, &id],
        )
        .await
        .map_err(|_| failure("failed to load update"))?
    } else {
        None
    };
    let status: String = row
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "update not found"))?
        .get(0);
    if !matches!(status.as_str(), "completed" | "failed" | "timeout") {
        let mut request = string_fields(&body, &["status", "output", "error"])?;
        let reported = request.remove("status").unwrap_or_default();
        match reported.as_str() {
            "completed" => {
                let output =
                    super::runtime_chat_redact::text(&request.remove("output").unwrap_or_default());
                tx.execute("UPDATE runtime_rpc_update SET status='completed',output=$2,updated_at=now() WHERE id=$1", &[&update_id.unwrap(), &output]).await.map_err(|_| failure("failed to persist completion"))?;
            }
            "failed" => {
                let message =
                    super::runtime_chat_redact::text(&request.remove("error").unwrap_or_default());
                tx.execute("UPDATE runtime_rpc_update SET status='failed',error=$2,updated_at=now() WHERE id=$1", &[&update_id.unwrap(), &message]).await.map_err(|_| failure("failed to persist failure"))?;
            }
            "running" => {}
            _ => {
                return Err(error(StatusCode::BAD_REQUEST, "invalid status"));
            }
        }
    }
    tx.commit()
        .await
        .map_err(|_| failure("failed to persist update result"))?;
    Ok(GoJson(json!({"status":"ok"})).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn update_decoder_matches_go_string_field_rules() {
        let fields = string_fields(br#"{"TARGET_VERSION":"a","target_version":null,"Target_Version":"b","ignored":{"deep":[1]}} garbage"#, &["target_version"]).unwrap();
        assert_eq!(fields.get("target_version").map(String::as_str), Some("b"));
        assert!(
            string_fields(b"null", &["target_version"])
                .unwrap()
                .is_empty()
        );
        for malformed in [b"".as_slice(), b"[]", b"1", br#"{"target_version":42}"#] {
            assert!(string_fields(malformed, &["target_version"]).is_err());
        }
        assert!(
            string_fields(br#"{"status":42}"#, &["target_version"])
                .unwrap()
                .is_empty()
        );
    }
}
