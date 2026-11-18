//! Bearer-only P0 daemon registration and HTTP heartbeat. Merge `routes()` into
//! `daemon::routes` before its authentication layer; no browser or task-token path.
use super::{
    ApiError, ApiState, GoJson, daemon::DaemonIdentity, error, json, runtime::runtime_row,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/daemon/register", post(register))
        .route("/api/daemon/heartbeat", post(heartbeat))
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Registration {
    workspace_id: String,
    daemon_id: String,
    device_name: String,
    cli_version: String,
    launched_by: String,
    legacy_daemon_ids: Vec<String>,
    runtimes: Vec<RuntimeRegistration>,
    failed_profiles: Vec<FailedProfile>,
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct RuntimeRegistration {
    name: String,
    runtime_mode: String,
    #[serde(rename = "type")]
    kind: String,
    version: String,
    status: String,
    profile_id: String,
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct FailedProfile {
    profile_id: String,
    command_name: String,
    reason: String,
}

fn decode<T: serde::de::DeserializeOwned + Default>(bytes: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice::<Option<T>>(bytes)
        .map(Option::unwrap_or_default)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}
fn parse_id(raw: &str, field: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            match field {
                "workspace_id" => "invalid workspace_id",
                "runtime_id" => "invalid runtime_id",
                "profile_id" => "invalid profile_id",
                _ => "invalid id",
            },
        )
    })
}
fn registration_request(bytes: &[u8]) -> Result<(Registration, Uuid), ApiError> {
    let mut req: Registration = decode(bytes)?;
    req.workspace_id = req.workspace_id.trim().to_owned();
    req.daemon_id = req.daemon_id.trim().to_owned();
    req.device_name = req.device_name.trim().to_owned();
    if req.daemon_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "daemon_id is required"));
    }
    if req.workspace_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "workspace_id is required"));
    }
    if req.runtimes.is_empty() && req.failed_profiles.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "at least one runtime or failed profile is required",
        ));
    }
    for rt in &mut req.runtimes {
        if rt.runtime_mode.is_empty() {
            rt.runtime_mode = "local".into();
        }
        if !matches!(rt.runtime_mode.as_str(), "local" | "cloud")
            || (!rt.profile_id.is_empty() && rt.runtime_mode != "local")
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid runtime_mode for registration",
            ));
        }
    }
    let ws = parse_id(&req.workspace_id, "workspace_id")?;
    req.workspace_id = ws.to_string();
    Ok((req, ws))
}
fn capabilities(headers: &HeaderMap) -> Option<Vec<String>> {
    let parts: Vec<_> = headers
        .get("x-client-capabilities")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (!parts.is_empty()).then_some(parts)
}
const OWNED: &str = "this daemon's runtimes are registered to another user; remove them from the workspace or sign the daemon in as their owner";
fn register_failure() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to register runtime",
    )
}

async fn qoder_allowed(
    db: &impl GenericClient,
    ws: Uuid,
    daemon: &str,
    hash: &str,
) -> Result<bool, ApiError> {
    db.query_one("SELECT EXISTS(SELECT 1 FROM cloud_agent_connection c JOIN daemon_token d ON d.id=c.token_id JOIN member m ON m.workspace_id=c.workspace_id AND m.user_id=c.owner_id WHERE c.provider='qoder_cloud' AND c.workspace_id=$1 AND c.enabled AND d.daemon_id=$2 AND d.token_hash=$3 AND d.expires_at>now() AND m.role IN ('owner','admin'))", &[&ws, &daemon, &hash]).await
        .map(|r| r.get(0)).map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "runtime authorization unavailable"))
}
async fn member_role(db: &impl GenericClient, ws: Uuid, user: Uuid) -> Result<String, ApiError> {
    db.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
    .map(|r| r.get(0))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))
}

const UPSERT_BUILTIN: &str = "INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,status,device_info,metadata,owner_id,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now()) ON CONFLICT(workspace_id,daemon_id,provider) WHERE profile_id IS NULL DO UPDATE SET name=EXCLUDED.name,runtime_mode=EXCLUDED.runtime_mode,status=EXCLUDED.status,device_info=EXCLUDED.device_info,metadata=EXCLUDED.metadata,owner_id=COALESCE(EXCLUDED.owner_id,agent_runtime.owner_id),last_seen_at=now(),updated_at=now() WHERE $9::uuid IS NULL OR agent_runtime.owner_id=$9 RETURNING *, (xmax=0) AS inserted";
const UPSERT_PROFILE: &str = "INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,status,device_info,metadata,owner_id,profile_id,last_seen_at) VALUES($1,$2,$3,'local',$4,$5,$6,$7,$8,$9,now()) ON CONFLICT(workspace_id,daemon_id,profile_id) WHERE profile_id IS NOT NULL DO UPDATE SET name=EXCLUDED.name,runtime_mode=EXCLUDED.runtime_mode,provider=EXCLUDED.provider,status=EXCLUDED.status,device_info=EXCLUDED.device_info,metadata=EXCLUDED.metadata,owner_id=COALESCE(EXCLUDED.owner_id,agent_runtime.owner_id),last_seen_at=now(),updated_at=now() WHERE $8::uuid IS NULL OR agent_runtime.owner_id=$8 RETURNING *, (xmax=0) AS inserted";

async fn inherit_name(
    db: &impl GenericClient,
    row: Row,
    inserted: bool,
    ws: Uuid,
    daemon: &str,
) -> Row {
    if !inserted || row.get::<_, Option<String>>("custom_name").is_some() {
        return row;
    }
    let id: Uuid = row.get("id");
    let Ok(names) = db.query("SELECT custom_name FROM agent_runtime WHERE workspace_id=$1 AND daemon_id=$2 AND id<>$3", &[&ws, &daemon, &id]).await else { return row };
    let mut shared: Option<String> = None;
    for n in &names {
        let Some(name) = n.get::<_, Option<String>>(0) else {
            return row;
        };
        if name.trim().is_empty() || shared.as_ref().is_some_and(|s| s != name.trim()) {
            return row;
        }
        shared = Some(name.trim().into());
    }
    if let Some(name) = shared
        && let Ok(Some(updated)) = db
            .query_opt(
                "UPDATE agent_runtime SET custom_name=$2,updated_at=now() WHERE id=$1 RETURNING *",
                &[&id, &name],
            )
            .await
    {
        return updated;
    }
    row
}

fn normalized_repos(raw: &Value) -> Vec<Value> {
    let Some(items) = raw.as_array() else {
        return Vec::new();
    };
    // Decode every field first: Go rejects the entire repo list, not just the
    // malformed item, even if it happens to have a duplicate/empty URL.
    let mut decoded = Vec::new();
    for item in items {
        if !item.is_object() && !item.is_null() {
            return Vec::new();
        }
        let mut fields = Vec::new();
        for field in ["url", "description", "ref"] {
            let value = item.as_object().and_then(|obj| {
                obj.get(field).or_else(|| {
                    obj.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(field))
                        .map(|(_, v)| v)
                })
            });
            let text = match value {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(v)) => v.clone(),
                _ => return Vec::new(),
            };
            fields.push(text);
        }
        decoded.push(fields);
    }
    let mut repos = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for fields in decoded {
        let url = fields[0].trim();
        if url.is_empty() || !seen.insert(url.to_owned()) {
            continue;
        }
        let mut repo = json!({"url":url});
        for (key, value) in ["description", "ref"]
            .into_iter()
            .zip(fields.into_iter().skip(1))
        {
            if !value.is_empty() {
                repo[key] = json!(value);
            }
        }
        repos.push(repo);
    }
    repos
}

// ApiError intentionally carries only static messages; profile errors contain
// the profile ID in the Go wire contract, so keep their owned message local.
enum RegistrationError {
    Plain(ApiError),
    Detailed(StatusCode, String),
}
impl From<ApiError> for RegistrationError {
    fn from(error: ApiError) -> Self {
        Self::Plain(error)
    }
}
impl IntoResponse for RegistrationError {
    fn into_response(self) -> Response {
        match self {
            Self::Plain(error) => error.into_response(),
            Self::Detailed(status, message) => {
                let mut response = GoJson(json!({"error":message})).into_response();
                *response.status_mut() = status;
                response
            }
        }
    }
}
fn reg_error(status: StatusCode, message: &'static str) -> RegistrationError {
    RegistrationError::Plain(error(status, message))
}

async fn register(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<GoJson<Value>, RegistrationError> {
    let (req, ws) = registration_request(&body)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| reg_error(StatusCode::NOT_FOUND, "workspace not found"))?;
    if let DaemonIdentity::Bound { daemon, .. } = &identity
        && daemon != &req.daemon_id
    {
        return Err(reg_error(StatusCode::FORBIDDEN, "daemon identity mismatch"));
    }
    let qoder = req.daemon_id.starts_with("qoder-managed-");
    if let DaemonIdentity::Bound { hash, .. } = &identity
        && qoder
    {
        if !qoder_allowed(&**db, ws, &req.daemon_id, hash).await? {
            return Err(reg_error(
                StatusCode::FORBIDDEN,
                "managed runtime credential required",
            ));
        }
        if !req.failed_profiles.is_empty()
            || req.runtimes.iter().any(|rt| {
                !rt.kind.trim().eq_ignore_ascii_case("qoder_cloud")
                    || !rt.profile_id.trim().is_empty()
            })
        {
            return Err(reg_error(
                StatusCode::FORBIDDEN,
                "managed runtime registration must be a single qoder_cloud runtime",
            ));
        }
    }
    let (owner, merge_owner) = match &identity {
        DaemonIdentity::Bound {
            workspace, owner, ..
        } => {
            if *workspace != ws {
                return Err(reg_error(StatusCode::NOT_FOUND, "workspace not found"));
            }
            if !qoder && owner.is_none() {
                return Err(reg_error(
                    StatusCode::FORBIDDEN,
                    "daemon credential is not bound to a runtime owner",
                ));
            }
            (if qoder { None } else { *owner }, None)
        }
        DaemonIdentity::User(user) => {
            let role = member_role(&**db, ws, *user).await?;
            if qoder {
                return Err(reg_error(StatusCode::FORBIDDEN, "reserved daemon_id"));
            }
            if req
                .runtimes
                .iter()
                .any(|rt| rt.kind.trim().eq_ignore_ascii_case("qoder_cloud"))
                && role != "owner"
                && role != "admin"
            {
                return Err(reg_error(
                    StatusCode::FORBIDDEN,
                    "workspace administrator required",
                ));
            }
            (Some(*user), Some(*user))
        }
    };
    if req
        .runtimes
        .iter()
        .any(|rt| rt.kind.trim().eq_ignore_ascii_case("qoder_cloud"))
        && let DaemonIdentity::Bound { hash, .. } = &identity
        && !qoder_allowed(&**db, ws, &req.daemon_id, hash)
            .await
            .unwrap_or(false)
    {
        return Err(reg_error(
            StatusCode::FORBIDDEN,
            "managed runtime credential required",
        ));
    }
    let workspace = db
        .query_opt("SELECT repos,settings FROM workspace WHERE id=$1", &[&ws])
        .await
        .map_err(|_| reg_error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| reg_error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let mut tx = db.transaction().await.map_err(|_| {
        reg_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "failed to register runtime",
        )
    })?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,8154))",
        &[&format!("{ws}/{}", req.daemon_id)],
    )
    .await
    .map_err(|_| {
        reg_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "failed to register runtime",
        )
    })?;
    if let Some(owner) = owner {
        let rows = tx
            .query(
                "SELECT owner_id FROM agent_runtime WHERE workspace_id=$1 AND daemon_id=$2",
                &[&ws, &req.daemon_id],
            )
            .await
            .map_err(|_| register_failure())?;
        if rows
            .iter()
            .any(|r| r.get::<_, Option<Uuid>>(0) != Some(owner))
        {
            return Err(reg_error(StatusCode::FORBIDDEN, OWNED));
        }
    }
    let caps = capabilities(&headers);
    let mut runtimes = Vec::new();
    let mut legacy = Vec::new();
    for rt in &req.runtimes {
        let mut provider = rt.kind.trim().to_ascii_lowercase();
        if provider.is_empty() {
            provider = "unknown".into();
        }
        let name = if rt.name.trim().is_empty() {
            if req.device_name.is_empty() {
                provider.clone()
            } else {
                format!("{} ({})", provider, req.device_name)
            }
        } else {
            rt.name.trim().into()
        };
        let device_info = match (req.device_name.is_empty(), rt.version.is_empty()) {
            (false, false) => format!("{} · {}", req.device_name, rt.version),
            (false, true) => req.device_name.clone(),
            (true, _) => rt.version.clone(),
        };
        let status = if rt.status == "offline" {
            "offline"
        } else {
            "online"
        };
        let metadata = json!({"version":rt.version,"cli_version":req.cli_version,"launched_by":req.launched_by,"capabilities":caps});
        let custom = !rt.profile_id.trim().is_empty();
        let row = if custom {
            let profile_id = parse_id(rt.profile_id.trim(), "profile_id")?;
            let profile = tx.query_opt("SELECT runtime_type,protocol_family,enabled FROM runtime_profile WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE", &[&profile_id, &ws]).await.map_err(|_| register_failure())?
                .ok_or_else(|| RegistrationError::Detailed(StatusCode::BAD_REQUEST, format!("unknown runtime profile: {}", rt.profile_id)))?;
            if !profile.get::<_, bool>("enabled") {
                return Err(RegistrationError::Detailed(StatusCode::CONFLICT, format!("runtime profile is disabled: {}", rt.profile_id)));
            }
            let runtime_type: Option<String> = profile.get("runtime_type");
            provider = runtime_type.filter(|v| !v.is_empty()).unwrap_or_else(|| profile.get("protocol_family"));
            tx.query_opt(UPSERT_PROFILE, &[&ws, &req.daemon_id, &name, &provider, &status, &device_info, &metadata, &owner, &profile_id]).await.map_err(|_| register_failure())?
        } else {
            tx.query_opt(UPSERT_BUILTIN, &[&ws, &req.daemon_id, &name, &rt.runtime_mode, &provider, &status, &device_info, &metadata, &owner]).await.map_err(|_| register_failure())?
        }.ok_or_else(|| reg_error(StatusCode::FORBIDDEN, OWNED))?;
        let inserted: bool = row.get("inserted");
        let row = inherit_name(&*tx, row, inserted, ws, &req.daemon_id).await;
        let id: Uuid = row.get("id");
        if !custom && merge_owner.is_some() {
            legacy.push((id, provider));
        }
        runtimes.push(runtime_row(&row));
    }
    for failed in &req.failed_profiles {
        if failed.profile_id.trim().is_empty() {
            continue;
        }
        let profile_id = parse_id(failed.profile_id.trim(), "profile_id")?;
        // A bad failed-profile record is best-effort. A savepoint prevents a
        // failed statement from poisoning the successful registration transaction.
        let savepoint = tx.transaction().await.map_err(|_| register_failure())?;
        let result = async {
            let profile = savepoint.query_opt("SELECT display_name,command_name,runtime_type,protocol_family,enabled FROM runtime_profile WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE", &[&profile_id, &ws]).await.ok().flatten()?;
            if !profile.get::<_, bool>("enabled") { return None; }
            let runtime_type: Option<String> = profile.get("runtime_type");
            let provider = runtime_type.filter(|v| !v.is_empty()).unwrap_or_else(|| profile.get("protocol_family"));
            let name: String = profile.get("display_name");
            let name = if req.device_name.is_empty() { name } else { format!("{} ({})", name, req.device_name) };
            let command: String = profile.get("command_name");
            let reason = if failed.reason.trim().is_empty() { "custom runtime command could not be resolved" } else { failed.reason.trim() };
            let metadata = json!({"version":"","cli_version":req.cli_version,"launched_by":req.launched_by,"capabilities":caps,"runtime_profile_registration_error":true,"runtime_profile_failure_reason":reason,"command_name":if failed.command_name.trim().is_empty(){command.as_str()}else{failed.command_name.trim()}});
            savepoint.query_opt(UPSERT_PROFILE, &[&ws, &req.daemon_id, &name, &provider, &"offline", &req.device_name, &metadata, &owner, &profile_id]).await.ok().flatten()
        }.await;
        if let Some(row) = result {
            let inserted: bool = row.get("inserted");
            if savepoint.commit().await.is_ok() {
                let _ = inherit_name(&*tx, row, inserted, ws, &req.daemon_id).await;
            }
        } else {
            let _ = savepoint.rollback().await;
        }
    }
    tx.commit().await.map_err(|_| register_failure())?;
    // Post-commit legacy migration is best-effort, and never runs for a daemon
    // token: its identity says nothing about ownership of historical rows.
    if let Some(owner) = merge_owner {
        for (id, provider) in legacy {
            for legacy_id in &req.legacy_daemon_ids {
                if !legacy_id.trim().is_empty() {
                    merge_legacy(&mut db, ws, id, &provider, legacy_id.trim(), owner).await;
                }
            }
        }
    }
    state.events.publish(super::runtime_chat_events::Event {
        event_type: "daemon:register".into(),
        workspace_id: ws,
        actor_type: "system".into(),
        actor_id: Uuid::nil(),
        chat_session_id: None,
        chat_session_owner_id: None,
        payload: json!({"runtimes":runtimes}),
    });
    // Workspaces store JSONB, with an invalid/empty repo document behaving as [].
    let raw: Value = workspace.get("repos");
    let repos = normalized_repos(&raw);
    let mut urls: Vec<_> = repos
        .iter()
        .filter_map(|repo| repo.get("url").and_then(Value::as_str))
        .collect();
    urls.sort_unstable();
    let version = hex::encode(Sha256::digest(urls.join("\n").as_bytes()));
    let mut response = json!({"runtimes":runtimes,"repos":repos,"repos_version":version,"settings":workspace.get::<_,Value>("settings")});
    if let Some(certs) = &state.git_extra_ca_certs
        && !certs.is_empty()
    {
        response["ca_certs"] = json!(certs);
    }
    Ok(GoJson(response))
}

// Each historical row is moved atomically, under the same workspace/task-owner
// locks and task-write fence as Go. A failure never deletes the old runtime.
async fn merge_legacy(
    db: &mut deadpool_postgres::Object,
    ws: Uuid,
    new_id: Uuid,
    provider: &str,
    legacy: &str,
    owner: Uuid,
) {
    let Ok(rows) = db.query("SELECT id FROM agent_runtime WHERE workspace_id=$1 AND provider=$2 AND LOWER(daemon_id)=LOWER($3)", &[&ws, &provider, &legacy]).await else { return };
    for row in rows {
        let old_id: Uuid = row.get(0);
        if old_id == new_id {
            continue;
        }
        let Ok(tx) = db.transaction().await else {
            continue;
        };
        let ids = vec![old_id, new_id];
        let ok = async {
            tx.query("SELECT 1 FROM workspace w WHERE w.id IN (SELECT r.workspace_id FROM agent_runtime r WHERE r.id=ANY($1::uuid[])) ORDER BY w.id FOR KEY SHARE", &[&ids]).await.ok()?;
            let locked = tx.query("SELECT id,workspace_id,owner_id FROM agent_runtime WHERE id=ANY($1::uuid[]) ORDER BY id FOR UPDATE", &[&ids]).await.ok()?;
            if locked.len()!=2 || locked.iter().any(|row| row.get::<_, Uuid>("workspace_id") != ws || row.get::<_, Option<Uuid>>("owner_id") != Some(owner)) { return None; }
            let moved = tx.query_one("WITH fence AS MATERIALIZED (SELECT lock_task_owner_rows(NULL,NULL,$1) AS ok), reassigned AS (UPDATE agent_task_queue SET runtime_id=$1 WHERE runtime_id=$2 AND (SELECT ok FROM fence) RETURNING id) SELECT (SELECT ok FROM fence) AS fence_ok", &[&new_id,&old_id]).await.ok()?;
            if !moved.get::<_,bool>("fence_ok") { return None; }
            tx.execute("UPDATE agent SET runtime_id=$1 WHERE runtime_id=$2", &[&new_id,&old_id]).await.ok()?;
            tx.execute("UPDATE agent_runtime SET legacy_daemon_id=COALESCE(legacy_daemon_id,$2) WHERE id=$1", &[&new_id,&legacy]).await.ok()?;
            tx.execute("DELETE FROM agent_runtime WHERE id=$1", &[&old_id]).await.ok()?;
            Some(())
        }.await.is_some();
        if ok {
            let _ = tx.commit().await;
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Heartbeat {
    runtime_id: String,
    supports_batch_import: bool,
}

async fn heartbeat(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let req: Heartbeat = decode(&body)?;
    if req.runtime_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "runtime_id is required"));
    }
    let id = parse_id(&req.runtime_id, "runtime_id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?;
    let row = db
        .query_opt(
            "SELECT workspace_id,daemon_id,owner_id,status FROM agent_runtime WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let ws: Uuid = row.get("workspace_id");
    let daemon: Option<String> = row.get("daemon_id");
    let owner: Option<Uuid> = row.get("owner_id");
    match &identity {
        DaemonIdentity::Bound {
            workspace,
            daemon: bound,
            owner: bound_owner,
            hash,
        } => {
            if *workspace != ws {
                return Err(error(StatusCode::NOT_FOUND, "not found"));
            }
            if bound.starts_with("qoder-managed-") && !qoder_allowed(&**db, ws, bound, hash).await?
            {
                return Err(error(StatusCode::FORBIDDEN, "runtime access revoked"));
            }
            if bound.is_empty()
                || daemon.as_deref() != Some(bound)
                || (!bound.starts_with("qoder-managed-")
                    && (bound_owner.is_none() || owner != *bound_owner))
            {
                return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
            }
        }
        DaemonIdentity::User(user) => {
            member_role(&**db, ws, *user)
                .await
                .map_err(|_| error(StatusCode::NOT_FOUND, "not found"))?;
            if owner != Some(*user) {
                return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
            }
        }
    }
    // A synchronous state transition is necessary before acknowledging recovery.
    let flipped = db.execute("UPDATE agent_runtime SET status='online',last_seen_at=now(),updated_at=now() WHERE id=$1 AND status<>'online'", &[&id]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "heartbeat failed"))?;
    if flipped == 0 {
        let changed = db
            .execute(
                "UPDATE agent_runtime SET last_seen_at=now() WHERE id=$1 AND status='online'",
                &[&id],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "heartbeat failed"))?;
        if changed == 0 {
            let row = db.query_opt("UPDATE agent_runtime SET status='online',last_seen_at=now(),updated_at=now() WHERE id=$1 RETURNING id", &[&id]).await
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "heartbeat failed"))?;
            if row.is_none() {
                return Err(error(StatusCode::INTERNAL_SERVER_ERROR, "heartbeat failed"));
            }
        }
    } else {
        state.events.publish(super::runtime_chat_events::Event {
            event_type: "daemon:register".into(),
            workspace_id: ws,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({"action":"heartbeat_recovery"}),
        });
    }
    let mut response = json!({"status":"ok"});
    if let Some(actions) = response.as_object_mut() {
        actions.extend(heartbeat_actions(&state, &**db, id, req.supports_batch_import).await);
    }
    Ok(GoJson(response))
}

// Both transports claim the same pending actions after recording liveness.
// A failed individual queue probe is advisory; the next beat may retry it.
pub(super) async fn heartbeat_actions(
    state: &ApiState,
    db: &impl GenericClient,
    id: Uuid,
    supports_batch_import: bool,
) -> serde_json::Map<String, Value> {
    let mut response = serde_json::Map::new();
    // Each claim is a durable transition, made only after authorization and
    // liveness succeeded. Individual queue failures are advisory (Go behavior).
    if state.runtime_rpc_transport.is_some() {
        if let Ok(Some((request, version))) =
            super::runtime_skills_sprint_control::claim_pending_update(db, id).await
        {
            response.insert(
                "pending_update".into(),
                json!({"id":request,"target_version":version}),
            );
        }
        if let Ok(Some(request)) =
            super::runtime_skills_sprint_models::claim_pending_model_list(db, id).await
        {
            response.insert("pending_model_list".into(), json!({"id":request}));
        }
        if let Ok(true) =
            super::runtime_skills_sprint_local::has_pending_local_skill_list(db, id).await
            && let Ok(Some(request)) =
                super::runtime_skills_sprint_local::claim_pending_local_skill_list(db, id).await
        {
            response.insert("pending_local_skills".into(), json!({"id":request}));
        }
        if state.skill_events.is_some()
            && let Ok(true) =
                super::runtime_skills_sprint_local_import::has_pending_local_skill_imports(db, id)
                    .await
        {
            let limit = if supports_batch_import { 10 } else { 1 };
            if let Ok(imports) =
                super::runtime_skills_sprint_local_import::claim_pending_local_skill_imports(
                    db, id, limit,
                )
                .await
                && let Some(first) = imports.first()
            {
                response.insert(
                    "pending_local_skill_import".into(),
                    json!({"id":first["id"],"skill_key":first["skill_key"]}),
                );
                if supports_batch_import {
                    response.insert(
                        "pending_local_skill_imports".into(),
                        json!(
                            imports
                                .iter()
                                .map(|v| json!({"id":v["id"],"skill_key":v["skill_key"]}))
                                .collect::<Vec<_>>()
                        ),
                    );
                }
            }
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_is_atomic_before_any_db_write() {
        assert!(
            registration_request(br#"{"daemon_id":"a","workspace_id":"bad","runtimes":[{}]}"#)
                .is_err()
        );
        assert!(registration_request(br#"{"daemon_id":"a","workspace_id":"00000000-0000-0000-0000-000000000001","runtimes":[{"runtime_mode":"cloud","profile_id":"x"}]}"#).is_err());
        assert!(registration_request(br#"{"daemon_id":"a","workspace_id":"00000000-0000-0000-0000-000000000001","failed_profiles":[{}]}"#).is_ok());
    }
    #[test]
    fn null_body_decodes_to_go_zero_value() {
        assert!(
            matches!(decode::<Heartbeat>(b"null"), Ok(Heartbeat { runtime_id, .. }) if runtime_id.is_empty())
        );
        assert!(registration_request(b"null").is_err());
    }
    #[test]
    fn repos_are_deduped_and_malformed_rows_invalidate_everything() {
        let raw = json!([null,{"url":" a ","description":"first"},{"url":"a","description":"second"},{"url":"b"}]);
        assert_eq!(
            normalized_repos(&raw),
            vec![json!({"url":"a","description":"first"}), json!({"url":"b"})]
        );
        assert!(normalized_repos(&json!([{"url":"a"},{"url":"b","ref":3}])).is_empty());
    }
    #[test]
    fn header_capabilities_keep_order_and_duplicates() {
        let mut headers = HeaderMap::new();
        assert!(capabilities(&headers).is_none());
        headers.insert("x-client-capabilities", " a, , b, a ".parse().unwrap());
        assert_eq!(capabilities(&headers).unwrap(), vec!["a", "b", "a"]);
    }
}
