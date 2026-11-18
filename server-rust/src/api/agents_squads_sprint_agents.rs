//! Agent creation and owner-controlled access. Runtime moves/cancellation are not emulated.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[path = "agents_squads_sprint_mika.rs"]
mod mika;
#[path = "agents_squads_sprint_permissions.rs"]
mod permissions;
use permissions::{Permission, Target};

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agents", post(create))
        .route("/api/agents/", post(create))
        .route("/api/agents/mika", post(mika::create))
        .route("/api/agents/{id}", put(update))
        .route("/api/agents/{id}/", put(update))
        .route("/api/agents/{id}/restore", post(restore))
}

#[derive(Deserialize, serde::Serialize)]
struct Starter {
    #[serde(default)]
    label: String,
    #[serde(default)]
    prompt: String,
}
fn starters(items: Option<Vec<Starter>>) -> Result<Value, ApiError> {
    let mut items = items.unwrap_or_default();
    if items.len() > 3 {
        return Err(bad("conversation_starters must contain at most 3 items"));
    }
    for item in &mut items {
        item.label = item.label.trim().to_owned();
        item.prompt = item.prompt.trim().to_owned();
        if item.label.is_empty() || item.prompt.is_empty() {
            return Err(bad("conversation starter label and prompt are required"));
        }
        if item.label.chars().count() > 80 || item.prompt.chars().count() > 4000 {
            return Err(bad(
                "conversation starter label/prompt exceeds character limit",
            ));
        }
    }
    Ok(json!(items))
}
fn description(text: &str) -> Result<(), ApiError> {
    if text.chars().count() > 255 {
        return Err(bad("description must be 255 characters or fewer"));
    }
    Ok(())
}
fn concurrency(value: i32) -> Result<i32, ApiError> {
    if !(1..=50).contains(&value) {
        return Err(bad("max_concurrent_tasks must be between 1 and 50"));
    }
    Ok(value)
}
fn database_error(err: tokio_postgres::Error) -> ApiError {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        error(
            StatusCode::CONFLICT,
            "an agent with this name already exists in this workspace",
        )
    } else {
        failed("failed to write agent")
    }
}
fn unavailable() -> ApiError {
    error(
        StatusCode::NOT_IMPLEMENTED,
        "runtime moves, custom args, thinking levels, service tiers and Composio changes require runtime/provider integration",
    )
}
fn object(value: Value) -> Result<serde_json::Map<String, Value>, ApiError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| bad("invalid request body"))
}
fn typed<T: serde::de::DeserializeOwned>(
    fields: &serde_json::Map<String, Value>,
) -> Result<T, ApiError> {
    serde_json::from_value(Value::Object(fields.clone())).map_err(|_| bad("invalid request body"))
}
// No provider execution. Unsupported nonempty overrides fail before mutation,
// rather than accepting settings that the eventual invocation would drop.
fn check_overrides(fields: &serde_json::Map<String, Value>) -> Result<(), ApiError> {
    permissions::validate_shape(fields)?;
    for field in ["custom_args", "composio_toolkit_allowlist"] {
        if let Some(value) = fields.get(field).filter(|v| !v.is_null()) {
            let list = value
                .as_array()
                .ok_or_else(|| bad("invalid request body"))?;
            if list.iter().any(|v| !v.is_string()) {
                return Err(bad("invalid request body"));
            }
            if !list.is_empty() {
                return Err(unavailable());
            }
        }
    }
    for field in ["thinking_level", "service_tier"] {
        if let Some(value) = fields.get(field).filter(|v| !v.is_null()) {
            let text = value.as_str().ok_or_else(|| bad("invalid request body"))?;
            if !text.is_empty() {
                return Err(unavailable());
            }
        }
    }
    if fields.contains_key("owner_id")
        || fields.contains_key("kind")
        || fields.contains_key("system_key")
    {
        return Err(bad(
            "agent owner and kind cannot be set through this endpoint",
        ));
    }
    Ok(())
}

async fn member(tx: &Transaction<'_>, ws: Uuid, user: Uuid) -> Result<(), ApiError> {
    tx.query_opt("SELECT id FROM workspace WHERE id=$1 FOR SHARE", &[&ws])
        .await
        .map_err(|_| failed("failed to lock workspace"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    tx.query_opt(
        "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
        &[&ws, &user],
    )
    .await
    .map_err(|_| failed("failed to check workspace membership"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    Ok(())
}
async fn targets(tx: &Transaction<'_>, id: Uuid) -> Result<Vec<Target>, ApiError> {
    tx.query("SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY created_at,id", &[&id]).await
        .map_err(|_| failed("failed to load agent invocation targets"))?
        .iter().map(|row| Ok(Target { target_type: row.get("target_type"), target_id: row.get("target_id") })).collect()
}
async fn replace_targets(
    tx: &Transaction<'_>,
    ws: Uuid,
    user: Uuid,
    id: Uuid,
    perm: &Permission,
) -> Result<(), ApiError> {
    // Parent agent lock serializes concurrent permission replacement. Lock
    // member recipients before INSERT so a concurrent removal cannot resurrect
    // a stale grant. Team UUIDs are inert reserved placeholders, as in Go V1.
    let members: BTreeSet<Uuid> = perm
        .targets
        .iter()
        .filter(|t| t.target_type == "member")
        .map(|t| t.target_id)
        .collect();
    for recipient in members {
        tx.query_opt(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &recipient],
        )
        .await
        .map_err(|_| failed("failed to validate invocation target"))?
        .ok_or_else(|| bad("member invocation target is not in this workspace"))?;
    }
    tx.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&id],
    )
    .await
    .map_err(|_| failed("failed to save agent access"))?;
    for target in &perm.targets {
        tx.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id,created_by) VALUES ($1,$2,$3,$4)", &[&id,&target.target_type,&target.target_id,&user]).await
            .map_err(|_| failed("failed to save agent access"))?;
    }
    Ok(())
}
async fn finish(
    tx: deadpool_postgres::Transaction<'_>,
    bus: &SharedSquadEventSink,
    ws: Uuid,
    user: Uuid,
    id: Uuid,
    event: &'static str,
) -> Result<GoJson<Value>, ApiError> {
    let payload = settings::broadcast(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit agent"))?;
    let result = payload["agent"].clone();
    publish(bus, ws, user, event, payload);
    Ok(GoJson(result))
}

#[derive(Deserialize)]
struct Create {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    instructions: String,
    #[serde(default)]
    runtime_id: String,
    runtime_config: Option<Value>,
    custom_env: Option<BTreeMap<String, String>>,
    mcp_config: Option<Value>,
    model: Option<String>,
    avatar_url: Option<String>,
    conversation_starters: Option<Vec<Starter>>,
    max_concurrent_tasks: Option<i32>,
    skill_ids: Option<Vec<String>>,
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    actor.require_human()?;
    let fields = object(decode(body)?)?;
    let req: Create = typed(&fields)?;
    check_overrides(&fields)?;
    if req.name.is_empty() {
        return Err(bad("name is required"));
    }
    description(&req.description)?;
    if req.runtime_id.is_empty() {
        return Err(bad("runtime_id is required"));
    }
    let runtime = parse(&req.runtime_id, "invalid runtime_id")?;
    let count = concurrency(req.max_concurrent_tasks.unwrap_or(6))?;
    let starters = starters(req.conversation_starters)?;
    let perm = permissions::parse(ws, &fields, true)?.ok_or_else(|| bad("invalid permission"))?;
    let avatar = req.avatar_url.as_deref().map(avatar).transpose()?;
    let mut config = req.runtime_config.unwrap_or_else(|| json!({}));
    if !config.is_object() {
        return Err(bad("runtime_config must be an object"));
    }
    if let Some(gateway) = config.get_mut("gateway").and_then(Value::as_object_mut)
        && gateway.get("token").and_then(Value::as_str) == Some("***")
    {
        gateway.remove("token");
    }
    let skill_ids: BTreeSet<Uuid> = req
        .skill_ids
        .unwrap_or_default()
        .iter()
        .map(|id| parse(id, "invalid skill_ids"))
        .collect::<Result<_, _>>()?;
    let bus = sink(&state, &actor)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to create agent"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    member(&tx, ws, user).await?;
    let rt = tx.query_opt("SELECT owner_id,visibility,shared_with,runtime_mode,provider,status FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE", &[&runtime,&ws]).await
        .map_err(|_| failed("failed to resolve runtime"))?.ok_or_else(|| bad("invalid runtime_id"))?;
    let owner: Option<Uuid> = rt.get("owner_id");
    let shared: Vec<Uuid> = rt.get("shared_with");
    let visibility: String = rt.get("visibility");
    if owner.is_none()
        || !(owner == Some(user)
            || visibility == "public"
            || (visibility == "private" && shared.contains(&user)))
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "this runtime is private; ask its owner to grant you access before creating agents",
        ));
    }
    if rt.get::<_, String>("provider") == "qoder_cloud" {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Qoder agent configuration validation unavailable",
        ));
    }
    for skill in &skill_ids {
        tx.query_opt(
            "SELECT id FROM skill WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[skill, &ws],
        )
        .await
        .map_err(|_| failed("failed to validate skill"))?
        .ok_or_else(|| bad("skill does not belong to this workspace"))?;
    }
    let env = json!(req.custom_env.unwrap_or_default());
    let model = req.model.filter(|s| !s.is_empty());
    let status = if rt.get::<_, String>("status") == "online" {
        "idle"
    } else {
        "offline"
    };
    let runtime_mode: String = rt.get("runtime_mode");
    let visibility = perm.visibility();
    let id: Uuid = tx.query_one("INSERT INTO agent(workspace_id,name,description,instructions,runtime_mode,runtime_id,runtime_config,owner_id,permission_mode,visibility,max_concurrent_tasks,custom_env,custom_args,mcp_config,model,conversation_starters,avatar_url,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'[]'::jsonb,$13,$14,$15,$16,$17) RETURNING id", &[&ws,&req.name,&req.description,&req.instructions,&runtime_mode,&runtime,&config,&user,&perm.mode,&visibility,&count,&env,&req.mcp_config,&model,&starters,&avatar,&status]).await.map_err(database_error)?.get(0);
    replace_targets(&tx, ws, user, id, &perm).await?;
    for skill in skill_ids {
        tx.execute(
            "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
            &[&id, &skill],
        )
        .await
        .map_err(|_| failed("failed to attach agent skill"))?;
    }
    Ok((
        StatusCode::CREATED,
        finish(tx, &bus, ws, user, id, "agent:created").await?,
    ))
}

#[derive(Deserialize)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    instructions: Option<String>,
    avatar_url: Option<String>,
    conversation_starters: Option<Vec<Starter>>,
    max_concurrent_tasks: Option<i32>,
    model: Option<String>,
    runtime_config: Option<Value>,
}
async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update agent"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let fields = object(decode(body)?)?;
    let req: Update = typed(&fields)?;
    if fields.contains_key("custom_env") {
        return Err(bad(
            "custom_env is no longer accepted on this endpoint; use PUT /api/agents/{id}/env (or `multica agent env set`)",
        ));
    }
    check_overrides(&fields)?;
    for key in [
        "runtime_id",
        "status",
        "skill_ids",
        "thinking_level",
        "service_tier",
        "composio_toolkit_allowlist",
    ] {
        if fields.get(key).is_some_and(|v| !v.is_null()) {
            return Err(unavailable());
        }
    }
    if let Some(text) = &req.description {
        description(text)?;
    }
    let count = req.max_concurrent_tasks.map(concurrency).transpose()?;
    let starters = req
        .conversation_starters
        .map(|items| starters(Some(items)))
        .transpose()?;
    let bus = sink(&state, &actor)?;
    let existing = tx.query_one("SELECT owner_id,permission_mode,avatar_url,runtime_config,thinking_level,runtime_id FROM agent WHERE id=$1", &[&id]).await
        .map_err(|_| failed("failed to load agent"))?;
    let current = Permission {
        mode: existing.get("permission_mode"),
        targets: targets(&tx, id).await?,
    };
    let touched = fields.contains_key("permission_mode")
        || fields.contains_key("invocation_targets")
        || fields.get("visibility").is_some_and(|v| !v.is_null());
    if touched {
        if existing.get::<_, Option<Uuid>>("owner_id") == Some(user) {
            let perm = permissions::parse(ws, &fields, false)?.ok_or_else(|| {
                bad("permission_mode or visibility required with invocation_targets")
            })?;
            replace_targets(&tx, ws, user, id, &perm).await?;
            tx.execute(
                "UPDATE agent SET permission_mode=$2,visibility=$3 WHERE id=$1",
                &[&id, &perm.mode, &perm.visibility()],
            )
            .await
            .map_err(|_| failed("failed to update agent permission"))?;
        } else if permissions::changes(ws, &fields, &current) {
            return Err(error(
                StatusCode::FORBIDDEN,
                "only the agent owner can change access (permission_mode / invocation_targets)",
            ));
        }
    }
    let avatar = if let Some(url) = req.avatar_url.as_deref() {
        let old: Option<String> = existing.get("avatar_url");
        if old.as_deref() == Some(url) {
            Some(url)
        } else {
            Some(avatar(url)?)
        }
    } else {
        None
    };
    let mut config = req.runtime_config;
    if let Some(value) = &mut config {
        if !value.is_object() {
            return Err(bad("runtime_config must be an object"));
        }
        if let Some(gateway) = value.get_mut("gateway").and_then(Value::as_object_mut)
            && gateway.get("token").and_then(Value::as_str) == Some("***")
        {
            let old: Value = existing.get("runtime_config");
            match old.pointer("/gateway/token") {
                Some(token) => {
                    gateway.insert("token".into(), token.clone());
                }
                None => {
                    gateway.remove("token");
                }
            }
        }
    }
    // Model/effort coupling needs provider capability validation. Do not clear a
    // model while silently preserving an effort the daemon may not honor.
    let thinking: Option<String> = existing.get("thinking_level");
    if req.model.as_deref().is_some_and(|v| v.trim().is_empty())
        && thinking.as_deref().is_some_and(|v| !v.is_empty())
    {
        return Err(unavailable());
    }
    if let Some(runtime) = existing.get::<_, Option<Uuid>>("runtime_id") {
        let rt = tx
            .query_opt(
                "SELECT provider FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE",
                &[&runtime, &ws],
            )
            .await
            .map_err(|_| failed("failed to resolve runtime"))?;
        if rt
            .as_ref()
            .is_some_and(|r| r.get::<_, String>("provider") == "qoder_cloud")
        {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Qoder agent configuration validation unavailable",
            ));
        }
    }
    let mcp = fields.get("mcp_config");
    let clear_mcp = mcp.is_some_and(Value::is_null);
    let mcp = mcp.filter(|v| !v.is_null());
    let args = fields
        .get("custom_args")
        .filter(|v| !v.is_null())
        .map(|_| json!([]));
    tx.execute("UPDATE agent SET name=COALESCE($2,name),description=COALESCE($3,description),instructions=COALESCE($4,instructions),max_concurrent_tasks=COALESCE($5,max_concurrent_tasks),conversation_starters=COALESCE($6,conversation_starters),avatar_url=COALESCE($7,avatar_url),runtime_config=COALESCE($8,runtime_config),model=CASE WHEN $9::text IS NULL THEN model ELSE NULLIF($9,'') END,mcp_config=CASE WHEN $10 THEN NULL ELSE COALESCE($11,mcp_config) END,custom_args=COALESCE($12,custom_args),updated_at=now() WHERE id=$1", &[&id,&req.name,&req.description,&req.instructions,&count,&starters,&avatar,&config,&req.model,&clear_mcp,&mcp,&args]).await.map_err(database_error)?;
    finish(tx, &bus, ws, user, id, "agent:status").await
}

async fn restore(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to restore agent"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let row = tx
        .query_one("SELECT archived_at FROM agent WHERE id=$1", &[&id])
        .await
        .map_err(|_| failed("failed to load agent"))?;
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_none()
    {
        return Err(error(StatusCode::CONFLICT, "agent is not archived"));
    }
    let bus = sink(&state, &actor)?;
    tx.execute(
        "UPDATE agent SET archived_at=NULL,archived_by=NULL,updated_at=now() WHERE id=$1",
        &[&id],
    )
    .await
    .map_err(|_| failed("failed to restore agent"))?;
    finish(tx, &bus, ws, user, id, "agent:restored").await
}
