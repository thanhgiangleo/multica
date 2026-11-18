//! Audited env access and per-agent local-runtime skill overrides.
use super::*;
use std::collections::BTreeMap;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/agents/{id}/env",
            axum::routing::get(reveal).put(update_env),
        )
        .route(
            "/api/agents/{id}/runtime-skills/enabled",
            put(runtime_skill),
        )
}

type Env = BTreeMap<String, String>;
#[derive(Deserialize)]
struct EnvRequest {
    #[serde(default)]
    custom_env: Option<Env>,
}

fn env(row: &Row) -> Result<Env, ApiError> {
    serde_json::from_value(row.get::<_, Value>("custom_env"))
        .map_err(|_| failed("invalid stored agent env"))
}

// Env management is owner-scoped: a workspace owner/admin, or the human who
// owns the agent. An agent actor (task token) may never touch it — that is the
// MUL-2600 impersonation boundary, and its denial is env-specific so an agent
// cannot mistake it for a generic permission failure.
fn env_actor(actor: &Actor) -> Result<(), ApiError> {
    match actor {
        Actor::Human => Ok(()),
        Actor::Task(_) => Err(error(
            StatusCode::FORBIDDEN,
            "agents may not access env management endpoints",
        )),
        // Deliberately stricter than the Go handler, which resolves a cloud
        // PAT to actorType "member" and would therefore serve plaintext to a
        // machine credential. A check is never removed to match a gap.
        Actor::Cloud | Actor::Daemon => Err(error(
            StatusCode::FORBIDDEN,
            "this endpoint is only available to human actors",
        )),
    }
}

// Mirrors Go's authorizeAgentEnv check order and messages: resolve the agent
// (404 first), then deny machine actors, then require membership and the
// owner/admin-or-agent-owner rule. Locks are still taken in the catalog order
// (workspace, member, agent) so a concurrent binding write cannot deadlock
// against this one.
async fn env_writer(
    tx: &Transaction<'_>,
    ws: Uuid,
    user: Uuid,
    raw: &str,
    actor: &Actor,
) -> Result<Uuid, ApiError> {
    let id = parse(raw, "invalid agent id")?;
    tx.query_opt("SELECT id FROM workspace WHERE id=$1 FOR SHARE", &[&ws])
        .await
        .map_err(|_| failed("failed to lock workspace"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failed("failed to check agent env permissions"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let agent = tx
        .query_opt(
            "SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failed("failed to load agent env"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    env_actor(actor)?;
    let role = member.get::<_, String>("role");
    if !matches!(role.as_str(), "owner" | "admin" | "member") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(role.as_str(), "owner" | "admin")
        && agent.get::<_, Option<Uuid>>("owner_id") != Some(user)
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the agent owner or a workspace owner/admin can manage this agent's env",
        ));
    }
    Ok(id)
}

// Caller holds workspace, membership and agent locks through audit commit.
async fn audit(
    tx: &Transaction<'_>,
    ws: Uuid,
    user: Uuid,
    action: &str,
    details: Value,
) -> Result<(), ApiError> {
    tx.execute("INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details) VALUES ($1,$2,NULL,'member',$3,$4,$5)", &[&Uuid::now_v7(),&ws,&user,&action,&details])
        .await.map_err(|_| failed("audit log write failed; refusing unaudited env access"))?;
    Ok(())
}

async fn reveal(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to load agent env"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = env_writer(&tx, ws, user, &raw, &actor).await?;
    let row = tx
        .query_one("SELECT name,custom_env FROM agent WHERE id=$1", &[&id])
        .await
        .map_err(|_| failed("failed to load agent env"))?;
    let values = env(&row)?;
    let keys: Vec<_> = values.keys().collect();
    audit(&tx, ws, user, "agent_env_revealed", json!({"agent_id":id,"agent_name":row.get::<_, String>("name"),"revealed_keys":keys,"key_count":keys.len()})).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit env reveal audit"))?;
    Ok(GoJson(json!({"agent_id":id,"custom_env":values})))
}

fn merge_env(existing: &Env, request: Env) -> (Env, Value) {
    let mut merged = Env::new();
    let mut added = Vec::new();
    let mut changed = Vec::new();
    let mut preserved = Vec::new();
    let removed: Vec<_> = existing
        .keys()
        .filter(|key| !request.contains_key(*key))
        .cloned()
        .collect();
    for (key, value) in request {
        if value == "****" {
            if let Some(old) = existing.get(&key) {
                merged.insert(key.clone(), old.clone());
                preserved.push(key);
            }
            continue;
        }
        match existing.get(&key) {
            None => added.push(key.clone()),
            Some(old) if old != &value => changed.push(key.clone()),
            _ => (),
        }
        merged.insert(key, value);
    }
    // Go nil slices encode as null; non-empty lists are sorted by BTreeMap.
    fn list(keys: Vec<String>) -> Value {
        if keys.is_empty() {
            Value::Null
        } else {
            json!(keys)
        }
    }
    (
        merged,
        json!({"added_keys":list(added),"removed_keys":list(removed),"changed_keys":list(changed),"preserved_keys":list(preserved)}),
    )
}

// Whitelist public fields. Never serialize a whole agent row: custom_env,
// MCP config, Composio connections and future secret columns are private.
pub(super) async fn broadcast(tx: &Transaction<'_>, ws: Uuid, id: Uuid) -> Result<Value, ApiError> {
    let row = tx
        .query_one(
            "SELECT to_jsonb(a) AS data FROM agent a WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failed("failed to load agent response"))?;
    let data: Value = row.get("data");
    let targets = tx.query("SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY created_at,id", &[&id])
        .await.map_err(|_| failed("failed to load agent invocation targets"))?;
    let targets = json!(targets.iter().map(|row| json!({"target_type":row.get::<_,String>("target_type"),"target_id":row.get::<_,Option<Uuid>>("target_id")})).collect::<Vec<_>>());
    Ok(json!({"agent":public_agent(&data, targets, agent_skill_summaries(tx, id).await?)}))
}

async fn agent_skill_summaries(tx: &Transaction<'_>, id: Uuid) -> Result<Value, ApiError> {
    let rows = tx.query("SELECT s.id,s.name,s.description,b.enabled FROM skill s JOIN agent_skill b ON b.skill_id=s.id WHERE b.agent_id=$1 ORDER BY s.name ASC", &[&id])
        .await.map_err(|_| failed("failed to load agent skills"))?;
    Ok(json!(rows.iter().map(|r| json!({
        "id":r.get::<_, Uuid>("id"),"name":r.get::<_, String>("name"),
        "description":r.get::<_, String>("description"),"enabled":r.get::<_, bool>("enabled")
    })).collect::<Vec<_>>()))
}

// The product-owned half of a built-in agent's prompt ships with the binary,
// exactly as Go's go:embed asset does, so a release updates every workspace
// without writing the text into any agent row. Only `mika` has one today; an
// unknown system_key has no product prompt and the field stays absent.
const MIKA_INSTRUCTIONS: &str = include_str!("builtin_agents_mika_INSTRUCTIONS.md");
const MIKA_SYSTEM_KEY: &str = "mika";
const MIKA_DEFAULT_NAME: &str = "Mika";
const MIKA_NAME_PLACEHOLDER: &str = "{{AGENT_NAME}}";

fn system_instructions(system_key: &str, name: &str) -> Option<String> {
    if system_key != MIKA_SYSTEM_KEY {
        return None;
    }
    let name = name.trim();
    let name = if name.is_empty() {
        MIKA_DEFAULT_NAME
    } else {
        name
    };
    Some(
        MIKA_INSTRUCTIONS
            .trim_end_matches('\n')
            .replace(MIKA_NAME_PLACEHOLDER, name),
    )
}

const MIKA_WORKSPACE_NOTES_SECTION: &str = "## Workspace notes

Workspace notes below add this team's context and preferences — repositories, languages, conventions, routing defaults. Follow them ahead of your own defaults; they refine how you apply these instructions, and they do not remove the identity or confirmation duties above.

Added by this workspace's admins:";

/// A built-in agent's full prompt: the product-owned instructions followed by
/// the workspace's own notes, when it has any.
pub(in crate::api) fn compose_mika_instructions(
    display_name: &str,
    workspace_notes: &str,
) -> Option<String> {
    let system = system_instructions(MIKA_SYSTEM_KEY, display_name)?;
    let notes = workspace_notes.trim();
    if notes.is_empty() {
        return Some(system);
    }
    Some(format!(
        "{system}\n\n{MIKA_WORKSPACE_NOTES_SECTION}\n\n{notes}"
    ))
}

pub(super) fn public_agent(data: &Value, targets: Value, skills: Value) -> Value {
    let mut out = serde_json::Map::new();
    for key in [
        "id",
        "workspace_id",
        "name",
        "description",
        "instructions",
        "conversation_starters",
        "avatar_url",
        "runtime_mode",
        "runtime_config",
        "custom_args",
        "visibility",
        "permission_mode",
        "status",
        "max_concurrent_tasks",
        "owner_id",
        "disabled_runtime_skills",
        "created_at",
        "updated_at",
        "archived_at",
        "archived_by",
    ] {
        out.insert(key.into(), data.get(key).cloned().unwrap_or(Value::Null));
    }
    for key in ["model", "thinking_level", "service_tier", "runtime_id"] {
        out.insert(
            key.into(),
            data.get(key)
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(json!("")),
        );
    }
    for key in [
        "custom_args",
        "conversation_starters",
        "disabled_runtime_skills",
    ] {
        if out[key].is_null() {
            out.insert(key.into(), json!([]));
        }
    }
    if out["runtime_config"].is_null() {
        out.insert("runtime_config".into(), json!({}));
    }
    for key in ["created_at", "updated_at", "archived_at"] {
        if let Some(raw) = data[key].as_str()
            && let Ok(time) = chrono::DateTime::parse_from_rfc3339(raw)
        {
            out.insert(
                key.into(),
                json!(timestamp(time.with_timezone(&chrono::Utc))),
            );
        }
    }
    let count = data["custom_env"].as_object().map_or(0, |env| env.len());
    out.insert("runtime_bound".into(), json!(!data["runtime_id"].is_null()));
    out.insert("has_custom_env".into(), json!(count > 0));
    out.insert("custom_env_key_count".into(), json!(count));
    out.insert("mcp_config".into(), Value::Null);
    let mcp_configured = !data["mcp_config"].is_null();
    out.insert("mcp_config_redacted".into(), json!(mcp_configured));
    if !data["composio_toolkit_allowlist"].is_null() {
        out.insert("composio_toolkit_allowlist_redacted".into(), json!(true));
    }
    if let Some(key) = data
        .get("system_key")
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
    {
        out.insert("system_key".into(), json!(key));
        if let Some(instructions) =
            system_instructions(key, data["name"].as_str().unwrap_or_default())
        {
            out.insert("system_instructions".into(), json!(instructions));
        }
    }
    out.insert("invocation_targets".into(), targets);
    out.insert("skills".into(), skills);
    if let Some(token) = out
        .get_mut("runtime_config")
        .and_then(|v| v.get_mut("gateway"))
        .and_then(|v| v.get_mut("token"))
        && token.as_str().is_some_and(|s| !s.is_empty())
    {
        *token = json!("***");
    }
    Value::Object(out)
}

async fn update_env(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<EnvRequest>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let bus = state.agent_event_bus().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "squad event bus unavailable",
        )
    })?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update agent env"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    // Go authorizes (agent lookup, actor denial, ownership) before it decodes
    // the body, so an unknown agent is a 404 even when the body is malformed.
    let id = env_writer(&tx, ws, user, &raw, &actor).await?;
    let req = decode(body)?.custom_env.unwrap_or_default();
    let row = tx
        .query_one(
            "SELECT name,custom_env,runtime_id FROM agent WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| failed("failed to load agent env"))?;
    let (merged, mut details) = merge_env(&env(&row)?, req);
    if !merged.is_empty()
        && let Some(runtime) = row.get::<_, Option<Uuid>>("runtime_id")
    {
        let rt = tx
            .query_opt(
                "SELECT provider FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE",
                &[&runtime, &ws],
            )
            .await
            .map_err(|_| failed("failed to resolve runtime provider"))?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
        if rt.get::<_, String>("provider") == "qoder_cloud" {
            return Err(bad(
                "Qoder agents take skills, tools, credentials and model from the Qoder Agent; remove them from this agent",
            ));
        }
    }
    tx.execute(
        "UPDATE agent SET custom_env=$2,updated_at=now() WHERE id=$1",
        &[&id, &json!(merged)],
    )
    .await
    .map_err(|_| failed("failed to update agent env"))?;
    details["agent_id"] = json!(id);
    details["agent_name"] = json!(row.get::<_, String>("name"));
    audit(&tx, ws, user, "agent_env_updated", details).await?;
    let payload = broadcast(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit agent env"))?;
    publish(&bus, ws, user, "agent:status", payload);
    Ok(GoJson(json!({"agent_id":id,"custom_env":merged})))
}

#[derive(Deserialize)]
struct RuntimeSkill {
    runtime_id: String,
    root: String,
    key: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    plugin: String,
    enabled: Option<bool>,
}

// Lexical POSIX normalization matches filepath.Clean on the frozen Linux Go
// server. No filesystem access or installed-agent executable lookup occurs.
fn normalize(root: &str, key: &str, plugin: &str) -> Result<(String, String, String), ApiError> {
    let root = root.trim();
    let key = key.trim();
    if key.is_empty() || key.len() > 512 || key.starts_with('/') {
        return Err(bad("invalid runtime skill identity"));
    }
    let mut parts = Vec::new();
    for part in key.split('/') {
        match part {
            "" | "." => (),
            ".." => {
                if parts.last().is_some_and(|p| *p != "..") {
                    parts.pop();
                } else {
                    parts.push(part);
                }
            }
            _ => parts.push(part),
        }
    }
    if parts.is_empty() || parts[0] == ".." {
        return Err(bad("invalid runtime skill identity"));
    }
    let plugin = match root {
        "provider" | "universal" => "",
        "plugin" if !plugin.trim().is_empty() => plugin.trim(),
        _ => return Err(bad("invalid runtime skill identity")),
    };
    Ok((root.into(), parts.join("/"), plugin.into()))
}

async fn runtime_skill(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<RuntimeSkill>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update runtime skill"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let req = decode(body)?;
    let enabled = req
        .enabled
        .ok_or_else(|| bad("runtime_id, root, key, and enabled are required"))?;
    let runtime = parse(&req.runtime_id, "invalid runtime_id")?;
    let (root, key, plugin) = normalize(&req.root, &req.key, &req.plugin)?;
    let name = req.name.trim();
    if name.len() > 512 {
        return Err(bad("invalid runtime skill name"));
    }
    let bus = sink(&state, &actor)?;
    let row = tx
        .query_one(
            "SELECT runtime_id,disabled_runtime_skills FROM agent WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| failed("failed to load agent"))?;
    if row.get::<_, Option<Uuid>>("runtime_id") != Some(runtime) {
        return Err(error(
            StatusCode::CONFLICT,
            "agent is no longer assigned to this runtime",
        ));
    }
    let rt = tx.query_opt("SELECT runtime_mode,provider FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR SHARE", &[&runtime,&ws]).await.map_err(|_| failed("failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"runtime not found"))?;
    let provider: String = rt.get("provider");
    if rt.get::<_, String>("runtime_mode") != "local"
        || !matches!(provider.as_str(), "codex" | "claude")
    {
        return Err(bad(
            "runtime skill controls are only supported for codex and claude",
        ));
    }
    if root == "plugin" && provider != "claude" {
        return Err(bad("invalid runtime skill identity"));
    }
    let mut target = json!({"runtime_id":runtime,"provider":provider,"root":root,"key":key});
    if !name.is_empty() {
        target["name"] = json!(name);
    }
    if !plugin.is_empty() {
        target["plugin"] = json!(plugin);
    }
    let current: Value = row.get("disabled_runtime_skills");
    let mut next = current
        .as_array()
        .cloned()
        .ok_or_else(|| failed("invalid stored runtime skills"))?;
    next.retain(|item| {
        !["runtime_id", "provider", "root", "key"]
            .iter()
            .all(|field| item[field] == target[field])
            || item["plugin"].as_str().unwrap_or("") != plugin
    });
    if !enabled {
        next.push(target);
    }
    tx.execute(
        "UPDATE agent SET disabled_runtime_skills=$2,updated_at=now() WHERE id=$1",
        &[&id, &json!(next)],
    )
    .await
    .map_err(|_| failed("failed to update runtime skill"))?;
    let payload = broadcast(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit runtime skill"))?;
    publish(&bus, ws, user, "agent:status", payload);
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn env_sentinel_diff_contains_only_sorted_keys() {
        let existing = Env::from([
            ("b".into(), "secret".into()),
            ("a".into(), "gone".into()),
            ("c".into(), "old".into()),
        ]);
        let (merged, details) = merge_env(
            &existing,
            Env::from([
                ("b".into(), "****".into()),
                ("new".into(), "****".into()),
                ("c".into(), "changed".into()),
                ("d".into(), "added".into()),
            ]),
        );
        assert_eq!(
            merged,
            Env::from([
                ("b".into(), "secret".into()),
                ("c".into(), "changed".into()),
                ("d".into(), "added".into())
            ])
        );
        assert_eq!(
            details,
            json!({"added_keys":["d"],"removed_keys":["a"],"changed_keys":["c"],"preserved_keys":["b"]})
        );
    }
    #[test]
    fn system_prompt_ships_for_mika_only_and_templates_the_current_name() {
        let prompt = system_instructions("mika", "  Ada  ").unwrap();
        assert!(prompt.starts_with("You are Ada, the default agent"));
        assert!(!prompt.contains(MIKA_NAME_PLACEHOLDER));
        assert!(!prompt.ends_with('\n'));
        assert_eq!(
            system_instructions("mika", "").unwrap(),
            MIKA_INSTRUCTIONS
                .trim_end_matches('\n')
                .replace(MIKA_NAME_PLACEHOLDER, MIKA_DEFAULT_NAME)
        );
        assert!(system_instructions("agent_builder:synthetic", "Ada").is_none());
        assert!(system_instructions("", "Ada").is_none());
    }

    #[test]
    fn skill_identity_matches_linux_clean_and_root_rules() {
        assert_eq!(
            normalize(" provider ", "a/../b//./c", "ignored").unwrap(),
            ("provider".into(), "b/c".into(), "".into())
        );
        for key in ["", "/root", "..", "a/../..", "../x", "."] {
            assert!(normalize("provider", key, "").is_err(), "{key}");
        }
        assert!(normalize("plugin", "key", "").is_err());
        assert_eq!(normalize("plugin", "key", " plugin ").unwrap().2, "plugin");
    }
}
