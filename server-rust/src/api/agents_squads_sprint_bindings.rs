//! Per-agent assignments only. Global skill and MCP catalog CRUD live elsewhere.
use super::*;
use axum::routing::{delete, put};

#[path = "agents_squads_sprint_labels.rs"]
mod labels;

#[path = "agents_squads_sprint_settings.rs"]
pub(in crate::api) mod settings;

#[path = "agents_squads_sprint_agents.rs"]
mod agents;

#[path = "agents_squads_sprint_read.rs"]
mod read;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agents/{id}/skills", put(set_skills))
        .route("/api/agents/{id}/skills/add", post(add_skills))
        .route(
            "/api/agents/{id}/skills/{skill_id}/enabled",
            put(skill_enabled),
        )
        .route("/api/agents/{id}/skills/{skill_id}", delete(remove_skill))
        .route("/api/agents/{id}/mcp-servers", post(add_mcp))
        .route(
            "/api/agents/{id}/mcp-servers/{server_id}/enabled",
            put(mcp_enabled),
        )
        .route(
            "/api/agents/{id}/mcp-servers/{server_id}",
            delete(remove_mcp),
        )
        .merge(labels::routes())
        .merge(settings::routes())
        .merge(agents::routes())
        .merge(read::routes())
}

// Order matches catalog-deletion contracts: workspace, member, agent, then
// sorted resource rows. Catalog deleters must lock their row BEFORE sweeping
// junctions, otherwise the no-FK MCP table can acquire an orphan after a sweep.
async fn writer(tx: &Transaction<'_>, ws: Uuid, user: Uuid, raw: &str) -> Result<Uuid, ApiError> {
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
        .map_err(|_| failed("failed to check agent permissions"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if !matches!(
        member.get::<_, String>("role").as_str(),
        "owner" | "admin" | "member"
    ) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let agent = tx
        .query_opt(
            "SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failed("failed to load agent"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if !matches!(member.get::<_, String>("role").as_str(), "owner" | "admin")
        && agent.get::<_, Option<Uuid>>("owner_id") != Some(user)
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the agent owner can manage this agent",
        ));
    }
    Ok(id)
}

#[derive(Deserialize)]
struct Skills {
    #[serde(default)]
    skill_ids: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct Enabled {
    enabled: Option<bool>,
}
impl Enabled {
    fn value(self) -> Result<bool, ApiError> {
        self.enabled.ok_or_else(|| bad("enabled is required"))
    }
}

async fn skill_summaries(tx: &Transaction<'_>, ws: Uuid, id: Uuid) -> Result<Value, ApiError> {
    let rows = tx.query("SELECT s.id,s.workspace_id,s.name,s.description,s.config,s.created_by,s.created_at,s.updated_at,b.enabled FROM skill s JOIN agent_skill b ON b.skill_id=s.id WHERE b.agent_id=$1 AND s.workspace_id=$2 ORDER BY s.name ASC", &[&id,&ws])
        .await.map_err(|_| failed("failed to list agent skills"))?;
    Ok(json!(rows.iter().map(|r| json!({
        "id":r.get::<_, Uuid>("id"), "workspace_id":r.get::<_, Uuid>("workspace_id"),
        "name":r.get::<_, String>("name"), "description":r.get::<_, String>("description"),
        "config":r.get::<_, Option<Value>>("config").unwrap_or_else(|| json!({})),
        "created_by":r.get::<_, Option<Uuid>>("created_by"),
        "created_at":timestamp(r.get("created_at")), "updated_at":timestamp(r.get("updated_at")),
        "enabled":r.get::<_, bool>("enabled")
    })).collect::<Vec<_>>()))
}

async fn finish_skills(
    tx: deadpool_postgres::Transaction<'_>,
    bus: &SharedSquadEventSink,
    ws: Uuid,
    user: Uuid,
    id: Uuid,
) -> Result<GoJson<Value>, ApiError> {
    let result = skill_summaries(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit agent skills"))?;
    publish(
        bus,
        ws,
        user,
        "agent:status",
        json!({"agent_id":id,"skills":result}),
    );
    Ok(GoJson(result))
}

async fn write_skills(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    body: Result<Json<Skills>, JsonRejection>,
    replace: bool,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update agent skills"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    // Go authorizes the agent before decoding, so a missing agent is 404
    // rather than a body-shape 400 or an event-bus 503.
    let mut ids: Vec<Uuid> = decode(body)?
        .skill_ids
        .unwrap_or_default()
        .iter()
        .map(|s| parse(s, "invalid skill_ids"))
        .collect::<Result<_, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    let bus = sink(&state, &actor)?;
    // Validate every target before deleting anything, and hold resource locks
    // until the binding insert commits. Duplicates preserve existing toggles.
    for skill in &ids {
        tx.query_opt(
            "SELECT id FROM skill WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[skill, &ws],
        )
        .await
        .map_err(|_| failed("failed to load skill"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "skill not found"))?;
    }
    if replace {
        tx.execute("DELETE FROM agent_skill WHERE agent_id=$1", &[&id])
            .await
            .map_err(|_| failed("failed to clear agent skills"))?;
    }
    for skill in ids {
        tx.execute(
            "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
            &[&id, &skill],
        )
        .await
        .map_err(|_| failed("failed to add agent skill"))?;
    }
    finish_skills(tx, &bus, ws, user, id).await
}

async fn set_skills(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Skills>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    write_skills(state, ws, user, actor, raw, body, true).await
}
async fn add_skills(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Skills>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    write_skills(state, ws, user, actor, raw, body, false).await
}
async fn skill_enabled(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, skill)): Path<(String, String)>,
    body: Result<Json<Enabled>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update agent skill"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let skill = parse(&skill, "invalid skill_id")?;
    let enabled = decode(body)?.value()?;
    let bus = sink(&state, &actor)?;
    let rows = tx.execute("UPDATE agent_skill b SET enabled=$3 WHERE b.agent_id=$1 AND b.skill_id=$2 AND EXISTS (SELECT 1 FROM skill s WHERE s.id=b.skill_id AND s.workspace_id=$4)", &[&id,&skill,&enabled,&ws])
        .await.map_err(|_| failed("failed to update agent skill"))?;
    if rows == 0 {
        return Err(error(StatusCode::NOT_FOUND, "agent skill not found"));
    }
    finish_skills(tx, &bus, ws, user, id).await
}
async fn remove_skill(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, skill)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to remove agent skill"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let skill = parse(&skill, "invalid skill_id")?;
    let bus = sink(&state, &actor)?;
    tx.execute(
        "DELETE FROM agent_skill WHERE agent_id=$1 AND skill_id=$2",
        &[&id, &skill],
    )
    .await
    .map_err(|_| failed("failed to remove agent skill"))?;
    finish_skills(tx, &bus, ws, user, id).await
}

async fn mcp_summaries(tx: &Transaction<'_>, ws: Uuid, id: Uuid) -> Result<Value, ApiError> {
    let rows = tx.query("SELECT s.id,s.workspace_id,s.name,s.config,s.created_at,s.updated_at,b.enabled FROM workspace_mcp_server s JOIN agent_mcp_server b ON b.server_id=s.id WHERE b.agent_id=$1 AND s.workspace_id=$2 ORDER BY s.name ASC", &[&id,&ws])
        .await.map_err(|_| failed("failed to list the agent's MCP servers"))?;
    // Never serialize config: it contains env, headers, URLs and credentials.
    Ok(json!(rows.iter().map(|r| json!({
        "id":r.get::<_, Uuid>("id"), "workspace_id":r.get::<_, Uuid>("workspace_id"),
        "name":r.get::<_, String>("name"), "transport":super::super::workspace_read::transport(&r.get::<_, Value>("config")),
        "enabled":r.get::<_, bool>("enabled"), "created_at":timestamp(r.get("created_at")), "updated_at":timestamp(r.get("updated_at"))
    })).collect::<Vec<_>>()))
}
async fn finish_mcp(
    tx: deadpool_postgres::Transaction<'_>,
    ws: Uuid,
    id: Uuid,
) -> Result<GoJson<Value>, ApiError> {
    let result = mcp_summaries(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to commit MCP assignments"))?;
    Ok(GoJson(result))
}
#[derive(Deserialize)]
struct Mcp {
    server_id: String,
}
async fn add_mcp(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Mcp>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to add the MCP server"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let server = parse(decode(body)?.server_id.trim(), "invalid server_id")?;
    tx.query_opt(
        "SELECT id FROM workspace_mcp_server WHERE id=$1 AND workspace_id=$2 FOR SHARE",
        &[&server, &ws],
    )
    .await
    .map_err(|_| failed("failed to load MCP server"))?
    .ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            "MCP server not found in this workspace",
        )
    })?;
    tx.execute(
        "INSERT INTO agent_mcp_server(agent_id,server_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
        &[&id, &server],
    )
    .await
    .map_err(|_| failed("failed to add the MCP server"))?;
    finish_mcp(tx, ws, id).await
}
async fn mcp_enabled(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, server)): Path<(String, String)>,
    body: Result<Json<Enabled>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update the MCP server"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let server = parse(&server, "invalid server id")?;
    let enabled = decode(body)?.value()?;
    let count = tx.execute("UPDATE agent_mcp_server b SET enabled=$3 WHERE b.agent_id=$1 AND b.server_id=$2 AND EXISTS (SELECT 1 FROM workspace_mcp_server s WHERE s.id=b.server_id AND s.workspace_id=$4)", &[&id,&server,&enabled,&ws])
        .await.map_err(|_| failed("failed to update the MCP server"))?;
    if count == 0 {
        return Err(error(
            StatusCode::NOT_FOUND,
            "this MCP server is not assigned to the agent",
        ));
    }
    finish_mcp(tx, ws, id).await
}
async fn remove_mcp(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, server)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to remove the MCP server"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to start transaction"))?;
    let id = writer(&tx, ws, user, &raw).await?;
    let server = parse(&server, "invalid server id")?;
    let count = tx
        .execute(
            "DELETE FROM agent_mcp_server WHERE agent_id=$1 AND server_id=$2",
            &[&id, &server],
        )
        .await
        .map_err(|_| failed("failed to remove the MCP server"))?;
    if count == 0 {
        return Err(error(
            StatusCode::NOT_FOUND,
            "this MCP server is not assigned to the agent",
        ));
    }
    finish_mcp(tx, ws, id).await
}
