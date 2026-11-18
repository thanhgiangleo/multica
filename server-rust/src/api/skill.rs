//! Workspace skill summaries, full detail and opt-in metadata reads from PostgreSQL.
use super::{ApiError, ApiState, GoJson, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::Digest;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/skills", get(list))
        .route("/api/skills/", get(list))
        .route("/api/skills/{id}", get(detail))
        .route("/api/skills/{id}/", get(detail))
        .route("/api/skills/{id}/files", get(files))
        .route("/api/agents/{id}/skills", get(agent_skills))
        .route("/api/agents/{id}/mcp-servers", get(agent_mcp_servers))
        .route("/api/agents/{id}/labels", get(agent_labels))
        .route("/api/skills/{id}/labels", get(skill_labels))
}

#[derive(Serialize)]
struct Label {
    id: Uuid,
    workspace_id: Uuid,
    resource_type: String,
    name: String,
    description: String,
    color: String,
    usage_count: i64,
    created_at: String,
    updated_at: String,
}

fn label(row: &Row) -> Label {
    Label {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        resource_type: row.get("resource_type"),
        name: row.get("name"),
        description: row.get("description"),
        color: row.get("color"),
        usage_count: 0,
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
    }
}

#[derive(Serialize)]
struct Skill {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    description: String,
    config: Value,
    created_by: Option<Uuid>,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<Label>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    files: Option<Vec<SkillFile>>,
}

fn skill(row: &Row) -> Skill {
    Skill {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        description: row.get("description"),
        config: row
            .get::<_, Option<Value>>("config")
            .unwrap_or_else(|| json!({})),
        created_by: row.get("created_by"),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
        content: None,
        labels: None,
        enabled: None,
        content_size: None,
        content_hash: None,
        files: None,
    }
}

#[derive(Serialize)]
struct SkillFile {
    id: Uuid,
    skill_id: Uuid,
    path: String,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
}

fn file(row: &Row, metadata: bool) -> SkillFile {
    SkillFile {
        id: row.get("id"),
        skill_id: row.get("skill_id"),
        path: row.get("path"),
        created_at: timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        updated_at: timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
        content: (!metadata).then(|| row.get("content")),
        size: metadata.then(|| row.get("size")),
        content_hash: metadata.then(|| row.get("content_hash")),
    }
}

fn include(raw: Option<&str>) -> Result<bool, ApiError> {
    let params = super::catalog::parse_params(raw);
    match params.get("include").map(|s| s.trim()).unwrap_or("") {
        "" | "content" => Ok(false),
        "metadata" => Ok(true),
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "invalid include: expected \"content\" or \"metadata\"",
        )),
    }
}

fn skill_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid skill id"))
}

async fn load_skill(db: &deadpool_postgres::Client, id: Uuid, ws: Uuid) -> Result<Row, ApiError> {
    // Go returns 404 for query errors and missing rows on this detail endpoint.
    db.query_opt("SELECT id, workspace_id, name, description, config, created_by, created_at, updated_at, content FROM skill WHERE id=$1 AND workspace_id=$2", &[&id, &ws])
        .await.ok().flatten().ok_or_else(|| error(StatusCode::NOT_FOUND, "skill not found"))
}

async fn load_files(
    db: &deadpool_postgres::Client,
    id: Uuid,
    metadata: bool,
) -> Result<Vec<SkillFile>, ApiError> {
    let query = if metadata {
        "SELECT id, skill_id, path, octet_length(content)::bigint AS size, encode(sha256(convert_to(content, 'UTF8')), 'hex') AS content_hash, created_at, updated_at FROM skill_file WHERE skill_id=$1 ORDER BY path ASC"
    } else {
        "SELECT id, skill_id, path, content, created_at, updated_at FROM skill_file WHERE skill_id=$1 ORDER BY path ASC"
    };
    let rows = db.query(query, &[&id]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list skill files",
        )
    })?;
    Ok(rows.iter().map(|r| file(r, metadata)).collect())
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
) -> Result<GoJson<Vec<Skill>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list skills"))?;
    let rows = db.query("SELECT id, workspace_id, name, description, config, created_by, created_at, updated_at FROM skill WHERE workspace_id=$1 ORDER BY name ASC", &[&ws])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list skills"))?;
    let mut skills: Vec<_> = rows.iter().map(skill).collect();
    if !skills.is_empty() {
        let ids: Vec<Uuid> = skills.iter().map(|s| s.id).collect();
        // Label failures are non-critical in Go. No cross-workspace labels may escape.
        if let Ok(labels) = db.query("SELECT stl.skill_id, l.* FROM issue_label l JOIN skill_to_label stl ON stl.label_id=l.id WHERE stl.skill_id=ANY($1::uuid[]) AND l.workspace_id=$2 AND l.resource_type='skill' ORDER BY stl.skill_id, lower(l.name) ASC", &[&ids, &ws]).await {
            for row in labels {
                if let Some(s) = skills.iter_mut().find(|s| s.id == row.get::<_, Uuid>("skill_id")) {
                    s.labels.get_or_insert_with(Vec::new).push(label(&row));
                }
            }
        }
    }
    for s in &mut skills {
        s.labels.get_or_insert_with(Vec::new);
    }
    Ok(GoJson(skills))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Skill>, ApiError> {
    let metadata = include(query.as_deref())?;
    let id = skill_id(&raw)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "skill not found"))?;
    let row = load_skill(&db, id, ws).await?;
    let mut result = skill(&row);
    if metadata {
        let content: String = row.get("content");
        result.content_size = Some(content.len());
        result.content_hash = Some(hex::encode(sha2::Sha256::digest(content.as_bytes())));
    } else {
        result.content = Some(row.get("content"));
    }
    result.files = Some(load_files(&db, id, metadata).await?);
    Ok(GoJson(result))
}

async fn files(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Vec<SkillFile>>, ApiError> {
    let metadata = include(query.as_deref())?;
    let id = skill_id(&raw)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "skill not found"))?;
    load_skill(&db, id, ws).await?;
    Ok(GoJson(load_files(&db, id, metadata).await?))
}

// Go's loadAgentForUser resolves the workspace first and only accepts kind=user.
// Keep the agent and skill workspace predicates in the queries so even a
// cross-tenant association cannot surface content from another workspace.
async fn agent_skills(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Vec<Skill>>, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let found = db
        .query_opt(
            "SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if found.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let rows = db.query(
        "SELECT s.id, s.workspace_id, s.name, s.description, s.config, s.created_by, s.created_at, s.updated_at, ask.enabled FROM skill s JOIN agent_skill ask ON ask.skill_id=s.id WHERE ask.agent_id=$1 AND s.workspace_id=$2 ORDER BY s.name ASC",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list agent skills"))?;
    Ok(GoJson(
        rows.iter()
            .map(|r| {
                let mut result = skill(r);
                result.enabled = Some(r.get("enabled"));
                result
            })
            .collect(),
    ))
}

// Both label handlers resolve the resource before reading its labels. The
// junction has no tenant FK; constrain both the resource and label workspace.
async fn agent_labels(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let exists = db
        .query_opt(
            "SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
            &[&id, &ws],
        )
        .await
        .ok()
        .flatten()
        .is_some();
    if !exists {
        return Err(error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let rows = db.query(
        "SELECT l.* FROM issue_label l JOIN agent_to_label atl ON atl.label_id=l.id WHERE atl.agent_id=$1 AND l.workspace_id=$2 AND l.resource_type='agent' ORDER BY lower(l.name) ASC",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list agent labels"))?;
    Ok(GoJson(
        json!({"labels": rows.iter().map(label).collect::<Vec<_>>()}),
    ))
}

async fn skill_labels(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let id = skill_id(&raw)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "skill not found"))?;
    load_skill(&db, id, ws).await?;
    let rows = db.query(
        "SELECT l.* FROM issue_label l JOIN skill_to_label stl ON stl.label_id=l.id WHERE stl.skill_id=$1 AND l.workspace_id=$2 AND l.resource_type='skill' ORDER BY lower(l.name) ASC",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list skill labels"))?;
    Ok(GoJson(
        json!({"labels": rows.iter().map(label).collect::<Vec<_>>()}),
    ))
}

// The binding has no FK to a workspace. Scope BOTH sides of this association;
// a corrupt cross-workspace binding cannot disclose another tenant's catalog.
#[derive(Serialize)]
struct AgentMcpServer {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    transport: String,
    enabled: bool,
    created_at: String,
    updated_at: String,
}

async fn agent_mcp_servers(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Vec<AgentMcpServer>>, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let exists = db
        .query_opt(
            "SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if exists.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let rows = db.query(
        "SELECT s.id, s.workspace_id, s.name, s.config, s.created_at, s.updated_at, ams.enabled \
         FROM workspace_mcp_server s JOIN agent_mcp_server ams ON ams.server_id=s.id \
         WHERE ams.agent_id=$1 AND s.workspace_id=$2 ORDER BY s.name ASC",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list the agent's MCP servers"))?;
    Ok(GoJson(
        rows.iter()
            .map(|r| AgentMcpServer {
                id: r.get("id"),
                workspace_id: r.get("workspace_id"),
                name: r.get("name"),
                transport: super::workspace_read::transport(&r.get::<_, Value>("config")),
                enabled: r.get("enabled"),
                created_at: timestamp(r.get("created_at")),
                updated_at: timestamp(r.get("updated_at")),
            })
            .collect(),
    ))
}
