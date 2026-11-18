//! Workspace-scoped metadata reads and anonymous share-link preview.
use super::{Actor, ApiError, ApiState, GoJson, Identity, account, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/invitations", get(invitations))
        .route(
            "/api/workspaces/{id}/runtime-profiles",
            get(runtime_profiles),
        )
        .route(
            "/api/workspaces/{id}/runtime-profiles/{profile_id}",
            get(runtime_profile),
        )
        .route("/api/workspaces/{id}/mcp-servers", get(mcp_servers))
        .route("/api/workspaces/{id}/share-links", get(share_links))
}

pub(super) fn public_routes() -> Router<ApiState> {
    Router::new().route("/api/share-links/{code}", get(share_link_preview))
}

async fn member_gate(
    db: &impl GenericClient,
    user: Uuid,
    actor: &Actor,
    raw_id: &str,
    admin: bool,
) -> Result<Uuid, ApiError> {
    actor.require_workspace_url(raw_id)?;
    let id = Uuid::parse_str(raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id = $1 AND user_id = $2",
            &[&id, &user],
        )
        .await
        // Go's URL middleware maps a missing member and DB error to the same 404.
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role: String = row.get(0);
    if admin && role != "owner" && role != "admin" {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(id)
}

async fn invitations(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Vec<account::Invitation>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let id = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let sql = format!(
        "SELECT {}, u.name AS inviter_name, u.email AS inviter_email, NULL::text AS workspace_name \
         FROM workspace_invitation wi JOIN \"user\" u ON u.id = wi.inviter_id \
         WHERE wi.workspace_id = $1 AND wi.status = 'pending' AND wi.expires_at > now() \
         ORDER BY wi.created_at DESC",
        account::INVITATION_COLUMNS
    );
    let rows = db.query(&sql, &[&id]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list invitations",
        )
    })?;
    Ok(GoJson(rows.iter().map(account::invitation_row).collect()))
}

#[derive(Serialize)]
pub(super) struct RuntimeProfile {
    id: Uuid,
    workspace_id: Uuid,
    display_name: String,
    protocol_family: String,
    runtime_type: String,
    command_name: String,
    description: Option<String>,
    fixed_args: Vec<String>,
    visibility: String,
    created_by: Option<Uuid>,
    enabled: bool,
    created_at: String,
    updated_at: String,
}

pub(super) fn profile_row(r: &Row) -> RuntimeProfile {
    let family: String = r.get("protocol_family");
    let kind: String = r.get("runtime_type");
    let fixed: Value = r.get("fixed_args");
    let args = fixed.as_array().map_or_else(Vec::new, |items| {
        items
            .iter()
            .map(|v| v.as_str().unwrap_or_default().to_owned())
            .collect()
    });
    RuntimeProfile {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        display_name: r.get("display_name"),
        runtime_type: if kind.is_empty() {
            family.clone()
        } else {
            kind
        },
        protocol_family: family,
        command_name: r.get("command_name"),
        description: r.get("description"),
        fixed_args: args,
        visibility: r.get("visibility"),
        created_by: r.get("created_by"),
        enabled: r.get("enabled"),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }
}

pub(super) const PROFILE_COLUMNS: &str = "id, workspace_id, display_name, protocol_family, runtime_type, command_name, description, fixed_args, visibility, created_by, enabled, created_at, updated_at";

async fn runtime_profiles(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let id = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let rows = db.query(&format!("SELECT {PROFILE_COLUMNS} FROM runtime_profile WHERE workspace_id = $1 ORDER BY created_at ASC"), &[&id])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list runtime profiles"))?;
    Ok(GoJson(
        serde_json::json!({"runtime_profiles": rows.iter().map(profile_row).collect::<Vec<_>>() }),
    ))
}

async fn runtime_profile(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_id, raw_profile)): Path<(String, String)>,
) -> Result<GoJson<RuntimeProfile>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let id = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let profile = Uuid::parse_str(&raw_profile)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid profile id"))?;
    let row = db
        .query_opt(
            &format!(
                "SELECT {PROFILE_COLUMNS} FROM runtime_profile WHERE id = $1 AND workspace_id = $2"
            ),
            &[&profile, &id],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "runtime profile not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime profile not found"))?;
    Ok(GoJson(profile_row(&row)))
}

pub(super) fn transport(config: &Value) -> String {
    let Some(obj) = config.as_object() else {
        return "unknown".into();
    };
    let field = |name: &str| {
        obj.get(name).or_else(|| {
            obj.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
        })
    };
    // Go's encoding/json folds struct field names; an invalid explicit type
    // fails decoding even when a command or URL is present.
    if obj.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("type") && !value.is_string() && !value.is_null()
    }) {
        return "unknown".into();
    }
    if let Some(value) = field("type") {
        let kind = value.as_str().unwrap_or("").trim().to_lowercase();
        if !kind.is_empty() {
            return match kind.as_str() {
                "local" | "stdio" => "stdio".into(),
                "remote" | "http" | "streamable-http" => "http".into(),
                _ => kind,
            };
        }
    }
    if field("command").is_some() {
        "stdio".into()
    } else if field("url").is_some() {
        "http".into()
    } else {
        "unknown".into()
    }
}

#[derive(Serialize)]
struct McpServer {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    transport: String,
    agent_count: i64,
    created_at: String,
    updated_at: String,
}

async fn mcp_servers(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Vec<McpServer>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let id = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let failed = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list workspace MCP servers",
        )
    };
    let rows = db
        .query(
            "SELECT s.id, s.workspace_id, s.name, s.config, s.created_at, s.updated_at, \
         (SELECT COUNT(*) FROM agent_mcp_server ams JOIN agent a ON a.id = ams.agent_id \
          WHERE ams.server_id = s.id AND a.archived_at IS NULL)::bigint AS agent_count \
         FROM workspace_mcp_server s WHERE s.workspace_id = $1 ORDER BY s.name ASC",
            &[&id],
        )
        .await
        .map_err(|_| failed())?;
    Ok(GoJson(
        rows.iter()
            .map(|r| McpServer {
                id: r.get("id"),
                workspace_id: r.get("workspace_id"),
                name: r.get("name"),
                transport: transport(&r.get::<_, Value>("config")),
                agent_count: r.get("agent_count"),
                created_at: timestamp(r.get("created_at")),
                updated_at: timestamp(r.get("updated_at")),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct ShareLink {
    id: Uuid,
    workspace_id: Uuid,
    code: String,
    created_by: Uuid,
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_uses: Option<i32>,
    use_count: i32,
    is_active: bool,
    created_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    creator_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    creator_email: String,
}

async fn share_links(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Vec<ShareLink>>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let id = member_gate(&**db, user, &actor, &raw_id, true).await?;
    let rows = db.query(
        "SELECT l.id, l.workspace_id, l.code, l.created_by, l.role, l.expires_at, l.max_uses, l.use_count, l.is_active, l.created_at, u.name AS creator_name, u.email AS creator_email \
         FROM workspace_share_link l JOIN \"user\" u ON u.id = l.created_by \
         WHERE l.workspace_id = $1 AND l.is_active = true ORDER BY l.created_at DESC",
        &[&id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list share links"))?;
    Ok(GoJson(
        rows.iter()
            .map(|r| ShareLink {
                id: r.get("id"),
                workspace_id: r.get("workspace_id"),
                code: r.get("code"),
                created_by: r.get("created_by"),
                role: r.get("role"),
                expires_at: r
                    .get::<_, Option<DateTime<Utc>>>("expires_at")
                    .map(timestamp),
                max_uses: r.get("max_uses"),
                use_count: r.get("use_count"),
                is_active: r.get("is_active"),
                created_at: timestamp(r.get("created_at")),
                creator_name: r.get("creator_name"),
                creator_email: r.get("creator_email"),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct SharePreview {
    workspace_name: String,
    workspace_slug: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    creator_name: String,
    role: String,
}

async fn share_link_preview(
    State(state): State<ApiState>,
    Path(raw_code): Path<String>,
) -> Result<GoJson<SharePreview>, ApiError> {
    let code = raw_code.trim();
    if code.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "code is required"));
    }
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "share link not found or expired"))?;
    let row = db.query_opt(
        "SELECT w.name AS workspace_name, w.slug AS workspace_slug, u.name AS creator_name, l.role \
         FROM workspace_share_link l JOIN workspace w ON w.id = l.workspace_id \
         JOIN \"user\" u ON u.id = l.created_by WHERE l.code = $1 AND l.is_active = true \
         AND (l.expires_at IS NULL OR l.expires_at > now()) \
         AND (l.max_uses IS NULL OR l.use_count < l.max_uses)",
        &[&code],
    ).await.map_err(|_| error(StatusCode::NOT_FOUND, "share link not found or expired"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "share link not found or expired"))?;
    Ok(GoJson(SharePreview {
        workspace_name: row.get("workspace_name"),
        workspace_slug: row.get("workspace_slug"),
        creator_name: row.get("creator_name"),
        role: row.get("role"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mcp_transport_never_exposes_configuration() {
        assert_eq!(
            transport(&serde_json::json!({"type":"WebSocket","url":"wss://private"})),
            "websocket"
        );
        assert_eq!(transport(&serde_json::json!({"type":"remote"})), "http");
        assert_eq!(transport(&serde_json::json!({"command":null})), "stdio");
        assert_eq!(
            transport(&serde_json::json!({"Type":"WebSocket","URL":"wss://secret"})),
            "websocket"
        );
        assert_eq!(transport(&serde_json::json!({"COMMAND":null})), "stdio");
        assert_eq!(
            transport(&serde_json::json!({"Url":"https://secret"})),
            "http"
        );
        assert_eq!(
            transport(&serde_json::json!({"TYPE":4,"URL":"https://secret"})),
            "unknown"
        );
        assert_eq!(
            transport(&serde_json::json!({"type":4,"url":"https://x"})),
            "unknown"
        );
    }
}
