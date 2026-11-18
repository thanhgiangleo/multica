//! Workspace MCP mutations. Config is write-only, including transport credentials.
use super::{Actor, ApiError, ApiState, GoJson, Identity, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{delete, post},
};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/mcp-servers", post(create))
        .route(
            "/api/workspaces/{id}/mcp-servers/{server_id}",
            delete(remove).put(update),
        )
}

fn failed(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

async fn writer(
    db: &impl GenericClient,
    user: Uuid,
    actor: &Actor,
    raw: &str,
) -> Result<Uuid, ApiError> {
    actor.require_workspace_url(raw)?;
    let ws =
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !matches!(actor, Actor::Human) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "agents cannot modify the workspace MCP servers",
        ));
    }
    if !matches!(row.get::<_, &str>(0), "owner" | "admin") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(ws)
}

async fn fence_writer(db: &impl GenericClient, ws: Uuid, user: Uuid) -> Result<(), ApiError> {
    // Lock workspace before membership, matching workspace teardown lock order.
    if db
        .query_opt("SELECT id FROM workspace WHERE id=$1 FOR KEY SHARE", &[&ws])
        .await
        .map_err(|_| failed("failed to authorize MCP mutation"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failed("failed to authorize MCP mutation"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !matches!(row.get::<_, &str>(0), "owner" | "admin") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(())
}

fn parse_request(bytes: &[u8]) -> Result<(String, Option<Value>), ApiError> {
    // Parse the document first so explicit null is still distinguishable from omission.
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let object = value
        .as_object()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let name = match object.get("name") {
        None | Some(Value::Null) => "",
        Some(Value::String(name)) => name,
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid request body")),
    };
    Ok((name.trim().to_owned(), object.get("config").cloned()))
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "name is required"));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name may only contain letters, digits, hyphens, and underscores",
        ));
    }
    Ok(())
}

fn validate_config(config: &Value) -> Result<(), ApiError> {
    let Some(config) = config.as_object() else {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "config must be a JSON object",
        ));
    };
    if config.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "config must not be empty"));
    }
    Ok(())
}

fn transport(config: &Value) -> String {
    if let Some(declared) = config.get("type").and_then(Value::as_str) {
        let declared = declared.trim().to_ascii_lowercase();
        match declared.as_str() {
            "" => (),
            "local" | "stdio" => return "stdio".into(),
            "remote" | "http" | "streamable-http" => return "http".into(),
            _ => return declared,
        }
    }
    if config.get("command").is_some() {
        "stdio".into()
    } else if config.get("url").is_some() {
        "http".into()
    } else {
        "unknown".into()
    }
}

fn summary(row: &Row) -> Value {
    json!({
        "id": row.get::<_, Uuid>("id"),
        "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "name": row.get::<_, String>("name"),
        "transport": transport(&row.get::<_, Value>("config")),
        "created_at": timestamp(row.get("created_at")),
        "updated_at": timestamp(row.get("updated_at")),
    })
}

fn write_error(err: tokio_postgres::Error, message: &'static str) -> ApiError {
    if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
        return error(
            StatusCode::CONFLICT,
            "an MCP server with this name already exists in the workspace",
        );
    }
    // Never expose PostgreSQL detail: it can include config, URLs and tokens.
    failed(message)
}

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    bytes: axum::body::Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to create the MCP server"))?;
    let ws = writer(&**db, user, &actor, &raw).await?;
    let (name, config) = parse_request(&bytes)?;
    validate_name(&name)?;
    let config =
        config.ok_or_else(|| error(StatusCode::BAD_REQUEST, "config must be a JSON object"))?;
    validate_config(&config)?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to create the MCP server"))?;
    fence_writer(&*tx, ws, user).await?;
    let row = tx.query_one(
        "INSERT INTO workspace_mcp_server(workspace_id,name,config,created_by) VALUES ($1,$2,$3,$4) RETURNING id,workspace_id,name,config,created_at,updated_at",
        &[&ws,&name,&config,&user],
    ).await.map_err(|e| write_error(e,"failed to create the MCP server"))?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to create the MCP server"))?;
    Ok((StatusCode::CREATED, GoJson(summary(&row))))
}

async fn update(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_server)): Path<(String, String)>,
    bytes: axum::body::Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update the MCP server"))?;
    let ws = writer(&**db, user, &actor, &raw).await?;
    let server = Uuid::parse_str(&raw_server)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid server id"))?;
    let (name, config) = parse_request(&bytes)?;
    let name = if name.is_empty() {
        None
    } else {
        validate_name(&name)?;
        Some(name)
    };
    if let Some(config) = &config {
        validate_config(config)?;
    }
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to update the MCP server"))?;
    fence_writer(&*tx, ws, user).await?;
    let row = tx.query_opt(
        "UPDATE workspace_mcp_server SET name=COALESCE($3,name),config=COALESCE($4,config),updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING id,workspace_id,name,config,created_at,updated_at",
        &[&server,&ws,&name,&config],
    ).await.map_err(|e| write_error(e,"failed to update the MCP server"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"MCP server not found"))?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to update the MCP server"))?;
    Ok(GoJson(summary(&row)))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_server)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to delete the MCP server"))?;
    let ws = writer(&**db, user, &actor, &raw).await?;
    let server = Uuid::parse_str(&raw_server)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid server id"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to delete the MCP server"))?;
    fence_writer(&*tx, ws, user).await?;
    if tx
        .query_opt(
            "SELECT id FROM workspace_mcp_server WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&server, &ws],
        )
        .await
        .map_err(|_| failed("failed to delete the MCP server"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "MCP server not found"));
    }
    tx.execute(
        "DELETE FROM workspace_mcp_server WHERE id=$1 AND workspace_id=$2",
        &[&server, &ws],
    )
    .await
    .map_err(|_| failed("failed to delete the MCP server"))?;
    tx.execute(
        "DELETE FROM agent_mcp_server WHERE server_id=$1",
        &[&server],
    )
    .await
    .map_err(|_| failed("failed to delete the MCP server"))?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to delete the MCP server"))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_only_validation_does_not_echo_config() {
        for raw in [
            br#"{"name":"safe","config":null}"#.as_slice(),
            br#"{"name":"safe","config":[]}"#,
            br#"{"name":"safe","config":"secret-token"}"#,
        ] {
            let (_, config) = parse_request(raw).unwrap();
            let err = validate_config(&config.unwrap()).unwrap_err();
            assert_eq!(err.1, "config must be a JSON object");
            assert!(!err.1.contains("secret-token"));
        }
        assert!(validate_name("safe-server_1").is_ok());
        for name in ["", "with space", "é", "../unsafe", "x\0"] {
            assert!(validate_name(name).is_err());
        }
        assert_eq!(
            transport(&json!({"type":"WebSocket","url":"synthetic-secret"})),
            "websocket"
        );
        assert_eq!(transport(&json!({"command":null})), "stdio");
        let (_, missing) = parse_request(br#"{"name":"safe"}"#).unwrap();
        assert!(missing.is_none());
    }
}
