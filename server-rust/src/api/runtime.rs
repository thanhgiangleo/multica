//! Runtime catalog only. Governance visibility is not a machine-use grant.
use super::{
    ApiError, ApiState, GoJson, Identity,
    catalog::{WorkspaceId, parse_params},
    error, timestamp,
};
use axum::extract::{Extension, RawQuery, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    // Match requireWorkspaceMember even when membership was revoked after the
    // outer guard. owner=me takes precedence over owner/admin governance access.
    let role: String = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .ok()
        .flatten()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let params = parse_params(raw.as_deref());
    let rows = if params.get("owner").is_some_and(|v| v == "me") {
        db.query("SELECT * FROM agent_runtime WHERE workspace_id=$1 AND owner_id=$2 ORDER BY created_at ASC", &[&ws, &user]).await
    } else if matches!(role.as_str(), "owner" | "admin") {
        db.query("SELECT * FROM agent_runtime WHERE workspace_id=$1 ORDER BY created_at ASC", &[&ws]).await
    } else {
        db.query("SELECT * FROM agent_runtime WHERE workspace_id=$1 AND (owner_id=$2 OR visibility='public' OR (visibility='private' AND $2=ANY(shared_with))) ORDER BY created_at ASC", &[&ws, &user]).await
    }.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list runtimes"))?;
    Ok(GoJson(json!(
        rows.iter().map(runtime_row).collect::<Vec<_>>()
    )))
}

pub(super) fn runtime_row(row: &Row) -> Value {
    let metadata: Option<Value> = row.get("metadata");
    let shared_with: Vec<Option<Uuid>> = row.get("shared_with");
    let provider: String = row.get("provider");
    json!({
        "id": row.get::<_, Uuid>("id"),
        "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "daemon_id": row.get::<_, Option<String>>("daemon_id"),
        "name": row.get::<_, String>("name"),
        "custom_name": row.get::<_, Option<String>>("custom_name"),
        "runtime_mode": row.get::<_, String>("runtime_mode"),
        "launch_header": launch_header(&provider),
        "provider": provider,
        "status": row.get::<_, String>("status"),
        "device_info": row.get::<_, String>("device_info"),
        "metadata": metadata.filter(|v| !v.is_null()).unwrap_or_else(|| json!({})),
        "owner_id": row.get::<_, Option<Uuid>>("owner_id"),
        "visibility": row.get::<_, String>("visibility"),
        "shared_with": shared_with.into_iter().map(|id| id.map(|id| id.to_string()).unwrap_or_default()).collect::<Vec<_>>(),
        "profile_id": row.get::<_, Option<Uuid>>("profile_id"),
        "last_seen_at": row.get::<_, Option<DateTime<Utc>>>("last_seen_at").map(timestamp),
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        "updated_at": timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
    })
}

// Frozen agent.LaunchHeader and the omp built-in descriptor. These are display
// strings, never executable discovery or commands run by this backend.
fn launch_header(provider: &str) -> &'static str {
    match provider {
        "antigravity" => "agy -p (non-interactive)",
        "claude" => "claude (stream-json)",
        "codebuddy" => "codebuddy (stream-json)",
        "codex" => "codex app-server",
        "copilot" => "copilot (json)",
        "cursor" => "cursor-agent (stream-json)",
        "codearts" => "codearts run (json)",
        "deveco" => "deveco run (json)",
        "hermes" => "hermes acp",
        "kimi" => "kimi acp",
        "reasonix" => "reasonix acp",
        "dsh" => "dsh --profile multica (stdio)",
        "kiro" => "kiro-cli acp",
        "openclaw" => "openclaw agent (json)",
        "opencode" => "opencode run (json)",
        "pi" => "pi (json mode)",
        "qoder" => "qodercli --acp",
        "qoderclicn" => "qoderclicn --acp",
        "traecli" => "traecli acp serve",
        "grok" => "grok agent stdio",
        "qwen" => "qwen -p (stream-json)",
        "qwenpaw" => "qwenpaw acp",
        "dim" => "dim acp",
        "devin" => "devin acp",
        "mcode" => "mcode acp",
        "muse" => "muse serve (msp)",
        "zeroclaw" => "zeroclaw acp",
        "omp" => "omp (json mode)",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn launch_preview_never_guesses_unknown_commands() {
        assert_eq!(super::launch_header("omp"), "omp (json mode)");
        assert_eq!(super::launch_header("synthetic"), "");
        assert_eq!(super::launch_header("CODEX"), "");
    }
}
