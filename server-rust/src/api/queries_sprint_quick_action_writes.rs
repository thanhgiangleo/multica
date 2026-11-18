//! Workspace quick-action definitions. Execution belongs to runtime_chat.
use super::{
    ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId,
    issue_write::WriteError as ApiError, timestamp,
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{patch, post},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Serialize)]
struct QuickAction {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    description: String,
    assignee_type: String,
    assignee_id: String,
    prompt: String,
    visibility: String,
    status: String,
    last_used_at: Option<String>,
    use_count: i64,
    created_by_id: Uuid,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    target_name: String,
    target_public: bool,
    target_missing: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Create {
    name: String,
    description: String,
    assignee_type: String,
    assignee_id: String,
    prompt: String,
    visibility: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    assignee_type: Option<String>,
    assignee_id: Option<String>,
    prompt: Option<String>,
    visibility: Option<String>,
    status: Option<String>,
}

struct Target {
    name: String,
    public: bool,
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/quick-actions", post(create))
        .route("/api/quick-actions/", post(create))
        .route("/api/quick-actions/{id}", patch(update).delete(delete))
        .route("/api/quick-actions/{id}/", patch(update).delete(delete))
}

fn error(status: StatusCode, msg: impl Into<String>) -> ApiError {
    ApiError(status, msg.into())
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    error(StatusCode::BAD_REQUEST, msg)
}

fn body_error(_: JsonRejection) -> ApiError {
    bad_request("invalid request body")
}

fn trim(raw: &str, name: &str, limit: usize, required: bool) -> Result<String, ApiError> {
    let trimmed = raw.trim();
    if required && trimmed.is_empty() {
        return Err(bad_request(format!("{name} is required")));
    }
    if trimmed.chars().count() > limit {
        return Err(bad_request(format!(
            "{name} must be at most {limit} characters"
        )));
    }
    Ok(trimmed.to_string())
}

fn validate_prompt(raw: &str) -> Result<String, ApiError> {
    let prompt = trim(raw, "prompt", 4000, true)?;
    static TEMPLATE: OnceLock<Regex> = OnceLock::new();
    static MENTION: OnceLock<Regex> = OnceLock::new();
    if let Some(token) = TEMPLATE
        .get_or_init(|| Regex::new(r"\{\{[^}]*\}\}").unwrap())
        .find(&prompt)
    {
        return Err(bad_request(format!(
            "template variables are not supported yet; remove {} — the agent already reads this issue",
            token.as_str()
        )));
    }
    if MENTION
        .get_or_init(|| Regex::new(r"mention://(agent|squad|member|all)/").unwrap())
        .is_match(&prompt)
    {
        return Err(bad_request(
            "the prompt cannot @mention an agent, squad, or person; a quick action reaches exactly the one target it is bound to (an issue link is fine)",
        ));
    }
    Ok(prompt)
}

fn visibility(raw: &str) -> Result<String, ApiError> {
    match raw.trim() {
        "" | "public" => Ok("public".to_string()),
        "private" => Ok("private".to_string()),
        _ => Err(bad_request("visibility must be \"public\" or \"private\"")),
    }
}

fn assignee(kind: &str, raw: &str) -> Result<Option<Uuid>, ApiError> {
    if kind.is_empty() && raw.is_empty() {
        return Ok(None);
    }
    if kind != "agent" && kind != "squad" {
        return Err(bad_request("assignee_type must be \"agent\" or \"squad\""));
    }
    if raw.trim().is_empty() {
        return Err(bad_request("assignee_id is required"));
    }
    Uuid::parse_str(raw)
        .map(Some)
        .map_err(|_| bad_request("invalid assignee_id"))
}

fn admin(role: &str) -> Result<(), ApiError> {
    if matches!(role, "owner" | "admin") {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "insufficient permissions"))
    }
}

async fn member(db: &impl GenericClient, ws: Uuid, user: Uuid) -> Result<String, ApiError> {
    db.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR UPDATE",
        &[&ws, &user],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to check membership",
        )
    })?
    .map(|row| row.get(0))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))
}

async fn target(
    db: &impl GenericClient,
    ws: Uuid,
    kind: &str,
    id: Option<Uuid>,
) -> Result<Option<Target>, ApiError> {
    let Some(id) = id else {
        return Ok(Some(Target {
            name: String::new(),
            public: true,
        }));
    };
    let query = if kind == "squad" {
        "SELECT s.name, a.permission_mode, EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND t.target_type='workspace' AND t.target_id=$1) AS invocable FROM squad s JOIN agent a ON a.id=s.leader_id AND a.workspace_id=s.workspace_id WHERE s.workspace_id=$1 AND s.id=$2 AND s.archived_at IS NULL AND a.archived_at IS NULL"
    } else {
        "SELECT a.name, a.permission_mode, EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND t.target_type='workspace' AND t.target_id=$1) AS invocable FROM agent a WHERE a.workspace_id=$1 AND a.id=$2 AND a.archived_at IS NULL"
    };
    let found = db
        .query_opt(query, &[&ws, &id])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load assignee"))?;
    Ok(found.map(|row| Target {
        name: row.get("name"),
        public: row.get::<_, String>("permission_mode") == "public_to"
            && row.get::<_, bool>("invocable"),
    }))
}

async fn valid_target(
    db: &impl GenericClient,
    ws: Uuid,
    kind: &str,
    id: Option<Uuid>,
    visibility: &str,
) -> Result<Target, ApiError> {
    let target = target(db, ws, kind, id)
        .await?
        .ok_or_else(|| bad_request("assignee not found in this workspace"))?;
    if visibility == "public" && !target.public {
        return Err(bad_request(
            "a public quick action must use an agent every workspace member can trigger; make the agent public or set this action to private",
        ));
    }
    Ok(target)
}

fn response(row: &Row, target: Option<Target>) -> QuickAction {
    QuickAction {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        description: row.get("description"),
        assignee_type: row.get("assignee_type"),
        assignee_id: row
            .get::<_, Option<Uuid>>("assignee_id")
            .map_or_else(String::new, |id| id.to_string()),
        prompt: row.get("prompt"),
        visibility: row.get("visibility"),
        status: row.get("status"),
        last_used_at: row
            .get::<_, Option<DateTime<Utc>>>("last_used_at")
            .map(timestamp),
        use_count: row.get("use_count"),
        created_by_id: row.get("created_by_id"),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
        target_name: target.as_ref().map_or_else(String::new, |t| t.name.clone()),
        target_public: target.as_ref().is_some_and(|t| t.public),
        target_missing: target.is_none(),
    }
}

fn id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad_request("invalid quick action id"))
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<Create>, JsonRejection>,
) -> Result<(StatusCode, GoJson<QuickAction>), ApiError> {
    actor.require_human()?;
    let Json(body) = body.map_err(body_error)?;
    let visibility = visibility(&body.visibility)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create quick action",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    let role = member(&tx, ws, user).await?;
    if visibility == "public" {
        admin(&role)?;
    }
    let name = trim(&body.name, "name", 32, true)?;
    let assignee = assignee(&body.assignee_type, &body.assignee_id)?;
    let prompt = validate_prompt(&body.prompt)?;
    let description = trim(&body.description, "description", 200, false)?;
    // Locking the workspace serializes concurrent creates and archive restores.
    tx.query_one("SELECT 1 FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| fail())?;
    let target = valid_target(&tx, ws, &body.assignee_type, assignee, &visibility).await?;
    let count: i64 = tx
        .query_one(
            "SELECT count(*) FROM quick_action WHERE workspace_id=$1 AND status='active'",
            &[&ws],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    if count >= 30 {
        return Err(bad_request(
            "a workspace can have at most 30 active quick actions; archive one first",
        ));
    }
    let row = tx.query_one(
        "INSERT INTO quick_action(workspace_id,name,description,assignee_type,assignee_id,prompt,visibility,created_by_type,created_by_id) VALUES ($1,$2,$3,$4,$5,$6,$7,'member',$8) RETURNING *",
        &[&ws, &name, &description, &body.assignee_type, &assignee, &prompt, &visibility, &user],
    ).await.map_err(|_| fail())?;
    let result = response(&row, Some(target));
    tx.commit().await.map_err(|_| fail())?;
    Ok((StatusCode::CREATED, GoJson(result)))
}

async fn load(db: &impl GenericClient, ws: Uuid, user: Uuid, id: Uuid) -> Result<Row, ApiError> {
    let not_found = || error(StatusCode::NOT_FOUND, "quick action not found");
    let row = db
        .query_opt(
            "SELECT * FROM quick_action WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    if row.get::<_, String>("visibility") == "private"
        && row.get::<_, Uuid>("created_by_id") != user
    {
        return Err(not_found());
    }
    Ok(row)
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Update>, JsonRejection>,
) -> Result<GoJson<QuickAction>, ApiError> {
    actor.require_human()?;
    let id = id(&raw)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update quick action",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    let role = member(&tx, ws, user).await?;
    let old = load(&tx, ws, user, id).await?;
    let Json(body) = body.map_err(body_error)?;
    let old_visibility: String = old.get("visibility");
    let visibility = body
        .visibility
        .as_deref()
        .map(visibility)
        .transpose()?
        .unwrap_or_else(|| old_visibility.clone());
    if visibility == "public" || old_visibility == "public" {
        admin(&role)?;
    }
    let name = body
        .name
        .as_deref()
        .map(|v| trim(v, "name", 32, true))
        .transpose()?;
    let description = body
        .description
        .as_deref()
        .map(|v| trim(v, "description", 200, false))
        .transpose()?;
    let prompt = body.prompt.as_deref().map(validate_prompt).transpose()?;
    let kind = body
        .assignee_type
        .clone()
        .unwrap_or_else(|| old.get("assignee_type"));
    let raw_id = body.assignee_id.clone().unwrap_or_else(|| {
        old.get::<_, Option<Uuid>>("assignee_id")
            .map_or_else(String::new, |v| v.to_string())
    });
    let assignee = assignee(&kind, &raw_id)?;
    let target = valid_target(&tx, ws, &kind, assignee, &visibility).await?;
    let status = match body.status.as_deref() {
        None => old.get::<_, String>("status"),
        Some("active") => "active".to_string(),
        Some("archived") => "archived".to_string(),
        _ => return Err(bad_request("status must be \"active\" or \"archived\"")),
    };
    if status == "active" && old.get::<_, String>("status") != "active" {
        tx.query_one("SELECT 1 FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
            .await
            .map_err(|_| fail())?;
        let count: i64 = tx
            .query_one(
                "SELECT count(*) FROM quick_action WHERE workspace_id=$1 AND status='active'",
                &[&ws],
            )
            .await
            .map_err(|_| fail())?
            .get(0);
        if count >= 30 {
            return Err(bad_request(
                "a workspace can have at most 30 active quick actions",
            ));
        }
    }
    let changed_target = body.assignee_type.is_some() || body.assignee_id.is_some();
    let set_kind = changed_target.then_some(kind);
    let set_id = changed_target.then_some(assignee);
    let row = tx.query_one(
        "UPDATE quick_action SET name=COALESCE($3,name), description=COALESCE($4,description), assignee_type=COALESCE($5,assignee_type), assignee_id=CASE WHEN $5::text IS NOT NULL THEN $6::uuid ELSE assignee_id END, prompt=COALESCE($7,prompt), visibility=COALESCE($8,visibility), status=COALESCE($9,status), updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",
        &[&id, &ws, &name, &description, &set_kind, &set_id.flatten(), &prompt, &body.visibility.as_ref().map(|_| visibility), &body.status.as_ref().map(|_| status)],
    ).await.map_err(|_| fail())?;
    let result = response(&row, Some(target));
    tx.commit().await.map_err(|_| fail())?;
    Ok(GoJson(result))
}

async fn delete(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    actor.require_human()?;
    let id = id(&raw)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete quick action",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    let role = member(&tx, ws, user).await?;
    let old = load(&tx, ws, user, id).await?;
    if old.get::<_, String>("visibility") == "public" {
        admin(&role)?;
    }
    tx.execute(
        "DELETE FROM quick_action WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail())?;
    tx.commit().await.map_err(|_| fail())?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_prompt_embedded_triggers_and_bounds() {
        assert!(validate_prompt("{{issue.title}}").is_err());
        assert!(validate_prompt("mention://agent/0000").is_err());
        assert!(validate_prompt("mention://member/0000").is_err());
        assert!(validate_prompt("mention://issue/0000").is_ok());
        assert!(trim(&"🌱".repeat(33), "name", 32, true).is_err());
    }
}
