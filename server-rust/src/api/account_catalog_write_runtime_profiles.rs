//! Runtime-profile definitions. Registered-instance teardown remains a runtime integration gate.
use super::account_catalog_write_catalog_publish::{self as events, CatalogChange};
use super::{
    Actor, ApiState, GoJson, Identity, account_catalog_write_profile::account_request,
    workspace_read,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{patch, post},
};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, error::SqlState};
use uuid::Uuid;

#[derive(Debug)]
struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, GoJson(json!({"error":self.1}))).into_response()
    }
}
fn error(status: StatusCode, message: impl Into<String>) -> ApiError {
    ApiError(status, message.into())
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/runtime-profiles", post(create))
        .route(
            "/api/workspaces/{id}/runtime-profiles/{profile_id}",
            patch(update).put(update).delete(remove),
        )
        .layer(DefaultBodyLimit::disable())
}

async fn admin(
    db: &impl GenericClient,
    raw: &str,
    user: Uuid,
    actor: &Actor,
) -> Result<Uuid, ApiError> {
    actor
        .require_workspace_url(raw)
        .map_err(|err| error(err.0, err.1))?;
    let ws =
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace id"))?;
    // Role mutations and workspace deletion take this same lock, so permission cannot go stale.
    db.query_opt("SELECT id FROM workspace WHERE id=$1 FOR UPDATE", &[&ws])
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let member = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !matches!(member.get::<_, String>(0).as_str(), "owner" | "admin") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if !matches!(actor, Actor::Human) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    Ok(ws)
}

fn command_name(raw: &str) -> Result<String, ApiError> {
    let value = raw.trim();
    let message = if value.is_empty() {
        Some("command_name is required")
    } else if value.chars().any(|c| matches!(c, ' ' | '\t' | '\r' | '\n')) {
        Some("command_name must be a single executable token; put arguments in fixed_args")
    } else if value.contains('\0') {
        Some("command_name cannot contain NUL bytes")
    } else {
        None
    };
    match message {
        Some(message) => Err(error(StatusCode::BAD_REQUEST, message)),
        None => Ok(value.to_owned()),
    }
}

fn fixed_args(args: Vec<String>) -> Result<Value, ApiError> {
    for arg in &args {
        if arg.trim().is_empty() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "fixed_args entries must be non-empty",
            ));
        }
        if arg.contains('\0') {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "fixed_args entries cannot contain NUL bytes",
            ));
        }
    }
    Ok(json!(args))
}

// Frozen Go agent.SupportedTypes and BuiltinRuntimes; no CLI lookup or execution.
fn family(kind: &str) -> Option<&str> {
    match kind {
        "omp" => Some("pi"),
        "claude" | "codebuddy" | "codex" | "copilot" | "opencode" | "codearts" | "deveco"
        | "openclaw" | "hermes" | "pi" | "cursor" | "kimi" | "reasonix" | "dsh" | "kiro"
        | "antigravity" | "qoder" | "qoderclicn" | "traecli" | "grok" | "qwen" | "qwenpaw"
        | "mcode" | "muse" | "dim" | "devin" | "zeroclaw" => Some(kind),
        _ => None,
    }
}

fn write_error(err: tokio_postgres::Error, message: &'static str) -> ApiError {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        error(
            StatusCode::CONFLICT,
            "a runtime profile with this display_name already exists",
        )
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, message)
    }
}
async fn notify(
    state: &ApiState,
    workspace: Uuid,
    user: Uuid,
    profile: Uuid,
    change: CatalogChange,
) {
    if let Some(publisher) = &state.catalog_publisher {
        publisher
            .notify_runtime_profiles_changed(workspace, profile)
            .await;
    }
    events::publish(state, workspace, user, change).await;
}

#[path = "account_catalog_write_runtime_profiles_decode.rs"]
mod decode;
use decode::{CreateRequest, UpdateRequest};

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_ws): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<workspace_read::RuntimeProfile>), ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create runtime profile",
        )
    })?;
    let ws = admin(&*tx, &raw_ws, user, &actor).await?;
    let req: CreateRequest =
        account_request(&body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let name = req.display_name.trim().to_owned();
    if name.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "display_name is required"));
    }
    let supplied_family = req.protocol_family.trim().to_owned();
    let kind = req.runtime_type.trim().to_owned();
    let kind = if kind.is_empty() {
        supplied_family.clone()
    } else {
        kind
    };
    let Some(protocol) = family(&kind) else {
        return Err(error(
            StatusCode::BAD_REQUEST,
            format!("unsupported runtime_type: {kind}"),
        ));
    };
    if !supplied_family.is_empty() && supplied_family != protocol {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "protocol_family does not match runtime_type",
        ));
    }
    let command = command_name(&req.command_name)?;
    let args = fixed_args(req.fixed_args)?;
    let enabled = req.enabled.unwrap_or(true);
    let sql = format!(
        "INSERT INTO runtime_profile(workspace_id,display_name,protocol_family,runtime_type,command_name,description,fixed_args,visibility,created_by,enabled) VALUES($1,$2,$3,$4,$5,$6,$7,'workspace',$8,$9) RETURNING {}",
        workspace_read::PROFILE_COLUMNS
    );
    let row = tx
        .query_one(
            &sql,
            &[
                &ws,
                &name,
                &protocol,
                &kind,
                &command,
                &req.description,
                &args,
                &user,
                &enabled,
            ],
        )
        .await
        .map_err(|err| write_error(err, "failed to create runtime profile"))?;
    let profile: Uuid = row.get("id");
    let response = workspace_read::profile_row(&row);
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create runtime profile",
        )
    })?;
    drop(db);
    notify(
        &state,
        ws,
        user,
        profile,
        CatalogChange::RuntimeProfileChanged {
            runtime_profile_id: profile,
        },
    )
    .await;
    Ok((StatusCode::CREATED, GoJson(response)))
}

async fn update(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_ws, raw_profile)): Path<(String, String)>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<workspace_read::RuntimeProfile>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update runtime profile",
        )
    })?;
    let ws = admin(&*tx, &raw_ws, user, &actor).await?;
    let profile = Uuid::parse_str(&raw_profile)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid profile id"))?;
    let req: UpdateRequest =
        account_request(&body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if req.runtime_type.is_some() || req.protocol_family.is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "runtime_type and protocol_family are immutable; create a new profile",
        ));
    }
    let name = req.display_name.map(|name| name.trim().to_owned());
    if name.as_deref() == Some("") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "display_name cannot be empty",
        ));
    }
    let command = req
        .command_name
        .map(|value| command_name(&value))
        .transpose()?;
    let args = req.fixed_args.map(fixed_args).transpose()?;
    let sql = format!(
        "UPDATE runtime_profile SET display_name=COALESCE($3,display_name),command_name=COALESCE($4,command_name),description=COALESCE($5,description),fixed_args=COALESCE($6,fixed_args),enabled=COALESCE($7,enabled),updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING {}",
        workspace_read::PROFILE_COLUMNS
    );
    let row = tx
        .query_opt(
            &sql,
            &[
                &profile,
                &ws,
                &name,
                &command,
                &req.description,
                &args,
                &req.enabled,
            ],
        )
        .await
        .map_err(|err| write_error(err, "failed to update runtime profile"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime profile not found"))?;
    let response = workspace_read::profile_row(&row);
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update runtime profile",
        )
    })?;
    drop(db);
    notify(
        &state,
        ws,
        user,
        profile,
        CatalogChange::RuntimeProfileChanged {
            runtime_profile_id: profile,
        },
    )
    .await;
    Ok(GoJson(response))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_ws, raw_profile)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to begin transaction",
        )
    })?;
    let ws = admin(&*tx, &raw_ws, user, &actor).await?;
    let profile = Uuid::parse_str(&raw_profile)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid profile id"))?;
    let existing = tx
        .query_opt(
            "SELECT id FROM runtime_profile WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&profile, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load runtime profile",
            )
        })?;
    // Registration must take KEY SHARE on the profile; also cover missing-profile orphan cleanup.
    // Teardown crosses runtime/task/agent domains, so never emulate it with a bare runtime DELETE.
    let instance = tx.query_opt("SELECT id FROM agent_runtime WHERE profile_id=$1 AND workspace_id=$2 ORDER BY id LIMIT 1 FOR UPDATE", &[&profile, &ws])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to enumerate profile runtimes"))?;
    if instance.is_some() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "runtime profile teardown integration is unavailable",
        ));
    }
    if existing.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "runtime profile not found"));
    }
    tx.execute(
        "DELETE FROM runtime_profile WHERE id=$1 AND workspace_id=$2",
        &[&profile, &ws],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete runtime profile",
        )
    })?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit transaction",
        )
    })?;
    drop(db);
    notify(
        &state,
        ws,
        user,
        profile,
        CatalogChange::RuntimeProfileDeleted {
            deleted_runtime_profile_id: profile,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_definition_validation_without_executables() {
        assert_eq!(family("omp"), Some("pi"));
        assert_eq!(family("codex"), Some("codex"));
        assert_eq!(family("gemini"), None);
        for kind in [
            "claude",
            "codebuddy",
            "codex",
            "copilot",
            "opencode",
            "codearts",
            "deveco",
            "openclaw",
            "hermes",
            "pi",
            "cursor",
            "kimi",
            "reasonix",
            "dsh",
            "kiro",
            "antigravity",
            "qoder",
            "qoderclicn",
            "traecli",
            "grok",
            "qwen",
            "qwenpaw",
            "mcode",
            "muse",
            "dim",
            "devin",
            "zeroclaw",
        ] {
            assert_eq!(family(kind), Some(kind));
        }
        for command in ["", "x y", "x\ty", "x\ny", "x\0y"] {
            assert!(command_name(command).is_err());
        }
        assert_eq!(command_name(" /opt/wrapper ").unwrap(), "/opt/wrapper");
        assert!(fixed_args(vec![" ".into()]).is_err());
        assert!(fixed_args(vec!["x\0y".into()]).is_err());
        assert_eq!(
            fixed_args(vec![" --flag ".into()]).unwrap(),
            json!([" --flag "])
        );
    }
}
