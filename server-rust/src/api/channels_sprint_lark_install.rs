//! Lark device-flow install sessions (begin + status). The registration
//! service is injected; this module never talks to Lark itself.
use super::{Actor, ApiError, ApiState, GoJson, Identity, channels_sprint, error};
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

type Params = HashMap<String, String>;

/// Session state as stored by the registration service. `status` is one of
/// "pending" | "success" | "error"; `error_reason` is a stable code.
#[derive(Clone, Debug)]
pub struct InstallSession {
    pub status: String,
    pub installation_id: Option<Uuid>,
    pub error_reason: String,
    pub error_message: String,
    pub initiator_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct BeginInstallResult {
    pub session_id: String,
    pub qr_code_url: String,
    pub expires_in_seconds: i64,
    pub poll_interval_seconds: i64,
}

#[derive(Clone, Debug)]
pub enum SessionLookupError {
    NotFound,
    Failed,
}

/// Device-flow registration seam. The integrator supplies the real service
/// (MULTICA_LARK_SECRET_KEY + APIClient); tests supply a deterministic fake.
#[async_trait]
pub trait LarkRegistrar: Send + Sync {
    async fn begin_install(
        &self,
        workspace_id: Uuid,
        agent_id: Uuid,
        initiator_id: Uuid,
        region: &str,
    ) -> Result<BeginInstallResult, String>;
    async fn install_session(
        &self,
        workspace_id: Uuid,
        session_id: &str,
    ) -> Result<InstallSession, SessionLookupError>;
}

pub(super) fn routes(root: &str) -> Router<ApiState> {
    Router::new()
        .route(&format!("{root}/install/begin"), post(begin))
        .route(&format!("{root}/install/{{session}}/status"), get(status))
}

fn coded(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, GoJson(json!({"error": message, "code": code}))).into_response()
}

/// canManageAgent: the bound agent's owner or a workspace owner/admin.
async fn can_manage_agent(
    db: &impl tokio_postgres::GenericClient,
    workspace: Uuid,
    owner_id: Uuid,
    user: Uuid,
) -> Result<(), ApiError> {
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found"))?
        .map(|r| r.get::<_, String>(0))
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    if !channels_sprint::admin(&role) && owner_id != user {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the agent owner can manage this agent",
        ));
    }
    Ok(())
}

async fn begin(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    axum::extract::Query(query): axum::extract::Query<Params>,
) -> Result<Response, ApiError> {
    let workspace = channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    let Some(registrar) = channels_sprint::runtime_for(
        &state,
        channels_sprint::Channel::Lark,
        channels_sprint::Operation::Install,
    )?
    .lark_registrar() else {
        return Ok(coded(
            StatusCode::FORBIDDEN,
            "lark_not_configured",
            "lark install not configured",
        ));
    };
    let agent_raw = query.get("agent_id").map_or("", |s| s.as_str()).trim();
    if agent_raw.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "agent_id is required"));
    }
    let agent = Uuid::parse_str(agent_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent_id"))?;
    // An unknown region would silently land the user on the wrong cloud.
    let region = query
        .get("region")
        .map_or("", |s| s.as_str())
        .trim()
        .to_lowercase();
    if !region.is_empty() && region != "feishu" && region != "lark" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "region must be 'feishu' or 'lark'",
        ));
    }
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let owner = db
        .query_opt(
            "SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
            &[&agent, &workspace],
        )
        .await
        .map_err(channels_sprint::db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))?;
    can_manage_agent(&**db, workspace, owner.get("owner_id"), user).await?;
    match registrar
        .begin_install(workspace, agent, user, &region)
        .await
    {
        Ok(result) => Ok(GoJson(json!({
            "session_id": result.session_id,
            "qr_code_url": result.qr_code_url,
            "expires_in_seconds": result.expires_in_seconds,
            "poll_interval_seconds": result.poll_interval_seconds,
        }))
        .into_response()),
        // Go appends the service error verbatim, so the body cannot be a
        // static message and is written here instead of through ApiError.
        Err(reason) => Ok((
            StatusCode::BAD_GATEWAY,
            GoJson(json!({"error": format!("failed to start install: {reason}")})),
        )
            .into_response()),
    }
}

#[derive(Serialize)]
struct InstallStatus {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    installation_id: Option<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_reason: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_message: String,
}

async fn status(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
) -> Result<Response, ApiError> {
    let workspace = channels_sprint::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    let Some(registrar) = channels_sprint::runtime_for(
        &state,
        channels_sprint::Channel::Lark,
        channels_sprint::Operation::Install,
    )?
    .lark_registrar() else {
        return Ok(coded(
            StatusCode::FORBIDDEN,
            "lark_not_configured",
            "lark install not configured",
        ));
    };
    let session_id = params.get("session").map_or("", String::as_str).trim();
    if session_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "session id is required"));
    }
    let session = match registrar.install_session(workspace, session_id).await {
        Ok(session) => session,
        Err(SessionLookupError::NotFound) => {
            return Err(error(StatusCode::NOT_FOUND, "install session not found"));
        }
        Err(SessionLookupError::Failed) => {
            return Err(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load install session",
            ));
        }
    };
    // Only the initiator or a workspace owner/admin may read the session; a
    // session id is handed back only to the initiator, so anyone else gets the
    // same 404 as an unknown session (no existence leak).
    if session.initiator_id != user {
        let db = state
            .pool
            .get()
            .await
            .map_err(|_| error(StatusCode::NOT_FOUND, "install session not found"))?;
        let role = db
            .query_opt(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&workspace, &user],
            )
            .await
            .ok()
            .flatten()
            .map(|r| r.get::<_, String>(0));
        if !role.is_some_and(|role| channels_sprint::admin(&role)) {
            return Err(error(StatusCode::NOT_FOUND, "install session not found"));
        }
    }
    Ok(GoJson(install_status(session)).into_response())
}

fn install_status(session: InstallSession) -> InstallStatus {
    InstallStatus {
        status: session.status,
        installation_id: session.installation_id.map(|id| id.to_string()),
        error_reason: session.error_reason,
        error_message: session.error_message,
    }
}
