//! Workspace-scoped GitHub App and self-hosted VCS integrations.
use super::channels_sprint_telegram::CredentialBox;
use super::{Actor, ApiError, ApiState, GoJson, Identity, error, timestamp};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use std::sync::Arc;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

#[async_trait]
pub trait GitHubRepositoryBrowser: Send + Sync {
    async fn list(&self, installation_id: i64, page: u32, per_page: u32) -> Result<Value, ()>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum VcsValidateError {
    Unauthorized,
    Unreachable,
}

#[async_trait]
pub trait VcsTokenValidator: Send + Sync {
    async fn validate(
        &self,
        provider: &str,
        instance_url: &str,
        token: &str,
    ) -> Result<String, VcsValidateError>;
}

#[derive(Clone, Default)]
pub struct IntegrationConfig {
    pub github_app_slug: String,
    pub github_webhook_secret: String,
    pub github_app_id: String,
    pub github_app_private_key: String,
    pub github_repository_browser: Option<Arc<dyn GitHubRepositoryBrowser>>,
    pub vcs_enabled: bool,
    pub vcs_secret_key: Option<[u8; 32]>,
    pub vcs_token_validator: Option<Arc<dyn VcsTokenValidator>>,
    pub public_url: String,
}

impl IntegrationConfig {
    pub fn from_env() -> Self {
        let vcs_secret_key = std::env::var("MULTICA_VCS_SECRET_KEY")
            .ok()
            .and_then(|raw| decode_vcs_key(&raw));
        Self {
            github_app_slug: std::env::var("GITHUB_APP_SLUG").unwrap_or_default(),
            github_webhook_secret: std::env::var("GITHUB_WEBHOOK_SECRET").unwrap_or_default(),
            github_app_id: std::env::var("GITHUB_APP_ID").unwrap_or_default(),
            github_app_private_key: std::env::var("GITHUB_APP_PRIVATE_KEY").unwrap_or_default(),
            github_repository_browser: None,
            vcs_enabled: std::env::var("MULTICA_VCS_INTEGRATION_ENABLED").as_deref() == Ok("true"),
            vcs_secret_key,
            vcs_token_validator: None,
            public_url: std::env::var("MULTICA_PUBLIC_URL")
                .unwrap_or_default()
                .trim()
                .trim_end_matches('/')
                .to_owned(),
        }
    }

    fn github_configured(&self) -> bool {
        !self.github_app_slug.trim().is_empty() && !self.github_webhook_secret.trim().is_empty()
    }

    fn repository_browse_configured(&self) -> bool {
        !self.github_app_id.trim().is_empty() && !self.github_app_private_key.trim().is_empty()
    }
}

// Go's base64.StdEncoding.DecodeString ignores CR and LF, but no other whitespace.
fn decode_vcs_key(raw: &str) -> Option<[u8; 32]> {
    let encoded: Vec<u8> = raw
        .bytes()
        .filter(|byte| *byte != b'\r' && *byte != b'\n')
        .collect();
    STANDARD.decode(encoded).ok()?.try_into().ok()
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/github/connect", get(github_connect))
        .route(
            "/api/workspaces/{id}/github/installations",
            get(github_installations),
        )
        .route(
            "/api/workspaces/{id}/github/installations/{installationId}/repositories",
            get(github_repositories),
        )
        .route(
            "/api/workspaces/{id}/github/installations/{installationId}",
            axum::routing::delete(delete_github_installation),
        )
        .route(
            "/api/workspaces/{id}/vcs/connections",
            get(vcs_connections).post(connect_vcs),
        )
        .route(
            "/api/workspaces/{id}/vcs/connections/{connectionId}",
            axum::routing::delete(delete_vcs_connection),
        )
        .route(
            "/api/workspaces/{id}/vcs/connections/{connectionId}/rotate-webhook",
            post(rotate_vcs_webhook),
        )
}

fn workspace_url_gate(actor: &Actor, raw_id: &str) -> Result<Uuid, ApiError> {
    if raw_id.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "workspace_id or workspace_slug is required",
        ));
    }
    // Task binding uses the literal URL value, before UUID parsing (Go middleware).
    actor.require_workspace_url(raw_id)?;
    Uuid::parse_str(raw_id).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))
}

async fn member_gate(
    db: &impl GenericClient,
    user: Uuid,
    actor: &Actor,
    raw_id: &str,
    admin: bool,
) -> Result<(Uuid, bool), ApiError> {
    let id = workspace_url_gate(actor, raw_id)?;
    let role: String = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id = $1 AND user_id = $2",
            &[&id, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let can_manage = role == "owner" || role == "admin";
    if admin && !can_manage {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok((id, can_manage))
}

fn coded_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, GoJson(json!({ "error": message, "code": code }))).into_response()
}

fn github_unconfigured() -> Value {
    json!({"url": "", "configured": false})
}

fn vcs_unavailable() -> Value {
    json!({"connections": [], "available": false, "configured": false, "can_manage": false})
}

fn allowed_return_to(value: &str) -> bool {
    matches!(value, "github" | "repositories")
}

fn query_value(uri: &Uri, name: &str) -> String {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
        .unwrap_or_default()
}

const GITHUB_SLUG_ESCAPE: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b' ')
    .add(b'%')
    .add(b'/')
    .add(b'?')
    .add(b'#');

fn sign_state(workspace: &str, return_to: &str, secret: &str) -> Result<String, ApiError> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to sign state"))?;
    let payload = if return_to == "github" {
        format!("{workspace}.{}", hex::encode(nonce))
    } else {
        format!("{workspace}.{return_to}.{}", hex::encode(nonce))
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to sign state"))?;
    mac.update(payload.as_bytes());
    Ok(format!(
        "{payload}.{}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

async fn github_connect(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    uri: Uri,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    member_gate(&**db, user, &actor, &raw_id, true).await?;
    let config = &state.integration_config;
    if !config.github_configured() {
        return Ok(GoJson(github_unconfigured()));
    }
    let return_to = query_value(&uri, "return_to");
    let return_to = if return_to.trim().is_empty() {
        "github"
    } else {
        return_to.trim()
    };
    if !allowed_return_to(return_to) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid return target"));
    }
    let signed = sign_state(&raw_id, return_to, config.github_webhook_secret.trim())?;
    let slug =
        percent_encoding::utf8_percent_encode(config.github_app_slug.trim(), GITHUB_SLUG_ESCAPE);
    let state_param: String = form_urlencoded::byte_serialize(signed.as_bytes()).collect();
    Ok(GoJson(json!({
        "url": format!("https://github.com/apps/{slug}/installations/new?state={state_param}"),
        "configured": true,
    })))
}

fn github_installations_response(
    installations: Vec<Value>,
    config: &IntegrationConfig,
    can_manage: bool,
) -> Value {
    json!({
        "installations": installations,
        "configured": config.github_configured(),
        "repository_browse_configured": config.repository_browse_configured(),
        "can_manage": can_manage,
    })
}

fn github_installation_row(row: &Row, can_manage: bool) -> Value {
    let mut response = json!({
        "id": row.get::<_, Uuid>("id").to_string(),
        "workspace_id": row.get::<_, Uuid>("workspace_id").to_string(),
        "account_login": row.get::<_, String>("account_login"),
        "account_type": row.get::<_, String>("account_type"),
        "account_avatar_url": row.get::<_, Option<String>>("account_avatar_url"),
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
    });
    if can_manage {
        response["installation_id"] = json!(row.get::<_, i64>("installation_id"));
    }
    response
}

async fn github_installations(
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
    let (id, can_manage) = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let rows = db.query("SELECT id, workspace_id, installation_id, account_login, account_type, account_avatar_url, created_at FROM github_installation WHERE workspace_id = $1 ORDER BY created_at ASC", &[&id]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list installations"))?;
    let installations: Vec<Value> = rows
        .iter()
        .map(|row| github_installation_row(row, can_manage))
        .collect();
    Ok(GoJson(github_installations_response(
        installations,
        &state.integration_config,
        can_manage,
    )))
}

fn parse_row_id(raw: &str, message: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, message))
}

fn page_param(uri: &Uri, name: &'static str, default: u32, max: u32) -> Result<u32, ApiError> {
    let raw = query_value(uri, name);
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<u32>()
        .ok()
        .filter(|value| (1..=max).contains(value))
        .ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                if name == "page" {
                    "invalid page"
                } else {
                    "invalid per_page"
                },
            )
        })
}

async fn github_repositories(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_id, raw_installation)): Path<(String, String)>,
    uri: Uri,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    member_gate(&**db, user, &actor, &raw_id, true).await?;
    let installation = parse_row_id(&raw_installation, "invalid installation id")?;
    let row = db
        .query_opt(
            "SELECT workspace_id, installation_id FROM github_installation WHERE id = $1",
            &[&installation],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load github installation",
            )
        })?;
    let Some(row) = row.filter(|row| row.get::<_, Uuid>("workspace_id").to_string() == raw_id)
    else {
        return Err(error(
            StatusCode::NOT_FOUND,
            "github installation not found",
        ));
    };
    if !state.integration_config.repository_browse_configured() {
        return Ok(coded_error(
            StatusCode::FORBIDDEN,
            "github_repository_browsing_not_configured",
            "github repository browsing is not configured",
        ));
    }
    let page = page_param(&uri, "page", 1, 100_000)?;
    let per_page = page_param(&uri, "per_page", 100, 100)?;
    let installation_id = row.get::<_, i64>("installation_id");
    browse_repositories(&state.integration_config, installation_id, page, per_page).await
}

async fn browse_repositories(
    config: &IntegrationConfig,
    installation_id: i64,
    page: u32,
    per_page: u32,
) -> Result<Response, ApiError> {
    let result = match &config.github_repository_browser {
        Some(browser) => browser.list(installation_id, page, per_page).await,
        None => Err(()),
    };
    result
        .map(|value| GoJson(value).into_response())
        .map_err(|_| {
            error(
                StatusCode::BAD_GATEWAY,
                "failed to list github repositories",
            )
        })
}

async fn delete_github_installation(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_id, raw_installation)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let (id, _) = member_gate(&**db, user, &actor, &raw_id, true).await?;
    let installation = parse_row_id(&raw_installation, "invalid installation id")?;
    db.execute(
        "DELETE FROM github_installation WHERE id = $1 AND workspace_id = $2",
        &[&installation, &id],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to remove installation",
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

fn webhook_path(id: Uuid) -> String {
    format!("/api/webhooks/vcs/{id}")
}

fn webhook_url(public_url: &str, id: Uuid) -> String {
    let base = public_url.trim_end_matches('/');
    if base.is_empty() {
        String::new()
    } else {
        format!("{base}{}", webhook_path(id))
    }
}

fn vcs_row(row: &Row, public_url: &str) -> Value {
    let id: Uuid = row.get("id");
    json!({
        "id": id.to_string(),
        "workspace_id": row.get::<_, Uuid>("workspace_id").to_string(),
        "provider": row.get::<_, String>("provider"),
        "instance_url": row.get::<_, String>("instance_url"),
        "account_login": row.get::<_, String>("account_login"),
        "webhook_url": webhook_url(public_url, id),
        "webhook_path": webhook_path(id),
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
    })
}

async fn vcs_connections(
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
    let (id, can_manage) = member_gate(&**db, user, &actor, &raw_id, false).await?;
    let config = &state.integration_config;
    if !config.vcs_enabled {
        return Ok(GoJson(vcs_unavailable()));
    }
    let rows = db.query("SELECT id, workspace_id, provider, instance_url, account_login, created_at FROM vcs_connection WHERE workspace_id = $1 ORDER BY created_at ASC", &[&id]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list connections"))?;
    let connections: Vec<Value> = rows
        .iter()
        .map(|row| vcs_row(row, &config.public_url))
        .collect();
    Ok(GoJson(json!({
        "connections": connections,
        "available": true,
        "configured": config.vcs_secret_key.is_some(),
        "can_manage": can_manage,
    })))
}

fn normalize_instance_url(raw: &str) -> &str {
    raw.trim().trim_end_matches('/')
}

fn valid_instance_url(raw: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(raw) else {
        return false;
    };
    matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
}

#[derive(Default, Deserialize)]
struct ConnectVcsRequest {
    #[serde(default)]
    provider: String,
    #[serde(default)]
    instance_url: String,
    #[serde(default)]
    access_token: String,
}

async fn connect_vcs(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let (id, _) = member_gate(&**db, user, &actor, &raw_id, true).await?;
    let config = &state.integration_config;
    if !config.vcs_enabled {
        return Err(error(
            StatusCode::NOT_FOUND,
            "vcs integration is not available on this deployment",
        ));
    }
    if config.vcs_secret_key.is_none() {
        return Ok(coded_error(
            StatusCode::FORBIDDEN,
            "vcs_not_configured",
            "vcs integration not configured (MULTICA_VCS_SECRET_KEY unset)",
        ));
    }
    let request: ConnectVcsRequest = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if !matches!(request.provider.as_str(), "forgejo" | "gitea" | "gitlab") {
        return Err(error(StatusCode::BAD_REQUEST, "unsupported provider"));
    }
    let instance = normalize_instance_url(&request.instance_url);
    if instance.is_empty() || request.access_token.trim().is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "instance_url and access_token are required",
        ));
    }
    if !valid_instance_url(instance) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "instance_url must be an absolute http(s) URL",
        ));
    }
    let token = request.access_token.trim();
    let account = validate_vcs(config, &request.provider, instance, token).await?;
    let secret = mint_webhook_secret()?;
    let key = config.vcs_secret_key.as_ref().expect("checked above");
    let box_ = CredentialBox::new(key)
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to encrypt token"))?;
    let token_encrypted = box_
        .seal(token.as_bytes())
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to encrypt token"))?;
    let secret_encrypted = box_.seal(secret.as_bytes()).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encrypt webhook secret",
        )
    })?;
    let row = db.query_one("INSERT INTO vcs_connection (workspace_id, provider, instance_url, account_login, access_token_encrypted, webhook_secret_encrypted, connected_by_id) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (workspace_id, instance_url) DO UPDATE SET provider = EXCLUDED.provider, account_login = EXCLUDED.account_login, access_token_encrypted = EXCLUDED.access_token_encrypted, webhook_secret_encrypted = EXCLUDED.webhook_secret_encrypted, connected_by_id = EXCLUDED.connected_by_id, updated_at = now() RETURNING id, workspace_id, provider, instance_url, account_login, created_at", &[&id, &request.provider, &instance, &account, &token_encrypted, &secret_encrypted, &user]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to save connection"))?;
    let mut response = vcs_row(&row, &config.public_url);
    response["webhook_secret"] = json!(secret);
    // Go publishes EventVCSConnectionCreated here; ApiState has no VCS event sink.
    Ok(GoJson(response).into_response())
}

async fn validate_vcs(
    config: &IntegrationConfig,
    provider: &str,
    instance: &str,
    token: &str,
) -> Result<String, ApiError> {
    let result = match &config.vcs_token_validator {
        Some(validator) => validator.validate(provider, instance, token).await,
        None => Err(VcsValidateError::Unreachable),
    };
    result.map_err(|err| match err {
        VcsValidateError::Unauthorized => error(
            StatusCode::BAD_REQUEST,
            "the provider rejected the access token",
        ),
        VcsValidateError::Unreachable => error(
            StatusCode::BAD_GATEWAY,
            "could not reach the provider instance",
        ),
    })
}

async fn delete_vcs_connection(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_id, raw_connection)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let (id, _) = member_gate(&**db, user, &actor, &raw_id, true).await?;
    let connection = parse_row_id(&raw_connection, "invalid connection id")?;
    db.execute("WITH target AS (SELECT id FROM vcs_connection WHERE id = $1 AND workspace_id = $2), cleared_links AS (DELETE FROM issue_vcs_pull_request WHERE pull_request_id IN (SELECT id FROM vcs_pull_request WHERE connection_id IN (SELECT id FROM target))), cleared_statuses AS (DELETE FROM vcs_commit_status WHERE connection_id IN (SELECT id FROM target)), cleared_prs AS (DELETE FROM vcs_pull_request WHERE connection_id IN (SELECT id FROM target)) DELETE FROM vcs_connection WHERE id = $1 AND workspace_id = $2", &[&connection, &id]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to remove connection"))?;
    // Go publishes EventVCSConnectionDeleted here; ApiState has no VCS event sink.
    Ok(StatusCode::NO_CONTENT)
}

fn mint_webhook_secret() -> Result<String, ApiError> {
    let mut secret = [0_u8; 32];
    getrandom::fill(&mut secret).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to mint webhook secret",
        )
    })?;
    Ok(hex::encode(secret))
}

async fn rotate_vcs_webhook(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_id, raw_connection)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let (id, _) = member_gate(&**db, user, &actor, &raw_id, true).await?;
    let connection = parse_row_id(&raw_connection, "invalid connection id")?;
    let config = &state.integration_config;
    if !config.vcs_enabled {
        return Err(error(
            StatusCode::NOT_FOUND,
            "vcs integration is not available on this deployment",
        ));
    }
    let Some(key) = config.vcs_secret_key.as_ref() else {
        return Ok(coded_error(
            StatusCode::FORBIDDEN,
            "vcs_not_configured",
            "vcs integration not configured (MULTICA_VCS_SECRET_KEY unset)",
        ));
    };
    let existing = db
        .query_opt(
            "SELECT workspace_id FROM vcs_connection WHERE id = $1",
            &[&connection],
        )
        .await;
    if existing.ok().flatten().map(|row| row.get::<_, Uuid>(0)) != Some(id) {
        return Err(error(StatusCode::NOT_FOUND, "vcs connection not found"));
    }
    let secret = mint_webhook_secret()?;
    let box_ = CredentialBox::new(key).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encrypt webhook secret",
        )
    })?;
    let encrypted = box_.seal(secret.as_bytes()).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encrypt webhook secret",
        )
    })?;
    let row = db.query_one("UPDATE vcs_connection SET webhook_secret_encrypted = $3, updated_at = now() WHERE id = $1 AND workspace_id = $2 RETURNING id, workspace_id, provider, instance_url, account_login, created_at", &[&connection, &id, &encrypted]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to rotate webhook secret"))?;
    let mut response = vcs_row(&row, &config.public_url);
    response["webhook_secret"] = json!(secret);
    // Go publishes EventVCSConnectionCreated here; ApiState has no VCS event sink.
    Ok(GoJson(response).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn vcs_key_matches_go_base64_loader() {
        let encoded = STANDARD.encode([7_u8; 32]);
        assert_eq!(decode_vcs_key(&encoded), Some([7_u8; 32]));
        assert_eq!(
            decode_vcs_key(&format!("{}\r\n{}", &encoded[..20], &encoded[20..])),
            Some([7_u8; 32])
        );
        assert_eq!(decode_vcs_key(&format!(" {encoded}")), None);
        assert_eq!(decode_vcs_key("00"), None);
    }

    #[test]
    fn workspace_gate_orders_empty_binding_and_uuid_parse() {
        let bound = Uuid::new_v4();
        let task = Actor::Task(super::super::actor::TaskIdentity {
            user: Uuid::new_v4(),
            agent: Uuid::new_v4(),
            task: Uuid::new_v4(),
            workspace: bound,
        });
        let empty = workspace_url_gate(&task, "").unwrap_err();
        assert_eq!(
            (empty.0, empty.1.as_str()),
            (
                StatusCode::BAD_REQUEST,
                "workspace_id or workspace_slug is required"
            )
        );
        let mismatch = workspace_url_gate(&task, "not-a-uuid").unwrap_err();
        assert_eq!(
            (mismatch.0, mismatch.1.as_str()),
            (
                StatusCode::FORBIDDEN,
                "task token is bound to a different workspace"
            )
        );
        let invalid = workspace_url_gate(&Actor::Human, "not-a-uuid").unwrap_err();
        assert_eq!(
            (invalid.0, invalid.1.as_str()),
            (StatusCode::BAD_REQUEST, "invalid workspace_id")
        );
        assert_eq!(
            workspace_url_gate(&task, &bound.to_string()).unwrap(),
            bound
        );
    }

    struct FakeBrowser {
        result: Result<Value, ()>,
        calls: std::sync::Mutex<Vec<(i64, u32, u32)>>,
    }

    #[async_trait]
    impl GitHubRepositoryBrowser for FakeBrowser {
        async fn list(&self, installation_id: i64, page: u32, per_page: u32) -> Result<Value, ()> {
            self.calls
                .lock()
                .unwrap()
                .push((installation_id, page, per_page));
            self.result.clone()
        }
    }

    #[tokio::test]
    async fn github_browser_passthrough_and_failures() {
        let repositories =
            json!({"repositories": [{"id": 42}], "total_count": 1, "next_page": null});
        let browser = Arc::new(FakeBrowser {
            result: Ok(repositories.clone()),
            calls: Default::default(),
        });
        let config = IntegrationConfig {
            github_repository_browser: Some(browser.clone()),
            ..Default::default()
        };
        let response = browse_repositories(&config, 123, 2, 17).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/json");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), format!("{repositories}\n").as_bytes());
        assert_eq!(*browser.calls.lock().unwrap(), vec![(123, 2, 17)]);

        let browser = Arc::new(FakeBrowser {
            result: Err(()),
            calls: Default::default(),
        });
        let config = IntegrationConfig {
            github_repository_browser: Some(browser.clone()),
            ..Default::default()
        };
        let failure = browse_repositories(&config, 123, 1, 100).await.unwrap_err();
        assert_eq!(
            (failure.0, failure.1.as_str()),
            (
                StatusCode::BAD_GATEWAY,
                "failed to list github repositories"
            )
        );
        assert_eq!(*browser.calls.lock().unwrap(), vec![(123, 1, 100)]);
        let failure = browse_repositories(&IntegrationConfig::default(), 123, 1, 100)
            .await
            .unwrap_err();
        assert_eq!(
            (failure.0, failure.1.as_str()),
            (
                StatusCode::BAD_GATEWAY,
                "failed to list github repositories"
            )
        );
    }

    struct FakeValidator(Result<String, VcsValidateError>);

    #[async_trait]
    impl VcsTokenValidator for FakeValidator {
        async fn validate(
            &self,
            provider: &str,
            instance_url: &str,
            token: &str,
        ) -> Result<String, VcsValidateError> {
            assert_eq!(
                (provider, instance_url, token),
                ("gitlab", "https://git.example", "token")
            );
            match &self.0 {
                Ok(account) => Ok(account.clone()),
                Err(VcsValidateError::Unauthorized) => Err(VcsValidateError::Unauthorized),
                Err(VcsValidateError::Unreachable) => Err(VcsValidateError::Unreachable),
            }
        }
    }

    #[tokio::test]
    async fn vcs_validator_success_and_errors() {
        let config = IntegrationConfig {
            vcs_token_validator: Some(Arc::new(FakeValidator(Ok("alice".to_owned())))),
            ..Default::default()
        };
        assert_eq!(
            validate_vcs(&config, "gitlab", "https://git.example", "token")
                .await
                .unwrap(),
            "alice"
        );
        for (kind, status, message) in [
            (
                VcsValidateError::Unauthorized,
                StatusCode::BAD_REQUEST,
                "the provider rejected the access token",
            ),
            (
                VcsValidateError::Unreachable,
                StatusCode::BAD_GATEWAY,
                "could not reach the provider instance",
            ),
        ] {
            let config = IntegrationConfig {
                vcs_token_validator: Some(Arc::new(FakeValidator(Err(kind)))),
                ..Default::default()
            };
            let failure = validate_vcs(&config, "gitlab", "https://git.example", "token")
                .await
                .unwrap_err();
            assert_eq!((failure.0, failure.1.as_str()), (status, message));
        }
        let failure = validate_vcs(
            &IntegrationConfig::default(),
            "gitlab",
            "https://git.example",
            "token",
        )
        .await
        .unwrap_err();
        assert_eq!(
            (failure.0, failure.1.as_str()),
            (
                StatusCode::BAD_GATEWAY,
                "could not reach the provider instance"
            )
        );
    }

    #[tokio::test]
    async fn coded_errors_are_go_json_once() {
        let response = coded_error(
            StatusCode::FORBIDDEN,
            "vcs_not_configured",
            "vcs integration not configured (MULTICA_VCS_SECRET_KEY unset)",
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["content-type"], "application/json");
        let length = response.headers()["content-length"]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.len(), length);
        assert_eq!(body.last(), Some(&b'\n'));
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "error": "vcs integration not configured (MULTICA_VCS_SECRET_KEY unset)",
                "code": "vcs_not_configured",
            })
        );
    }

    #[test]
    fn github_pagination() {
        let uri = Uri::from_static("/repositories");
        assert_eq!(page_param(&uri, "page", 1, 100_000).unwrap(), 1);
        assert_eq!(page_param(&uri, "per_page", 100, 100).unwrap(), 100);
        for (name, max) in [("page", 100_000), ("per_page", 100)] {
            for value in ["0", "-1", "abc", "1.5", "100001"] {
                let uri = Uri::from_str(&format!("/repositories?{name}={value}")).unwrap();
                let error =
                    page_param(&uri, name, if name == "page" { 1 } else { 100 }, max).unwrap_err();
                assert_eq!(error.0, StatusCode::BAD_REQUEST);
                assert_eq!(
                    error.1,
                    if name == "page" {
                        "invalid page"
                    } else {
                        "invalid per_page"
                    }
                );
            }
            let uri = Uri::from_str(&format!("/repositories?{name}={max}")).unwrap();
            assert_eq!(page_param(&uri, name, 1, max).unwrap(), max);
            let uri = Uri::from_str(&format!("/repositories?{name}=%20")).unwrap();
            assert_eq!(page_param(&uri, name, 1, max).unwrap(), 1);
        }
    }

    #[test]
    fn github_return_target() {
        assert!(allowed_return_to("github"));
        assert!(allowed_return_to("repositories"));
        for invalid in ["", "settings", "Repositories", "../repositories", "github/"] {
            assert!(!allowed_return_to(invalid));
        }
    }

    #[test]
    fn vcs_url_validation() {
        assert_eq!(
            normalize_instance_url(" https://git.example/a/// "),
            "https://git.example/a"
        );
        assert!(valid_instance_url("https://git.example/a"));
        assert!(valid_instance_url("http://localhost:3000"));
        for invalid in ["", "git.example", "ftp://git.example", "https:///", "/git"] {
            assert!(!valid_instance_url(invalid));
        }
    }

    #[test]
    fn vcs_webhook_location() {
        let id = Uuid::nil();
        assert_eq!(webhook_path(id), format!("/api/webhooks/vcs/{id}"));
        assert_eq!(
            webhook_url("https://example.com///", id),
            format!("https://example.com/api/webhooks/vcs/{id}")
        );
        assert_eq!(webhook_url("", id), "");
    }

    #[test]
    fn webhook_secret_has_32_hex_bytes() {
        let minted = mint_webhook_secret().unwrap();
        assert_eq!(minted.len(), 64);
        assert_eq!(hex::decode(minted).unwrap().len(), 32);
    }

    #[test]
    fn unconfigured_json_shapes() {
        assert_eq!(
            github_unconfigured(),
            json!({"url": "", "configured": false})
        );
        assert_eq!(
            github_installations_response(vec![], &IntegrationConfig::default(), false),
            json!({"installations": [], "configured": false, "repository_browse_configured": false, "can_manage": false})
        );
        assert_eq!(
            vcs_unavailable(),
            json!({"connections": [], "available": false, "configured": false, "can_manage": false})
        );
    }
}
