//! P0 session endpoints and anonymous app config. The integrator mounts the
//! authenticated router before `authenticate` and the public router after it.
use super::{
    Actor, ApiError, ApiState, Claims, GoJson, Identity, credential, decode_session_claims, error,
    random_nonce, renew_session, temporarily_disabled,
};
use axum::{
    Router,
    extract::{Extension, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::SET_COOKIE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, SecondsFormat, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn authenticated_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/auth/refresh", post(refresh))
        .route("/api/cli-token", post(cli_token))
}

/// Inputs which the current ApiState does not hold. Wire these from the real
/// startup config/storage/feature-flag service; never infer private flags from
/// request headers or silently report a capable deployment as incapable.
#[derive(Clone)]
pub(super) struct PublicConfig {
    pub cdn_domain: String,
    pub attachment_public_url: String,
    pub cdn_signed: bool,
    pub vcs_integration_available: bool,
    pub server_version: String,
    /// Evaluate on every request, like Go's FeatureFlags service. The callback
    /// must return only these three frontend-safe decisions, never raw rules.
    pub feature_flags: Arc<dyn Fn() -> PublicFeatureFlags + Send + Sync>,
}

pub(super) struct PublicFeatureFlags {
    pub billing_workspace_subscriptions: bool,
    pub composio_mcp_apps: bool,
    pub plugins_v1: bool,
}

impl PublicConfig {
    pub fn from_state(state: &ApiState) -> Self {
        Self {
            cdn_domain: env("CDN_DOMAIN"),
            attachment_public_url: state.files_public_url.clone().unwrap_or_default(),
            cdn_signed: false,
            vcs_integration_available: false,
            server_version: env("MULTICA_SERVER_VERSION"),
            feature_flags: Arc::new(|| PublicFeatureFlags {
                billing_workspace_subscriptions: enabled_env_flag(
                    "BILLING_WORKSPACE_SUBSCRIPTIONS",
                ),
                composio_mcp_apps: enabled_env_flag("COMPOSIO_MCP_APPS"),
                plugins_v1: enabled_env_flag("PLUGINS_V1"),
            }),
        }
    }
}

pub(super) fn enabled_env_flag(key: &str) -> bool {
    let Ok(raw) = std::env::var(format!("FF_{key}")) else {
        return false;
    };
    let raw = raw.trim().to_ascii_lowercase();
    match raw.as_str() {
        "" | "false" | "off" | "0" | "no" => false,
        "true" | "on" | "1" | "yes" => true,
        _ if raw.ends_with('%') => raw[..raw.len() - 1].parse::<u8>().ok() == Some(100),
        _ => true,
    }
}

pub(super) fn public_routes(config: PublicConfig) -> Router<ApiState> {
    Router::new().route("/auth/logout", post(logout)).route(
        "/api/config",
        get(move || {
            let config = config.clone();
            async move { GoJson(app_config(&config)) }
        }),
    )
}

#[derive(Serialize)]
struct RefreshResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    expires_at: String,
    renewed: bool,
    check_again_in_seconds: i64,
}

fn renewal_interval(ttl: i64) -> i64 {
    (ttl / 10).clamp(5, 7 * 24 * 3600).min(ttl / 4)
}

fn expiry(exp: usize) -> String {
    i64::try_from(exp)
        .ok()
        .and_then(|exp| DateTime::<Utc>::from_timestamp(exp, 0))
        .map(|date| date.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}

fn refresh_answer(claims: &Claims, ttl: i64) -> RefreshResponse {
    RefreshResponse {
        token: None,
        expires_at: expiry(claims.exp),
        renewed: false,
        check_again_in_seconds: renewal_interval(ttl),
    }
}

async fn refresh(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // The middleware already authenticated the caller and enforced cookie CSRF.
    // Re-read the raw token in exactly its bearer-first order: a PAT or task
    // credential must never be upgraded into an interactive JWT.
    let (raw, from_cookie) = credential(&headers).ok_or_else(|| {
        error(
            StatusCode::BAD_REQUEST,
            "only interactive sessions can be refreshed",
        )
    })?;
    let claims = decode_session_claims(&raw, &state.jwt_secret).map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "only interactive sessions can be refreshed",
        )
    })?;
    if claims.sub != user.to_string() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "token does not belong to caller",
        ));
    }
    // The auth middleware rejects shadow users, but this second lookup mirrors
    // the Go handler and fences a shadow conversion between the two checks.
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to look up user"))?;
    let row = db
        .query_opt("SELECT zbs_is_shadow FROM \"user\" WHERE id = $1", &[&user])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to look up user"))?
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "user not found"))?;
    if row.get::<_, bool>(0) {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    drop(db);
    let mut answer = refresh_answer(&claims, state.session.ttl_seconds);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok());
    let remaining = now.and_then(|now| {
        i64::try_from(claims.exp)
            .ok()
            .and_then(|exp| exp.checked_sub(now))
    });
    if !remaining.is_some_and(|n| n > 0 && n <= state.session.ttl_seconds / 2) {
        return Ok(GoJson(answer).into_response());
    }
    if temporarily_disabled(user, &claims.email) {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    // Cookie refresh uses the same cookie minting path as middleware's safe
    // requests. A bearer refresh must not emit cookies or leak a browser token.
    if from_cookie {
        if let Some(cookies) = renew_session(&claims, &state) {
            // renew_session creates the new cookie JWT. Read its expiry for the
            // response rather than guessing from a second clock sample.
            if let Some(token) = cookies
                .first()
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("multica_auth="))
                .and_then(|value| value.split(';').next())
                && let Ok(next) = decode_session_claims(token, &state.jwt_secret)
            {
                answer.expires_at = expiry(next.exp);
                answer.renewed = true;
                let mut response = GoJson(answer).into_response();
                for cookie in cookies {
                    response.headers_mut().append(SET_COOKIE, cookie);
                }
                return Ok(response);
            }
        }
    } else if let Some((token, exp)) = renewed_bearer(&state, &claims, now.unwrap_or_default()) {
        answer.expires_at = expiry(exp);
        answer.token = Some(token);
        answer.renewed = true;
    }
    Ok(GoJson(answer).into_response())
}

fn renewed_bearer(state: &ApiState, claims: &Claims, now: i64) -> Option<(String, usize)> {
    let sid = if claims.sid.is_empty() {
        hex::encode(random_nonce().ok()?)
    } else {
        claims.sid.clone()
    };
    let exp = now.checked_add(state.session.ttl_seconds)?;
    let mut payload = json!({"sub": claims.sub, "exp": exp, "iat": now, "sid": sid});
    if !claims.email.is_empty() {
        payload["email"] = json!(claims.email);
    }
    if claims.name.as_str().is_some_and(|name| !name.is_empty()) {
        payload["name"] = claims.name.clone();
    }
    let token = encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(&state.jwt_secret),
    )
    .ok()?;
    Some((token, usize::try_from(exp).ok()?))
}

async fn cli_token(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<serde_json::Value>, ApiError> {
    actor.require_human()?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "user not found"))?;
    let row = db
        .query_opt(
            "SELECT email, name, zbs_is_shadow FROM \"user\" WHERE id = $1",
            &[&user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "user not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user not found"))?;
    let email: String = row.get("email");
    let name: String = row.get("name");
    if row.get::<_, bool>("zbs_is_shadow") || temporarily_disabled(user, &email) {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    let now = Utc::now().timestamp();
    let exp = now.checked_add(state.session.ttl_seconds).ok_or_else(|| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate token",
        )
    })?;
    let sid = random_nonce().map(hex::encode).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate token",
        )
    })?;
    let payload = json!({"sub": user.to_string(), "email": email, "name": name, "sid": sid, "exp": exp, "iat": now});
    let token = encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(&state.jwt_secret),
    )
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate token",
        )
    })?;
    Ok(GoJson(json!({"token": token})))
}

async fn logout(State(state): State<ApiState>) -> Response {
    // Go's ClearAuthCookies is deliberately public: it does not revoke the
    // bearer JWT or any PAT and does not run the CSRF/auth middleware.
    let mut response = GoJson(json!({"message": "logged out"})).into_response();
    for (name, http_only) in [
        ("multica_auth", true),
        ("multica_csrf", false),
        ("multica_csrf_session", false),
    ] {
        let domain = if state.session.domain.is_empty() {
            String::new()
        } else {
            format!("; Domain={}", state.session.domain)
        };
        let raw = format!(
            "{name}=; Path=/{domain}; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; SameSite=Strict{}{}",
            if http_only { "; HttpOnly" } else { "" },
            if state.session.secure { "; Secure" } else { "" },
        );
        if let Ok(value) = HeaderValue::from_str(&raw) {
            response.headers_mut().append(SET_COOKIE, value);
        }
    }
    response
}

#[derive(Serialize)]
struct AppConfig {
    cdn_domain: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    attachment_public_url: String,
    #[serde(skip_serializing_if = "is_false")]
    cdn_signed: bool,
    allow_signup: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    google_client_id: String,
    #[serde(skip_serializing_if = "is_false")]
    workspace_creation_disabled: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    daemon_server_url: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    daemon_app_url: String,
    daemon_setup_target: &'static str,
    #[serde(skip_serializing_if = "is_false")]
    vcs_integration_available: bool,
    posthog_key: String,
    posthog_host: String,
    analytics_environment: String,
    feature_flags: BTreeMap<&'static str, bool>,
    local_worktree_supported: bool,
    agent_conversation_starters_supported: bool,
    issue_create_properties_supported: bool,
    comment_delete_keep_replies_supported: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    server_version: String,
}

fn is_false(value: &bool) -> bool {
    !value
}
fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}
fn normalize(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_owned()
}
fn app_url() -> String {
    let primary = normalize(&env("MULTICA_APP_URL"));
    if primary.is_empty() {
        normalize(&env("FRONTEND_ORIGIN"))
    } else {
        primary
    }
}
fn official_cloud(url: &str) -> bool {
    let url = if url.contains("://") {
        url.to_owned()
    } else {
        format!("https://{url}")
    };
    reqwest::Url::parse(&url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .is_some_and(|host| {
            host.trim_end_matches('.')
                .eq_ignore_ascii_case("multica.ai")
        })
}
fn analytics_environment() -> String {
    for raw in [env("ANALYTICS_ENVIRONMENT"), env("APP_ENV")] {
        match raw.trim().to_ascii_lowercase().as_str() {
            "production" | "prod" => return "production".into(),
            "staging" | "stage" => return "staging".into(),
            "development" | "dev" | "test" | "local" => return "dev".into(),
            _ => {}
        }
    }
    "dev".into()
}
fn app_config(source: &PublicConfig) -> AppConfig {
    let flags = (source.feature_flags)();
    let app = app_url();
    let cloud = official_cloud(&app);
    let mut server = normalize(&env("MULTICA_DAEMON_SERVER_URL"));
    if server.is_empty() {
        server = normalize(&env("MULTICA_PUBLIC_URL"));
    }
    let (daemon_server_url, daemon_app_url) = if cloud || app.is_empty() {
        (String::new(), String::new())
    } else {
        (
            if server.is_empty() {
                app.clone()
            } else {
                server
            },
            app,
        )
    };
    let disabled = matches!(env("ANALYTICS_DISABLED").as_str(), "true" | "1");
    let posthog_key = if disabled {
        String::new()
    } else {
        env("POSTHOG_API_KEY")
    };
    let posthog_host = if disabled {
        String::new()
    } else {
        let host = env("POSTHOG_HOST");
        if host.is_empty() && !posthog_key.is_empty() {
            "https://us.i.posthog.com".into()
        } else {
            host
        }
    };
    AppConfig {
        cdn_domain: source.cdn_domain.clone(),
        attachment_public_url: source.attachment_public_url.clone(),
        cdn_signed: source.cdn_signed,
        allow_signup: env("ALLOW_SIGNUP") != "false",
        google_client_id: env("GOOGLE_CLIENT_ID"),
        workspace_creation_disabled: env("DISABLE_WORKSPACE_CREATION") == "true",
        daemon_server_url,
        daemon_app_url,
        daemon_setup_target: if cloud { "cloud" } else { "self_host" },
        vcs_integration_available: source.vcs_integration_available,
        posthog_key,
        posthog_host,
        analytics_environment: if disabled {
            String::new()
        } else {
            analytics_environment()
        },
        feature_flags: BTreeMap::from([
            (
                "billing_workspace_subscriptions",
                flags.billing_workspace_subscriptions,
            ),
            ("composio_mcp_apps", flags.composio_mcp_apps),
            ("plugins_v1", flags.plugins_v1),
            ("agents_agent_builder", true),
            ("agents_skill_toggles", true),
            ("settings_resource_labels", true),
            ("project_workflows_v1", true),
        ]),
        local_worktree_supported: true,
        agent_conversation_starters_supported: true,
        issue_create_properties_supported: true,
        comment_delete_keep_replies_supported: true,
        server_version: if cloud {
            String::new()
        } else {
            source.server_version.clone()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    #[test]
    fn renewal_cadence_matches_go_bounds() {
        assert_eq!(renewal_interval(30 * 24 * 3600), 3 * 24 * 3600);
        assert_eq!(renewal_interval(60), 6);
        assert_eq!(renewal_interval(100 * 24 * 3600), 7 * 24 * 3600);
    }
    #[test]
    fn cloud_hostname_does_not_accept_subdomains() {
        assert!(official_cloud("https://MULTICA.AI./"));
        assert!(official_cloud("multica.ai"));
        assert!(!official_cloud("https://api.multica.ai"));
        assert!(!official_cloud("https://multica.ai.evil.test"));
    }
    #[test]
    fn config_has_public_capability_flags() {
        let config = app_config(&PublicConfig {
            cdn_domain: String::new(),
            attachment_public_url: String::new(),
            cdn_signed: false,
            vcs_integration_available: false,
            server_version: String::new(),
            feature_flags: Arc::new(|| PublicFeatureFlags {
                billing_workspace_subscriptions: false,
                composio_mcp_apps: false,
                plugins_v1: false,
            }),
        });
        assert_eq!(
            config.feature_flags.get("agents_agent_builder"),
            Some(&true)
        );
        assert_eq!(config.feature_flags.get("plugins_v1"), Some(&false));
        assert!(config.local_worktree_supported);
        assert!(config.issue_create_properties_supported);
    }
    #[test]
    fn config_evaluates_feature_flags_again_on_each_request() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let enabled = Arc::new(AtomicBool::new(false));
        let current = enabled.clone();
        let source = PublicConfig {
            cdn_domain: String::new(),
            attachment_public_url: String::new(),
            cdn_signed: false,
            vcs_integration_available: false,
            server_version: String::new(),
            feature_flags: Arc::new(move || PublicFeatureFlags {
                billing_workspace_subscriptions: current.load(Ordering::Relaxed),
                composio_mcp_apps: false,
                plugins_v1: false,
            }),
        };
        assert!(!app_config(&source).feature_flags["billing_workspace_subscriptions"]);
        enabled.store(true, Ordering::Relaxed);
        assert!(app_config(&source).feature_flags["billing_workspace_subscriptions"]);
    }
    #[test]
    fn response_never_includes_bearer_on_unrenewed_session() {
        let claims = Claims {
            sub: Uuid::nil().to_string(),
            email: String::new(),
            name: json!(null),
            sid: String::new(),
            exp: 1_800_000_000,
        };
        let body = serde_json::to_value(refresh_answer(&claims, 60)).unwrap();
        assert_eq!(body["renewed"], false);
        assert!(body.get("token").is_none());
        assert_eq!(body["check_again_in_seconds"], 6);
    }

    fn state() -> ApiState {
        let db_config = "host=127.0.0.1 port=1 dbname=unused".parse().unwrap();
        let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
            db_config,
            tokio_postgres::NoTls,
        ))
        .build()
        .unwrap();
        ApiState::new(Arc::new(pool), b"unit-test-secret".to_vec())
    }

    #[tokio::test]
    async fn public_logout_clears_both_csrf_cookies_without_auth_or_db() {
        let mut state = state();
        state.session.domain = "example.test".into();
        state.session.secure = true;
        let response = logout(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let values: Vec<_> = response
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(values.len(), 3);
        for name in ["multica_auth", "multica_csrf", "multica_csrf_session"] {
            let cookie = values
                .iter()
                .find(|v| v.starts_with(&format!("{name}=")))
                .unwrap();
            assert!(cookie.contains("; Domain=example.test"));
            assert!(cookie.contains("; Max-Age=0"));
            assert!(cookie.contains("; SameSite=Strict"));
            assert!(cookie.contains("; Secure"));
            assert_eq!(cookie.contains("; HttpOnly"), name == "multica_auth");
        }
    }

    #[test]
    fn bearer_renewal_keeps_session_identity_without_cookie() {
        let state = state();
        let now = Utc::now().timestamp();
        let claims = Claims {
            sub: Uuid::new_v4().to_string(),
            email: "a@example.test".into(),
            name: json!("A"),
            sid: "stable".into(),
            exp: usize::try_from(now + 300).unwrap(),
        };
        let (token, exp) = renewed_bearer(&state, &claims, now).unwrap();
        let parsed = decode_session_claims(&token, &state.jwt_secret).unwrap();
        assert_eq!(parsed.sub, claims.sub);
        assert_eq!(parsed.sid, claims.sid);
        assert_eq!(parsed.email, claims.email);
        assert_eq!(parsed.name, claims.name);
        assert_eq!(parsed.exp, exp);
    }

    #[tokio::test]
    async fn cli_token_rejects_machine_actor_before_db_lookup() {
        let error = cli_token(
            State(state()),
            Extension(Identity(Uuid::new_v4())),
            Extension(Actor::Cloud),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert_eq!(error.1, "this endpoint is only available to human actors");
    }

    #[tokio::test]
    async fn refresh_refuses_machine_credentials_before_db_lookup() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer mat_not-a-session".parse().unwrap());
        let error = refresh(State(state()), Extension(Identity(Uuid::new_v4())), headers)
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1, "only interactive sessions can be refreshed");
    }
}
