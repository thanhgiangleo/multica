//! Zalo BYO install. Provider verification only; no polling, sends or webhook mutation.
use super::channels_sprint::{self as channel, Channel, ChannelEvent, Params};
use super::channels_sprint_telegram::CredentialBox;
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json, value::RawValue};
use std::{net::IpAddr, time::Duration};
use uuid::Uuid;

/// Explicit runtime injection. Never reads environment credentials or provider endpoints.
pub struct ZaloInstaller {
    cipher: CredentialBox,
    http: reqwest::Client,
    origin: reqwest::Url,
}
impl ZaloInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Self::at_origin(
            key,
            reqwest::Url::parse("https://bot-api.zaloplatforms.com").unwrap(),
        )
    }
    /// Synthetic tests only. Production wiring must use new().
    pub fn for_loopback_tests(key: &[u8], origin: &str) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(origin).map_err(|_| "invalid test provider origin")?;
        let ip = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok());
        if url.scheme() != "http"
            || !ip.is_some_and(|ip| ip.is_loopback())
            || url.port().is_none()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("test provider must be an explicit loopback HTTP origin");
        }
        Self::at_origin(key, url)
    }
    fn at_origin(key: &[u8], origin: reqwest::Url) -> Result<Self, &'static str> {
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            origin,
            http: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .map_err(|_| "zalo HTTP client unavailable")?,
        })
    }
    async fn request<T: DeserializeOwned>(
        &self,
        token: &str,
        method: &str,
        allow_missing: bool,
    ) -> Result<Option<T>, ApiError> {
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .map_err(|_| unverifiable())?
            .clear()
            .push(&format!("bot{token}"))
            .push(method);
        // URLs and provider errors can contain credentials. Never propagate either.
        let mut response = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body("{}")
            .send()
            .await
            .map_err(|_| unverifiable())?;
        let status = response.status();
        if response.content_length().is_some_and(|n| n > 65536) {
            return Err(unverifiable());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unverifiable())? {
            if chunk.len() > 65536 - bytes.len() {
                return Err(unverifiable());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(rejected());
        }
        // A missing webhook record is allowed only after successful getMe.
        if allow_missing && status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| unverifiable())?;
        if !status.is_success() || !envelope.ok {
            if envelope.error_code == Some(401) {
                return Err(rejected());
            }
            if allow_missing && envelope.error_code == Some(404) && !status.is_redirection() {
                return Ok(None);
            }
            return Err(unverifiable());
        }
        let result = envelope.result.ok_or_else(unverifiable)?;
        serde_json::from_str(result.get())
            .map(Some)
            .map_err(|_| unverifiable())
    }
    async fn verify(&self, token: &str) -> Result<VerifiedBot, ApiError> {
        let bot: VerifiedBot = self
            .request(token, "getMe", false)
            .await?
            .ok_or_else(unverifiable)?;
        if bot.id.0.is_empty()
            || [&bot.id.0, &bot.display_name, &bot.account_name]
                .iter()
                .any(|s| s.contains(token))
        {
            return Err(unverifiable());
        }
        if let Some(webhook) = self
            .request::<WebhookInfo>(token, "getWebhookInfo", true)
            .await?
            && !webhook.url.is_empty()
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "this Zalo bot has a webhook configured — remove the webhook before connecting it with long polling",
            ));
        }
        Ok(bot)
    }
}
#[derive(Deserialize)]
struct Envelope {
    ok: bool,
    result: Option<Box<RawValue>>,
    error_code: Option<i64>,
}
#[derive(Deserialize)]
struct VerifiedBot {
    id: ZaloId,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    account_name: String,
}
#[derive(Deserialize)]
struct WebhookInfo {
    url: String,
}

// Capture integer lexemes before serde_json::Value can round them through f64.
struct ZaloId(String);
impl<'de> Deserialize<'de> for ZaloId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let text = raw.get();
        if text.starts_with('"') {
            serde_json::from_str::<String>(text)
                .map(Self)
                .map_err(serde::de::Error::custom)
        } else if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
            Ok(Self(text.into()))
        } else {
            Err(serde::de::Error::custom("invalid Zalo identifier"))
        }
    }
}
fn rejected() -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        "Zalo rejected this bot token — verify bot token and try again",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not reach Zalo to verify this bot — check the server network or proxy and try again; the token was not saved",
    )
}
fn validate_token(token: &str) -> Result<(), ApiError> {
    if token.is_empty()
        || token.len() > 4096
        || token
            .chars()
            .any(|c| c <= ' ' || c == '\u{7f}' || matches!(c, '/' | '\\' | '?' | '#'))
    {
        return Err(error(StatusCode::BAD_REQUEST, "zalo: invalid bot token"));
    }
    Ok(())
}
#[derive(Deserialize)]
pub(super) struct RegisterBody {
    bot_token: String,
}
pub(super) async fn register(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    Query(query): Query<Params>,
    body: Result<Json<RegisterBody>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let workspace = channel::workspace_gate(&state, &actor, user, &params, true).await?;
    actor.require_human()?;
    let rt = channel::runtime_for(&state, Channel::Zalo, channel::Operation::Install)?;
    let installer = rt
        .zalo_installer()
        .ok_or_else(|| channel::disabled(Channel::Zalo, channel::Operation::Install))?;

    let agent_id = query.get("agent_id").map_or("", String::as_str).trim();
    if agent_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "agent_id is required"));
    }
    let agent = channel::uuid_param(agent_id, "invalid agent_id")?;
    {
        let db = state
            .pool
            .get()
            .await
            .map_err(|_| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))?;
        channel::active_agent_in_workspace(&**db, workspace, agent).await?;
    }
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let token = body.bot_token.trim();
    validate_token(token)?;
    {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel database unavailable",
            )
        })?;
        channel::authorize_install(&**db, workspace, agent, user, true).await?;
    }
    let bot = installer.verify(token).await?;
    let encrypted = installer.cipher.seal(token.as_bytes()).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "channel encryption failed",
        )
    })?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channel::db_error)?;
    for key in [
        format!("zalo-agent:{workspace}:{agent}"),
        format!("zalo-bot:{}", bot.id.0),
    ] {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='zalo' FOR UPDATE", &[&workspace,&agent]).await.map_err(channel::db_error)?;
    channel::authorize_install(&*tx, workspace, agent, user, true).await?;
    let mut share_link = String::new();
    let mut generation = Uuid::new_v4().to_string();
    if let Some(row) = &prior {
        let old: Value = row.get("config");
        if old.get("app_id").and_then(Value::as_str) != Some(bot.id.0.as_str()) {
            return Err(error(
                StatusCode::CONFLICT,
                "Zalo bot replacement requires bot-scoped cleanup; existing installation was not changed",
            ));
        }
        share_link = old
            .get("share_link")
            .and_then(Value::as_str)
            .unwrap_or("")
            .into();
        if row.get::<_, String>("status") == "active" {
            // Legacy epochs need coordinated consumer migration; never silently change them.
            generation = old.get("generation").and_then(Value::as_str)
                .filter(|s| Uuid::parse_str(s).is_ok_and(|id| id.to_string() == *s))
                .ok_or_else(|| error(StatusCode::CONFLICT, "Zalo legacy installation requires generation migration; existing installation was not changed"))?.into();
        } else {
            // No reconnect until consumers fence pre-revoke work by installation epoch.
            return Err(error(
                StatusCode::CONFLICT,
                "Zalo reconnect requires installation-generation delivery fences; existing installation was not changed",
            ));
        }
    }
    let owner = tx.query_opt("SELECT id FROM channel_installation WHERE channel_type='zalo' AND config->>'app_id'=$1 FOR UPDATE", &[&bot.id.0]).await.map_err(channel::db_error)?;
    if let Some(owner) = owner
        && prior.as_ref().map(|r| r.get::<_, Uuid>("id")) != Some(owner.get("id"))
    {
        return Err(error(
            StatusCode::CONFLICT,
            "this Zalo bot belongs to another installation; reassignment requires bot-scoped cleanup, which is not yet supported",
        ));
    }
    let config = json!({"app_id":bot.id.0,"bot_name":bot.display_name,"account_name":bot.account_name,"bot_token_encrypted":encrypted,"generation":generation,"share_link":share_link});
    let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'zalo',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,status='active',installed_at=now(),updated_at=now() RETURNING *", &[&workspace,&agent,&config,&user]).await.map_err(channel::db_error)?;
    let result = channel::installation_json(&row, Channel::Zalo);
    let installation = row.get("id");
    tx.commit().await.map_err(channel::db_error)?;
    rt.publish(ChannelEvent {
        event_type: "zalo_installation:created".into(),
        workspace_id: workspace,
        actor_id: user,
        installation_id: installation,
    });
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn channels_sprint_zalo_provider_contract_bounds_and_redaction() {
        use axum::{Router, body::Body, extract::State, http::Request, response::Response};
        use std::sync::{Arc, Mutex};
        type Reply = Arc<Mutex<(String, StatusCode, String, bool, Option<String>)>>;
        async fn reply(State(state): State<Reply>, req: Request<Body>) -> Response {
            assert_eq!(req.method(), "POST");
            assert_eq!(req.headers()["content-type"], "application/json");
            assert!(req.uri().query().is_none());
            let method = req.uri().path().rsplit('/').next().unwrap().to_string();
            assert!(
                ["getMe", "getWebhookInfo"].contains(&method.as_str()),
                "never follow redirects or send messages"
            );
            let bytes = axum::body::to_bytes(req.into_body(), 16).await.unwrap();
            assert_eq!(&bytes[..], b"{}");
            let (target, status, body, chunked, location) = state.lock().unwrap().clone();
            let (status, body, chunked, location) = if method == target {
                (status, body, chunked, location)
            } else {
                (
                    StatusCode::OK,
                    if method == "getMe" {
                        r#"{"ok":true,"result":{"id":123456789012345678901234567890,"display_name":"Synthetic Bot"}}"#.into()
                    } else {
                        r#"{"ok":true,"result":{"url":""}}"#.into()
                    },
                    false,
                    None,
                )
            };
            let mut response = Response::builder().status(status);
            if let Some(location) = location {
                response = response.header("location", location);
            }
            response
                .body(if chunked {
                    Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::io::Error>(vec![b'x'; 40000]),
                        Ok(vec![b'x'; 40000]),
                    ]))
                } else {
                    Body::from(body)
                })
                .unwrap()
        }
        let state: Reply = Arc::new(Mutex::new((
            String::new(),
            StatusCode::OK,
            String::new(),
            false,
            None,
        )));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(reply).with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let installer = ZaloInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let token = "synthetic:private%73";
        assert_eq!(
            installer.verify(token).await.unwrap().id.0,
            "123456789012345678901234567890"
        );
        for (method, status, body, expected) in [
            (
                "getMe",
                StatusCode::UNAUTHORIZED,
                "not JSON".into(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "getMe",
                StatusCode::OK,
                json!({"ok":false,"error_code":401,"description":token}).to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "getMe",
                StatusCode::NOT_FOUND,
                json!({"ok":false,"error_code":404}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getMe",
                StatusCode::OK,
                "not JSON".into(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getMe",
                StatusCode::OK,
                json!({"ok":true,"result":{"id":1.5}}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getMe",
                StatusCode::OK,
                json!({"ok":true,"result":{"id":""}}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getMe",
                StatusCode::OK,
                json!({"ok":true,"result":{"id":"123","display_name":token}}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getMe",
                StatusCode::OK,
                "x".repeat(65537),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getWebhookInfo",
                StatusCode::OK,
                json!({"ok":true,"result":{"url":"https://example.invalid/webhook"}}).to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "getWebhookInfo",
                StatusCode::OK,
                json!({"ok":true,"result":{}}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                "getWebhookInfo",
                StatusCode::OK,
                json!({"ok":false,"error_code":401,"description":token}).to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "getWebhookInfo",
                StatusCode::BAD_GATEWAY,
                json!({"ok":false,"error_code":502,"description":token}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            *state.lock().unwrap() = (method.into(), status, body, false, None);
            let err = installer
                .verify(token)
                .await
                .err()
                .expect("invalid provider result");
            assert_eq!(err.0, expected, "{method}");
            assert!(!err.1.contains(token));
            assert!(!err.1.contains(&origin));
        }
        for (status, body) in [
            (StatusCode::NOT_FOUND, "missing".into()),
            (
                StatusCode::OK,
                json!({"ok":false,"error_code":404}).to_string(),
            ),
        ] {
            *state.lock().unwrap() = ("getWebhookInfo".into(), status, body, false, None);
            assert!(installer.verify(token).await.is_ok());
        }
        *state.lock().unwrap() = (
            "getMe".into(),
            StatusCode::TEMPORARY_REDIRECT,
            String::new(),
            false,
            Some(format!("{origin}/credential-leak")),
        );
        assert_eq!(
            installer.verify(token).await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        *state.lock().unwrap() = ("getMe".into(), StatusCode::OK, String::new(), true, None);
        assert_eq!(
            installer.verify(token).await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        server.abort();
        let _ = server.await;
        assert_eq!(
            installer.verify(token).await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    #[test]
    fn channels_sprint_zalo_ids_tokens_and_origins() {
        for (raw, expected) in [
            (
                "123456789012345678901234567890",
                "123456789012345678901234567890",
            ),
            ("\"12345678901234567890\"", "12345678901234567890"),
            ("0", "0"),
        ] {
            assert_eq!(serde_json::from_str::<ZaloId>(raw).unwrap().0, expected);
        }
        for raw in ["-1", "1.2", "1e20", "null", "true", "{}"] {
            assert!(serde_json::from_str::<ZaloId>(raw).is_err());
        }
        assert!(validate_token("synthetic:token%value").is_ok());
        for token in [
            "",
            "abc/def",
            "abc\\def",
            "abc?def",
            "abc#def",
            "abc def",
            "abc\n",
            "abc\u{7f}",
        ] {
            assert!(validate_token(token).is_err());
        }
        assert!(validate_token(&"x".repeat(4097)).is_err());
        for origin in [
            "https://127.0.0.1:1234",
            "http://example.com:1234",
            "http://localhost:1234",
            "http://127.0.0.1",
            "http://127.0.0.1:1234/path",
            "http://user@127.0.0.1:1234",
            "http://127.0.0.1:1234/?x",
            "http://127.0.0.1:1234/#x",
        ] {
            assert!(ZaloInstaller::for_loopback_tests(&[73; 32], origin).is_err());
        }
    }
}
