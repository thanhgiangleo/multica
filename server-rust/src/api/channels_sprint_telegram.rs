//! Telegram BYO install and opt-in, connection-pinned polling ingress.

#[path = "channels_sprint_telegram_admit.rs"]
pub mod admission;
#[path = "channels_sprint_telegram_notice.rs"]
pub mod notice;
#[path = "channels_sprint_telegram_pair.rs"]
pub mod pairing;
#[path = "channels_sprint_telegram_poll.rs"]
pub mod polling;
#[path = "channels_sprint_telegram_reply.rs"]
pub mod reply;
use super::channels_sprint::{self as channel, Channel, ChannelEvent, Params};
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{net::IpAddr, time::Duration};
use uuid::Uuid;

/// Go secretbox wire format: base64(nonce[12] || AES-256-GCM ciphertext || tag[16]).
/// Key/ciphertext/plaintext deliberately have no Debug or Serialize implementation.
pub struct CredentialBox(LessSafeKey);
impl CredentialBox {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        UnboundKey::new(&aead::AES_256_GCM, key)
            .map(|key| Self(LessSafeKey::new(key)))
            .map_err(|_| "channel encryption key must be 32 bytes")
    }
    pub fn seal(&self, plaintext: &[u8]) -> Result<String, &'static str> {
        let mut nonce = [0; 12];
        getrandom::fill(&mut nonce).map_err(|_| "channel encryption unavailable")?;
        let mut encrypted = plaintext.to_vec();
        self.0
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut encrypted,
            )
            .map_err(|_| "channel encryption failed")?;
        let mut sealed = nonce.to_vec();
        sealed.extend_from_slice(&encrypted);
        Ok(STANDARD.encode(sealed))
    }
    pub fn open(&self, sealed: &str) -> Result<Vec<u8>, &'static str> {
        let mut bytes = STANDARD
            .decode(sealed)
            .map_err(|_| "invalid channel ciphertext")?;
        if bytes.len() < 28 {
            return Err("invalid channel ciphertext");
        }
        let nonce: [u8; 12] = bytes[..12]
            .try_into()
            .map_err(|_| "invalid channel ciphertext")?;
        let plaintext = self
            .0
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut bytes[12..],
            )
            .map_err(|_| "invalid channel ciphertext")?;
        Ok(plaintext.to_vec())
    }
}

/// Explicitly injected by the runtime. No default key or environment credential lookup.
pub struct TelegramInstaller {
    cipher: CredentialBox,
    http: reqwest::Client,
    origin: String,
}
impl TelegramInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Self::at_origin(key, "https://api.telegram.org")
    }
    /// Synthetic provider tests only; arbitrary hosts, credentials and paths are rejected.
    /// Production wiring must use new(), never this loopback-only constructor.
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
        Self::at_origin(key, origin.trim_end_matches('/'))
    }
    fn at_origin(key: &[u8], origin: &str) -> Result<Self, &'static str> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| "telegram HTTP client unavailable")?;
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            http,
            origin: origin.into(),
        })
    }
    async fn request(&self, token: &str, method: &str) -> Result<Value, ApiError> {
        // Do not propagate reqwest errors: URLs contain credentials. Provider descriptions
        // are untrusted and can echo a token. Neither belongs in logs or API errors.
        let mut response = self
            .http
            .post(format!("{}/bot{token}/{method}", self.origin))
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
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| unverifiable())?;
        if !status.is_success() || envelope.get("ok") != Some(&Value::Bool(true)) {
            if envelope.get("ok") == Some(&Value::Bool(false))
                && envelope.get("error_code").and_then(Value::as_u64) == Some(401)
            {
                return Err(rejected());
            }
            return Err(unverifiable());
        }
        envelope.get("result").cloned().ok_or_else(unverifiable)
    }
    async fn verify(&self, token: &str, bot_id: &str) -> Result<String, ApiError> {
        let me = self.request(token, "getMe").await?;
        let username = me
            .get("username")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(rejected)?;
        // Stronger than frozen Go: the authenticated bot must match the routing key.
        if me.get("is_bot") != Some(&Value::Bool(true))
            || me
                .get("id")
                .and_then(Value::as_i64)
                .map(|n| n.to_string())
                .as_deref()
                != Some(bot_id)
        {
            return Err(rejected());
        }
        let webhook = self.request(token, "getWebhookInfo").await?;
        let url = webhook
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(unverifiable)?;
        if !url.is_empty() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "this Telegram bot has a webhook configured — remove the webhook before connecting it with long polling",
            ));
        }
        Ok(username.into())
    }
}
fn rejected() -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        "Telegram rejected this bot token — generate a current token in @BotFather and try again",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not verify this Telegram bot — check the server network and try again; the token was not saved",
    )
}
fn parse_token(token: &str) -> Result<&str, ApiError> {
    let invalid = || {
        error(
            StatusCode::BAD_REQUEST,
            "telegram: bot token must look like 123456:ABC-DEF…",
        )
    };
    let (id, secret) = token.split_once(':').ok_or_else(invalid)?;
    // Bound URL length; reject path/control characters instead of interpolating them.
    if id.is_empty()
        || !id.bytes().all(|b| b.is_ascii_digit())
        || id.starts_with('0')
        || id.parse::<i64>().is_err()
        || secret.is_empty()
        || token.len() > 1024
        || !secret
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(invalid());
    }
    Ok(id)
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
    let rt = channel::runtime_for(&state, Channel::Telegram, channel::Operation::Install)?;
    let installer = rt
        .telegram_installer()
        .ok_or_else(|| channel::disabled(Channel::Telegram, channel::Operation::Install))?;

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
    let bot_id = parse_token(token)?;
    // Preflight auth before provider traffic. Recheck under row locks after network I/O.
    let snapshot = {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel database unavailable",
            )
        })?;
        channel::authorize_install(&**db, workspace, agent, user, true).await?;
        db.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='telegram'", &[&workspace,&agent]).await.map_err(channel::db_error)?
            .map(|r| (r.get::<_,Uuid>("id"),r.get::<_,String>("status"),r.get::<_,Value>("config")))
    };
    let username = installer.verify(token, bot_id).await?;
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
    // Serialize contested routing slots and repeated installs before acquiring row locks.
    for key in [
        format!("telegram-agent:{workspace}:{agent}"),
        format!("telegram-bot:{bot_id}"),
    ] {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='telegram' FOR UPDATE", &[&workspace,&agent]).await.map_err(channel::db_error)?;
    channel::authorize_install(&*tx, workspace, agent, user, true).await?;
    let current = prior.as_ref().map(|r| {
        (
            r.get::<_, Uuid>("id"),
            r.get::<_, String>("status"),
            r.get::<_, Value>("config"),
        )
    });
    if current != snapshot {
        return Err(error(
            StatusCode::CONFLICT,
            "Telegram installation changed or was revoked during verification; existing installation was not changed",
        ));
    }
    if prior.as_ref().is_some_and(|row| {
        row.get::<_, Value>("config")
            .get("app_id")
            .and_then(Value::as_str)
            != Some(bot_id)
    }) {
        // Swapping bots requires fenced task/route cleanup. Never carry old identities
        // into a new bot namespace while that integrator boundary is missing.
        return Err(error(
            StatusCode::CONFLICT,
            "Telegram bot replacement requires bot-scoped cleanup; existing installation was not changed",
        ));
    }
    let owner = tx.query_opt("SELECT ci.id,ci.workspace_id,a.archived_at FROM channel_installation ci LEFT JOIN agent a ON a.id=ci.agent_id WHERE ci.channel_type='telegram' AND ci.config->>'app_id'=$1 FOR UPDATE OF ci", &[&bot_id]).await.map_err(channel::db_error)?;
    if let Some(owner) = owner
        && prior.as_ref().map(|row| row.get::<_, Uuid>("id")) != Some(owner.get("id"))
    {
        let message = if owner.get::<_, Uuid>("workspace_id") != workspace {
            "this Telegram bot belongs to another workspace installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        } else if owner
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
        {
            "this Telegram bot belongs to an archived agent installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        } else {
            "this Telegram bot belongs to another agent installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        };
        return Err(error(StatusCode::CONFLICT, message));
    }
    if let Some(prior) = &prior {
        let installation: Uuid = prior.get("id");
        tx.execute("DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NULL", &[&installation]).await.map_err(channel::db_error)?;
        tx.execute(
            "DELETE FROM channel_pairing_notice WHERE installation_id=$1 AND workspace_id=$2",
            &[&installation, &workspace],
        )
        .await
        .map_err(channel::db_error)?;
        tx.execute(
            "DELETE FROM channel_binding_token WHERE installation_id=$1 AND consumed_at IS NULL",
            &[&installation],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let config = json!({"app_id":bot_id,"bot_username":username,"bot_token_encrypted":encrypted,"generation":Uuid::new_v4().to_string()});
    let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'telegram',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,status='active',installed_at=now(),updated_at=now() RETURNING *", &[&workspace,&agent,&config,&user]).await.map_err(channel::db_error)?;
    let installation = row.get("id");
    let result = channel::installation_json(&row, Channel::Telegram);
    tx.commit().await.map_err(channel::db_error)?;
    rt.publish(ChannelEvent {
        event_type: "telegram_installation:created".into(),
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
    async fn channels_sprint_telegram_provider_errors_are_bounded_and_redacted() {
        use axum::{Router, body::Body, extract::State, http::Request, response::Response};
        use std::sync::{Arc, Mutex};
        type Reply = Arc<Mutex<(StatusCode, String, Option<String>, bool)>>;
        async fn reply(State(state): State<Reply>, request: Request<Body>) -> Response {
            assert_eq!(request.method(), "POST");
            assert!(
                request.uri().path().ends_with("/getMe"),
                "redirects must never be followed"
            );
            let (status, body, location, chunked) = state.lock().unwrap().clone();
            let mut response = Response::builder().status(status);
            if let Some(location) = location {
                response = response.header("location", location);
            }
            let body = if chunked {
                Body::from_stream(futures_util::stream::iter([
                    Ok::<_, std::io::Error>(vec![b'x'; 40000]),
                    Ok(vec![b'x'; 40000]),
                ]))
            } else {
                Body::from(body)
            };
            response.body(body).unwrap()
        }
        let state: Reply = Arc::new(Mutex::new((StatusCode::OK, String::new(), None, false)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(reply).with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let installer = TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let token = "123456:synthetic_private";
        for (status, body, expected) in [
            (
                StatusCode::UNAUTHORIZED,
                json!({"ok":false,"error_code":401,"description":token}).to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                StatusCode::BAD_GATEWAY,
                json!({"ok":false,"error_code":502,"description":token}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                StatusCode::OK,
                "not JSON".into(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (StatusCode::OK, "{}".into(), StatusCode::SERVICE_UNAVAILABLE),
            (
                StatusCode::OK,
                json!({"ok":true}).to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                StatusCode::OK,
                json!({"ok":true,"result":{"id":123456,"is_bot":false,"username":"SyntheticBot"}})
                    .to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                StatusCode::OK,
                json!({"ok":true,"result":{"id":999,"is_bot":true,"username":"SyntheticBot"}})
                    .to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                StatusCode::OK,
                json!({"ok":true,"result":{"id":123456,"is_bot":true}}).to_string(),
                StatusCode::BAD_REQUEST,
            ),
            (
                StatusCode::OK,
                "x".repeat(65537),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            *state.lock().unwrap() = (status, body, None, false);
            let err = installer
                .verify(token, "123456")
                .await
                .expect_err("invalid provider response");
            assert_eq!(err.0, expected);
            assert!(!err.1.contains(token));
        }
        *state.lock().unwrap() = (
            StatusCode::TEMPORARY_REDIRECT,
            String::new(),
            Some(format!("{origin}/credential-leak")),
            false,
        );
        assert_eq!(
            installer.verify(token, "123456").await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        *state.lock().unwrap() = (StatusCode::OK, String::new(), None, true);
        assert_eq!(
            installer.verify(token, "123456").await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        server.abort();
        let _ = server.await;
        assert_eq!(
            installer.verify(token, "123456").await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn channels_sprint_cipher_roundtrip_tamper_and_nonce() {
        assert!(CredentialBox::new(&[0; 31]).is_err());
        let cipher = CredentialBox::new(&[73; 32]).unwrap();
        let first = cipher.seal(b"123456:synthetic_token").unwrap();
        assert_ne!(first, cipher.seal(b"123456:synthetic_token").unwrap());
        assert_eq!(cipher.open(&first).unwrap(), b"123456:synthetic_token");
        let mut corrupt = STANDARD.decode(&first).unwrap();
        corrupt[15] ^= 1;
        assert!(cipher.open(&STANDARD.encode(corrupt)).is_err());
        assert!(CredentialBox::new(&[74; 32]).unwrap().open(&first).is_err());
        assert!(cipher.open("MTIz").is_err());
        // NIST AES-256-GCM: all-zero key, nonce and 16-byte plaintext, no AAD.
        let nist = hex::decode("000000000000000000000000cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919").unwrap();
        assert_eq!(
            CredentialBox::new(&[0; 32])
                .unwrap()
                .open(&STANDARD.encode(nist))
                .unwrap(),
            vec![0; 16]
        );
    }
    #[test]
    fn channels_sprint_telegram_token_and_origin_validation() {
        assert_eq!(parse_token("123456:abc_DEF-01").unwrap(), "123456");
        for token in [
            "",
            "0:abc",
            "123:",
            "x:abc",
            "123:abc/redirect",
            "123:abc?x",
            "123:abc\n",
            "123:abc:def",
        ] {
            assert!(parse_token(token).is_err());
        }
        for origin in [
            "https://example.com",
            "http://localhost:9999",
            "http://127.0.0.1:9999/path",
            "http://127.0.0.1:9999/?q=x",
            "http://user@127.0.0.1:9999",
        ] {
            assert!(TelegramInstaller::for_loopback_tests(&[73; 32], origin).is_err());
        }
    }
}
