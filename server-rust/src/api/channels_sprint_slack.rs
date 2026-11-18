//! Slack BYO install. Verifies credentials without opening sockets or sending messages.
use super::channels_sprint::{self as channel, Channel, ChannelEvent, Params};
use super::channels_sprint_telegram::CredentialBox;
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{net::IpAddr, time::Duration};
use uuid::Uuid;

/// Runtime-injected at-rest key and fixed Slack Web API endpoint. No environment lookup.
pub struct SlackInstaller {
    cipher: CredentialBox,
    http: reqwest::Client,
    base: String,
}
impl SlackInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Self::at_base(key, "https://slack.com/api")
    }
    /// Synthetic tests only. Production wiring must use new(), not this constructor.
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
        Self::at_base(key, &format!("{}/api", origin.trim_end_matches('/')))
    }
    fn at_base(key: &[u8], base: &str) -> Result<Self, &'static str> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| "slack HTTP client unavailable")?;
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            http,
            base: base.into(),
        })
    }
    async fn request(
        &self,
        token: &str,
        method: &str,
        form: &[(&str, &str)],
    ) -> Result<Value, ApiError> {
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form.iter().copied())
            .finish();
        // Never return provider bodies, URLs or reqwest errors; any can echo credentials.
        let mut response = self
            .http
            .post(format!("{}/{method}", self.base))
            .bearer_auth(token)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
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
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(rejected());
        }
        if !status.is_success() {
            return Err(unverifiable());
        }
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| unverifiable())?;
        match envelope.get("ok") {
            Some(Value::Bool(true)) => Ok(envelope),
            Some(Value::Bool(false)) => Err(rejected()),
            _ => Err(unverifiable()),
        }
    }
    async fn verify(&self, bot: &str, app: &str, app_id: &str) -> Result<VerifiedBot, ApiError> {
        let auth = self.request(bot, "auth.test", &[]).await?;
        let team_id = response_id(&auth, "team_id", b"T")?;
        let user_id = response_id(&auth, "user_id", b"UW")?;
        let bot_id = response_id(&auth, "bot_id", b"B")?;
        let info = self.request(bot, "bots.info", &[("bot", bot_id)]).await?;
        let info = info.get("bot").ok_or_else(unverifiable)?;
        if response_id(info, "id", b"B")? != bot_id || response_id(info, "app_id", b"A")? != app_id
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "slack: the bot token and app-level token are from different Slack apps",
            ));
        }
        let opened = self.request(app, "apps.connections.open", &[]).await?;
        let url = opened
            .get("url")
            .and_then(Value::as_str)
            .and_then(|s| reqwest::Url::parse(s).ok())
            .ok_or_else(unverifiable)?;
        if url.scheme() != "wss"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(unverifiable());
        }
        // The returned socket URL is a capability: never follow, store or publish it.
        Ok(VerifiedBot {
            team_id: team_id.into(),
            user_id: user_id.into(),
        })
    }
}
struct VerifiedBot {
    team_id: String,
    user_id: String,
}
fn rejected() -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        "could not verify the Slack tokens — check the bot token and app-level token, that the app is installed to your workspace, and that it has the users:read scope",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not verify the Slack tokens — check the server network and try again; the tokens were not saved",
    )
}
fn valid_id(id: &str, prefixes: &[u8]) -> bool {
    (2..=64).contains(&id.len())
        && prefixes.contains(&id.as_bytes()[0])
        && id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}
fn response_id<'a>(value: &'a Value, field: &str, prefixes: &[u8]) -> Result<&'a str, ApiError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| valid_id(s, prefixes))
        .ok_or_else(unverifiable)
}
fn parse_tokens<'a>(bot: &str, app: &'a str) -> Result<&'a str, ApiError> {
    let safe = |s: &str| {
        !s.is_empty()
            && s.len() <= 4096
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    };
    if !bot.starts_with("xoxb-") || bot.len() <= 5 || !safe(bot) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "slack: bot token must start with xoxb-",
        ));
    }
    let invalid_app = || {
        error(
            StatusCode::BAD_REQUEST,
            "slack: app-level token must start with xapp- and embed an app id",
        )
    };
    if !safe(app) {
        return Err(invalid_app());
    }
    let parts: Vec<_> = app.splitn(5, '-').collect();
    if parts.len() != 5
        || parts[0] != "xapp"
        || parts[1] != "1"
        || !valid_id(parts[2], b"A")
        || parts[3].is_empty()
        || !parts[3].bytes().all(|b| b.is_ascii_digit())
        || parts[4].is_empty()
    {
        return Err(invalid_app());
    }
    Ok(parts[2])
}

#[derive(Deserialize)]
pub(super) struct RegisterBody {
    bot_token: String,
    app_token: String,
}
pub(super) async fn register_byo(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    Query(query): Query<Params>,
    body: Result<Json<RegisterBody>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let workspace = channel::workspace_gate(&state, &actor, user, &params, true).await?;
    actor.require_human()?;
    let rt = channel::runtime_for(&state, Channel::Slack, channel::Operation::Install)?;
    let installer = rt
        .slack_installer()
        .ok_or_else(|| channel::disabled(Channel::Slack, channel::Operation::Install))?;

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
    let bot_token = body.bot_token.trim();
    let app_token = body.app_token.trim();
    let app_id = parse_tokens(bot_token, app_token)?;
    let snapshot = {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel database unavailable",
            )
        })?;
        channel::authorize_install(&**db, workspace, agent, user, true).await?;
        db.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='slack'", &[&workspace,&agent]).await.map_err(channel::db_error)?
            .map(|r| (r.get::<_,Uuid>("id"),r.get::<_,String>("status"),r.get::<_,Value>("config")))
    };
    let verified = installer.verify(bot_token, app_token, app_id).await?;
    let encrypt = |token: &str| {
        installer.cipher.seal(token.as_bytes()).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "channel encryption failed",
            )
        })
    };
    let config = json!({"app_id":app_id,"team_id":verified.team_id,"bot_user_id":verified.user_id,
        "bot_token_encrypted":encrypt(bot_token)?,"app_token_encrypted":encrypt(app_token)?,"generation":Uuid::new_v4().to_string()});
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(channel::db_error)?;
    for key in [
        format!("slack-agent:{workspace}:{agent}"),
        format!("slack-app:{app_id}"),
    ] {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='slack' FOR UPDATE", &[&workspace,&agent])
        .await.map_err(channel::db_error)?;
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
            "Slack installation changed or was revoked during verification; existing installation was not changed",
        ));
    }
    if let Some(row) = &prior {
        let old: Value = row.get("config");
        if ["app_id", "team_id", "bot_user_id"]
            .iter()
            .any(|key| old.get(key) != config.get(key))
        {
            // Do not carry bindings, tasks or thread sessions across app/team/bot namespaces.
            return Err(error(
                StatusCode::CONFLICT,
                "Slack bot replacement requires bot-scoped cleanup; existing installation was not changed",
            ));
        }
    }
    let owner = tx.query_opt("SELECT ci.id,ci.workspace_id,a.archived_at FROM channel_installation ci LEFT JOIN agent a ON a.id=ci.agent_id AND a.workspace_id=ci.workspace_id WHERE ci.channel_type='slack' AND ci.config->>'app_id'=$1 FOR UPDATE OF ci", &[&app_id])
        .await.map_err(channel::db_error)?;
    if let Some(owner) = owner
        && prior.as_ref().map(|r| r.get::<_, Uuid>("id")) != Some(owner.get("id"))
    {
        let message = if owner.get::<_, Uuid>("workspace_id") != workspace {
            "this Slack app belongs to another workspace installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        } else if owner
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
        {
            "this Slack app belongs to an archived agent installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        } else {
            "this Slack app belongs to another agent installation; reassignment requires bot-scoped cleanup, which is not yet supported"
        };
        return Err(error(StatusCode::CONFLICT, message));
    }
    if let Some(prior) = &prior {
        let installation: Uuid = prior.get("id");
        tx.execute("DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NULL", &[&installation]).await.map_err(channel::db_error)?;
        tx.execute(
            "DELETE FROM channel_binding_token WHERE installation_id=$1 AND consumed_at IS NULL",
            &[&installation],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'slack',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,status='active',installed_at=now(),updated_at=now() RETURNING *", &[&workspace,&agent,&config,&user])
        .await.map_err(channel::db_error)?;
    let result = channel::installation_json(&row, Channel::Slack);
    let installation = row.get("id");
    tx.commit().await.map_err(channel::db_error)?;
    rt.publish(ChannelEvent {
        event_type: "slack_installation:created".into(),
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
    async fn channels_sprint_slack_provider_validation_redaction_and_no_redirects() {
        use axum::{Router, body::Body, extract::State, http::Request, response::Response};
        use std::sync::{Arc, Mutex};
        type ProviderReply = (String, StatusCode, String, bool, Option<String>);
        #[derive(Clone)]
        struct Provider {
            // method, HTTP status, body, chunked flag, optional redirect.
            reply: Arc<Mutex<ProviderReply>>,
            calls: Arc<Mutex<Vec<String>>>,
        }
        async fn reply(State(p): State<Provider>, req: Request<Body>) -> Response {
            assert_eq!(req.method(), "POST");
            assert!(req.uri().query().is_none(), "no token in URL");
            let method = req
                .uri()
                .path()
                .strip_prefix("/api/")
                .expect("no redirect followed")
                .to_string();
            assert!(["auth.test", "bots.info", "apps.connections.open"].contains(&method.as_str()));
            p.calls.lock().unwrap().push(method.clone());
            let auth = req.headers()["authorization"].to_str().unwrap();
            assert_eq!(
                auth,
                if method == "apps.connections.open" {
                    "Bearer xapp-1-A73-1-private_73"
                } else {
                    "Bearer xoxb-private_73"
                }
            );
            let body = axum::body::to_bytes(req.into_body(), 4096).await.unwrap();
            if method == "bots.info" {
                assert_eq!(body.as_ref(), b"bot=B73");
            } else {
                assert!(body.is_empty());
            }
            let (target, status, text, chunked, redirect) = p.reply.lock().unwrap().clone();
            let mut out = Response::builder();
            let body = if target == method {
                out = out.status(status);
                if let Some(url) = redirect {
                    out = out.header("location", url);
                }
                if chunked {
                    Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::io::Error>(vec![b'x'; 40000]),
                        Ok(vec![b'x'; 40000]),
                    ]))
                } else {
                    Body::from(text)
                }
            } else {
                let success = match method.as_str() {
                    "auth.test" => {
                        json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"})
                    }
                    "bots.info" => json!({"ok":true,"bot":{"id":"B73","app_id":"A73"}}),
                    "apps.connections.open" => {
                        json!({"ok":true,"url":"wss://fake.invalid/private"})
                    }
                    _ => unreachable!(),
                };
                Body::from(success.to_string())
            };
            out.body(body).unwrap()
        }
        let p = Provider {
            reply: Arc::new(Mutex::new((
                String::new(),
                StatusCode::OK,
                String::new(),
                false,
                None,
            ))),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(reply).with_state(p.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let installer = SlackInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let bot = "xoxb-private_73";
        let app = "xapp-1-A73-1-private_73";
        assert!(installer.verify(bot, app, "A73").await.is_ok());
        assert_eq!(
            p.calls.lock().unwrap().as_slice(),
            ["auth.test", "bots.info", "apps.connections.open"]
        );
        for method in ["auth.test", "bots.info", "apps.connections.open"] {
            for (status, text, expected, chunked, redirect) in [
                (
                    StatusCode::OK,
                    json!({"ok":false,"error":format!("{bot} {app}")}).to_string(),
                    StatusCode::BAD_REQUEST,
                    false,
                    None,
                ),
                (
                    StatusCode::UNAUTHORIZED,
                    bot.into(),
                    StatusCode::BAD_REQUEST,
                    false,
                    None,
                ),
                (
                    StatusCode::BAD_GATEWAY,
                    app.into(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    None,
                ),
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    app.into(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    None,
                ),
                (
                    StatusCode::OK,
                    "not json".into(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    None,
                ),
                (
                    StatusCode::OK,
                    "{}".into(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    None,
                ),
                (
                    StatusCode::OK,
                    "x".repeat(65537),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    None,
                ),
                (
                    StatusCode::OK,
                    String::new(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    true,
                    None,
                ),
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    String::new(),
                    StatusCode::SERVICE_UNAVAILABLE,
                    false,
                    Some(format!("{origin}/credential-leak")),
                ),
            ] {
                *p.reply.lock().unwrap() = (method.into(), status, text, chunked, redirect);
                p.calls.lock().unwrap().clear();
                let err = installer
                    .verify(bot, app, "A73")
                    .await
                    .err()
                    .expect("reject invalid provider");
                assert_eq!(err.0, expected, "{method}");
                assert!(!err.1.contains(bot) && !err.1.contains(app));
                assert_eq!(
                    p.calls.lock().unwrap().last().map(String::as_str),
                    Some(method)
                );
            }
        }
        for (method, text) in [
            (
                "auth.test",
                json!({"ok":true,"team_id":"T73","user_id":"U73"}),
            ),
            (
                "auth.test",
                json!({"ok":true,"team_id":bot,"user_id":"U73","bot_id":"B73"}),
            ),
            (
                "bots.info",
                json!({"ok":true,"bot":{"id":"B73","app_id":"A74"}}),
            ),
            (
                "bots.info",
                json!({"ok":true,"bot":{"id":"B74","app_id":"A73"}}),
            ),
            ("apps.connections.open", json!({"ok":true})),
            (
                "apps.connections.open",
                json!({"ok":true,"url":"http://fake.invalid/credential-leak"}),
            ),
            (
                "apps.connections.open",
                json!({"ok":true,"url":"wss://user:password@fake.invalid/"}),
            ),
        ] {
            *p.reply.lock().unwrap() =
                (method.into(), StatusCode::OK, text.to_string(), false, None);
            let err = installer
                .verify(bot, app, "A73")
                .await
                .err()
                .expect("reject invalid identity");
            assert!(!err.1.contains(bot) && !err.1.contains(app));
        }
        server.abort();
        let _ = server.await;
        assert_eq!(
            installer.verify(bot, app, "A73").await.err().unwrap().0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn channels_sprint_slack_token_and_origin_validation() {
        assert_eq!(
            parse_tokens("xoxb-synthetic_73", "xapp-1-A73-1-synthetic").unwrap(),
            "A73"
        );
        for bot in [
            "",
            "xoxp-synthetic",
            "xoxb-",
            "xoxb-test\n",
            "xoxb-a/b",
            "xoxb-a?b",
        ] {
            assert!(parse_tokens(bot, "xapp-1-A73-1-synthetic").is_err());
        }
        for app in [
            "",
            "xapp-1--1-secret",
            "xapp-1-B73-1-secret",
            "xapp-2-A73-1-secret",
            "xapp-1-A73-1",
            "xapp-1-A73--secret",
            "xapp-1-A73-gen-secret",
            "xapp-1-A73-1-",
            "xapp-1-A73-1-secret\n",
            "xapp-1-A73-1-secret/",
            "xapp-1-A73-1-secret?x",
        ] {
            assert!(parse_tokens("xoxb-synthetic", app).is_err());
        }
        assert!(
            parse_tokens(&format!("xoxb-{}", "x".repeat(4096)), "xapp-1-A73-1-secret").is_err()
        );
        assert!(SlackInstaller::new(&[73; 31]).is_err());
        for origin in [
            "https://example.com",
            "http://localhost:9999",
            "http://127.0.0.1:9999/path",
            "http://127.0.0.1:9999/?q=x",
            "http://user@127.0.0.1:9999",
            "http://127.0.0.1:9999/#x",
        ] {
            assert!(SlackInstaller::for_loopback_tests(&[73; 32], origin).is_err());
        }
        assert!(SlackInstaller::for_loopback_tests(&[73; 32], "http://127.0.0.1:9999").is_ok());
    }
}
