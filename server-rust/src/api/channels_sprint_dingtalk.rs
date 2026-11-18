//! DingTalk Stream-mode BYO installation; no outbound sender or production worker.
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
use tokio_postgres::GenericClient;
use uuid::Uuid;

/// Key and credentials are injected by the integrator, never read from request-independent env.
pub struct DingTalkInstaller {
    cipher: CredentialBox,
    http: reqwest::Client,
    origin: String,
}
impl DingTalkInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Self::build(key, "https://api.dingtalk.com")
    }
    /// Synthetic provider only; never use this constructor for production traffic.
    pub fn for_loopback_tests(key: &[u8], origin: &str) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(origin).map_err(|_| "invalid test provider origin")?;
        let ip = url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<IpAddr>().ok());
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
        Self::build(key, origin.trim_end_matches('/'))
    }
    fn build(key: &[u8], origin: &str) -> Result<Self, &'static str> {
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            http: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .map_err(|_| "dingtalk HTTP client unavailable")?,
            origin: origin.into(),
        })
    }
    async fn verify(&self, app_key: &str, secret: &str) -> Result<(), ApiError> {
        // Never expose provider bodies or reqwest diagnostics: both may echo AppSecret.
        let mut response = self
            .http
            .post(format!("{}/v1.0/oauth2/accessToken", self.origin))
            .header("content-type", "application/json")
            .body(json!({"appKey":app_key,"appSecret":secret}).to_string())
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
        if matches!(status.as_u16(), 400 | 401 | 403) {
            return Err(rejected());
        }
        if !status.is_success() {
            return Err(unverifiable());
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| unverifiable())?;
        if value.get("code").is_some()
            || value
                .get("accessToken")
                .and_then(Value::as_str)
                .is_none_or(|token| {
                    token.is_empty()
                        || token.len() > 4096
                        || !token.bytes().all(|b| b.is_ascii_graphic())
                })
            || value
                .get("expireIn")
                .and_then(Value::as_i64)
                .is_none_or(|n| n <= 0)
        {
            return Err(unverifiable());
        }
        Ok(())
    }
}
fn rejected() -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        "DingTalk rejected this AppKey and AppSecret; credentials were not saved",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not verify DingTalk credentials; installation was not changed",
    )
}
fn conflict() -> ApiError {
    error(
        StatusCode::CONFLICT,
        "DingTalk robot replacement, reassignment or reconnect requires bot-scoped cleanup; installation was not changed",
    )
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
async fn authorize<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    agent: Uuid,
    user: Uuid,
) -> Result<(), ApiError> {
    let role = channel::membership(db, workspace, user).await?;
    let row = db.query_opt("SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL FOR SHARE", &[&agent,&workspace]).await.map_err(channel::db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found in this workspace"))?;
    if !channel::admin(&role) && row.get::<_, Uuid>("owner_id") != user {
        return Err(error(
            StatusCode::FORBIDDEN,
            "insufficient permission to install DingTalk robot",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct RegisterBody {
    client_id: String,
    client_secret: String,
}
pub(super) async fn register_byo(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    Query(query): Query<Params>,
    body: Result<Json<RegisterBody>, axum::extract::rejection::JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let workspace = channel::workspace_gate(&state, &actor, user, &params, false).await?;
    actor.require_human()?;
    let rt = channel::runtime_for(&state, Channel::DingTalk, channel::Operation::Install)?;
    let installer = rt
        .dingtalk_installer()
        .ok_or_else(|| channel::disabled(Channel::DingTalk, channel::Operation::Install))?;

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
        channel::agent_in_workspace(&**db, workspace, agent).await?;
    }
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let app_key = body.client_id.trim();
    let secret = body.client_secret.trim();
    if !identifier(app_key)
        || secret.is_empty()
        || secret.len() > 4096
        || !secret.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid DingTalk AppKey or AppSecret",
        ));
    }
    let snapshot = {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "channel database unavailable",
            )
        })?;
        authorize(&**db, workspace, agent, user).await?;
        db.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='dingtalk'", &[&workspace,&agent]).await.map_err(channel::db_error)?
            .map(|r| (r.get::<_,Uuid>("id"),r.get::<_,String>("status"),r.get::<_,Value>("config")))
    };
    installer.verify(app_key, secret).await?;
    let sealed = installer.cipher.seal(secret.as_bytes()).map_err(|_| {
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
        format!("dingtalk-agent:{workspace}:{agent}"),
        format!("dingtalk-app:{app_key}"),
    ] {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .await
        .map_err(channel::db_error)?;
    }
    let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='dingtalk' FOR UPDATE", &[&workspace,&agent]).await.map_err(channel::db_error)?;
    authorize(&*tx, workspace, agent, user).await?;
    let current = prior.as_ref().map(|r| {
        (
            r.get::<_, Uuid>("id"),
            r.get::<_, String>("status"),
            r.get::<_, Value>("config"),
        )
    });
    if current != snapshot {
        return Err(conflict());
    }
    if let Some((_, status, config)) = &current
        && (status != "active" || config.get("app_id").and_then(Value::as_str) != Some(app_key))
    {
        return Err(conflict());
    }
    // The unique app slot includes revoked/orphaned installs. Do not reclaim it until
    // integrator can fence old workers and clean bound identities and tasks.
    let owner = tx.query_opt("SELECT id FROM channel_installation WHERE channel_type='dingtalk' AND config->>'app_id'=$1 FOR UPDATE", &[&app_key]).await.map_err(channel::db_error)?;
    if owner.is_some_and(|r| Some(r.get::<_, Uuid>("id")) != prior.as_ref().map(|r| r.get("id"))) {
        return Err(conflict());
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
    let config = json!({"app_id":app_key,"robot_code":app_key,"app_secret_encrypted":sealed,"generation":Uuid::new_v4().to_string()});
    let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'dingtalk',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=channel_installation.config || EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,installed_at=now(),updated_at=now() RETURNING *, true AS agent_available", &[&workspace,&agent,&config,&user]).await.map_err(channel::db_error)?;
    let installation: Uuid = row.get("id");
    let result = channel::installation_json(&row, Channel::DingTalk);
    tx.commit().await.map_err(channel::db_error)?;
    rt.publish(ChannelEvent {
        event_type: "dingtalk_installation:created".into(),
        workspace_id: workspace,
        actor_id: user,
        installation_id: installation,
    });
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channels_sprint_dingtalk_rejects_untrusted_provider_origins() {
        let key = [0u8; 32];
        for origin in [
            "https://127.0.0.1:90",
            "http://example.com:90",
            "http://localhost:90",
            "http://127.0.0.1",
            "http://127.0.0.1:90/other",
            "http://user@127.0.0.1:90",
            "http://127.0.0.1:90/?token=x",
        ] {
            assert!(DingTalkInstaller::for_loopback_tests(&key, origin).is_err());
        }
        assert!(DingTalkInstaller::for_loopback_tests(&key, "http://127.0.0.1:12345").is_ok());
        assert!(DingTalkInstaller::new(&[]).is_err());
    }
    #[tokio::test]
    async fn channels_sprint_dingtalk_provider_redacts_response() {
        use axum::{Router, body::Body, http::Request, routing::post};
        async fn reply(request: Request<Body>) -> (StatusCode, String) {
            let bytes = axum::body::to_bytes(request.into_body(), 8192)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["appKey"], "synthetic-app");
            assert_eq!(body["appSecret"], "synthetic-secret");
            (StatusCode::BAD_REQUEST, "synthetic-secret rejected".into())
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1.0/oauth2/accessToken", post(reply)),
            )
            .await
            .unwrap();
        });
        let installer = DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let error = installer
            .verify("synthetic-app", "synthetic-secret")
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(!error.1.contains("synthetic-secret"));
        server.abort();
    }
    #[tokio::test]
    async fn channels_sprint_dingtalk_provider_fails_closed_on_redirect_and_bad_envelopes() {
        use axum::{
            Router, body::Body, extract::State, http::Request, response::Response, routing::post,
        };
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct Provider(Arc<Mutex<(StatusCode, String, bool)>>);
        async fn reply(State(state): State<Provider>, _req: Request<Body>) -> Response {
            let (status, text, redirect) = state.0.lock().unwrap().clone();
            let mut builder = Response::builder().status(status);
            if redirect {
                builder = builder.header("location", "http://127.0.0.1:1/stolen");
            }
            builder.body(Body::from(text)).unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let provider = Provider(Arc::new(Mutex::new((StatusCode::OK, String::new(), false))));
        let server = tokio::spawn({
            let provider = provider.clone();
            async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/v1.0/oauth2/accessToken", post(reply))
                        .with_state(provider),
                )
                .await
                .unwrap();
            }
        });
        let installer = DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let cases = [
            (StatusCode::TEMPORARY_REDIRECT, "{}".to_owned(), true),
            (StatusCode::OK, "{".to_owned(), false),
            (
                StatusCode::OK,
                json!({"accessToken":"x","expireIn":0}).to_string(),
                false,
            ),
            (
                StatusCode::OK,
                json!({"accessToken":"x","expireIn":3600,"code":"InvalidAuthentication"})
                    .to_string(),
                false,
            ),
            (StatusCode::OK, "x".repeat(65537), false),
        ];
        for (status, text, redirect) in cases {
            *provider.0.lock().unwrap() = (status, text, redirect);
            let err = installer
                .verify("synthetic-app", "synthetic-secret")
                .await
                .unwrap_err();
            assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
            assert!(!err.1.contains("synthetic-secret"));
        }
        server.abort();
    }
}
