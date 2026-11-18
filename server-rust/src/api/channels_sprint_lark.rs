//! Lark device-flow completion boundary. No public BYO endpoint or background worker.
//! Only a trusted device-flow worker may supply the provider-authenticated installer
//! open_id. Browser input must never be mapped directly into DeviceCompletion.
use super::channels_sprint::{self as channel, Channel, ChannelEvent};
use super::channels_sprint_telegram::CredentialBox;
use super::{ApiError, ApiState, error};
use axum::http::StatusCode;
use serde_json::{Value, json};
use std::{net::IpAddr, time::Duration};
use tokio_postgres::GenericClient;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub enum Region {
    Feishu,
    Lark,
}
impl Region {
    fn name(self) -> &'static str {
        match self {
            Self::Feishu => "feishu",
            Self::Lark => "lark",
        }
    }
    fn origin(self) -> &'static str {
        match self {
            Self::Feishu => "https://open.feishu.cn",
            Self::Lark => "https://open.larksuite.com",
        }
    }
}

/// Trusted result from device-code exchange, not a deserializable HTTP body.
/// Contains plaintext credentials: intentionally neither Debug nor Serialize.
pub struct DeviceCompletion<'a> {
    pub workspace_id: Uuid,
    pub agent_id: Uuid,
    pub initiator_id: Uuid,
    pub app_id: &'a str,
    pub app_secret: &'a str,
    pub installer_open_id: &'a str,
    pub region: Region,
}

/// Redacted completion failure, usable by a future device-session worker.
#[derive(Debug)]
pub struct LarkInstallError {
    pub status: StatusCode,
    pub message: String,
}

pub struct LarkInstaller {
    cipher: CredentialBox,
    http: reqwest::Client,
    test_origin: Option<String>,
}
impl LarkInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Self::build(key, None)
    }
    /// Only numeric loopback HTTP origins; production must use new().
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
        Self::build(key, Some(origin.trim_end_matches('/').into()))
    }
    fn build(key: &[u8], test_origin: Option<String>) -> Result<Self, &'static str> {
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            http: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(5))
                .build()
                .map_err(|_| "lark HTTP client unavailable")?,
            test_origin,
        })
    }
    async fn response(request: reqwest::RequestBuilder) -> Result<Value, ApiError> {
        // Never expose provider messages, reqwest diagnostics, tokens or secrets.
        let mut response = request.send().await.map_err(|_| unverifiable())?;
        if !response.status().is_success() || response.content_length().is_some_and(|n| n > 65536) {
            return Err(unverifiable());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| unverifiable())? {
            if chunk.len() > 65536 - bytes.len() {
                return Err(unverifiable());
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: Value = serde_json::from_slice(&bytes).map_err(|_| unverifiable())?;
        // Missing code is not success. Do not accept the Go zero-value shortcut.
        if result.get("code").and_then(Value::as_i64) != Some(0) {
            return Err(unverifiable());
        }
        Ok(result)
    }
    async fn verify(&self, p: &DeviceCompletion<'_>) -> Result<String, ApiError> {
        let origin = self.test_origin.as_deref().unwrap_or(p.region.origin());
        // No token cache: a secret rotation always verifies with freshly minted credentials.
        let auth = Self::response(
            self.http
                .post(format!(
                    "{origin}/open-apis/auth/v3/tenant_access_token/internal"
                ))
                .header("content-type", "application/json")
                .body(json!({"app_id":p.app_id,"app_secret":p.app_secret}).to_string()),
        )
        .await?;
        let token = auth
            .get("tenant_access_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 4096 && s.bytes().all(|b| b.is_ascii_graphic()))
            .ok_or_else(unverifiable)?;
        if auth
            .get("expire")
            .and_then(Value::as_i64)
            .is_none_or(|n| n <= 0)
        {
            return Err(unverifiable());
        }
        let bot = Self::response(
            self.http
                .get(format!("{origin}/open-apis/bot/v3/info"))
                .bearer_auth(token),
        )
        .await?;
        bot.get("bot")
            .and_then(|b| b.get("open_id"))
            .and_then(Value::as_str)
            .filter(|s| identifier(s))
            .map(str::to_owned)
            .ok_or_else(unverifiable)
    }

    /// Complete credential verification + installation + initial installer binding.
    /// Caller must validate session ownership/expiry and authenticated device exchange.
    /// This is not wired to production until device-flow/scheduler/sender gates exist.
    pub async fn complete_device_install(
        &self,
        state: &ApiState,
        p: DeviceCompletion<'_>,
    ) -> Result<Value, LarkInstallError> {
        self.complete_inner(state, p)
            .await
            .map_err(|e| LarkInstallError {
                status: e.0,
                message: e.1,
            })
    }

    async fn complete_inner(
        &self,
        state: &ApiState,
        p: DeviceCompletion<'_>,
    ) -> Result<Value, ApiError> {
        let rt = channel::runtime(state, Channel::Lark)?;
        if !identifier(p.app_id)
            || !identifier(p.installer_open_id)
            || p.app_secret.is_empty()
            || p.app_secret.len() > 4096
            || !p.app_secret.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid Lark device credentials",
            ));
        }
        let snapshot = {
            let db = state.pool.get().await.map_err(|_| unavailable())?;
            authorize(&**db, &p).await?;
            db.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='feishu'", &[&p.workspace_id,&p.agent_id]).await.map_err(channel::db_error)?
                .map(|row| (row.get::<_,Uuid>("id"), row.get::<_,String>("status"), row.get::<_,Value>("config")))
        };
        let bot_open_id = self.verify(&p).await?;
        let sealed = self.cipher.seal(p.app_secret.as_bytes()).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "channel encryption failed",
            )
        })?;
        let mut db = state.pool.get().await.map_err(|_| unavailable())?;
        let tx = db.transaction().await.map_err(channel::db_error)?;
        // Identical ordering for concurrent app/agent installs; never hold locks across HTTP.
        for key in [
            format!("lark-agent:{}:{}", p.workspace_id, p.agent_id),
            format!("lark-app:{}", p.app_id),
        ] {
            tx.query_one(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                &[&key],
            )
            .await
            .map_err(channel::db_error)?;
        }
        let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='feishu' FOR UPDATE", &[&p.workspace_id,&p.agent_id]).await.map_err(channel::db_error)?;
        authorize(&*tx, &p).await?;
        // Reject stale completions when another install, rotation or revoke won during provider I/O.
        // Compare stored state, not updated_at: server-side config changes also fail closed.
        let locked_snapshot = prior.as_ref().map(|row| {
            (
                row.get::<_, Uuid>("id"),
                row.get::<_, String>("status"),
                row.get::<_, Value>("config"),
            )
        });
        if locked_snapshot != snapshot {
            return Err(conflict());
        }
        if let Some(prior) = &prior {
            let config: Value = prior.get("config");
            if prior.get::<_, String>("status") != "active"
                || config.get("app_id").and_then(Value::as_str) != Some(p.app_id)
                || config.get("bot_open_id").and_then(Value::as_str) != Some(&bot_open_id)
                || config
                    .get("region")
                    .and_then(Value::as_str)
                    .unwrap_or("feishu")
                    != p.region.name()
            {
                return Err(conflict());
            }
        }
        let owner = tx.query_opt("SELECT id FROM channel_installation WHERE channel_type='feishu' AND config->>'app_id'=$1 FOR UPDATE", &[&p.app_id]).await.map_err(channel::db_error)?;
        if owner
            .is_some_and(|r| Some(r.get::<_, Uuid>("id")) != prior.as_ref().map(|r| r.get("id")))
        {
            return Err(conflict());
        }
        if let Some(prior) = &prior {
            let installation: Uuid = prior.get("id");
            tx.execute("DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NULL", &[&installation]).await.map_err(channel::db_error)?;
            tx.execute("DELETE FROM channel_binding_token WHERE installation_id=$1 AND consumed_at IS NULL", &[&installation]).await.map_err(channel::db_error)?;
        }
        let config = json!({"app_id":p.app_id,"app_secret_encrypted":sealed,"generation":Uuid::new_v4().to_string(),"bot_open_id":bot_open_id,"region":p.region.name()});
        // Preserve server-managed config/first-message state on same-bot rotation.
        let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'feishu',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=channel_installation.config || EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,installed_at=now(),updated_at=now() RETURNING *", &[&p.workspace_id,&p.agent_id,&config,&p.initiator_id]).await.map_err(channel::db_error)?;
        let installation: Uuid = row.get("id");
        // Do not steal an external identity already bound to another human, workspace or channel.
        let binding = tx.query_opt("INSERT INTO channel_user_binding(workspace_id,multica_user_id,installation_id,channel_type,channel_user_id,config) VALUES($1,$2,$3,'feishu',$4,'{}') ON CONFLICT(installation_id,channel_user_id) DO UPDATE SET bound_at=now() WHERE channel_user_binding.multica_user_id=EXCLUDED.multica_user_id AND channel_user_binding.workspace_id=EXCLUDED.workspace_id AND channel_user_binding.channel_type='feishu' RETURNING id", &[&p.workspace_id,&p.initiator_id,&installation,&p.installer_open_id]).await.map_err(channel::db_error)?;
        if binding.is_none() {
            return Err(error(
                StatusCode::CONFLICT,
                "Lark account is already bound to a different user",
            ));
        }
        let result = channel::installation_json(&row, Channel::Lark);
        tx.commit().await.map_err(channel::db_error)?;
        rt.publish(ChannelEvent {
            event_type: "lark_installation:created".into(),
            workspace_id: p.workspace_id,
            actor_id: p.initiator_id,
            installation_id: installation,
        });
        Ok(result)
    }
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
async fn authorize<C: GenericClient + Sync>(
    db: &C,
    p: &DeviceCompletion<'_>,
) -> Result<(), ApiError> {
    let role = channel::membership(db, p.workspace_id, p.initiator_id).await?;
    let agent = db.query_opt("SELECT owner_id FROM agent WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL FOR SHARE", &[&p.agent_id,&p.workspace_id]).await.map_err(channel::db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"agent not found in this workspace"))?;
    if !channel::admin(&role) && agent.get::<_, Uuid>("owner_id") != p.initiator_id {
        return Err(error(
            StatusCode::FORBIDDEN,
            "insufficient permission to install Lark bot",
        ));
    }
    Ok(())
}
fn conflict() -> ApiError {
    error(
        StatusCode::CONFLICT,
        "Lark bot reconnect, replacement or reassignment requires fenced cleanup; installation was not changed",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not verify Lark bot; credentials were not saved",
    )
}
fn unavailable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "channel database unavailable",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channels_sprint_lark_region_and_test_origin_validation() {
        assert_eq!(Region::Feishu.origin(), "https://open.feishu.cn");
        assert_eq!(Region::Lark.origin(), "https://open.larksuite.com");
        assert!(LarkInstaller::new(&[73; 31]).is_err());
        for url in [
            "https://127.0.0.1:1234",
            "http://localhost:1234",
            "http://192.0.2.1:1234",
            "http://127.0.0.1",
            "http://127.0.0.1:1234/path",
            "http://user@127.0.0.1:1234",
            "http://127.0.0.1:1234/?token=x",
        ] {
            assert!(
                LarkInstaller::for_loopback_tests(&[73; 32], url).is_err(),
                "{url}"
            );
        }
        assert!(LarkInstaller::for_loopback_tests(&[73; 32], "http://127.0.0.1:1234").is_ok());
    }

    #[tokio::test]
    async fn channels_sprint_lark_provider_malformed_and_secret_echo_are_redacted() {
        use axum::{Router, extract::State, response::Response, routing::any};
        use std::sync::{Arc, Mutex};
        type Reply = Arc<Mutex<(StatusCode, String, Option<String>)>>;
        async fn reply(State(state): State<Reply>) -> Response {
            let (status, body, location) = state.lock().unwrap().clone();
            let mut response = Response::builder().status(status);
            if let Some(location) = location {
                response = response.header("location", location);
            }
            response.body(axum::body::Body::from(body)).unwrap()
        }
        let state: Reply = Arc::new(Mutex::new((StatusCode::OK, String::new(), None)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn({
            let state = state.clone();
            async move {
                axum::serve(
                    listener,
                    Router::new().fallback(any(reply)).with_state(state),
                )
                .await
                .unwrap();
            }
        });
        let installer = LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let completion = DeviceCompletion {
            workspace_id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            initiator_id: Uuid::new_v4(),
            app_id: "cli_synthetic",
            app_secret: "synthetic_app_secret",
            installer_open_id: "ou_synthetic_installer",
            region: Region::Feishu,
        };
        for (status, body, location) in [
            (
                StatusCode::OK,
                json!({"code":0,"tenant_access_token":"synthetic_tenant_token"}).to_string(),
                None,
            ),
            (
                StatusCode::OK,
                json!({"code":1,"msg":"synthetic_app_secret"}).to_string(),
                None,
            ),
            (StatusCode::OK, "not json".into(), None),
            (StatusCode::OK, "x".repeat(65537), None),
            (
                StatusCode::TEMPORARY_REDIRECT,
                String::new(),
                Some(format!("{origin}/leak")),
            ),
        ] {
            *state.lock().unwrap() = (status, body, location);
            let error = installer.verify(&completion).await.unwrap_err();
            assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
            assert!(!error.1.contains("synthetic_app_secret"));
        }
        server.abort();
        let _ = server.await;
    }
}
