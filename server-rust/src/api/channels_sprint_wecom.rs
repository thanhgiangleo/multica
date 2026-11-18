//! WeCom smart-bot BYO installation. Handshake proves control before persisting a bot slot.
//! Reassignment and reconnect stay closed until worker and binding cleanup are integrated.
use super::channels_sprint::{self as channel, Channel, ChannelEvent, Params};
use super::channels_sprint_telegram::CredentialBox;
use super::{Actor, ApiError, ApiState, GoJson, Identity, error};
use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::net::IpAddr;
use tokio::time::{Duration, timeout};
use tokio_postgres::GenericClient;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use uuid::Uuid;

const PROD_URL: &str = "wss://openws.work.weixin.qq.com";
const PROBE_TIMEOUT: Duration = Duration::from_secs(12);

/// Production uses fixed WeCom WSS origin; local test provider requires numeric loopback.
pub struct WecomInstaller {
    cipher: CredentialBox,
    url: String,
}
impl WecomInstaller {
    pub fn new(key: &[u8]) -> Result<Self, &'static str> {
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            url: PROD_URL.into(),
        })
    }
    pub fn for_loopback_tests(key: &[u8], origin: &str) -> Result<Self, &'static str> {
        let url = reqwest::Url::parse(origin).map_err(|_| "invalid test provider origin")?;
        let ip = url
            .host_str()
            .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok());
        if url.scheme() != "ws"
            || !ip.is_some_and(|ip| ip.is_loopback())
            || url.port().is_none()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("test provider must be an explicit loopback WS origin");
        }
        Ok(Self {
            cipher: CredentialBox::new(key)?,
            url: origin.into(),
        })
    }
    async fn probe(&self, bot_id: &str, secret: &str) -> Result<(), ApiError> {
        // A successful subscribe displaces any other subscriber. Caller must lock and
        // reject live ownership *before* calling this method.
        timeout(PROBE_TIMEOUT, async {
            let config = WebSocketConfig::default()
                .max_message_size(Some(65536))
                .max_frame_size(Some(65536));
            let (mut socket, _) = connect_async_with_config(&self.url, Some(config), false)
                .await
                .map_err(|_| unverifiable())?;
            let request_id = Uuid::new_v4().to_string();
            socket
                .send(Message::Text(
                    json!({"cmd":"aibot_subscribe", "headers":{"req_id":request_id},
                "body":{"bot_id":bot_id,"secret":secret}})
                    .to_string()
                    .into(),
                ))
                .await
                .map_err(|_| unverifiable())?;
            let mut frames = 0;
            loop {
                frames += 1;
                if frames > 32 {
                    return Err(unverifiable());
                }
                let frame = socket
                    .next()
                    .await
                    .ok_or_else(unverifiable)?
                    .map_err(|_| unverifiable())?;
                let body = match frame {
                    Message::Text(text) => serde_json::from_str::<Value>(&text).ok(),
                    Message::Binary(bytes) => serde_json::from_slice::<Value>(&bytes).ok(),
                    Message::Close(_) => return Err(unverifiable()),
                    _ => None,
                };
                let Some(body) = body else { continue };
                if body.pointer("/headers/req_id").and_then(Value::as_str)
                    != Some(request_id.as_str())
                {
                    continue;
                }
                // WeCom acks may omit cmd; req_id plus an explicit integer
                // errcode are required. Missing errcode must never default to success.
                if let Some(cmd) = body.get("cmd")
                    && cmd != ""
                    && cmd != "aibot_subscribe"
                {
                    return Err(unverifiable());
                }
                let code = body
                    .get("errcode")
                    .and_then(Value::as_i64)
                    .ok_or_else(unverifiable)?;
                return match code {
                    0 => Ok(()),
                    40001 | 40013 => Err(rejected()),
                    _ => Err(unverifiable()),
                };
            }
        })
        .await
        .unwrap_or_else(|_| Err(unverifiable()))
    }
}
fn rejected() -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        "WeCom rejected this Bot ID and secret; credentials were not saved",
    )
}
fn unverifiable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not verify WeCom bot; credentials were not changed",
    )
}
fn conflict() -> ApiError {
    error(
        StatusCode::CONFLICT,
        "WeCom bot already connected or requires bot-scoped cleanup; installation was not changed",
    )
}
fn valid_bot_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
async fn authorize_before_probe<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    agent: Uuid,
    user: Uuid,
) -> Result<(), ApiError> {
    // No row locks before provider I/O: a concurrent permission removal must
    // be able to commit so the post-I/O locked check can observe it.
    let role = db.query_opt("SELECT m.role FROM member m JOIN workspace w ON w.id=m.workspace_id JOIN \"user\" u ON u.id=m.user_id WHERE m.workspace_id=$1 AND m.user_id=$2 AND NOT u.zbs_is_shadow", &[&workspace,&user]).await.map_err(channel::db_error)?
        .map(|row|row.get::<_,String>(0)).ok_or_else(|| error(StatusCode::FORBIDDEN,"workspace membership required"))?;
    if !channel::admin(&role) {
        return Err(error(StatusCode::FORBIDDEN, "workspace admin required"));
    }
    if db
        .query_opt(
            "SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL",
            &[&agent, &workspace],
        )
        .await
        .map_err(channel::db_error)?
        .is_none()
    {
        return Err(error(
            StatusCode::NOT_FOUND,
            "agent not found in this workspace",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct RegisterBody {
    bot_id: String,
    secret: String,
    #[serde(default)]
    bot_name: String,
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
    let rt = channel::runtime_for(&state, Channel::Wecom, channel::Operation::Install)?;
    let installer = rt
        .wecom_installer()
        .ok_or_else(|| channel::disabled(Channel::Wecom, channel::Operation::Install))?;

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
    let bot_id = body.bot_id.trim();
    let secret = body.secret.trim();
    let bot_name = body.bot_name.trim();
    if !valid_bot_id(bot_id)
        || secret.is_empty()
        || secret.len() > 4096
        || !secret.bytes().all(|b| b.is_ascii_graphic())
        || bot_name.len() > 256
        || bot_name.chars().any(char::is_control)
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid WeCom Bot ID, secret or display name",
        ));
    }
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    authorize_before_probe(&**db, workspace, agent, user).await?;
    let tx = db.transaction().await.map_err(channel::db_error)?;
    // Agent first, then bot: installs with different bots for one agent use
    // the same order. The global bot slot includes other workspaces.
    for key in [
        format!("wecom-agent:{workspace}:{agent}"),
        format!("wecom-bot:{bot_id}"),
    ] {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&key],
        )
        .await
        .map_err(channel::db_error)?;
    }
    // Check permissions again after waiting for any competing installer.
    authorize_before_probe(&*tx, workspace, agent, user).await?;
    let prior = tx.query_opt("SELECT id,status,config FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='wecom' FOR UPDATE", &[&workspace,&agent]).await.map_err(channel::db_error)?;
    if let Some(row) = &prior {
        let status: String = row.get("status");
        let config: Value = row.get("config");
        if status != "active" || config.get("app_id").and_then(Value::as_str) != Some(bot_id) {
            return Err(conflict());
        }
    }
    let owner = tx.query_opt("SELECT id FROM channel_installation WHERE channel_type='wecom' AND config->>'app_id'=$1 FOR UPDATE", &[&bot_id]).await.map_err(channel::db_error)?;
    if owner
        .is_some_and(|row| Some(row.get::<_, Uuid>("id")) != prior.as_ref().map(|r| r.get("id")))
    {
        return Err(conflict());
    }
    installer.probe(bot_id, secret).await?;
    // This check runs after external I/O; revocation/membership changes during
    // the handshake cannot authorize persistence with stale rights.
    channel::authorize_install(&*tx, workspace, agent, user, true).await?;
    let sealed = installer.cipher.seal(secret.as_bytes()).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "channel encryption failed",
        )
    })?;
    let display_name = if !bot_name.is_empty() {
        bot_name.to_owned()
    } else {
        prior
            .as_ref()
            .and_then(|row| {
                row.get::<_, Value>("config")
                    .get("bot_display_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_default()
    };
    // Every successful subscribe replaces the previous stream. Fence work from
    // that stream even on same-bot secret rotation; consumed dedup history and
    // existing bindings still belong to this bot and are retained.
    if let Some(prior) = &prior {
        let installation: Uuid = prior.get("id");
        tx.execute(
            "DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NULL",
            &[&installation],
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
    let config = json!({"app_id":bot_id,"bot_id":bot_id,"secret_encrypted":sealed,"bot_display_name":display_name,"generation":Uuid::new_v4().to_string()});
    let row = tx.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,config,installer_user_id) VALUES($1,$2,'wecom',$3,$4) ON CONFLICT(workspace_id,agent_id,channel_type) DO UPDATE SET config=channel_installation.config || EXCLUDED.config,installer_user_id=EXCLUDED.installer_user_id,installed_at=now(),updated_at=now() RETURNING *", &[&workspace,&agent,&config,&user]).await.map_err(channel::db_error)?;
    let installation: Uuid = row.get("id");
    let result = channel::installation_json(&row, Channel::Wecom);
    tx.commit().await.map_err(channel::db_error)?;
    rt.publish(ChannelEvent {
        event_type: "wecom_installation:created".into(),
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
    fn channels_sprint_wecom_origin_restrictions() {
        for origin in [
            "wss://127.0.0.1:9000",
            "ws://localhost:9000",
            "ws://example.org:9000",
            "ws://127.0.0.1",
            "ws://127.0.0.1:9000/path",
            "ws://u@127.0.0.1:9000",
            "ws://127.0.0.1:9000/?secret=x",
        ] {
            assert!(WecomInstaller::for_loopback_tests(&[73; 32], origin).is_err());
        }
        assert!(WecomInstaller::for_loopback_tests(&[73; 32], "ws://127.0.0.1:9000").is_ok());
        assert!(WecomInstaller::new(&[73; 32]).is_ok());
        assert!(WecomInstaller::new(&[]).is_err());
    }

    #[tokio::test]
    async fn channels_sprint_wecom_probe_classifies_only_documented_rejection() {
        use tokio_tungstenite::accept_async;
        for (code, expected) in [
            (0, None),
            (40001, Some(StatusCode::BAD_REQUEST)),
            (40013, Some(StatusCode::BAD_REQUEST)),
            (45009, Some(StatusCode::SERVICE_UNAVAILABLE)),
            (99999, Some(StatusCode::SERVICE_UNAVAILABLE)),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(conn).await.unwrap();
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let body: Value = serde_json::from_str(&request).unwrap();
                assert_eq!(body["body"]["bot_id"], "synthetic-bot");
                assert_eq!(body["body"]["secret"], "synthetic-secret");
                // An unrelated frame must not count as proof.
                ws.send(Message::Text(
                    json!({"headers":{"req_id":"not-ours"},"errcode":0})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
                ws.send(Message::Text(json!({"headers":{"req_id":body["headers"]["req_id"]},"errcode":code,"errmsg":"synthetic-secret leaked"}).to_string().into())).await.unwrap();
            });
            let result = WecomInstaller::for_loopback_tests(&[73; 32], &origin)
                .unwrap()
                .probe("synthetic-bot", "synthetic-secret")
                .await;
            assert_eq!(result.err().map(|e| e.0), expected);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn channels_sprint_wecom_probe_does_not_accept_missing_or_malformed_verdict() {
        use tokio_tungstenite::accept_async;
        for invalid in [
            json!({"errmsg":"ok"}),
            json!({"errcode":"0"}),
            json!({"cmd":"unrelated","errcode":0}),
            json!({"errcode":-1}),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(conn).await.unwrap();
                let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let body: Value = serde_json::from_str(&request).unwrap();
                let mut response = invalid;
                response["headers"] = json!({"req_id":body["headers"]["req_id"]});
                ws.send(Message::Text(response.to_string().into()))
                    .await
                    .unwrap();
            });
            let err = WecomInstaller::for_loopback_tests(&[73; 32], &origin)
                .unwrap()
                .probe("synthetic-bot", "synthetic-secret")
                .await
                .unwrap_err();
            assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
            assert!(!err.1.contains("synthetic-secret"));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn channels_sprint_wecom_probe_bounds_frames_and_rejects_disconnect() {
        use tokio_tungstenite::accept_async;
        for mode in ["oversize", "unrelated", "disconnect"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (conn, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(conn).await.unwrap();
                let _ = ws.next().await.unwrap().unwrap();
                match mode {
                    "oversize" => {
                        let _ = ws.send(Message::Text("s".repeat(65537).into())).await;
                    }
                    "unrelated" => {
                        for _ in 0..33 {
                            if ws
                                .send(Message::Text(
                                    json!({"headers":{"req_id":"not-ours"},"errcode":0})
                                        .to_string()
                                        .into(),
                                ))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    _ => {
                        let _ = ws.close(None).await;
                    }
                }
            });
            let err = WecomInstaller::for_loopback_tests(&[73; 32], &origin)
                .unwrap()
                .probe("synthetic-bot", "synthetic-secret")
                .await
                .unwrap_err();
            assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE, "{mode}");
            assert!(!err.1.contains("synthetic-secret"));
            server.await.unwrap();
        }
    }
}
