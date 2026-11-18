//! User WebSocket delivery of committed, owner-scoped chat/runtime events.
use super::{
    ApiError, ApiState, cookie, decode_session_claims, error, runtime_chat_events::Event,
    temporarily_disabled,
};
use axum::{
    Router,
    extract::{
        Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header::HOST},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::time::Instant;
use tokio_postgres::GenericClient;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/ws", get(connect))
}

#[derive(Deserialize)]
struct Selector {
    workspace_id: Option<String>,
    workspace_slug: Option<String>,
}

async fn connect(
    State(state): State<ApiState>,
    Query(selector): Query<Selector>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "membership lookup failed"))?;
    let workspace = if let Some(id) = selector.workspace_id.as_deref().filter(|s| !s.is_empty()) {
        Uuid::parse_str(id).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?
    } else if let Some(slug) = selector.workspace_slug.as_deref().filter(|s| !s.is_empty()) {
        db.query_opt("SELECT id FROM workspace WHERE slug=$1", &[&slug])
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "membership lookup failed"))?
            .map(|row| row.get(0))
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
    } else {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "workspace_id or workspace_slug required",
        ));
    };
    if !origin_allowed(&headers) {
        return Err(error(StatusCode::FORBIDDEN, "forbidden origin"));
    }
    let preauthenticated = match cookie(&headers, "multica_auth") {
        Some(token) => {
            let user = authenticate_token(&**db, &state, &token).await?;
            if !membership(&**db, workspace, user).await? {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "not a member of this workspace",
                ));
            }
            Some((user, token))
        }
        None => None,
    };
    drop(db);
    Ok(upgrade
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| serve(socket, state, workspace, preauthenticated))
        .into_response())
}

// Do not trust forwarded host headers: unlike the Go listener this slice has no
// trusted-proxy CIDR configuration. Cross-origin browser access is restricted
// to the configured frontend origin.
fn origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let host = headers
        .get(HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if let Ok(url) = reqwest::Url::parse(origin)
        && matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && url.port_or_known_default().is_some()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
    {
        let authority = url
            .as_str()
            .strip_prefix("https://")
            .or_else(|| url.as_str().strip_prefix("http://"))
            .unwrap_or("")
            .trim_end_matches('/');
        if authority.eq_ignore_ascii_case(host) {
            return true;
        }
    }
    std::env::var("FRONTEND_ORIGIN").ok().as_deref() == Some(origin)
}

async fn authenticate_token(
    db: &impl GenericClient,
    state: &ApiState,
    token: &str,
) -> Result<Uuid, ApiError> {
    if token.starts_with("mul_") {
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        let row = db.query_opt("SELECT p.user_id FROM personal_access_token p JOIN \"user\" u ON u.id=p.user_id WHERE p.token_hash=$1 AND NOT p.revoked AND (p.expires_at IS NULL OR p.expires_at>now()) AND NOT u.zbs_is_shadow", &[&hash]).await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "authentication temporarily unavailable"))?
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
        let id: Uuid = row.get(0);
        if temporarily_disabled(id, "") {
            return Err(error(StatusCode::FORBIDDEN, "account disabled"));
        }
        return Ok(id);
    }
    // A daemon, cloud or task token must never become a human WS identity.
    if token.starts_with("mdt_") || token.starts_with("mat_") || token.starts_with("mcn_") {
        return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
    }
    let claims = decode_session_claims(token, &state.jwt_secret)?;
    let id = Uuid::parse_str(&claims.sub)
        .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
    if temporarily_disabled(id, &claims.email) {
        return Err(error(StatusCode::FORBIDDEN, "account disabled"));
    }
    let found = db
        .query_opt(
            "SELECT 1 FROM \"user\" WHERE id=$1 AND NOT zbs_is_shadow",
            &[&id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable",
            )
        })?;
    found.ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
    Ok(id)
}

async fn membership(
    db: &impl GenericClient,
    workspace: Uuid,
    user: Uuid,
) -> Result<bool, ApiError> {
    db.query_opt(
        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &user],
    )
    .await
    .map(|row| row.is_some())
    .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "membership lookup failed"))
}

// An open socket can outlive PAT revocation, JWT expiry or membership changes.
// Revalidate before handling commands and before emitting private events.
async fn authorized(
    db: &impl GenericClient,
    state: &ApiState,
    workspace: Uuid,
    user: Uuid,
    credential: &str,
) -> bool {
    matches!(authenticate_token(db, state, credential).await, Ok(id) if id == user)
        && matches!(membership(db, workspace, user).await, Ok(true))
}

const WRITE_WAIT: Duration = Duration::from_secs(10);
const PONG_WAIT: Duration = Duration::from_secs(60);

async fn send(socket: &mut WebSocket, frame: Value) -> bool {
    matches!(
        tokio::time::timeout(
            WRITE_WAIT,
            socket.send(Message::Text(frame.to_string().into()))
        )
        .await,
        Ok(Ok(()))
    )
}

async fn serve(
    mut socket: WebSocket,
    state: ApiState,
    workspace: Uuid,
    preauthenticated: Option<(Uuid, String)>,
) {
    let (user, credential) = if let Some(preauthenticated) = preauthenticated {
        preauthenticated
    } else {
        let first = tokio::time::timeout(Duration::from_secs(10), socket.recv()).await;
        let Some(Ok(Message::Text(raw))) = first.ok().flatten() else {
            let _ = send(&mut socket, json!({"error":"auth timeout or read error"})).await;
            return;
        };
        let parsed: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
        let Some(token) = parsed
            .get("payload")
            .and_then(|p| p.get("token"))
            .and_then(Value::as_str)
            .filter(|_| parsed.get("type").and_then(Value::as_str) == Some("auth"))
        else {
            let _ = send(
                &mut socket,
                json!({"error":"expected auth message as first frame"}),
            )
            .await;
            return;
        };
        let Ok(db) = state.pool.get().await else {
            return;
        };
        let user = match authenticate_token(&**db, &state, token).await {
            Ok(user) => user,
            Err(err) => {
                let _ = send(&mut socket, json!({"error":err.1})).await;
                return;
            }
        };
        match membership(&**db, workspace, user).await {
            Ok(true) => {}
            Ok(false) => {
                let _ = send(
                    &mut socket,
                    json!({"error":"not a member of this workspace"}),
                )
                .await;
                return;
            }
            Err(_) => {
                let _ = send(&mut socket, json!({"error":"membership lookup failed"})).await;
                return;
            }
        }
        drop(db);
        if !send(&mut socket, json!({"type":"auth_ack"})).await {
            return;
        }
        (user, token.to_owned())
    };
    let mut events = state.events.subscribe_for(workspace, user);
    let mut ping = tokio::time::interval(Duration::from_secs(54));
    ping.tick().await;
    let mut last_pong = Instant::now();
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let Ok(db) = state.pool.get().await else { break };
                    let still_member = authorized(&**db, &state, workspace, user, &credential).await;
                    drop(db);
                    if !still_member { break }
                    if text.len() > 64 * 1024 { break }
                    let frame: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                    match frame.get("type").and_then(Value::as_str) {
                        Some("ping") if !send(&mut socket, json!({"type":"pong"})).await => break,
                        Some("ping") => {},
                        Some("subscribe" | "unsubscribe") if !subscription(&mut socket, &state, workspace, user, &frame).await => break,
                        Some("subscribe" | "unsubscribe") => {},
                        _ => {},
                    }
                },
                Some(Ok(Message::Pong(_))) => last_pong = Instant::now(),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {},
            },
            result = events.recv() => {
                let event = match result { Ok(event) => event, Err(_) => break };
                let Ok(db) = state.pool.get().await else { break };
                if !authorized(&**db, &state, workspace, user, &credential).await { break }
                // A queued private event must still be readable when delivered.
                if !event_access(&**db, workspace, user, &event).await { continue }
                drop(db);
                if !send(&mut socket, event_frame(&event)).await { break }
            },
            _ = ping.tick() => {
                let Ok(db) = state.pool.get().await else { break };
                if !authorized(&**db, &state, workspace, user, &credential).await { break }
                drop(db);
                if !matches!(tokio::time::timeout(WRITE_WAIT, socket.send(Message::Ping(Vec::new().into()))).await, Ok(Ok(()))) { break }
            },
            _ = tokio::time::sleep_until(last_pong + PONG_WAIT) => break,
        }
    }
    let _ = tokio::time::timeout(
        WRITE_WAIT,
        socket.send(Message::Close(Some(CloseFrame {
            code: 1000,
            reason: "".into(),
        }))),
    )
    .await;
}

// The event bus enforces its published audience; the DB is authoritative if
// ownership, guest history or membership changes before the socket delivers it.
async fn event_access(db: &impl GenericClient, workspace: Uuid, user: Uuid, event: &Event) -> bool {
    let Some(session) = event.chat_session_id else {
        return true;
    };
    if event.chat_session_owner_id != Some(user) {
        return false;
    }
    // The row is intentionally gone after DELETE. Its tombstone carries only
    // the already-known session ID and lets the prior viewer clear their UI.
    if event.event_type == "chat:session_deleted" {
        return true;
    }
    matches!(
        db.query_opt(
            "SELECT cs.creator_id,a.owner_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type FROM chat_session cs JOIN agent a ON a.id=cs.agent_id WHERE cs.id=$1 AND cs.workspace_id=$2",
            &[&session, &workspace],
        ).await,
        Ok(Some(row)) if can_read_session(
            row.get("creator_id"), row.get("owner_id"),
            row.get("zbs_has_guest_history"),
            row.get::<_, Option<String>>("zbs_source_channel_type").as_deref().unwrap_or(""), user,
        )
    )
}

fn event_frame(event: &Event) -> Value {
    let actor_id = if event.actor_id.is_nil() {
        String::new()
    } else {
        event.actor_id.to_string()
    };
    json!({"type":event.event_type,"payload":event.payload,"actor_type":event.actor_type,"actor_id":actor_id})
}

// Mirrors chatpolicy.WebSocketReadAccess: the creator while the session has no
// guest history, or the current agent owner of an owner-visible channel source.
fn can_read_session(
    creator: Uuid,
    owner: Option<Uuid>,
    guest_history: bool,
    channel: &str,
    user: Uuid,
) -> bool {
    (creator == user && !guest_history)
        || (owner == Some(user) && matches!(channel, "telegram" | "zalo"))
}

async fn subscription(
    socket: &mut WebSocket,
    state: &ApiState,
    workspace: Uuid,
    user: Uuid,
    frame: &Value,
) -> bool {
    let kind = frame["type"].as_str().unwrap_or("");
    let scope = frame["payload"]["scope"].as_str().unwrap_or("");
    let id = frame["payload"]["id"].as_str().unwrap_or("");
    let mut reason = "forbidden";
    let allowed = if scope.is_empty() || id.is_empty() {
        reason = "invalid payload";
        false
    } else if kind == "unsubscribe" {
        true
    } else if scope == "workspace" {
        id == workspace.to_string()
    } else if scope == "user" {
        id == user.to_string()
    } else if !matches!(scope, "chat" | "task") {
        reason = "unknown_scope";
        false
    } else if let Ok(resource) = Uuid::parse_str(id) {
        let Ok(db) = state.pool.get().await else {
            return send(socket, json!({"type":"subscribe_error","payload":{"scope":scope,"id":id,"error":"lookup_failed"}})).await;
        };
        // A chat task follows its session's read rule; an issue task is visible
        // to every member of the workspace that owns the issue.
        let query = if scope == "chat" {
            let session: Option<(Uuid, Option<Uuid>, bool, Option<String>)> = match db.query_opt("SELECT cs.creator_id,a.owner_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type FROM chat_session cs JOIN agent a ON a.id=cs.agent_id WHERE cs.id=$1 AND cs.workspace_id=$2", &[&resource, &workspace]).await {
                Ok(row) => row.map(|row| (row.get("creator_id"), row.get("owner_id"), row.get("zbs_has_guest_history"), row.get("zbs_source_channel_type"))),
                Err(_) => { reason = "lookup_failed"; None }
            };
            session.map(|(creator, owner, guest, channel)| {
                can_read_session(
                    creator,
                    owner,
                    guest,
                    channel.as_deref().unwrap_or(""),
                    user,
                )
            })
        } else {
            match db.query_opt("SELECT i.workspace_id AS issue_workspace,cs.workspace_id AS session_workspace,cs.creator_id,a.owner_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type FROM agent_task_queue t JOIN agent task_agent ON task_agent.id=t.agent_id AND task_agent.workspace_id=$2 LEFT JOIN chat_session cs ON cs.id=t.chat_session_id LEFT JOIN agent a ON a.id=cs.agent_id LEFT JOIN issue i ON i.id=t.issue_id WHERE t.id=$1", &[&resource, &workspace]).await {
                Ok(Some(row)) => Some(match row.get::<_, Option<Uuid>>("session_workspace") {
                    None => row.get::<_, Option<Uuid>>("issue_workspace") == Some(workspace),
                    Some(session_workspace) => session_workspace == workspace && can_read_session(row.get("creator_id"), row.get("owner_id"), row.get("zbs_has_guest_history"), row.get::<_, Option<String>>("zbs_source_channel_type").as_deref().unwrap_or(""), user),
                }),
                Ok(None) => Some(false),
                Err(_) => { reason = "lookup_failed"; None }
            }
        };
        query.unwrap_or_default()
    } else {
        false
    };
    let message = if allowed {
        format!("{kind}_ack")
    } else {
        format!("{kind}_error")
    };
    let payload = if allowed {
        json!({"scope":scope,"id":id})
    } else {
        json!({"scope":scope,"id":id,"error":reason})
    };
    send(socket, json!({"type":message,"payload":payload})).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
    async fn queued_private_event_rechecks_current_session_access() {
        use std::str::FromStr;
        let config =
            tokio_postgres::Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap())
                .unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
        assert!(config.get_hosts().iter().all(|host| matches!(
            host, tokio_postgres::config::Host::Tcp(value) if value == "127.0.0.1"
        )));
        assert!(config.get_ports().iter().all(|port| *port != 5432));
        let (db, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        let suffix = Uuid::new_v4().simple().to_string();
        let creator: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('WS creator',$1) RETURNING id",
                &[&format!("ws-creator-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let new_owner: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('WS owner',$1) RETURNING id",
                &[&format!("ws-owner-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let workspace: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES ('WS',$1) RETURNING id",
                &[&format!("ws-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        let agent: Uuid = db.query_one(
            "INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,status,runtime_mode) VALUES ('WS',$1,$2,'user','private','working','cloud') RETURNING id",
            &[&workspace, &creator],
        ).await.unwrap().get(0);
        let session: Uuid = db.query_one(
            "INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'WS',now()) RETURNING id",
            &[&workspace, &agent, &creator],
        ).await.unwrap().get(0);
        let event = Event::chat_session_read(workspace, creator, session);
        assert!(event_access(&db, workspace, creator, &event).await);
        db.execute(
            "UPDATE chat_session SET zbs_has_guest_history=true,zbs_source_channel_type='telegram' WHERE id=$1",
            &[&session],
        ).await.unwrap();
        db.execute(
            "UPDATE agent SET owner_id=$2 WHERE id=$1",
            &[&agent, &new_owner],
        )
        .await
        .unwrap();
        assert!(!event_access(&db, workspace, creator, &event).await);
        // A workspace-only event remains visible; a private event cannot be
        // reassigned to the new owner merely by editing its envelope.
        assert!(!event_access(&db, workspace, new_owner, &event).await);
        db.execute("DELETE FROM chat_session WHERE id=$1", &[&session])
            .await
            .unwrap();
        assert!(!event_access(&db, workspace, creator, &event).await);
        let deleted = Event::chat_session_deleted(workspace, creator, session);
        assert!(event_access(&db, workspace, creator, &deleted).await);
        assert!(!event_access(&db, workspace, new_owner, &deleted).await);
        connection.abort();
    }

    #[test]
    fn event_envelope_keeps_actor_and_private_payload() {
        let (workspace, user, session) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let event = Event::chat_session_read(workspace, user, session);
        assert_eq!(
            event_frame(&event),
            json!({"type":"chat:session_read","payload":{"chat_session_id":session},"actor_type":"member","actor_id":user})
        );
    }

    #[test]
    fn system_event_has_empty_actor_id_like_go() {
        let event = Event::task_queued(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let frame = event_frame(&event);
        assert_eq!(frame["actor_type"], "system");
        assert_eq!(frame["actor_id"], "");
    }

    #[test]
    fn cross_origin_upgrade_cannot_use_browser_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "private.example".parse().unwrap());
        headers.insert("origin", "https://other.example".parse().unwrap());
        assert!(!origin_allowed(&headers));
        headers.insert("origin", "https://private.example".parse().unwrap());
        assert!(origin_allowed(&headers));
    }
}
