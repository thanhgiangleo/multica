//! Bearer-authenticated daemon WebSocket transport: task wake hints, heartbeat
//! leases and the `tasks.claim` RPC. A missed hint or a dropped ack is safe:
//! the daemon's own heartbeat/claim schedule remains the correctness path.
//! This local hub does not implement Redis fanout or Go's Redis empty-claim
//! version cache.
//!
//! Parity notes against Go (`internal/daemonws`):
//! - Heartbeats reuse the connection's runtime lease, so the hot path does not
//!   re-read `agent_runtime`. Without Go's Redis liveness store, every beat
//!   writes the DB row; this keeps the DB-only sweeper's liveness correct.
//! - A deleted runtime reports `runtime_gone` in the ack instead of tearing the
//!   socket down, and is dropped from this connection's scope.
//! - `daemon:rpc_request` runs the shared claim core with the connection's own
//!   identity, bounded to `MAX_IN_FLIGHT_RPC` concurrent requests per socket.
use super::{ApiError, ApiState, GoJson, daemon::DaemonIdentity, error};
use axum::{
    Router,
    extract::{
        Extension, State,
        ws::{Message, WebSocket, WebSocketUpgrade, rejection::WebSocketUpgradeRejection},
    },
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_postgres::GenericClient;
use uuid::Uuid;

const WRITE_WAIT: Duration = Duration::from_secs(10);
const PONG_WAIT: Duration = Duration::from_secs(60);
/// Go bounds concurrent RPC handlers per connection at 8.
const MAX_IN_FLIGHT_RPC: usize = 8;
const SERVER_CAPABILITIES: [&str; 2] = ["rpc-v1", "task-steer-v1"];

const EVENT_DEDUP_CAPACITY: usize = 128;

#[derive(Default)]
pub struct Hub {
    next_id: AtomicU64,
    state: Mutex<HubState>,
}

#[derive(Default)]
struct HubState {
    clients: HashMap<u64, HubClient>,
    by_runtime: HashMap<Uuid, HashSet<u64>>,
    by_workspace: HashMap<Uuid, HashSet<u64>>,
    by_user: HashMap<Uuid, HashSet<u64>>,
}

struct HubClient {
    sender: mpsc::Sender<String>,
    runtimes: HashSet<Uuid>,
    workspaces: HashSet<Uuid>,
    user: Option<Uuid>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
}

impl HubClient {
    fn mark_seen(&mut self, event_id: &str) -> bool {
        if event_id.is_empty() {
            return true;
        }
        if !self.seen.insert(event_id.to_owned()) {
            return false;
        }
        self.seen_order.push_back(event_id.to_owned());
        if self.seen_order.len() > EVENT_DEDUP_CAPACITY
            && let Some(old) = self.seen_order.pop_front()
        {
            self.seen.remove(&old);
        }
        true
    }
}

fn remove_index(index: &mut HashMap<Uuid, HashSet<u64>>, key: Uuid, client: u64) {
    if let Some(watchers) = index.get_mut(&key) {
        watchers.remove(&client);
        if watchers.is_empty() {
            index.remove(&key);
        }
    }
}

impl HubState {
    fn unregister(&mut self, id: u64) {
        if let Some(client) = self.clients.remove(&id) {
            for runtime in client.runtimes {
                remove_index(&mut self.by_runtime, runtime, id);
            }
            for workspace in client.workspaces {
                remove_index(&mut self.by_workspace, workspace, id);
            }
            if let Some(user) = client.user {
                remove_index(&mut self.by_user, user, id);
            }
        }
    }

    fn deliver(&mut self, recipients: Vec<u64>, frame: &str, event_id: &str) {
        let mut slow = Vec::new();
        for id in recipients {
            if let Some(client) = self.clients.get_mut(&id) {
                if !client.mark_seen(event_id) {
                    continue;
                }
                if client.sender.try_send(frame.to_owned()).is_err() {
                    slow.push(id);
                }
            }
        }
        for id in slow {
            self.unregister(id);
        }
    }
}

impl Hub {
    fn register(
        &self,
        ids: &[Uuid],
        workspaces: impl IntoIterator<Item = Uuid>,
        user: Option<Uuid>,
        sender: mpsc::Sender<String>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let runtimes: HashSet<_> = ids.iter().copied().collect();
        let workspaces: HashSet<_> = workspaces.into_iter().collect();
        let mut state = self.state.lock().unwrap();
        for runtime in &runtimes {
            state.by_runtime.entry(*runtime).or_default().insert(id);
        }
        for workspace in &workspaces {
            state.by_workspace.entry(*workspace).or_default().insert(id);
        }
        if let Some(user) = user {
            state.by_user.entry(user).or_default().insert(id);
        }
        state.clients.insert(
            id,
            HubClient {
                sender,
                runtimes,
                workspaces,
                user,
                seen: HashSet::new(),
                seen_order: VecDeque::new(),
            },
        );
        id
    }

    fn unregister(&self, id: u64) {
        self.state.lock().unwrap().unregister(id);
    }

    fn allows_runtime(&self, runtime: Uuid, id: u64) -> bool {
        self.state
            .lock()
            .unwrap()
            .clients
            .get(&id)
            .is_some_and(|client| client.runtimes.contains(&runtime))
    }

    /// A heartbeat discovering a deleted row revokes the connection's runtime
    /// scope just like an externally delivered runtime_gone event.
    fn forget(&self, runtime: Uuid, id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(client) = state.clients.get_mut(&id) {
            client.runtimes.remove(&runtime);
        }
        remove_index(&mut state.by_runtime, runtime, id);
    }

    fn notify_runtime_frame(&self, runtime: Uuid, frame: String, event_id: &str) {
        let mut state = self.state.lock().unwrap();
        let recipients = state
            .by_runtime
            .get(&runtime)
            .map(|watchers| watchers.iter().copied().collect())
            .unwrap_or_default();
        state.deliver(recipients, &frame, event_id);
    }

    fn notify_workspace_frame(&self, workspace: Uuid, frame: String, event_id: &str) {
        let mut state = self.state.lock().unwrap();
        let recipients = state
            .by_workspace
            .get(&workspace)
            .map(|watchers| watchers.iter().copied().collect())
            .unwrap_or_default();
        state.deliver(recipients, &frame, event_id);
    }

    fn notify_user_frame(&self, user: Uuid, frame: String, event_id: &str) {
        let mut state = self.state.lock().unwrap();
        let recipients = state
            .by_user
            .get(&user)
            .map(|watchers| watchers.iter().copied().collect())
            .unwrap_or_default();
        state.deliver(recipients, &frame, event_id);
    }

    /// Never emit a terminal task ID: a terminal wake means that a successor
    /// might now be claimable, not that the retired task can be claimed again.
    pub fn notify_task_available(&self, runtime: Uuid, task: Option<Uuid>) {
        self.notify_runtime_frame(runtime, task_available_frame(runtime, task).to_string(), "");
    }

    pub fn notify_task_supplement_available(&self, runtime: Uuid, task: Uuid) {
        if runtime.is_nil() || task.is_nil() {
            return;
        }
        self.notify_runtime_frame(
            runtime,
            json!({
                "type":"daemon:task_supplement_available",
                "payload":{"runtime_id":runtime,"task_id":task},
            })
            .to_string(),
            "",
        );
    }

    pub fn notify_runtime_profiles_changed(&self, workspace: Uuid, profile: Option<Uuid>) {
        let mut payload = json!({"workspace_id":workspace});
        if let Some(profile) = profile {
            payload["runtime_profile_id"] = json!(profile);
        }
        self.notify_workspace_frame(
            workspace,
            json!({
                "type":"daemon:runtime_profiles_changed","payload":payload
            })
            .to_string(),
            "",
        );
    }

    pub fn notify_workspaces_changed(&self, user: Uuid) {
        self.notify_user_frame(
            user,
            json!({
                "type":"daemon:workspaces_changed","payload":{}
            })
            .to_string(),
            "",
        );
    }

    pub fn notify_pending_work(&self, runtime: Uuid, kind: &str) {
        let mut payload = json!({"runtime_id":runtime});
        if !kind.is_empty() {
            payload["kind"] = json!(kind);
        }
        self.notify_runtime_frame(
            runtime,
            json!({
                "type":"daemon:pending_work","payload":payload
            })
            .to_string(),
            "",
        );
    }

    /// Invalidate first, even when the outbound queue is full: a later
    /// heartbeat must not keep a deleted runtime online.
    pub fn notify_runtime_gone(&self, runtime: Uuid) {
        self.invalidate_runtime(
            runtime,
            json!({
                "type":"daemon:heartbeat_ack",
                "payload":{"runtime_id":runtime,"status":"runtime_gone","runtime_gone":true}
            })
            .to_string(),
            "",
        );
    }

    fn invalidate_runtime(&self, runtime: Uuid, frame: String, event_id: &str) {
        let mut state = self.state.lock().unwrap();
        let recipients = state.by_runtime.remove(&runtime).unwrap_or_default();
        for id in &recipients {
            if let Some(client) = state.clients.get_mut(id) {
                client.runtimes.remove(&runtime);
            }
        }
        state.deliver(recipients.into_iter().collect(), &frame, event_id);
    }

    /// Accept a validated relay frame from a future transport integration.
    /// No Redis subscription or publication is installed by this local hub.
    pub fn deliver_daemon_runtime(&self, scope: &str, frame: &str, event_id: &str) {
        let Ok(msg) = serde_json::from_str::<Value>(frame) else {
            return;
        };
        let payload = msg.get("payload");
        let id = |name| {
            payload
                .and_then(|p| p.get(name))
                .and_then(Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok())
        };
        match msg.get("type").and_then(Value::as_str) {
            Some("daemon:task_available" | "daemon:task_supplement_available")
                if id("runtime_id").is_some()
                    && (msg["type"] != "daemon:task_supplement_available"
                        || id("task_id").is_some()) =>
            {
                self.notify_runtime_frame(id("runtime_id").unwrap(), frame.to_owned(), event_id);
            }
            Some("daemon:runtime_profiles_changed") if id("workspace_id").is_some() => {
                self.notify_workspace_frame(
                    id("workspace_id").unwrap(),
                    frame.to_owned(),
                    event_id,
                );
            }
            Some("daemon:workspaces_changed") => {
                if let Ok(user) = Uuid::parse_str(scope) {
                    self.notify_user_frame(user, frame.to_owned(), event_id);
                }
            }
            Some("daemon:pending_work") if id("runtime_id").is_some() => {
                self.notify_runtime_frame(id("runtime_id").unwrap(), frame.to_owned(), event_id);
            }
            Some("daemon:heartbeat_ack")
                if id("runtime_id").is_some()
                    && payload
                        .and_then(|p| p.get("status"))
                        .and_then(Value::as_str)
                        == Some("runtime_gone")
                    && payload
                        .and_then(|p| p.get("runtime_gone"))
                        .and_then(Value::as_bool)
                        == Some(true) =>
            {
                self.invalidate_runtime(id("runtime_id").unwrap(), frame.to_owned(), event_id);
            }
            _ => {}
        }
    }
}

fn task_available_frame(runtime: Uuid, task: Option<Uuid>) -> Value {
    let mut payload = json!({"runtime_id": runtime});
    if let Some(task) = task {
        payload["task_id"] = json!(task);
    }
    json!({"type":"daemon:task_available", "payload":payload})
}

/// Connection-scoped liveness state for one already-authorized runtime.
/// Ownership is immutable after the upgrade; status advances after a write.
struct Lease {
    workspace: Uuid,
    status: String,
    last_seen_at: Option<DateTime<Utc>>,
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/daemon/ws", get(connect))
}

fn runtime_ids(uri: &Uri) -> Vec<String> {
    let pairs: Vec<_> = form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).collect();
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    for name in ["runtime_id", "runtime_ids"] {
        for (_, value) in pairs.iter().filter(|(key, _)| key == name) {
            for part in value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
            {
                if seen.insert(part.to_owned()) {
                    ids.push(part.to_owned());
                }
            }
        }
    }
    ids
}

fn runtime_binding_allowed(identity: &DaemonIdentity, daemon: Option<&str>, owner: Option<Uuid>) -> bool {
    match identity {
        DaemonIdentity::User(user) => owner == Some(*user),
        DaemonIdentity::Bound { daemon: expected, owner: token_owner, .. } => {
            daemon == Some(expected.as_str())
                && !expected.trim().is_empty()
                && (expected.starts_with("qoder-managed-")
                    || token_owner.is_some() && *token_owner == owner)
        }
    }
}

/// Validation order mirrors Go: hub availability, then the identity
/// requirement, then per-runtime authorization, and only then the upgrade
/// itself. A request that is not a WebSocket handshake reaches Go's gorilla
/// upgrader and gets its 400 — hence the rejection is flattened here instead of
/// letting axum answer before the authorization checks run.
async fn connect(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    uri: Uri,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Result<Response, ApiError> {
    let Some(hub) = state.daemon_wake.clone() else {
        return Ok((
            StatusCode::INTERNAL_SERVER_ERROR,
            GoJson(json!({"error":"daemon websocket unavailable","code":"daemon_websocket_misconfigured"})),
        )
            .into_response());
    };
    let raw_ids = runtime_ids(&uri);
    // Only the authenticated identity can establish account-wide scope;
    // an untrusted X-User-ID header must not subscribe a bound daemon.
    let user = match &identity {
        DaemonIdentity::User(user) => Some(*user),
        DaemonIdentity::Bound { .. } => None,
    };
    if raw_ids.is_empty() && user.is_none() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "runtime_ids or user identity required",
        ));
    }
    let ids: Vec<_> = raw_ids
        .iter()
        .map(|raw| {
            Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_ids"))
        })
        .collect::<Result<_, _>>()?;
    let mut leases = HashMap::new();
    if !ids.is_empty() {
        let db = state
            .pool
            .get()
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtimes"))?;
        let rows = db
            .query(
                "SELECT id,workspace_id,daemon_id,status,last_seen_at,owner_id FROM agent_runtime WHERE id=ANY($1::uuid[])",
                &[&ids],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtimes"))?;
        let by_id: HashMap<Uuid, _> = rows
            .iter()
            .map(|row| (row.get::<_, Uuid>(0), row))
            .collect();
        for id in &ids {
            let row = by_id
                .get(id)
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
            let daemon: Option<String> = row.get(2);
            let owner: Option<Uuid> = row.get(5);
            let workspace: Uuid = row.get(1);
            super::daemon::workspace_access(&**db, &identity, &workspace.to_string()).await?;
            if !runtime_binding_allowed(&identity, daemon.as_deref(), owner) {
                return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
            }
            leases.insert(
                *id,
                Lease {
                    workspace,
                    status: row.get(3),
                    last_seen_at: row.get(4),
                },
            );
        }
    }
    let Ok(upgrade) = upgrade else {
        // gorilla's upgrader answers a non-handshake request with http.Error.
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "websocket upgrade failed".into(),
            true,
        ));
    };
    let capabilities = headers
        .get("x-client-capabilities")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    Ok(upgrade
        .max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| serve(socket, hub, state, identity, ids, leases, capabilities))
        .into_response())
}

async fn send(socket: &mut WebSocket, frame: Message) -> bool {
    matches!(
        tokio::time::timeout(WRITE_WAIT, socket.send(frame)).await,
        Ok(Ok(()))
    )
}

async fn serve(
    mut socket: WebSocket,
    hub: Arc<Hub>,
    state: ApiState,
    identity: DaemonIdentity,
    ids: Vec<Uuid>,
    mut leases: HashMap<Uuid, Lease>,
    capabilities: String,
) {
    let (sender, mut frames) = mpsc::channel(16);
    let user = match &identity {
        DaemonIdentity::User(user) => Some(*user),
        DaemonIdentity::Bound { .. } => None,
    };
    let client = hub.register(
        &ids,
        leases.values().map(|lease| lease.workspace),
        user,
        sender.clone(),
    );
    let in_flight = Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT_RPC));
    let mut rpc_tasks = tokio::task::JoinSet::new();
    let mut ping = tokio::time::interval(Duration::from_secs(54));
    ping.tick().await;
    let mut last_pong = Instant::now();
    loop {
        tokio::select! {
            received = socket.recv() => match received {
                Some(Ok(Message::Pong(_))) | Some(Ok(Message::Ping(_))) => { last_pong = Instant::now(); }
                Some(Ok(Message::Text(raw))) => {
                    if raw.len() > 64 * 1024 { break; }
                    let Ok(msg) = serde_json::from_str::<Value>(&raw) else { continue };
                    match msg.get("type").and_then(Value::as_str) {
                        Some("daemon:heartbeat") => {
                            let payload = msg.get("payload").cloned().unwrap_or(Value::Null);
                            let runtime = payload
                                .get("runtime_id")
                                .and_then(Value::as_str)
                                .and_then(|raw| Uuid::parse_str(raw).ok());
                            let Some(runtime) = runtime else { continue };
                            // A connection authenticated for a fixed runtime set:
                            // reject any heartbeat outside it.
                            if !hub.allows_runtime(runtime, client) { continue; }
                            let batch = payload
                                .get("supports_batch_import")
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            let Ok(gone) = record_liveness(&state, runtime, &mut leases).await else {
                                // No ack on a DB write failure: the daemon
                                // falls back to its stateless HTTP heartbeat.
                                continue;
                            };
                            if gone {
                                hub.forget(runtime, client);
                                leases.remove(&runtime);
                            }
                            let Ok(ack) = heartbeat_ack(&state, runtime, batch, gone).await else {
                                // Pending actions could not be checked; retry
                                // through the daemon's HTTP heartbeat path.
                                continue;
                            };
                            let frame = json!({"type":"daemon:heartbeat_ack","payload":ack});
                            if sender.try_send(frame.to_string()).is_err() { break; }
                        }
                        Some("daemon:rpc_request") => {
                            let payload = msg.get("payload").cloned().unwrap_or(Value::Null);
                            match in_flight.clone().try_acquire_owned() {
                                // Saturated: tell the daemon to fall back rather
                                // than queueing unbounded work on one socket.
                                Err(_) => {
                                    let Some(request_id) = request_id(&payload) else { continue };
                                    let frame = rpc_error_frame(request_id, 429, "too many in-flight rpc requests");
                                    if sender.try_send(frame).is_err() { break; }
                                }
                                Ok(permit) => {
                                    let Some(_) = request_id(&payload) else { continue };
                                    let outbound = sender.clone();
                                    // One JoinSet per connection: the daemon's
                                    // pending claims still finish and reply
                                    // while the read pump stays free.
                                    let state = state.clone();
                                    let identity = identity.clone();
                                    let capabilities = capabilities.clone();
                                    rpc_tasks.spawn(async move {
                                        let frame = run_rpc(
                                            state,
                                            identity,
                                            capabilities,
                                            payload,
                                            permit,
                                        )
                                        .await;
                                        if !frame.is_empty() {
                                            let _ = outbound.try_send(frame);
                                        }
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            Some(_) = rpc_tasks.join_next(), if !rpc_tasks.is_empty() => {}
            frame = frames.recv() => {
                let Some(frame) = frame else { break; };
                if !send(&mut socket, Message::Text(frame.into())).await { break; }
            },
            _ = ping.tick() => if !send(&mut socket, Message::Ping(Vec::new().into())).await { break; },
            _ = tokio::time::sleep_until(last_pong + PONG_WAIT) => break,
        }
    }
    hub.unregister(client);
}

fn request_id(payload: &Value) -> Option<&str> {
    payload
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

fn rpc_error_frame(request_id: &str, status: u16, error: &str) -> String {
    json!({"type":"daemon:rpc_response","payload":{"request_id":request_id,"status":status,"error":error}})
        .to_string()
}

/// Run one `daemon:rpc_request` and build its `daemon:rpc_response` frame.
/// `tasks.claim` reuses the HTTP claim core with the connection's own identity,
/// so authorization, ordering and the minted task token are identical.
async fn run_rpc(
    state: ApiState,
    identity: DaemonIdentity,
    capabilities: String,
    payload: Value,
    _permit: tokio::sync::OwnedSemaphorePermit,
) -> String {
    let request_id = request_id(&payload).unwrap_or_default().to_owned();
    let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
    if request_id.is_empty() {
        return String::new();
    }
    if method != "tasks.claim" {
        return rpc_error_frame(&request_id, 404, &format!("unknown rpc method {method:?}"));
    }
    let body = payload
        .get("body")
        .map(|body| body.to_string())
        .unwrap_or_else(|| "{}".into());
    let mut headers = HeaderMap::new();
    if !capabilities.is_empty()
        && let Ok(value) = capabilities.parse()
    {
        headers.insert("x-client-capabilities", value);
    }
    let budget = payload
        .get("timeout_ms")
        .and_then(Value::as_i64)
        .filter(|ms| *ms > 0);
    let call = super::p0_daemon_claim::claim_tasks(&state, &identity, &headers, body.as_bytes());
    let outcome = match budget {
        // Cancelling an in-flight transaction rolls it back. Claims that
        // committed earlier in this multi-step core still rely on stale-claim
        // recovery if delivery has not completed when the budget expires.
        Some(ms) => match tokio::time::timeout(Duration::from_millis(ms as u64), call).await {
            Ok(result) => result,
            Err(_) => return rpc_error_frame(&request_id, 500, "context deadline exceeded"),
        },
        None => call.await,
    };
    match outcome {
        Ok(outcome) => {
            let mut payload = json!({"request_id":request_id,"status":outcome.status.as_u16()});
            payload["body"] = outcome.body;
            json!({"type":"daemon:rpc_response","payload":payload}).to_string()
        }
        Err(ApiError(status, message, _)) => json!({
            "type":"daemon:rpc_response",
            "payload":{"request_id":request_id,"status":status.as_u16(),"body":{"error":message}}
        })
        .to_string(),
    }
}

/// Record liveness from the connection lease. Returns `true` when the runtime
/// row no longer exists, which the ack reports as `runtime_gone`.
async fn record_liveness(
    state: &ApiState,
    runtime: Uuid,
    leases: &mut HashMap<Uuid, Lease>,
) -> Result<bool, ()> {
    let Some(lease) = leases.get(&runtime) else {
        return Err(());
    };
    let now = Utc::now();
    let db = state.pool.get().await.map_err(|_| ())?;
    let workspace = lease.workspace;
    let flipping = lease.status != "online" || lease.last_seen_at.is_none();
    // Mirror Go's passthrough scheduler: bump last_seen_at on an already-online
    // row, and only fall back to the status write (whose zero-row result means
    // the row is gone) when that bump matches nothing.
    let (gone, recovered) = if flipping {
        flip_online(&**db, runtime).await?
    } else {
        match db
            .execute(
                "UPDATE agent_runtime SET last_seen_at=now() WHERE id=$1 AND status='online'",
                &[&runtime],
            )
            .await
            .map_err(|_| ())?
        {
            0 => flip_online(&**db, runtime).await?,
            _ => (false, false),
        }
    };
    if gone {
        return Ok(true);
    }
    if let Some(lease) = leases.get_mut(&runtime) {
        lease.status = "online".into();
        lease.last_seen_at = Some(now);
    }
    if recovered {
        // Reuse daemon:register so web and mobile invalidate both the runtime
        // and agent projections. Ordinary online beats stay silent.
        state.events.publish(super::runtime_chat_events::Event {
            event_type: "daemon:register".into(),
            workspace_id: workspace,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({"action":"heartbeat_recovery"}),
        });
    }
    Ok(gone)
}

/// Flip a runtime online and report `(gone, recovered)`. A concurrent beat
/// can win the status transition; only a confirmed absent row means gone.
async fn flip_online(db: &impl GenericClient, runtime: Uuid) -> Result<(bool, bool), ()> {
    let rows = db
        .execute(
            "UPDATE agent_runtime SET status='online',last_seen_at=now(),updated_at=now() WHERE id=$1 AND status<>'online'",
            &[&runtime],
        )
        .await
        .map_err(|_| ())?;
    if rows > 0 {
        return Ok((false, true));
    }
    let row = db
        .query_opt(
            "UPDATE agent_runtime SET status='online',last_seen_at=now(),updated_at=now() WHERE id=$1 RETURNING id",
            &[&runtime],
        )
        .await
        .map_err(|_| ())?;
    Ok((row.is_none(), false))
}

async fn heartbeat_ack(
    state: &ApiState,
    runtime: Uuid,
    supports_batch_import: bool,
    gone: bool,
) -> Result<Value, ()> {
    if gone {
        return Ok(json!({"runtime_id":runtime,"status":"runtime_gone","runtime_gone":true}));
    }
    let mut ack = json!({
        "runtime_id": runtime,
        "status": "ok",
        "server_capabilities": SERVER_CAPABILITIES,
    });
    let db = state.pool.get().await.map_err(|_| ())?;
    // Same pending-action claims as the HTTP heartbeat; failures are advisory.
    if let Some(actions) = ack.as_object_mut() {
        actions.extend(
            super::p0_daemon_registration::heartbeat_actions(
                state,
                &**db,
                runtime,
                supports_batch_import,
            )
            .await,
        );
    }
    Ok(ack)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_runtime_binding_requires_owner_and_daemon() {
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();
        let user = DaemonIdentity::User(owner);
        assert!(runtime_binding_allowed(&user, None, Some(owner)));
        assert!(!runtime_binding_allowed(&user, None, Some(other)));
        assert!(!runtime_binding_allowed(&user, None, None));
        let bound = DaemonIdentity::Bound {
            workspace: Uuid::new_v4(),
            daemon: "synthetic-daemon".into(),
            owner: Some(owner),
            hash: String::new(),
        };
        assert!(runtime_binding_allowed(&bound, Some("synthetic-daemon"), Some(owner)));
        assert!(!runtime_binding_allowed(&bound, Some("synthetic-daemon"), Some(other)));
        assert!(!runtime_binding_allowed(&bound, Some("other-daemon"), Some(owner)));
        assert!(!runtime_binding_allowed(&bound, None, Some(owner)));
    }

    #[test]
    fn scope_and_terminal_payload() {
        let hub = Hub::default();
        let runtime = Uuid::new_v4();
        let other = Uuid::new_v4();
        let task = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(16);
        let id = hub.register(&[runtime], [], None, tx);
        hub.notify_task_available(other, Some(task));
        assert!(rx.try_recv().is_err());
        hub.notify_task_available(runtime, Some(task));
        let queued: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(queued.pointer("/payload/task_id"), Some(&json!(task)));
        hub.notify_task_available(runtime, None);
        let terminal: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert!(terminal.pointer("/payload/task_id").is_none());
        hub.forget(runtime, id);
        hub.notify_task_available(runtime, None);
        assert!(rx.try_recv().is_err());
        hub.unregister(id);
        hub.notify_task_available(runtime, None);
        assert!(rx.try_recv().is_err());
    }

    fn next(rx: &mut mpsc::Receiver<String>) -> Value {
        serde_json::from_str(&rx.try_recv().expect("notification missing")).unwrap()
    }

    #[test]
    fn workspace_and_user_fanout_uses_authorized_scopes_and_go_payloads() {
        let hub = Hub::default();
        let workspace = Uuid::new_v4();
        let second = Uuid::new_v4();
        let other_workspace = Uuid::new_v4();
        let user = Uuid::new_v4();
        let other_user = Uuid::new_v4();
        let profile = Uuid::new_v4();
        let runtime = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(16);
        let id = hub.register(&[runtime], [workspace, second, workspace], Some(user), tx);
        let (other_tx, mut other_rx) = mpsc::channel(16);
        hub.register(&[], [other_workspace], Some(other_user), other_tx);
        hub.notify_runtime_profiles_changed(workspace, Some(profile));
        assert_eq!(
            next(&mut rx),
            json!({
                "type":"daemon:runtime_profiles_changed",
                "payload":{"workspace_id":workspace,"runtime_profile_id":profile}
            })
        );
        hub.notify_runtime_profiles_changed(second, None);
        assert_eq!(
            next(&mut rx),
            json!({
                "type":"daemon:runtime_profiles_changed","payload":{"workspace_id":second}
            })
        );
        hub.notify_workspaces_changed(user);
        assert_eq!(
            next(&mut rx),
            json!({"type":"daemon:workspaces_changed","payload":{}})
        );
        hub.notify_pending_work(runtime, "model_list");
        assert_eq!(
            next(&mut rx),
            json!({
                "type":"daemon:pending_work","payload":{"runtime_id":runtime,"kind":"model_list"}
            })
        );
        hub.notify_pending_work(runtime, "");
        assert_eq!(
            next(&mut rx),
            json!({
                "type":"daemon:pending_work","payload":{"runtime_id":runtime}
            })
        );
        assert!(other_rx.try_recv().is_err());
        hub.unregister(id);
        hub.notify_runtime_profiles_changed(workspace, None);
        hub.notify_workspaces_changed(user);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn runtime_gone_revokes_only_runtime_scope_even_when_queue_full() {
        let hub = Hub::default();
        let runtime = Uuid::new_v4();
        let other = Uuid::new_v4();
        let workspace = Uuid::new_v4();
        let user = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(16);
        let id = hub.register(&[runtime, other], [workspace], Some(user), tx);
        hub.notify_runtime_gone(runtime);
        assert_eq!(
            next(&mut rx),
            json!({
                "type":"daemon:heartbeat_ack",
                "payload":{"runtime_id":runtime,"status":"runtime_gone","runtime_gone":true}
            })
        );
        assert!(!hub.allows_runtime(runtime, id));
        assert!(hub.allows_runtime(other, id));
        hub.notify_task_available(runtime, None);
        hub.notify_pending_work(runtime, "local_skills");
        assert!(rx.try_recv().is_err());
        hub.notify_task_available(other, None);
        assert_eq!(next(&mut rx), task_available_frame(other, None));
        hub.notify_runtime_profiles_changed(workspace, None);
        assert_eq!(next(&mut rx)["type"], "daemon:runtime_profiles_changed");
        hub.notify_workspaces_changed(user);
        assert_eq!(next(&mut rx)["type"], "daemon:workspaces_changed");
        hub.unregister(id);

        let (full_tx, mut full_rx) = mpsc::channel(1);
        let full_id = hub.register(&[runtime], [], None, full_tx);
        hub.notify_task_available(runtime, None);
        hub.notify_runtime_gone(runtime);
        assert!(!hub.allows_runtime(runtime, full_id));
        assert_eq!(next(&mut full_rx), task_available_frame(runtime, None));
    }

    #[test]
    fn relay_deduplicates_per_client_with_bounded_event_history() {
        let hub = Hub::default();
        let runtime = Uuid::new_v4();
        let workspace = Uuid::new_v4();
        let user = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(EVENT_DEDUP_CAPACITY + 4);
        let id = hub.register(&[runtime], [workspace], Some(user), tx);
        let frame = task_available_frame(runtime, None).to_string();
        hub.deliver_daemon_runtime("", &frame, "first");
        hub.deliver_daemon_runtime("", &frame, "first");
        assert_eq!(next(&mut rx), task_available_frame(runtime, None));
        assert!(rx.try_recv().is_err());
        let profiles = json!({"type":"daemon:runtime_profiles_changed",
            "payload":{"workspace_id":workspace}})
        .to_string();
        hub.deliver_daemon_runtime("", &profiles, "first");
        assert!(rx.try_recv().is_err());
        for i in 0..EVENT_DEDUP_CAPACITY {
            hub.deliver_daemon_runtime("", &frame, &format!("event-{i}"));
            assert_eq!(next(&mut rx), task_available_frame(runtime, None));
        }
        hub.deliver_daemon_runtime("", &profiles, "first");
        assert_eq!(next(&mut rx)["type"], "daemon:runtime_profiles_changed");
        let workspaces = json!({"type":"daemon:workspaces_changed","payload":{}}).to_string();
        hub.deliver_daemon_runtime(&user.to_string(), &workspaces, "account");
        assert_eq!(next(&mut rx)["type"], "daemon:workspaces_changed");
        hub.deliver_daemon_runtime(&Uuid::new_v4().to_string(), &workspaces, "other");
        assert!(rx.try_recv().is_err());
        hub.unregister(id);
    }

    #[test]
    fn relayed_runtime_gone_invalidates_heartbeat_scope() {
        let hub = Hub::default();
        let runtime = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(16);
        let id = hub.register(&[runtime], [], None, tx);
        let invalid = json!({"type":"daemon:heartbeat_ack",
            "payload":{"runtime_id":runtime,"status":"ok","runtime_gone":true}})
        .to_string();
        hub.deliver_daemon_runtime("", &invalid, "wrong");
        assert!(hub.allows_runtime(runtime, id));
        let gone = json!({"type":"daemon:heartbeat_ack",
            "payload":{"runtime_id":runtime,"status":"runtime_gone","runtime_gone":true}})
        .to_string();
        hub.deliver_daemon_runtime("", &gone, "gone");
        assert_eq!(next(&mut rx), serde_json::from_str::<Value>(&gone).unwrap());
        assert!(!hub.allows_runtime(runtime, id));
        hub.deliver_daemon_runtime("", &gone, "gone");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn parse_both_query_parameters_with_stable_deduplication() {
        let uri: Uri = "/api/daemon/ws?runtime_ids=b,a&runtime_id=a&runtime_id=c,,b"
            .parse()
            .unwrap();
        assert_eq!(runtime_ids(&uri), vec!["a", "c", "b"]);
    }
}
