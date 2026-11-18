//! Synthetic HTTP/DB/WS preparation lifecycle; never invokes an agent/provider.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Config, NoTls};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tower::ServiceExt;
use uuid::Uuid;
const SECRET: &[u8] = b"zbsi67-preparation-synthetic-only";
include!("runtime_chat_support/delivery_cases.inc.rs");
include!("runtime_chat_support/start_cases.inc.rs");
fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap()
}
async fn post(
    app: &axum::Router,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method("POST").uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    // Go's decoder does not require content-type for daemon POSTs.
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    owner: Uuid,
    peer: Uuid,
    ws: Uuid,
    foreign: Uuid,
    agent: Uuid,
    runtime: Uuid,
    peer_runtime: Uuid,
    issue: Uuid,
    chat: Uuid,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
        assert!(
            config
                .get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        assert!(config.get_ports().iter().all(|p| *p != 5432));
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(12)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let [
            owner,
            peer,
            ws,
            foreign,
            agent,
            runtime,
            peer_runtime,
            issue,
            chat,
        ]: [Uuid; 9] = std::array::from_fn(|_| Uuid::new_v4());
        for user in [owner, peer] {
            db.execute(
                "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Preparation',$2)",
                &[&user, &format!("prepare-{user}@invalid.example")],
            )
            .await
            .unwrap();
        }
        for workspace in [ws, foreign] {
            db.execute(
                "INSERT INTO workspace(id,name,slug) VALUES ($1,'Preparation',$2)",
                &[&workspace, &format!("prepare-{workspace}")],
            )
            .await
            .unwrap();
        }
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
            &[&ws, &owner, &peer],
        )
        .await
        .unwrap();
        for (rt, daemon, user) in [
            (runtime, "prepare-daemon", owner),
            (peer_runtime, "peer-daemon", peer),
        ] {
            db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id) VALUES ($1,$2,$3,'Preparation','cloud','synthetic','online',$4)", &[&rt,&ws,&daemon,&user]).await.unwrap();
        }
        db.execute("INSERT INTO agent(id,workspace_id,name,owner_id,runtime_mode,status) VALUES ($1,$2,'Preparation',$3,'cloud','working')", &[&agent,&ws,&owner]).await.unwrap();
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id) VALUES ($1,$2,'Preparation','member',$3)", &[&issue,&ws,&owner]).await.unwrap();
        db.execute("INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,$4,'Private')", &[&chat,&ws,&agent,&owner]).await.unwrap();
        let token = format!("mdt_prepare_{ws}");
        db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'prepare-daemon',$3,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&ws,&owner]).await.unwrap();
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()),
        );
        drop(db);
        Self {
            pool,
            app,
            owner,
            peer,
            ws,
            foreign,
            agent,
            runtime,
            peer_runtime,
            issue,
            chat,
            token,
        }
    }
    async fn task(&self, status: &str, chat: bool) -> Uuid {
        // Migration 516 derives pending scope from wakeup_id or trigger comment;
        // direct comment_thread_id inserts are overwritten by the DB trigger.
        let context =
            json!({"wakeup_id":Uuid::new_v4(),"wakeup_joined":[{"note":"  one  "},{"note":"two"}]});
        self.pool.get().await.unwrap().query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,chat_session_id,status,dispatched_at,context) VALUES ($1,$2,$3,$4,$5,now(),$6) RETURNING id", &[&self.agent,&self.runtime,&if chat {None} else {Some(self.issue)},&if chat {Some(self.chat)} else {None},&status,&context]).await.unwrap().get(0)
    }
    fn lease(&self, id: Uuid) -> String {
        format!(
            "/api/daemon/runtimes/{}/tasks/{id}/prepare-lease",
            self.runtime
        )
    }
    fn wait(&self, id: Uuid) -> String {
        format!("/api/daemon/tasks/{id}/wait-local-directory")
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn lease_and_wait_have_real_db_effects_full_task_response_and_binding_fences() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let id = f.task("dispatched", false).await;
    let lease = f.lease(id);
    for token in [None, Some("mat_not-a-daemon"), Some("mdt_invalid")] {
        assert_eq!(
            post(&f.app, &lease, token, "").await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let cookie = f
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&lease)
                .header("cookie", format!("multica_auth={}", jwt(f.owner)))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        cookie.status(),
        StatusCode::UNAUTHORIZED,
        "daemon API must never inherit cookies"
    );
    assert_eq!(
        post(&f.app, &lease, Some(&jwt(f.peer)), "").await.0,
        StatusCode::NOT_FOUND
    );
    let (status, row) = post(&f.app, &lease, Some(&f.token), "ignored body").await;
    assert_eq!(status, StatusCode::OK, "{row}");
    assert_eq!(row["id"], id.to_string());
    assert_eq!(row["runtime_id"], f.runtime.to_string());
    assert_eq!(row["workspace_id"], f.ws.to_string());
    assert_eq!(row["issue_id"], f.issue.to_string());
    assert_eq!(row["kind"], "direct");
    assert_eq!(row["delivered_comment_ids"], json!([]));
    assert_eq!(
        row["attribution"],
        json!({"source":"unattributed","precise":false})
    );
    assert_eq!(row["wakeup_joined"], "one\n\ntwo");
    for key in ["started_at", "completed_at", "result", "error"] {
        assert!(row[key].is_null(), "{key}");
    }
    for key in [
        "trigger_summary",
        "parent_task_id",
        "coalesced_comment_ids",
        "auth_token",
        "context",
        "prepare_lease_expires_at",
        "wait_reason",
    ] {
        assert!(row.get(key).is_none(), "unexpected field {key}");
    }
    let valid:bool=db.query_one("SELECT prepare_lease_expires_at BETWEEN now()+interval '40 seconds' AND now()+interval '46 seconds' FROM agent_task_queue WHERE id=$1", &[&id]).await.unwrap().get(0);
    assert!(valid);
    let pat = format!("mul_prepare_{}", f.ws);
    let pat_id:Uuid=db.query_one("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Preparation',$2,'mul_prepare') RETURNING id", &[&f.owner,&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap().get(0);
    assert_eq!(post(&f.app, &lease, Some(&pat), "").await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE id=$1",
        &[&pat_id],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &lease, Some(&pat), "").await.0,
        StatusCode::UNAUTHORIZED
    );
    for state in [
        "queued",
        "running",
        "completed",
        "failed",
        "cancelled",
        "deferred",
    ] {
        let task = f.task(state, false).await;
        assert_eq!(
            post(&f.app, &f.lease(task), Some(&f.token), "").await,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"extend task prepare lease: no rows in result set"})
            ),
            "{state}"
        );
        assert_eq!(
            post(&f.app, &f.wait(task), Some(&f.token), "{}").await.0,
            StatusCode::BAD_REQUEST,
            "{state}"
        );
        assert!(
            db.query_one(
                "SELECT prepare_lease_expires_at IS NULL FROM agent_task_queue WHERE id=$1",
                &[&task]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
    }
    for body in ["[]", "{bad", r#"{"reason":5}"#] {
        assert_eq!(
            post(&f.app, &f.wait(id), Some(&f.token), body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let waiting = post(
        &f.app,
        &f.wait(id),
        Some(&f.token),
        r#"{"Reason":" local_directory /home/private/repo (held by task 123) "}"#,
    )
    .await;
    assert_eq!(waiting.0, StatusCode::OK, "{}", waiting.1);
    assert_eq!(waiting.1["status"], "waiting_local_directory");
    let state=db.query_one("SELECT status,wait_reason,prepare_lease_expires_at>now() AS leased FROM agent_task_queue WHERE id=$1", &[&id]).await.unwrap();
    assert_eq!(state.get::<_, String>("status"), "waiting_local_directory");
    assert!(state.get::<_, Option<String>>("wait_reason").is_none());
    assert!(state.get::<_, bool>("leased"));
    assert_eq!(
        post(&f.app, &f.wait(id), Some(&f.token), "{}").await.0,
        StatusCode::BAD_REQUEST,
        "wait is a single-winner transition"
    );
    assert_eq!(
        post(&f.app, &lease, Some(&jwt(f.owner)), "").await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE agent_task_queue SET started_at=now() WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &lease, Some(&f.token), "").await.0,
        StatusCode::BAD_REQUEST,
        "started tasks cannot renew preparation"
    );
    db.execute(
        "UPDATE agent_task_queue SET started_at=NULL,runtime_id=$2 WHERE id=$1",
        &[&id, &f.peer_runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &lease, Some(&f.token), "").await.0,
        StatusCode::NOT_FOUND
    );
    let peer_lease = format!(
        "/api/daemon/runtimes/{}/tasks/{id}/prepare-lease",
        f.peer_runtime
    );
    assert_eq!(
        post(&f.app, &peer_lease, Some(&jwt(f.peer)), "").await.0,
        StatusCode::OK
    );
    db.execute(
        // 251 requires completed_at once runtime_id is dropped, so the unbound
        // case is a terminal row rather than a live unbound queue entry.
        "UPDATE agent_task_queue SET status='completed',runtime_id=NULL,completed_at=now() WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &f.wait(id), Some(&f.token), "{}").await.0,
        StatusCode::NOT_FOUND
    );
    let foreign = f.task("dispatched", false).await;
    db.execute(
        "UPDATE issue SET workspace_id=$2 WHERE id=$1",
        &[&f.issue, &f.foreign],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &f.lease(foreign), Some(&f.token), "").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &f.app,
            "/api/daemon/tasks/not-uuid/wait-local-directory",
            Some(&f.token),
            "{}"
        )
        .await
        .1["error"],
        "invalid task_id"
    );
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/runtimes/bad/tasks/{id}/prepare-lease"),
            Some(&f.token),
            ""
        )
        .await
        .1["error"],
        "invalid runtime_id"
    );
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn frame(socket: &mut Socket) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = msg {
            return serde_json::from_str(&text).unwrap();
        }
    }
}
async fn socket(url: &str, user: Uuid) -> Socket {
    let (mut s, _) = connect_async(url).await.unwrap();
    s.send(Message::Text(
        json!({"type":"auth","payload":{"token":jwt(user)}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert_eq!(frame(&mut s).await["type"], "auth_ack");
    s
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn wait_events_are_post_commit_private_and_progress_is_issue_only() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let task = f.task("dispatched", true).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = f.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={}", f.ws);
    let mut owner = socket(&url, f.owner).await;
    let mut peer = socket(&url, f.peer).await;
    assert_eq!(
        post(
            &f.app,
            &f.wait(task),
            Some(&f.token),
            r#"{"reason":" Repo (held by task 123) "}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    let status = frame(&mut owner).await;
    assert_eq!(status["type"], "agent:status");
    assert_eq!(status["payload"]["agent"]["status"], "idle");
    assert!(status["payload"]["agent"].get("custom_env").is_none());
    assert_eq!(frame(&mut peer).await["type"], "agent:status");
    let waiting = frame(&mut owner).await;
    assert_eq!(waiting["type"], "task:waiting_local_directory");
    assert_eq!(waiting["payload"]["chat_session_id"], f.chat.to_string());
    assert_eq!(waiting["payload"]["wait_reason"], "Repo (held by task 123)");
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
            .await
            .unwrap()
            .get::<_, String>(0),
        "waiting_local_directory"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), peer.next())
            .await
            .is_err(),
        "peer must not receive private task hint"
    );
    assert_eq!(
        post(&f.app, &f.wait(task), Some(&f.token), "{}").await.0,
        StatusCode::BAD_REQUEST
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), owner.next())
            .await
            .is_err(),
        "failed transition must not publish"
    );
    let progress = format!("/api/daemon/tasks/{task}/progress");
    assert_eq!(
        post(
            &f.app,
            &progress,
            Some(&f.token),
            r#"{"summary":"private","step":1,"total":2}"#
        )
        .await
        .1,
        json!({"status":"ok"})
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), peer.next())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), owner.next())
            .await
            .is_err(),
        "frozen Go omits chat progress fanout"
    );
    let issue_task = f.task("running", false).await;
    let progress = format!("/api/daemon/tasks/{issue_task}/progress");
    for body in ["", "[]", r#"{"step":1.5}"#, r#"{"summary":true}"#] {
        assert_eq!(
            post(&f.app, &progress, Some(&f.token), body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        post(&f.app, &progress, Some(&jwt(f.peer)), "{}").await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &f.app,
            &progress,
            Some(&f.token),
            r#"{"SUMMARY":"done","step":-1,"total":3}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    for ws in [&mut owner, &mut peer] {
        let event = frame(ws).await;
        assert_eq!(event["type"], "task:progress");
        assert_eq!(
            event["payload"],
            json!({"task_id":issue_task,"summary":"done","step":-1,"total":3})
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&issue_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "running"
    );
    let wait_path = f.wait(issue_task);
    let (a, b) = tokio::join!(
        post(&f.app, &wait_path, Some(&f.token), "{}"),
        post(&f.app, &wait_path, Some(&f.token), "{}")
    );
    assert_eq!(a.0, StatusCode::BAD_REQUEST);
    assert_eq!(b.0, StatusCode::BAD_REQUEST);
    owner.close(None).await.unwrap();
    peer.close(None).await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn wait_concurrency_has_one_winner_and_does_not_idle_another_running_task() {
    let f = Fixture::new().await;
    let id = f.task("dispatched", false).await;
    let running = f.task("running", false).await;
    let path = f.wait(id);
    let (a, b) = tokio::join!(
        post(&f.app, &path, Some(&f.token), r#"{"reason":"first"}"#),
        post(&f.app, &path, Some(&f.token), r#"{"reason":"second"}"#)
    );
    let mut codes = [a.0.as_u16(), b.0.as_u16()];
    codes.sort();
    assert_eq!(codes, [200, 400]);
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one("SELECT status FROM agent WHERE id=$1", &[&f.agent])
            .await
            .unwrap()
            .get::<_, String>(0),
        "working"
    );
    let reason: Option<String> = db
        .query_one(
            "SELECT wait_reason FROM agent_task_queue WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        reason.as_deref(),
        Some(if a.0 == StatusCode::OK {
            "first"
        } else {
            "second"
        })
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&running]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "running"
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn transcript_batch_redacts_sanitizes_preserves_tristate_and_deduplicates() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let task = f.task("running", true).await;
    let path = format!("/api/daemon/tasks/{task}/messages");
    let secret = "github_pat_abcdefghijklmnopqrstuvwxyz";
    // PostgreSQL rounds nanoseconds to microseconds; send exact microseconds
    // so this assertion tests persistence, not timestamp rounding direction.
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let body = json!({"messages":[
        {"seq":3,"type":"tool_result","call_id":"call\0id","output":format!("{secret}\0"),"output_truncated":false,"idempotency_key":"third","created_at":now},
        {"seq":1,"type":"text","content":"first\0","idempotency_key":"first","created_at":now},
        {"seq":2,"type":"tool_use","tool":"shell\0","input":{"changes":[{"diff":format!("{secret}\0")}],"a\0b":1,"ab":2},"output_truncated":true,"idempotency_key":"second","created_at":now}
    ]}).to_string();
    for token in [None, Some("mat_invalid"), Some("mdt_invalid")] {
        assert_eq!(
            post(&f.app, &path, token, &body).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        post(&f.app, &path, Some(&jwt(f.peer)), &body).await.0,
        StatusCode::NOT_FOUND
    );
    let (left, right) = tokio::join!(
        post(&f.app, &path, Some(&f.token), &body),
        post(&f.app, &path, Some(&f.token), &body)
    );
    assert_eq!(left, (StatusCode::OK, json!({"status":"ok"})));
    assert_eq!(right.0, StatusCode::OK);
    let rows = db
        .query(
            "SELECT * FROM task_message WHERE task_id=$1 ORDER BY seq",
            &[&task],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].get::<_, Option<String>>("content").as_deref(),
        Some("first")
    );
    assert_eq!(rows[0].get::<_, Option<bool>>("output_truncated"), None);
    assert_eq!(
        rows[1].get::<_, Option<bool>>("output_truncated"),
        Some(true)
    );
    assert_eq!(
        rows[2].get::<_, Option<bool>>("output_truncated"),
        Some(false)
    );
    assert_eq!(
        rows[2].get::<_, Option<String>>("output").as_deref(),
        Some("[REDACTED GITHUB TOKEN]")
    );
    assert_eq!(
        rows[2].get::<_, Option<String>>("call_id").as_deref(),
        Some("callid")
    );
    let input: Value = rows[1].get("input");
    assert_eq!(input["changes"][0]["diff"], "[REDACTED GITHUB TOKEN]");
    assert_eq!(input["ab"], 2);
    assert_eq!(input["ab#2"], 1);
    let expected = chrono::DateTime::parse_from_rfc3339(&now)
        .unwrap()
        .timestamp_micros();
    assert_eq!(
        rows[0]
            .get::<_, chrono::DateTime<chrono::Utc>>("created_at")
            .timestamp_micros(),
        expected
    );
    // No idempotency key retains legacy append-only behavior.
    for _ in 0..2 {
        assert_eq!(
            post(
                &f.app,
                &path,
                Some(&f.token),
                r#"{"messages":[{"seq":4,"type":"text"}]}"#
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_message WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        5
    );
    let other = f.task("completed", true).await;
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{other}/messages"),
            Some(&f.token),
            &body
        )
        .await
        .0,
        StatusCode::OK,
        "late transcript flush is allowed even after terminal"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_message WHERE task_id=$1",
            &[&other]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        3,
        "idempotency namespace is task-bound"
    );
    for malformed in [
        "",
        "[]",
        r#"{"messages":[{"input":1}]}"#,
        r#"{"messages":[{"seq":1.5}]}"#,
        r#"{"messages":[{"output_truncated":"false"}]}"#,
        r#"{"messages":[{"created_at":"not-time"}]}"#,
    ] {
        assert_eq!(
            post(&f.app, &path, Some(&f.token), malformed).await.0,
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }
    let overlong=json!({"messages":[{"seq":5,"type":"text"},{"seq":6,"type":"text","idempotency_key":"é".repeat(257)}]}).to_string();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), &overlong).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"message idempotency_key exceeds 512 bytes"})
        )
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_message WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        5,
        "invalid batch must not leave prefix rows"
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn transcript_ws_orders_committed_rows_and_never_rebroadcasts_replays() {
    let f = Fixture::new().await;
    let task = f.task("running", true).await;
    let path = format!("/api/daemon/tasks/{task}/messages");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = f.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={}", f.ws);
    let mut owner = socket(&url, f.owner).await;
    let mut peer = socket(&url, f.peer).await;
    let body = r#"{"messages":[{"seq":2,"type":"tool_result","output":"Bearer synthetic-secret","output_truncated":false,"idempotency_key":"2"},{"seq":1,"type":"text","content":"hello","idempotency_key":"1"}]}"#;
    assert_eq!(
        post(&f.app, &path, Some(&f.token), body).await.0,
        StatusCode::OK
    );
    for seq in [1, 2] {
        let event = frame(&mut owner).await;
        assert_eq!(event["type"], "task:message");
        assert_eq!(event["payload"]["seq"], seq);
        assert_eq!(event["payload"]["task_id"], task.to_string());
        assert!(event["payload"].get("issue_id").is_none());
        if seq == 2 {
            assert_eq!(event["payload"]["output"], "Bearer [REDACTED]");
            assert_eq!(event["payload"]["output_truncated"], false);
        } else {
            assert!(event["payload"].get("output_truncated").is_none());
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(80), peer.next())
            .await
            .is_err(),
        "private chat message must not reach peer"
    );
    assert_eq!(
        post(&f.app, &path, Some(&f.token), body).await.0,
        StatusCode::OK
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), owner.next())
            .await
            .is_err(),
        "deduped rows must not republish"
    );
    assert_eq!(
        post(
            &f.app,
            &path,
            Some(&f.token),
            r#"{"messages":[{"seq":3,"type":"tool_use","input":{}}]}"#,
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(frame(&mut owner).await["payload"].get("input").is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(80), peer.next())
            .await
            .is_err()
    );
    let public = f.task("running", false).await;
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{public}/messages"),
            Some(&f.token),
            r#"{"messages":[{"seq":1,"type":"text","content":"public"}]}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    for socket in [&mut owner, &mut peer] {
        let event = frame(socket).await;
        assert_eq!(event["payload"]["issue_id"], f.issue.to_string());
    }
    owner.close(None).await.unwrap();
    peer.close(None).await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn transcript_batch_failure_is_atomic_and_clock_fallback_is_whole_batch() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let task = f.task("running", false).await;
    let path = format!("/api/daemon/tasks/{task}/messages");
    // Disposable database only. Task-specific trigger cannot perturb sibling cases.
    let suffix = task.simple().to_string();
    db.batch_execute(&format!("CREATE FUNCTION fail_stream_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id='{task}' AND NEW.seq=2 THEN RAISE EXCEPTION 'synthetic batch failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_stream_{suffix} BEFORE INSERT ON task_message FOR EACH ROW EXECUTE FUNCTION fail_stream_{suffix}();")).await.unwrap();
    let body = r#"{"messages":[{"seq":1,"type":"text"},{"seq":2,"type":"text"}]}"#;
    assert_eq!(
        post(&f.app, &path, Some(&f.token), body).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to persist task message"})
        )
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_message WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(&format!(
        "DROP TRIGGER fail_stream_{suffix} ON task_message; DROP FUNCTION fail_stream_{suffix}();"
    ))
    .await
    .unwrap();
    let body=json!({"messages":[{"seq":1,"type":"text","created_at":chrono::Utc::now().to_rfc3339()},{"seq":2,"type":"text"}]}).to_string();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), &body).await.0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT count(DISTINCT created_at) FROM task_message WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&f.runtime, &f.peer],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), &body).await.0,
        StatusCode::NOT_FOUND,
        "current runtime owner, not cached token owner, controls ingest"
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn usage_upsert_backfills_provider_corrects_counters_and_fences_daemon() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let task = f.task("running", false).await;
    let path = format!("/api/daemon/tasks/{task}/usage");
    assert_eq!(
        post(&f.app, &path, Some(&jwt(f.peer)), "{}").await.0,
        StatusCode::NOT_FOUND
    );
    for malformed in [
        "",
        "[]",
        r#"{"usage":[{"input_tokens":1.5}]}"#,
        r#"{"usage":{}}"#,
    ] {
        assert_eq!(
            post(&f.app, &path, Some(&f.token), malformed).await.0,
            StatusCode::BAD_REQUEST,
            "{malformed}"
        );
    }
    let original = r#"{"USAGE":[{"model":"auto","input_tokens":100,"output_tokens":10,"cost_usd_ticks":0},{"Provider":" CoDeX ","model":"agent","input_tokens":8,"cost_usd_ticks":250}]}"#;
    assert_eq!(
        post(&f.app, &path, Some(&f.token), original).await,
        (StatusCode::OK, json!({"status":"ok"}))
    );
    let rows=db.query("SELECT provider,model,input_tokens,cost_usd_ticks,updated_at FROM task_usage WHERE task_id=$1 ORDER BY model", &[&task]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>("provider"), "codex");
    assert_eq!(rows[0].get::<_, Option<i64>>("cost_usd_ticks"), Some(250));
    assert_eq!(rows[1].get::<_, String>("provider"), "synthetic");
    assert_eq!(rows[1].get::<_, Option<i64>>("cost_usd_ticks"), None);
    let correction = r#"{"usage":[{"model":"auto","input_tokens":45,"cost_usd_ticks":-1}]}"#;
    assert_eq!(
        post(&f.app, &path, Some(&f.token), correction).await.0,
        StatusCode::OK
    );
    let row=db.query_one("SELECT input_tokens,cost_usd_ticks,updated_at>=created_at AS updated FROM task_usage WHERE task_id=$1 AND model='auto'", &[&task]).await.unwrap();
    assert_eq!(row.get::<_, i64>("input_tokens"), 45);
    assert_eq!(row.get::<_, Option<i64>>("cost_usd_ticks"), None);
    assert!(row.get::<_, bool>("updated"));
    let suffix = task.simple().to_string();
    db.batch_execute(&format!("CREATE FUNCTION fail_usage_{suffix}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.task_id='{task}' AND NEW.model='synthetic-fail' THEN RAISE EXCEPTION 'synthetic usage failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_usage_{suffix} BEFORE INSERT ON task_usage FOR EACH ROW EXECUTE FUNCTION fail_usage_{suffix}();")).await.unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), r#"{"usage":[{"model":"synthetic-fail","input_tokens":1},{"model":"after-failure","input_tokens":9}]}"#).await.0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT input_tokens FROM task_usage WHERE task_id=$1 AND model='after-failure'",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        9
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM task_usage WHERE task_id=$1 AND model='synthetic-fail'",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(&format!(
        "DROP TRIGGER fail_usage_{suffix} ON task_usage; DROP FUNCTION fail_usage_{suffix}();"
    ))
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&f.runtime, &f.peer],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&f.app, &path, Some(&f.token), original).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT input_tokens FROM task_usage WHERE task_id=$1 AND model='auto'",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        45
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on random loopback port"]
async fn completion_marks_issue_first_executed_only_once() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let first = f.task("running", false).await;
    let path = format!("/api/daemon/tasks/{first}/complete");
    let (status, response) = post(&f.app, &path, Some(&f.token), r#"{"output":"done"}"#).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let stamp: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT first_executed_at FROM issue WHERE id=$1",
            &[&f.issue],
        )
        .await
        .unwrap()
        .get(0);
    let stamp = stamp.expect("first successful completion stamps issue");
    assert_eq!(
        post(&f.app, &path, Some(&f.token), r#"{"output":"replayed"}"#)
            .await
            .0,
        StatusCode::OK
    );
    let second = f.task("running", false).await;
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{second}/complete"),
            Some(&f.token),
            r#"{"output":"another"}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT first_executed_at FROM issue WHERE id=$1",
            &[&f.issue]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0),
        Some(stamp)
    );
    let chat_task = f.task("running", true).await;
    assert_eq!(
        post(
            &f.app,
            &format!("/api/daemon/tasks/{chat_task}/complete"),
            Some(&f.token),
            r#"{"output":"chat"}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT first_executed_at FROM issue WHERE id=$1",
            &[&f.issue]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0),
        Some(stamp)
    );
}
