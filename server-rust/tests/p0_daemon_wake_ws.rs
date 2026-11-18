//! Post-commit enqueue and terminal hints delivered to a real, scoped daemon socket.
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, runtime_skills_sprint_control::RuntimeRpcTransport},
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
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

struct NoopRuntimeRpc;

impl RuntimeRpcTransport for NoopRuntimeRpc {
    fn notify_update_pending(&self, _: Uuid) {}
    fn notify_model_list_pending(&self, _: Uuid) {}
    fn notify_local_skill_list_pending(&self, _: Uuid) {}
    fn notify_local_skill_import_pending(&self, _: Uuid) {}
}

async fn frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL multica_zbsi67 on isolated loopback port"]
async fn rerun_and_cancel_wake_only_the_authorized_runtime() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
    assert!(config.get_hosts().iter().all(
        |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
    ));
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Wake owner',$1) RETURNING id",
            &[&format!("wake-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Wake',$1) RETURNING id",
            &[&format!("wake-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Wake foreign',$1) RETURNING id",
            &[&format!("wake-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status,owner_id,daemon_id) VALUES ($1,'Wake synthetic','cloud','synthetic','offline',$2,'wake-test') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status,owner_id) VALUES ($1,'Other synthetic','cloud','synthetic','online',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let foreign_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Foreign synthetic','cloud','synthetic','online') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,runtime_id) VALUES ('Wake agent',$1,$2,'user','private','cloud',$3) RETURNING id", &[&ws,&user,&runtime]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,assignee_type,assignee_id) VALUES ($1,'Wake issue','member',$2,'agent',$3) RETURNING id", &[&ws,&user,&agent]).await.unwrap().get(0);
    let bound_token = format!("mdt_{}", Uuid::new_v4().simple());
    let bound_hash = hex::encode(Sha256::digest(bound_token.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'wake-test',$3,now()+interval '1 hour')", &[&bound_hash,&ws,&Some(user)]).await.unwrap();
    let pending_update = Uuid::new_v4();
    for schema in [
        include_str!("../runtime_skills_sprint_migrations/001_update_table.sql"),
        include_str!("../runtime_skills_sprint_migrations/002_update_id_index.sql"),
        include_str!("../runtime_skills_sprint_migrations/003_update_active_index.sql"),
    ] {
        db.batch_execute(schema).await.unwrap();
    }
    db.execute("INSERT INTO runtime_rpc_update(id,runtime_id,initiator_user_id,status,target_version,output,error) VALUES ($1,$2,$3,'pending','v-test','','')", &[&pending_update,&runtime,&user]).await.unwrap();
    let secret = b"synthetic-wake-only-test";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, secret.to_vec())
            .with_local_p0_task_events()
            .with_local_daemon_wake()
            .with_runtime_rpc_transport(Arc::new(NoopRuntimeRpc)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let token = encode(&Header::default(), &json!({"sub":user,"exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let connect = |runtime: Uuid, auth: &str| {
        let mut request = format!("ws://127.0.0.1:{port}/api/daemon/ws?runtime_ids={runtime}")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {auth}").parse().unwrap());
        request
    };
    let (mut daemon, _) = connect_async(connect(runtime, &token)).await.unwrap();
    let (mut other_daemon, _) = connect_async(connect(other, &token)).await.unwrap();
    let unauthorized = connect_async(connect(foreign_runtime, &token))
        .await
        .unwrap_err();
    assert!(
        matches!(unauthorized, tokio_tungstenite::tungstenite::Error::Http(ref response) if response.status() == 404)
    );
    daemon
        .send(Message::Text(
            json!({"type":"daemon:heartbeat","payload":{"runtime_id":foreign_runtime}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), daemon.next())
            .await
            .is_err()
    );
    daemon
        .send(Message::Text(
            json!({"type":"daemon:heartbeat","payload":{"runtime_id":runtime}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let ack = frame(&mut daemon).await;
    assert_eq!(ack["type"], "daemon:heartbeat_ack");
    assert_eq!(ack["payload"]["runtime_id"], runtime.to_string());
    assert_eq!(ack["payload"]["status"], "ok");
    assert_eq!(
        ack["payload"]["server_capabilities"],
        json!(["rpc-v1", "task-steer-v1"])
    );
    assert_eq!(
        ack["payload"]["pending_update"]["id"],
        pending_update.to_string()
    );
    assert_eq!(ack["payload"]["pending_update"]["target_version"], "v-test");
    let update_status: String = db
        .query_one(
            "SELECT status FROM runtime_rpc_update WHERE id=$1",
            &[&pending_update],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(update_status, "running");
    let (status, seen): (String, Option<chrono::DateTime<chrono::Utc>>) = {
        let row = db
            .query_one(
                "SELECT status,last_seen_at FROM agent_runtime WHERE id=$1",
                &[&runtime],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(status, "online");
    assert!(seen.is_some());
    daemon
        .send(Message::Text(
            json!({"type":"daemon:heartbeat","payload":{"runtime_id":runtime}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let second_ack = frame(&mut daemon).await;
    assert_eq!(second_ack["payload"]["status"], "ok");
    assert!(second_ack["payload"].get("pending_update").is_none());
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/issues/{issue}/rerun?workspace_id={ws}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::ACCEPTED,
        "{}",
        response.text().await.unwrap()
    );
    let wake = frame(&mut daemon).await;
    assert_eq!(wake["type"], "daemon:task_available");
    assert_eq!(wake["payload"]["runtime_id"], runtime.to_string());
    let task = wake["payload"]["task_id"].as_str().unwrap();
    // Force authorization to wait on a row lock. The server-side RPC budget
    // expires without creating a claim; after unlock the HTTP fallback could
    // still take the queued task instead of racing a late WS commit.
    db.batch_execute("BEGIN; LOCK TABLE agent_runtime IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    daemon
        .send(Message::Text(
            json!({
                "type":"daemon:rpc_request",
                "payload":{"request_id":"timeout-1","method":"tasks.claim","timeout_ms":50,
                    "body":{"daemon_id":"wake-test","runtime_ids":[runtime],"max_tasks":1}}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let timeout_reply = frame(&mut daemon).await;
    assert_eq!(timeout_reply["payload"]["request_id"], "timeout-1");
    assert_eq!(timeout_reply["payload"]["status"], 500);
    assert_eq!(
        timeout_reply["payload"]["error"],
        "context deadline exceeded"
    );
    db.batch_execute("ROLLBACK").await.unwrap();
    let queued: String = db
        .query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&Uuid::parse_str(task).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(queued, "queued");
    // A wake is advisory; a real daemon-token socket claims the same
    // committed row without needing X-User-ID or a second HTTP round trip.
    let (mut bound, _) = connect_async(connect(runtime, &bound_token)).await.unwrap();
    bound
        .send(Message::Text(
            json!({
                "type":"daemon:rpc_request",
                "payload":{"request_id":"wrong-daemon","method":"tasks.claim","timeout_ms":5000,
                    "body":{"daemon_id":"other-daemon","runtime_ids":[runtime],"max_tasks":1}}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let denied_claim = frame(&mut bound).await;
    assert_eq!(denied_claim["payload"]["request_id"], "wrong-daemon");
    assert_eq!(denied_claim["payload"]["status"], 403);
    assert!(denied_claim["payload"]["body"]["error"].is_string());
    bound
        .send(Message::Text(
            json!({
                "type":"daemon:rpc_request",
                "payload":{"request_id":"claim-1","method":"tasks.claim","timeout_ms":5000,
                    "body":{"daemon_id":"wake-test","runtime_ids":[runtime],"max_tasks":1}}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let reply = frame(&mut bound).await;
    assert_eq!(reply["type"], "daemon:rpc_response");
    assert_eq!(reply["payload"]["request_id"], "claim-1");
    assert_eq!(reply["payload"]["status"], 200);
    assert_eq!(reply["payload"]["body"]["tasks"][0]["id"], task);
    daemon
        .send(Message::Text(
            json!({
                "type":"daemon:rpc_request",
                "payload":{"request_id":"foreign-1","method":"tasks.claim","timeout_ms":5000,
                    "body":{"daemon_id":"wake-test","runtime_ids":[foreign_runtime],"max_tasks":1}}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let foreign_reply = frame(&mut daemon).await;
    assert_eq!(foreign_reply["payload"]["request_id"], "foreign-1");
    assert_eq!(foreign_reply["payload"]["status"], 200);
    assert_eq!(foreign_reply["payload"]["body"]["tasks"], json!([]));
    let claim = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/api/daemon/tasks/claim"))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body(json!({"daemon_id":"wake-test","runtime_ids":[runtime],"max_tasks":1}).to_string())
        .send()
        .await
        .unwrap();
    let claim_status = claim.status();
    let claim_text = claim.text().await.unwrap();
    assert_eq!(claim_status, reqwest::StatusCode::OK, "{claim_text}");
    let body: Value = serde_json::from_str(&claim_text).unwrap();
    assert_eq!(body["tasks"], json!([]));
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/agents/{agent}/cancel-tasks?workspace_id={ws}"
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let terminal = frame(&mut daemon).await;
    assert_eq!(terminal["type"], "daemon:task_available");
    assert_eq!(terminal["payload"]["runtime_id"], runtime.to_string());
    assert!(terminal["payload"].get("task_id").is_none());
    let state: String = db
        .query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&Uuid::parse_str(task).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(state, "cancelled");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), other_daemon.next())
            .await
            .is_err()
    );
    db.execute("DELETE FROM agent_runtime WHERE id=$1", &[&other])
        .await
        .unwrap();
    other_daemon
        .send(Message::Text(
            json!({"type":"daemon:heartbeat","payload":{"runtime_id":other}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let gone = frame(&mut other_daemon).await;
    assert_eq!(gone["type"], "daemon:heartbeat_ack");
    assert_eq!(gone["payload"]["status"], "runtime_gone");
    assert_eq!(gone["payload"]["runtime_gone"], true);
    other_daemon
        .send(Message::Text(
            json!({"type":"daemon:heartbeat","payload":{"runtime_id":other}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), other_daemon.next())
            .await
            .is_err()
    );
    // A daemon token bound to a different daemon may not even enumerate this
    // runtime: authorization fails before the upgrade.
    let wrong_daemon = format!("mdt_{}", Uuid::new_v4().simple());
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'other-daemon',$3,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(wrong_daemon.as_bytes())),&ws,&Some(user)]).await.unwrap();
    let denied = connect_async(connect(runtime, &wrong_daemon))
        .await
        .unwrap_err();
    assert!(
        matches!(denied, tokio_tungstenite::tungstenite::Error::Http(ref response) if response.status() == 404)
    );
    server.abort();
    server.await.unwrap_err();
}
