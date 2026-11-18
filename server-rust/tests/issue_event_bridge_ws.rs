//! HTTP issue writes reach only authenticated members of the same workspace.
use deadpool_postgres::{Manager, Pool};
use futures_util::{SinkExt, StreamExt};
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, IssueWakeupDispatcher, issue_write::IssueWakeupDispatchRequest},
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
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

struct Dispatcher;
impl IssueWakeupDispatcher for Dispatcher {
    fn request_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("metadata writes must not request wakeup dispatch");
    }
}

fn token(user: Uuid, secret: &[u8]) -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":exp}),
        &EncodingKey::from_secret(secret),
    )
    .unwrap()
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
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn committed_metadata_is_delivered_over_member_websocket_only() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('WS member',$1) RETURNING id",
            &[&format!("bridge-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Bridge',$1) RETURNING id",
            &[&format!("bridge-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("bridge-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for ws in [workspace, foreign] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, &user],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'Bridge','member',$2) RETURNING id",
        &[&workspace,&user]).await.unwrap().get(0);
    let secret = b"bridge-synthetic-test-secret";
    let state = ApiState::new(pool.clone(), secret.to_vec())
        .with_local_issue_event_dispatcher(Arc::new(Dispatcher));
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let connect = |workspace: Uuid| {
        let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}");
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "cookie",
            format!("multica_auth={}", token(user, secret))
                .parse()
                .unwrap(),
        );
        request
    };
    let (mut member, _) = connect_async(connect(workspace)).await.unwrap();
    let (mut other, _) = connect_async(connect(foreign)).await.unwrap();
    for socket in [&mut member, &mut other] {
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        assert_eq!(frame(socket).await["type"], "pong");
    }
    let response = reqwest::Client::new()
        .put(format!(
            "http://127.0.0.1:{port}/api/issues/{issue}/metadata/ready?workspace_id={workspace}"
        ))
        .bearer_auth(token(user, secret))
        .header("content-type", "application/json")
        .body(json!({"value":true}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let message = frame(&mut member).await;
    assert_eq!(message["type"], "issue_metadata:changed");
    assert_eq!(message["actor_type"], "member");
    assert_eq!(message["actor_id"], user.to_string());
    assert_eq!(message["payload"]["issue_id"], issue.to_string());
    assert_eq!(message["payload"]["metadata"], json!({"ready":true}));
    assert!(message["payload"]["issue_revision"].is_number());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.next())
            .await
            .is_err()
    );
    server.abort();
    server.await.unwrap_err();
}

#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn p0_env_update_reaches_member_ws_without_secrets_or_foreign_delivery() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = Vec::new();
    for label in ["owner", "peer", "stranger"] {
        let user: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("p0-env-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(user);
    }
    let mut workspaces = Vec::new();
    for label in ["home", "foreign"] {
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("p0-env-{label}-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        workspaces.push(ws);
    }
    let (ws, foreign) = (workspaces[0], workspaces[1]);
    for (workspace, user, role) in [
        (ws, users[0], "owner"),
        (ws, users[1], "member"),
        (foreign, users[0], "owner"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,custom_env,runtime_config) VALUES ($1,'P0 env','local',$2,$3,$4) RETURNING id", &[&ws,&users[0],&json!({"KEEP":"synthetic-secret-marker"}),&json!({"gateway":{"token":"gateway-secret-marker"}})]).await.unwrap().get(0);
    let secret = b"p0-env-synthetic-test-secret";
    let state = ApiState::new(pool.clone(), secret.to_vec()).with_local_p0_agent_events();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let connect = |workspace: Uuid, user: Uuid| {
        let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}");
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "cookie",
            format!("multica_auth={}", token(user, secret))
                .parse()
                .unwrap(),
        );
        request
    };
    let (mut member, _) = connect_async(connect(ws, users[1])).await.unwrap();
    let (mut other, _) = connect_async(connect(foreign, users[0])).await.unwrap();
    for socket in [&mut member, &mut other] {
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        assert_eq!(frame(socket).await["type"], "pong");
    }
    let client = reqwest::Client::new();
    let path = format!("http://127.0.0.1:{port}/api/agents/{agent}/env?workspace_id={ws}");
    let forbidden = client
        .put(&path)
        .bearer_auth(token(users[1], secret))
        .header("content-type", "application/json")
        .body(json!({"custom_env":{"KEEP":"wrong"}}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);
    let outsider = client
        .put(&path)
        .bearer_auth(token(users[2], secret))
        .header("content-type", "application/json")
        .body(json!({"custom_env":{"KEEP":"wrong"}}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(outsider.status(), reqwest::StatusCode::NOT_FOUND);
    let response = client
        .put(&path)
        .bearer_auth(token(users[0], secret))
        .header("content-type", "application/json")
        .body(json!({"custom_env":{"KEEP":"****","NEW":"synthetic-new-marker"}}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let event = frame(&mut member).await;
    assert_eq!(event["type"], "agent:status");
    assert_eq!(event["actor_type"], "member");
    assert_eq!(event["actor_id"], users[0].to_string());
    assert_eq!(event["payload"]["agent"]["id"], agent.to_string());
    assert_eq!(event["payload"]["agent"]["custom_env_key_count"], 2);
    assert_eq!(
        event["payload"]["agent"]["runtime_config"]["gateway"]["token"],
        "***"
    );
    assert!(event["payload"]["agent"].get("custom_env").is_none());
    assert!(!event.to_string().contains("secret-marker"));
    assert!(!event.to_string().contains("synthetic-new-marker"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.next())
            .await
            .is_err()
    );
    let saved: Value = db
        .query_one("SELECT custom_env FROM agent WHERE id=$1", &[&agent])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        saved,
        json!({"KEEP":"synthetic-secret-marker","NEW":"synthetic-new-marker"})
    );
    let audit: Value = db.query_one("SELECT details FROM activity_log WHERE workspace_id=$1 AND action='agent_env_updated' ORDER BY created_at DESC LIMIT 1", &[&ws]).await.unwrap().get(0);
    assert_eq!(audit["preserved_keys"], json!(["KEEP"]));
    assert!(!audit.to_string().contains("secret-marker"));
    server.abort();
    server.await.unwrap_err();
}

#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn p0_task_cancel_reaches_member_ws_only() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 cancel ws',$1) RETURNING id",
            &[&format!("p0-cancel-ws-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 cancel ws',$1,'PZW') RETURNING id", &[&format!("p0-cancel-ws-{suffix}")]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 cancel foreign',$1,'PZF') RETURNING id", &[&format!("p0-cancel-foreign-{suffix}")]).await.unwrap().get(0);
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 cancel ws issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 cancel ws agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at) VALUES ($1,$2,$3,'running',now()) RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let secret = b"p0-cancel-synthetic-test-secret";
    let state = ApiState::new(pool.clone(), secret.to_vec()).with_local_p0_task_events();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let connect = |workspace: Uuid| {
        let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}");
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "cookie",
            format!("multica_auth={}", token(user, secret))
                .parse()
                .unwrap(),
        );
        request
    };
    let (mut member, _) = connect_async(connect(ws)).await.unwrap();
    let (mut other, _) = connect_async(connect(foreign)).await.unwrap();
    for socket in [&mut member, &mut other] {
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        assert_eq!(frame(socket).await["type"], "pong");
    }
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/tasks/{task}/cancel?workspace_id={ws}"
        ))
        .bearer_auth(token(user, secret))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let event = frame(&mut member).await;
    assert_eq!(event["type"], "task:cancelled");
    assert_eq!(event["actor_type"], "system");
    assert_eq!(event["actor_id"], "");
    assert_eq!(event["payload"]["task_id"], task.to_string());
    assert_eq!(event["payload"]["issue_id"], issue.to_string());
    assert_eq!(event["payload"]["status"], "cancelled");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.next())
            .await
            .is_err()
    );
    server.abort();
    server.await.unwrap_err();
}

#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn p0_agent_archive_and_cancel_tasks_reach_member_ws_only() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 agent ws',$1) RETURNING id",
            &[&format!("p0-agent-ws-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 agent ws',$1,'PAW') RETURNING id", &[&format!("p0-agent-ws-{suffix}")]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 agent foreign',$1,'PAF') RETURNING id", &[&format!("p0-agent-foreign-{suffix}")]).await.unwrap().get(0);
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 agent ws issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let archived_agent: Uuid = db.query_one(
        "INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,status,mcp_config,runtime_config,custom_env) \
         VALUES ('P0 archive agent',$1,$2,'user','private','cloud','working',$3,$4,$5) RETURNING id",
        &[&ws,&user,&json!({"token":"mcp-marker"}),&json!({"gateway":{"token":"gateway-marker"}}),&json!({"KEEP":"env-marker"})],
    ).await.unwrap().get(0);
    let skill: Uuid = db.query_one("INSERT INTO skill(workspace_id,name,description) VALUES ($1,'P0 ws skill','Synthetic') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&archived_agent, &skill],
    )
    .await
    .unwrap();
    let cancel_agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,status) VALUES ('P0 cancel agent',$1,$2,'user','private','cloud','working') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at) VALUES ($1,$2,$3,'running',now()) RETURNING id", &[&cancel_agent,&runtime,&issue]).await.unwrap().get(0);
    let secret = b"p0-agent-ws-synthetic-secret";
    let state = ApiState::new(pool.clone(), secret.to_vec())
        .with_local_p0_agent_events()
        .with_local_p0_task_events();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let connect = |workspace: Uuid| {
        let url = format!("ws://127.0.0.1:{port}/ws?workspace_id={workspace}");
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "cookie",
            format!("multica_auth={}", token(user, secret))
                .parse()
                .unwrap(),
        );
        request
    };
    let (mut member, _) = connect_async(connect(ws)).await.unwrap();
    let (mut other, _) = connect_async(connect(foreign)).await.unwrap();
    for socket in [&mut member, &mut other] {
        socket
            .send(Message::Text(json!({"type":"ping"}).to_string().into()))
            .await
            .unwrap();
        assert_eq!(frame(socket).await["type"], "pong");
    }
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/agents/{archived_agent}/archive?workspace_id={ws}"
        ))
        .bearer_auth(token(user, secret))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let event = frame(&mut member).await;
    assert_eq!(event["type"], "agent:archived");
    assert_eq!(event["actor_type"], "member");
    assert_eq!(event["actor_id"], user.to_string());
    let agent = &event["payload"]["agent"];
    assert_eq!(agent["id"], archived_agent.to_string());
    // The broadcast projection strips secrets but still shows which fields exist.
    assert!(agent["mcp_config"].is_null());
    assert_eq!(agent["mcp_config_redacted"], true);
    assert_eq!(agent["skills"][0]["id"], skill.to_string());
    assert_eq!(agent["skills"][0]["enabled"], true);
    assert_eq!(agent["invocation_targets"], json!([]));
    let body = event.to_string();
    assert!(!body.contains("mcp-marker"));
    assert!(!body.contains("gateway-marker"));
    assert!(!body.contains("env-marker"));
    let response = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/agents/{cancel_agent}/cancel-tasks?workspace_id={ws}"
        ))
        .bearer_auth(token(user, secret))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let cancelled = frame(&mut member).await;
    assert_eq!(cancelled["type"], "task:cancelled");
    assert_eq!(cancelled["actor_type"], "system");
    assert_eq!(cancelled["actor_id"], "");
    assert_eq!(cancelled["payload"]["task_id"], task.to_string());
    assert_eq!(cancelled["payload"]["issue_id"], issue.to_string());
    let status = frame(&mut member).await;
    assert_eq!(status["type"], "agent:status");
    assert_eq!(status["actor_type"], "system");
    assert_eq!(status["actor_id"], "");
    assert_eq!(status["payload"]["agent"]["id"], cancel_agent.to_string());
    assert_eq!(status["payload"]["agent"]["status"], "idle");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), other.next())
            .await
            .is_err()
    );
    server.abort();
    server.await.unwrap_err();
}
