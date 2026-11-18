//! P0 unconfigured branches against a child-owned migrated PostgreSQL instance.
//! Both routes must answer Go's "capability not configured" contract without
//! calling a provider or enqueueing an agent.
use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn call(
    app: &axum::Router,
    method: Method,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Option<Value>) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.map(|b| b.to_string()).unwrap_or_default()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).ok())
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn p0_unconfigured_quick_actions_and_plugin_hooks_match_go_contract() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
    assert!(config.get_hosts().iter().all(
        |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
    ));
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 unconf',$1) RETURNING id",
            &[&format!("p0-unconf-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('P0 unconf',$1) RETURNING id",
            &[&format!("p0-unconf-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('P0 unconf other',$1) RETURNING id",
            &[&format!("p0-unconf-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 unconf agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'P0 unconf chat',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let archived: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,status,explicitly_created_at) VALUES ($1,$2,$3,'P0 archived chat','archived',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let tasks_before: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE agent_id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    let secret = b"synthetic-p0-unconf-test";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, secret.to_vec()),
    );
    let token = encode(&Header::default(), &json!({"sub":owner,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let regenerate =
        format!("/api/chat/sessions/{session}/quick-actions/regenerate?workspace_id={ws}");

    // Go: json.Decode on an empty body is EOF -> 400 invalid request body.
    let (status, body) = call(
        &app,
        Method::POST,
        &regenerate,
        &token,
        Some(json!({"message_id": Uuid::new_v4()})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body.unwrap(),
        json!({
            "error": "suggestions are not available on this deployment",
            "code": "suggestions_not_available"
        })
    );

    // Malformed JSON and a malformed message_id are rejected before the gate.
    assert_eq!(
        call(&app, Method::POST, &regenerate, &token, None).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &regenerate,
            &token,
            Some(json!({"message_id":"not-a-uuid"}))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/chat/sessions/{archived}/quick-actions/regenerate?workspace_id={ws}"),
            &token,
            Some(json!({"message_id": Uuid::new_v4()})),
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/chat/sessions/{session}/quick-actions/regenerate?workspace_id={other}"),
            &token,
            Some(json!({"message_id": Uuid::new_v4()})),
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (task, _) = {
        let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
        (task, ())
    };
    // Go answers requirePluginsV1 before it resolves the task, so even a task in
    // another workspace and even a malformed id get the same 403.
    for id in [task.to_string(), "not-a-uuid".to_owned()] {
        let (status, body) = call(
            &app,
            Method::POST,
            &format!("/api/daemon/tasks/{id}/plugin-hooks"),
            &token,
            Some(json!({"installation_id":"x","hook_key":"y"})),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body.unwrap(),
            json!({"error":"Plugin management is not enabled","code":"plugin_api_disabled"})
        );
    }
    let tasks_after: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE agent_id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        tasks_after,
        tasks_before + 1,
        "regeneration must not enqueue"
    );
}
