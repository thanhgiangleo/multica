//! P0 agent lifecycle against a child-owned migrated PostgreSQL instance.
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

async fn call(app: &axum::Router, method: Method, path: &str, token: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn p0_agent_routes_archive_cancel_and_page_real_rows() {
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
            "INSERT INTO \"user\"(name,email) VALUES ('P0 owner',$1) RETURNING id",
            &[&format!("p0-agent-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('P0 agent',$1) RETURNING id",
            &[&format!("p0-agent-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 outsider',$1) RETURNING id",
            &[&format!("p0-outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('P0 foreign',$1) RETURNING id",
            &[&format!("p0-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&foreign_ws, &outsider],
    )
    .await
    .unwrap();
    let foreign_agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 foreign agent',$1,$2,'user','private','cloud') RETURNING id", &[&foreign_ws,&outsider]).await.unwrap().get(0);
    let skill: Uuid = db
        .query_one(
            "INSERT INTO skill(workspace_id,name,description) VALUES ($1,'P0 skill','Synthetic') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&agent, &skill],
    )
    .await
    .unwrap();
    let secret = b"synthetic-p0-agent-test";
    // Production-shaped wiring: only the authenticated workspace bus, never a
    // no-op sink.
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
    let token = encode(&Header::default(), &json!({"sub":owner,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let outsider_token = encode(&Header::default(), &json!({"sub":outsider,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    // Another workspace's owner sees this workspace's agent exactly as absent.
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/agents/{agent}/archive?workspace_id={foreign_ws}"),
            &outsider_token
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/agents/{foreign_agent}/cancel-tasks?workspace_id={ws}"),
            &token
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let path = format!("/api/agents/{agent}/tasks?workspace_id={ws}&limit=1");
    let (status, rows) = call(&app, Method::GET, &path, &token).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.as_array().unwrap()[0]["id"], task.to_string());
    let (status, cancelled) = call(
        &app,
        Method::POST,
        &format!("/api/agents/{agent}/cancel-tasks?workspace_id={ws}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cancelled["cancelled"], 1);
    let status: String = db
        .query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "cancelled");
    let (status, archived) = call(
        &app,
        Method::POST,
        &format!("/api/agents/{agent}/archive?workspace_id={ws}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(archived["id"], agent.to_string());
    // The HTTP body is the human projection: no env values inline, and the
    // skills list is the Go four-key summary.
    assert!(archived.get("custom_env").is_none());
    assert_eq!(archived["custom_env_key_count"], 0);
    assert_eq!(archived["skills"][0]["id"], skill.to_string());
    assert_eq!(archived["skills"][0]["name"], "P0 skill");
    assert_eq!(archived["skills"][0]["enabled"], true);
    assert_eq!(archived["invocation_targets"], json!([]));
    assert!(
        db.query_one(
            "SELECT archived_at IS NOT NULL FROM agent WHERE id=$1",
            &[&agent]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &format!("/api/agents/{agent}/archive?workspace_id={ws}"),
            &token
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}
