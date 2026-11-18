//! Go ListPendingTasksByRuntime contract against a disposable PostgreSQL 17 database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    migrate::{self, Direction, Stream},
    router_with_api,
};
use serde_json::{Value, json};
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls, config::Host};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"synthetic-pending-test-only";
const DB_NAME: &str = "multica_p0_daemon_pending_contract";

async fn get(app: &axum::Router, path: &str, user: Uuid) -> (StatusCode, Value) {
    let jwt = encode(
        &Header::default(),
        &json!({"sub":user}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let request = Request::builder()
        .method("GET")
        .uri(path)
        .header("authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires tests/p0_daemon_pending_db_verify.sh (owned disposable PostgreSQL)"]
async fn pending_tasks_match_go_filter_order_response_and_runtime_access() {
    let config =
        Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").expect("run-owned URL"))
            .unwrap();
    assert_eq!(config.get_dbname(), Some(DB_NAME));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let hooks: Value =
        serde_json::from_str(include_str!("../baseline/concurrent-index-hooks.json")).unwrap();
    let upstream: HashMap<String, String> =
        serde_json::from_value(hooks["upstream_up"].clone()).unwrap();
    let zbs: HashMap<String, String> = serde_json::from_value(hooks["zbs_up"].clone()).unwrap();
    migrate::run_both(
        &pool,
        &Stream {
            directory: &root.join("migrations"),
            table: "schema_migrations",
            lock_key: 7601,
            hooks: &upstream,
            production_gates: true,
        },
        &Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 7602,
            hooks: &zbs,
            production_gates: true,
        },
        Direction::Up,
    )
    .await
    .unwrap();

    let db = pool.get().await.unwrap();
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Pending owner',$1) RETURNING id",
            &[&format!("pending-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let member: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Other member',$1) RETURNING id",
            &[&format!("member-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("outsider-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let workspace: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Pending contract',$1,'PND') RETURNING id", &[&format!("pending-{}", Uuid::new_v4())]).await.unwrap().get(0);
    for user in [owner, member] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,status) VALUES($1,'Pending runtime','local','synthetic',$2,'online') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let other_runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,status) VALUES($1,'Other runtime','local','synthetic',$2,'online') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Pending agent','local',$2,$3) RETURNING id", &[&workspace,&owner,&runtime]).await.unwrap().get(0);
    // Distinct issues avoid the one-active-task-per-(issue, agent, thread) constraint.
    let mut issues = Vec::new();
    for number in 1..=7i32 {
        let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Pending contract issue','member',$2,$3) RETURNING id", &[&workspace,&owner,&number]).await.unwrap().get(0);
        issues.push(issue);
    }
    let mut ids = Vec::new();
    for (n, status, priority, target_runtime) in [
        (0, "queued", 10, runtime),
        (1, "dispatched", 10, runtime),
        (2, "queued", 2, runtime),
        (3, "running", 99, runtime),
        (4, "completed", 99, runtime),
        (5, "failed", 99, runtime),
        (6, "queued", 99, other_runtime),
    ] {
        let id = Uuid::new_v4();
        let created = format!("2026-01-01T00:00:{n:02}.123456Z")
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,priority,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)",
            &[&id,&agent,&target_runtime,&issues[n],&status,&priority,&created]).await.unwrap();
        if n == 1 {
            db.execute(
                "UPDATE agent_task_queue SET dispatched_at=$2 WHERE id=$1",
                &[&id, &created],
            )
            .await
            .unwrap();
        }
        ids.push(id);
    }
    drop(db);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let path = format!("/api/daemon/runtimes/{runtime}/tasks/pending");
    let (status, body) = get(&app, &path, owner).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let tasks = body
        .as_array()
        .expect("Go returns a JSON array, not an envelope");
    let returned: Vec<_> = tasks
        .iter()
        .map(|task| task["id"].as_str().unwrap())
        .collect();
    let expected = [ids[0].to_string(), ids[1].to_string(), ids[2].to_string()];
    assert_eq!(
        returned,
        [
            expected[0].as_str(),
            expected[1].as_str(),
            expected[2].as_str()
        ]
    );
    assert_eq!(tasks[0]["workspace_id"], workspace.to_string());
    assert_eq!(tasks[0]["runtime_id"], runtime.to_string());
    assert_eq!(tasks[0]["issue_id"], issues[0].to_string());
    assert_eq!(tasks[0]["status"], "queued");
    assert_eq!(tasks[1]["status"], "dispatched");
    assert_eq!(tasks[0]["priority"], 10);
    assert_eq!(tasks[0]["kind"], "direct");
    assert_eq!(tasks[0]["dispatched_at"], Value::Null);
    assert_eq!(tasks[0]["started_at"], Value::Null);
    assert_eq!(tasks[0]["completed_at"], Value::Null);
    assert_eq!(tasks[0]["result"], Value::Null);
    assert_eq!(tasks[0]["error"], Value::Null);
    assert_eq!(tasks[0]["delivered_comment_ids"], json!([]));
    assert!(tasks[0].get("coalesced_comment_ids").is_none());
    // Go formats task timestamps with time.RFC3339: whole seconds, no fraction.
    assert_eq!(tasks[0]["created_at"], json!("2026-01-01T00:00:00Z"));
    assert_eq!(tasks[1]["dispatched_at"], json!("2026-01-01T00:00:01Z"));
    assert!(tasks[0]["attribution"].is_object());

    // The other runtime has an independent pending row; no query-string limit or
    // cursor changes Go's unpaginated list.
    let (status, body) = get(&app, &format!("{path}?limit=1&cursor=irrelevant"), owner).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 3);
    let (status, body) = get(
        &app,
        &format!("/api/daemon/runtimes/{other_runtime}/tasks/pending"),
        owner,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 1);
    assert_eq!(body[0]["id"], ids[6].to_string());
    let (status, body) = get(
        &app,
        &format!("/api/daemon/runtimes/{}/tasks/pending", Uuid::new_v4()),
        owner,
    )
    .await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::NOT_FOUND, &json!("runtime not found"))
    );
    let (status, body) = get(&app, &path, member).await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::NOT_FOUND, &json!("runtime not found"))
    );
    let (status, body) = get(&app, &path, outsider).await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::NOT_FOUND, &json!("not found"))
    );
    let (status, body) = get(&app, "/api/daemon/runtimes/not-a-uuid/tasks/pending", owner).await;
    assert_eq!(
        (status, &body["error"]),
        (StatusCode::BAD_REQUEST, &json!("invalid runtime_id"))
    );
}
