//! Claim delivery against a disposable migrated database; no real agent or provider.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        issue_write::{IssueCountPolicy, IssueCreateRuntime, IssueCreatedEvent},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"synthetic-claim-db-test-only";

async fn claim(
    app: &axum::Router,
    path: &str,
    jwt: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("authorization", format!("Bearer {jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(body.map_or_else(String::new, |b| b.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn batch_issue_gc_check_keeps_order_and_hides_foreign_workspaces() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let owner = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let other_ws = Uuid::new_v4();
    let todo_issue = Uuid::new_v4();
    let custom_issue = Uuid::new_v4();
    let foreign_issue = Uuid::new_v4();
    let missing = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    for (id, name) in [(owner, "Batch GC owner"), (outsider, "Batch GC outsider")] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,$2,$3)",
            &[&id, &name, &format!("{id}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for (id, name) in [(ws, "Batch GC"), (other_ws, "Batch GC other")] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,$2,$3,'BGC')",
            &[&id, &name, &format!("batch-{id}-{suffix}")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,'qa_review','QA review','started','#ffffff')", &[&ws]).await.unwrap();
    for (id, workspace, number, status) in [
        (todo_issue, ws, 45, "todo"),
        (custom_issue, ws, 46, "qa_review"),
        (foreign_issue, other_ws, 47, "done"),
    ] {
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,status) VALUES ($1,$2,'Batch GC issue','member',$3,$4,$5)",
            &[&id, &workspace, &owner, &number, &status]).await.unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = format!("/api/daemon/workspaces/{ws}/issues/gc-check");
    let ask = |body: String, user: Uuid| {
        let app = app.clone();
        let path = path.clone();
        async move {
            let jwt = encode(
                &Header::default(),
                &json!({"sub":user}),
                &EncodingKey::from_secret(SECRET),
            )
            .unwrap();
            let request = Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {jwt}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    let ids = json!({"issue_ids":[custom_issue.to_string(), foreign_issue.to_string(), todo_issue.to_string(), todo_issue.to_string(), missing.to_string()]});
    let (status, body) = ask(ids.to_string(), owner).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["issues"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["id"], custom_issue.to_string());
    assert_eq!(
        (
            rows[0]["found"].as_bool(),
            rows[0]["status"].as_str(),
            rows[0]["category"].as_str()
        ),
        (Some(true), Some("in_progress"), Some("started"))
    );
    // A foreign workspace id is indistinguishable from a deleted one.
    assert_eq!(
        rows[1],
        json!({"id": foreign_issue.to_string(), "found": false})
    );
    assert_eq!(
        (rows[2]["status"].as_str(), rows[2]["category"].as_str()),
        (Some("todo"), Some("unstarted"))
    );
    assert!(rows[2]["updated_at"].is_string());
    assert_eq!(rows[3]["id"], todo_issue.to_string());
    assert_eq!(rows[4], json!({"id": missing.to_string(), "found": false}));
    let (status, body) = ask("{\"issue_ids\":[\"nope\"]}".into(), owner).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid issue_id"))
    );
    let (status, body) = ask("{".into(), owner).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid request body"))
    );
    let (status, _) = ask(
        json!({"issue_ids":[todo_issue.to_string()]}).to_string(),
        outsider,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn daemon_recovery_settles_and_retries_or_resets_issue() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let owner = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let retry_issue = Uuid::new_v4();
    let reset_issue = Uuid::new_v4();
    let retry_task = Uuid::new_v4();
    let reset_task = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Recovery owner',$2)",
        &[&owner, &format!("recovery-{suffix}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Recovery DB test',$2,'RCV')",
        &[&ws, &format!("recovery-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Recovery runtime','local','synthetic',$3,'online',now())", &[&runtime,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,$2,'Recovery agent','local',$3,$4)", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    for (id, number) in [(retry_issue, 43), (reset_issue, 44)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,status) VALUES ($1,$2,'Recoverable issue','member',$3,$4,'in_progress')",
            &[&id,&ws,&owner,&number]).await.unwrap();
    }
    for (id, issue, attempt, max_attempts) in [
        (retry_task, retry_issue, 1, 2),
        (reset_task, reset_issue, 2, 2),
    ] {
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,attempt,max_attempts) VALUES ($1,$2,$3,$4,'running',$5,$6)",
            &[&id,&agent,&runtime,&issue,&attempt,&max_attempts]).await.unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let auth = encode(
        &Header::default(),
        &json!({"sub":owner}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let path = format!("/api/daemon/runtimes/{runtime}/recover-orphans");
    let (status, response) = claim(&app, &path, &auth, None).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response, json!({"orphaned":2,"retried":1}));
    for task in [retry_task, reset_task] {
        let row = db
            .query_one(
                "SELECT status,error,failure_reason,completed_at FROM agent_task_queue WHERE id=$1",
                &[&task],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>("status"), "failed");
        assert_eq!(
            row.get::<_, Option<String>>("error").as_deref(),
            Some("daemon restarted while task was in flight")
        );
        assert_eq!(
            row.get::<_, Option<String>>("failure_reason").as_deref(),
            Some("runtime_recovery")
        );
        assert!(
            row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("completed_at")
                .is_some()
        );
    }
    let child = db
        .query_one(
            "SELECT status,retry_of_task_id FROM agent_task_queue WHERE retry_of_task_id=$1",
            &[&retry_task],
        )
        .await
        .unwrap();
    assert_eq!(child.get::<_, String>("status"), "queued");
    assert_eq!(
        child.get::<_, Option<Uuid>>("retry_of_task_id"),
        Some(retry_task)
    );
    let reset = db
        .query_one("SELECT status FROM issue WHERE id=$1", &[&reset_issue])
        .await
        .unwrap();
    assert_eq!(reset.get::<_, String>("status"), "todo");
    let retry = db
        .query_one("SELECT status FROM issue WHERE id=$1", &[&retry_issue])
        .await
        .unwrap();
    assert_eq!(retry.get::<_, String>("status"), "in_progress");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM comment WHERE source_task_id=$1 AND type='system'",
            &[&reset_task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        claim(&app, &path, &auth, None).await,
        (StatusCode::OK, json!({"orphaned":0,"retried":0}))
    );
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn task_rerun_uses_source_issue_and_fresh_lineage() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let user = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let source = Uuid::new_v4();
    let chat = Uuid::new_v4();
    let chat_task = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Rerun owner',$2)",
        &[&user, &format!("rerun-{suffix}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Rerun DB test',$2,'RRN')",
        &[&ws, &format!("rerun-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Rerun runtime','local','synthetic',$3,'online',now())", &[&runtime,&ws,&user]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id,permission_mode) VALUES ($1,$2,'Rerun agent','local',$3,$4,'private')", &[&agent,&ws,&user,&runtime]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,assignee_type,assignee_id) VALUES ($1,$2,'Rerun issue','member',$3,40,'agent',$4)", &[&issue,&ws,&user,&agent]).await.unwrap();
    db.execute("INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,title,status,explicitly_created_at) VALUES ($1,$2,$3,$4,'Rerun chat','active',now())", &[&chat,&ws,&agent,&user]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,completed_at) VALUES ($1,$2,$3,$4,'completed',now())", &[&source,&agent,&runtime,&issue]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,chat_session_id,status,completed_at) VALUES ($1,$2,$3,$4,'completed',now())", &[&chat_task,&agent,&runtime,&chat]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let jwt = encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(SECRET)).unwrap();
    let send = |task: Uuid, workspace: Uuid, body: String| {
        let app = app.clone();
        let jwt = jwt.clone();
        async move {
            let request = Request::builder()
                .method("POST")
                .uri(format!("/api/tasks/{task}/rerun?workspace_id={workspace}"))
                .header("authorization", format!("Bearer {jwt}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    let (status, response) = send(source, Uuid::new_v4(), "{}".into()).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    assert_eq!(response["error"], "workspace not found");
    let (status, response) = send(chat_task, ws, "{}".into()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(
        response["error"],
        "only issue-linked runs can be rerun with this endpoint"
    );
    let (status, response) = send(source, ws, "{".into()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert_eq!(response["error"], "invalid request body");
    let (status, response) = send(source, ws, "{\"fresh\":true}".into()).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    assert_eq!(response["issue_id"], issue.to_string());
    assert_eq!(
        response["attribution"]["rerun_of_task_id"],
        source.to_string()
    );
    let child: Uuid = response["id"].as_str().unwrap().parse().unwrap();
    let row = db
        .query_one(
            "SELECT status,rerun_of_task_id,context FROM agent_task_queue WHERE id=$1",
            &[&child],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "queued");
    assert_eq!(row.get::<_, Option<Uuid>>("rerun_of_task_id"), Some(source));
    assert_eq!(
        row.get::<_, Option<Value>>("context").unwrap()["fresh_rerun"],
        true
    );
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn daemon_pending_tasks_preserves_order_shape_and_runtime_access() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let owner = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    let workspace = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let other_issue = Uuid::new_v4();
    let queued = Uuid::new_v4();
    let dispatched = Uuid::new_v4();
    let failed = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    for (id, name) in [(owner, "Pending owner"), (outsider, "Pending outsider")] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,$2,$3)",
            &[&id, &name, &format!("{id}@invalid.example")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Pending DB test',$2,'PND')",
        &[&workspace, &format!("pending-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&workspace, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Pending runtime','local','synthetic',$3,'online',now())",
        &[&runtime,&workspace,&owner]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,$2,'Pending agent','local',$3,$4)",
        &[&agent,&workspace,&owner,&runtime]).await.unwrap();
    for (id, number) in [(issue, 39), (other_issue, 41)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number) VALUES ($1,$2,'Pending issue','member',$3,$4)",
            &[&id,&workspace,&owner,&number]).await.unwrap();
    }
    // One live row per (issue, agent, thread): the two pending rows must sit on
    // different threads, which is also what makes the priority ordering real.
    let rows: [(Uuid, &str, i32, Uuid, Option<Uuid>); 3] = [
        (queued, "queued", 3, issue, None),
        (dispatched, "dispatched", 2, other_issue, None),
        (failed, "failed", 4, issue, None),
    ];
    for (task, status, priority, target, thread) in rows {
        db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,priority,comment_thread_id) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            &[&task,&agent,&runtime,&target,&status,&priority,&thread]).await.unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = format!("/api/daemon/runtimes/{runtime}/tasks/pending");
    let get = |path: String, user: Uuid| {
        let app = app.clone();
        async move {
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
            let response = app.oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap())
        }
    };
    let (status, body) = get(path.clone(), owner).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], queued.to_string());
    assert_eq!(rows[1]["id"], dispatched.to_string());
    assert_eq!(rows[0]["workspace_id"], workspace.to_string());
    assert_eq!(rows[0]["issue_id"], issue.to_string());
    assert_eq!(rows[0]["dispatched_at"], Value::Null);
    assert_eq!(rows[0]["kind"], "direct");
    let (status, body) = get(path, outsider).await;
    assert_ne!(status, StatusCode::OK, "{body}");
    let (status, body) = get(
        format!("/api/daemon/runtimes/{}/tasks/pending", Uuid::new_v4()),
        owner,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn singular_and_batch_claim_deliver_issue_and_chat_with_go_skill_wire_shape() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let owner = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let chat = Uuid::new_v4();
    let skill = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Synthetic claim owner',$2)",
        &[&owner, &format!("{suffix}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Claim DB test',$2,'CLM')",
        &[&ws, &format!("claim-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Synthetic runtime','local','synthetic',$3,'online',now())", &[&runtime,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,$2,'Synthetic agent','local',$3,$4)", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number) VALUES ($1,$2,'Synthetic issue','member',$3,37)", &[&issue,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO chat_session(id,workspace_id,agent_id,creator_id,title,status,explicitly_created_at) VALUES ($1,$2,$3,$4,'Synthetic chat','active',now())", &[&chat,&ws,&agent,&owner]).await.unwrap();
    db.execute("INSERT INTO skill(id,workspace_id,name,content) VALUES ($1,$2,'synthetic-skill','synthetic only')", &[&skill,&ws]).await.unwrap();
    db.execute(
        "INSERT INTO skill_file(skill_id,path,content) VALUES ($1,'notes.txt','synthetic note')",
        &[&skill],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&agent, &skill],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let auth = encode(
        &Header::default(),
        &json!({"sub":owner}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let singular = format!("/api/daemon/runtimes/{runtime}/tasks/claim");
    let batch = "/api/daemon/tasks/claim";
    let batch_body = json!({"daemon_id":"synthetic-daemon","runtime_ids":[runtime],"max_tasks":1});
    assert_eq!(
        claim(&app, &singular, &auth, None).await,
        (StatusCode::OK, json!({"task":null}))
    );
    assert_eq!(
        claim(&app, batch, &auth, Some(batch_body.clone())).await,
        (StatusCode::OK, json!({"tasks":[]}))
    );
    for (index, issue_task) in [true, false].into_iter().enumerate() {
        let task = Uuid::new_v4();
        if issue_task {
            db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,$4,'queued')", &[&task,&agent,&runtime,&issue]).await.unwrap();
        } else {
            db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,$4,'queued')", &[&task,&agent,&runtime,&chat]).await.unwrap();
        }
        let (status, body) = if index == 0 {
            claim(&app, &singular, &auth, None).await
        } else {
            claim(&app, batch, &auth, Some(batch_body.clone())).await
        };
        assert_eq!(status, StatusCode::OK, "{body}");
        let delivered = if index == 0 {
            &body["task"]
        } else {
            &body["tasks"][0]
        };
        assert_eq!(delivered["id"], task.to_string(), "{body}");
        assert_eq!(delivered["agent_id"], agent.to_string());
        assert_eq!(delivered["runtime_id"], runtime.to_string());
        assert_eq!(delivered["workspace_id"], ws.to_string());
        assert_eq!(delivered["start_claim_supported"], true);
        assert_eq!(delivered["leader_role_resolved"], true);
        assert!(delivered["dispatched_at"].as_str().is_some());
        assert_eq!(delivered["delivered_comment_ids"], json!([]));
        assert!(delivered.get("plugin_hook_tools").is_none());
        assert!(delivered.get("remote_mcp_connections").is_none());
        assert!(delivered.get("remote_mcp_daemon_token").is_none());
        assert!(delivered["auth_token"].as_str().is_some_and(|token| !token.is_empty()));
        assert_eq!(delivered["agent"]["skills"][0]["id"], skill.to_string());
        assert!(delivered["agent"]["skills"][0].get("source").is_none());
        // Go's AgentSkillData has no omitempty on id, so a builtin ships an
        // empty id and no source (its identity is derived only on the bundle
        // path). The workspace skill above keeps its id and omits source.
        assert!(
            delivered["agent"]["skills"]
                .as_array()
                .unwrap()
                .iter()
                .any(|skill| {
                    skill["name"] == "multica-platform"
                        && skill["id"] == ""
                        && skill.get("source").is_none()
                })
        );
        if issue_task {
            assert_eq!(delivered["issue_id"], issue.to_string());
            assert_eq!(delivered["issue_identifier"], "CLM-37");
        } else {
            assert_eq!(delivered["chat_session_id"], chat.to_string());
        }
        assert_eq!(
            db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
                .await
                .unwrap()
                .get::<_, String>(0),
            "dispatched"
        );
        assert_eq!(
            db.query_one("SELECT count(*) FROM task_token WHERE task_id=$1", &[&task])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
    }
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn claim_cancels_superseded_auto_retry_before_promotion() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let owner = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let original = Uuid::new_v4();
    let retry = Uuid::new_v4();
    let successor = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Synthetic retry owner',$2)",
        &[&owner, &format!("retry-{suffix}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Retry DB test',$2,'RTY')",
        &[&ws, &format!("retry-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Synthetic runtime','local','synthetic',$3,'online',now())", &[&runtime,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,$2,'Synthetic agent','local',$3,$4)", &[&agent,&ws,&owner,&runtime]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number) VALUES ($1,$2,'Synthetic retry issue','member',$3,38)", &[&issue,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,completed_at) VALUES ($1,$2,$3,$4,'failed',now())", &[&original,&agent,&runtime,&issue]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,retry_of_task_id,fire_at) VALUES ($1,$2,$3,$4,'deferred',$5,now()-interval '1 second')", &[&retry,&agent,&runtime,&issue,&original]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,$4,'queued')", &[&successor,&agent,&runtime,&issue]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_local_p0_task_events(),
    );
    let auth = encode(
        &Header::default(),
        &json!({"sub":owner}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let (status, response) = claim(
        &app,
        &format!("/api/daemon/runtimes/{runtime}/tasks/claim"),
        &auth,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["task"]["id"], successor.to_string());
    let row = db
        .query_one(
            "SELECT status,completed_at,cancelled_by_type FROM agent_task_queue WHERE id=$1",
            &[&retry],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "cancelled");
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("completed_at")
            .is_some()
    );
    assert_eq!(
        row.get::<_, Option<String>>("cancelled_by_type").as_deref(),
        Some("system")
    );
}

struct LimitPolicy(Result<IssueCountPolicy, ()>);
#[async_trait::async_trait]
impl IssueCreateRuntime for LimitPolicy {
    async fn count_policy(&self, _: Uuid) -> Result<IssueCountPolicy, ()> {
        self.0
    }
    fn created(&self, _: &IssueCreatedEvent) {}
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn issue_limit_usage_degrades_to_204_and_caps_reported_usage() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let user = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let suffix = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Limit owner',$2)",
        &[&user, &format!("limit-{suffix}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'Limit DB test',$2,'LMT')",
        &[&ws, &format!("limit-{suffix}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    for number in 1..=3 {
        db.execute("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Limited issue','member',$2,$3)",
            &[&ws, &user, &(50 + number)]).await.unwrap();
    }
    let app = |policy: Option<Result<IssueCountPolicy, ()>>| {
        let state = ApiState::new(pool.clone(), SECRET.to_vec());
        let state = match policy {
            Some(policy) => state.with_issue_create_runtime(Arc::new(LimitPolicy(policy))),
            None => state,
        };
        router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        )
    };
    let path = format!("/api/issues/limit-usage?workspace_id={ws}");
    let get = |app: axum::Router, path: String| async move {
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let jwt = encode(
            &Header::default(),
            &json!({"sub":user,"exp":exp}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap();
        let request = Request::builder()
            .method("GET")
            .uri(path)
            .header("authorization", format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
        )
    };
    // A missing provider and a failed policy lookup both degrade to "off".
    for policy in [
        None,
        Some(Err(())),
        Some(Ok(IssueCountPolicy {
            limit: None,
            revision: 1,
        })),
        Some(Ok(IssueCountPolicy {
            limit: Some(0),
            revision: 2,
        })),
    ] {
        let (status, body) = get(app(policy), path.clone()).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        assert_eq!(body, Value::Null);
    }
    let (status, body) = get(
        app(Some(Ok(IssueCountPolicy {
            limit: Some(10),
            revision: 3,
        }))),
        path.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"used": 3, "limit": 10}));
    // Counting stops at limit+1 and the answer is capped: a workspace over
    // quota never reports its true size.
    let (status, body) = get(
        app(Some(Ok(IssueCountPolicy {
            limit: Some(2),
            revision: 4,
        }))),
        path,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"used": 2, "limit": 2}));
}
