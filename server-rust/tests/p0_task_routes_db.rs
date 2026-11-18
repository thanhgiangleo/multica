//! P0 issue task reads against a child-owned migrated PostgreSQL instance.
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
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

async fn read(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(path);
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn p0_issue_task_reads_enforce_workspace_and_preserve_run_shape() {
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
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 task tester',$1) RETURNING id",
            &[&format!("p0-task-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 tasks',$1,'PZT') RETURNING id", &[&format!("p0-task-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 other',$1,'PZO') RETURNING id", &[&format!("p0-other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 task issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 task agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let active: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let terminal: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at) VALUES ($1,$2,$3,'completed',now(),now()) RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    db.execute("INSERT INTO task_supplement_capability(task_id,workspace_id,issue_id,capability) VALUES ($1,$2,$3,'task-supplement-v1')", &[&active,&ws,&issue]).await.unwrap();
    let comment: Uuid = db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'P0 steering comment') RETURNING id", &[&ws,&issue,&user]).await.unwrap().get(0);
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$6,'delivered')", &[&active,&ws,&issue,&comment,&user,&Uuid::new_v4()]).await.unwrap();
    let viewer: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 viewer',$1) RETURNING id",
            &[&format!("p0-task-viewer-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &viewer],
    )
    .await
    .unwrap();
    let secret = b"synthetic-p0-task-test";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, secret.to_vec()),
    );
    let token = encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let own = format!("/api/issues/{issue}/active-task?workspace_id={ws}");
    assert_eq!(read(&app, &own, None).await.0, StatusCode::UNAUTHORIZED);
    let (status, body) = read(&app, &own, Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(body["tasks"][0]["id"], active.to_string());
    let (status, history) = read(
        &app,
        &format!("/api/issues/{issue}/task-runs?workspace_id={ws}"),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let runs = history.as_array().unwrap();
    assert_eq!(runs.len(), 2);
    assert!(runs.iter().any(|run| run["id"] == terminal.to_string()));
    let running_run = runs
        .iter()
        .find(|run| run["id"] == active.to_string())
        .unwrap();
    assert_eq!(running_run["supplement_capability"], "task-supplement-v1");
    assert_eq!(
        running_run["supplement_comment_ids"][0],
        comment.to_string()
    );
    assert_eq!(running_run["can_supplement"], true);
    // A member without invocation rights sees the capability but is not
    // offered a steering affordance.
    let viewer_token = encode(&Header::default(), &json!({"sub":viewer,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let (status, viewer_history) = read(
        &app,
        &format!("/api/issues/{issue}/task-runs?workspace_id={ws}"),
        Some(&viewer_token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let viewer_run = viewer_history
        .as_array()
        .unwrap()
        .iter()
        .find(|run| run["id"] == active.to_string())
        .unwrap();
    assert_eq!(viewer_run["supplement_capability"], "task-supplement-v1");
    assert!(viewer_run.get("can_supplement").is_none());
    db.execute(
        "UPDATE issue SET triage_state='pending' WHERE id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    let (status, denied) = write(
        &app,
        &format!("/api/issues/{issue}/rerun?workspace_id={ws}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert_eq!(denied["reason_code"], "issue_in_triage");
    db.execute(
        "UPDATE issue SET triage_state=NULL,assignee_type='agent',assignee_id=$2 WHERE id=$1",
        &[&issue, &agent],
    )
    .await
    .unwrap();
    let (status, denied) = write(
        &app,
        &format!("/api/issues/{issue}/rerun?workspace_id={ws}"),
        &viewer_token,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert_eq!(denied["reason_code"], "invocation_not_allowed");
    assert_eq!(
        read(
            &app,
            &format!("/api/issues/{issue}/active-task?workspace_id={other}"),
            Some(&token)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issues/not-a-uuid/task-runs?workspace_id={ws}"),
            Some(&token)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn task_messages_are_visible_only_to_the_chat_creator() {
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
    let mut users = Vec::new();
    for name in ["creator", "owner", "bystander"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("p0-task-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 chat task',$1,'PZM') RETURNING id", &[&format!("p0-chat-task-{suffix}")]).await.unwrap().get(0);
    for (user, role) in [
        (users[0], "member"),
        (users[1], "owner"),
        (users[2], "member"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 private agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[1]]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,zbs_source_channel_type,explicitly_created_at) VALUES ($1,$2,$3,'P0 private chat','telegram',now()) RETURNING id", &[&ws,&agent,&users[0]]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    db.execute("INSERT INTO task_message(task_id,seq,type,content) VALUES ($1,1,'text','private transcript')", &[&task]).await.unwrap();
    let secret = b"synthetic-p0-private-messages";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, secret.to_vec()),
    );
    let tokens: Vec<String> = users.iter().map(|user| encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap()).collect();
    let path = format!("/api/tasks/{task}/messages?workspace_id={ws}");
    let (status, body) = read(&app, &path, Some(&tokens[0])).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body[0]["content"], "private transcript");
    for token in [&tokens[1], &tokens[2]] {
        let (status, body) = read(&app, &path, Some(token)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"], "task not found");
        assert_eq!(
            read(&app, &format!("{path}&since=invalid"), Some(token))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        read(&app, &format!("{path}&since=invalid"), Some(&tokens[0]))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

async fn write(app: &axum::Router, path: &str, token: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
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
async fn p0_task_cancel_routes_commit_and_keep_tenancy() {
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
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 canceller',$1) RETURNING id",
            &[&format!("p0-cancel-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let stranger: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 stranger',$1) RETURNING id",
            &[&format!("p0-cancel-stranger-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 cancel',$1,'PZC') RETURNING id", &[&format!("p0-cancel-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 cancel other',$1,'PZD') RETURNING id", &[&format!("p0-cancel-other-{suffix}")]).await.unwrap().get(0);
    for (workspace, member, role) in [(ws, user, "owner"), (other, stranger, "owner")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &member, &role],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 cancel issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 cancel agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let running: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at) VALUES ($1,$2,$3,'running',now()) RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let queued: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let done: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at) VALUES ($1,$2,$3,'completed',now(),now()) RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let secret = b"synthetic-p0-cancel-test";
    let state = ApiState::new(pool.clone(), secret.to_vec()).with_local_p0_task_events();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    );
    let token = encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let stranger_token = encode(&Header::default(), &json!({"sub":stranger,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    // Foreign workspace cannot even resolve the task or the issue.
    assert_eq!(
        write(
            &app,
            &format!("/api/tasks/{running}/cancel?workspace_id={other}"),
            &token
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        write(
            &app,
            &format!("/api/issues/{issue}/tasks/{running}/cancel?workspace_id={other}"),
            &stranger_token
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    // Task must belong to the issue named in the path.
    assert_eq!(
        write(
            &app,
            &format!("/api/issues/{issue}/tasks/{done}/cancel?workspace_id={ws}"),
            &token
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, body) = write(
        &app,
        &format!("/api/issues/{issue}/tasks/{running}/cancel?workspace_id={ws}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["id"], running.to_string());
    assert_eq!(body["status"], "cancelled");
    // Bare route cancels the remaining queued row; terminal rows are idempotent.
    let (status, body) = write(
        &app,
        &format!("/api/tasks/{queued}/cancel?workspace_id={ws}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "cancelled");
    assert_eq!(
        write(
            &app,
            &format!("/api/tasks/{queued}/cancel?workspace_id={ws}"),
            &token
        )
        .await
        .0,
        StatusCode::OK
    );
    for id in [running, queued] {
        assert_eq!(
            db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, String>(0),
            "cancelled"
        );
    }
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&done])
            .await
            .unwrap()
            .get::<_, String>(0),
        "completed"
    );
    assert_eq!(
        db.query_one("SELECT status FROM agent WHERE id=$1", &[&agent])
            .await
            .unwrap()
            .get::<_, String>(0),
        "idle"
    );
    assert_eq!(
        write(
            &app,
            &format!("/api/tasks/not-a-uuid/cancel?workspace_id={ws}"),
            &token
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
}

#[derive(Default)]
struct PolicySource(std::sync::Mutex<multica_server::api::issue_write::IssueCountPolicy>);
#[async_trait::async_trait]
impl multica_server::api::issue_write::IssueCreateRuntime for PolicySource {
    async fn count_policy(
        &self,
        _: Uuid,
    ) -> Result<multica_server::api::issue_write::IssueCountPolicy, ()> {
        Ok(*self.0.lock().unwrap())
    }
    fn created(&self, _: &multica_server::api::issue_write::IssueCreatedEvent) {}
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn retry_source_context_moves_pending_context_and_reports_capacity() {
    use multica_server::api::issue_write::IssueCountPolicy;
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
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 retry',$1) RETURNING id",
            &[&format!("p0-retry-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let stranger: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 retry stranger',$1) RETURNING id",
            &[&format!("p0-retry-stranger-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 retry',$1,'PZR') RETURNING id", &[&format!("p0-retry-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 retry other',$1,'PZQ') RETURNING id", &[&format!("p0-retry-other-{suffix}")]).await.unwrap().get(0);
    for (workspace, member, role) in [
        (ws, user, "owner"),
        (ws, stranger, "member"),
        (other, stranger, "owner"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &member, &role],
        )
        .await
        .unwrap();
    }
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    // Owned by someone else: the requester passes the requester check but the
    // invoke gate denies because a private agent only admits its owner.
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,runtime_id) VALUES ('P0 retry agent',$1,$2,'user','private','cloud',$3) RETURNING id", &[&ws,&stranger,&runtime]).await.unwrap().get(0);
    let source_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 retry source','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let context_id = Uuid::new_v4();
    let context = json!({
        "type": "quick_create",
        "prompt": "retry source context",
        "requester_id": user,
        "workspace_id": ws,
        "source_context_id": context_id,
    });
    let source: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,priority,context,originator_user_id,accountable_user_id) VALUES ($1,$2,'failed',0,$3,$4,$4) RETURNING id", &[&agent,&runtime,&context,&user]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_source_context(id,workspace_id,origin_task_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state) VALUES ($1,$2,$3,$4,gen_random_uuid(),$5,1,'{}'::jsonb,'digest','pending')", &[&context_id,&ws,&source,&source_issue,&user]).await.unwrap();
    let policy = Arc::new(PolicySource::default());
    let secret = b"synthetic-p0-retry-context";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), secret.to_vec()).with_issue_create_runtime(policy.clone()),
    );
    let token = encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    let path = format!("/api/tasks/{source}/retry-source-context?workspace_id={ws}");
    let stranger_token = encode(&Header::default(), &json!({"sub":stranger,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap();
    // The workspace guard rejects a caller who is not a member before the
    // handler runs at all.
    let (status, body) = write(
        &app,
        &format!("/api/tasks/{source}/retry-source-context?workspace_id={other}"),
        &token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "workspace not found");
    // A member of another workspace resolves it but cannot reach the task,
    // whose tenancy is its agent's workspace.
    let (status, body) = write(
        &app,
        &format!("/api/tasks/{source}/retry-source-context?workspace_id={other}"),
        &stranger_token,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "source_context_retry_unavailable");
    // Not the requester: the prompt is private to whoever captured it, and Go
    // reports that as "unavailable" rather than a permission error.
    let (status, body) = write(&app, &path, &stranger_token).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "source_context_retry_unavailable");
    // The requester passes that check but is denied by the invoke gate: the
    // agent is private and owned by someone else.
    let (status, body) = write(&app, &path, &token).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["reason_code"], "invocation_not_allowed");
    assert_eq!(
        body["error"],
        "you don't have permission to use this target"
    );
    // Grant the requester access the way the product does, so the remaining
    // branches are reachable.
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&agent],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)",
        &[&agent, &user],
    )
    .await
    .unwrap();
    // A context already claimed by a create can no longer be retried.
    db.execute("UPDATE issue_source_context SET state='attached',issue_id=$2,attached_at=now() WHERE id=$1", &[&context_id,&source_issue]).await.unwrap();
    let (status, body) = write(&app, &path, &token).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"],
        "This context can no longer be retried. Start again from the branch point."
    );
    db.execute("UPDATE issue_source_context SET state='pending',issue_id=NULL,attached_at=NULL WHERE id=$1", &[&context_id]).await.unwrap();
    // Enforcement at the current row count rejects the retry before any child
    // exists — Go's TestRetrySourceContextQuickCreateReturnsIssueLimitRecovery.
    let issue_count: i64 = db
        .query_one("SELECT count(*) FROM issue WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap()
        .get(0);
    *policy.0.lock().unwrap() = IssueCountPolicy {
        limit: Some(issue_count),
        revision: 43,
    };
    let (status, body) = write(&app, &path, &token).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
    assert_eq!(body["code"], "issue_limit_reached");
    assert_eq!(body["limit"], issue_count);
    assert_eq!(body["policy_revision"], 43);
    let children: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE rerun_of_task_id=$1",
            &[&source],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(children, 0);
    let origin: Uuid = db
        .query_one(
            "SELECT origin_task_id FROM issue_source_context WHERE id=$1",
            &[&context_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(origin, source);
    // Unlimited: the retry succeeds and takes the pending context with it.
    *policy.0.lock().unwrap() = IssueCountPolicy::default();
    let (status, body) = write(&app, &path, &token).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let child: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    assert_ne!(child, source);
    assert_eq!(body["status"], "queued");
    assert_eq!(body["kind"], "quick_create");
    // Lineage lives on the attribution block, matching taskAttributionBase.
    assert_eq!(body["attribution"]["rerun_of_task_id"], source.to_string());
    assert_eq!(body["attribution"]["source"], "direct_human");
    assert_eq!(body["attribution"]["precise"], true);
    assert_eq!(body["attribution"]["initiator"]["id"], user.to_string());
    assert_eq!(body["attribution"]["originator"]["id"], user.to_string());
    let row = db
        .query_one("SELECT agent_id,runtime_id,status,force_fresh_session,rerun_of_task_id,originator_source,context FROM agent_task_queue WHERE id=$1", &[&child])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Uuid>("agent_id"), agent);
    assert_eq!(row.get::<_, Option<Uuid>>("runtime_id"), Some(runtime));
    assert!(row.get::<_, bool>("force_fresh_session"));
    assert_eq!(row.get::<_, Option<Uuid>>("rerun_of_task_id"), Some(source));
    assert_eq!(
        row.get::<_, Option<String>>("originator_source").as_deref(),
        Some("direct_human")
    );
    assert_eq!(
        row.get::<_, Option<Value>>("context").unwrap()["source_context_id"],
        json!(context_id)
    );
    let origin: Uuid = db
        .query_one(
            "SELECT origin_task_id FROM issue_source_context WHERE id=$1",
            &[&context_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(origin, child);
    // The context row is the serialization point: a second submit finds no
    // pending context on the original task and refuses.
    let (status, body) = write(&app, &path, &token).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "source_context_retry_unavailable");
    let children: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE rerun_of_task_id=$1",
            &[&source],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(children, 1);
    // Only a failed run can be retried.
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&source],
    )
    .await
    .unwrap();
    assert_eq!(write(&app, &path, &token).await.0, StatusCode::CONFLICT);
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn task_runs_can_supplement_answers_for_agent_caller_never_self() {
    use sha2::{Digest, Sha256};
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
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 agent steer',$1) RETURNING id",
            &[&format!("p0-agent-steer-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('P0 agent steer owner',$1) RETURNING id",
            &[&format!("p0-agent-steer-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 steer',$1,'PZS') RETURNING id", &[&format!("p0-steer-{suffix}")]).await.unwrap().get(0);
    for member in [user, owner] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, &member],
        )
        .await
        .unwrap();
    }
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 steer issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 steer runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    // The target is owned by somebody else, so only the allow-list decides.
    let target: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,runtime_id) VALUES ('P0 steer target',$1,$2,'user','public_to','cloud',$3) RETURNING id", &[&ws,&owner,&runtime]).await.unwrap().get(0);
    let caller: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode,runtime_id) VALUES ('P0 steer caller',$1,$2,'user','private','cloud',$3) RETURNING id", &[&ws,&owner,&runtime]).await.unwrap().get(0);
    let target_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,originator_user_id,accountable_user_id) VALUES ($1,$2,$3,'running',now(),$4,$4) RETURNING id", &[&target,&runtime,&issue,&user]).await.unwrap().get(0);
    let caller_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,originator_user_id,accountable_user_id,originator_source) VALUES ($1,$2,$3,'running',now(),$4,$4,'comment_source') RETURNING id", &[&caller,&runtime,&issue,&user]).await.unwrap().get(0);
    for task in [target_task, caller_task] {
        db.execute("INSERT INTO task_supplement_capability(task_id,workspace_id,issue_id,capability) VALUES ($1,$2,$3,'task-supplement-v1')", &[&task,&ws,&issue]).await.unwrap();
    }
    // A member grant only: an agent caller with no resolvable originator has
    // nobody to match it with, which is exactly Go's deny branch.
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id,created_by) VALUES ($1,'member',$2,$3)", &[&target,&user,&owner]).await.unwrap();
    let secret = b"synthetic-p0-agent-steer";
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), secret.to_vec()),
    );
    let agent_token = format!("mat_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(agent_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&caller_task,&caller,&ws,&user]).await.unwrap();
    let path = format!("/api/issues/{issue}/task-runs?workspace_id={ws}");
    let supplement = |body: &Value| -> (Option<Value>, Option<Value>) {
        let runs = body.as_array().unwrap();
        let get = |id: Uuid| {
            runs.iter()
                .find(|run| run["id"] == id.to_string())
                .and_then(|run| run.get("can_supplement").cloned())
        };
        (get(target_task), get(caller_task))
    };
    let (status, body) = read(&app, &path, Some(&agent_token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (target_allowed, self_allowed) = supplement(&body);
    // The live caller's originator grants the member target; the caller's own
    // run is never steerable by the agent that is running it.
    assert_eq!(target_allowed, Some(json!(true)), "{body}");
    assert_eq!(self_allowed, None, "{body}");
    // A terminal-origin chain lends nothing: the allow-list entry stops
    // matching and the affordance disappears.
    db.execute(
        "UPDATE agent_task_queue SET originator_user_id=NULL,accountable_user_id=NULL,originator_source=NULL WHERE id=$1",
        &[&caller_task],
    )
    .await
    .unwrap();
    let (status, body) = read(&app, &path, Some(&agent_token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(supplement(&body).0, None, "{body}");
    // Only a live task lends its originator, matching invokeOriginatorFromRequest.
    db.execute(
        "UPDATE agent_task_queue SET originator_user_id=$2,accountable_user_id=$2,originator_source='comment_source',status='completed' WHERE id=$1",
        &[&caller_task,&user],
    )
    .await
    .unwrap();
    let (status, body) = read(&app, &path, Some(&agent_token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(supplement(&body).0, None, "{body}");
}
