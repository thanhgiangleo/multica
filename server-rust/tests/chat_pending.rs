//! Pending chat indicators against a disposable migrated PostgreSQL 16 database.
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
use sha2::{Digest, Sha256};
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
    let code = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on random loopback port"]
async fn pending_chat_respects_creator_agent_guest_and_task_session_fences() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("pending-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Pending A',$1) RETURNING id",
            &[&format!("pending-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Pending B',$1) RETURNING id",
            &[&format!("pending-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in users.iter().zip(["owner", "member"]) {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&other, &users[0]],
    )
    .await
    .unwrap();
    let mut agents = Vec::new();
    for (name, workspace, owner, mode) in [
        ("Private", ws, users[0], "private"),
        ("Public", ws, users[0], "public_to"),
        ("Mine", ws, users[1], "private"),
        ("Foreign", other, users[0], "public_to"),
    ] {
        agents.push(db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ($1,$2,$3,'user',$4,'cloud') RETURNING id", &[&name,&workspace,&owner,&mode]).await.unwrap().get::<_, Uuid>(0));
    }
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Pending runtime {suffix}")]).await.unwrap().get(0);
    let mut sessions = Vec::new();
    for agent in &agents[..3] {
        sessions.push(db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'pending') RETURNING id", &[&ws,agent,&users[1]]).await.unwrap().get::<_, Uuid>(0));
    }
    let mut tasks = Vec::new();
    for (agent, session) in agents.iter().zip(sessions.iter()) {
        tasks.push(db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[agent,&runtime,session]).await.unwrap().get::<_, Uuid>(0));
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-chat-pending-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-chat-pending-secret")).unwrap()
    };
    let path = format!("/api/chat/pending-tasks?workspace_id={ws}");
    let any = format!("/api/chat/pending-tasks/has-any?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &any, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &any, Some(&jwt(users[1]))).await.1,
        json!({"has_pending":true})
    );
    let initial = read(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(initial.0, StatusCode::OK);
    assert_eq!(initial.1["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(initial.1["tasks"][0]["task_id"], json!(tasks[2]));
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2)", &[&agents[1],&ws]).await.unwrap();
    let visible = read(&app, &path, Some(&jwt(users[1]))).await.1;
    assert_eq!(visible["tasks"].as_array().unwrap().len(), 2);
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&sessions[1]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await.1["tasks"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&sessions[2]],
    )
    .await
    .unwrap();
    // The human agent owner retains read access after a guest turn. A task
    // actor does not inherit that owner exception.
    assert_eq!(
        read(&app, &any, Some(&jwt(users[1]))).await.1,
        json!({"has_pending":true})
    );
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&tasks[2]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &any, Some(&jwt(users[1]))).await.1,
        json!({"has_pending":false})
    );
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&tasks[2]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0]))).await.1,
        json!({"tasks":[]})
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&sessions[2]],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&sessions[0]],
    )
    .await
    .unwrap();
    let task_token = format!("mat_pending_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&tasks[0],&agents[0],&ws,&users[1]]).await.unwrap();
    assert_eq!(
        read(&app, &any, Some(&task_token)).await.1,
        json!({"has_pending":false})
    );
    db.execute(
        "UPDATE task_token SET task_id=$1,agent_id=$2 WHERE token_hash=$3",
        &[&tasks[2], &agents[2], &task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&task_token)).await.1,
        json!({"tasks":[{"task_id":tasks[2],"status":"running","chat_session_id":sessions[2]}]})
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/pending-tasks?workspace_id={other}"),
            Some(&task_token)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let pat = format!("mul_pending_{suffix}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Pending synthetic',$2,'mul_pending')", &[&users[1],&pat_hash]).await.unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.1,
        read(&app, &path, Some(&jwt(users[1]))).await.1
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &any, Some(&task_token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "ALTER TABLE agent_task_queue RENAME TO agent_task_queue_unavailable",
        &[],
    )
    .await
    .unwrap();
    let failure = read(&app, &any, Some(&jwt(users[1]))).await;
    db.execute(
        "ALTER TABLE agent_task_queue_unavailable RENAME TO agent_task_queue",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        failure,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to check pending chat tasks"})
        )
    );
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
