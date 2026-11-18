//! Session pending queue against a disposable, migrated PostgreSQL database.
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
async fn session_queue_requires_creator_public_projection_and_agent_access() {
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
    for role in ["owner", "creator", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("queue-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Queue A',$1) RETURNING id",
            &[&format!("queue-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Queue B',$1) RETURNING id",
            &[&format!("queue-b-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Queue agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'queue') RETURNING id", &[&ws,&agent,&users[1]]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-session-queue-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-session-queue-secret")).unwrap()
    };
    let path = format!("/api/chat/sessions/{session}/pending-task?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/sessions/{session}/pending-task?workspace_id={other}"),
            Some(&jwt(users[0]))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0]))).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'member',$2)",&[&agent,&users[1]]).await.unwrap();
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&agent],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await.0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE chat_session SET explicitly_created_at=now() WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await,
        (StatusCode::OK, json!({"supports_queue":true}))
    );
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Queue runtime {suffix}")]).await.unwrap().get(0);
    let head: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,wait_reason) VALUES ($1,$2,$3,'waiting_local_directory',' held by first ') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let queued: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let pat = format!("mul_session_{suffix}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Queue synthetic',$2,'mul_session')", &[&users[1],&pat_hash]).await.unwrap();
    assert_eq!(read(&app, &path, Some(&pat)).await.0, StatusCode::OK);
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
    let task_token = format!("mat_session_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&head,&agent,&ws,&users[1]]).await.unwrap();
    assert_eq!(read(&app, &path, Some(&task_token)).await.0, StatusCode::OK);
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/sessions/{session}/pending-task?workspace_id={other}"),
            Some(&task_token)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/sessions/not-a-uuid/pending-task?workspace_id={ws}"),
            Some(&jwt(users[1]))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let response = read(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1["task_id"], json!(head));
    assert_eq!(response.1["wait_reason"], "held by first");
    assert_eq!(response.1["queued_tasks"][0]["task_id"], json!(queued));
    assert_eq!(response.1["queued_tasks"][0]["content"], Value::Null);
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&head],
    )
    .await
    .unwrap();
    assert!(
        read(&app, &path, Some(&jwt(users[1])))
            .await
            .1
            .get("wait_reason")
            .is_none()
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[1]))).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE chat_session SET creator_id=$1 WHERE id=$2",
        &[&users[0], &session],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0]))).await.0,
        StatusCode::OK
    );
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id=$1",
            &[&session],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "ALTER TABLE agent_task_queue RENAME TO agent_task_queue_missing",
        &[],
    )
    .await
    .unwrap();
    let failure = read(&app, &path, Some(&jwt(users[0]))).await;
    db.execute(
        "ALTER TABLE agent_task_queue_missing RENAME TO agent_task_queue",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        failure,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list pending chat tasks"})
        )
    );
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id=$1",
            &[&session],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
