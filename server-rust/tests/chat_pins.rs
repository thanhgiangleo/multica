//! Quick-chat pins against a disposable migrated PostgreSQL 16 database.
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
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn pins_respect_user_workspace_agent_and_invocation_visibility() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
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
    for role in ["owner", "admin", "member", "outsider"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&role, &format!("pins-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Pins A',$1) RETURNING id",
            &[&format!("pins-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Pins B',$1) RETURNING id",
            &[&format!("pins-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in users.iter().zip(["owner", "admin", "member"]) {
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
    for (index, (workspace, owner, kind, mode)) in [
        (ws, users[0], "user", "private"),
        (ws, users[0], "user", "public_to"),
        (ws, users[0], "user", "public_to"),
        (ws, users[2], "user", "private"),
        (ws, users[0], "user", "public_to"),
        (ws, users[0], "system", "public_to"),
        (other, users[0], "user", "public_to"),
    ]
    .into_iter()
    .enumerate()
    {
        let id: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,kind,permission_mode) VALUES ($1,$5,'cloud',$2,$3,$4) RETURNING id", &[&workspace,&owner,&kind,&mode,&format!("Pinned {index}")]).await.unwrap().get(0);
        agents.push(id);
    }
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2),($3,'member',$4),($5,'team',$4),($6,'workspace',$2)", &[&agents[1],&ws,&agents[2],&users[2],&agents[4],&agents[5]]).await.unwrap();
    for (index, agent) in agents.iter().enumerate() {
        db.execute("INSERT INTO chat_pinned_agent(workspace_id,user_id,agent_id,position) VALUES ($1,$2,$3,$4)", &[&ws,&users[2],agent,&(index as f64)]).await.unwrap();
    }
    db.execute("INSERT INTO chat_pinned_agent(workspace_id,user_id,agent_id,position) VALUES ($1,$2,$3,20)", &[&ws,&users[0],&agents[0]]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-chat-pins-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-chat-pins-secret")).unwrap()
    };
    let path = format!("/api/chat/pinned-agents?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&jwt(users[3]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    let response = read(&app, &path, Some(&jwt(users[2]))).await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(
        response.1,
        json!([
            {"agent_id":agents[1],"position":1.0}, {"agent_id":agents[2],"position":2.0},
            {"agent_id":agents[3],"position":3.0},
        ])
    );
    assert_eq!(
        read(&app, &path, Some(&jwt(users[0]))).await.1,
        json!([{"agent_id":agents[0],"position":20.0}])
    );
    assert_eq!(read(&app, &path, Some(&jwt(users[1]))).await.1, json!([]));
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/pinned-agents?workspace_id={other}"),
            Some(&jwt(users[2]))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'cloud','synthetic','online') RETURNING id", &[&ws,&format!("Pin runtime {suffix}")]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agents[3],&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_pin_{suffix}");
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agents[3],&ws,&users[2]]).await.unwrap();
    let task_response = read(&app, &path, Some(&task_token)).await;
    assert_eq!(task_response.0, StatusCode::OK);
    assert_eq!(
        task_response.1,
        json!(
            (0..5)
                .map(|i| json!({"agent_id":agents[i],"position":i as f64}))
                .collect::<Vec<_>>()
        )
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/chat/pinned-agents?workspace_id={other}"),
            Some(&task_token)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 hour' WHERE token_hash=$1",
        &[&task_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&task_token)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&agents[2]],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&jwt(users[2]))).await.1,
        json!([
            {"agent_id":agents[1],"position":1.0},{"agent_id":agents[3],"position":3.0}
        ])
    );
    let pat = format!("mul_pin_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'Pinned test',$2,'mul_pin')", &[&users[2],&hash]).await.unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.1,
        read(&app, &path, Some(&jwt(users[2]))).await.1
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM chat_pinned_agent WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 8);
    db.execute(
        "ALTER TABLE chat_pinned_agent RENAME TO chat_pinned_agent_unavailable",
        &[],
    )
    .await
    .unwrap();
    let failure = read(&app, &path, Some(&jwt(users[2]))).await;
    db.execute(
        "ALTER TABLE chat_pinned_agent_unavailable RENAME TO chat_pinned_agent",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        failure,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to list pinned agents"})
        )
    );
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM chat_pinned_agent WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}
