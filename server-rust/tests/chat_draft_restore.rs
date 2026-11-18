//! Draft recovery from a disposable migrated PostgreSQL, never production data.
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

async fn get(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 16 on random loopback port"]
async fn draft_restore_fences_creator_guest_task_workspace_and_attachment() {
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
                &[&role, &format!("restore-{role}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let mut workspaces = Vec::new();
    for name in ["One", "Two"] {
        workspaces.push(
            db.query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("restore-{name}-{suffix}")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    for (user, role) in users[..2].iter().zip(["owner", "member"]) {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspaces[0], user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&workspaces[1], &users[0]],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('restore',$1,$2,'user','private','cloud') RETURNING id", &[&workspaces[0],&users[0]]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'restore') RETURNING id", &[&workspaces[0],&agent,&users[1]]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title) VALUES ($1,$2,$3,'foreign') RETURNING id", &[&workspaces[0],&agent,&users[0]]).await.unwrap().get(0);
    let restore = Uuid::new_v4();
    let task = Uuid::new_v4();
    let attachment: Uuid = db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,'member',$2,'draft.txt','/uploads/draft.txt','text/plain',12) RETURNING id", &[&workspaces[0],&users[1]]).await.unwrap().get(0);
    let cross_attachment: Uuid = db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,'member',$2,'secret.txt','/uploads/secret.txt','text/plain',42) RETURNING id", &[&workspaces[1],&users[0]]).await.unwrap().get(0);
    db.execute("INSERT INTO chat_draft_restore(id,chat_session_id,task_id,content,attachment_ids) VALUES ($1,$2,$3,'private draft',$4)", &[&restore,&session,&task,&vec![attachment,cross_attachment]]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'restore','cloud','synthetic','online') RETURNING id", &[&workspaces[0]]).await.unwrap().get(0);
    let bound_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let other_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&foreign]).await.unwrap().get(0);
    let raw_token = format!("mat_restore_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(raw_token.as_bytes())),&bound_task,&agent,&workspaces[0],&users[1]]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-restore-secret".to_vec()),
    );
    let jwt = |user: Uuid| {
        encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"synthetic-restore-secret")).unwrap()
    };
    let path = format!(
        "/api/chat/sessions/{session}/draft-restores?workspace_id={}",
        workspaces[0]
    );
    assert_eq!(get(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&app, &path, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &path, Some(&jwt(users[0]))).await.1,
        json!({"error":"not your chat session"})
    );
    assert_eq!(
        get(
            &app,
            &path.replace(&workspaces[0].to_string(), &workspaces[1].to_string()),
            Some(&jwt(users[1]))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(
            &app,
            &path.replace(&session.to_string(), "invalid"),
            Some(&jwt(users[1]))
        )
        .await
        .1,
        json!({"error":"invalid chat session id"})
    );
    let empty = format!(
        "/api/chat/sessions/{foreign}/draft-restores?workspace_id={}",
        workspaces[0]
    );
    assert_eq!(
        get(&app, &empty, Some(&jwt(users[0]))).await.1,
        json!({"restores":[]})
    );
    let pat = format!("mul_restore_{suffix}");
    let pat_hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'restore',$2,'mul_restore')", &[&users[1], &pat_hash]).await.unwrap();
    assert_eq!(get(&app, &path, Some(&pat)).await.0, StatusCode::OK);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&pat_hash],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(get(&app, &path, Some(&raw_token)).await.0, StatusCode::OK);
    let ok = get(&app, &path, Some(&jwt(users[1]))).await;
    assert_eq!(ok.0, StatusCode::OK);
    assert_eq!(ok.1["restores"][0]["content"], "private draft");
    assert_eq!(
        ok.1["restores"][0]["attachments"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        ok.1["restores"][0]["attachments"][0]["id"],
        json!(attachment)
    );
    assert_eq!(
        ok.1["restores"][0]["attachments"][0]["download_url"],
        format!("/api/attachments/{attachment}/download")
    );
    db.execute(
        "UPDATE task_token SET task_id=$1 WHERE token_hash=$2",
        &[
            &other_task,
            &hex::encode(Sha256::digest(raw_token.as_bytes())),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&raw_token)).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[1]))).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE agent SET owner_id=$1 WHERE id=$2",
        &[&users[1], &agent],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[1]))).await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE task_token SET task_id=$1 WHERE token_hash=$2",
        &[
            &bound_task,
            &hex::encode(Sha256::digest(raw_token.as_bytes())),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&raw_token)).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=false WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE attachment RENAME TO attachment_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[1]))).await.1,
        json!({"error":"failed to load draft restore attachments"})
    );
    db.execute(
        "ALTER TABLE attachment_unavailable RENAME TO attachment",
        &[],
    )
    .await
    .unwrap();
    db.execute(
        "ALTER TABLE chat_draft_restore RENAME TO chat_draft_restore_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &path, Some(&jwt(users[1]))).await.1,
        json!({"error":"failed to list draft restores"})
    );
    db.execute(
        "ALTER TABLE chat_draft_restore_unavailable RENAME TO chat_draft_restore",
        &[],
    )
    .await
    .unwrap();
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM chat_draft_restore WHERE id=$1",
            &[&restore],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1, "GET must not consume draft");
}
