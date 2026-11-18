//! Clear only positional queued follow-ups under session/agent locks.
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

const SECRET: &[u8] = b"zbsi67-clear-queue-test-secret";

async fn clear(
    app: &axum::Router,
    ws: Uuid,
    session: &str,
    user: Uuid,
) -> (StatusCode, Option<Value>) {
    let jwt = encode(&Header::default(), &json!({
        "sub": user,
        "exp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600,
    }), &EncodingKey::from_secret(SECRET)).unwrap();
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/api/chat/sessions/{session}/queued-tasks?workspace_id={ws}"
        ))
        .header("authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if bytes.is_empty() {
            None
        } else {
            Some(serde_json::from_slice(&bytes).unwrap())
        },
    )
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn clear_preserves_visible_head_and_cleans_follow_ups() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
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
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Owner',$1) RETURNING id",
            &[&format!("clear-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other',$1) RETURNING id",
            &[&format!("clear-other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Clear',$1) RETURNING id",
            &[&format!("clear-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign',$1) RETURNING id",
            &[&format!("clear-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in [owner, other] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, &user],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&foreign, &owner],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Clear',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'synthetic clear','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Clear',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let head: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',2) RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let tail: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',0) RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let message: Uuid = db.query_one("INSERT INTO chat_message(chat_session_id,role,content,task_id) VALUES ($1,'user','follow up',$2) RETURNING id", &[&session,&tail]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    assert_eq!(
        clear(&app, ws, "invalid", owner).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        clear(&app, foreign, &session.to_string(), owner).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        clear(&app, ws, &session.to_string(), other).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        clear(&app, ws, &session.to_string(), owner).await.0,
        StatusCode::NO_CONTENT
    );
    let rows = db.query("SELECT id,status,cancelled_by_type,prepare_lease_expires_at FROM agent_task_queue WHERE id=ANY($1::uuid[])", &[&vec![head,tail]]).await.unwrap();
    assert_eq!(
        rows.iter()
            .find(|r| r.get::<_, Uuid>("id") == head)
            .unwrap()
            .get::<_, String>("status"),
        "queued"
    );
    let removed = rows
        .iter()
        .find(|r| r.get::<_, Uuid>("id") == tail)
        .unwrap();
    assert_eq!(removed.get::<_, String>("status"), "cancelled");
    assert_eq!(
        removed
            .get::<_, Option<String>>("cancelled_by_type")
            .as_deref(),
        Some("system")
    );
    assert!(
        db.query_opt("SELECT id FROM chat_message WHERE id=$1", &[&message])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        clear(&app, ws, &session.to_string(), owner).await.0,
        StatusCode::NO_CONTENT
    );
    let next: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&head],
    )
    .await
    .unwrap();
    assert_eq!(
        clear(&app, ws, &session.to_string(), owner).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&head])
            .await
            .unwrap()
            .get::<_, String>(0),
        "running"
    );
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&next])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cancelled"
    );
    let channel_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let channel_input: Uuid = db.query_one("INSERT INTO chat_message(chat_session_id,role,content,task_id,channel_ingested) VALUES ($1,'user','from channel',$2,true) RETURNING id", &[&session,&channel_task]).await.unwrap().get(0);
    let session_id = session.to_string();
    let (a, b) = tokio::join!(
        clear(&app, ws, &session_id, owner),
        clear(&app, ws, &session_id, owner),
    );
    assert_eq!(a.0, StatusCode::NO_CONTENT);
    assert_eq!(b.0, StatusCode::NO_CONTENT);
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&channel_task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    assert!(
        db.query_opt("SELECT id FROM chat_message WHERE id=$1", &[&channel_input])
            .await
            .unwrap()
            .is_some()
    );
    let stopped: i64 = db.query_one("SELECT count(*) FROM chat_message WHERE task_id=$1 AND role='assistant' AND content='Stopped.'", &[&channel_task]).await.unwrap().get(0);
    assert_eq!(
        stopped, 1,
        "concurrent clear must settle channel input once"
    );
    let third: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'queued') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &other],
    )
    .await
    .unwrap();
    assert_eq!(
        clear(&app, ws, &session.to_string(), owner).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&third])
            .await
            .unwrap()
            .get::<_, String>(0),
        "queued"
    );
}
