//! Transactional queue priority and access boundaries on a disposable database.
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

const SECRET: &[u8] = b"zbsi67-priority-test-secret";

fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({
        "sub": user,
        "exp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600,
    }), &EncodingKey::from_secret(SECRET)).unwrap()
}

async fn prioritize(
    app: &axum::Router,
    ws: Uuid,
    session: &str,
    task: &str,
    user: Uuid,
    cookie: bool,
) -> (StatusCode, Value) {
    let token = jwt(user);
    let mut req = Request::builder().method(Method::POST).uri(format!(
        "/api/chat/sessions/{session}/queued-tasks/{task}/prioritize?workspace_id={ws}"
    ));
    req = if cookie {
        req.header("cookie", format!("multica_auth={token}"))
    } else {
        req.header("authorization", format!("Bearer {token}"))
    };
    let res = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires child-owned migrated PostgreSQL on isolated loopback port"]
async fn queue_prioritization_is_atomic_creator_only_and_claim_ordered() {
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
            &[&format!("priority-owner-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other',$1) RETURNING id",
            &[&format!("priority-other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Queue',$1) RETURNING id",
            &[&format!("priority-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other',$1) RETURNING id",
            &[&format!("priority-other-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('Queue',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'synthetic queue','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let session: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id,title,explicitly_created_at) VALUES ($1,$2,$3,'Queue',now()) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let active: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let first: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',4) RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let second: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status,priority) VALUES ($1,$2,$3,'queued',0) RETURNING id", &[&agent,&runtime,&session]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let run = |who: Uuid, workspace: Uuid, s: String, t: String, cookie: bool| {
        let app = app.clone();
        async move { prioritize(&app, workspace, &s, &t, who, cookie).await }
    };
    assert_eq!(
        run(owner, ws, "bad".into(), second.to_string(), false)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        run(owner, ws, session.to_string(), "bad".into(), false)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        run(
            owner,
            foreign,
            session.to_string(),
            second.to_string(),
            false
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        run(other, ws, session.to_string(), second.to_string(), false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        run(owner, ws, session.to_string(), second.to_string(), true)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        run(owner, ws, session.to_string(), active.to_string(), false)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let (status, result) = run(owner, ws, session.to_string(), second.to_string(), false).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result, json!({"task_id":second,"active_task_id":active}));
    let priorities = db
        .query(
            "SELECT id,priority FROM agent_task_queue WHERE id=ANY($1::uuid[])",
            &[&vec![first, second]],
        )
        .await
        .unwrap();
    assert_eq!(
        priorities
            .iter()
            .find(|r| r.get::<_, Uuid>("id") == first)
            .unwrap()
            .get::<_, i32>("priority"),
        3
    );
    assert_eq!(
        priorities
            .iter()
            .find(|r| r.get::<_, Uuid>("id") == second)
            .unwrap()
            .get::<_, i32>("priority"),
        4
    );
    // Both requests contend on the agent lock; a successful reorder leaves a
    // single priority-4 head even when both selected tasks were queued.
    let (left, right) = tokio::join!(
        run(owner, ws, session.to_string(), first.to_string(), false),
        run(owner, ws, session.to_string(), second.to_string(), false),
    );
    assert_eq!(left.0, StatusCode::OK, "{}", left.1);
    assert_eq!(right.0, StatusCode::OK, "{}", right.1);
    let priorities: Vec<i32> = db
        .query(
            "SELECT priority FROM agent_task_queue WHERE id=ANY($1::uuid[]) ORDER BY priority",
            &[&vec![first, second]],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(priorities, [3, 4]);
    // Restore the head expected by the following no-active-reply assertion.
    let (status, result) = run(owner, ws, session.to_string(), second.to_string(), false).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&active],
    )
    .await
    .unwrap();
    let (status, error) = run(owner, ws, session.to_string(), first.to_string(), false).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["error"], "there is no active reply to replace");
    let priority: i32 = db
        .query_one(
            "SELECT priority FROM agent_task_queue WHERE id=$1",
            &[&second],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(priority, 4, "rejected request demoted a valid queue head");
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&active],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_session SET zbs_has_guest_history=true WHERE id=$1",
        &[&session],
    )
    .await
    .unwrap();
    assert_eq!(
        run(owner, ws, session.to_string(), first.to_string(), false)
            .await
            .0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &other],
    )
    .await
    .unwrap();
    assert_eq!(
        run(owner, ws, session.to_string(), second.to_string(), false)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let priority: i32 = db
        .query_one(
            "SELECT priority FROM agent_task_queue WHERE id=$1",
            &[&first],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(priority, 4);
}
