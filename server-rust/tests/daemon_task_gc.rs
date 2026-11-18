//! Synthetic daemon task GC scope and runtime binding contract.
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

async fn get(app: &axum::Router, id: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!("/api/daemon/tasks/{id}/gc-check"));
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
#[ignore = "requires run-owned PostgreSQL 16 on a random loopback port"]
async fn task_gc_resolves_all_sources_and_fails_closed() {
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
    let [
        owner,
        peer,
        stranger,
        ws,
        other,
        agent,
        rt,
        peer_rt,
        foreign_rt,
        issue,
        foreign_issue,
    ]: [Uuid; 11] = std::array::from_fn(|_| Uuid::new_v4());
    for id in [owner, peer, stranger] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'gc-test',$2)",
            &[&id, &format!("{id}@gc.invalid")],
        )
        .await
        .unwrap();
    }
    for id in [ws, other] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'gc-test',$2)",
            &[&id, &format!("gc-{id}")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &owner, &peer],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,owner_id,runtime_mode) VALUES ($1,$2,'gc-test',$3,'cloud')", &[&agent,&ws,&owner]).await.unwrap();
    for (id, workspace, daemon, runtime_owner) in [
        (rt, ws, "gc-daemon", owner),
        (peer_rt, ws, "peer-daemon", peer),
        (foreign_rt, other, "gc-daemon", owner),
    ] {
        db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id) VALUES ($1,$2,$3,'gc-test','cloud','synthetic','online',$4)", &[&id,&workspace,&daemon,&runtime_owner]).await.unwrap();
    }
    for (id, workspace) in [(issue, ws), (foreign_issue, other)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id) VALUES ($1,$2,'gc-test','member',$3)", &[&id,&workspace,&owner]).await.unwrap();
    }
    let [
        valid,
        quick,
        foreign,
        wrong_runtime,
        no_runtime,
        no_scope,
        peer_task,
    ]: [Uuid; 7] = std::array::from_fn(|_| Uuid::new_v4());
    for (id, issue_id, runtime_id, context, completed) in [
        (valid, Some(issue), Some(rt), None, false),
        (
            quick,
            None,
            Some(rt),
            Some(json!({"type":"quick_create","workspace_id":ws.to_string()})),
            true,
        ),
        (foreign, Some(foreign_issue), Some(rt), None, false),
        (wrong_runtime, Some(issue), Some(foreign_rt), None, false),
        (no_runtime, Some(issue), None, None, true),
        (no_scope, None, Some(rt), None, false),
        (peer_task, Some(issue), Some(peer_rt), None, false),
    ] {
        db.execute("INSERT INTO agent_task_queue(id,agent_id,issue_id,runtime_id,context,status,completed_at) VALUES ($1,$2,$3,$4,$5,'completed',CASE WHEN $6 THEN now() ELSE NULL END)", &[&id,&agent,&issue_id,&runtime_id,&context,&completed]).await.unwrap();
    }
    let chat: Uuid = db.query_one("INSERT INTO chat_session(workspace_id,agent_id,creator_id) VALUES ($1,$2,$3) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let automation: Uuid = db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,created_by_type,created_by_id) VALUES ($1,'gc-test',$2,'member',$3) RETURNING id", &[&ws,&agent,&owner]).await.unwrap().get(0);
    let run: Uuid = db
        .query_one(
            "INSERT INTO autopilot_run(autopilot_id,source,status) VALUES ($1,'manual','running') RETURNING id",
            &[&automation],
        )
        .await
        .unwrap()
        .get(0);
    let chat_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&rt,&chat]).await.unwrap().get(0);
    let run_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,autopilot_run_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&rt,&run]).await.unwrap().get(0);
    let raw = format!("mdt_gc_{ws}");
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,'gc-daemon',$3,now()+interval '1 hour')", &[&hash,&ws,&owner]).await.unwrap();
    let secret = b"synthetic-gc-only";
    let jwt = |id| {
        encode(&Header::default(), &json!({"sub":id,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(secret)).unwrap()
    };
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), secret.to_vec()),
    );
    assert_eq!(
        get(&app, &valid.to_string(), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(&app, "not-uuid", Some(&raw)).await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid task_id"}))
    );
    assert_eq!(
        get(&app, &Uuid::new_v4().to_string(), Some(&raw)).await,
        (StatusCode::NOT_FOUND, json!({"error":"task not found"}))
    );
    assert_eq!(
        get(&app, &valid.to_string(), Some(&raw)).await,
        (
            StatusCode::OK,
            json!({"status":"completed","completed_at":"0001-01-01T00:00:00Z"})
        )
    );
    assert_eq!(
        get(&app, &quick.to_string(), Some(&raw)).await.0,
        StatusCode::OK
    );
    for id in [chat_task, run_task] {
        assert_eq!(
            get(&app, &id.to_string(), Some(&raw)).await.0,
            StatusCode::OK
        );
    }
    for id in [foreign, wrong_runtime, no_runtime, no_scope, peer_task] {
        assert_eq!(
            get(&app, &id.to_string(), Some(&raw)).await.0,
            StatusCode::NOT_FOUND,
            "{id}"
        );
    }
    assert_eq!(
        get(&app, &valid.to_string(), Some(&jwt(owner))).await.0,
        StatusCode::OK
    );
    assert_eq!(
        get(&app, &valid.to_string(), Some(&jwt(peer))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &valid.to_string(), Some(&jwt(stranger))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, &peer_task.to_string(), Some(&jwt(peer))).await.0,
        StatusCode::OK
    );
    let pat = format!("mul_gc_{ws}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'gc-test',$2,'mul_gc')", &[&owner,&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    assert_eq!(
        get(&app, &valid.to_string(), Some(&pat)).await.0,
        StatusCode::OK
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hex::encode(Sha256::digest(pat.as_bytes()))],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &valid.to_string(), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("ALTER TABLE issue RENAME TO issue_gc_unavailable", &[])
        .await
        .unwrap();
    assert_eq!(
        get(&app, &valid.to_string(), Some(&raw)).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to load task"})
        )
    );
    db.execute("ALTER TABLE issue_gc_unavailable RENAME TO issue", &[])
        .await
        .unwrap();
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE id=$1",
            &[&valid],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
}
