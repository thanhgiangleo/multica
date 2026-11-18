//! Synthetic PostgreSQL daemon status polling; no installed daemon or Fleet account.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
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

const SECRET: &[u8] = b"synthetic-status-only";

fn jwt(user: Uuid) -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &json!({"sub":user}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

async fn get(app: &axum::Router, path: &str, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(path);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn status_poll_enforces_runtime_owner_daemon_and_qoder_revocation() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        peer,
        outside,
        ws,
        other_ws,
        agent,
        own_rt,
        peer_rt,
        other_rt,
        qoder_rt,
        task,
        peer_task,
        foreign_task,
        unbound_task,
        qoder_task,
    ]: [Uuid; 15] = std::array::from_fn(|_| Uuid::new_v4());
    for id in [owner, peer, outside] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'status-test',$2)",
            &[&id, &format!("{id}@status.invalid")],
        )
        .await
        .unwrap();
    }
    for id in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'status-test',$2)",
            &[&id, &format!("status-{id}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "owner"), (peer, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,'status-test','cloud',$3)", &[&agent,&ws,&owner]).await.unwrap();
    for (id, workspace, daemon, runtime_owner) in [
        (own_rt, ws, "test-daemon", owner),
        (peer_rt, ws, "peer-daemon", peer),
        (other_rt, other_ws, "other-daemon", owner),
        (qoder_rt, ws, "qoder-managed-test", peer),
    ] {
        db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id) VALUES ($1,$2,$3,'status-test','cloud','synthetic','online',$4)", &[&id,&workspace,&daemon,&runtime_owner]).await.unwrap();
    }
    for (id, runtime) in [
        (task, Some(own_rt)),
        (peer_task, Some(peer_rt)),
        (foreign_task, Some(other_rt)),
        (unbound_task, None),
        (qoder_task, Some(qoder_rt)),
    ] {
        let status = if runtime.is_some() {
            "running"
        } else {
            "completed"
        };
        db.execute(
            "INSERT INTO agent_task_queue(id,agent_id,runtime_id,status,completed_at) VALUES ($1,$2,$3,$4,CASE WHEN $3::uuid IS NULL THEN now() ELSE NULL END)",
            &[&id, &agent, &runtime, &status],
        )
        .await
        .unwrap();
    }
    let token = format!("mdt_synthetic_status_{ws}");
    let wrong = format!("mdt_synthetic_wrong_{ws}");
    let qoder = format!("mdt_synthetic_qoder_{ws}");
    for (raw, daemon) in [
        (&token, "test-daemon"),
        (&wrong, "wrong-daemon"),
        (&qoder, "qoder-managed-test"),
    ] {
        let hash = hex::encode(Sha256::digest(raw.as_bytes()));
        let token_owner = if daemon.starts_with("qoder-managed-") {
            None
        } else {
            Some(owner)
        };
        db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES ($1,$2,$3,$4,now()+interval '1 hour')", &[&hash,&ws,&daemon,&token_owner]).await.unwrap();
    }
    let qoder_hash = hex::encode(Sha256::digest(qoder.as_bytes()));
    db.execute("INSERT INTO cloud_agent_connection(workspace_id,provider,owner_id,token_id,config_encrypted) SELECT $1,'qoder_cloud',$2,id,'synthetic-only' FROM daemon_token WHERE token_hash=$3", &[&ws,&owner,&qoder_hash]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let url = |id| format!("/api/daemon/tasks/{id}/status");
    let snapshot: Value = db
        .query_one(
            "SELECT jsonb_agg(to_jsonb(q) ORDER BY q.id) FROM agent_task_queue q WHERE agent_id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    for (id, credential, status, body) in [
        (
            task,
            Some(jwt(owner)),
            StatusCode::OK,
            json!({"status":"running"}),
        ),
        (
            task,
            Some(token.clone()),
            StatusCode::OK,
            json!({"status":"running"}),
        ),
        (
            task,
            Some(wrong.clone()),
            StatusCode::NOT_FOUND,
            json!({"error":"task not found"}),
        ),
        (
            task,
            Some(jwt(peer)),
            StatusCode::NOT_FOUND,
            json!({"error":"task not found"}),
        ),
        (
            peer_task,
            Some(jwt(peer)),
            StatusCode::OK,
            json!({"status":"running"}),
        ),
        (
            peer_task,
            Some(token.clone()),
            StatusCode::NOT_FOUND,
            json!({"error":"task not found"}),
        ),
        (
            task,
            Some(jwt(outside)),
            StatusCode::NOT_FOUND,
            json!({"error":"not found"}),
        ),
        (
            foreign_task,
            Some(jwt(owner)),
            StatusCode::NOT_FOUND,
            json!({"error":"task not found"}),
        ),
        (
            unbound_task,
            Some(jwt(peer)),
            StatusCode::OK,
            json!({"status":"completed"}),
        ),
        (
            unbound_task,
            Some(token.clone()),
            StatusCode::OK,
            json!({"status":"completed"}),
        ),
        (
            qoder_task,
            Some(qoder.clone()),
            StatusCode::OK,
            json!({"status":"running"}),
        ),
        (
            task,
            None,
            StatusCode::UNAUTHORIZED,
            json!({"error":"missing authorization header"}),
        ),
    ] {
        let (actual, response) = get(&app, &url(id), credential.as_deref()).await;
        assert_eq!(
            (actual, response),
            (status, body),
            "task {id} credential {credential:?}"
        );
    }
    assert_eq!(
        get(&app, &url(Uuid::new_v4()), Some(&token)).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&app, "/api/daemon/tasks/invalid/status", Some(&token))
            .await
            .1,
        json!({"error":"invalid task_id"})
    );
    db.execute(
        "UPDATE cloud_agent_connection SET enabled=false WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(qoder_task), Some(&qoder)).await.1,
        json!({"error":"runtime access revoked"})
    );
    db.execute(
        "UPDATE cloud_agent_connection SET enabled=true WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        get(&app, &url(qoder_task), Some(&qoder)).await.0,
        StatusCode::FORBIDDEN
    );
    let after: Value = db
        .query_one(
            "SELECT jsonb_agg(to_jsonb(q) ORDER BY q.id) FROM agent_task_queue q WHERE agent_id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(snapshot, after, "GET changed task rows");
}
