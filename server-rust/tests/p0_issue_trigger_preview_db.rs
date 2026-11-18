//! Read-only trigger-preview contract against an isolated, migrated PostgreSQL 17.
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

const SECRET: &[u8] = b"synthetic-trigger-preview-test-only";

async fn preview(app: &axum::Router, ws: Uuid, jwt: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/issues/preview-trigger?workspace_id={ws}"))
        .header("authorization", format!("Bearer {jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn trigger_preview_is_read_only_and_workspace_scoped() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
    assert!(!config.get_hosts().is_empty());
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let owner = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let other_ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let squad = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let foreign_issue = Uuid::new_v4();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES($1,'Trigger test',$2)",
        &[&owner, &format!("trigger-{owner}@invalid.example")],
    )
    .await
    .unwrap();
    for workspace in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Trigger test',$2)",
            &[&workspace, &format!("trigger-{workspace}")],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
            &[&workspace, &owner],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,status) VALUES($1,$2,'Trigger runtime','cloud','synthetic','online')", &[&runtime, &ws]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,$2,'Trigger agent','cloud',$3,$4)", &[&agent, &ws, &owner, &runtime]).await.unwrap();
    db.execute("INSERT INTO squad(id,workspace_id,name,leader_id,creator_id) VALUES($1,$2,'Trigger squad',$3,$4)", &[&squad, &ws, &agent, &owner]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,status,assignee_type,assignee_id) VALUES($1,$2,'Pending issue','member',$3,47,'backlog','agent',$4)", &[&issue, &ws, &owner, &agent]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,status,assignee_type,assignee_id) VALUES($1,$2,'Foreign issue','member',$3,48,'backlog','agent',$4)", &[&foreign_issue, &other_ws, &owner, &agent]).await.unwrap();
    let jwt = encode(
        &Header::default(),
        &json!({"sub":owner,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let (code, body) = preview(
        &app,
        ws,
        &jwt,
        json!({"is_create":true,"status":"todo","assignee_type":"agent","assignee_id":agent}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"triggers":[{"issue_id":"","agent_id":agent,"source":"assign"}],"total_count":1})
    );
    assert_eq!(
        preview(
            &app,
            ws,
            &jwt,
            json!({"is_create":true,"status":"backlog","assignee_type":"agent","assignee_id":agent})
        )
        .await,
        (StatusCode::OK, json!({"triggers":[],"total_count":0}))
    );
    let (code, body) = preview(
        &app,
        ws,
        &jwt,
        json!({"is_create":true,"status":"todo","assignee_type":"squad","assignee_id":squad}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({"triggers":[{"issue_id":"","agent_id":agent,"source":"assign"}],"total_count":1})
    );
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        preview(
            &app,
            ws,
            &jwt,
            json!({"is_create":true,"assignee_type":"agent","assignee_id":agent})
        )
        .await,
        (StatusCode::OK, json!({"triggers":[],"total_count":0}))
    );
    db.execute("UPDATE agent SET archived_at=NULL WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        preview(&app, ws, &jwt, json!({"issue_ids":[issue],"status":"todo"})).await,
        (
            StatusCode::OK,
            json!({"triggers":[{"issue_id":issue,"agent_id":agent,"source":"status"}],"total_count":1})
        )
    );
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,$4,'queued')", &[&Uuid::new_v4(), &agent, &runtime, &issue]).await.unwrap();
    assert_eq!(
        preview(&app, ws, &jwt, json!({"issue_ids":[issue],"status":"todo"})).await,
        (StatusCode::OK, json!({"triggers":[],"total_count":0}))
    );
    assert_eq!(
        preview(
            &app,
            ws,
            &jwt,
            json!({"issue_ids":[foreign_issue],"status":"todo"})
        )
        .await,
        (StatusCode::OK, json!({"triggers":[],"total_count":0}))
    );
    assert_eq!(
        preview(
            &app,
            ws,
            &jwt,
            json!({"issue_ids":vec![issue.to_string();501]})
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"too many issue_ids"})
        )
    );
    assert_eq!(
        preview(
            &app,
            ws,
            &jwt,
            json!({"is_create":true,"assignee_type":"agent","assignee_id":"bad"})
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid assignee_id"})
        )
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}
