//! Workflow creation and deletion require an owned migrated PostgreSQL database.
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

#[path = "workflow_support/sink.rs"]
mod sink;

const SECRET: &str = "zbsi65-workflow-synthetic-secret";
async fn call(app: &axum::Router, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("DELETE")
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn workflow_delete_respects_owner_default_usage_and_workspace() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(10)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let tag = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["creator", "member", "admin", "outsider"] {
        let id: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[&role, &format!("{role}-{tag}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        users.push(id);
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Workflow test',$1) RETURNING id",
            &[&format!("workflow-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign workflow test',$1) RETURNING id",
            &[&format!("workflow-other-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (id, role, workspace) in [
        (users[0], "member", ws),
        (users[1], "member", ws),
        (users[2], "admin", ws),
        (users[3], "owner", other),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &id, &role],
        )
        .await
        .unwrap();
    }
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,owner_id) VALUES($1,'Test','todo',$2) RETURNING id",&[&ws,&users[0]]).await.unwrap().get(0);
    let default:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,is_default) VALUES($1,'Default','todo',true) RETURNING id",&[&ws]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let jwt = |id: Uuid, ttl: i64| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(users[0], 3600);
    let member = jwt(users[1], 3600);
    let admin = jwt(users[2], 3600);
    let outsider = jwt(users[3], 3600);
    let expired = jwt(users[0], -3600);
    let path = format!("/api/issue-workflows/{workflow}/?workspace_id={ws}");
    assert_eq!(
        call(&app, &path, &expired).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(call(&app, &path, &member).await.0, StatusCode::FORBIDDEN);
    assert_eq!(call(&app, &path, &outsider).await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        call(
            &app,
            &format!("/api/issue-workflows/not-a-uuid/?workspace_id={ws}"),
            &owner
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            &format!("/api/issue-workflows/{workflow}/?workspace_id={other}"),
            &outsider
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, body) = call(
        &app,
        &format!("/api/issue-workflows/{default}/?workspace_id={ws}"),
        &admin,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "workflow_is_default");
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Linked',$2) RETURNING id",&[&ws,&workflow]).await.unwrap().get(0);
    let (status, body) = call(&app, &path, &owner).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "workflow_in_use");
    assert_eq!(body["project_count"], 1);
    db.execute(
        "UPDATE project SET workflow_id=NULL WHERE id=$1",
        &[&project],
    )
    .await
    .unwrap();
    // pgx UUID scanner accepts 32 hex or arbitrary separator bytes at the four
    // UUID delimiter positions. The URL form still identifies the same row.
    let compact = workflow.simple().to_string();
    assert_eq!(
        call(
            &app,
            &format!("/api/issue-workflows/{compact}/?workspace_id={ws}"),
            &owner
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,owner_id) VALUES($1,'Test again','todo',$2) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let path = format!("/api/issue-workflows/{workflow}/?workspace_id={ws}");
    let odd = workflow.to_string().replace('-', "x");
    let odd_path = format!("/api/issue-workflows/{odd}/?workspace_id={ws}");
    let (first, second) = tokio::join!(call(&app, &path, &owner), call(&app, &odd_path, &admin));

    assert!(
        (first.0 == StatusCode::NO_CONTENT && second.0 == StatusCode::NOT_FOUND)
            || (second.0 == StatusCode::NO_CONTENT && first.0 == StatusCode::NOT_FOUND),
        "concurrent delete: {first:?} {second:?}"
    );
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_workflow WHERE id=$1",
            &[&workflow],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    let admin_workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,owner_id) VALUES($1,'By admin',$2,$3) RETURNING id",&[&ws,&"todo",&users[1]]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            &format!("/api/issue-workflows/{admin_workflow}?workspace_id={ws}"),
            &admin
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    db.execute("DELETE FROM project WHERE id=$1", &[&project])
        .await
        .unwrap();
    db.execute("DELETE FROM issue_workflow WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 OR workspace_id=$2",
        &[&ws, &other],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1 OR id=$2", &[&ws, &other])
        .await
        .unwrap();
    for id in users {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&id])
            .await
            .unwrap();
    }
}

async fn create_call(
    app: &axum::Router,
    workspace: Uuid,
    token: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/issue-workflows/?workspace_id={workspace}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn raw_workflow_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    payload: String,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(payload))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn workflow_create_persists_and_rolls_back_invalid_inputs() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(10)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let tag = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &role,
                    &format!("workflow-create-{role}-{tag}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Workflow create',$1) RETURNING id",
            &[&format!("workflow-create-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign workflow',$1) RETURNING id",
            &[&format!("foreign-workflow-create-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (id, role, workspace) in [
        (users[0], "owner", ws),
        (users[1], "member", ws),
        (users[2], "owner", foreign),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &id, &role],
        )
        .await
        .unwrap();
    }
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let jwt = |id: Uuid, ttl: i64| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(users[0], 3600);
    let member = jwt(users[1], 3600);
    let outsider = jwt(users[2], 3600);
    assert_eq!(
        create_call(&app, ws, &jwt(users[0], -3600), json!({"name":"X"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        create_call(&app, ws, &outsider, json!({"name":"X"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, body) = create_call(
        &app,
        ws,
        &member,
        json!({"name":"No admin status", "steps":[{"new_status":{"name":"Needs approval"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["code"], "status_create_forbidden");
    let workflow_path = format!("/api/issue-workflows/?workspace_id={ws}");
    let (status, body) = raw_workflow_call(
        &app,
        "POST",
        &workflow_path,
        &owner,
        format!(
            r#"{{"name":"discarded","NAME":"Raw workflow","steps":null}}{}"#,
            "x".repeat(2_200_000)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["name"], "Raw workflow");
    let raw_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let stored: String = db
        .query_one("SELECT name FROM issue_workflow WHERE id=$1", &[&raw_id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "Raw workflow");
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&raw_id])
        .await
        .unwrap();
    let (status, body) = raw_workflow_call(
        &app, "POST", &workflow_path, &owner,
        r#"{"name":"Reused steps","steps":[{"status_key":"in_progress","handler":{"type":"none"}},{"status_key":"done"}],"STEPS":[{"instructions":"handoff"}]}"#.to_owned(),
    ).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["steps"][1]["status_key"], "in_progress");
    let reused_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&reused_id])
        .await
        .unwrap();
    let prior: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    for bad in [
        r#"{"name":"bad","steps":{}}"#,
        r#"{"name":"bad","dry_run":"yes"}"#,
        r#"{"name":"bad","steps":[{"handler":{"type":123}}]}"#,
    ] {
        assert_eq!(
            raw_workflow_call(&app, "POST", &workflow_path, &owner, bad.into())
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let after: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(prior, after, "invalid request must not write workflow rows");
    let invalid = json!({"name":"Invalid flow", "steps":[{"new_status":{"name":"Review waiting"}}, {"status_key":"missing"}]});
    let (status, body) = create_call(&app, ws, &owner, invalid).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_workflow");
    let absent: i64 = db.query_one("SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND lower(name)='review waiting'", &[&ws]).await.unwrap().get(0);
    assert_eq!(absent, 0, "status must roll back with workflow");
    let (status, body) = create_call(&app, ws, &owner, json!({"name":"Delivery path", "description":"  A test flow  ", "steps":[
        {"new_status":{"name":"Review waiting","category":"started","color":"ABCDEF"}, "handler":{"type":"member","id":users[1]}, "instructions":"  Check output  ", "next_status_key":"done"},
        {"status_key":"in_progress","handler":{"type":"project_lead"}},
    ]})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["description"], "A test flow");
    assert_eq!(body["owner_id"], users[0].to_string());
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let stored: Value = db
        .query_one(
            "SELECT steps FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored.as_array().unwrap().len(), 5);
    assert_eq!(stored[0]["status_key"], "todo");
    assert_eq!(stored[1]["handler"]["type"], "member");
    assert_eq!(stored[1]["instructions"], "Check output");
    assert_eq!(stored[2]["status_key"], "in_progress");
    assert_eq!(stored[3]["status_key"], "done");
    let status_key: String = db.query_one("SELECT key FROM issue_status WHERE workspace_id=$1 AND name='Review waiting' AND color='#abcdef'", &[&ws]).await.unwrap().get(0);
    assert_eq!(stored[1]["status_key"], status_key);
    let default_steps: Value = db
        .query_one(
            "SELECT steps FROM issue_workflow WHERE workspace_id=$1 AND is_default",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !default_steps.to_string().contains(&status_key),
        "Default snapshot excludes new statuses"
    );
    let (status, body) = create_call(
        &app,
        ws,
        &member,
        json!({"name":"Reuse status", "steps":[{"new_status":{"name":"review WAITING"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["steps"][1]["status_key"], status_key);
    let (a, b) = tokio::join!(
        create_call(&app, ws, &owner, json!({"name":"Collision"})),
        create_call(&app, ws, &member, json!({"name":"Collision"})),
    );
    assert!(
        (a.0 == StatusCode::CREATED && b.0 == StatusCode::CONFLICT)
            || (b.0 == StatusCode::CREATED && a.0 == StatusCode::CONFLICT),
        "{a:?} {b:?}"
    );
    assert_eq!(
        create_call(&app, ws, &member, json!({"name":"Default"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(create_call(&app, ws, &owner, json!({"name":"Bad handoff", "steps":[{"status_key":"todo","handler":{"type":"member","id":users[2]}}]})).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(create_call(&app, ws, &owner, json!({"name":"Bad agent", "steps":[{"status_key":"todo","handler":{"type":"agent","id":Uuid::new_v4()}}]})).await.0, StatusCode::FORBIDDEN);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND NOT is_default",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 3);
    db.execute("DELETE FROM issue_workflow WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap();
    db.execute("DELETE FROM issue_status WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 OR workspace_id=$2",
        &[&ws, &foreign],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM workspace WHERE id=$1 OR id=$2",
        &[&ws, &foreign],
    )
    .await
    .unwrap();
    for id in users {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&id])
            .await
            .unwrap();
    }
}

async fn patch_call(
    app: &axum::Router,
    workspace: Uuid,
    id: &str,
    token: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("PATCH")
        .uri(format!(
            "/api/issue-workflows/{id}/?workspace_id={workspace}"
        ))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn workflow_patch_dry_run_guards_and_safe_updates() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(10)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let tag = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &role,
                    &format!("workflow-patch-{role}-{tag}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Workflow patch',$1) RETURNING id",
            &[&format!("workflow-patch-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other workflow patch',$1) RETURNING id",
            &[&format!("workflow-patch-other-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role, workspace) in [
        (users[0], "owner", ws),
        (users[1], "member", ws),
        (users[2], "owner", foreign),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,owner_id,steps) VALUES($1,'Editable','todo',$2,$3) RETURNING id", &[&ws,&users[0],&json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"done"},{"status_key":"cancelled"}])]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let jwt = |user: Uuid, ttl: i64| {
        encode(
            &Header::default(),
            &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let owner = jwt(users[0], 3600);
    let member = jwt(users[1], 3600);
    let outsider = jwt(users[2], 3600);
    let path = workflow.to_string();
    let edit = json!({"name":"Renamed", "description":"  Updated flow  ","steps":[{"status_key":"in_progress","handler":{"type":"member","id":users[1]}},{"status_key":"done"}]});
    assert_eq!(
        patch_call(&app, ws, &path, &jwt(users[0], -3600), edit.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        patch_call(&app, ws, &path, &outsider, edit.clone()).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        patch_call(&app, ws, &path, &member, edit.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        patch_call(&app, ws, "not-a-uuid", &owner, edit.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let compact = workflow.simple().to_string();
    let (status, dry_run) = patch_call(
        &app,
        ws,
        &compact,
        &owner,
        json!({"name":"UUID-compatible", "dry_run":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{dry_run}");
    let stored: String = db
        .query_one("SELECT name FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "Editable");
    let odd = workflow.to_string().replace('-', "x");
    let (status, dry_run) = patch_call(
        &app,
        ws,
        &odd,
        &owner,
        json!({"name":"UUID-compatible", "dry_run":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{dry_run}");
    let raw_path = format!("/api/issue-workflows/{path}/?workspace_id={ws}");
    let (status, body) = raw_workflow_call(
        &app,
        "PATCH",
        &raw_path,
        &owner,
        r#"{"NAME":"Should not persist","name":null,"dry_run":true,"steps":null}"#.to_owned(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["dry_run"], true);
    let stored: String = db
        .query_one("SELECT name FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "Editable", "dry run must roll back");
    let (status, body) = patch_call(
        &app,
        ws,
        &path,
        &owner,
        json!({"dry_run":true,"name":"Preview","steps":[{"new_status":{"name":"Only preview"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["plan"]["affected_issues"], 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND name='Only preview'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let (status, body) = patch_call(&app, ws, &path, &owner, edit.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["description"], "Updated flow");
    assert_eq!(body["steps"][1]["handler"]["type"], "member");
    let stored: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored[1]["handler"]["id"], users[1].to_string());
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Using flow',$2) RETURNING id", &[&ws,&workflow]).await.unwrap().get(0);
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number) VALUES($1,'Moved?','member',$2,$3,'in_progress',1) RETURNING id", &[&ws,&users[0],&project]).await.unwrap().get(0);
    let remove_status = json!({"steps":[{"status_key":"todo"},{"status_key":"done"}],"status_mapping":{"in_progress":"done"}});
    let (status, body) = patch_call(
        &app,
        ws,
        &path,
        &owner,
        json!({"dry_run":true,"steps":remove_status["steps"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["plan"]["required"][0]["issue_count"], 1);
    let (status, body) = patch_call(&app, ws, &path, &owner, remove_status).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(
        body["error"],
        "workflow issue remapping is not available yet"
    );
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "in_progress"
    );
    assert_eq!(
        db.query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
            .await
            .unwrap()
            .get::<_, Value>(0),
        stored
    );
    let blocked:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number) VALUES($1,'Blocked mapping','member',$2,$3,'blocked',2) RETURNING id", &[&ws,&users[0],&project]).await.unwrap().get(0);
    for (mapping, message) in [
        (json!({"blocked":"done"}), "choose a destination status"),
        (
            json!({"blocked":"done","in_progress":"missing"}),
            "is not part of the target workflow",
        ),
    ] {
        let (status, body) = patch_call(
            &app,
            ws,
            &path,
            &owner,
            json!({"steps":[{"status_key":"todo"}],"status_mapping":mapping}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(body["code"], "workflow_status_mapping_required");
        assert!(body["error"].as_str().unwrap().contains(message));
        assert_eq!(body["plan"]["total_issues"], 2);
        assert_eq!(body["plan"]["affected_issues"], 2);
        assert_eq!(body["plan"]["required"].as_array().unwrap().len(), 2);
        assert!(body["plan"].get("Required").is_none());
    }
    db.execute("DELETE FROM issue WHERE id=$1", &[&blocked])
        .await
        .unwrap();
    db.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    db.execute("DELETE FROM project WHERE id=$1", &[&project])
        .await
        .unwrap();
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap();
    db.execute("DELETE FROM issue_status WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 OR workspace_id=$2",
        &[&ws, &foreign],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM workspace WHERE id=$1 OR id=$2",
        &[&ws, &foreign],
    )
    .await
    .unwrap();
    for user in users {
        db.execute("DELETE FROM \"user\" WHERE id=$1", &[&user])
            .await
            .unwrap();
    }
}
