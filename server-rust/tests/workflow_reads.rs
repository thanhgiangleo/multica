//! Frozen workflow and project-resource reads on an isolated migrated database.
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
async fn workflow_and_resource_reads_are_isolated_and_snapshot_once() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["owner", "member", "outsider"] {
        let email = format!("workflow-{name}-{suffix}@invalid.example");
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &email],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Workflow A',$1) RETURNING id",
            &[&format!("workflow-a-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Workflow B',$1) RETURNING id",
            &[&format!("workflow-b-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let custom: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps,owner_id) VALUES ($1,'Named','todo',$2,$3) RETURNING id", &[&ws,&json!([{"status_key":"todo","handler":{"type":"agent","id":"synthetic-agent"},"instructions":"do <&>"},{"status_key":"claimed"},{"status_key":"done"}]),&users[0]]).await.unwrap().get(0);
    let corrupt: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES ($1,'Malformed','todo',$2) RETURNING id", &[&ws,&json!([{"handler":7}])]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position,is_system) VALUES ($1,'unclaimed','Unclaimed','started','#123456',1,false),($1,'claimed','Claimed','started','#123456',2,false),($1,'archived','Archived','started','#123456',3,false)", &[&ws]).await.unwrap();
    db.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key='archived'",
        &[&ws],
    )
    .await
    .unwrap();
    let implicit: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Implicit') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let explicit: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES ($1,'Explicit',$2) RETURNING id", &[&ws,&custom]).await.unwrap().get(0);
    let dangling: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES ($1,'Dangling',$2) RETURNING id", &[&ws,&Uuid::new_v4()]).await.unwrap().get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Foreign') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    let resource: Uuid = db.query_one("INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,position,created_by) VALUES ($1,$2,'github_repo',$3,1,$4) RETURNING id", &[&implicit,&ws,&json!({"url":"https://example.invalid/a"}),&users[0]]).await.unwrap().get(0);
    db.execute("INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,position) VALUES ($1,$2,'local_directory',$3,0)", &[&implicit,&ws,&json!({"path":"/synthetic"})]).await.unwrap();
    // Deliberate isolation beyond Go: a corrupt resource workspace must not leak.
    db.execute("INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,position) VALUES ($1,$2,'github_repo',$3,2)", &[&implicit,&other,&json!({"url":"https://other.invalid/private"})]).await.unwrap();
    let pat = format!("mul_workflow_{suffix}");
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_workflow')", &[&users[0],&hex::encode(Sha256::digest(pat.as_bytes()))]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"synthetic-workflow-secret".to_vec()),
    );
    drop(db);
    let auth = |id: Uuid| {
        encode(&Header::default(),&json!({"sub":id.to_string(),"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(b"synthetic-workflow-secret")).unwrap()
    };
    let owner = auth(users[0]);
    let member = auth(users[1]);
    let outsider = auth(users[2]);
    let path = format!("/api/issue-workflows/?workspace_id={ws}");
    assert_eq!(read(&app, &path, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, &path, Some(&outsider)).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"workspace not found"})
        )
    );
    let db = pool.get().await.unwrap();
    let absent: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND is_default",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(absent, 0);
    drop(db);
    let first = read(&app, &path, Some(&member)).await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(first.1["total"], 2); // malformed row omitted, still stored
    let default = first.1["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["is_default"] == true)
        .unwrap();
    let default_id = Uuid::parse_str(default["id"].as_str().unwrap()).unwrap();
    let statuses: Vec<&str> = default["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["status_key"].as_str().unwrap())
        .collect();
    assert_eq!(
        statuses,
        [
            "backlog",
            "todo",
            "unclaimed",
            "in_progress",
            "in_review",
            "blocked",
            "done",
            "cancelled"
        ]
    );
    assert_eq!(default["project_ids"], json!([dangling, implicit]));
    assert_eq!(first.1["workflows"][1]["project_ids"], json!([explicit]));
    assert_eq!(
        first.1["workflows"][1]["steps"][0]["instructions"],
        "do <&>"
    );
    assert_eq!(read(&app, &path, Some(&pat)).await.1, first.1);
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/{custom}/?workspace_id={ws}"),
            Some(&member)
        )
        .await
        .1,
        first.1["workflows"][1]
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/{corrupt}/?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1["steps"],
        json!([])
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/{custom}/?workspace_id={other}"),
            Some(&owner)
        )
        .await,
        (StatusCode::NOT_FOUND, json!({"error":"workflow not found"}))
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/{custom}/?workspace_id={ws}"),
            Some(&outsider)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/not-a-uuid/?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid workflow id"})
        )
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/issue-workflows/{default_id}/?workspace_id={ws}"),
            Some(&owner)
        )
        .await
        .1,
        first.1["workflows"][0]
    );
    let resources = read(
        &app,
        &format!("/api/projects/{implicit}/resources?workspace_id={ws}"),
        Some(&member),
    )
    .await;
    assert_eq!(resources.0, StatusCode::OK);
    assert_eq!(resources.1["total"], 2);
    assert_eq!(resources.1["resources"][1]["id"], resource.to_string());
    assert_eq!(
        resources.1["resources"][1]["created_by"],
        users[0].to_string()
    );
    assert!(!resources.1.to_string().contains("other.invalid"));
    for (candidate, expected) in [
        (foreign, StatusCode::NOT_FOUND),
        (Uuid::new_v4(), StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            read(
                &app,
                &format!("/api/projects/{candidate}/resources?workspace_id={ws}"),
                Some(&owner)
            )
            .await
            .0,
            expected
        );
    }
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/not-a-uuid/resources?workspace_id={ws}"),
            Some(&owner)
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid project id"})
        )
    );
    assert_eq!(
        read(
            &app,
            &format!("/api/projects/{implicit}/resources?workspace_id={ws}"),
            Some(&outsider)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let db = pool.get().await.unwrap();
    let stored: Value = db
        .query_one(
            "SELECT steps FROM issue_workflow WHERE id=$1",
            &[&default_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored.as_array().unwrap().len(), 8);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND is_default",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND is_default",
            &[&other]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let empty = read(
        &app,
        &format!("/api/projects/{explicit}/resources?workspace_id={ws}"),
        Some(&owner),
    )
    .await;
    assert_eq!(empty, (StatusCode::OK, json!({"resources":[],"total":0})));
}
