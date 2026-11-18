//! Go-shaped workflow-handoff preview against a disposable migrated PostgreSQL.
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

const SECRET: &[u8] = b"synthetic-workflow-handoff-only";

async fn get(
    app: &axum::Router,
    ws: Uuid,
    id: &str,
    query: &str,
    jwt: &str,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!(
            "/api/issues/{id}/workflow-handoff?workspace_id={ws}&{query}"
        ))
        .header("authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires run-owned disposable PostgreSQL on an isolated loopback port"]
async fn preview_handoff_matches_go_response_and_never_writes() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_handoff"));
    assert!(config.get_hosts().iter().all(
        |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
    ));
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let other = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let foreign_issue = Uuid::new_v4();
    let task = Uuid::new_v4();
    let tag = Uuid::new_v4().simple().to_string();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES ($1,'Preview owner',$2)",
        &[&user, &format!("handoff-{tag}@invalid.example")],
    )
    .await
    .unwrap();
    for (id, name, prefix) in [
        (ws, "Preview workspace", "PRE"),
        (other, "Foreign workspace", "FOR"),
    ] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,$2,$3,$4)",
            &[&id, &name, &format!("handoff-{id}"), &prefix],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,owner_id,status,last_seen_at) VALUES ($1,$2,'Synthetic runtime','local','synthetic',$3,'online',now())", &[&runtime,&ws,&user]).await.unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,$2,'Preview agent','local',$3,$4)", &[&agent,&ws,&user,&runtime]).await.unwrap();
    let steps = json!([
        {"status_key":"todo"},
        {"status_key":"in_progress","handler":{"type":"agent","id":agent.to_string()},"instructions":"Implement it and open a PR."},
        {"status_key":"in_review"}, {"status_key":"done"}, {"status_key":"cancelled"}
    ]);
    let flow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES ($1,'Delivery','todo',$2) RETURNING id", &[&ws,&steps]).await.unwrap().get(0);
    let project: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES ($1,'Launch',$2) RETURNING id", &[&ws,&flow]).await.unwrap().get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Elsewhere') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,project_id,status,number,assignee_type,assignee_id) VALUES ($1,$2,'Preview','member',$3,$4,'todo',42,'agent',$5)", &[&issue,&ws,&user,&project,&agent]).await.unwrap();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,status,number) VALUES ($1,$2,'Foreign','member',$3,'todo',12)", &[&foreign_issue,&other,&user]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,$4,'queued')", &[&task,&agent,&runtime,&issue]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let jwt = encode(
        &Header::default(),
        &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();

    let (code, plain) = get(&app, ws, &issue.to_string(), "status=done", &jwt).await;
    assert_eq!(code, StatusCode::OK, "{plain}");
    assert_eq!(
        plain,
        json!({"handoff":false,"from_status":"todo","to_status":"done","previous_assignee_type":"agent","previous_assignee_id":agent,"previous_runs":[]})
    );
    let (code, same) = get(&app, ws, &issue.to_string(), "status=todo", &jwt).await;
    assert_eq!(code, StatusCode::OK, "{same}");
    assert_eq!(same["handoff"], false);

    let (code, preview) = get(&app, ws, "PRE-42", "status=in_progress", &jwt).await;
    assert_eq!(code, StatusCode::OK, "{preview}");
    assert_eq!(preview["handoff"], true);
    assert_eq!(preview["workflow_name"], "Delivery");
    assert_eq!(preview["handler_type"], "agent");
    assert_eq!(preview["handler_id"], agent.to_string());
    // Same handler retains their run; a previous run is only offered when
    // the prospective handler changes.
    assert_eq!(preview["previous_runs"], json!([]));
    assert!(
        preview["brief"]
            .as_str()
            .unwrap()
            .contains("This project's workflow — Delivery (Launch)")
    );
    assert!(
        preview["brief"]
            .as_str()
            .unwrap()
            .contains("Implement it and open a PR.")
    );
    assert!(preview["brief"].as_str().unwrap().contains("PRE-42"));
    db.execute(
        "UPDATE issue SET assignee_type='squad',assignee_id=$2 WHERE id=$1",
        &[&issue, &Uuid::new_v4()],
    )
    .await
    .unwrap();
    let squad: Uuid = db
        .query_one("SELECT assignee_id FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE agent_task_queue SET squad_id=$2 WHERE id=$1",
        &[&task, &squad],
    )
    .await
    .unwrap();
    let (code, previous) = get(&app, ws, &issue.to_string(), "status=in_progress", &jwt).await;
    assert_eq!(code, StatusCode::OK, "{previous}");
    assert_eq!(previous["previous_assignee_type"], "squad");
    assert_eq!(previous["previous_runs"][0]["task_id"], task.to_string());
    assert_eq!(previous["previous_runs"][0]["agent_id"], agent.to_string());
    assert_eq!(previous["previous_runs"][0]["status"], "queued");
    assert!(previous["previous_runs"][0]["started_at"].is_string());
    assert_eq!(previous["previous_runs"].as_array().unwrap().len(), 1);

    let (code, missing) = get(&app, ws, &issue.to_string(), "", &jwt).await;
    assert_eq!(
        (code, missing["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("status is required"))
    );
    let (code, invalid) = get(&app, ws, &issue.to_string(), "status=not_in_flow", &jwt).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["code"], "status_not_in_workflow");
    assert_eq!(invalid["workflow_name"], "Delivery");
    assert_eq!(
        invalid["allowed_statuses"],
        json!(["todo", "in_progress", "in_review", "done", "cancelled"])
    );
    assert_eq!(
        invalid["error"],
        "status \"not_in_flow\" is not part of this project's workflow \"Delivery\"; use one of: todo, in_progress, in_review, done, cancelled"
    );
    let (code, invalid) = get(&app, ws, &issue.to_string(), "project_id=not-a-uuid", &jwt).await;
    assert_eq!(
        (code, invalid["error"].as_str()),
        (StatusCode::BAD_REQUEST, Some("invalid project_id"))
    );
    let (code, invalid) = get(
        &app,
        ws,
        &issue.to_string(),
        &format!("project_id={foreign_project}"),
        &jwt,
    )
    .await;
    assert_eq!(
        (code, invalid["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("project not found"))
    );
    let (code, invalid) = get(&app, ws, &foreign_issue.to_string(), "status=done", &jwt).await;
    assert_eq!(
        (code, invalid["error"].as_str()),
        (StatusCode::NOT_FOUND, Some("issue not found"))
    );

    // Moving into the project maps a status the workflow does not list
    // (blocked, started) to its first started column and enters that step's
    // handler on this preview only.
    let entering: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,number) VALUES ($1,'Entering','member',$2,'blocked',43) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let (code, moved) = get(
        &app,
        ws,
        &entering.to_string(),
        &format!("project_id={project}"),
        &jwt,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{moved}");
    assert_eq!(moved["to_status"], "in_progress");
    assert_eq!(moved["handoff"], true);
    assert_eq!(moved["previous_assignee_type"], Value::Null);
    assert_eq!(moved["previous_assignee_id"], Value::Null);
    assert_eq!(moved["previous_runs"], json!([]));
    let row = db
        .query_one(
            "SELECT project_id,status,assignee_type,assignee_id FROM issue WHERE id=$1",
            &[&entering],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), None);
    assert_eq!(row.get::<_, String>(1), "blocked");
    assert_eq!(row.get::<_, Option<String>>(2), None);
    assert_eq!(row.get::<_, Option<Uuid>>(3), None);
    let row = db
        .query_one("SELECT status,revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "todo");
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
            .await
            .unwrap()
            .get::<_, String>(0),
        "queued"
    );
}
