//! Task-provenance contract for squad leader evaluations against disposable PostgreSQL.
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

const SECRET: &[u8] = b"synthetic-squad-evaluation-test-only";

async fn post(
    app: &axum::Router,
    issue: Uuid,
    ws: Uuid,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/issues/{issue}/squad-evaluated?workspace_id={ws}"
        ))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
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
#[ignore = "requires migrated run-owned PostgreSQL 17 on isolated loopback port"]
async fn squad_evaluation_checks_task_provenance_and_live_leadership() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi100_claim"));
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
    let foreign_ws = Uuid::new_v4();
    let runtime = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let other_agent = Uuid::new_v4();
    let squad = Uuid::new_v4();
    let issue = Uuid::new_v4();
    let other_issue = Uuid::new_v4();
    let task = Uuid::new_v4();
    let foreign_runtime = Uuid::new_v4();
    let foreign_agent = Uuid::new_v4();
    let foreign_task = Uuid::new_v4();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES($1,'Squad test',$2)",
        &[&owner, &format!("squad-{owner}@invalid.example")],
    )
    .await
    .unwrap();
    for workspace in [ws, foreign_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Squad test',$2)",
            &[&workspace, &format!("squad-{workspace}")],
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
    for (rt, workspace) in [(runtime, ws), (foreign_runtime, foreign_ws)] {
        db.execute("INSERT INTO agent_runtime(id,workspace_id,name,runtime_mode,provider,status) VALUES ($1,$2,'Synthetic runtime','cloud','synthetic','online')", &[&rt,&workspace]).await.unwrap();
    }
    for (id, workspace, rt, name) in [
        (agent, ws, runtime, "Synthetic leader"),
        (other_agent, ws, runtime, "Synthetic other"),
        (
            foreign_agent,
            foreign_ws,
            foreign_runtime,
            "Synthetic leader",
        ),
    ] {
        db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,$2,$3,'cloud',$4,$5)", &[&id,&workspace,&name,&owner,&rt]).await.unwrap();
    }
    db.execute("INSERT INTO squad(id,workspace_id,name,leader_id,creator_id) VALUES($1,$2,'Evaluation squad',$3,$4)", &[&squad,&ws,&agent,&owner]).await.unwrap();
    for (id, number) in [(issue, 47), (other_issue, 48)] {
        // The issue is not squad-assigned: the task carries authority.
        db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id,number,assignee_type,assignee_id) VALUES($1,$2,'Not squad assigned','member',$3,$4,'agent',$5)", &[&id,&ws,&owner,&number,&other_agent]).await.unwrap();
    }
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,is_leader_task,squad_id) VALUES ($1,$2,$3,$4,'running',true,$5)", &[&task,&agent,&runtime,&issue,&squad]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,is_leader_task) VALUES ($1,$2,$3,NULL,'running',true)", &[&foreign_task,&foreign_agent,&foreign_runtime]).await.unwrap();
    let raw = format!("mat_squad_{task}");
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&owner]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let ask = |target: Uuid, token: String, outcome: Value| {
        let app = app.clone();
        async move { post(&app, target, ws, &token, outcome).await }
    };
    assert_eq!(
        ask(issue, raw.clone(), json!({"outcome":"invalid"})).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"outcome must be 'action', 'no_action', or 'failed'"})
        )
    );
    let jwt = encode(
        &Header::default(),
        &json!({"sub":owner,"exp":(chrono::Utc::now().timestamp()+3600)}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap();
    assert_eq!(
        ask(issue, jwt, json!({"outcome":"no_action"})).await,
        (StatusCode::BAD_REQUEST, json!({"error":"invalid task id"}))
    );
    let (status, data) = ask(
        issue,
        raw.clone(),
        json!({"outcome":"no_action","reason":"no intervention"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{data}");
    assert_eq!(data["action"], "squad_leader_evaluated");
    assert!(data["id"].as_str().is_some());
    let record = db
        .query_one(
            "SELECT actor_type,actor_id,details FROM activity_log WHERE id=$1",
            &[&Uuid::parse_str(data["id"].as_str().unwrap()).unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(
        record.get::<_, Option<String>>("actor_type"),
        Some("agent".into())
    );
    assert_eq!(record.get::<_, Option<Uuid>>("actor_id"), Some(agent));
    let details: Value = record.get("details");
    assert_eq!(
        details,
        json!({"squad_id":squad,"task_id":task,"outcome":"no_action","reason":"no intervention"})
    );
    assert_eq!(
        ask(other_issue, raw.clone(), json!({"outcome":"action"})).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":format!("task does not belong to issue; record the evaluation on issue {issue} (the issue this task is running on)")})
        )
    );
    db.execute(
        "UPDATE agent_task_queue SET is_leader_task=false WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        ask(issue, raw.clone(), json!({"outcome":"action"})).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"task is not a squad leader task"})
        )
    );
    db.execute(
        "UPDATE agent_task_queue SET is_leader_task=true,squad_id=NULL WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        ask(issue, raw.clone(), json!({"outcome":"action"})).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"leader task has no squad_id"})
        )
    );
    db.execute(
        "UPDATE agent_task_queue SET squad_id=$2 WHERE id=$1",
        &[&task, &squad],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE squad SET leader_id=$2 WHERE id=$1",
        &[&squad, &other_agent],
    )
    .await
    .unwrap();
    assert_eq!(
        ask(issue, raw, json!({"outcome":"action"})).await,
        (
            StatusCode::FORBIDDEN,
            json!({"error":"only the squad leader agent can record evaluations"})
        )
    );
    // Tenant-scoped lookup hides the foreign task before ownership or issue diagnostics.
    assert!(db.query_one("SELECT EXISTS(SELECT 1 FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id WHERE q.id=$1 AND a.workspace_id=$2)", &[&foreign_task,&ws]).await.unwrap().get::<_,bool>(0) == false);
}
