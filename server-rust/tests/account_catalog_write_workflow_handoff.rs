//! HTTP/DB contract for workflow handoff and per-project copy on a disposable database.
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

#[path = "workflow_support/sink.rs"]
mod sink;

const SECRET: &str = "zbsi65-workflow-handoff-synthetic-secret";
fn jwt(user: Uuid, ttl: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+ttl}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    ws: Uuid,
    workflow: &str,
    status: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/issue-workflows/{workflow}/handoffs/{status}/?workspace_id={ws}"
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
async fn call_raw(
    app: &axum::Router,
    ws: Uuid,
    workflow: Uuid,
    token: &str,
    body: String,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/issue-workflows/{workflow}/handoffs/in_progress/?workspace_id={ws}"
        ))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
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
async fn handoff_edit_copy_permissions_and_rollbacks() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let tag = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for role in ["creator", "lead", "admin", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &role,
                    &format!("workflow-handoff-{role}-{tag}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Handoff', $1) RETURNING id",
            &[&format!("handoff-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign handoff', $1) RETURNING id",
            &[&format!("foreign-handoff-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role, workspace) in [
        (users[0], "member", ws),
        (users[1], "member", ws),
        (users[2], "admin", ws),
        (users[3], "member", ws),
        (users[4], "owner", foreign),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let original = json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"done"},{"status_key":"cancelled"}]);
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,owner_id,steps) VALUES($1,'Flow','todo',$2,$3) RETURNING id",
        &[&ws,&users[0],&original]).await.unwrap().get(0);
    let default:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,is_default,steps) VALUES($1,'Default','todo',true,$2) RETURNING id",
        &[&ws,&original]).await.unwrap().get(0);
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id,lead_type,lead_id) VALUES($1,'Campaign',$2,'member',$3) RETURNING id",
        &[&ws,&workflow,&users[1]]).await.unwrap().get(0);
    let another:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Other',$2) RETURNING id",
        &[&ws,&workflow]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let body = json!({"handler":{"type":"member","id":users[3]},"instructions":"  Run checks  ","next_status_key":"done"});
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &jwt(users[0], -3600),
            body.clone()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &jwt(users[3], 3600),
            body.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &jwt(users[4], 3600),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            ws,
            "bad-uuid",
            "in_progress",
            &jwt(users[0], 3600),
            body.clone()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for invalid in [
        json!({"handler":{"type":"member","id":users[4]}}),
        json!({"handler":{"type":"squad","id":Uuid::new_v4()}}),
        json!({"handler":{"type":"member","id":"not-a-uuid"}}),
        json!({"handler":{"type":"project_lead"},"back_status_key":"in_progress"}),
    ] {
        let (status, detail) = call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &jwt(users[0], 3600),
            invalid,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{detail}");
        assert_eq!(detail["code"], "invalid_handoff");
    }
    let stored: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, original);
    let compact_workflow = workflow.simple().to_string();
    let odd_member = users[3].to_string().replace('-', "x");
    let (status, compatible) = call(
        &app,
        ws,
        &compact_workflow,
        "in_progress",
        &jwt(users[0], 3600),
        json!({"handler":{"type":"member","id":odd_member},"instructions":"UUID boundary"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{compatible}");
    let stored: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored[1]["handler"]["id"], odd_member);
    let (status, edited) = call(
        &app,
        ws,
        &workflow.to_string(),
        "in_progress",
        &jwt(users[0], 3600),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["steps"][1]["handler"]["id"], users[3].to_string());
    assert_eq!(edited["steps"][1]["instructions"], "Run checks");
    let stored: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored[1]["handler"]["id"], users[3].to_string());
    // Repeated embedded handler objects merge, scalar null retains; no Content-Type
    // or Axum's 2 MiB body cap on this Go-compatible endpoint.
    let raw = format!(
        "{{\"handler\":{{\"TYPE\":\"member\",\"ID\":\"{}\"}},\"HANDLER\":{{\"type\":null}},\"instructions\":\"  Run checks  \",\"INSTRUCTIONS\":null,\"next_status_key\":\"done\",\"unused\":\"{}\"}} trailing",
        users[3],
        "x".repeat(2_100_000)
    );
    let (status, edited) = call_raw(&app, ws, workflow, &jwt(users[0], 3600), raw).await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["steps"][1]["handler"]["id"], users[3].to_string());
    assert_eq!(edited["steps"][1]["instructions"], "Run checks");
    for raw in [
        "{\"handler\":{\"type\":42}}",
        "{\"status_key\":false}",
        "{\"handler\":[]}",
    ] {
        assert_eq!(
            call_raw(&app, ws, workflow, &jwt(users[0], 3600), raw.to_owned())
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let stored: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored[1]["handler"]["id"], users[3].to_string());
    let agent: Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic handoff agent','cloud',$2) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic handoff runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_handoff_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&users[0]]).await.unwrap();
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &task_token,
            body.clone()
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&hash])
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &task_token,
            body.clone()
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let cookie_request = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/issue-workflows/{workflow}/handoffs/in_progress/?workspace_id={ws}"
        ))
        .header("cookie", format!("multica_auth={}", jwt(users[0], 3600)))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie_request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    // A project lead may copy only a workflow they also own or an admin granted them access to.
    assert_eq!(
        call(
            &app,
            ws,
            &workflow.to_string(),
            "in_progress",
            &jwt(users[1], 3600),
            json!({"copy_for_project_id":project})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let denied = json!({"copy_for_project_id":another,"handler":{"type":"project_lead"}});
    let (status, detail) = call(
        &app,
        ws,
        &workflow.to_string(),
        "in_progress",
        &jwt(users[0], 3600),
        denied,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{detail}");
    let (status, detail) = call(
        &app,
        ws,
        &default.to_string(),
        "in_progress",
        &jwt(users[2], 3600),
        json!({"copy_for_project_id":project}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{detail}");
    let copy = json!({"copy_for_project_id":project.simple().to_string(),"handler":{"type":"project_lead"},"instructions":"  Move on  "});
    let (status, created) = call(
        &app,
        ws,
        &workflow.to_string().replace('-', "x"),
        "in_progress",
        &jwt(users[2], 3600),
        copy,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    assert_ne!(id, workflow);
    assert_eq!(created["name"], "Flow (Campaign)");
    assert_eq!(created["owner_id"], users[2].to_string());
    assert_eq!(created["steps"][1]["instructions"], "Move on");
    assert_eq!(created["project_ids"], json!([project]));
    let copy_row: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(copy_row[1]["handler"]["type"], "project_lead");
    let project_workflow: Option<Uuid> = db
        .query_one("SELECT workflow_id FROM project WHERE id=$1", &[&project])
        .await
        .unwrap()
        .get(0);
    assert_eq!(project_workflow, Some(id));
    let original_row: Value = db
        .query_one("SELECT steps FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap()
        .get(0);
    assert_eq!(original_row, stored);
    let (status, detail) = call(
        &app,
        ws,
        &workflow.to_string(),
        "in_progress",
        &jwt(users[2], 3600),
        json!({"copy_for_project_id":project}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{detail}");
    assert_eq!(detail["code"], "invalid_handoff");
    let copy_target:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Campaign',$2) RETURNING id", &[&ws,&workflow]).await.unwrap().get(0);
    let copy_request = json!({"copy_for_project_id":copy_target,"handler":{"type":"none"}});
    let workflow_path = workflow.to_string();
    let admin_token = jwt(users[2], 3600);
    let (first, second) = tokio::join!(
        call(
            &app,
            ws,
            &workflow_path,
            "in_progress",
            &admin_token,
            copy_request.clone()
        ),
        call(
            &app,
            ws,
            &workflow_path,
            "in_progress",
            &admin_token,
            copy_request
        )
    );
    let created = if first.0 == StatusCode::CREATED {
        &first
    } else {
        &second
    };
    let conflict = if first.0 == StatusCode::CONFLICT {
        &first
    } else {
        &second
    };
    assert_eq!(created.0, StatusCode::CREATED, "{first:?} {second:?}");
    assert_eq!(conflict.0, StatusCode::CONFLICT, "{first:?} {second:?}");
    assert_eq!(created.1["name"], "Flow (Campaign 2)");
    let copies:i64=db.query_one("SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND name LIKE 'Flow (Campaign%'", &[&ws]).await.unwrap().get(0);
    assert_eq!(copies, 2);
    let default_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Default project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let (status, default_edit) = call(
        &app,
        ws,
        &default.to_string(),
        "in_progress",
        &admin_token,
        json!({"handler":{"type":"project_lead"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{default_edit}");
    assert_eq!(default_edit["is_default"], true);
    assert!(
        default_edit["project_ids"]
            .as_array()
            .unwrap()
            .contains(&json!(default_project))
    );
    // A DB failure after inserting a copy must roll back both the new row and project switch.
    let failing:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Rollback target',$2) RETURNING id", &[&ws,&workflow]).await.unwrap().get(0);
    db.batch_execute(&format!("CREATE FUNCTION zbsi65_reject_project_switch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = '{failing}'::uuid AND NEW.workflow_id IS DISTINCT FROM OLD.workflow_id THEN RAISE EXCEPTION 'synthetic update fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi65_reject_switch BEFORE UPDATE ON project FOR EACH ROW EXECUTE FUNCTION zbsi65_reject_project_switch()"))
        .await.unwrap();
    let (status, response) = call(
        &app,
        ws,
        &workflow.to_string(),
        "in_progress",
        &admin_token,
        json!({"copy_for_project_id":failing,"handler":{"type":"none"}}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{response}");
    let current: Option<Uuid> = db
        .query_one("SELECT workflow_id FROM project WHERE id=$1", &[&failing])
        .await
        .unwrap()
        .get(0);
    assert_eq!(current, Some(workflow));
    let orphan:i64=db.query_one("SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND name='Flow (Rollback target)'", &[&ws]).await.unwrap().get(0);
    assert_eq!(orphan, 0);
    db.batch_execute("DROP TRIGGER zbsi65_reject_switch ON project; DROP FUNCTION zbsi65_reject_project_switch()").await.unwrap();
}
