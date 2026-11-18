//! Project workflow switch without issue remapping on a disposable PostgreSQL database.
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

const SECRET: &str = "zbsi65-project-workflow-synthetic-secret";
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
    id: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/projects/{id}/workflow/?workspace_id={ws}"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn call_raw(
    app: &axum::Router,
    ws: Uuid,
    id: &str,
    token: &str,
    body: String,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/projects/{id}/workflow/?workspace_id={ws}"))
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body))
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
async fn workflow_switch_dry_run_auth_and_atomicity() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
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
    for name in ["owner", "lead", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("project-workflow-{name}-{tag}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Switch',$1) RETURNING id",
            &[&format!("switch-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other switch',$1) RETURNING id",
            &[&format!("foreign-switch-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role, workspace) in [
        (users[0], "owner", ws),
        (users[1], "member", ws),
        (users[2], "member", ws),
        (users[3], "owner", foreign),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let steps = json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"done"},{"status_key":"cancelled"}]);
    let narrow = json!([{"status_key":"todo"},{"status_key":"done"},{"status_key":"cancelled"}]);
    let flow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Flow','todo',$2) RETURNING id",&[&ws,&steps]).await.unwrap().get(0);
    let narrow_flow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Narrow','todo',$2) RETURNING id",&[&ws,&narrow]).await.unwrap().get(0);
    let other_flow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Elsewhere','todo',$2) RETURNING id",&[&foreign,&steps]).await.unwrap().get(0);
    let default:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,is_default,steps) VALUES($1,'Default','todo',true,$2) RETURNING id",&[&ws,&steps]).await.unwrap().get(0);
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id,lead_type,lead_id) VALUES($1,'Campaign',$2,'member',$3) RETURNING id",&[&ws,&flow,&users[1]]).await.unwrap().get(0);
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number) VALUES($1,'Run','member',$2,$3,'in_progress',1) RETURNING id",&[&ws,&users[0],&project]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let owner = jwt(users[0], 3600);
    let lead = jwt(users[1], 3600);
    let path = project.to_string();
    let switch = json!({"workflow_id":narrow_flow});
    for (who, expected) in [
        (&jwt(users[0], -3600), StatusCode::UNAUTHORIZED),
        (&jwt(users[2], 3600), StatusCode::FORBIDDEN),
        (&jwt(users[3], 3600), StatusCode::NOT_FOUND),
    ] {
        assert_eq!(call(&app, ws, &path, who, switch.clone()).await.0, expected);
    }
    assert_eq!(
        call(&app, ws, "bad-uuid", &owner, switch.clone()).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, foreign, &path, &jwt(users[3], 3600), switch.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for body in [
        json!({"workflow_id":"bad-uuid"}),
        json!({"workflow_id":other_flow}),
        json!({"workflow_id":true}),
    ] {
        assert_eq!(
            call(&app, ws, &path, &owner, body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    // Go decodes the first JSON value without Content-Type, merging repeated map keys.
    let raw = format!(
        "{{\"WORKFLOW_ID\":\"{narrow_flow}\",\"dry_run\":true,\"DRY_RUN\":null,\"status_mapping\":{{\"todo\":\"done\"}},\"STATUS_MAPPING\":{{\"blocked\":null}},\"ignored\":\"{}\"}} trailing",
        "x".repeat(2_100_000)
    );
    let (status, result) = call_raw(&app, ws, &path, &lead, raw).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["plan"]["affected_issues"], 1);
    assert_eq!(
        db.query_one("SELECT workflow_id FROM project WHERE id=$1", &[&project])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(flow)
    );
    for raw in [
        "{\"dry_run\":3}",
        "{\"status_mapping\":{\"todo\":false}}",
        "{\"workflow_id\":{}",
    ] {
        assert_eq!(
            call_raw(&app, ws, &path, &lead, raw.to_owned()).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let preview = json!({"workflow_id":narrow_flow,"dry_run":true});
    let (status, result) = call(&app, ws, &path, &lead, preview).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["plan"]["required"][0]["status_key"], "in_progress");
    assert_eq!(
        result["plan"]["required"][0]["suggested_status_key"],
        "todo"
    );
    assert_eq!(result["plan"]["total_issues"], 1);
    assert_eq!(result["plan"]["affected_issues"], 1);
    for body in [
        json!({"workflow_id":narrow_flow,"dry_run":null,"status_mapping":null}),
        json!({"workflow_id":narrow_flow,"status_mapping":{"in_progress":null}}),
    ] {
        let (status, result) = call(&app, ws, &path, &lead, body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{result}");
        assert_eq!(result["code"], "workflow_status_mapping_required");
    }
    for body in [
        json!({"dry_run":0}),
        json!({"status_mapping":{"in_progress":false}}),
    ] {
        assert_eq!(
            call(&app, ws, &path, &lead, body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let (status, result) = call(&app, ws, &path, &lead, switch.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    assert_eq!(result["code"], "workflow_status_mapping_required");
    let (status, result) = call(
        &app,
        ws,
        &path,
        &lead,
        json!({"workflow_id":narrow_flow,"status_mapping":{"in_progress":"bogus"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    let (status, result) = call(
        &app,
        ws,
        &path,
        &lead,
        json!({"workflow_id":narrow_flow,"status_mapping":{"in_progress":"done"}}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{result}");
    let row = db
        .query_one("SELECT workflow_id FROM project WHERE id=$1", &[&project])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), Some(flow));
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "in_progress"
    );
    let issue_before: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let (status, result) = call(&app, ws, &path, &lead, json!({"workflow_id":flow})).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["project"]["workflow_id"], flow.to_string());
    assert_eq!(result["updated_issues"], 0);
    assert_eq!(result["plan"]["unchanged"][0]["issue_count"], 1);
    let (status, result) = call(&app, ws, &path, &owner, json!({"workflow_id":default})).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["project"]["workflow_id"], Value::Null);
    let (status, result) = call(&app, ws, &path, &lead, json!({"workflow_id":flow})).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["project"]["workflow_id"], flow.to_string());
    assert_eq!(
        db.query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, Value>(0),
        issue_before
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic','cloud',$2) RETURNING id",&[&ws,&users[0]]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic','cloud','synthetic','online') RETURNING id",&[&ws]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id",&[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_workflow_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')",&[&hash,&task,&agent,&ws,&users[0]]).await.unwrap();
    assert_eq!(
        call(
            &app,
            ws,
            &path,
            &token,
            json!({"workflow_id":narrow_flow,"dry_run":true})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&hash])
        .await
        .unwrap();
    assert_eq!(
        call(&app, ws, &path, &token, json!({"workflow_id":narrow_flow}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/projects/{project}/workflow/?workspace_id={ws}"
        ))
        .header("cookie", format!("multica_auth={owner}"))
        .header("content-type", "application/json")
        .body(Body::from(switch.to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (first, second) = tokio::join!(
        call(&app, ws, &path, &owner, json!({"workflow_id":default})),
        call(&app, ws, &path, &lead, json!({"workflow_id":flow}))
    );
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(second.0, StatusCode::OK, "{second:?}");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND is_system",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        7
    );
    // Default keeps archived statuses already on issues but cannot map into them.
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,archived_at) VALUES($1,'old','Old','started','#334455',now())", &[&ws]).await.unwrap();
    db.execute("UPDATE issue SET status='old' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    let (status, result) = call(
        &app,
        ws,
        &path,
        &owner,
        json!({"workflow_id":null,"dry_run":true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["plan"]["unchanged"][0]["status_key"], "old");
    let (status, result) = call(&app, ws, &path, &owner, json!({"workflow_id":null})).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["project"]["workflow_id"], Value::Null);
    let (status, result) = call(
        &app,
        ws,
        &path,
        &owner,
        json!({"workflow_id":narrow_flow,"status_mapping":{"old":"old"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    assert_eq!(result["code"], "workflow_status_mapping_required");
    assert_eq!(
        db.query_one("SELECT workflow_id FROM project WHERE id=$1", &[&project])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        None
    );
}
