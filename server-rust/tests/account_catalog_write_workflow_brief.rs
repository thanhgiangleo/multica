//! Synthetic HTTP/DB checks for the read-only workflow editor preview.
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

const SECRET: &str = "zbsi65-workflow-brief-synthetic-secret";
fn jwt(user: Uuid, ttl: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+ttl}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn request(
    app: &axum::Router,
    ws: Uuid,
    workflow: &str,
    token: &str,
    body: &str,
    cookie: bool,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/issue-workflows/{workflow}/brief/?workspace_id={ws}"
        ))
        .header("content-type", "application/json");
    if cookie {
        builder = builder.header("cookie", format!("multica_auth={token}"));
    } else {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn editor_preview_membership_privacy_credentials_and_no_writes() {
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
    for name in ["Editor", "Sam\nReviewer", "Private outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("brief-{}-{tag}@invalid.example", users.len()),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Brief',$1,'BRF') RETURNING id",
            &[&format!("brief-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Private',$1) RETURNING id",
            &[&format!("private-brief-{tag}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, workspace) in [(users[0], ws), (users[1], ws), (users[2], foreign)] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
            &[&workspace, &user],
        )
        .await
        .unwrap();
    }
    let steps = json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"review","handler":{"type":"member","id":users[1]},"instructions":"Old instruction"},{"status_key":"blocked"},{"status_key":"done"},{"status_key":"cancelled"}]);
    let flow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Flow','todo',$2) RETURNING id", &[&ws,&steps]).await.unwrap().get(0);
    let private_flow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Private flow','todo',$2) RETURNING id", &[&foreign,&steps]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,icon,position) VALUES($1,'review','Review checks','started','#123456','',0),($1,'todo','English seeded todo','unstarted','#123456','',0)", &[&ws]).await.unwrap();
    let project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Campaign',$2) RETURNING id", &[&ws,&flow]).await.unwrap().get(0);
    let private_project:Uuid=db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Private project',$2) RETURNING id", &[&foreign,&private_flow]).await.unwrap().get(0);
    let before: Value = db
        .query_one(
            "SELECT to_jsonb(w) FROM issue_workflow w WHERE id=$1",
            &[&flow],
        )
        .await
        .unwrap()
        .get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec())
            .with_issue_event_sink(Arc::new(sink::StrictSink)),
    );
    let token = jwt(users[0], 3600);
    let draft = json!({"status_key":" review ","handler":{"type":"member","id":users[1]},"instructions":"  Verify\r\nchecks\u{001b}","back_status_key":"todo","next_status_key":"done","project_id":project});
    let (status, result) = request(
        &app,
        ws,
        &flow.to_string(),
        &token,
        &draft.to_string(),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let brief = result["brief"].as_str().unwrap();
    assert!(brief.starts_with("This project's workflow — Flow (Campaign)\nYou are handling the \"Review checks\" step of BRF-123.\n"),"{brief}");
    assert!(
        brief.contains("\nStep instructions:\n> Verify\n> checks\n"),
        "{brief}"
    );
    assert!(brief.contains("- `review` Review checks — hands off to Sam Reviewer   ← current\n"));
    assert!(brief.contains("Step done      → multica issue status BRF-123 done\n"));
    assert!(brief.contains("Needs changes  → multica issue status BRF-123 todo\n"));
    assert!(!brief.contains("Old instruction"));
    assert!(!brief.contains("English seeded todo"));
    let odd_flow = flow.to_string().replace('-', "x");
    let compatible_draft = json!({"status_key":" review ","handler":{"type":"member","id":users[1].simple().to_string()},"project_id":project.simple().to_string()});
    let (status, result) = request(
        &app,
        ws,
        &odd_flow,
        &token,
        &compatible_draft.to_string(),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(
        result["brief"]
            .as_str()
            .unwrap()
            .contains("Flow (Campaign)")
    );
    assert!(result["brief"].as_str().unwrap().contains("Sam Reviewer"));
    assert_eq!(
        request(
            &app,
            ws,
            &format!("{{{flow}}}"),
            &token,
            &draft.to_string(),
            false
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for project_id in [private_project.to_string(), "invalid-project".into()] {
        let body = json!({"status_key":"todo","project_id":project_id});
        let (status, result) = request(
            &app,
            ws,
            &flow.to_string(),
            &token,
            &body.to_string(),
            false,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        let brief = result["brief"].as_str().unwrap();
        assert!(!brief.contains("Private project"));
        assert!(brief.contains("Starting work  → multica issue status BRF-123 in_progress\n"));
        assert!(brief.contains(
            "Step done      → multica issue status BRF-123 review — hands off to Sam Reviewer\n"
        ));
    }
    for (workspace, workflow, auth, body, expected) in [
        (
            ws,
            flow.to_string(),
            jwt(users[0], -3600),
            draft.to_string(),
            StatusCode::UNAUTHORIZED,
        ),
        (
            ws,
            flow.to_string(),
            jwt(users[2], 3600),
            draft.to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            ws,
            private_flow.to_string(),
            token.clone(),
            draft.to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            foreign,
            flow.to_string(),
            jwt(users[2], 3600),
            draft.to_string(),
            StatusCode::NOT_FOUND,
        ),
        (
            ws,
            "invalid-id".into(),
            token.clone(),
            draft.to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            ws,
            flow.to_string(),
            token.clone(),
            "{".into(),
            StatusCode::BAD_REQUEST,
        ),
        (
            ws,
            flow.to_string(),
            token.clone(),
            json!({"status_key":"absent"}).to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            ws,
            flow.to_string(),
            token.clone(),
            json!({"status_key":"review","handler":{"type":"member","id":users[2]}}).to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            ws,
            flow.to_string(),
            token.clone(),
            json!({"status_key":"review","handler":{"type":"member","id":"bad-id"}}).to_string(),
            StatusCode::BAD_REQUEST,
        ),
        (
            ws,
            flow.to_string(),
            token.clone(),
            json!({"status_key":"review","handler":{"type":"agent","id":Uuid::new_v4()}})
                .to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let (status, result) = request(&app, workspace, &workflow, &auth, &body, false).await;
        assert_eq!(status, expected, "{result}");
        assert!(!result.to_string().contains("Private outsider"));
    }
    assert_eq!(
        request(
            &app,
            ws,
            &flow.to_string(),
            &token,
            &draft.to_string(),
            true
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let pat = format!("mcp_brief_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at,revoked) VALUES($1,'brief',$2,'mcp_brief',now()+interval '1 hour',true)", &[&users[0],&hash]).await.unwrap();
    assert_eq!(
        request(&app, ws, &flow.to_string(), &pat, &draft.to_string(), false)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let expired_pat = format!("mcp_brief_{}", Uuid::new_v4().simple());
    let expired_hash = hex::encode(Sha256::digest(expired_pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at) VALUES($1,'expired brief',$2,'mcp_brief',now()-interval '1 hour')", &[&users[0],&expired_hash]).await.unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            &flow.to_string(),
            &expired_pat,
            &draft.to_string(),
            false
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic brief agent','cloud',$2) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic brief runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_brief_{}", Uuid::new_v4().simple());
    let task_hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&task_hash,&task,&agent,&ws,&users[0]]).await.unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            &flow.to_string(),
            &task_token,
            &draft.to_string(),
            false
        )
        .await
        .0,
        StatusCode::OK
    );
    db.execute("DELETE FROM task_token WHERE token_hash=$1", &[&task_hash])
        .await
        .unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            &flow.to_string(),
            &task_token,
            &draft.to_string(),
            false
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let draft = draft.to_string();
    let flow_id = flow.to_string();
    let (left, right) = tokio::join!(
        request(&app, ws, &flow_id, &token, &draft, false),
        request(&app, ws, &flow_id, &token, &draft, false)
    );
    assert_eq!(left, right);
    assert_eq!(left.0, StatusCode::OK);
    let after: Value = db
        .query_one(
            "SELECT to_jsonb(w) FROM issue_workflow w WHERE id=$1",
            &[&flow],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        before, after,
        "preview must not change steps, timestamps or owner"
    );
}
