//! Issue wakeup runs: frozen SQL and tenant boundary on a disposable database.
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

const SECRET: &[u8] = b"synthetic-issue-wakeup";

async fn read(
    app: &axum::Router,
    workspace: Uuid,
    issue: &str,
    wakeup: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!(
        "/api/issues/{issue}/wakeups/{wakeup}/runs?workspace_id={workspace}"
    ));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn run_history_is_scoped_ordered_bounded_and_read_only() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi63"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(3)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        member,
        outsider,
        ws,
        other_ws,
        agent,
        issue,
        other_issue,
        rule,
        other_rule,
    ]: [Uuid; 10] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            r#"INSERT INTO "user"(id,name,email) VALUES ($1,'run-test',$2)"#,
            &[&user, &format!("{user}@runs.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'run-test',$2,'RUN')",
            &[&workspace, &format!("run-{workspace}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,'run-agent','cloud',$3)", &[&agent,&ws,&owner]).await.unwrap();
    for (id, workspace, number) in [(issue, ws, 1), (other_issue, other_ws, 2)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'run','todo','member',$3,$4)", &[&id,&workspace,&owner,&number]).await.unwrap();
    }
    for (id, issue_id) in [(rule, issue), (other_rule, other_issue)] {
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode) VALUES ($1,$2,$3,$4,$5,'run','event','once')", &[&id,&if issue_id==issue {ws} else {other_ws},&issue_id,&agent,&owner]).await.unwrap();
    }
    let mut expected = Vec::new();
    for index in 0..12 {
        let id = Uuid::new_v4();
        db.execute("INSERT INTO agent_task_queue(id,agent_id,issue_id,status,context,created_at,completed_at) VALUES ($1,$2,$3,'completed',$4,now()+$5::int * interval '1 second',now())", &[&id,&agent,&issue,&json!({"wakeup_id":rule,"wakeup_checkin":{"note":format!("note-{index}")},"wakeup_evidence":{"facts":[{"event_type":"comment.created"},{"event_type":"wakeup.manual"},{"event_type":"comment.created"}]}}),&index]).await.unwrap();
        expected.push(id);
    }
    let foreign = Uuid::new_v4();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,issue_id,status,context,completed_at) VALUES ($1,$2,$3,'cancelled',$4,now())", &[&foreign,&agent,&issue,&json!({"wakeup_id":other_rule})]).await.unwrap();
    db.execute("INSERT INTO comment(issue_id,author_type,author_id,content,source_task_id,workspace_id) VALUES ($1,'member',$2,'hello',$3,$4)", &[&issue,&owner,&expected[11],&ws]).await.unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.to_vec()),
    );
    let jwt = |user| {
        encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
    };
    let fetch = |user: Uuid| jwt(user);
    assert_eq!(
        read(&app, ws, &issue.to_string(), &rule.to_string(), None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some(&fetch(outsider))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(
            &app,
            ws,
            &other_issue.to_string(),
            &other_rule.to_string(),
            Some(&fetch(owner))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, ws, &issue.to_string(), "wrong", Some(&fetch(owner))).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid wakeup id"})
        )
    );
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &other_rule.to_string(),
            Some(&fetch(owner))
        )
        .await,
        (StatusCode::NOT_FOUND, json!({"error":"wakeup not found"}))
    );
    let (status, body) = read(&app, ws, "RUN-1", &rule.to_string(), Some(&fetch(member))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0]["id"], json!(expected[11]));
    assert_eq!(rows[0]["checkin_note"], "note-11");
    assert_eq!(rows[0]["commented"], true);
    assert_eq!(
        rows[0]["triggers"],
        json!(["comment.created", "wakeup.manual"])
    );
    assert_eq!(rows[9]["id"], json!(expected[2]));
    assert_eq!(rows[1]["commented"], false);
    assert_eq!(rows[0]["started_at"], Value::Null);
    assert!(rows[0]["created_at"].is_string());
    let hash = hex::encode(Sha256::digest(b"mul_runtest"));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'run-test',$2,'mul_runtest')", &[&member,&hash]).await.unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some("mul_runtest")
        )
        .await
        .1,
        body
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some("mul_runtest")
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some(&fetch(member))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let before = db
        .query_one(
            "SELECT revision,fire_count FROM issue_wakeup WHERE id=$1",
            &[&rule],
        )
        .await
        .unwrap();
    assert_eq!(before.get::<_, i64>("revision"), 1);
    assert_eq!(before.get::<_, i32>("fire_count"), 0);
    let task_count: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(task_count, 13);
    db.execute(
        "ALTER TABLE agent_task_queue RENAME TO agent_task_queue_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some(&fetch(owner))
        )
        .await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"could not save wakeup"})
        )
    );
    db.execute(
        "ALTER TABLE agent_task_queue_unavailable RENAME TO agent_task_queue",
        &[],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue_wakeup WHERE id=$1", &[&rule])
        .await
        .unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some(&fetch(owner))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "ALTER TABLE issue_wakeup RENAME TO issue_wakeup_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(
            &app,
            ws,
            &issue.to_string(),
            &rule.to_string(),
            Some(&fetch(owner))
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "ALTER TABLE issue_wakeup_unavailable RENAME TO issue_wakeup",
        &[],
    )
    .await
    .unwrap();
}
