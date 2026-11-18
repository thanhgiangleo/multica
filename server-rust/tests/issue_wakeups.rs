//! Issue wakeup list: frozen SQL redactions and tenant boundary on a disposable database.
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
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!(
        "/api/issues/{issue}/wakeups?workspace_id={workspace}"
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
async fn issue_wakeups_redact_agent_filters_and_isolate_workspaces() {
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
        private_agent,
        issue,
        other_issue,
        rule,
        missing,
    ]: [Uuid; 11] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            r#"INSERT INTO "user"(id,name,email) VALUES ($1,'issue-wakeup-test',$2)"#,
            &[&user, &format!("{user}@wakeup.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, other_ws] {
        db.execute(
            "INSERT INTO workspace(id,name,slug,issue_prefix) VALUES ($1,'test',$2,'WAKE')",
            &[&workspace, &format!("wakeup-{workspace}")],
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
    for id in [agent, private_agent] {
        db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,$4,'cloud',$3)", &[&id,&ws,&owner,&format!("agent-{id}")]).await.unwrap();
    }
    for (id, workspace) in [(issue, ws), (other_issue, other_ws)] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'test','todo','member',$3,1)", &[&id,&workspace,&owner]).await.unwrap();
    }
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,condition,event_types,filter_actor_type,filter_actor_id,filter_agent_id) VALUES ($1,$2,$3,$4,$5,'SECRET INSTRUCTION','event','once',$6::text::jsonb,ARRAY['comment.created']::text[],'agent',$7,$7)", &[&rule,&ws,&issue,&agent,&owner,&r#"{"type":"test"}"#,&private_agent]).await.unwrap();
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
    assert_eq!(
        read(&app, ws, &issue.to_string(), None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&jwt(outsider)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, other_ws, &other_issue.to_string(), Some(&jwt(owner)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, ws, &missing.to_string(), Some(&jwt(owner)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, ws, &other_issue.to_string(), Some(&jwt(owner)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, body) = read(&app, ws, "WAKE-1", Some(&jwt(owner))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(rule));
    assert_eq!(rows[0]["condition"], json!({"type":"test"}));
    assert_eq!(rows[0]["instruction"], "SECRET INSTRUCTION");
    assert_eq!(rows[0]["filter_actor_id"], json!(private_agent));
    assert_eq!(rows[0]["filter_agent_id"], json!(private_agent));
    assert_eq!(rows[0]["created_by_agent"], false);
    let (_, member_rows) = read(&app, ws, &issue.to_string(), Some(&jwt(member))).await;
    assert_eq!(member_rows[0]["filter_actor_id"], Value::Null);
    assert_eq!(member_rows[0]["filter_agent_id"], Value::Null);
    assert_eq!(member_rows[0]["filter_actor_name"], "");
    assert_eq!(member_rows[0]["filter_agent_name"], Value::Null);
    let source = db
        .query_one(
            "SELECT revision,fire_count FROM issue_wakeup WHERE id=$1",
            &[&rule],
        )
        .await
        .unwrap();
    assert_eq!(source.get::<_, i64>("revision"), 1);
    assert_eq!(source.get::<_, i32>("fire_count"), 0);
    let pat = format!("mul_issuewakeup_{rule}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'test',$2,'mul_issuewakeup')", &[&member,&hash]).await.unwrap();
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&pat)).await.1,
        member_rows
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&pat)).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&jwt(member)))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &member],
    )
    .await
    .unwrap();
    db.execute("DELETE FROM issue_wakeup WHERE id=$1", &[&rule])
        .await
        .unwrap();
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&jwt(owner))).await,
        (StatusCode::OK, json!([]))
    );
    db.execute(
        "ALTER TABLE issue_wakeup RENAME TO issue_wakeup_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, &issue.to_string(), Some(&jwt(owner))).await,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"could not save wakeup"})
        )
    );
    db.execute(
        "ALTER TABLE issue_wakeup_unavailable RENAME TO issue_wakeup",
        &[],
    )
    .await
    .unwrap();
}
