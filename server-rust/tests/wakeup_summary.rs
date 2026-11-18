//! Board wakeup summaries on an isolated, fully migrated PostgreSQL instance.
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

const SECRET: &[u8] = b"synthetic-wakeup-summary";

async fn read(app: &axum::Router, workspace: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!(
        "/api/issue-wakeup-summaries?workspace_id={workspace}"
    ));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn summaries_limit_preview_redact_sources_and_isolate_workspace() {
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
        foreign,
        agent,
        issue,
        closed,
        foreign_issue,
        private_agent,
    ]: [Uuid; 10] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'summary-test',$2)",
            &[&user, &format!("{user}@summary.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'summary-test',$2)",
            &[&workspace, &format!("summary-{workspace}")],
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
        db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,$4,'cloud',$3)", &[&id,&ws,&owner,&format!("summary-{id}")]).await.unwrap();
    }
    for (id, workspace, status, number) in [
        (issue, ws, "todo", 1),
        (closed, ws, "done", 2),
        (foreign_issue, foreign, "todo", 1),
    ] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'summary-test',$3,'member',$4,$5)", &[&id,&workspace,&status,&owner,&number]).await.unwrap();
    }
    let rules: [Uuid; 7] = std::array::from_fn(|_| Uuid::new_v4());
    for (n, id) in rules.iter().enumerate() {
        let (workspace, target, enabled) = if n == 5 {
            (ws, closed, true)
        } else if n == 6 {
            (foreign, foreign_issue, true)
        } else {
            (ws, issue, n != 4)
        };
        let kind = if n == 0 { "at" } else { "event" };
        let filter_agent = (n != 0).then_some(private_agent);
        let filter_type = (n != 0).then_some("agent");
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,enabled,event_types,filter_actor_type,filter_actor_id,condition,created_at) VALUES ($1,$2,$3,$4,$5,'PRIVATE PROMPT',$6,'once',$7,ARRAY['comment.created']::text[],$11,$8,$9::text::jsonb,('2026-11-01T02:00:00Z'::timestamptz + ($10::int * INTERVAL '1 second')))", &[id,&workspace,&target,&agent,&owner,&kind,&enabled,&filter_agent,&Some(r#"{"type":"test"}"#),&(n as i32),&filter_type]).await.unwrap();
    }
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
    assert_eq!(read(&app, ws, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        read(&app, ws, Some("invalid")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        read(&app, ws, Some(&jwt(outsider))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        read(&app, foreign, Some(&jwt(owner))).await.0,
        StatusCode::NOT_FOUND
    );
    let (status, owner_rows) = read(&app, ws, Some(&jwt(owner))).await;
    assert_eq!(status, StatusCode::OK);
    let rows = owner_rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"], json!(rules[0]));
    assert_eq!(rows[0]["active_count"], 4);
    assert_eq!(rows[0]["event_count"], 3);
    assert_eq!(rows[1]["filter_actor_id"], json!(private_agent));
    assert_eq!(rows[0]["condition"], json!({"type":"test"}));
    assert!(
        rows.iter()
            .all(|r| r.get("instruction").is_none() && r.get("created_by").is_none())
    );
    let (_, member_rows) = read(&app, ws, Some(&jwt(member))).await;
    let rows = member_rows.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1]["filter_actor_id"], Value::Null);
    assert_eq!(rows[1]["filter_actor_name"], "");
    assert_eq!(rows[0]["active_count"], 4);
    let pat = format!("mul_summary_{ws}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'summary-test',$2,'mul_summary')", &[&member,&hash]).await.unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.1, member_rows);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.0, StatusCode::UNAUTHORIZED);
    db.execute(
        "UPDATE issue_wakeup SET enabled=false WHERE issue_id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, ws, Some(&jwt(owner))).await.1, json!([]));
    db.execute(
        "ALTER TABLE issue_wakeup RENAME TO issue_wakeup_unavailable",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, Some(&jwt(owner))).await,
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
