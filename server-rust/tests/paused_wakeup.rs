//! Paused board cues on an isolated, fully migrated PostgreSQL instance.
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

const SECRET: &[u8] = b"synthetic-paused-wakeup";

async fn read(app: &axum::Router, workspace: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut req =
        Request::builder().uri(format!("/api/issue-wakeup-paused?workspace_id={workspace}"));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, value)
}

#[tokio::test]
#[ignore = "requires run-owned multica_zbsi63 disposable database migrated to both ledgers"]
async fn paused_wakeups_filter_scope_status_and_revoked_tokens() {
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
        open_issue,
        closed_issue,
        foreign_issue,
        open_rule,
        closed_rule,
        foreign_rule,
    ]: [Uuid; 12] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'paused-test',$2)",
            &[&user, &format!("{user}@paused.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'paused-test',$2)",
            &[&workspace, &format!("paused-{workspace}")],
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
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES ($1,$2,'paused-agent','cloud',$3)", &[&agent,&ws,&owner]).await.unwrap();
    for (id, workspace, status, number) in [
        (open_issue, ws, "todo", 1),
        (closed_issue, ws, "done", 2),
        (foreign_issue, foreign, "todo", 1),
    ] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'paused-test',$3,'member',$4,$5)", &[&id,&workspace,&status,&owner,&number]).await.unwrap();
    }
    for (id, workspace, issue, reason, when) in [
        (open_rule, ws, open_issue, "loop", "2026-11-01T02:00:00Z"),
        (
            closed_rule,
            ws,
            closed_issue,
            "rate",
            "2026-11-01T03:00:00Z",
        ),
        (
            foreign_rule,
            foreign,
            foreign_issue,
            "max_fires",
            "2026-11-01T04:00:00Z",
        ),
    ] {
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,enabled,paused_reason,updated_at) VALUES ($1,$2,$3,$4,$5,'synthetic','event','once',false,$6,$7::text::timestamptz)", &[&id,&workspace,&issue,&agent,&owner,&reason,&when]).await.unwrap();
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
    for user in [owner, member] {
        assert_eq!(
            read(&app, ws, Some(&jwt(user))).await,
            (
                StatusCode::OK,
                json!([{"issue_id":open_issue,"id":open_rule,"agent_id":agent,"paused_reason":"loop"}])
            )
        );
        assert_eq!(
            read(&app, foreign, Some(&jwt(user))).await.0,
            StatusCode::NOT_FOUND
        );
    }
    let pat = format!("mul_paused_{ws}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'paused-test',$2,'mul_paused')", &[&member,&hash]).await.unwrap();
    assert_eq!(
        read(&app, ws, Some(&pat)).await.1.as_array().unwrap().len(),
        1
    );
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.0, StatusCode::UNAUTHORIZED);
    db.execute(
        "UPDATE issue_wakeup SET enabled=true WHERE id=$1",
        &[&open_rule],
    )
    .await
    .unwrap();
    assert_eq!(
        read(&app, ws, Some(&jwt(owner))).await,
        (StatusCode::OK, json!([]))
    );
    db.execute(
        "UPDATE issue_wakeup SET enabled=false,paused_reason='rate' WHERE id=$1",
        &[&open_rule],
    )
    .await
    .unwrap();
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
