//! Workspace system defaults on an isolated, fully migrated PostgreSQL instance.
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

const SECRET: &[u8] = b"synthetic-system-wakeup";
const BUILTIN: &str = "Sub-issues of this issue have closed; the trigger facts list each stage and which one is next. If a later stage is waiting, check that its dependencies are met, then move its sub-issues out of backlog so they start. If a sub-issue in the closed stages was cancelled rather than finished, decide whether its work is still needed before advancing. When every sub-issue is closed, bring their results together on this issue and move it forward, or mark it ready for review when nothing remains.";

async fn read(app: &axum::Router, ws: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().uri(format!("/api/system-wakeups?workspace_id={ws}"));
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
async fn defaults_count_open_customizations_without_cross_workspace_leaks() {
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
        open,
        closed,
        other,
        rule1,
        rule2,
        rule3,
    ]: [Uuid; 11] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES ($1,'synthetic',$2)",
            &[&user, &format!("{user}@system.invalid")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES ($1,'synthetic',$2)",
            &[&workspace, &format!("system-{workspace}")],
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
    for (issue, workspace, status, number) in [
        (open, ws, "todo", 1),
        (closed, ws, "done", 2),
        (other, foreign, "todo", 1),
    ] {
        db.execute("INSERT INTO issue(id,workspace_id,title,status,creator_type,creator_id,number) VALUES ($1,$2,'synthetic',$3,'member',$4,$5)", &[&issue,&workspace,&status,&owner,&number]).await.unwrap();
    }
    for (rule, workspace, issue) in [
        (rule1, ws, open),
        (rule2, ws, closed),
        (rule3, foreign, other),
    ] {
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,system_rule,instruction,kind,mode,customized_at) VALUES ($1,$2,$3,'child_done','','event','continuous',now())", &[&rule,&workspace,&issue]).await.unwrap();
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
    let expected = json!([{"rule":"child_done","enabled":true,"instruction":"","builtin_instruction":BUILTIN,"customized":1}]);
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
            (StatusCode::OK, expected.clone())
        );
        assert_eq!(
            read(&app, foreign, Some(&jwt(user))).await.0,
            StatusCode::NOT_FOUND
        );
    }
    let pat = format!("mul_system_{ws}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_system')", &[&member,&hash]).await.unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.1, expected);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(read(&app, ws, Some(&pat)).await.0, StatusCode::UNAUTHORIZED);
    let settings = json!({"system_wakeup_child_done":false,"system_wakeup_child_done_instruction":"  Wrap up.  "});
    db.execute(
        "UPDATE workspace SET settings=$2 WHERE id=$1",
        &[&ws, &settings],
    )
    .await
    .unwrap();
    let expected_disabled = json!([{"rule":"child_done","enabled":false,"instruction":"Wrap up.","builtin_instruction":BUILTIN,"customized":1}]);
    assert_eq!(read(&app, ws, Some(&jwt(owner))).await.1, expected_disabled);
    // A custom closed status must not count as an open issue.
    db.execute("INSERT INTO issue_status(workspace_id,key,name,color,category,position) VALUES ($1,'resolved_test','Resolved','#aaaaaa','closed',998)", &[&ws]).await.unwrap();
    db.execute(
        "UPDATE issue SET status='resolved_test' WHERE id=$1",
        &[&open],
    )
    .await
    .unwrap();
    let result = read(&app, ws, Some(&jwt(owner))).await;
    assert_eq!(result.1[0]["customized"], json!(0));
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2); // GET never materializes rules.
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
            json!({"error":"could not load system wakeups"})
        )
    );
    db.execute(
        "ALTER TABLE issue_wakeup_unavailable RENAME TO issue_wakeup",
        &[],
    )
    .await
    .unwrap();
}
