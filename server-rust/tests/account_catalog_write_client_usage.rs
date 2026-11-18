//! Account client-usage HTTP/DB parity on an isolated, migrated database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
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

const SECRET: &str = "synthetic-zbsi65-client-usage-secret";
fn token(user: Uuid, seconds: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+seconds}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    user: &str,
    body: &str,
    platform: &str,
    workspace: Option<Uuid>,
) -> StatusCode {
    let mut req = Request::builder()
        .method("POST")
        .uri("/api/client-usage")
        .header("authorization", format!("Bearer {user}"))
        .header("x-client-platform", platform)
        .header("x-client-version", " 0.1.0 ")
        .header("x-client-os", " MacOS ");
    if let Some(ws) = workspace {
        req = req.header("x-workspace-id", ws.to_string());
    }
    app.clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap()
        .status()
}
fn database() -> Config {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(v) if v == "127.0.0.1"))
    );
    cfg
}
#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn usage_report_is_scoped_validated_and_preserves_runtime_probe() {
    let pool = Arc::new(
        Pool::builder(Manager::new(database(), NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('usage',$1) RETURNING id",
            &[&format!("usage-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('usage',$1) RETURNING id",
            &[&format!("usage-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('other',$1) RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    let valid = token(user, 3600);
    let install = Uuid::new_v4();
    let report = json!({"install_id":install,"runtime":{"probe_result":"SUCCESS","runtime_count":2,
        "provider_summary":{"claude":2},"online_count":1,"offline_count":1}})
    .to_string();
    assert_eq!(
        call(&app, &valid, &report, "web", None).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, &valid, &report, "desktop", Some(foreign)).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, &token(user, -600), &report, "desktop", Some(ws)).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, &valid, &report, "desktop", Some(ws)).await,
        StatusCode::NO_CONTENT
    );
    let row = db.query_one("SELECT workspace_id,client_version,os,probe_result,runtime_count,provider_summary,online_count,offline_count,first_active_at,last_active_at FROM client_usage_daily WHERE user_id=$1 AND install_id=$2", &[&user,&install]).await.unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), Some(ws));
    assert_eq!(row.get::<_, &str>(1), "0.1.0");
    assert_eq!(row.get::<_, &str>(2), "macos");
    assert_eq!(row.get::<_, Option<&str>>(3), Some("success"));
    assert_eq!(row.get::<_, Option<i32>>(4), Some(2));
    assert_eq!(row.get::<_, Option<Value>>(5), Some(json!({"claude":2})));
    assert_eq!(row.get::<_, Option<i32>>(6), Some(1));
    assert_eq!(row.get::<_, Option<i32>>(7), Some(1));
    let first: chrono::DateTime<chrono::Utc> = row.get(8);
    assert_eq!(
        call(
            &app,
            &valid,
            &json!({"install_id":install}).to_string(),
            "desktop",
            None
        )
        .await,
        StatusCode::NO_CONTENT
    );
    let row = db.query_one("SELECT count(*) AS n,workspace_id,probe_result,provider_summary,runtime_probed_at,first_active_at,last_active_at FROM client_usage_daily WHERE user_id=$1 AND install_id=$2 GROUP BY workspace_id,probe_result,provider_summary,runtime_probed_at,first_active_at,last_active_at", &[&user,&install]).await.unwrap();
    assert_eq!(row.get::<_, i64>("n"), 1);
    assert_eq!(row.get::<_, Option<Uuid>>("workspace_id"), Some(ws));
    assert_eq!(row.get::<_, Option<&str>>("probe_result"), Some("success"));
    assert_eq!(
        row.get::<_, Option<Value>>("provider_summary"),
        Some(json!({"claude":2}))
    );
    assert_eq!(
        row.get::<_, chrono::DateTime<chrono::Utc>>("first_active_at"),
        first
    );
    assert!(row.get::<_, chrono::DateTime<chrono::Utc>>("last_active_at") >= first);
    let bad = [
        r#"{"install_id":"bad"}"#,
        r#"{"install_id":"bad","extra":1}"#,
        r#"{"install_id":"bad"}{}"#,
        r#"{"install_id":"bad","runtime":{"probe_result":"error","runtime_count":0}}"#,
    ];
    for payload in bad {
        assert_eq!(
            call(&app, &valid, payload, "desktop", None).await,
            StatusCode::BAD_REQUEST
        );
    }
    for probe in [
        json!({"probe_result":"error","runtime_count":1}),
        json!({"probe_result":"success","runtime_count":2,"provider_summary":{"claude":3},"online_count":1,"offline_count":1}),
        json!({"probe_result":"success","runtime_count":1,"provider_summary":{"CLAUDE":1},"online_count":1,"offline_count":0}),
    ] {
        assert_eq!(
            call(
                &app,
                &valid,
                &json!({"install_id":install,"runtime":probe}).to_string(),
                "desktop",
                None
            )
            .await,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(&app, &valid, &"x".repeat(16 * 1024 + 1), "desktop", None).await,
        StatusCode::BAD_REQUEST
    );
    // Go's Decoder folds keys, merges duplicate runtime structs/maps, and
    // ignores null when decoding a non-pointer install_id string.
    let merged = format!(
        r#"{{"INSTALL_ID":"{install}","install_id":null,"RUNTIME":{{"probe_result":"success","runtime_count":2,"provider_summary":{{"claude":1}},"online_count":1}},"runtime":{{"PROVIDER_SUMMARY":{{"claude":2}},"offline_count":1}}}}"#
    );
    assert_eq!(
        call(&app, &valid, &merged, "desktop", Some(ws)).await,
        StatusCode::NO_CONTENT
    );
    let row = db.query_one("SELECT probe_result, provider_summary FROM client_usage_daily WHERE user_id=$1 AND install_id=$2", &[&user,&install]).await.unwrap();
    assert_eq!(row.get::<_, Option<&str>>(0), Some("success"));
    assert_eq!(row.get::<_, Option<Value>>(1), Some(json!({"claude":2})));
    for payload in [
        format!(r#"{{"install_id":"{install}","runtime":{{"probe_result":"error","unknown":1}}}}"#),
        format!(r#"{{"install_id":"{install}","UNKNOWN":1}}"#),
        format!(r#"{{"install_id":"{install}","runtime":{{"probe_result":false}}}}"#),
    ] {
        assert_eq!(
            call(&app, &valid, &payload, "desktop", Some(ws)).await,
            StatusCode::BAD_REQUEST
        );
    }
    let row = db
        .query_one(
            "SELECT provider_summary FROM client_usage_daily WHERE user_id=$1 AND install_id=$2",
            &[&user, &install],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Value>>(0), Some(json!({"claude":2})));
    let cookie_req = Request::builder()
        .method("POST")
        .uri("/api/client-usage")
        .header("cookie", format!("multica_auth={valid}"))
        .header("x-client-platform", "desktop")
        .body(Body::from(json!({"install_id":install}).to_string()))
        .unwrap();
    assert_eq!(
        app.oneshot(cookie_req).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}
