//! HTTP/DB PAT request parity, finite expiry, and credential fences on synthetic data.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::Engine;
use chrono::{DateTime, Utc};
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

const SECRET: &str = "synthetic-pat-edges-only";

async fn call(
    app: &axum::Router,
    credential: &str,
    raw: Vec<u8>,
    content_type: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/tokens/")
        .header("authorization", format!("Bearer {credential}"));
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(raw)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn pat_request_oracle_http_db_and_actor_fences() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi65"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('PAT edges',$1) RETURNING id",
            &[&format!("pat-edges-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let jwt = encode(
        &Header::default(),
        &json!({"sub":user.to_string(),"exp":Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("account_catalog_write_pat_oracle.json")).unwrap();
    for (index, case) in cases.iter().enumerate() {
        let label = case["label"].as_str().unwrap();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(case["body"].as_str().unwrap())
            .unwrap();
        let count: i64 = db
            .query_one(
                "SELECT count(*) FROM personal_access_token WHERE user_id=$1",
                &[&user],
            )
            .await
            .unwrap()
            .get(0);
        let before = Utc::now();
        let (status, body) = call(
            &app,
            &jwt,
            raw,
            if index % 2 == 0 {
                None
            } else {
                Some("text/plain")
            },
        )
        .await;
        let expected_error = if case["decode_error"] == true {
            Some((StatusCode::BAD_REQUEST, "invalid request body"))
        } else if case["name"].as_str().unwrap().is_empty() {
            Some((StatusCode::BAD_REQUEST, "name is required"))
        } else if case["name"].as_str().unwrap().contains('\0') {
            Some((StatusCode::INTERNAL_SERVER_ERROR, "failed to create token"))
        } else {
            None
        };
        if let Some((expected_status, message)) = expected_error {
            assert_eq!(
                (status, body),
                (expected_status, json!({"error":message})),
                "{label}"
            );
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM personal_access_token WHERE user_id=$1",
                    &[&user]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                count,
                "{label}"
            );
            continue;
        }
        assert_eq!(status, StatusCode::CREATED, "{label}: {body}");
        assert_eq!(body["name"], case["name"], "{label}");
        assert!(body.get("token_hash").is_none(), "{label}");
        let token = body["token"].as_str().unwrap();
        assert!(token.starts_with("mul_") && token.len() == 44, "{label}");
        let id = Uuid::parse_str(body["id"].as_str().unwrap()).unwrap();
        let row = db.query_one("SELECT name,token_hash,token_prefix,expires_at FROM personal_access_token WHERE id=$1 AND user_id=$2", &[&id,&user]).await.unwrap();
        assert_eq!(row.get::<_, String>("name"), body["name"], "{label}");
        assert_eq!(
            row.get::<_, String>("token_hash"),
            hex::encode(Sha256::digest(token.as_bytes())),
            "{label}"
        );
        assert_eq!(
            row.get::<_, String>("token_prefix"),
            &token[..12],
            "{label}"
        );
        let stored: Option<DateTime<Utc>> = row.get("expires_at");
        if let Some(duration) = case["expiry_ns"].as_str() {
            let duration = chrono::Duration::nanoseconds(duration.parse().unwrap());
            let stored = stored.expect("positive days must never become a permanent credential");
            assert!(
                stored >= before + duration - chrono::Duration::microseconds(1),
                "{label}"
            );
            assert!(stored <= Utc::now() + duration, "{label}");
            let response_time = DateTime::parse_from_rfc3339(body["expires_at"].as_str().unwrap())
                .unwrap()
                .timestamp();
            // Go/Rust RFC3339 second precision drops historical local offsets' seconds.
            let drift = (response_time - stored.timestamp()).abs();
            assert!(
                drift
                    <= if stored.timestamp() < -2_208_988_800 {
                        60
                    } else {
                        0
                    },
                "{label}: {drift}"
            );
        } else {
            assert!(stored.is_none(), "{label}");
            assert!(body["expires_at"].is_null(), "{label}");
        }
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM personal_access_token WHERE user_id=$1",
                &[&user]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            count + 1,
            "{label}"
        );
    }
    // A valid machine token must not mint a long-lived account credential.
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('PAT edges',$1) RETURNING id",
            &[&format!("pat-{}", Uuid::new_v4().simple())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'PAT edge agent','cloud',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'PAT edge runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let machine = format!("mat_pat_edges_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(machine.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&user]).await.unwrap();
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM personal_access_token WHERE user_id=$1",
            &[&user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        call(&app, &machine, b"{\"name\":\"escalation\"}".to_vec(), None)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM personal_access_token WHERE user_id=$1",
            &[&user]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        count
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, &machine, b"{}".to_vec(), None).await.0,
        StatusCode::UNAUTHORIZED
    );
}
