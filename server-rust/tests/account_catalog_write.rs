//! Frozen-Go PAT mutation contract on a disposable, migrated database.
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

const SECRET: &str = "isolated-zbsi65-test-secret";

async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    credential: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn pat_write_auth_redaction_renewal_cas_and_revocation() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").unwrap();
    let config = Config::from_str(&url).unwrap();
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
    let suffix = Uuid::new_v4().simple().to_string();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('PAT test', $1) RETURNING id",
            &[&format!("{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other', $1) RETURNING id",
            &[&format!("other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let shadow: Uuid = db.query_one("INSERT INTO \"user\"(name,email,zbs_is_shadow) VALUES ('Shadow', $1, true) RETURNING id", &[&format!("shadow-{suffix}@invalid.example")]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()),
    );
    let exp = (chrono::Utc::now().timestamp() + 3600) as usize;
    let jwt = |id: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":id.to_string(),"exp":exp}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let user_jwt = jwt(user);
    let cookie_write = Request::builder()
        .method("POST")
        .uri("/api/tokens/")
        .header("cookie", format!("multica_auth={user_jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"cookie-no-csrf"}"#))
        .unwrap();
    let cookie_result = app.clone().oneshot(cookie_write).await.unwrap();
    assert_eq!(cookie_result.status(), StatusCode::FORBIDDEN);
    let bad = request(
        &app,
        "POST",
        "/api/tokens/",
        &jwt(shadow),
        json!({"name":"bad"}),
    )
    .await;
    assert_eq!(bad.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        request(&app, "POST", "/api/tokens/", &user_jwt, json!({"name":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let (created, pat) = request(
        &app,
        "POST",
        "/api/tokens/",
        &user_jwt,
        json!({"name":"CLI", "expires_in_days":1}),
    )
    .await;
    assert_eq!(created, StatusCode::CREATED);
    let raw = pat["token"].as_str().unwrap();
    assert!(raw.starts_with("mul_") && raw.len() == 44);
    assert_eq!(pat["token_prefix"], &raw[..12]);
    let (blocked_status, _) = request(
        &app,
        "POST",
        "/api/tokens/",
        "mul_not_a_valid_pat",
        json!({"name":"bad"}),
    )
    .await;
    assert_eq!(blocked_status, StatusCode::UNAUTHORIZED);
    let id = Uuid::parse_str(pat["id"].as_str().unwrap()).unwrap();
    let row = db
        .query_one(
            "SELECT token_hash,expires_at,revoked FROM personal_access_token WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, String>("token_hash"),
        hex::encode(Sha256::digest(raw.as_bytes()))
    );
    assert!(!row.get::<_, bool>("revoked"));
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("expires_at")
            .is_some()
    );
    let (_, listed) = request(&app, "GET", "/api/tokens/", &user_jwt, Value::Null).await;
    assert!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["id"] == id.to_string()
                && v.get("token").is_none()
                && v.get("token_hash").is_none())
    );
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/tokens/current/renew",
            &user_jwt,
            Value::Null
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"only personal access tokens can be renewed"})
        )
    );
    let (first, second) = tokio::join!(
        request(&app, "POST", "/api/tokens/current/renew", raw, Value::Null),
        request(&app, "POST", "/api/tokens/current/renew", raw, Value::Null)
    );
    assert_eq!(first.0, StatusCode::OK);
    assert_eq!(second.0, StatusCode::OK);
    assert_ne!(first.1["renewed"], second.1["renewed"]);
    assert_eq!(first.1["expires_at"], second.1["expires_at"]);
    assert_eq!(
        request(&app, "POST", "/api/tokens/current/renew", raw, Value::Null)
            .await
            .1["renewed"],
        false
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            &format!("/api/tokens/{id}"),
            &jwt(other),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        !db.query_one(
            "SELECT revoked FROM personal_access_token WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            "/api/tokens/not-a-uuid",
            &user_jwt,
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            &format!("/api/tokens/{id}"),
            &user_jwt,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            &app,
            "DELETE",
            &format!("/api/tokens/{id}"),
            &user_jwt,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(&app, "POST", "/api/tokens/current/renew", raw, Value::Null)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, "POST", "/api/tokens/", raw, json!({"name":"invalid"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
}
