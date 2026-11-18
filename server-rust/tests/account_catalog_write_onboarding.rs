//! HTTP/DB onboarding transitions and typed post-commit analytics on a disposable database.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        account_catalog_write_onboarding::{
            CompletionPath, OnboardingMetric, OnboardingMetrics, Platform,
        },
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "zbsi65-onboarding-test-secret";
#[derive(Default)]
struct Recorder(Mutex<Vec<OnboardingMetric>>);
impl OnboardingMetrics for Recorder {
    fn record<'a>(
        &'a self,
        event: OnboardingMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
        })
    }
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn onboarding_mutations_are_serialized_and_observed_after_commit() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let id: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Onboard', $1) RETURNING id",
            &[&format!("{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Other', $1) RETURNING id",
            &[&format!("{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let recorder = Arc::new(Recorder::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()).with_onboarding_metrics(recorder.clone()),
    );
    let jwt = |user: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":user.to_string(),"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    };
    let token = jwt(id);
    let stranger = jwt(other);
    assert_eq!(
        call(&app, "PATCH", "/api/me/onboarding", "invalid", "{}")
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "PATCH", "/api/me/onboarding", &token, "")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me/onboarding",
            &token,
            &json!({"questionnaire":{"x":"a".repeat(17_000)}}).to_string()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let cookie = Request::builder()
        .method("PATCH")
        .uri("/api/me/onboarding")
        .header("cookie", format!("multica_auth={token}"))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"questionnaire":{"role":"blocked"}}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me/onboarding",
            &token,
            r#"{"QUESTIONNAIRE":{"version":2,"role":"discarded"},"questionnaire":null} trailing"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT onboarding_questionnaire FROM \"user\" WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, Value>(0),
        json!({})
    );
    assert!(recorder.0.lock().unwrap().is_empty());
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me/onboarding",
            &token,
            r#"{"questionnaire":null,"QUESTIONNAIRE":{"version":2,"role":"dev"}} ignored"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT onboarding_questionnaire FROM \"user\" WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, Value>(0)["role"],
        "dev"
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            "/api/me/onboarding",
            &token,
            r#"{"questionnaire":{"version":2,"role":"dev"}}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    let completed = r#"{"questionnaire":{"version":2,"role":"dev","use_case":["automation"]}}"#;
    let (a, b) = tokio::join!(
        call(&app, "PATCH", "/api/me/onboarding", &token, completed),
        call(&app, "PATCH", "/api/me/onboarding", &token, completed)
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    assert_eq!(call(&app,"PATCH","/api/me/onboarding",&token,r#"{"questionnaire":{"version":2,"role":"dev","use_case":["automation"],"source_skipped":true}}"#).await.0,StatusCode::OK);
    let before = db
        .query_one(
            "SELECT onboarding_questionnaire FROM \"user\" WHERE id=$1",
            &[&other],
        )
        .await
        .unwrap();
    assert_eq!(before.get::<_, Value>(0), json!({}));
    assert_eq!(
        call(&app, "PATCH", "/api/me/onboarding", &stranger, "{}")
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/me/onboarding/complete",
            &token,
            r#"{"workspace_id":"not-a-uuid"}"#
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (c, d) = tokio::join!(
        call(
            &app,
            "POST",
            "/api/me/onboarding/complete",
            &token,
            r#"{"COMPLETION_PATH":"full","completion_path":null} ignored"#
        ),
        call(
            &app,
            "POST",
            "/api/me/onboarding/complete",
            &token,
            r#"{"completion_path":"full"}"#
        )
    );
    assert_eq!(c.0, StatusCode::OK);
    assert_eq!(d.0, StatusCode::OK);
    assert_eq!(c.1["onboarded_at"], d.1["onboarded_at"]);
    let row = db
        .query_one(
            "SELECT onboarding_questionnaire,onboarded_at FROM \"user\" WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Value>(0)["source_skipped"], true);
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
            .is_some()
    );
    let events = recorder.0.lock().unwrap().clone();
    assert_eq!(
        events,
        [
            OnboardingMetric::Started {
                platform: Platform::Unknown
            },
            OnboardingMetric::QuestionnaireSubmitted,
            OnboardingMetric::SourceSubmitted,
            OnboardingMetric::Completed {
                path: CompletionPath::Full,
            },
        ]
    );
    db.execute("DELETE FROM \"user\" WHERE id=$1 OR id=$2", &[&id, &other])
        .await
        .unwrap();
}
