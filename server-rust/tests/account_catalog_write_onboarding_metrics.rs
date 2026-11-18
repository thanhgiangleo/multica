//! Metrics-only onboarding effects see committed rows and never receive account PII.
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
    time::Duration,
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi65-onboarding-metrics-synthetic-secret";
#[derive(Clone)]
enum Expected {
    Questionnaire(Value),
    Completed,
    Waitlist {
        email: String,
        reason: Option<String>,
    },
}
struct Recorder {
    pool: Arc<Pool>,
    user: Uuid,
    expected: Mutex<Option<Expected>>,
    metrics: Mutex<Vec<OnboardingMetric>>,
}
impl OnboardingMetrics for Recorder {
    fn record<'a>(
        &'a self,
        metric: OnboardingMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let expected = self
                .expected
                .lock()
                .unwrap()
                .clone()
                .expect("unexpected counter");
            let db = self.pool.get().await.unwrap();
            let row = db.query_one("SELECT onboarding_questionnaire,onboarded_at,cloud_waitlist_email,cloud_waitlist_reason FROM \"user\" WHERE id=$1", &[&self.user]).await.unwrap();
            match expected {
                Expected::Questionnaire(q) => assert_eq!(row.get::<_, Value>(0), q),
                Expected::Completed => assert!(
                    row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
                        .is_some()
                ),
                Expected::Waitlist { email, reason } => {
                    assert!(
                        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
                            .is_none(),
                        "waitlist must not complete onboarding"
                    );
                    assert_eq!(row.get::<_, String>(2), email);
                    assert_eq!(row.get::<_, Option<String>>(3), reason);
                }
            }
            self.metrics.lock().unwrap().push(metric);
        })
    }
}
struct Fixture {
    pool: Arc<Pool>,
    user: Uuid,
    token: String,
    recorder: Arc<Recorder>,
    app: axum::Router,
}
impl Fixture {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
        );
        // A callback holding the mutation connection would deadlock this pool.
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(1)
                .build()
                .unwrap(),
        );
        let user = Uuid::new_v4();
        pool.get()
            .await
            .unwrap()
            .execute(
                "INSERT INTO \"user\"(id,name,email) VALUES($1,'Private account',$2)",
                &[&user, &format!("{user}@invalid.example")],
            )
            .await
            .unwrap();
        let recorder = Arc::new(Recorder {
            pool: pool.clone(),
            user,
            expected: Mutex::default(),
            metrics: Mutex::default(),
        });
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec()).with_onboarding_metrics(recorder.clone()),
        );
        Self {
            pool,
            user,
            token: jwt(user, 3600),
            recorder,
            app,
        }
    }
    fn expect(&self, expected: Expected) {
        *self.recorder.expected.lock().unwrap() = Some(expected);
    }
    fn take(&self) -> Vec<OnboardingMetric> {
        std::mem::take(&mut *self.recorder.metrics.lock().unwrap())
    }
    async fn call(&self, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
        self.request(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {}", self.token))
                .header("content-type", "application/json")
                .header("x-client-platform", "private-device-build")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
    }
    async fn request(&self, request: Request<Body>) -> (StatusCode, Value) {
        let response =
            tokio::time::timeout(Duration::from_secs(10), self.app.clone().oneshot(request))
                .await
                .expect("onboarding metrics retained mutation DB connection")
                .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    async fn fail_updates(&self) -> String {
        let name = format!("zbsi65_onboard_fail_{}", Uuid::new_v4().simple());
        self.pool.get().await.unwrap().batch_execute(&format!(
            "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{}'::uuid THEN RAISE EXCEPTION 'synthetic onboarding commit failure'; END IF; RETURN NEW; END $$;
             CREATE CONSTRAINT TRIGGER {name} AFTER UPDATE ON \"user\" DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {name}();", self.user
        )).await.unwrap();
        name
    }
    async fn allow_updates(&self, name: &str) {
        self.pool
            .get()
            .await
            .unwrap()
            .batch_execute(&format!(
                "DROP TRIGGER {name} ON \"user\"; DROP FUNCTION {name}();"
            ))
            .await
            .unwrap();
    }
}
fn jwt(user: Uuid, ttl: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+ttl}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
fn questionnaire() -> Value {
    json!({"version":2,"role":"private-role","source":["private-acquisition"],"use_case":["private-purpose"],"source_other":"private-source@example.test","role_other":"private-role-other","use_case_other":"private-use-other"})
}
fn questionnaire_metrics() -> Vec<OnboardingMetric> {
    vec![
        OnboardingMetric::Started {
            platform: Platform::Unknown,
        },
        OnboardingMetric::QuestionnaireSubmitted,
        OnboardingMetric::SourceSubmitted,
    ]
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn questionnaire_counters_are_private_committed_and_retry_safe() {
    let f = Fixture::new().await;
    let q = questionnaire();
    f.expect(Expected::Questionnaire(q.clone()));
    let (status, result) = f
        .call("PATCH", "/api/me/onboarding", json!({"questionnaire":q}))
        .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["onboarding_questionnaire"], q);
    assert!(result["onboarded_at"].is_null());
    assert_eq!(f.take(), questionnaire_metrics());
    assert_eq!(
        f.call("PATCH", "/api/me/onboarding", json!({"questionnaire":q}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        f.call("PATCH", "/api/me/onboarding", json!({})).await.0,
        StatusCode::OK
    );
    assert!(f.take().is_empty());
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn completion_counts_exact_go_path_once_without_identity_labels() {
    let f = Fixture::new().await;
    f.expect(Expected::Completed);
    let body = json!({"completion_path":" FULL ","workspace_id":Uuid::new_v4()});
    let (status, first) = f
        .call("POST", "/api/me/onboarding/complete", body.clone())
        .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(
        f.take(),
        [OnboardingMetric::Completed {
            path: CompletionPath::Unknown
        }]
    );
    let second = f.call("POST", "/api/me/onboarding/complete", body).await;
    assert_eq!(second.0, StatusCode::OK);
    assert_eq!(first["onboarded_at"], second.1["onboarded_at"]);
    assert!(f.take().is_empty());
    assert_eq!(
        f.call(
            "POST",
            "/api/me/onboarding/complete",
            json!({"workspace_id":"invalid"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(f.take().is_empty());
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn waitlist_counts_each_success_without_account_data_or_completion() {
    let f = Fixture::new().await;
    for reason in [Some("private free-text reason".to_owned()), None] {
        f.expect(Expected::Waitlist {
            email: "private-person@example.test".into(),
            reason: reason.clone(),
        });
        let body =
            json!({"email":" Private-Person@EXAMPLE.TEST ","reason":reason.unwrap_or_default()});
        let (status, result) = f
            .call("POST", "/api/me/onboarding/cloud-waitlist", body)
            .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert!(result["onboarded_at"].is_null());
        assert!(result.get("cloud_waitlist_email").is_none());
        assert!(result.get("cloud_waitlist_reason").is_none());
        assert_eq!(f.take(), [OnboardingMetric::CloudWaitlistJoined]);
    }
    assert_eq!(
        f.call(
            "POST",
            "/api/me/onboarding/cloud-waitlist",
            json!({"email":"invalid","reason":""})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(f.take().is_empty());
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn failed_transactions_do_not_count_and_retries_count_once() {
    let f = Fixture::new().await;
    let q = questionnaire();
    let name = f.fail_updates().await;
    assert_eq!(
        f.call("PATCH", "/api/me/onboarding", json!({"questionnaire":q}))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.take().is_empty());
    let before: Value = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT onboarding_questionnaire FROM \"user\" WHERE id=$1",
            &[&f.user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, json!({}));
    f.allow_updates(&name).await;
    f.expect(Expected::Questionnaire(q.clone()));
    assert_eq!(
        f.call("PATCH", "/api/me/onboarding", json!({"questionnaire":q}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(f.take(), questionnaire_metrics());

    let name = f.fail_updates().await;
    assert_eq!(
        f.call(
            "POST",
            "/api/me/onboarding/complete",
            json!({"completion_path":"full"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.take().is_empty());
    let before: Option<chrono::DateTime<chrono::Utc>> = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT onboarded_at FROM \"user\" WHERE id=$1", &[&f.user])
        .await
        .unwrap()
        .get(0);
    assert!(before.is_none());
    f.allow_updates(&name).await;
    f.expect(Expected::Completed);
    assert_eq!(
        f.call(
            "POST",
            "/api/me/onboarding/complete",
            json!({"completion_path":"full"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        f.take(),
        [OnboardingMetric::Completed {
            path: CompletionPath::Full
        }]
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn waitlist_failed_implicit_commit_never_counts_or_stores_pii() {
    let f = Fixture::new().await;
    let body = json!({"email":"private-person@example.test","reason":"private reason"});
    let name = f.fail_updates().await;
    assert_eq!(
        f.call("POST", "/api/me/onboarding/cloud-waitlist", body.clone())
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.take().is_empty());
    let row = f.pool.get().await.unwrap().query_one("SELECT cloud_waitlist_email,cloud_waitlist_reason,onboarded_at FROM \"user\" WHERE id=$1", &[&f.user]).await.unwrap();
    assert!(row.get::<_, Option<String>>(0).is_none());
    assert!(row.get::<_, Option<String>>(1).is_none());
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(2)
            .is_none()
    );
    f.allow_updates(&name).await;
    f.expect(Expected::Waitlist {
        email: "private-person@example.test".into(),
        reason: Some("private reason".into()),
    });
    assert_eq!(
        f.call("POST", "/api/me/onboarding/cloud-waitlist", body)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(f.take(), [OnboardingMetric::CloudWaitlistJoined]);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn rejected_credentials_and_csrf_never_reach_counter_adapter() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new().await;
    let mut tokens = vec!["invalid".to_owned(), jwt(f.user, -3600)];
    for (revoked, expires) in [(true, 3600_i64), (false, -3600)] {
        let token = format!("mul_{}", Uuid::new_v4().simple());
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        f.pool.get().await.unwrap().execute(
            "INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked,expires_at) VALUES($1,'Synthetic rejected PAT',$2,$3,$4,$5)",
            &[&f.user,&hash,&&token[..12],&revoked,&(chrono::Utc::now()+chrono::Duration::seconds(expires))],
        ).await.unwrap();
        tokens.push(token);
    }
    for path in [
        "/api/me/onboarding",
        "/api/me/onboarding/complete",
        "/api/me/onboarding/cloud-waitlist",
    ] {
        let method = if path == "/api/me/onboarding" {
            "PATCH"
        } else {
            "POST"
        };
        for token in &tokens {
            let (status, _) = f
                .request(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header("authorization", format!("Bearer {token}"))
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        }
        let (status, _) = f
            .request(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("cookie", format!("multica_auth={}", f.token))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    assert!(f.take().is_empty());
}
