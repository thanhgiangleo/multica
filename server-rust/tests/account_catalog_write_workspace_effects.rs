//! Workspace-create metrics and daemon refresh use committed DB state, not request data.
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
        account_catalog_write_catalog_publish::{CatalogPublication, CatalogPublisher},
        account_catalog_write_workspace_effects::{WorkspaceCreated, WorkspaceMetrics},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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

const SECRET: &[u8] = b"zbsi65-workspace-effects-synthetic-secret";
type Events = Arc<Mutex<Vec<(String, Value)>>>;
struct Recorder {
    pool: Arc<Pool>,
    events: Events,
    user: Uuid,
}
impl WorkspaceMetrics for Recorder {
    fn record<'a>(
        &'a self,
        event: WorkspaceCreated,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(event.user_id, self.user);
            let db = self.pool.get().await.unwrap();
            let owner: String = db
                .query_one(
                    "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&event.workspace_id, &event.user_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(owner, "owner");
            let statuses: i64 = db
                .query_one(
                    "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND is_system",
                    &[&event.workspace_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(statuses, 7, "metrics must see committed status catalog");
            self.events.lock().unwrap().push((
                event.name().into(),
                json!({"user_id":event.user_id,"workspace_id":event.workspace_id,"properties":event.properties()}),
            ));
        })
    }
}
impl CatalogPublisher for Recorder {
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("non-profile mutation must not refresh runtime profiles") })
    }

    fn disconnect_member<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("workspace create must not disconnect any member") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("workspace create must not invalidate unrelated memberships") })
    }
    fn publish<'a>(
        &'a self,
        _: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("frozen Go does not broadcast workspace:created") })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(users, vec![self.user], "notify creator only");
            let db = self.pool.get().await.unwrap();
            let count: i64 = db
                .query_one(
                    "SELECT count(*) FROM member m JOIN workspace w ON w.id=m.workspace_id WHERE m.user_id=$1 AND m.role='owner'",
                    &[&self.user],
                )
                .await
                .unwrap()
                .get(0);
            assert!(count > 0, "daemon refresh must see committed membership");
            self.events
                .lock()
                .unwrap()
                .push(("daemon".into(), json!(users)));
        })
    }
}
struct Fixture {
    pool: Arc<Pool>,
    recorder: Arc<Recorder>,
    user: Uuid,
    token: String,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_pool_size(1).await
    }
    async fn with_pool_size(size: usize) -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
        );
        // Default pool size one detects callbacks retaining the mutation connection.
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(size)
                .build()
                .unwrap(),
        );
        let user = Uuid::new_v4();
        let db = pool.get().await.unwrap();
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Private creator',$2)",
            &[&user, &format!("{user}@invalid.example")],
        )
        .await
        .unwrap();
        drop(db);
        let recorder = Arc::new(Recorder {
            pool: pool.clone(),
            events: Arc::default(),
            user,
        });
        Self {
            pool,
            recorder,
            user,
            token: jwt(user, 3600),
        }
    }
    fn app(&self, metrics: bool, daemon: bool) -> axum::Router {
        let mut state = ApiState::new(self.pool.clone(), SECRET.to_vec());
        if metrics {
            state = state.with_workspace_metrics(self.recorder.clone());
        }
        if daemon {
            state = state.with_catalog_publisher(self.recorder.clone());
        }
        router_with_api(
            Arc::new(Health::new(
                Some(self.pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        )
    }
    fn take(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.recorder.events.lock().unwrap())
    }
    async fn counts(&self) -> (i64, i64, i64) {
        let db = self.pool.get().await.unwrap();
        let row = db.query_one("SELECT (SELECT count(*) FROM workspace), (SELECT count(*) FROM member), (SELECT count(*) FROM issue_status)", &[]).await.unwrap();
        (row.get(0), row.get(1), row.get(2))
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
fn create(slug: &str) -> Value {
    json!({"name":"  Private workspace  ","slug":slug,"description":"Private description","context":"Private context"})
}
async fn request(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = tokio::time::timeout(Duration::from_secs(10), app.clone().oneshot(req))
        .await
        .expect("workspace-create callback retained only DB connection")
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn call(app: &axum::Router, token: &str, body: Value) -> (StatusCode, Value) {
    request(
        app,
        Request::builder()
            .method("POST")
            .uri("/api/workspaces/")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}
fn check_pair(events: &[(String, Value)], user: Uuid, workspace: Uuid) {
    assert_eq!(
        events,
        &[
            (
                "workspace_created".into(),
                json!({"user_id":user,"workspace_id":workspace,"properties":{"user_id":user,"source":"manual","is_demo":false}})
            ),
            ("daemon".into(), json!([user])),
        ]
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn create_records_metrics_then_refreshes_creator_after_commit() {
    let f = Fixture::new().await;
    let app = f.app(true, true);
    let slug = format!("effects-{}", Uuid::new_v4());
    let (status, result) = call(&app, &f.token, create(&slug)).await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    assert_eq!(result["name"], "Private workspace");
    let workspace = serde_json::from_value(result["id"].clone()).unwrap();
    check_pair(&f.take(), f.user, workspace);
    let db = f.pool.get().await.unwrap();
    let onboarded: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one("SELECT onboarded_at FROM \"user\" WHERE id=$1", &[&f.user])
        .await
        .unwrap()
        .get(0);
    assert!(onboarded.is_none(), "create must not complete onboarding");
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn duplicate_race_and_retry_emit_one_pair_without_first_workspace_flag() {
    let f = Fixture::with_pool_size(4).await;
    let app = f.app(true, true);
    let slug = format!("effects-race-{}", Uuid::new_v4());
    let (a, b) = tokio::join!(
        call(&app, &f.token, create(&slug)),
        call(&app, &f.token, create(&slug))
    );
    let mut statuses = [a.0, b.0];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    let success = if a.0 == StatusCode::CREATED { a.1 } else { b.1 };
    check_pair(
        &f.take(),
        f.user,
        serde_json::from_value(success["id"].clone()).unwrap(),
    );
    assert_eq!(
        call(&app, &f.token, create(&slug)).await.0,
        StatusCode::CONFLICT
    );
    assert!(f.take().is_empty());
    let (status, second) = call(
        &app,
        &f.token,
        create(&format!("effects-second-{}", Uuid::new_v4())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    check_pair(
        &f.take(),
        f.user,
        serde_json::from_value(second["id"].clone()).unwrap(),
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn validation_and_negative_credentials_do_not_emit_effects() {
    let f = Fixture::new().await;
    let app = f.app(true, true);
    let before = f.counts().await;
    for token in ["missing".to_owned(), jwt(f.user, -3600)] {
        assert_eq!(
            call(&app, &token, create("denied-create")).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let cookie = Request::builder()
        .method("POST")
        .uri("/api/workspaces")
        .header("cookie", format!("multica_auth={}", f.token))
        .header("content-type", "application/json")
        .body(Body::from(create("denied-csrf").to_string()))
        .unwrap();
    assert_eq!(request(&app, cookie).await.0, StatusCode::FORBIDDEN);
    for invalid in [
        json!({}),
        create("login"),
        create("-invalid"),
        json!({"name":"Valid","slug":"valid","issue_prefix":"x-y"}),
    ] {
        assert_eq!(
            call(&app, &f.token, invalid).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let malformed = Request::builder()
        .method("POST")
        .uri("/api/workspaces")
        .header("authorization", format!("Bearer {}", f.token))
        .header("content-type", "application/json")
        .body(Body::from("{invalid"))
        .unwrap();
    assert_eq!(request(&app, malformed).await.0, StatusCode::BAD_REQUEST);
    for (label, expiry, revoked) in [("expired", "-1 hour", false), ("revoked", "1 hour", true)] {
        let token = format!("mul_workspace_effects_{label}_{}", Uuid::new_v4());
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        let db = f.pool.get().await.unwrap();
        db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at,revoked) VALUES($1,$2,$3,'mul_test',now()+$4::text::interval,$5)", &[&f.user,&label,&hash,&expiry,&revoked]).await.unwrap();
        drop(db);
        assert_eq!(
            call(&app, &token, create("denied-pat")).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(f.counts().await, before);
    assert!(f.take().is_empty());
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn active_pat_uses_same_post_commit_effects() {
    let f = Fixture::new().await;
    let token = format!("mul_workspace_effects_active_{}", Uuid::new_v4());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    let db = f.pool.get().await.unwrap();
    db.execute(
        "INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,expires_at,revoked) VALUES($1,'active',$2,'mul_test',now()+interval '1 hour',false)",
        &[&f.user, &hash],
    )
    .await
    .unwrap();
    drop(db);
    let (status, result) = call(
        &f.app(true, true),
        &token,
        create(&format!("effects-pat-{}", Uuid::new_v4())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{result}");
    check_pair(
        &f.take(),
        f.user,
        serde_json::from_value(result["id"].clone()).unwrap(),
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn seed_and_deferred_commit_failures_rollback_without_effects() {
    let f = Fixture::new().await;
    let app = f.app(true, true);
    let before = f.counts().await;
    for deferred in [false, true] {
        let db = f.pool.get().await.unwrap();
        db.batch_execute("CREATE OR REPLACE FUNCTION pg_temp.reject_workspace_effects() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic workspace effects failure'; END $$").await.unwrap();
        let (create_trigger, drop_trigger) = if deferred {
            (
                "CREATE CONSTRAINT TRIGGER account_catalog_workspace_effects_fault AFTER INSERT ON workspace DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_effects()",
                "DROP TRIGGER account_catalog_workspace_effects_fault ON workspace",
            )
        } else {
            (
                "CREATE TRIGGER account_catalog_workspace_effects_fault BEFORE INSERT ON issue_status FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_effects()",
                "DROP TRIGGER account_catalog_workspace_effects_fault ON issue_status",
            )
        };
        db.batch_execute(create_trigger).await.unwrap();
        drop(db);
        let (status, result) = call(
            &app,
            &f.token,
            create(&format!("effects-rollback-{}", Uuid::new_v4())),
        )
        .await;
        let db = f.pool.get().await.unwrap();
        db.batch_execute(drop_trigger).await.unwrap();
        drop(db);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{result}");
        assert_eq!(f.counts().await, before);
        assert!(f.take().is_empty());
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn metrics_and_daemon_adapters_remain_independently_optional() {
    let f = Fixture::new().await;
    for (metrics, daemon) in [(false, false), (true, false), (false, true)] {
        let app = f.app(metrics, daemon);
        let (status, result) = call(
            &app,
            &f.token,
            create(&format!("effects-optional-{}", Uuid::new_v4())),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{result}");
        let events = f.take();
        assert_eq!(events.len(), usize::from(metrics) + usize::from(daemon));
        if metrics {
            assert_eq!(events[0].0, "workspace_created");
            assert_eq!(events[0].1["workspace_id"], result["id"]);
        }
        if daemon {
            assert_eq!(events[0], ("daemon".into(), json!([f.user])));
        }
    }
}
