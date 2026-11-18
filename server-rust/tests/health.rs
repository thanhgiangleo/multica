use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use multica_server::{
    health::{Health, MigrationCatalog, ReadinessDb},
    router,
};
use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};
use tokio::time::Duration;
use tower::ServiceExt;

struct FakeDb {
    mode: AtomicU8,
    pings: AtomicUsize,
    queries: AtomicUsize,
}

impl FakeDb {
    fn new(mode: u8) -> Arc<Self> {
        Arc::new(Self {
            mode: AtomicU8::new(mode),
            pings: AtomicUsize::new(0),
            queries: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl ReadinessDb for FakeDb {
    async fn ping(&self) -> Result<(), ()> {
        self.pings.fetch_add(1, Ordering::SeqCst);
        match self.mode.load(Ordering::SeqCst) {
            1 => Err(()),
            6 => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn applied_count(&self, zbs: bool, versions: &[String]) -> Result<i64, ()> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        match (self.mode.load(Ordering::SeqCst), zbs) {
            (2, false) | (4, true) => Ok(versions.len() as i64 - 1),
            (3, false) | (5, true) => Err(()),
            (7, _) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                Ok(versions.len() as i64)
            }
            _ => Ok(versions.len() as i64),
        }
    }
}

fn catalog() -> MigrationCatalog {
    MigrationCatalog {
        upstream: vec!["001".into(), "002".into()],
        zbs: vec!["zbs-1".into()],
    }
}

fn state(db: Option<Arc<dyn ReadinessDb>>) -> Arc<Health> {
    Arc::new(Health::new(db, catalog(), "fixture-commit".into()).with_cache_ttl(Duration::ZERO))
}

async fn request(
    state: Arc<Health>,
    path: &str,
) -> (StatusCode, serde_json::Value, String, String) {
    let response = router(state)
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let ct = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let len = response
        .headers()
        .get("content-length")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(bytes.ends_with(b"\n"));
    assert_eq!(len, bytes.len().to_string());
    (status, serde_json::from_slice(&bytes).unwrap(), ct, len)
}

#[tokio::test]
async fn live_is_independent_of_db_and_identifies_process() {
    let (status, body, ct, _) = request(state(None), "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct, "application/json");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["commit"], "fixture-commit");
    assert_eq!(body["pid"], std::process::id());
    assert!(body["started_at"].as_str().unwrap().ends_with('Z'));
}

#[tokio::test]
async fn both_ready_routes_use_same_migration_checks() {
    let db = FakeDb::new(0);
    for path in ["/readyz", "/healthz"] {
        let (status, body, ct, _) = request(state(Some(db.clone())), path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ct, "application/json");
        assert_eq!(
            body,
            serde_json::json!({"status":"ok", "checks":{"db":"ok","migrations":"ok"}})
        );
    }
    assert_eq!(db.queries.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn no_db_returns_unknown_migrations() {
    let (status, body, _, _) = request(state(None), "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        body,
        serde_json::json!({"status":"not_ready", "checks":{"db":"error","migrations":"unknown"}})
    );
}

#[tokio::test]
async fn ping_error_skips_migration_queries() {
    let db = FakeDb::new(1);
    let (_, body, _, _) = request(state(Some(db.clone())), "/healthz").await;
    assert_eq!(
        body["checks"],
        serde_json::json!({"db":"error","migrations":"unknown"})
    );
    assert_eq!(db.queries.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn upstream_missing_or_error_fails_closed() {
    for (mode, expected) in [(2, "out_of_date"), (3, "error")] {
        let (status, body, _, _) = request(state(Some(FakeDb::new(mode))), "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["checks"]["migrations"], expected);
    }
}

#[tokio::test]
async fn zbs_missing_or_error_fails_closed() {
    for (mode, expected) in [(4, "out_of_date"), (5, "error")] {
        let (status, body, _, _) = request(state(Some(FakeDb::new(mode))), "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["checks"]["migrations"], expected);
    }
}

#[tokio::test]
async fn empty_upstream_catalog_fails_closed() {
    let state = Arc::new(Health::new(
        Some(FakeDb::new(0)),
        MigrationCatalog {
            upstream: vec![],
            zbs: vec![],
        },
        "".into(),
    ));
    let (status, body, _, _) = request(state, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["migrations"], "error");
}

#[tokio::test]
async fn readiness_cache_single_flight_and_expires() {
    let db = FakeDb::new(0);
    let state = Arc::new(
        Health::new(Some(db.clone()), catalog(), "".into())
            .with_cache_ttl(Duration::from_millis(40)),
    );
    let tasks: Vec<_> = (0..12)
        .map(|_| {
            let state = state.clone();
            tokio::spawn(async move { state.readiness().await })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap().status, "ok");
    }
    assert_eq!(db.pings.load(Ordering::SeqCst), 1);
    assert_eq!(db.queries.load(Ordering::SeqCst), 2);
    db.mode.store(1, Ordering::SeqCst);
    assert_eq!(state.readiness().await.status, "ok");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(state.readiness().await.status, "not_ready");
    assert_eq!(db.pings.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn ping_timeout_returns_db_error() {
    let state = state(Some(FakeDb::new(6)));
    let task = tokio::spawn(async move { task_result(state).await });
    tokio::time::advance(Duration::from_secs(3)).await;
    let (status, body) = task.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["db"], "error");
}

#[tokio::test(start_paused = true)]
async fn query_timeout_returns_migration_error() {
    let state = state(Some(FakeDb::new(7)));
    let task = tokio::spawn(async move { task_result(state).await });
    tokio::time::advance(Duration::from_secs(3)).await;
    let (status, body) = task.await.unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["checks"]["migrations"], "error");
}

async fn task_result(state: Arc<Health>) -> (StatusCode, serde_json::Value) {
    let (status, body, _, _) = request(state, "/readyz").await;
    (status, body)
}

#[tokio::test]
async fn embedded_catalog_has_both_migration_streams() {
    let catalog = MigrationCatalog::embedded().unwrap();
    assert_eq!(catalog.upstream.len(), 619);
    assert_eq!(catalog.zbs.len(), 44);
    assert!(
        catalog
            .upstream
            .iter()
            .any(|version| version.starts_with("001_"))
    );
}

#[tokio::test]
async fn unknown_route_is_not_claimed_as_ported() {
    let response = router(state(None))
        .oneshot(
            Request::builder()
                .uri("/api/issues")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
