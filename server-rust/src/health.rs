use async_trait::async_trait;
use axum::{body::Body, extract::State, http::StatusCode, response::Response};
use chrono::{SecondsFormat, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

pub const READINESS_TIMEOUT: Duration = Duration::from_secs(2);
pub const READINESS_CACHE_TTL: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationCatalog {
    pub upstream: Vec<String>,
    pub zbs: Vec<String>,
}

impl MigrationCatalog {
    pub fn embedded() -> Result<Self, serde_json::Error> {
        serde_json::from_str(include_str!("../baseline/health-migrations.json"))
    }
}

#[async_trait]
pub trait ReadinessDb: Send + Sync {
    async fn ping(&self) -> Result<(), ()>;
    async fn applied_count(&self, zbs: bool, versions: &[String]) -> Result<i64, ()>;
}

#[async_trait]
impl ReadinessDb for Pool {
    async fn ping(&self) -> Result<(), ()> {
        let conn = self.get().await.map_err(|_| ())?;
        conn.simple_query("SELECT 1").await.map_err(|_| ())?;
        Ok(())
    }

    async fn applied_count(&self, zbs: bool, versions: &[String]) -> Result<i64, ()> {
        let sql = if zbs {
            "SELECT COUNT(*) FROM schema_migrations_zbs WHERE version = ANY($1)"
        } else {
            "SELECT COUNT(*) FROM schema_migrations WHERE version = ANY($1)"
        };
        let conn = self.get().await.map_err(|_| ())?;
        let row = conn.query_one(sql, &[&versions]).await.map_err(|_| ())?;
        row.try_get(0).map_err(|_| ())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadinessChecks {
    pub db: &'static str,
    pub migrations: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReadinessResponse {
    pub status: &'static str,
    pub checks: ReadinessChecks,
}

impl ReadinessResponse {
    fn ok() -> Self {
        Self {
            status: "ok",
            checks: ReadinessChecks {
                db: "ok",
                migrations: "ok",
            },
        }
    }

    fn unavailable(db: &'static str, migrations: &'static str) -> Self {
        Self {
            status: "not_ready",
            checks: ReadinessChecks { db, migrations },
        }
    }

    pub fn status_code(&self) -> StatusCode {
        if self.status == "ok" {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

#[derive(Serialize)]
struct LiveResponse<'a> {
    status: &'static str,
    pid: u32,
    #[serde(skip_serializing_if = "str::is_empty")]
    commit: &'a str,
    started_at: &'a str,
}

struct CachedReadiness {
    response: ReadinessResponse,
    expires_at: Instant,
}

pub struct Health {
    db: Option<Arc<dyn ReadinessDb>>,
    catalog: MigrationCatalog,
    cache_ttl: Duration,
    cache: Mutex<Option<CachedReadiness>>,
    commit: String,
    started_at: String,
    pid: u32,
}

impl Health {
    pub fn new(
        db: Option<Arc<dyn ReadinessDb>>,
        catalog: MigrationCatalog,
        commit: String,
    ) -> Self {
        Self {
            db,
            catalog,
            cache_ttl: READINESS_CACHE_TTL,
            cache: Mutex::new(None),
            commit,
            started_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            pid: std::process::id(),
        }
    }

    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }

    pub async fn readiness(&self) -> ReadinessResponse {
        if self.cache_ttl.is_zero() {
            return self.compute_readiness().await;
        }
        // Single-flight for both successful and failed probes, matching Go's
        // refresh mutex. Cache expiry starts before the potentially slow probe.
        let mut cache = self.cache.lock().await;
        let now = Instant::now();
        if let Some(cached) = cache.as_ref().filter(|cached| now < cached.expires_at) {
            return cached.response.clone();
        }
        let response = self.compute_readiness().await;
        *cache = Some(CachedReadiness {
            response: response.clone(),
            expires_at: now + self.cache_ttl,
        });
        response
    }

    async fn compute_readiness(&self) -> ReadinessResponse {
        let Some(db) = &self.db else {
            return ReadinessResponse::unavailable("error", "unknown");
        };
        let deadline = Instant::now() + READINESS_TIMEOUT;
        if !matches!(
            tokio::time::timeout_at(deadline, db.ping()).await,
            Ok(Ok(()))
        ) {
            return ReadinessResponse::unavailable("error", "unknown");
        }
        if self.catalog.upstream.is_empty() {
            return ReadinessResponse::unavailable("ok", "error");
        }
        for (zbs, versions) in [(false, &self.catalog.upstream), (true, &self.catalog.zbs)] {
            if versions.is_empty() {
                continue;
            }
            match tokio::time::timeout_at(deadline, db.applied_count(zbs, versions)).await {
                Ok(Ok(count)) if count >= versions.len() as i64 => (),
                Ok(Ok(_)) => return ReadinessResponse::unavailable("ok", "out_of_date"),
                _ => return ReadinessResponse::unavailable("ok", "error"),
            }
        }
        ReadinessResponse::ok()
    }
}

pub async fn live(State(health): State<Arc<Health>>) -> Response {
    json_response(
        StatusCode::OK,
        &LiveResponse {
            status: "ok",
            pid: health.pid,
            commit: &health.commit,
            started_at: &health.started_at,
        },
    )
}

pub async fn ready(State(health): State<Arc<Health>>) -> Response {
    let response = health.readiness().await;
    json_response(response.status_code(), &response)
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response {
    let (status, mut bytes) = match serde_json::to_vec(value) {
        Ok(bytes) => (status, bytes),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            br#"{"error":"failed to encode response"}"#.to_vec(),
        ),
    };
    bytes.push(b'\n');
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Content-Length", bytes.len())
        .body(Body::from(bytes))
        .expect("fixed response headers are valid")
}
