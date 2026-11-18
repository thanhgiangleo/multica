//! Standalone Rust backend under construction. Only registered routes are implemented.

pub mod api;
pub mod health;
pub mod migrate;
pub mod scheduler;
mod scheduler_issue_wakeup;

use axum::{Router, http::StatusCode, routing::get};
use std::sync::Arc;

pub fn router(health: Arc<health::Health>) -> Router {
    Router::new()
        .route("/health", get(health::live))
        .route("/readyz", get(health::ready))
        .route("/healthz", get(health::ready))
        .with_state(health)
}
pub mod data_hooks;

/// Authenticated read slice; all other endpoints remain unregistered until ported.
pub fn router_with_api(health: Arc<health::Health>, api: api::ApiState) -> Router {
    router(health)
        .merge(api::routes(api))
        .fallback(|| async { (StatusCode::NOT_FOUND, "404 page not found\n") })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use deadpool_postgres::{Manager, Pool};
    use http_body_util::BodyExt;
    use tokio_postgres::{Config, NoTls};
    use tower::ServiceExt;

    #[tokio::test]
    async fn unmatched_path_uses_go_text_404_without_overriding_methods() {
        let pool = Arc::new(
            Pool::builder(Manager::new(Config::new(), NoTls))
                .build()
                .unwrap(),
        );
        let app = router_with_api(
            Arc::new(health::Health::new(
                None,
                health::MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            api::ApiState::new(pool, b"synthetic-only".to_vec()),
        );
        let unknown = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/this-route-does-not-exist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            unknown.headers()["content-type"],
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            unknown.into_body().collect().await.unwrap().to_bytes(),
            "404 page not found\n"
        );
        let method = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
