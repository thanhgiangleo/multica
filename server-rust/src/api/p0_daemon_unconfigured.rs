//! P0 daemon routes whose capability needs a subsystem this backend does not
//! have. The branch below is the contract Go returns when that subsystem is
//! unconfigured; nothing here calls a provider or enqueues an agent.
//! Merge `routes()` only inside the Bearer-only daemon router.
use super::{ApiError, ApiState, GoJson};
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/daemon/tasks/{taskId}/plugin-hooks",
            post(plugin_hooks),
        )
        .route(
            "/api/daemon/tasks/{taskId}/plugin-mcp/{contributionId}/credential",
            get(plugin_mcp_credential),
        )
}

/// Go: `InvokeAgentPluginHook` calls `requirePluginsV1` (plugin.go:20) before it
/// resolves the task, so a deployment with plugins off answers every caller with
/// 403 and never reveals whether the task exists.
///
/// `featureflags.PluginsV1Enabled` defaults to false and this backend has no
/// Plugin execution service at all (it has no hook signing, circuit breaker or
/// invocation record), so the disabled branch is the only reachable one. The
/// route exists so a daemon that calls it gets Go's exact refusal instead of a
/// 404 from a missing route.
async fn plugin_hooks(
    State(_state): State<ApiState>,
    Path(_raw): Path<String>,
) -> Result<Response, ApiError> {
    // writeFeatureDisabled(w, code, msg) == writeErrorCode(w, 403, code, msg)
    // == {"error": msg, "code": code} with Content-Type: application/json.
    Ok((
        StatusCode::FORBIDDEN,
        GoJson(json!({
            "error": "Plugin management is not enabled",
            "code": "plugin_api_disabled"
        })),
    )
        .into_response())
}

/// Go: `ResolvePluginMCPCredential` also calls `requirePluginsV1` first
/// (plugin_mcp.go:130), before task access and before the contribution id is
/// parsed, so a plugins-off deployment refuses identically for a missing task, a
/// malformed contribution and a real one.
async fn plugin_mcp_credential(
    State(_state): State<ApiState>,
    Path((_task, _contribution)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    Ok((
        StatusCode::FORBIDDEN,
        GoJson(json!({
            "error": "Plugin management is not enabled",
            "code": "plugin_api_disabled"
        })),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::super::ApiState;
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
    };
    use deadpool_postgres::{Manager, Pool};
    use http_body_util::BodyExt;
    use std::{str::FromStr, sync::Arc};
    use tokio_postgres::{Config, NoTls};
    use tower::ServiceExt;

    fn state() -> ApiState {
        // The handler answers before it touches the pool, so an unreachable URL
        // proves the disabled branch never reaches the database.
        let config = Config::from_str("postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
            .expect("synthetic config");
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(1)
                .build()
                .unwrap(),
        );
        ApiState::new(pool, b"synthetic-test-secret".to_vec())
    }

    #[tokio::test]
    async fn plugin_hooks_is_refused_without_resolving_the_task() {
        let app: Router = super::routes().with_state(state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/daemon/tasks/not-even-a-uuid/plugin-hooks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": "Plugin management is not enabled",
                "code": "plugin_api_disabled"
            })
        );
    }

    #[tokio::test]
    async fn plugin_mcp_credential_is_refused_before_parsing_the_contribution() {
        // Go gates this route with requirePluginsV1 before requireDaemonTask
        // access and before splitPluginContributionID, so an unparseable
        // contribution id must still see the plugin refusal, not a 400.
        let app: Router = super::routes().with_state(state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/daemon/tasks/not-even-a-uuid/plugin-mcp/not-a-contribution/credential")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "error": "Plugin management is not enabled",
                "code": "plugin_api_disabled"
            })
        );
    }
}
