//! Human-only account billing and workspace subscription proxy boundaries.
use super::{Actor, ApiError, ApiState, GoJson, Identity, WorkspaceSelectors};
use axum::{
    Router,
    body::Body,
    extract::{Extension, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{IgnoredAny, MapAccess, Visitor},
};
use std::{fmt, future::Future};
use uuid::Uuid;

const MAX_BODY: usize = 1 << 20;
const FLAG: &str = "billing_workspace_subscriptions";

#[derive(Clone, Default)]
pub struct CloudRuntimeConfig {
    pub cloud_url: String,
}

impl CloudRuntimeConfig {
    pub fn from_env() -> Self {
        Self {
            cloud_url: std::env::var("MULTICA_CLOUD_URL").unwrap_or_default(),
        }
    }

    // Go's NewClient trims whitespace and trailing slashes at construction, so
    // "/" or "//" is not configured at all rather than misconfigured.
    fn base_url(&self) -> &str {
        self.cloud_url.trim().trim_end_matches('/')
    }

    pub fn enabled(&self) -> bool {
        !self.base_url().is_empty()
    }
}

/// Environment overrides are read at evaluation time, as in Go's EnvProvider.
#[derive(Clone)]
pub struct CloudBillingFlagProvider {
    prefix: String,
}

impl Default for CloudBillingFlagProvider {
    fn default() -> Self {
        Self::new("FF_")
    }
}

impl CloudBillingFlagProvider {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
    pub fn from_env() -> Self {
        Self::default()
    }
    pub fn enabled(&self, key: &str, user_id: &str) -> bool {
        std::env::var(format!("{}{}", self.prefix, flag_key_to_env(key)))
            .ok()
            .is_some_and(|value| flag_value(&value, key, user_id))
    }
}

fn flag_key_to_env(key: &str) -> String {
    let mut out = String::new();
    let mut underscore = false;
    for c in key.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_uppercase());
            underscore = false;
        } else if !underscore {
            out.push('_');
            underscore = true;
        }
    }
    out.trim_matches('_').to_owned()
}

fn bucket(key: &str, user: &str) -> u32 {
    key.bytes()
        .chain(std::iter::once(0))
        .chain(user.bytes())
        .fold(2166136261u32, |hash, byte| {
            (hash ^ u32::from(byte)).wrapping_mul(16777619)
        })
        % 100
}

fn flag_value(value: &str, key: &str, user: &str) -> bool {
    let value = value.trim();
    if let Some(number) = value.strip_suffix('%') {
        // EnvProvider rejects out-of-range percentages; inPercent itself clamps.
        return number
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|v| (0..=100).contains(v))
            .is_some_and(|v| bucket(key, user) < v as u32);
    }
    !matches!(
        value.to_ascii_lowercase().as_str(),
        "" | "false" | "off" | "0" | "no"
    )
}

#[derive(Debug)]
struct Fault {
    status: StatusCode,
    message: &'static str,
    code: Option<&'static str>,
    middleware: bool,
}

impl Fault {
    fn handler(status: StatusCode, message: &'static str) -> Self {
        Self {
            status,
            message,
            code: None,
            middleware: false,
        }
    }
    fn middleware(status: StatusCode, message: &'static str) -> Self {
        Self {
            status,
            message,
            code: None,
            middleware: true,
        }
    }
    fn disabled(code: &'static str, message: &'static str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message,
            code: Some(code),
            middleware: false,
        }
    }
}

#[derive(Serialize)]
struct CodedError {
    error: &'static str,
    code: &'static str,
}

impl IntoResponse for Fault {
    fn into_response(self) -> Response {
        let mut response = if self.middleware {
            // workspace.go writes its JSON directly, without handler.go's newline.
            let body = format!("{{\"error\":\"{}\"}}", self.message);
            ([("content-type", "application/json")], body).into_response()
        } else if let Some(code) = self.code {
            GoJson(CodedError {
                error: self.message,
                code,
            })
            .into_response()
        } else {
            GoJson(serde_json::json!({"error": self.message})).into_response()
        };
        *response.status_mut() = self.status;
        response
    }
}

fn cloud_result(config: &CloudRuntimeConfig) -> Fault {
    if !config.enabled() {
        Fault::disabled(
            "cloud_runtime_not_configured",
            "cloud runtime is not configured",
        )
    } else if reqwest::Url::parse(config.base_url())
        .ok()
        .is_none_or(|url| url.host_str().is_none())
    {
        Fault {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "cloud runtime is misconfigured",
            code: Some("cloud_runtime_misconfigured"),
            middleware: false,
        }
    } else {
        // The upstream cloud call is intentionally absent; never contact a provider here.
        Fault::handler(StatusCode::BAD_GATEWAY, "cloud runtime request failed")
    }
}

async fn human_gate(
    Extension(actor): Extension<Actor>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    actor.require_human()?;
    Ok(next.run(request).await)
}

pub(super) fn routes() -> Router<ApiState> {
    let billing = Router::new()
        .route("/api/cloud-billing/balance", get(billing))
        .route("/api/cloud-billing/transactions", get(billing))
        .route("/api/cloud-billing/batches", get(billing))
        .route("/api/cloud-billing/topups", get(billing))
        .route("/api/cloud-billing/price-tiers", get(billing))
        .route("/api/cloud-billing/checkout-sessions", post(billing))
        .route(
            "/api/cloud-billing/checkout-sessions/{sessionId}",
            get(billing),
        )
        .route("/api/cloud-billing/portal-sessions", post(billing))
        .route_layer(middleware::from_fn(human_gate));
    let subscriptions = Router::new()
        .route("/api/cloud-subscriptions/summary", get(subscription))
        .route("/api/cloud-subscriptions/prices", get(subscription))
        .route(
            "/api/cloud-subscriptions/checkout-sessions",
            post(subscription),
        )
        .route(
            "/api/cloud-subscriptions/seats/purchase-preview",
            post(subscription),
        )
        .route(
            "/api/cloud-subscriptions/seats/purchases",
            post(subscription),
        )
        .route(
            "/api/cloud-subscriptions/seats/reconcile",
            post(subscription),
        )
        .route(
            "/api/cloud-subscriptions/portal-sessions",
            post(subscription),
        );
    billing.merge(subscriptions)
}

async fn billing(
    State(state): State<ApiState>,
    Extension(Identity(_user)): Extension<Identity>,
    request: Request,
) -> Result<Response, Fault> {
    let config = &state.cloud_billing;
    let path = request.uri().path();
    if let Some(raw) = path.strip_prefix("/api/cloud-billing/checkout-sessions/") {
        let id = percent_encoding::percent_decode_str(raw).decode_utf8_lossy();
        validate_session_id(&id)?;
    }
    // Auth has already installed a nonempty Identity (the Go X-User-ID);
    // requireUserID cannot fail after authentication. Only checkout POST reads
    // a body, after proxyCloudRuntime's enabled check. Portal POST does not.
    if !config.enabled() {
        return Err(cloud_result(config));
    }
    if path == "/api/cloud-billing/checkout-sessions" {
        read_json_body(request.into_body(), MAX_BODY).await?;
    }
    Err(cloud_result(config))
}

fn validate_session_id(id: &str) -> Result<(), Fault> {
    if id.is_empty() {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "session_id is required",
        ));
    }
    if !id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "invalid session_id",
        ));
    }
    Ok(())
}

fn check_body(data: &[u8], limit: usize) -> Result<(), Fault> {
    if data.len() > limit {
        return Err(Fault::handler(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body is too large",
        ));
    }
    if std::str::from_utf8(data).is_ok_and(|s| s.trim().is_empty()) {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "request body is required",
        ));
    }
    serde_json::from_slice::<Box<serde_json::value::RawValue>>(data)
        .map_err(|_| Fault::handler(StatusCode::BAD_REQUEST, "invalid request body"))?;
    Ok(())
}

async fn read_json_body(body: Body, limit: usize) -> Result<Vec<u8>, Fault> {
    let mut stream = body.into_data_stream();
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|_| Fault::handler(StatusCode::BAD_REQUEST, "invalid request body"))?;
        if chunk.len() > limit.saturating_sub(data.len()) {
            return Err(Fault::handler(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body is too large",
            ));
        }
        data.extend_from_slice(&chunk);
    }
    check_body(&data, limit)?;
    Ok(data)
}

fn idempotency_key<'a>(body: &'a str, header: &'a str, seat: bool) -> Result<&'a str, Fault> {
    let key = if body.trim().is_empty() {
        header.trim()
    } else {
        body.trim()
    };
    if key.is_empty() {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "Idempotency-Key or idempotency_key is required",
        ));
    }
    if key.len() > 255 {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "idempotency key must be at most 255 bytes",
        ));
    }
    if seat && key.len() > 200 {
        return Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "seat purchase idempotency key must be at most 200 bytes",
        ));
    }
    Ok(key)
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|c| c.is_ascii_alphabetic())
}
fn validate_interval(interval: &str) -> Result<(), Fault> {
    if interval == "month" || interval == "year" {
        Ok(())
    } else {
        Err(Fault::handler(
            StatusCode::BAD_REQUEST,
            "interval must be month or year",
        ))
    }
}

#[derive(Default)]
struct Checkout {
    interval: String,
    idempotency_key: String,
}
#[derive(Default)]
struct SeatPreview {
    additional_seats: i64,
}
#[derive(Default)]
struct SeatPurchase {
    additional_seats: i64,
    expected_current_seats: i64,
    expected_purchase_version: i64,
    accepted_proration_amount: i64,
    currency: String,
    idempotency_key: String,
}

// Go encoding/json matches struct tags case-insensitively, ignores unknown
// fields, and leaves primitive fields unchanged for JSON null.
macro_rules! go_object {
    ($name:ident { $($field:ident),* $(,)? }) => {
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct Object;
                impl<'de> Visitor<'de> for Object {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result { f.write_str("an object") }
                    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<$name, M::Error> {
                        let mut out = $name::default();
                        while let Some(key) = map.next_key::<String>()? {
                            let folded: String = key.chars().map(|c| match c { '\u{017f}' => 's', '\u{212a}' => 'k', c => c.to_ascii_lowercase() }).collect();
                            match folded.as_str() {
                                $(stringify!($field) => { if let Some(value) = map.next_value()? { out.$field = value; } },)*
                                _ => { map.next_value::<IgnoredAny>()?; }
                            }
                        }
                        Ok(out)
                    }
                }
                deserializer.deserialize_map(Object)
            }
        }
    };
}
go_object!(Checkout {
    interval,
    idempotency_key
});
go_object!(SeatPreview { additional_seats });
go_object!(SeatPurchase {
    additional_seats,
    expected_current_seats,
    expected_purchase_version,
    accepted_proration_amount,
    currency,
    idempotency_key
});

fn decode<T: for<'de> Deserialize<'de> + Default>(data: &[u8]) -> Result<T, Fault> {
    serde_json::from_slice::<Option<T>>(data)
        .map(|v| v.unwrap_or_default())
        .map_err(|_| Fault::handler(StatusCode::BAD_REQUEST, "invalid request body"))
}

fn query_value(uri: &axum::http::Uri, name: &str) -> Option<String> {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
        .filter(|s| !s.is_empty())
}

async fn workspace_guard(
    state: &ApiState,
    user: Uuid,
    actor: &Actor,
    selectors: &WorkspaceSelectors,
    uri: &axum::http::Uri,
    owner: bool,
) -> Result<(), Fault> {
    let not_found = || Fault::middleware(StatusCode::NOT_FOUND, "workspace not found");
    let workspace = if let Some(bound) = actor.workspace() {
        bound.to_string()
    } else if let Some(slug) = query_value(uri, "workspace_slug")
        .or_else(|| selectors.slug.clone().filter(|s| !s.is_empty()))
    {
        let db = state.pool.get().await.map_err(|_| not_found())?;
        db.query_opt("SELECT id FROM workspace WHERE slug = $1", &[&slug])
            .await
            .map_err(|_| not_found())?
            .ok_or_else(not_found)?
            .get::<_, Uuid>(0)
            .to_string()
    } else {
        query_value(uri, "workspace_id")
            .or_else(|| selectors.id.clone().filter(|s| !s.is_empty()))
            .unwrap_or_default()
    };
    if workspace.is_empty() {
        return Err(Fault::middleware(
            StatusCode::BAD_REQUEST,
            "workspace_id or workspace_slug is required",
        ));
    }
    actor.require_workspace_url(&workspace).map_err(|_| {
        Fault::middleware(
            StatusCode::FORBIDDEN,
            "task token is bound to a different workspace",
        )
    })?;
    let workspace_id = Uuid::parse_str(&workspace)
        .map_err(|_| Fault::middleware(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let db = state.pool.get().await.map_err(|_| not_found())?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE user_id = $1 AND workspace_id = $2",
            &[&user, &workspace_id],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    let role: String = row.get(0);
    if owner && role != "owner" && role != "admin" {
        return Err(Fault::middleware(
            StatusCode::FORBIDDEN,
            "insufficient permissions",
        ));
    }
    Ok(())
}

// Keep the human gate ahead of workspace resolution. The closure also makes
// the absence of any workspace lookup for machine actors testable without a DB.
async fn subscription_access<F, Fut>(actor: &Actor, resolve: F) -> Result<(), Fault>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), Fault>>,
{
    actor.require_human().map_err(|_| {
        Fault::handler(
            StatusCode::FORBIDDEN,
            "this endpoint is only available to human actors",
        )
    })?;
    resolve().await
}

async fn subscription(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(selectors): Extension<WorkspaceSelectors>,
    request: Request,
) -> Result<Response, Fault> {
    let path = request.uri().path().to_owned();
    let owner = request.method() == axum::http::Method::POST;
    let uri = request.uri().clone();
    subscription_access(&actor, || {
        workspace_guard(&state, user, &actor, &selectors, &uri, owner)
    })
    .await?;
    // Go's handler then checks the flag, followed by workspace ID/member/role
    // backstops. Those branches cannot fail after successful workspace_guard:
    // it already resolved a UUID, membership, and (for POST) owner/admin role.
    // Identity also makes the handler's requireUserID branch unreachable.
    let flags = &state.cloud_billing_flags;
    if !flags.enabled(FLAG, &user.to_string()) {
        return Err(Fault::disabled(
            "workspace_subscriptions_disabled",
            "workspace subscriptions are not enabled",
        ));
    }
    let config = &state.cloud_billing;
    let headers: HeaderMap = request.headers().clone();
    let header = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    match path.as_str() {
        "/api/cloud-subscriptions/checkout-sessions" => {
            let data = read_json_body(request.into_body(), MAX_BODY).await?;
            let input: Checkout = decode(&data)?;
            validate_interval(&input.interval)?;
            idempotency_key(&input.idempotency_key, header, false)?;
            let db = state.pool.get().await.map_err(|_| {
                Fault::handler(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to resolve checkout payer",
                )
            })?;
            let row = db
                .query_opt("SELECT email FROM \"user\" WHERE id = $1", &[&user])
                .await
                .map_err(|_| {
                    Fault::handler(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to resolve checkout payer",
                    )
                })?
                .ok_or_else(|| {
                    Fault::handler(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to resolve checkout payer",
                    )
                })?;
            let email: String = row.get(0);
            if email.trim().is_empty() {
                return Err(Fault::handler(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "checkout payer email is unavailable",
                ));
            }
        }
        "/api/cloud-subscriptions/seats/purchase-preview" => {
            let data = read_json_body(request.into_body(), MAX_BODY).await?;
            let input: SeatPreview = decode(&data)?;
            if input.additional_seats < 1 {
                return Err(Fault::handler(
                    StatusCode::BAD_REQUEST,
                    "additional_seats must be positive",
                ));
            }
        }
        "/api/cloud-subscriptions/seats/purchases" => {
            let data = read_json_body(request.into_body(), MAX_BODY).await?;
            let input: SeatPurchase = decode(&data)?;
            if input.additional_seats < 1
                || input.expected_current_seats < 1
                || input.expected_purchase_version < 1
                || input.accepted_proration_amount < 0
                || !valid_currency(&input.currency)
            {
                return Err(Fault::handler(
                    StatusCode::BAD_REQUEST,
                    "invalid seat purchase confirmation",
                ));
            }
            idempotency_key(&input.idempotency_key, header, true)?;
        }
        "/api/cloud-subscriptions/portal-sessions" => {
            idempotency_key("", header, false)?;
        }
        _ => {}
    }
    Err(cloud_result(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[test]
    fn currency_and_session_id() {
        for value in ["usd", "USD", "aBc"] {
            assert!(valid_currency(value));
        }
        for value in ["", "US", "USDD", "u1d", "éab"] {
            assert!(!valid_currency(value));
        }
        assert!(validate_session_id("cs_abc123").is_ok());
        assert_eq!(
            validate_session_id("").unwrap_err().message,
            "session_id is required"
        );
        for value in ["x-y", "x/y", "é"] {
            assert_eq!(
                validate_session_id(value).unwrap_err().message,
                "invalid session_id"
            );
        }
    }

    #[test]
    fn idempotency_and_interval_order() {
        assert_eq!(
            idempotency_key("  body  ", "header", false).unwrap(),
            "body"
        );
        assert_eq!(idempotency_key(" ", " header ", false).unwrap(), "header");
        assert_eq!(
            idempotency_key("", "", true).unwrap_err().message,
            "Idempotency-Key or idempotency_key is required"
        );
        assert_eq!(
            idempotency_key(&"a".repeat(256), "", true)
                .unwrap_err()
                .message,
            "idempotency key must be at most 255 bytes"
        );
        assert_eq!(
            idempotency_key(&"a".repeat(201), "", true)
                .unwrap_err()
                .message,
            "seat purchase idempotency key must be at most 200 bytes"
        );
        assert!(validate_interval("month").is_ok() && validate_interval("year").is_ok());
        assert_eq!(
            validate_interval("Month").unwrap_err().message,
            "interval must be month or year"
        );
    }

    #[tokio::test]
    async fn body_and_go_object_decoding() {
        for value in ["", " \t\n", "\u{2003}"] {
            assert_eq!(
                read_json_body(Body::from(value), 10)
                    .await
                    .unwrap_err()
                    .message,
                "request body is required"
            );
        }
        assert_eq!(
            read_json_body(Body::from("12345"), 4)
                .await
                .unwrap_err()
                .status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            read_json_body(Body::from("{"), 10)
                .await
                .unwrap_err()
                .message,
            "invalid request body"
        );
        assert!(
            decode::<Checkout>(br#"{"INTERVAL":"month","idempotency_key":null,"ignored":true}"#)
                .is_ok()
        );
        assert!(decode::<Checkout>(br#"{"interval":1}"#).is_err());
        assert!(decode::<Checkout>(b"null").is_ok());
    }

    #[test]
    fn flag_values_and_rollout() {
        assert_eq!(flag_key_to_env("..billing--new.flag_"), "BILLING_NEW_FLAG");
        for value in ["true", "ON", "1", " yes ", "variant"] {
            assert!(flag_value(value, FLAG, "user"));
        }
        for value in ["", "false", "OFF", "0", "no", "x%", "-1%", "101%"] {
            assert!(!flag_value(value, FLAG, "user"));
        }
        assert!(!flag_value("0%", FLAG, "user"));
        assert!(flag_value("100%", FLAG, "user"));
        let b = bucket(FLAG, "user");
        assert_eq!(b, 42); // FNV-1a 32-bit over billing_workspace_subscriptions\0user.
        assert_eq!(b, bucket(FLAG, "user"));
        assert_eq!(flag_key_to_env("__a...9-βZ___"), "A_9_Z");
        assert!(flag_value(" +100 % ", FLAG, "user"));
        assert!(!flag_value(" -0% ", FLAG, "user"));
        assert!(!flag_value(" 101% ", FLAG, "user"));
        assert!(flag_value("unknown_variant", FLAG, "user"));
        assert!(!flag_value(&format!("{b}%"), FLAG, "user"));
        assert!(flag_value(&format!("{}%", b + 1), FLAG, "user"));
    }

    #[tokio::test]
    async fn subscription_rejects_machine_before_workspace_resolution() {
        for actor in [
            Actor::Cloud,
            Actor::Task(crate::api::actor::TaskIdentity {
                user: Uuid::nil(),
                agent: Uuid::nil(),
                task: Uuid::nil(),
                workspace: Uuid::nil(),
            }),
        ] {
            let resolved = std::cell::Cell::new(false);
            let error = subscription_access(&actor, || async {
                resolved.set(true);
                Ok(())
            })
            .await
            .unwrap_err();
            assert!(!resolved.get());
            assert_eq!(error.status, StatusCode::FORBIDDEN);
            assert_eq!(
                error.message,
                "this endpoint is only available to human actors"
            );
        }
    }

    #[tokio::test]
    async fn subscription_human_reaches_workspace_resolution() {
        let resolved = std::cell::Cell::new(false);
        subscription_access(&Actor::Human, || async {
            resolved.set(true);
            Ok(())
        })
        .await
        .unwrap();
        assert!(resolved.get());
    }

    #[tokio::test]
    async fn feature_error_and_human_gate() {
        let response = cloud_result(&CloudRuntimeConfig::default()).into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(&to_bytes(response.into_body(), 1024).await.unwrap()[..], b"{\"error\":\"cloud runtime is not configured\",\"code\":\"cloud_runtime_not_configured\"}\n");
        let misconfigured = cloud_result(&CloudRuntimeConfig {
            cloud_url: "not-a-url".into(),
        })
        .into_response();
        assert_eq!(misconfigured.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            &to_bytes(misconfigured.into_body(), 1024).await.unwrap()[..],
            b"{\"error\":\"cloud runtime is misconfigured\",\"code\":\"cloud_runtime_misconfigured\"}\n"
        );
        for url in ["/", "  //  ", "   "] {
            let disabled = cloud_result(&CloudRuntimeConfig {
                cloud_url: url.into(),
            })
            .into_response();
            assert_eq!(disabled.status(), StatusCode::FORBIDDEN);
        }
        assert_eq!(
            cloud_result(&CloudRuntimeConfig {
                cloud_url: "https://example.invalid/".into()
            })
            .status,
            StatusCode::BAD_GATEWAY
        );
        let error = Actor::Cloud.require_human().unwrap_err().into_response();
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            &to_bytes(error.into_body(), 1024).await.unwrap()[..],
            b"{\"error\":\"this endpoint is only available to human actors\"}\n"
        );
    }
}
