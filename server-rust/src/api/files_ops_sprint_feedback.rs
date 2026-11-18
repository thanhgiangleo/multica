//! Synthetic-testable feedback writes with durable rate limit and telemetry hook.
use super::{ApiError, ApiState, GoJson, Identity, error, timestamp};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, State},
    http::{HeaderMap, StatusCode, header},
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

const BODY_LIMIT: usize = 64 * 1024;
const MESSAGE_LIMIT: usize = 10_000;
const URL_LIMIT: usize = 2048;
const CLIENT_HEADER_LIMIT: usize = 512;
const HOURLY_LIMIT: i64 = 10;

/// The analytics event carries the trimmed, case-preserved category the client
/// sent, matching Go's `strings.TrimSpace(req.Kind)` handoff; `normalized_kind`
/// is the allow-list form the Prometheus label uses.
#[derive(Clone, Debug, PartialEq)]
pub struct FeedbackEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub workspace_id: Option<Uuid>,
    pub kind: String,
    pub message_length: usize,
    pub has_images: bool,
    pub platform: String,
    pub version: String,
}

/// Integrator connects this to the real analytics/metrics service. No event
/// is emitted before commit; caller must not configure a no-op production sink.
pub trait FeedbackEventSink: Send + Sync {
    fn record_feedback(&self, event: FeedbackEvent);
}
pub type SharedFeedbackEventSink = Arc<dyn FeedbackEventSink>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/feedback",
        post(create).layer(DefaultBodyLimit::max(BODY_LIMIT)),
    )
}

#[derive(Deserialize)]
struct FeedbackRequest {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    workspace_id: Option<String>,
    context: Option<FeedbackContext>,
}
#[derive(Deserialize)]
struct FeedbackContext {
    kind: Option<String>,
    trigger: Option<String>,
    error: Option<FeedbackErrorContext>,
}
#[derive(Deserialize)]
struct FeedbackErrorContext {
    name: Option<String>,
    message: Option<String>,
    stack: Option<String>,
}

impl FeedbackContext {
    /// Serialize with the Go struct's exact tags: `kind`/`trigger`/`error.name`/
    /// `error.message` are plain strings (an absent value is `""`, never null),
    /// and `stack` is `omitempty`, so an empty stack is dropped entirely.
    ///
    /// Only a context that passed `valid_context` reaches here, but spelling the
    /// shape out keeps the persisted metadata equal to Go's for any future
    /// caller of this conversion.
    fn to_metadata(&self) -> serde_json::Value {
        let mut error = serde_json::json!({
            "name": self.error.as_ref().and_then(|e| e.name.as_deref()).unwrap_or(""),
            "message": self.error.as_ref().and_then(|e| e.message.as_deref()).unwrap_or(""),
        });
        if let Some(stack) = self
            .error
            .as_ref()
            .and_then(|e| e.stack.as_deref())
            .filter(|stack| !stack.is_empty())
        {
            error["stack"] = serde_json::Value::from(stack);
        }
        serde_json::json!({
            "kind": self.kind.as_deref().unwrap_or(""),
            "trigger": self.trigger.as_deref().unwrap_or(""),
            "error": error,
        })
    }
}
#[derive(Serialize)]
struct FeedbackResponse {
    id: Uuid,
    created_at: String,
}

fn valid_context(context: &FeedbackContext) -> bool {
    fn present(value: Option<&String>) -> bool {
        value.is_some_and(|value| !value.trim().is_empty())
    }
    context.kind.as_deref() == Some("desktop_route_error")
        && present(context.trigger.as_ref())
        && context
            .error
            .as_ref()
            .is_some_and(|error| present(error.name.as_ref()) && present(error.message.as_ref()))
}

/// The analytics event keeps Go's `strings.TrimSpace(req.Kind)` handoff, so a
/// client-sent category survives with its original case for the analytics
/// property; the Prometheus label goes through `normalized_kind` instead.
fn event_kind(kind: Option<&str>) -> String {
    match kind.map(str::trim) {
        None | Some("") => "general".to_owned(),
        Some(kind) => kind.to_owned(),
    }
}

/// Mirrors `metrics.NormalizeFeedbackKind`: lowercase, trim, then a closed
/// allow-list with an `other` fallback. Case-insensitive so `Bug` counts as
/// `bug` rather than being collapsed away with genuinely unknown values.
/// The Prometheus label path (shared wiring, ZBSI-63) must call this on the
/// event's `kind`; a raw free-form value never reaches a label.
pub fn normalized_kind(kind: &str) -> String {
    match kind.trim().to_ascii_lowercase().as_str() {
        "bug" => "bug",
        "feature" => "feature",
        "general" => "general",
        "praise" => "praise",
        _ => "other",
    }
    .to_owned()
}

/// Mirrors `metrics.NormalizePlatform`, which the feedback counter applies to
/// the X-Client-Platform header. The header is client-controlled and therefore
/// unbounded, so the Prometheus label uses this closed set with an `unknown`
/// fallback while the analytics event keeps the raw value.
pub fn normalized_platform(platform: &str) -> String {
    match platform.trim().to_ascii_lowercase().as_str() {
        "server" => "server",
        "web" => "web",
        "desktop" => "desktop",
        "cli" => "cli",
        "mobile" => "mobile",
        "ios" => "ios",
        _ => "unknown",
    }
    .to_owned()
}
fn has_images(message: &str) -> bool {
    static IMAGE: OnceLock<regex::Regex> = OnceLock::new();
    IMAGE
        .get_or_init(|| regex::Regex::new(r"!\[[^\]]*\]\([^)]+\)").expect("valid image expression"))
        .is_match(message)
}
fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some()
        || value.len() > CLIENT_HEADER_LIMIT
        || value.chars().any(char::is_control)
    {
        return None;
    }
    Some(value.to_owned())
}

fn safe_route(raw: &str) -> Option<&str> {
    // Feedback route URLs may carry auth codes or private search parameters.
    // Never persist the query or fragment in the durable analytics metadata.
    if raw.len() > URL_LIMIT || raw.chars().any(char::is_control) {
        return None;
    }
    Some(raw.split(['?', '#']).next().unwrap_or(""))
}

async fn create(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    headers: HeaderMap,
    // Go decodes with `json.NewDecoder(r.Body)` and ignores Content-Type, so a
    // missing or unusual media type must not become axum's 415. Reading raw
    // bytes keeps the port's contract: any unparseable body is a 400.
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<FeedbackResponse>), ApiError> {
    // The synthetic checkout has no analytics service wired yet; without a sink
    // the endpoint fails closed rather than returning 201 with telemetry lost.
    let sink = state.files_feedback.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "feedback telemetry unavailable",
        )
    })?;
    // A 64 KiB cap is enforced by DefaultBodyLimit below; an over-limit or
    // unreadable body is Go's "invalid request body", not axum's 413/400 pair.
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let request: FeedbackRequest = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let message = request.message.as_deref().unwrap_or("").trim();
    if message.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "message is required"));
    }
    if message.len() > MESSAGE_LIMIT {
        return Err(error(StatusCode::BAD_REQUEST, "message too long"));
    }
    if request
        .context
        .as_ref()
        .is_some_and(|context| !valid_context(context))
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid feedback context"));
    }
    let route = safe_route(request.url.as_deref().unwrap_or(""))
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid feedback url"))?;
    for name in [
        "x-client-platform",
        "x-client-version",
        "x-client-os",
        header::USER_AGENT.as_str(),
    ] {
        if headers.contains_key(name) && header_value(&headers, name).is_none() {
            return Err(error(StatusCode::BAD_REQUEST, "invalid client metadata"));
        }
    }
    let ws = request
        .workspace_id
        .as_deref()
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))
        })
        .transpose()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    // Serialize submissions across instances on the existing user row. A
    // count-then-insert without this lock permits concurrent rate-limit bypass.
    if tx
        .query_opt("SELECT id FROM \"user\" WHERE id=$1 FOR UPDATE", &[&user])
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
        .is_none()
    {
        return Err(error(StatusCode::UNAUTHORIZED, "user not found"));
    }
    if let Some(ws) = ws
        && tx
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR KEY SHARE",
                &[&ws, &user],
            )
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
            .is_none()
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "not a member of this workspace",
        ));
    }
    let count: i64 = tx.query_one(
        "SELECT count(*) FROM feedback WHERE user_id=$1 AND created_at > now()-interval '1 hour'", &[&user]
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check rate limit"))?.get(0);
    if count >= HOURLY_LIMIT {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many feedback submissions, please try again later",
        ));
    }
    let platform = header_value(&headers, "x-client-platform").unwrap_or_default();
    let version = header_value(&headers, "x-client-version").unwrap_or_default();
    let os = header_value(&headers, "x-client-os").unwrap_or_default();
    let mut metadata = serde_json::json!({
        "url": route,
        "platform": platform,
        "version": version,
        "os": os,
        "user_agent": header_value(&headers, header::USER_AGENT.as_str()).unwrap_or_default(),
    });
    // Go writes the key only when the client sent a context, so consumers can
    // tell "no diagnostic context" from "context with empty fields".
    if let Some(context) = request.context.as_ref() {
        metadata["context"] = context.to_metadata();
    }
    let row = tx.query_one(
        "INSERT INTO feedback(user_id,workspace_id,message,metadata) VALUES ($1,$2,$3,$4) RETURNING id,created_at",
        &[&user, &ws, &message, &metadata],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to submit feedback"))?;
    let id: Uuid = row.get(0);
    let created_at: DateTime<Utc> = row.get(1);
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to submit feedback",
        )
    })?;
    sink.record_feedback(FeedbackEvent {
        id,
        user_id: user,
        workspace_id: ws,
        kind: event_kind(request.kind.as_deref()),
        message_length: message.len(),
        has_images: has_images(message),
        platform,
        version,
    });
    Ok((
        StatusCode::CREATED,
        GoJson(FeedbackResponse {
            id,
            created_at: timestamp(created_at),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn feedback_validation_and_event_normalization() {
        assert_eq!(
            safe_route("https://example.invalid/path?token=private#frag"),
            Some("https://example.invalid/path")
        );
        assert_eq!(safe_route("/route#section?private"), Some("/route"));
        assert_eq!(safe_route("/route\ncredential"), None);
        assert_eq!(safe_route(&"x".repeat(URL_LIMIT + 1)), None);
        let mut headers = HeaderMap::new();
        headers.append("x-client-platform", HeaderValue::from_static("desktop"));
        assert_eq!(
            header_value(&headers, "x-client-platform").as_deref(),
            Some("desktop")
        );
        headers.append("x-client-platform", HeaderValue::from_static("forged"));
        assert_eq!(header_value(&headers, "x-client-platform"), None);
        assert!(!has_images("text ![not an image]"));
        assert!(has_images(
            "text ![caption](https://invalid.example/image.png)"
        ));
        assert_eq!(normalized_kind(""), "other");
        assert_eq!(normalized_kind("unknown"), "other");
        // Go lowercases before consulting the allow-list, so mixed case is the
        // documented category rather than an unknown one.
        assert_eq!(normalized_kind(" Bug "), "bug");
        assert_eq!(normalized_kind("FEATURE"), "feature");
        assert_eq!(normalized_kind("Praise"), "praise");
        assert_eq!(normalized_platform(" DESKTOP "), "desktop");
        assert_eq!(normalized_platform("unknown-header-value"), "unknown");
        assert_eq!(normalized_platform(""), "unknown");
        // The event property keeps the client's spelling; only the metric label
        // is normalized. Go falls back to "general" only for a missing/blank
        // field.
        assert_eq!(event_kind(None), "general");
        assert_eq!(event_kind(Some("   ")), "general");
        assert_eq!(event_kind(Some(" Bug ")), "Bug");
        assert_eq!(event_kind(Some("arbitrary")), "arbitrary");
        assert!(valid_context(&FeedbackContext {
            kind: Some("desktop_route_error".into()),
            trigger: Some("route".into()),
            error: Some(FeedbackErrorContext {
                name: Some("TypeError".into()),
                message: Some("synthetic".into()),
                stack: None,
            }),
        }));
        // Go decodes into value structs, so an explicit JSON null or a missing
        // key both fail `validFeedbackContext` — never a panic and never a 500.
        assert!(!valid_context(&FeedbackContext {
            kind: Some("desktop_route_error".into()),
            trigger: Some("route".into()),
            error: None,
        }));
        assert!(!valid_context(&FeedbackContext {
            kind: Some("desktop_route_error".into()),
            trigger: Some("route".into()),
            error: Some(FeedbackErrorContext {
                name: Some("TypeError".into()),
                message: Some(" \n".into()),
                stack: None,
            }),
        }));
        assert!(!valid_context(&FeedbackContext {
            kind: None,
            trigger: Some("route".into()),
            error: Some(FeedbackErrorContext {
                name: Some("TypeError".into()),
                message: Some("synthetic".into()),
                stack: None,
            }),
        }));
    }

    #[test]
    fn request_body_tolerates_null_and_absent_fields() {
        // Go's json.Decoder accepts `null` for a struct field and absent keys
        // alike, so both must reach validation instead of a deserialize error.
        let request: FeedbackRequest =
            serde_json::from_slice(br#"{"message":null,"url":null,"kind":null,"context":null}"#)
                .expect("null fields decode");
        assert!(request.message.is_none());
        assert!(request.url.is_none());
        assert!(request.context.is_none());
        assert_eq!(event_kind(request.kind.as_deref()), "general");
        // A present but non-object context is Go's decode failure = 400.
        assert!(
            serde_json::from_slice::<FeedbackRequest>(br#"{"message":"x","context":"nope"}"#)
                .is_err()
        );
        // Numbers and booleans for string fields also fail the decode.
        assert!(serde_json::from_slice::<FeedbackRequest>(br#"{"message":7}"#).is_err());
    }
}
