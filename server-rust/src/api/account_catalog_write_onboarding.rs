//! User-scoped onboarding writes and post-commit, metrics-only transitions.
use super::account_catalog_write_profile::{USER_COLUMNS, user_response};
use super::{ApiError, ApiState, GoJson, Identity, User, error};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, State, rejection::BytesRejection},
    http::{HeaderMap, StatusCode},
    routing::{patch, post},
};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::Value;
use std::fmt;
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/me/onboarding",
            patch(patch_onboarding).layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/api/me/onboarding/complete", post(complete))
}

/// The frozen Go server sends these events to Prometheus only. Keep all
/// user-supplied answers, IDs, email, reason and timestamps out of the adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnboardingMetric {
    Started { platform: Platform },
    QuestionnaireSubmitted,
    SourceSubmitted,
    Completed { path: CompletionPath },
    CloudWaitlistJoined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Server,
    Web,
    Desktop,
    Cli,
    Mobile,
    Ios,
    Unknown,
}

impl Platform {
    fn from_header(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "server" => Self::Server,
            "web" => Self::Web,
            "desktop" => Self::Desktop,
            "cli" => Self::Cli,
            "mobile" => Self::Mobile,
            "ios" => Self::Ios,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Web => "web",
            Self::Desktop => "desktop",
            Self::Cli => "cli",
            Self::Mobile => "mobile",
            Self::Ios => "ios",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionPath {
    Full,
    RuntimeSkipped,
    CloudWaitlist,
    SkipExisting,
    InviteAccept,
    Unknown,
}

impl CompletionPath {
    pub(super) fn from_request(value: &str) -> Self {
        match value {
            "full" => Self::Full,
            "runtime_skipped" => Self::RuntimeSkipped,
            "cloud_waitlist" => Self::CloudWaitlist,
            "skip_existing" => Self::SkipExisting,
            "invite_accept" => Self::InviteAccept,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::RuntimeSkipped => "runtime_skipped",
            Self::CloudWaitlist => "cloud_waitlist",
            Self::SkipExisting => "skip_existing",
            Self::InviteAccept => "invite_accept",
            Self::Unknown => "unknown",
        }
    }
}

impl OnboardingMetric {
    pub fn counter_name(self) -> &'static str {
        match self {
            Self::Started { .. } => "multica_onboarding_started_total",
            Self::QuestionnaireSubmitted => "multica_onboarding_questionnaire_submitted_total",
            Self::SourceSubmitted => "multica_onboarding_source_submitted_total",
            Self::Completed { .. } => "multica_onboarding_completed_total",
            Self::CloudWaitlistJoined => "multica_cloud_waitlist_joined_total",
        }
    }

    pub fn labels(self) -> Vec<(&'static str, &'static str)> {
        match self {
            Self::Started { platform } => vec![("platform", platform.as_str())],
            Self::Completed { path } => vec![("completion_path", path.as_str())],
            _ => vec![],
        }
    }
}

/// Integrator installs a real Prometheus counter adapter; no second event bus
/// or PostHog capture. A test recorder is not production delivery parity.
pub trait OnboardingMetrics: Send + Sync {
    fn record<'a>(
        &'a self,
        metric: OnboardingMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) async fn record(state: &ApiState, events: Vec<OnboardingMetric>) {
    if let Some(metrics) = &state.onboarding_metrics {
        for event in events {
            metrics.record(event).await;
        }
    }
}

#[derive(Default)]
struct PatchRequest {
    questionnaire: Option<Value>,
}

#[derive(Default)]
struct CompleteRequest {
    completion_path: String,
    workspace_id: String,
}

impl<'de> Deserialize<'de> for PatchRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = PatchRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an onboarding request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(PatchRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = PatchRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if super::account_catalog_write_profile::decode::matches_go_field(
                        &key,
                        "questionnaire",
                    ) {
                        req.questionnaire = map.next_value()?;
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(req)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

impl<'de> Deserialize<'de> for CompleteRequest {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> Visitor<'de> for Fields {
            type Value = CompleteRequest;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an onboarding completion request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(CompleteRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut req = CompleteRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if super::account_catalog_write_profile::decode::matches_go_field(
                        &key,
                        "completion_path",
                    ) {
                        if let Some(value) = map.next_value()? {
                            req.completion_path = value;
                        }
                    } else if super::account_catalog_write_profile::decode::matches_go_field(
                        &key,
                        "workspace_id",
                    ) {
                        if let Some(value) = map.next_value()? {
                            req.workspace_id = value;
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(req)
            }
        }
        decoder.deserialize_any(Fields)
    }
}

// Go's decoder accepts a top-level null and reads one JSON value, not a whole stream.
fn decode<T: serde::de::DeserializeOwned + Default>(
    body: &[u8],
    optional: bool,
) -> Result<T, ApiError> {
    let mut stream = serde_json::Deserializer::from_slice(body).into_iter::<Option<T>>();
    match stream.next() {
        Some(Ok(value)) => Ok(value.unwrap_or_default()),
        None if optional => Ok(T::default()),
        _ => Err(error(StatusCode::BAD_REQUEST, "invalid request body")),
    }
}

fn string_or_slice<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Answer {
        Many(Vec<Option<String>>),
        One(String),
    }
    Ok(match Option::<Answer>::deserialize(d)? {
        None => Vec::new(),
        Some(Answer::Many(values)) => values.into_iter().map(Option::unwrap_or_default).collect(),
        Some(Answer::One(s)) if s.is_empty() => Vec::new(),
        Some(Answer::One(s)) => vec![s],
    })
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Answers {
    #[serde(deserialize_with = "string_or_slice")]
    source: Vec<String>,
    #[serde(deserialize_with = "string_or_slice")]
    use_case: Vec<String>,
    #[serde(deserialize_with = "super::account_catalog_write_workflows::decode::null_default")]
    role: String,
    #[serde(deserialize_with = "super::account_catalog_write_workflows::decode::null_default")]
    source_skipped: bool,
    #[serde(deserialize_with = "super::account_catalog_write_workflows::decode::null_default")]
    role_skipped: bool,
    #[serde(deserialize_with = "super::account_catalog_write_workflows::decode::null_default")]
    use_case_skipped: bool,
    #[serde(deserialize_with = "super::account_catalog_write_workflows::decode::null_default")]
    version: i64,
}
impl Answers {
    fn complete(&self) -> bool {
        self.version == 2
            && (!self.role.is_empty() || self.role_skipped)
            && (!self.use_case.is_empty() || self.use_case_skipped)
    }
    fn source_resolved(&self) -> bool {
        !self.source.is_empty() || self.source_skipped
    }
}

fn transitions(
    before: Value,
    after: Value,
    supplied: bool,
    platform: String,
) -> Vec<OnboardingMetric> {
    let first = before.is_null() || before.as_object().is_some_and(|o| o.is_empty());
    let nonempty = after.as_object().is_none_or(|o| !o.is_empty());
    let before: Answers = serde_json::from_value(before).unwrap_or_default();
    let after: Answers = serde_json::from_value(after).unwrap_or_default();
    let mut events = Vec::new();
    if first && supplied && nonempty {
        events.push(OnboardingMetric::Started {
            platform: Platform::from_header(&platform),
        });
    }
    if after.complete() && !before.complete() {
        events.push(OnboardingMetric::QuestionnaireSubmitted);
    }
    if after.version == 2 && after.source_resolved() && !before.source_resolved() {
        events.push(OnboardingMetric::SourceSubmitted);
    }
    events
}

async fn patch_onboarding(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<GoJson<User>, ApiError> {
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: PatchRequest = decode(&body, false)?;
    let failure = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update onboarding",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| failure())?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    // Serialize transitions on the account row so concurrent retries do not double-count.
    let before = tx
        .query_opt(
            "SELECT onboarding_questionnaire FROM \"user\" WHERE id=$1 FOR UPDATE",
            &[&user],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(failure)?;
    let row = tx.query_one(&format!("UPDATE \"user\" SET onboarding_questionnaire=COALESCE($2,onboarding_questionnaire), updated_at=now() WHERE id=$1 RETURNING {USER_COLUMNS}"), &[&user, &req.questionnaire]).await.map_err(|_| failure())?;
    let events = transitions(
        before.get(0),
        row.get("onboarding_questionnaire"),
        req.questionnaire.is_some(),
        headers
            .get("x-client-platform")
            .and_then(|s| s.to_str().ok())
            .unwrap_or("")
            .to_owned(),
    );
    let result = user_response(&row);
    tx.commit().await.map_err(|_| failure())?;
    drop(db);
    record(&state, events).await;
    Ok(GoJson(result))
}

async fn complete(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    body: Result<Bytes, BytesRejection>,
) -> Result<GoJson<User>, ApiError> {
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: CompleteRequest = decode(&body, true)?;
    if !req.workspace_id.is_empty() {
        Uuid::parse_str(&req.workspace_id)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    }
    let failure = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to complete onboarding",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| failure())?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    let before = tx
        .query_opt(
            "SELECT onboarded_at IS NULL FROM \"user\" WHERE id=$1 FOR UPDATE",
            &[&user],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(failure)?;
    let row = tx.query_one(&format!("UPDATE \"user\" SET onboarded_at=COALESCE(onboarded_at,now()), updated_at=now() WHERE id=$1 RETURNING {USER_COLUMNS}"), &[&user]).await.map_err(|_| failure())?;
    let result = user_response(&row);
    let events = if before.get::<_, bool>(0) {
        vec![OnboardingMetric::Completed {
            path: CompletionPath::from_request(&req.completion_path),
        }]
    } else {
        vec![]
    };
    tx.commit().await.map_err(|_| failure())?;
    drop(db);
    record(&state, events).await;
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn requests_accept_first_value_folded_duplicates_and_null_like_go() {
        let patch: PatchRequest = decode(
            r#"{"QUESTIONNAIRE":{"version":2},"questionnaire":null,"Queſtionnaire":{"version":3}} trailing"#.as_bytes(),
            false,
        ).unwrap();
        assert_eq!(patch.questionnaire, Some(json!({"version":3})));
        let empty: PatchRequest = decode(
            br#"{"questionnaire":{"version":2},"QUESTIONNAIRE":null}"#,
            false,
        )
        .unwrap();
        assert!(empty.questionnaire.is_none());
        let complete: CompleteRequest = decode(
            br#"{"completion_path":"full","COMPLETION_PATH":null,"WORKSPACE_ID":"00000000-0000-0000-0000-000000000001","workspace_id":null} trailing"#,
            true,
        ).unwrap();
        assert_eq!(complete.completion_path, "full");
        assert_eq!(
            complete.workspace_id,
            "00000000-0000-0000-0000-000000000001"
        );
        let reset: CompleteRequest = decode(
            br#"{"completion_path":"full","COMPLETION_PATH":"invite_accept"}"#,
            true,
        )
        .unwrap();
        assert_eq!(reset.completion_path, "invite_accept");
        let empty: CompleteRequest = decode(b"", true).unwrap();
        assert_eq!(empty.completion_path, "");
        for invalid in [br#"{"completion_path":4}"#.as_slice(), b"[]", b"{"] {
            assert!(decode::<CompleteRequest>(invalid, true).is_err());
        }
    }

    #[test]
    fn metric_names_labels_and_normalization_match_go() {
        for (metric, counter, labels) in [
            (
                OnboardingMetric::Started {
                    platform: Platform::Web,
                },
                "multica_onboarding_started_total",
                vec![("platform", "web")],
            ),
            (
                OnboardingMetric::QuestionnaireSubmitted,
                "multica_onboarding_questionnaire_submitted_total",
                vec![],
            ),
            (
                OnboardingMetric::SourceSubmitted,
                "multica_onboarding_source_submitted_total",
                vec![],
            ),
            (
                OnboardingMetric::Completed {
                    path: CompletionPath::InviteAccept,
                },
                "multica_onboarding_completed_total",
                vec![("completion_path", "invite_accept")],
            ),
            (
                OnboardingMetric::CloudWaitlistJoined,
                "multica_cloud_waitlist_joined_total",
                vec![],
            ),
        ] {
            assert_eq!(metric.counter_name(), counter);
            assert_eq!(metric.labels(), labels);
        }
        for platform in [
            "server", "web", "desktop", "cli", "mobile", "ios", "unknown",
        ] {
            assert_eq!(Platform::from_header(platform).as_str(), platform);
        }
        assert_eq!(Platform::from_header(" WEB "), Platform::Web);
        for input in ["", "android", "custom-user-private-build", "web,cli"] {
            assert_eq!(Platform::from_header(input), Platform::Unknown);
        }
        for path in [
            "full",
            "runtime_skipped",
            "cloud_waitlist",
            "skip_existing",
            "invite_accept",
            "unknown",
        ] {
            assert_eq!(CompletionPath::from_request(path).as_str(), path);
        }
        // Go's handler validates exact path BEFORE metrics label normalization.
        for path in [" FULL ", "runtime_Skipped", "private-reason", ""] {
            assert_eq!(CompletionPath::from_request(path), CompletionPath::Unknown);
        }
    }

    #[test]
    fn questionnaire_answers_never_reach_metrics() {
        let after = json!({"version":2,"role":"private-role","use_case":["private-use"],"source":["private-source"],"source_other":"private-email@example.test","role_other":"private-role-other","use_case_other":"private-use-other"});
        let metrics = transitions(json!({}), after, true, "private-device".into());
        assert_eq!(
            metrics,
            [
                OnboardingMetric::Started {
                    platform: Platform::Unknown
                },
                OnboardingMetric::QuestionnaireSubmitted,
                OnboardingMetric::SourceSubmitted,
            ]
        );
        assert!(!format!("{metrics:?}").contains("private"));
    }

    #[test]
    fn questionnaire_versions_legacy_answers_and_transitions() {
        let q = json!({"version":2,"role":"developer","use_case":"coding"});
        let events = transitions(json!({}), q.clone(), true, "web".into());
        assert!(matches!(
            &events[..],
            [
                OnboardingMetric::Started { .. },
                OnboardingMetric::QuestionnaireSubmitted
            ]
        ));
        assert!(transitions(q.clone(), q.clone(), true, String::new()).is_empty());
        let sourced =
            json!({"version":2,"role":"developer","use_case":["coding"],"source_skipped":true});
        assert!(matches!(
            &transitions(q, sourced, true, String::new())[..],
            [OnboardingMetric::SourceSubmitted]
        ));
        assert_eq!(transitions(json!({}), json!({"version":3,"role_skipped":true,"use_case_skipped":true,"source_skipped":true}), true, String::new()).len(), 1);
        assert!(transitions(json!({}), json!({}), false, String::new()).is_empty());
    }
}
