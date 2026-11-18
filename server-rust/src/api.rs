//! Staged-Go authenticated read slices. No fallback to a Go service or fixture data.
mod account;
mod account_catalog_write;
pub mod account_catalog_write_avatar;
pub mod account_catalog_write_catalog_publish;
mod account_catalog_write_client_usage;
mod account_catalog_write_invitation_admission;
pub mod account_catalog_write_invitation_effects;
mod account_catalog_write_invitations;
mod account_catalog_write_labels;
mod account_catalog_write_members;
pub mod account_catalog_write_onboarding;
mod account_catalog_write_profile;
mod account_catalog_write_projects;
mod account_catalog_write_properties;
mod account_catalog_write_resources;
pub mod account_catalog_write_runtime_profiles;
pub mod account_catalog_write_share_links;
mod account_catalog_write_statuses;
mod account_catalog_write_views;
pub mod account_catalog_write_workflow_publish;
pub mod account_catalog_write_workflow_remap;
mod account_catalog_write_workflows;
mod account_catalog_write_workspace_delete;
pub mod account_catalog_write_workspace_effects;
mod account_catalog_write_workspaces;
mod actor;
mod agent_activity;
pub mod agents_squads_sprint;
mod archived_facets;
mod archived_page;
mod assignee_frequency;
mod automation_sprint;
pub mod files_ops_sprint;
pub use automation_sprint::{
    AutopilotQuotaPolicy, AutopilotQuotaPolicyError, AutopilotQuotaPolicySource, DispatchFailure,
    DurableLifecycleEvents, DurablePauseEvents, FailureMonitor, FailureMonitorConfig,
    FailureMonitorPage, ManualDispatchFailure, ManualDispatcher, ManualJob, PauseNotice,
    PauseTransition, QuotaRecoveryFailure, QuotaRecoveryPage, ReconcileFailure, ReconcilePage,
    RunLifecycle, RunTransition, ScheduleDispatcher, ScheduleJob, ScheduleWorker,
    WebhookDispatcher, WebhookJob, WebhookWorker,
};
pub mod agent_invoke;
pub mod agent_ready;
pub mod attribution;
pub mod attribution_db;
mod autopilot_history_read;
mod catalog;
pub mod channels_sprint;
pub mod channels_sprint_binding;
pub mod channels_sprint_dingtalk;
mod channels_sprint_groups;
pub mod channels_sprint_guest;
pub mod channels_sprint_ingress;
pub mod channels_sprint_lark;
pub mod channels_sprint_lark_install;
pub mod channels_sprint_notify;
pub mod channels_sprint_notify_event;
pub mod channels_sprint_notify_targets;
pub mod channels_sprint_pair;
pub mod channels_sprint_slack;
pub mod channels_sprint_telegram;
pub mod channels_sprint_wecom;
pub mod channels_sprint_zalo;
mod chat_draft_restore;
mod chat_pending;
mod chat_pin;
mod child_progress;
mod cloud;
pub mod cloud_billing;
pub mod comment_dispatch;
pub mod comment_enqueue;
pub mod comment_mention;
pub mod comment_routing;
pub mod comment_routing_db;
mod daemon;
mod daemon_claim_build;
mod daemon_claim_check;
mod daemon_claim_compose;
mod daemon_claim_join;
mod daemon_claim_load;
mod daemon_claim_pure;
mod daemon_claim_response;
mod daemon_claim_service;
mod dashboard_agents;
mod dashboard_daily;
mod extensions_sprint;
mod extensions_sprint_install;
mod extensions_sprint_lifecycle;
mod extensions_sprint_packages;
mod extensions_sprint_public;
mod extensions_sprint_token;
mod inbox;
mod issue_comment_runtime;
mod issue_event_bridge;
mod issue_read;
mod issue_relations_read;
mod issue_system_wakeup;
mod issue_view;
mod issue_wakeup;
mod issue_workflow;
pub mod issue_write;
mod issue_write_batch;
mod issue_write_comment;
mod issue_write_comment_decode;
mod issue_write_comment_mutate;
mod issue_write_create;
mod issue_write_create_wakeup;
mod issue_write_delete;
mod issue_write_issue_reaction;
mod issue_write_labels;
mod issue_write_manage;
mod issue_write_move;
mod issue_write_properties;
mod issue_write_pull_request;
mod issue_write_quick_create;
mod issue_write_reaction;
mod issue_write_resolution;
mod issue_write_structure;
mod issue_write_subscriber;
mod issue_write_update;
mod issue_write_value;
mod issue_write_wakeup;
mod p0_agents;
mod p0_auth;
mod p0_chat;
mod p0_daemon;
mod p0_daemon_claim;
pub mod p0_daemon_outcomes;
mod p0_daemon_registration;
mod p0_daemon_unconfigured;
mod p0_daemon_ws;
mod p0_task_events;
mod p0_tasks;
mod p0_trigger_preview;
mod paused_wakeup;
mod personal_read;
pub mod queries_sprint_attachments;
mod queries_sprint_comments;
pub mod queries_sprint_inbox;
mod queries_sprint_issue_detail;
mod queries_sprint_issue_list;
mod queries_sprint_issue_search;
pub mod queries_sprint_pins;
pub mod queries_sprint_pull_requests;
mod queries_sprint_quick_action_writes;
mod queries_sprint_search;
mod queries_sprint_search_shapes;
mod queries_sprint_source_context;
mod queries_sprint_table_rows;
mod queries_sprint_task_snapshot;
pub mod queries_sprint_timeline;
mod quick_action;
mod runtime;
mod runtime_chat;
mod runtime_chat_archive;
mod runtime_chat_daemon;
mod runtime_chat_delivery;
mod runtime_chat_events;
mod runtime_chat_pinned;
mod runtime_chat_queue;
mod runtime_chat_redact;
mod runtime_chat_send;
mod runtime_chat_start;
mod runtime_chat_stream;
mod runtime_chat_supplement;
mod runtime_chat_task;
mod runtime_chat_usage;
mod runtime_chat_ws;
pub mod runtime_skills_sprint;
mod runtime_skills_sprint_bundles;
pub mod runtime_skills_sprint_control;
pub mod runtime_skills_sprint_local;
pub mod runtime_skills_sprint_local_import;
pub mod runtime_skills_sprint_manage;
pub mod runtime_skills_sprint_models;
mod runtime_usage;
mod skill;
mod squad;
mod squad_evaluation;
mod system_wakeup;
mod wakeup_summary;
mod workflow;
pub mod workspace_integrations;
mod workspace_read;
mod workspace_wakeup;
use actor::Actor;
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header::SET_COOKIE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Local, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use hmac::{Hmac, Mac};
pub use issue_event_bridge::IssueWakeupDispatcher;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct ApiState {
    pool: Arc<Pool>,
    files_storage: Option<files_ops_sprint::SharedObjectStore>,
    files_events: Option<files_ops_sprint::SharedAttachmentEventSink>,
    files_feedback: Option<files_ops_sprint::SharedFeedbackEventSink>,
    files_public_url: Option<String>,
    realtime_metrics: Option<files_ops_sprint::SharedRealtimeMetrics>,
    realtime_token: Option<String>,
    events: runtime_chat_events::Events,
    jwt_secret: Arc<Vec<u8>>,
    session: SessionConfig,
    cloud: Option<cloud::CloudVerifier>,
    git_extra_ca_certs: Option<String>,
    issue_events: Option<issue_write::SharedIssueEventSink>,
    issue_comment_runtime: Option<issue_write::SharedIssueCommentRuntime>,
    issue_create_runtime: Option<issue_write::SharedIssueCreateRuntime>,
    issue_delete_runtime: Option<issue_write::SharedIssueDeleteRuntime>,
    issue_child_runtime: Option<issue_write::SharedIssueChildRuntime>,
    issue_pull_request_runtime: Option<issue_write::SharedIssuePullRequestRuntime>,
    inbox_events: Option<queries_sprint_inbox::SharedInboxEventSink>,
    pin_events: Option<queries_sprint_pins::SharedPinEventSink>,
    timeline_avatar_urls: Option<queries_sprint_timeline::SharedTimelineAvatarUrlPolicy>,
    attachment_urls: Option<queries_sprint_attachments::SharedAttachmentUrlPolicy>,
    pull_request_refresh: Option<queries_sprint_pull_requests::SharedPullRequestRefresh>,
    squad_events: Option<agents_squads_sprint::SharedSquadEventSink>,
    p0_agent_events: Option<agents_squads_sprint::SharedSquadEventSink>,
    task_cancel_events: Option<p0_task_events::SharedTaskCancelSink>,
    daemon_wake: Option<std::sync::Arc<p0_daemon_ws::Hub>>,
    skill_events: Option<runtime_skills_sprint::SharedSkillEventSink>,
    runtime_rpc_transport: Option<runtime_skills_sprint_control::SharedRuntimeRpcTransport>,
    runtime_metadata_events: Option<runtime_skills_sprint_manage::SharedRuntimeMetadataEventSink>,
    local_search_index: bool,
    invitation_admission: Arc<account_catalog_write_invitation_admission::Admission>,
    automation_sprint_limits: Arc<automation_sprint::IngressLimits>,
    automation_sprint_manual: Option<Arc<dyn automation_sprint::ManualDispatcher>>,
    automation_sprint_quota: Option<Arc<dyn automation_sprint::AutopilotQuotaPolicySource>>,
    channels: Option<channels_sprint::SharedChannelRuntime>,
    workflow_publisher: Option<Arc<dyn account_catalog_write_workflow_publish::WorkflowPublisher>>,
    invitation_mailer: Option<Arc<dyn account_catalog_write_invitation_effects::InvitationMailer>>,
    invitation_metrics:
        Option<Arc<dyn account_catalog_write_invitation_effects::InvitationMetrics>>,
    workspace_metrics: Option<Arc<dyn account_catalog_write_workspace_effects::WorkspaceMetrics>>,
    catalog_publisher: Option<Arc<dyn account_catalog_write_catalog_publish::CatalogPublisher>>,
    onboarding_metrics: Option<Arc<dyn account_catalog_write_onboarding::OnboardingMetrics>>,
    avatar_storage_urls: Option<Arc<dyn account_catalog_write_avatar::AvatarStorageUrls>>,
    extensions_sprint_enabled: bool,
    extensions_sprint_signing_key: Option<[u8; 32]>,
    extensions_sprint_config_key: Option<[u8; 32]>,
    integration_config: workspace_integrations::IntegrationConfig,
    cloud_billing: cloud_billing::CloudRuntimeConfig,
    cloud_billing_flags: cloud_billing::CloudBillingFlagProvider,
}

impl ApiState {
    pub fn new(pool: Arc<Pool>, jwt_secret: Vec<u8>) -> Self {
        Self {
            pool,
            files_storage: None,
            files_events: None,
            files_feedback: None,
            files_public_url: None,
            realtime_metrics: None,
            realtime_token: None,
            events: runtime_chat_events::Events::default(),
            jwt_secret: Arc::new(jwt_secret),
            session: SessionConfig::default(),
            cloud: None,
            git_extra_ca_certs: None,
            issue_events: None,
            issue_comment_runtime: None,
            issue_create_runtime: None,
            issue_delete_runtime: None,
            issue_child_runtime: None,
            issue_pull_request_runtime: None,
            inbox_events: None,
            pin_events: None,
            timeline_avatar_urls: None,
            attachment_urls: None,
            pull_request_refresh: None,
            squad_events: None,
            p0_agent_events: None,
            task_cancel_events: None,
            daemon_wake: None,
            skill_events: None,
            runtime_rpc_transport: None,
            runtime_metadata_events: None,
            local_search_index: true,
            invitation_admission: Arc::new(
                account_catalog_write_invitation_admission::Admission::default(),
            ),
            automation_sprint_limits: Arc::new(automation_sprint::IngressLimits::default()),
            automation_sprint_manual: None,
            automation_sprint_quota: None,
            channels: None,
            workflow_publisher: None,
            invitation_mailer: None,
            invitation_metrics: None,
            workspace_metrics: None,
            catalog_publisher: None,
            onboarding_metrics: None,
            avatar_storage_urls: None,
            extensions_sprint_enabled: false,
            extensions_sprint_signing_key: None,
            extensions_sprint_config_key: None,
            integration_config: workspace_integrations::IntegrationConfig::default(),
            cloud_billing: cloud_billing::CloudRuntimeConfig::default(),
            cloud_billing_flags: cloud_billing::CloudBillingFlagProvider::default(),
        }
    }

    pub fn with_channel_runtime(mut self, runtime: channels_sprint::SharedChannelRuntime) -> Self {
        self.channels = Some(runtime);
        self
    }

    pub fn with_extensions_sprint(mut self, enabled: bool) -> Self {
        self.extensions_sprint_enabled = enabled;
        self
    }

    pub fn with_extensions_sprint_signing_key(mut self, key: [u8; 32]) -> Self {
        self.extensions_sprint_signing_key = Some(key);
        self
    }

    pub fn with_extensions_sprint_config_key(mut self, key: [u8; 32]) -> Self {
        self.extensions_sprint_config_key = Some(key);
        self
    }

    pub fn with_workflow_publisher(
        mut self,
        publisher: Arc<dyn account_catalog_write_workflow_publish::WorkflowPublisher>,
    ) -> Self {
        self.workflow_publisher = Some(publisher);
        self
    }

    pub fn with_onboarding_metrics(
        mut self,
        metrics: Arc<dyn account_catalog_write_onboarding::OnboardingMetrics>,
    ) -> Self {
        self.onboarding_metrics = Some(metrics);
        self
    }

    pub fn with_avatar_storage_urls(
        mut self,
        storage: Arc<dyn account_catalog_write_avatar::AvatarStorageUrls>,
    ) -> Self {
        self.avatar_storage_urls = Some(storage);
        self
    }

    pub fn with_catalog_publisher(
        mut self,
        publisher: Arc<dyn account_catalog_write_catalog_publish::CatalogPublisher>,
    ) -> Self {
        self.catalog_publisher = Some(publisher);
        self
    }

    pub fn with_invitation_mailer(
        mut self,
        mailer: Arc<dyn account_catalog_write_invitation_effects::InvitationMailer>,
    ) -> Self {
        self.invitation_mailer = Some(mailer);
        self
    }

    pub fn with_invitation_metrics(
        mut self,
        metrics: Arc<dyn account_catalog_write_invitation_effects::InvitationMetrics>,
    ) -> Self {
        self.invitation_metrics = Some(metrics);
        self
    }

    pub fn with_workspace_metrics(
        mut self,
        metrics: Arc<dyn account_catalog_write_workspace_effects::WorkspaceMetrics>,
    ) -> Self {
        self.workspace_metrics = Some(metrics);
        self
    }

    pub fn with_invitation_admission_from_env(mut self) -> Self {
        self.invitation_admission =
            Arc::new(account_catalog_write_invitation_admission::Admission::from_env());
        self
    }

    /// Issue writes remain unregistered until a real workspace bus is supplied.
    pub fn with_issue_event_sink(mut self, sink: issue_write::SharedIssueEventSink) -> Self {
        self.issue_events = Some(sink);
        self
    }

    /// Local issue WebSocket fanout still needs real dispatch and downstream
    /// notification listeners before production writes may be registered.
    pub fn with_local_issue_event_dispatcher(
        mut self,
        dispatcher: Arc<dyn IssueWakeupDispatcher>,
    ) -> Self {
        self.issue_events = Some(Arc::new(issue_event_bridge::IssueEvents::new(
            self.events.clone(),
            dispatcher,
        )));
        self
    }

    /// Comment writes require notification/dispatch hooks, never a no-op runtime.
    pub fn with_issue_comment_runtime(
        mut self,
        runtime: issue_write::SharedIssueCommentRuntime,
    ) -> Self {
        self.issue_comment_runtime = Some(runtime);
        self
    }

    /// Issue creation stays closed until entitlement and analytics hooks exist.
    pub fn with_issue_create_runtime(
        mut self,
        runtime: issue_write::SharedIssueCreateRuntime,
    ) -> Self {
        self.issue_create_runtime = Some(runtime);
        self
    }

    pub fn with_realtime_metrics(
        mut self,
        metrics: files_ops_sprint::SharedRealtimeMetrics,
    ) -> Self {
        self.realtime_metrics = Some(metrics);
        self
    }

    pub fn with_realtime_token(mut self, token: Option<String>) -> Self {
        self.realtime_token = token;
        self
    }

    pub fn with_files_storage(mut self, storage: files_ops_sprint::SharedObjectStore) -> Self {
        self.files_storage = Some(storage);
        self
    }

    pub fn with_attachment_events(
        mut self,
        sink: files_ops_sprint::SharedAttachmentEventSink,
    ) -> Self {
        self.files_events = Some(sink);
        self
    }

    pub fn with_feedback_events(mut self, sink: files_ops_sprint::SharedFeedbackEventSink) -> Self {
        self.files_feedback = Some(sink);
        self
    }

    pub fn with_files_public_url(mut self, url: String) -> Self {
        self.files_public_url = Some(url);
        self
    }

    /// Deletion requires real task reconciliation, autopilot and storage wiring.
    pub fn with_issue_delete_runtime(
        mut self,
        runtime: issue_write::SharedIssueDeleteRuntime,
    ) -> Self {
        self.issue_delete_runtime = Some(runtime);
        self
    }

    /// Structural writes require a post-commit reconciler for child events.
    pub fn with_issue_child_runtime(
        mut self,
        runtime: issue_write::SharedIssueChildRuntime,
    ) -> Self {
        self.issue_child_runtime = Some(runtime);
        self
    }

    /// PR links require provider reads, merge decisions and event publication.
    pub fn with_issue_pull_request_runtime(
        mut self,
        runtime: issue_write::SharedIssuePullRequestRuntime,
    ) -> Self {
        self.issue_pull_request_runtime = Some(runtime);
        self
    }

    /// Inbox writes require the real recipient-scoped event bus.
    pub fn with_inbox_event_sink(
        mut self,
        sink: queries_sprint_inbox::SharedInboxEventSink,
    ) -> Self {
        self.inbox_events = Some(sink);
        self
    }

    /// Deliver committed inbox changes through the authenticated user WebSocket bus.
    pub fn with_local_inbox_events(mut self) -> Self {
        self.inbox_events = Some(Arc::new(self.events.clone()));
        self
    }

    /// Agent and squad writes require a privacy-aware, authenticated event sink.
    pub fn with_squad_event_sink(
        mut self,
        sink: agents_squads_sprint::SharedSquadEventSink,
    ) -> Self {
        self.squad_events = Some(sink);
        self
    }

    /// Wire only P0 agent env notifications; squad CRUD and agent lifecycle
    /// remain fail-closed until their other dependencies are integrated.
    pub fn with_local_p0_agent_events(mut self) -> Self {
        self.p0_agent_events = Some(Arc::new(self.events.clone()));
        self
    }

    /// The P0 agent-event bus, defaulting to the full squad sink once wired.
    pub(super) fn agent_event_bus(&self) -> Option<agents_squads_sprint::SharedSquadEventSink> {
        self.p0_agent_events
            .clone()
            .or_else(|| self.squad_events.clone())
    }

    /// Deliver committed P0 task cancellations through the authenticated
    /// workspace bus. Manual wakeup dispatch stays a separate dependency and
    /// is deliberately not fabricated here.
    pub fn with_local_p0_task_events(mut self) -> Self {
        self.task_cancel_events = Some(Arc::new(self.events.clone()));
        self
    }

    /// Install the in-process daemon wake hub. This is the only wake transport
    /// in this slice: Go's Redis empty-claim version cache has no Rust
    /// equivalent yet; Rust claims currently query PostgreSQL rather than
    /// consulting Go's Redis-backed empty-verdict cache.
    pub fn with_local_daemon_wake(mut self) -> Self {
        self.daemon_wake = Some(std::sync::Arc::new(p0_daemon_ws::Hub::default()));
        self
    }

    pub(super) fn wake_daemon(&self, runtime: Option<Uuid>, task: Option<Uuid>) {
        if let (Some(hub), Some(runtime)) = (&self.daemon_wake, runtime) {
            hub.notify_task_available(runtime, task);
        }
    }

    /// Pin writes require a recipient-scoped event sink.
    /// Provider refresh requires a validated external-cloud adapter; no implicit no-op.
    pub fn with_pull_request_refresh(
        mut self,
        refresh: queries_sprint_pull_requests::SharedPullRequestRefresh,
    ) -> Self {
        self.pull_request_refresh = Some(refresh);
        self
    }

    pub fn with_pin_event_sink(mut self, sink: queries_sprint_pins::SharedPinEventSink) -> Self {
        self.pin_events = Some(sink);
        self
    }

    /// Storage owner supplies URL signing; no raw private avatar fallback.
    pub fn with_timeline_avatar_url_policy(
        mut self,
        policy: queries_sprint_timeline::SharedTimelineAvatarUrlPolicy,
    ) -> Self {
        self.timeline_avatar_urls = Some(policy);
        self
    }

    /// Storage owner provides scoped attachment URLs; missing policy fails closed.
    pub fn with_attachment_url_policy(
        mut self,
        policy: queries_sprint_attachments::SharedAttachmentUrlPolicy,
    ) -> Self {
        self.attachment_urls = Some(policy);
        self
    }

    /// Deliver committed pin changes only to the pin owner's user WebSocket.
    pub fn with_local_pin_events(mut self) -> Self {
        self.pin_events = Some(Arc::new(self.events.clone()));
        self
    }

    /// Register global skill mutations only with the real workspace event bus.
    pub fn with_skill_event_sink(
        mut self,
        sink: runtime_skills_sprint::SharedSkillEventSink,
    ) -> Self {
        self.skill_events = Some(sink);
        self
    }

    /// Enable only after applying runtime RPC schema and wiring heartbeat claims.
    pub fn with_runtime_rpc_transport(
        mut self,
        transport: runtime_skills_sprint_control::SharedRuntimeRpcTransport,
    ) -> Self {
        self.runtime_rpc_transport = Some(transport);
        self
    }

    /// Runtime metadata writes require the real daemon:register workspace event bus.
    pub fn with_runtime_metadata_event_sink(
        mut self,
        sink: runtime_skills_sprint_manage::SharedRuntimeMetadataEventSink,
    ) -> Self {
        self.runtime_metadata_events = Some(sink);
        self
    }

    /// Go enables local search by default; callers must propagate its kill switch.
    pub fn with_local_search_index(mut self, enabled: bool) -> Self {
        self.local_search_index = enabled;
        self
    }

    pub fn with_session_config(mut self, session: SessionConfig) -> Self {
        self.session = session;
        self
    }

    pub fn with_git_extra_ca_certs(mut self, certs: String) -> Self {
        if !certs.is_empty() {
            self.git_extra_ca_certs = Some(certs);
        }
        self
    }

    pub fn with_workspace_integrations(
        mut self,
        config: workspace_integrations::IntegrationConfig,
    ) -> Self {
        self.integration_config = config;
        self
    }

    pub fn with_cloud_billing(
        mut self,
        config: cloud_billing::CloudRuntimeConfig,
        flags: cloud_billing::CloudBillingFlagProvider,
    ) -> Self {
        self.cloud_billing = config;
        self.cloud_billing_flags = flags;
        self
    }

    pub fn with_cloud_url(mut self, base: &str) -> Result<Self, reqwest::Error> {
        self.cloud = cloud::CloudVerifier::new(base)?;
        Ok(self)
    }
}

#[derive(Clone)]
pub struct SessionConfig {
    ttl_seconds: i64,
    domain: String,
    secure: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: 30 * 24 * 3600,
            domain: String::new(),
            secure: false,
        }
    }
}

impl SessionConfig {
    pub fn from_env() -> Result<Self, String> {
        // A renewed JWT without renewed CDN signed cookies would outlive its
        // CloudFront policy. This read slice has no signer: refuse that setup.
        if std::env::var("CLOUDFRONT_KEY_PAIR_ID").is_ok_and(|v| !v.is_empty()) {
            return Err("CloudFront signer is not implemented in the Rust read slice; cookie renewal cannot keep CDN cookies in lockstep".into());
        }
        let mut config = Self::default();
        if let Ok(raw) = std::env::var("AUTH_TOKEN_TTL")
            && !raw.trim().is_empty()
        {
            if let Some(seconds) = parse_go_ttl(&raw) {
                config.ttl_seconds = seconds.max(60);
            } else {
                return Err("AUTH_TOKEN_TTL is unsupported or invalid for the Rust read slice; use an integer number of seconds or a Go duration resolving to whole seconds".into());
            }
        }
        config.domain = std::env::var("COOKIE_DOMAIN")
            .unwrap_or_default()
            .trim()
            .to_owned();
        if config
            .domain
            .trim_start_matches('.')
            .parse::<IpAddr>()
            .is_ok()
            || !config
                .domain
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'-')
        {
            config.domain.clear();
        }
        config.secure = std::env::var("FRONTEND_ORIGIN")
            .ok()
            .and_then(|raw| {
                raw.trim()
                    .split_once(':')
                    .map(|(scheme, _)| scheme.eq_ignore_ascii_case("https"))
            })
            .unwrap_or(false);
        Ok(config)
    }
}

// Go time.ParseDuration accepts concatenated units and fractional units. Its
// duration is bounded by signed 64-bit nanoseconds; bare integers are seconds.
fn parse_go_ttl(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<i64>() {
        return (seconds > 0 && seconds <= i64::MAX / 1_000_000_000).then_some(seconds);
    }
    let mut rest = raw;
    let mut total = 0f64;
    while !rest.is_empty() {
        let end = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let value: f64 = rest.get(..end)?.parse().ok()?;
        rest = rest.get(end..)?;
        let (unit, nanos) = [
            ("ns", 1f64),
            ("us", 1_000f64),
            ("µs", 1_000f64),
            ("μs", 1_000f64),
            ("ms", 1_000_000f64),
            ("s", 1_000_000_000f64),
            ("m", 60_000_000_000f64),
            ("h", 3_600_000_000_000f64),
        ]
        .into_iter()
        .find(|(unit, _)| rest.starts_with(unit))?;
        total += value * nanos;
        rest = &rest[unit.len()..];
    }
    // Go clamps durations under a minute; above that, refuse subsecond
    // precision rather than silently renewing with a different expiry.
    (total.is_finite()
        && total > 0.0
        && total <= i64::MAX as f64
        && (total < 60_000_000_000f64 || total % 1_000_000_000f64 == 0.0))
        .then_some(((total / 1_000_000_000f64) as i64).max(1))
}

#[derive(Clone)]
struct Identity(Uuid);

#[derive(Clone)]
struct WorkspaceSelectors {
    slug: Option<String>,
    id: Option<String>,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    email: String,
    // JWT expiry is checked by jsonwebtoken, not the session CSRF helper.
    exp: usize,
    #[serde(default)]
    sid: String,
    #[serde(default)]
    name: Value,
}

#[derive(Debug)]
pub struct ApiError(StatusCode, String, bool);

struct GoJson<T>(T);

impl<T: Serialize> IntoResponse for GoJson<T> {
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(mut body) => {
                body.push(b'\n');
                let len = body.len();
                let mut response = body.into_response();
                response
                    .headers_mut()
                    .insert("content-type", "application/json".parse().unwrap());
                response.headers_mut().insert("content-length", len.into());
                response
            }
            Err(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to encode response",
            )
                .into_response(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = match self.1.as_str() {
            "lark install not configured" | "lark integration not configured" => {
                Some("lark_not_configured")
            }
            "slack integration not enabled" | "slack integration not configured" => {
                Some("slack_not_configured")
            }
            "wecom integration not enabled" | "wecom integration not configured" => {
                Some("wecom_not_configured")
            }
            "dingtalk integration not enabled" | "dingtalk integration not configured" => {
                Some("dingtalk_not_configured")
            }
            "telegram integration not enabled"
            | "telegram integration not configured"
            | "telegram guest invitations are not configured" => Some("telegram_not_configured"),
            "zalo integration not enabled"
            | "zalo integration not configured"
            | "zalo guest invitations are not configured" => Some("zalo_not_configured"),
            _ => None,
        };
        let mut response = match code {
            Some(code) if self.0 == StatusCode::FORBIDDEN => {
                GoJson(serde_json::json!({"error": self.1, "code": code})).into_response()
            }
            _ => GoJson(serde_json::json!({"error": self.1})).into_response(),
        };
        *response.status_mut() = self.0;
        if self.2 {
            // Go's auth middleware uses http.Error even though its body is JSON.
            response
                .headers_mut()
                .insert("content-type", "text/plain; charset=utf-8".parse().unwrap());
        }
        response
    }
}

fn error(status: StatusCode, message: impl Into<String>) -> ApiError {
    ApiError(status, message.into(), false)
}

pub fn routes(state: ApiState) -> Router {
    Router::new()
        .route("/api/me", get(me))
        .route("/api/workspaces", get(workspaces))
        .route("/api/workspaces/", get(workspaces))
        .route("/api/workspaces/{id}/", get(workspace))
        .route("/api/workspaces/{id}/members", get(members))
        .merge(catalog::routes(state.clone()))
        .merge(p0_trigger_preview::routes(state.clone()))
        .merge(account::routes())
        .merge(runtime_usage::routes())
        .merge(account_catalog_write::routes())
        .merge(account_catalog_write_client_usage::routes())
        .merge(account_catalog_write_members::routes())
        .merge(account_catalog_write_invitations::routes())
        .merge(account_catalog_write_workspaces::routes())
        .merge(account_catalog_write_workspace_delete::routes())
        .merge(account_catalog_write_profile::routes())
        .merge(account_catalog_write_onboarding::routes())
        .merge(account_catalog_write_share_links::routes())
        .merge(account_catalog_write_runtime_profiles::routes())
        .merge(workspace_read::routes())
        .merge(workspace_integrations::routes())
        .merge(cloud_billing::routes())
        .merge(files_ops_sprint::authenticated_routes())
        .merge(channels_sprint::routes())
        .merge(channels_sprint_groups::routes())
        .merge(extensions_sprint::routes())
        .merge(extensions_sprint_install::routes())
        .merge(extensions_sprint_lifecycle::routes())
        .merge(extensions_sprint_packages::routes())
        .merge(extensions_sprint_token::routes())
        .merge(extensions_sprint_public::bridge_routes(state.clone()))
        .merge(p0_auth::authenticated_routes())
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        // DaemonAuth is bearer-only and must never inherit browser Auth.
        .merge(daemon::routes(state.clone()))
        .merge(runtime_chat_ws::routes())
        // Public previews must not enter Auth, even with invalid credentials.
        .merge(workspace_read::public_routes())
        .merge(files_ops_sprint::public_routes())
        .merge(automation_sprint::public_routes())
        .merge(extensions_sprint_public::public_routes(state.clone()))
        .merge(p0_auth::public_routes(p0_auth::PublicConfig::from_state(
            &state,
        )))
        .with_state(state)
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|h| h.to_str().ok())
        .flat_map(|h| h.split(';'))
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == name && !value.is_empty()).then(|| value.to_owned())
        })
}

fn credential(headers: &HeaderMap) -> Option<(String, bool)> {
    if let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        && !token.is_empty()
    {
        return Some((token.to_owned(), false));
    }
    cookie(headers, "multica_auth").map(|token| (token, true))
}

fn decode_session_claims(token: &str, secret: &[u8]) -> Result<Claims, ApiError> {
    let invalid = || error(StatusCode::UNAUTHORIZED, "invalid token");
    let mut validation = Validation::new(Algorithm::HS256);
    // Frozen Go's jwt.Parse applies exp/nbf without clock-skew allowance.
    validation.leeway = 0;
    validation.validate_nbf = true;
    let claims = decode::<Claims>(token, &DecodingKey::from_secret(secret), &validation)
        .map_err(|_| invalid())?
        .claims;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid())?
        .as_secs() as usize;
    // jsonwebtoken accepts exp == now; Go requires now.Before(exp).
    if claims.exp <= now {
        return Err(invalid());
    }
    Ok(claims)
}

fn signature(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts all key lengths");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

fn csrf_valid(headers: &HeaderMap, token: &str, secret: &[u8]) -> bool {
    let Some(presented) = headers.get("x-csrf-token").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some((nonce_hex, sig_hex)) = presented.split_once('.') else {
        return false;
    };
    let (Ok(nonce), Ok(sig)) = (hex::decode(nonce_hex), hex::decode(sig_hex)) else {
        return false;
    };
    if sig.len() != 32 {
        return false;
    }
    if let Some(sid) = decode::<Claims>(token, &DecodingKey::from_secret(secret), &{
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        validation.required_spec_claims.clear();
        validation
    })
    .ok()
    .map(|t| t.claims.sid)
    .filter(|sid| !sid.is_empty())
    {
        let mut data = sid.into_bytes();
        data.push(0);
        data.extend_from_slice(&nonce);
        if signature(secret, &data) == sig {
            return true;
        }
    }
    signature(token.as_bytes(), &nonce) == sig
}

// Frozen emergency denylist from staged Go auth/temporary_disabled_users.go.
fn temporarily_disabled(user: Uuid, email: &str) -> bool {
    matches!(
        user.to_string().as_str(),
        "514492f7-b30f-4147-bd33-c0e8ce5d6d4f" | "1d542296-17c6-484a-9914-dcee589be116"
    ) || matches!(
        email.trim().to_ascii_lowercase().as_str(),
        "pdzzer68@embassybase.com" | "gtwtrox@mowan666.com"
    )
}

async fn authenticate(
    state: State<ApiState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    authenticate_inner(state, request, next)
        .await
        .map_err(|mut error| {
            error.2 = true;
            error
        })
}

async fn authenticate_inner(
    State(state): State<ApiState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let (token, from_cookie) = credential(request.headers())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "missing authorization"))?;
    if from_cookie
        && !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        )
        && !csrf_valid(request.headers(), &token, &state.jwt_secret)
    {
        return Err(error(StatusCode::FORBIDDEN, "CSRF validation failed"));
    }
    // Fleet verification precedes its own local owner lookup, including when
    // the DB is unavailable. An unconfigured verifier must remain a 401.
    let db = if token.starts_with("mcn_") {
        None
    } else {
        Some(state.pool.get().await.map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication temporarily unavailable",
            )
        })?)
    };
    let mut session_claims = None;
    let mut actor = Actor::Human;
    let user_id = if token.starts_with("mul_") {
        let db = db.as_ref().expect("non-cloud auth acquired DB");
        let hash = hex::encode(Sha256::digest(token.as_bytes()));
        let row = db.query_opt(
            "SELECT p.id, p.user_id FROM personal_access_token p JOIN \"user\" u ON u.id = p.user_id  WHERE p.token_hash = $1 AND NOT p.revoked AND (p.expires_at IS NULL OR p.expires_at > now()) AND NOT u.zbs_is_shadow",
            &[&hash],
        ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "authentication temporarily unavailable"))?
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
        let pat_id: Uuid = row.get(0);
        let id = row.get::<_, Uuid>(1);
        if temporarily_disabled(id, "") {
            return Err(error(StatusCode::FORBIDDEN, "account disabled"));
        }
        // Usage bookkeeping is best-effort; it must not invalidate a valid PAT.
        // Go's asynchronous cache-miss update remains a separate parity gate.
        let _ = db.execute(
            "UPDATE personal_access_token SET last_used_at = now() WHERE id = $1 AND NOT revoked",
            &[&pat_id],
        )
        .await;
        id
    } else if token.starts_with("mat_") {
        let db = db.as_ref().expect("non-cloud auth acquired DB");
        let task =
            actor::authenticate_task(&***db, &token, request.headers(), request.uri()).await?;
        let user = task.user;
        actor = Actor::Task(task);
        user
    } else if token.starts_with("mcn_") {
        actor = Actor::Cloud;
        cloud::authenticate(&state, &token).await?
    } else if token.starts_with("mdt_") {
        // Daemon tokens are never valid on human / task Auth routes.
        return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
    } else {
        let db = db.as_ref().expect("non-cloud auth acquired DB");
        let claims = decode_session_claims(&token, &state.jwt_secret)?;
        let id = Uuid::parse_str(&claims.sub)
            .map_err(|_| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
        if temporarily_disabled(id, &claims.email) {
            return Err(error(StatusCode::FORBIDDEN, "account disabled"));
        }
        let exists = db
            .query_opt(
                "SELECT 1 FROM \"user\" WHERE id = $1 AND NOT zbs_is_shadow",
                &[&id],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "authentication temporarily unavailable",
                )
            })?;
        if exists.is_none() {
            return Err(error(StatusCode::UNAUTHORIZED, "invalid token"));
        }
        session_claims = Some(claims);
        id
    };
    // Preserve only the selector values for the membership-gated catalog. Strip
    // caller-supplied context headers before any handler sees the request.
    let selectors = match &actor {
        Actor::Daemon => return Err(error(StatusCode::UNAUTHORIZED, "invalid token")),
        Actor::Task(task) => WorkspaceSelectors {
            slug: None,
            id: Some(task.workspace.to_string()),
        },
        Actor::Human | Actor::Cloud => WorkspaceSelectors {
            slug: request
                .headers()
                .get("x-workspace-slug")
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned),
            id: request
                .headers()
                .get("x-workspace-id")
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned),
        },
    };
    for key in [
        "x-user-id",
        "x-user-email",
        "x-agent-id",
        "x-task-id",
        "x-actor-source",
        "x-workspace-id",
        "x-workspace-slug",
    ] {
        request.headers_mut().remove(key);
    }
    drop(db);
    if let Actor::Task(task) = &actor {
        for (key, value) in [
            ("x-user-id", task.user.to_string()),
            ("x-agent-id", task.agent.to_string()),
            ("x-task-id", task.task.to_string()),
            ("x-workspace-id", task.workspace.to_string()),
            ("x-actor-source", "task_token".to_owned()),
        ] {
            request.headers_mut().insert(key, value.parse().unwrap());
        }
    }
    if matches!(actor, Actor::Cloud) {
        request
            .headers_mut()
            .insert("x-actor-source", "cloud_pat".parse().unwrap());
    }
    request.extensions_mut().insert(actor);
    request.extensions_mut().insert(Identity(user_id));
    request.extensions_mut().insert(selectors);
    let cookies = if from_cookie
        && matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        ) {
        session_claims
            .as_ref()
            .and_then(|claims| renew_session(claims, &state))
    } else {
        None
    };
    let mut response = next.run(request).await;
    if let Some(cookies) = cookies {
        for cookie in cookies {
            response.headers_mut().append(SET_COOKIE, cookie);
        }
    }
    Ok(response)
}

// Authentication and the user DB check must succeed before this is called.
// Failures here leave the original valid session untouched.
fn renew_session(claims: &Claims, state: &ApiState) -> Option<Vec<HeaderValue>> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs()).ok()?;
    let expiry = i64::try_from(claims.exp).ok()?;
    let remaining = expiry.checked_sub(now)?;
    if remaining <= 0 || remaining > state.session.ttl_seconds / 2 {
        return None;
    }
    let sid = if claims.sid.is_empty() {
        hex::encode(random_nonce().ok()?)
    } else {
        claims.sid.clone()
    };
    let new_exp = now.checked_add(state.session.ttl_seconds)?;
    let mut payload = json!({"sub": claims.sub, "exp": new_exp, "iat": now, "sid": sid});
    if !claims.email.is_empty() {
        payload["email"] = json!(claims.email);
    }
    if claims.name.as_str().is_some_and(|name| !name.is_empty()) {
        payload["name"] = claims.name.clone();
    }
    let token = encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(&state.jwt_secret),
    )
    .ok()?;
    let token_csrf = csrf_value(token.as_bytes(), &random_nonce().ok()?);
    let session_nonce = random_nonce().ok()?;
    let mut binding = sid.as_bytes().to_vec();
    binding.push(0);
    binding.extend_from_slice(&session_nonce);
    let session_csrf = format!(
        "{}.{}",
        hex::encode(session_nonce),
        hex::encode(signature(&state.jwt_secret, &binding))
    );
    let expires = DateTime::<Utc>::from_timestamp(new_exp, 0)?
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let cookie = |name: &str, value: &str, http_only: bool| {
        let domain = if state.session.domain.is_empty() {
            String::new()
        } else {
            format!("; Domain={}", state.session.domain)
        };
        let flags = format!(
            "{}{}",
            if http_only { "; HttpOnly" } else { "" },
            if state.session.secure { "; Secure" } else { "" }
        );
        HeaderValue::from_str(&format!(
            "{name}={value}; Path=/{domain}; Expires={expires}; Max-Age={}; SameSite=Strict{flags}",
            state.session.ttl_seconds
        ))
        .ok()
    };
    Some(vec![
        cookie("multica_auth", &token, true)?,
        cookie("multica_csrf", &token_csrf, false)?,
        cookie("multica_csrf_session", &session_csrf, false)?,
    ])
}

fn random_nonce() -> Result<[u8; 16], getrandom::Error> {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce)?;
    Ok(nonce)
}

fn csrf_value(token: &[u8], nonce: &[u8; 16]) -> String {
    format!(
        "{}.{}",
        hex::encode(nonce),
        hex::encode(signature(token, nonce))
    )
}

fn timestamp(t: DateTime<Utc>) -> String {
    // Match pgx's local-time decoding and Go's RFC3339 second precision.
    t.with_timezone(&Local)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[derive(Serialize)]
struct Workspace {
    id: Uuid,
    name: String,
    slug: String,
    description: Option<String>,
    context: Option<String>,
    settings: Value,
    repos: Value,
    issue_prefix: String,
    avatar_url: Option<String>,
    created_at: String,
    updated_at: String,
}

fn workspace_row(r: &tokio_postgres::Row) -> Workspace {
    Workspace {
        id: r.get("id"),
        name: r.get("name"),
        slug: r.get("slug"),
        description: r.get("description"),
        context: r.get("context"),
        settings: r.get("settings"),
        repos: r.get("repos"),
        issue_prefix: r.get("issue_prefix"),
        avatar_url: r.get("avatar_url"),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }
}

const WORKSPACE_COLUMNS: &str = "w.id, w.name, w.slug, w.description, w.context, w.settings, w.repos, w.issue_prefix, w.avatar_url, w.created_at, w.updated_at";

async fn workspaces(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<Workspace>>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list workspaces",
        )
    })?;
    let sql = format!(
        "SELECT {WORKSPACE_COLUMNS} FROM member m JOIN workspace w ON w.id = m.workspace_id WHERE m.user_id = $1 ORDER BY w.created_at ASC"
    );
    let rows = db.query(&sql, &[&user]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list workspaces",
        )
    })?;
    Ok(GoJson(rows.iter().map(workspace_row).collect()))
}

async fn workspace(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Workspace>, ApiError> {
    actor.require_workspace_url(&raw_id)?;
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load workspace",
        )
    })?;
    let sql = format!(
        "SELECT {WORKSPACE_COLUMNS} FROM workspace w JOIN member m ON m.workspace_id = w.id AND m.user_id = $2 WHERE w.id = $1"
    );
    let row = db
        .query_opt(&sql, &[&id, &user])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load workspace",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    Ok(GoJson(workspace_row(&row)))
}

#[derive(Serialize)]
struct Member {
    id: Uuid,
    workspace_id: Uuid,
    user_id: Uuid,
    role: String,
    created_at: String,
    name: String,
    email: String,
    avatar_url: Option<String>,
}

async fn members(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_id): Path<String>,
) -> Result<GoJson<Vec<Member>>, ApiError> {
    actor.require_workspace_url(&raw_id)?;
    let id = Uuid::parse_str(&raw_id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list members"))?;
    // Membership gate is in the SAME query as the result set: no cross-workspace
    // rows can be returned even if membership changes while the query runs.
    let rows = db.query("SELECT m.id, m.workspace_id, m.user_id, m.role, m.created_at, u.name, u.email, u.avatar_url  FROM member m JOIN \"user\" u ON u.id = m.user_id WHERE m.workspace_id = $1  AND EXISTS (SELECT 1 FROM member viewer WHERE viewer.workspace_id = m.workspace_id AND viewer.user_id = $2)  ORDER BY m.created_at ASC", &[&id, &user]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list members"))?;
    // Go's member middleware returns 404 for non-members, not an empty array.
    if rows.is_empty() {
        let member = db
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id = $1 AND user_id = $2",
                &[&id, &user],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list members"))?;
        if member.is_none() {
            return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
        }
    }
    Ok(GoJson(
        rows.iter()
            .map(|r| Member {
                id: r.get("id"),
                workspace_id: r.get("workspace_id"),
                user_id: r.get("user_id"),
                role: r.get("role"),
                created_at: timestamp(r.get("created_at")),
                name: r.get("name"),
                email: r.get("email"),
                avatar_url: r.get("avatar_url"),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct User {
    id: Uuid,
    name: String,
    email: String,
    avatar_url: Option<String>,
    language: Option<String>,
    timezone: Option<String>,
    onboarded_at: Option<String>,
    onboarding_questionnaire: Value,
    profile_description: String,
    created_at: String,
    updated_at: String,
}

async fn me(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<User>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?;
    let r = db.query_opt("SELECT id, name, email, avatar_url, language, timezone, onboarded_at, onboarding_questionnaire, profile_description, created_at, updated_at FROM \"user\" WHERE id = $1", &[&user]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load user"))?
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "user not found"))?;
    Ok(GoJson(User {
        id: r.get("id"),
        name: r.get("name"),
        email: r.get("email"),
        avatar_url: r.get("avatar_url"),
        language: r.get("language"),
        timezone: r.get("timezone"),
        onboarded_at: r
            .get::<_, Option<DateTime<Utc>>>("onboarded_at")
            .map(timestamp),
        onboarding_questionnaire: r.get("onboarding_questionnaire"),
        profile_description: r.get("profile_description"),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn auth_rejections_keep_go_content_type() {
        // Go's middleware writes these through http.Error even though the body
        // is JSON, so clients see JSON with a text/plain content type.
        let mut auth_error = error(StatusCode::UNAUTHORIZED, "missing authorization");
        auth_error.2 = true;
        let response = auth_error.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"{\"error\":\"missing authorization\"}\n");
        assert_eq!(
            error(StatusCode::BAD_REQUEST, "bad request")
                .into_response()
                .headers()
                .get("content-type")
                .unwrap(),
            "application/json"
        );
    }

    #[test]
    fn go_ttl_parsing_and_clamp_inputs() {
        assert_eq!(parse_go_ttl("720h30m"), Some(2_593_800));
        assert_eq!(parse_go_ttl("1.5h"), Some(5_400));
        assert_eq!(parse_go_ttl("30"), Some(30));
        assert_eq!(parse_go_ttl("0.5s"), Some(1));
        assert_eq!(parse_go_ttl("90.5s"), None);
        for invalid in [
            "",
            "0",
            "-1h",
            "nope",
            "1h30",
            "9223372037",
            "999999999999h",
        ] {
            assert_eq!(parse_go_ttl(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn session_expiry_and_not_before_have_no_grace_period() {
        let secret = b"unit-test-secret";
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        for expiry in [0, now - 60, now - 30, now - 1, now] {
            let token = encode(
                &Header::default(),
                &json!({"sub": Uuid::new_v4(), "exp": expiry}),
                &EncodingKey::from_secret(secret),
            )
            .unwrap();
            assert!(
                decode_session_claims(&token, secret).is_err(),
                "exp={expiry}"
            );
        }
        for nbf in [None, Some(now - 1)] {
            let token = encode(
                &Header::default(),
                &json!({"sub": Uuid::new_v4(), "exp": now + 30, "nbf": nbf}),
                &EncodingKey::from_secret(secret),
            )
            .unwrap();
            assert!(decode_session_claims(&token, secret).is_ok());
        }
        let future = encode(
            &Header::default(),
            &json!({"sub": Uuid::new_v4(), "exp": now + 30, "nbf": now + 30}),
            &EncodingKey::from_secret(secret),
        )
        .unwrap();
        assert!(decode_session_claims(&future, secret).is_err());
    }

    fn test_state(config: SessionConfig) -> ApiState {
        let db_config = "host=127.0.0.1 port=1 dbname=unused".parse().unwrap();
        let pool = deadpool_postgres::Pool::builder(deadpool_postgres::Manager::new(
            db_config,
            tokio_postgres::NoTls,
        ))
        .build()
        .unwrap();
        ApiState::new(Arc::new(pool), b"unit-test-secret".to_vec()).with_session_config(config)
    }

    #[test]
    fn cookie_renewal_preserves_identity_and_csrf_posture() {
        let state = test_state(SessionConfig {
            ttl_seconds: 3600,
            domain: "example.com".into(),
            secure: true,
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let claims = Claims {
            sub: Uuid::new_v4().to_string(),
            email: "user@example.com".into(),
            name: json!("Example"),
            exp: now + 1000,
            sid: "stable-id".into(),
        };
        let cookies = renew_session(&claims, &state).unwrap();
        assert_eq!(cookies.len(), 3);
        let values: Vec<_> = cookies
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert!(values[0].starts_with("multica_auth="));
        assert!(values[0].contains("; HttpOnly"));
        for value in &values {
            assert!(value.contains("; Domain=example.com"));
            assert!(value.contains("; Path=/;"));
            assert!(value.contains("; Secure"));
            assert!(value.contains("; SameSite=Strict"));
            assert!(value.contains("; Max-Age=3600"));
            assert!(value.contains("; Expires="));
        }
        assert!(!values[1].contains("; HttpOnly"));
        assert!(!values[2].contains("; HttpOnly"));
        let new_token = values[0]
            .split(';')
            .next()
            .unwrap()
            .strip_prefix("multica_auth=")
            .unwrap();
        let new_claims = decode::<Value>(
            new_token,
            &DecodingKey::from_secret(&state.jwt_secret),
            &Validation::new(Algorithm::HS256),
        )
        .unwrap()
        .claims;
        assert_eq!(new_claims["sub"], claims.sub);
        assert_eq!(new_claims["sid"], "stable-id");
        assert_eq!(new_claims["email"], claims.email);
        assert_eq!(new_claims["name"], claims.name);
        assert!(new_claims["exp"].as_i64().unwrap() >= now as i64 + 3599);
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-csrf-token",
            values[1]
                .split(';')
                .next()
                .unwrap()
                .strip_prefix("multica_csrf=")
                .unwrap()
                .parse()
                .unwrap(),
        );
        assert!(csrf_valid(&headers, new_token, &state.jwt_secret));
        headers.insert(
            "x-csrf-token",
            values[2]
                .split(';')
                .next()
                .unwrap()
                .strip_prefix("multica_csrf_session=")
                .unwrap()
                .parse()
                .unwrap(),
        );
        assert!(csrf_valid(&headers, new_token, &state.jwt_secret));
        let old_token = encode(
            &Header::new(Algorithm::HS256),
            &json!({"sub":claims.sub,"sid":claims.sid,"exp":claims.exp}),
            &EncodingKey::from_secret(&state.jwt_secret),
        )
        .unwrap();
        assert!(csrf_valid(&headers, &old_token, &state.jwt_secret));
    }

    #[test]
    fn renewal_window_and_legacy_sid() {
        let state = test_state(SessionConfig {
            ttl_seconds: 3600,
            ..SessionConfig::default()
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let mut claims = Claims {
            sub: Uuid::new_v4().to_string(),
            email: String::new(),
            name: Value::Null,
            exp: now + 1801,
            sid: String::new(),
        };
        assert!(renew_session(&claims, &state).is_none());
        claims.exp = now - 1;
        assert!(renew_session(&claims, &state).is_none());
        claims.exp = now + 1000;
        let cookies = renew_session(&claims, &state).unwrap();
        let auth = cookies[0].to_str().unwrap();
        assert!(!auth.contains("; Secure"));
        assert!(!auth.contains("; Domain="));
        let token = auth
            .split(';')
            .next()
            .unwrap()
            .strip_prefix("multica_auth=")
            .unwrap();
        let next = decode::<Value>(
            token,
            &DecodingKey::from_secret(&state.jwt_secret),
            &Validation::new(Algorithm::HS256),
        )
        .unwrap()
        .claims;
        assert_eq!(next["sid"].as_str().unwrap().len(), 32);
        assert!(next.get("email").is_none());
        assert!(next.get("name").is_none());
    }

    #[test]
    fn csrf_token_and_session_binding() {
        let secret = b"unit-test-secret";
        let token = "token";
        let nonce = [9u8; 16];
        let mut headers = HeaderMap::new();
        headers.insert("cookie", "multica_auth=token".parse().unwrap());
        let sig = signature(token.as_bytes(), &nonce);
        headers.insert(
            "x-csrf-token",
            format!("{}.{}", hex::encode(nonce), hex::encode(sig))
                .parse()
                .unwrap(),
        );
        assert!(csrf_valid(&headers, token, secret));
        assert!(!csrf_valid(&headers, "different-token", secret));
        headers.remove("x-csrf-token");
        assert!(!csrf_valid(&headers, token, secret));
    }
}
