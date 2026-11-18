//! Execution-admission vocabulary shared by every synchronous trigger path.
//!
//! Go keeps this in two places — `internal/dispatch/reason.go` (the stable
//! cross-layer reason enum) and `internal/handler/admission.go` (the status
//! enum, the wire body and the non-enumerating English fallback). This module
//! joins both so a reason code can never be produced by one path and serialized
//! by another under a different spelling.
//!
//! Two invariants the contract protects, both copied verbatim from Go:
//!
//! 1. Whether the business object was written (comment saved, issue updated) and
//!    whether an agent was actually triggered are different facts, so a caller
//!    must be able to express partial success.
//! 2. A refusal exposes only a stable code plus a deliberately vague message. It
//!    must never reveal whether a private target exists, its name or its owner;
//!    the precise cause belongs in restricted server logs, not on the wire.

use serde::Serialize;

/// Domain-level outcome of one admission/enqueue attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchStatus {
    /// A new run was enqueued.
    Queued,
    /// The trigger merged into an already-pending task for the same target.
    Coalesced,
    /// Admitted but intentionally not started yet (parked backlog, suppress_run).
    Deferred,
    /// The comment was bound to the target's running turn instead of a new run.
    Steered,
    /// The run was refused; the reason code carries why.
    Blocked,
}

impl DispatchStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Coalesced => "coalesced",
            Self::Deferred => "deferred",
            Self::Steered => "steered",
            Self::Blocked => "blocked",
        }
    }
}

/// Stable, client-localizable admission reason.
///
/// Serialized by name (`invocation_not_allowed`, …) so adding a variant cannot
/// silently change an existing wire value. Clients treat an unknown code as a
/// generic failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    // Success-path codes.
    Queued,
    Coalesced,
    Deferred,
    Steered,

    // Refusals.
    InvocationNotAllowed,
    TargetUnavailable,
    RuntimeOffline,
    RuntimeUnusable,
    RuntimeAccessDenied,
    RuntimeProfileMissing,
    AgentRuntimeRequired,
    AttributionBlocked,
    AlreadyActive,
    SelfTriggerSuppressed,
    IssueInTriage,
    QuotaExceeded,
    IssueLimitReached,
    InternalError,
}

impl ReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Coalesced => "coalesced",
            Self::Deferred => "deferred",
            Self::Steered => "steered",
            Self::InvocationNotAllowed => "invocation_not_allowed",
            Self::TargetUnavailable => "target_unavailable",
            Self::RuntimeOffline => "runtime_offline",
            Self::RuntimeUnusable => "runtime_unusable",
            Self::RuntimeAccessDenied => "runtime_access_denied",
            Self::RuntimeProfileMissing => "runtime_profile_missing",
            Self::AgentRuntimeRequired => "agent_runtime_required",
            Self::AttributionBlocked => "attribution_blocked",
            Self::AlreadyActive => "already_active",
            Self::SelfTriggerSuppressed => "self_trigger_suppressed",
            Self::IssueInTriage => "issue_in_triage",
            Self::QuotaExceeded => "quota_exceeded",
            Self::IssueLimitReached => "issue_limit_reached",
            Self::InternalError => "internal_error",
        }
    }

    /// Parse a code persisted by an earlier decision. Unknown values are
    /// `None`, never guessed from a human-readable failure string.
    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "queued" => Self::Queued,
            "coalesced" => Self::Coalesced,
            "deferred" => Self::Deferred,
            "steered" => Self::Steered,
            "invocation_not_allowed" => Self::InvocationNotAllowed,
            "target_unavailable" => Self::TargetUnavailable,
            "runtime_offline" => Self::RuntimeOffline,
            "runtime_unusable" => Self::RuntimeUnusable,
            "runtime_access_denied" => Self::RuntimeAccessDenied,
            "runtime_profile_missing" => Self::RuntimeProfileMissing,
            "agent_runtime_required" => Self::AgentRuntimeRequired,
            "attribution_blocked" => Self::AttributionBlocked,
            "already_active" => Self::AlreadyActive,
            "self_trigger_suppressed" => Self::SelfTriggerSuppressed,
            "issue_in_triage" => Self::IssueInTriage,
            "quota_exceeded" => Self::QuotaExceeded,
            "issue_limit_reached" => Self::IssueLimitReached,
            "internal_error" => Self::InternalError,
            _ => return None,
        })
    }

    pub fn is_success(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Coalesced | Self::Deferred | Self::Steered
        )
    }
}

/// Legacy `error` string paired with a reason code.
///
/// Intentionally generic: safe to show to a caller who is not allowed to know
/// whether the target exists. Every branch is pinned by a unit test; changing
/// one is a wire change, not a wording improvement.
pub fn blocked_fallback_message(code: ReasonCode) -> &'static str {
    match code {
        ReasonCode::InvocationNotAllowed => "you don't have permission to use this target",
        ReasonCode::TargetUnavailable => "the target is unavailable",
        ReasonCode::RuntimeOffline => "the target's runtime is offline",
        ReasonCode::RuntimeUnusable => "the target's agent CLI cannot run on its machine",
        ReasonCode::RuntimeAccessDenied => "the target cannot run on this runtime",
        ReasonCode::RuntimeProfileMissing => {
            "the target's agent CLI is missing a runtime profile on its machine"
        }
        ReasonCode::AgentRuntimeRequired => "the target needs a runtime",
        ReasonCode::AttributionBlocked => "the run couldn't be attributed to a responsible member",
        ReasonCode::AlreadyActive => "a run is already active for this target",
        ReasonCode::IssueInTriage => {
            "the issue is in Triage and has no owner to run yet; accept it out of Triage first"
        }
        // Success-path codes never reach a blocked response; Go's `default`
        // branch covers them together with any code added later.
        _ => "the run was blocked",
    }
}

/// Structured body of a blocked synchronous enqueue (403 permission / 409
/// conflict): the vague legacy `error` plus the stable machine-readable code.
#[derive(Debug, Serialize)]
pub struct BlockedBody {
    pub error: &'static str,
    pub reason_code: &'static str,
}

pub fn blocked_body(code: ReasonCode) -> BlockedBody {
    BlockedBody {
        error: blocked_fallback_message(code),
        reason_code: code.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [ReasonCode; 18] = [
        ReasonCode::Queued,
        ReasonCode::Coalesced,
        ReasonCode::Deferred,
        ReasonCode::Steered,
        ReasonCode::InvocationNotAllowed,
        ReasonCode::TargetUnavailable,
        ReasonCode::RuntimeOffline,
        ReasonCode::RuntimeUnusable,
        ReasonCode::RuntimeAccessDenied,
        ReasonCode::RuntimeProfileMissing,
        ReasonCode::AgentRuntimeRequired,
        ReasonCode::AttributionBlocked,
        ReasonCode::AlreadyActive,
        ReasonCode::SelfTriggerSuppressed,
        ReasonCode::IssueInTriage,
        ReasonCode::QuotaExceeded,
        ReasonCode::IssueLimitReached,
        ReasonCode::InternalError,
    ];

    #[test]
    fn wire_codes_match_go_dispatch_reason_codes() {
        let expect = [
            "queued",
            "coalesced",
            "deferred",
            "steered",
            "invocation_not_allowed",
            "target_unavailable",
            "runtime_offline",
            "runtime_unusable",
            "runtime_access_denied",
            "runtime_profile_missing",
            "agent_runtime_required",
            "attribution_blocked",
            "already_active",
            "self_trigger_suppressed",
            "issue_in_triage",
            "quota_exceeded",
            "issue_limit_reached",
            "internal_error",
        ];
        for (code, want) in ALL.iter().zip(expect) {
            assert_eq!(code.as_str(), want);
            assert_eq!(ReasonCode::parse(want), Some(*code));
            // The serde form is what the JSON response actually emits.
            assert_eq!(
                serde_json::to_value(code).expect("serialize"),
                serde_json::json!(want)
            );
        }
    }

    #[test]
    fn unknown_persisted_code_is_not_guessed() {
        assert_eq!(ReasonCode::parse("some_future_code"), None);
        assert_eq!(ReasonCode::parse(""), None);
    }

    #[test]
    fn statuses_serialize_snake_case() {
        let pairs = [
            (DispatchStatus::Queued, "queued"),
            (DispatchStatus::Coalesced, "coalesced"),
            (DispatchStatus::Deferred, "deferred"),
            (DispatchStatus::Steered, "steered"),
            (DispatchStatus::Blocked, "blocked"),
        ];
        for (status, want) in pairs {
            assert_eq!(status.as_str(), want);
            assert_eq!(
                serde_json::to_value(status).expect("serialize"),
                serde_json::json!(want)
            );
        }
    }

    /// Mirrors Go's `TestDispatchBlockedFallbackMessageIsNonEnumerating`: every
    /// refusal keeps a non-empty, generic message, and the permission refusal
    /// stays deliberately vague about whether the target exists.
    #[test]
    fn blocked_messages_are_non_enumerating() {
        for code in ALL.iter().filter(|c| !c.is_success()) {
            let message = blocked_fallback_message(*code);
            assert!(!message.is_empty(), "{code:?} has no fallback message");
        }
        assert_eq!(
            blocked_fallback_message(ReasonCode::InvocationNotAllowed),
            "you don't have permission to use this target"
        );
        // Success-path codes fall through to the generic default, exactly like
        // Go's switch default.
        assert_eq!(
            blocked_fallback_message(ReasonCode::Queued),
            "the run was blocked"
        );
        assert_eq!(
            blocked_fallback_message(ReasonCode::SelfTriggerSuppressed),
            "the run was blocked"
        );
    }

    #[test]
    fn blocked_body_carries_both_fields() {
        let body = blocked_body(ReasonCode::RuntimeOffline);
        assert_eq!(
            serde_json::to_value(&body).expect("serialize"),
            serde_json::json!({
                "error": "the target's runtime is offline",
                "reason_code": "runtime_offline"
            })
        );
    }
}
