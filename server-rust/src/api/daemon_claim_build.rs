//! Runtime authorization, pre-launch rejection and claim credential minting.
//!
//! These are the pieces of Go's claim path that decide whether a task may be
//! handed to a daemon at all, and what failure a refusal records. They sit
//! between the transport (`p0_daemon_claim`, `daemon`) and the response builder
//! so both the batch and singular handlers share one implementation.
use super::{
    ApiError, ApiState,
    daemon::DaemonIdentity,
    daemon_claim_compose::daemon_runtime_binding_allowed,
    daemon_claim_pure::{ISSUE_SNAPSHOT_VERSION, IssueStateSnapshot},
};
use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;
use tokio_postgres::Row;
use uuid::Uuid;

/// The runtime columns every claim gate reads. Loaded once per task so the
/// authorization and the builder cannot disagree about which runtime it is.
#[derive(Clone)]
pub(super) struct ClaimRuntime {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub daemon_id: Option<String>,
    pub owner_id: Option<Uuid>,
    pub visibility: String,
    pub shared_with: Vec<Uuid>,
    /// Selects the openclaw model override and the runtime-scoped skill filter.
    pub provider: String,
}

const RUNTIME_SQL: &str = "SELECT id,workspace_id,daemon_id,owner_id,visibility,shared_with,provider FROM agent_runtime WHERE id=$1";

pub(super) fn runtime_from_row(row: &Row) -> ClaimRuntime {
    ClaimRuntime {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        daemon_id: row.get("daemon_id"),
        owner_id: row.get("owner_id"),
        visibility: row.get("visibility"),
        shared_with: row
            .get::<_, Option<Vec<Uuid>>>("shared_with")
            .unwrap_or_default(),
        provider: row.get::<_, Option<String>>("provider").unwrap_or_default(),
    }
}

pub(super) async fn load_runtime(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<ClaimRuntime>, ApiError> {
    db.query_opt(RUNTIME_SQL, &[&id])
        .await
        .map(|row| row.as_ref().map(runtime_from_row))
        .map_err(|_| internal("failed to load runtime"))
}

/// Go `verifyDaemonRuntimeAuthorized`: the runtime's own workspace must be
/// reachable by this credential AND the credential must own or be bound to the
/// runtime. Both halves are load-bearing — membership alone does not let a
/// member drive somebody else's daemon.
pub(super) async fn verify_daemon_runtime_authorized(
    db: &impl GenericClient,
    identity: &DaemonIdentity,
    runtime: &ClaimRuntime,
) -> Result<bool, ApiError> {
    super::daemon::workspace_access(db, identity, &runtime.workspace_id.to_string()).await?;
    Ok(daemon_runtime_binding_allowed(
        identity,
        runtime.daemon_id.as_deref(),
        runtime.owner_id,
    ))
}

/// Go `requireDaemonRuntimeAccess`. `BadRequest` for an unparseable id, `404`
/// for a missing runtime, `500` only for a read failure — the daemon is told
/// "not found" rather than being handed a distinct existence oracle.
pub(super) async fn require_daemon_runtime_access(
    db: &impl GenericClient,
    identity: &DaemonIdentity,
    raw: &str,
) -> Result<ClaimRuntime, ApiError> {
    let id = Uuid::parse_str(raw.trim())
        .map_err(|_| super::error(StatusCode::BAD_REQUEST, "invalid runtime_id"))?;
    let runtime = load_runtime(db, id)
        .await?
        .ok_or_else(|| super::error(StatusCode::NOT_FOUND, "runtime not found"))?;
    if !verify_daemon_runtime_authorized(db, identity, &runtime).await? {
        return Err(super::error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    Ok(runtime)
}

fn internal(message: &'static str) -> ApiError {
    super::error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

/// Go `claimBuildFailure`: what the builder decided instead of a payload.
/// `settled` means the task was moved to a terminal state by the rejection, so
/// the caller must not requeue it.
pub(super) struct ClaimBuildFailure {
    pub outcome: &'static str,
    pub status: StatusCode,
    /// Owned: Go formats the missing-source and worktree refusals with runtime
    /// data, and the daemon sees that text verbatim.
    pub message: String,
    pub settled: bool,
}

impl ClaimBuildFailure {
    pub(super) fn unsettled(
        outcome: &'static str,
        status: StatusCode,
        message: &'static str,
    ) -> Self {
        Self::unsettled_owned(outcome, status, message.to_owned())
    }

    pub(super) fn unsettled_owned(
        outcome: &'static str,
        status: StatusCode,
        message: String,
    ) -> Self {
        Self {
            outcome,
            status,
            message,
            settled: false,
        }
    }
}

/// Go's `ClaimDeliveryAuthzError`: the delivery-time authorization gate re-reads
/// the runtime and agent under `FOR UPDATE`, so it can refuse a claim the
/// builder already approved. The reason is a fixed vocabulary the caller maps to
/// a status and a user-facing message; it must survive the transaction as a
/// value rather than collapse into a generic failure.
#[derive(Debug)]
pub(super) struct ClaimDeliveryAuthz {
    pub reason: &'static str,
    pub detail: &'static str,
    /// The agent owner the gate read under lock, so the caller can pick between
    /// "agent has no owner" and "agent and runtime have different owners".
    pub agent_owner_id: Option<Uuid>,
}

/// `finalize_task_claim` can fail either because the delivery gate refused the
/// claim (a reason the caller turns into a pre-launch failure) or because the
/// transaction itself broke (a plain 500). Keeping them distinct is what lets
/// the daemon receive Go's typed refusal instead of a generic failure.
#[derive(Debug)]
pub(super) enum FinalizeClaimError {
    Authz(ClaimDeliveryAuthz),
    Api(ApiError),
}

/// Go `rejectClaimSourceLoad`. A missing row means the task outlived the thing
/// it was queued for, which is unrecoverable: cancel rather than requeue, or
/// the next poll claims the same dead task forever. A transient read failure is
/// only reported — the row may still be there.
pub(super) async fn reject_claim_source_load(
    state: &ApiState,
    task: &Row,
    missing: bool,
    source: &str,
) -> ClaimBuildFailure {
    if !missing {
        return ClaimBuildFailure::unsettled_owned(
            "error_source_load",
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to load task {source}"),
        );
    }
    let id: Uuid = task.get("id");
    if super::daemon_claim_service::cancel_task(state, id, None)
        .await
        .is_err()
    {
        eprintln!("task claim: failed to cancel task {id} with a missing {source}");
    }
    ClaimBuildFailure::unsettled_owned(
        "error_source_missing",
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("task is missing its {source}"),
    )
}

/// Go `rejectClaimSkillLoad`: skills are part of the run's contract, so an
/// unreadable set is a build failure rather than a claim without skills.
pub(super) fn reject_claim_skill_load() -> ClaimBuildFailure {
    ClaimBuildFailure::unsettled(
        "error_skill_load",
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to load agent skills",
    )
}

/// Go `rejectClaimOnWorkspaceMismatch`. A claim whose resolved workspace
/// differs from the runtime's is an isolation failure, never something to
/// retry: the task is cancelled so it cannot be re-offered to the same daemon.
pub(super) async fn reject_claim_on_workspace_mismatch(
    state: &ApiState,
    task: &Row,
    resolved_workspace: Uuid,
    runtime_workspace: Uuid,
) -> Option<ClaimBuildFailure> {
    if resolved_workspace == runtime_workspace {
        return None;
    }
    let id: Uuid = task.get("id");
    if super::daemon_claim_service::cancel_task(state, id, None)
        .await
        .is_err()
    {
        eprintln!("task claim: failed to cancel task {id} on workspace mismatch");
    }
    Some(ClaimBuildFailure::unsettled(
        "error_workspace",
        StatusCode::INTERNAL_SERVER_ERROR,
        "task workspace isolation check failed",
    ))
}

/// Go `failClaimedTaskBeforeLaunch`: the task is still unstarted, so it is
/// failed rather than left for reclaim. If the failure itself cannot commit,
/// the claim is requeued so another poll can settle it — the outcome gains a
/// `_settle` suffix and the daemon sees a generic 500.
pub(super) async fn fail_claimed_task_before_launch(
    state: &ApiState,
    task: &Row,
    user_message: &str,
    failure_reason: &'static str,
    outcome: &'static str,
    status: StatusCode,
    claim_message: &'static str,
) -> ClaimBuildFailure {
    let id: Uuid = task.get("id");
    match super::daemon_claim_service::fail_task(state, id, user_message, failure_reason).await {
        Ok(_) => ClaimBuildFailure {
            outcome,
            status,
            message: claim_message.to_owned(),
            settled: true,
        },
        Err(_) => {
            eprintln!("task claim: fail rejected task {id} failed ({outcome}); requeueing claim");
            let _ =
                super::daemon_claim_service::requeue_task_after_claim_failure(state, task).await;
            ClaimBuildFailure::unsettled(
                settle_outcome(outcome),
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to settle a task rejected before launch",
            )
        }
    }
}

/// The `_settle` variant of each launch-rejection outcome, resolved at compile
/// time so the wire string can never be assembled from runtime input.
fn settle_outcome(outcome: &'static str) -> &'static str {
    match outcome {
        "wakeup_unavailable" => "wakeup_unavailable_settle",
        "runtime_access_denied" => "runtime_access_denied_settle",
        "invalid_task_identity" => "invalid_task_identity_settle",
        _ => "claim_rejected_settle",
    }
}

/// The credentials a claim hands out. `finalize_task_claim` writes the token and
/// the receipt in one transaction.
pub(super) struct TaskToken<'a> {
    pub id: Uuid,
    pub hash: String,
    pub task_id: Uuid,
    pub agent_id: Uuid,
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub expires_at: DateTime<Utc>,
    pub runtime_id: Uuid,
    pub dispatched_at: DateTime<Utc>,
    pub trigger_comment_id: Option<Uuid>,
    /// Kept for the caller so the same raw value reaches the response; never
    /// stored.
    pub raw: &'a str,
}

impl TaskToken<'_> {
    pub fn raw(&self) -> &str {
        self.raw
    }
}

pub(super) fn hash_token(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

/// Go `GenerateAgentTaskToken`: `mat_` plus 40 hex characters, 24h validity.
pub(super) fn mint_task_token<'a>(
    raw: &'a str,
    task: &Row,
    agent_id: Uuid,
    workspace_id: Uuid,
    user_id: Uuid,
) -> TaskToken<'a> {
    let dispatched: Option<DateTime<Utc>> = task.get("dispatched_at");
    TaskToken {
        id: Uuid::now_v7(),
        hash: hash_token(raw),
        task_id: task.get("id"),
        agent_id,
        workspace_id,
        user_id,
        expires_at: Utc::now() + Duration::hours(24),
        runtime_id: task.get("runtime_id"),
        dispatched_at: dispatched.unwrap_or_else(Utc::now),
        trigger_comment_id: task.get("trigger_comment_id"),
        raw,
    }
}

/// Go `remoteMCPDaemonTokenForClaim`: a Remote MCP connection needs a daemon
/// credential the container can present, scoped to the runtime it runs on. No
/// connections means no token; a runtime without a daemon id cannot have one.
pub(super) struct RemoteMcpDaemonToken {
    pub raw: String,
    pub hash: String,
    pub workspace_id: Uuid,
    pub daemon_id: String,
    pub expires_at: DateTime<Utc>,
}

pub(super) fn remote_mcp_daemon_token_for_claim(
    has_remote_mcp_connections: bool,
    runtime: &ClaimRuntime,
) -> Result<Option<RemoteMcpDaemonToken>, ()> {
    if !has_remote_mcp_connections {
        return Ok(None);
    }
    let Some(daemon_id) = runtime
        .daemon_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    else {
        return Err(());
    };
    let mut bytes = [0u8; 20];
    getrandom::fill(&mut bytes).map_err(|_| ())?;
    let raw = format!("mdt_{}", hex::encode(bytes));
    Ok(Some(RemoteMcpDaemonToken {
        hash: hash_token(&raw),
        raw,
        workspace_id: runtime.workspace_id,
        daemon_id: daemon_id.to_owned(),
        expires_at: Utc::now() + Duration::hours(24),
    }))
}

/// The issue snapshot a claim records so the NEXT run can tell whether the
/// issue moved. Go's `buildIssueStateSnapshot` hashes title and description
/// only; the version gate makes a future field addition a clean mismatch.
pub(super) fn issue_snapshot_bytes(snapshot: &IssueStateSnapshot) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "v": ISSUE_SNAPSHOT_VERSION,
        "title_sha256": snapshot.title_sha256,
        "description_sha256": snapshot.description_sha256,
    }))
    .unwrap_or_default()
}

/// Whether the runtime's capabilities forbid the task's worktree directory.
/// Kept next to the gate because the rejection cancels the task.
pub(super) fn worktree_capability(headers: &axum::http::HeaderMap) -> bool {
    super::p0_daemon_claim::capability(headers, "local-worktree-v1")
}

/// Go's per-claim `ConnectedApps`: read from the task's own column, not the
/// runtime's, because the set was frozen when the task was enqueued.
pub(super) fn connected_apps_for_claim(
    raw_connected_apps: Option<&Value>,
    task: Uuid,
) -> Option<Value> {
    let bytes = raw_connected_apps.and_then(|value| match value {
        Value::Null => None,
        other => serde_json::to_vec(other).ok(),
    });
    super::daemon_claim_compose::parse_runtime_connected_apps_for_claim(bytes.as_deref(), task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settle_outcomes_are_distinct_from_their_base() {
        // A collision would make the two failure modes indistinguishable in the
        // logs that decide whether a task is requeued.
        for base in [
            "wakeup_unavailable",
            "runtime_access_denied",
            "invalid_task_identity",
        ] {
            assert_ne!(settle_outcome(base), base);
            assert!(settle_outcome(base).ends_with("_settle"));
        }
    }

    #[test]
    fn remote_mcp_token_requires_a_daemon_binding() {
        let runtime = ClaimRuntime {
            id: Uuid::nil(),
            workspace_id: Uuid::nil(),
            daemon_id: None,
            owner_id: None,
            visibility: "public".into(),
            shared_with: vec![],
            provider: String::new(),
        };
        assert!(
            remote_mcp_daemon_token_for_claim(false, &runtime)
                .unwrap()
                .is_none()
        );
        // No connections is fine without a daemon; connections is not.
        assert!(remote_mcp_daemon_token_for_claim(true, &runtime).is_err());
        let bound = ClaimRuntime {
            daemon_id: Some("d1".into()),
            ..runtime
        };
        let token = remote_mcp_daemon_token_for_claim(true, &bound)
            .unwrap()
            .unwrap();
        assert!(token.raw.starts_with("mdt_"));
        assert_eq!(token.raw.len(), 44);
        assert_eq!(token.hash, hash_token(&token.raw));
        assert_eq!(token.daemon_id, "d1");
    }

    #[test]
    fn connected_apps_accept_the_stored_json_shape() {
        let task = Uuid::now_v7();
        assert_eq!(connected_apps_for_claim(None, task), None);
        assert_eq!(connected_apps_for_claim(Some(&Value::Null), task), None);
        let apps = serde_json::json!([{"app": "gmail"}]);
        assert_eq!(connected_apps_for_claim(Some(&apps), task), Some(apps));
    }

    #[test]
    fn issue_snapshot_matches_the_pure_builder() {
        let snapshot = super::super::daemon_claim_pure::build_issue_state_snapshot("title", "body");
        let raw = issue_snapshot_bytes(&snapshot);
        let decoded: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded["v"], ISSUE_SNAPSHOT_VERSION);
        assert_eq!(decoded["title_sha256"], snapshot.title_sha256);
        assert_eq!(decoded["description_sha256"], snapshot.description_sha256);
    }
}
