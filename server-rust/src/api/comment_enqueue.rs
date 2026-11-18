//! The comment → task enqueue ladder.
//!
//! Go splits this across `handler/comment.go` (`resolveCommentTriggerEnqueue`,
//! `mergeCommentIntoPendingTask`, `registerPlannedCommentForActiveTask`,
//! `enqueueSingleCommentTrigger`) and `service/task.go`
//! (`enqueueIssueTaskWithCommentPlan`, `enqueueMentionTaskWithCommentPlan`).
//! This module keeps the same two layers:
//!
//! * [`resolve_comment_trigger_enqueue`] is the bounded 4-attempt ladder. It is
//!   pure: every write goes through [`CommentEnqueueStore`], so the branch order
//!   — which is the actual contract — is unit-tested without a database.
//! * [`PgCommentEnqueue`] is the PostgreSQL half: one place that knows the
//!   merge/register/insert SQL and the post-commit broadcast order.
//!
//! The invariant the ladder protects: a success-shaped outcome (`coalesced`,
//! `deferred`, `queued`) is returned ONLY when the comment is provably attached
//! to a run that will address it — a completed merge, a completed planned-id
//! registration, or a completed insert. Never a prediction about the current
//! active-task snapshot. Non-convergence under sustained churn reports a truthful
//! `internal_error`; the comment row persists and a later trigger can still
//! cover it.

use super::{
    ApiState,
    attribution::Source,
    attribution_db::{self, AttributionFailClosed, IssueFacts},
    comment_dispatch::{DispatchStatus, ReasonCode},
    comment_routing::{CommentAgentTrigger, EnqueueResult, TriggerSource, decide_post_merge_miss},
    runtime_chat_daemon::system_event,
};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_postgres::{GenericClient, Row, error::SqlState};
use uuid::Uuid;

/// Why a fresh enqueue did not create a task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnqueueError {
    /// A sibling task won the unique-slot race. Benign: the caller re-loops and
    /// folds this comment into that sibling instead of dropping it.
    DuplicatePendingTask,
    /// No precise accountable human and the workspace refuses fallback.
    AttributionFailClosed,
    IssueInTriage,
    IssueHasNoAssignee,
    AgentLoad,
    AgentArchived,
    AgentNoRuntime,
    CreateTask,
}

/// Go's `commentEnqueueFailureReason`. Only the fail-closed refusal has its own
/// wire code; every other failure is an unclassified `internal_error`, including
/// `ErrIssueInTriage` — derived routes never reach the guard because routing
/// already declines to derive an executor on a Triage issue, and explicit
/// mentions bypass it by design (`OriginNamed`).
pub fn comment_enqueue_failure_reason(err: EnqueueError) -> ReasonCode {
    match err {
        EnqueueError::AttributionFailClosed => ReasonCode::AttributionBlocked,
        _ => ReasonCode::InternalError,
    }
}

/// How a pending-task merge attempt resolved (Go's `commentMergeResult`). A
/// refused or failed merge — even when it correctly fails closed by keeping the
/// original task and spawning no duplicate — must never be reported as a
/// success-shaped `coalesced`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommentMergeResult {
    /// The comment folded into the queued task.
    Succeeded,
    /// No queued task to fold into any more (it was claimed between the dedup
    /// check and now). The caller runs the active-task decision.
    NoPendingTask,
    /// Fail-closed attribution refused the re-stamp. The original task is kept
    /// and no duplicate is enqueued, but the re-attribution did not happen.
    AttributionBlocked,
    /// Unknown attribution/DB error. Fail closed, but nothing was written.
    Error,
}

/// Map a merge result that carries its own final outcome to the reported pair.
/// `None` only for [`CommentMergeResult::NoPendingTask`], which still needs the
/// active-task decision.
pub fn comment_merge_terminal_outcome(
    result: CommentMergeResult,
) -> Option<(DispatchStatus, ReasonCode)> {
    match result {
        CommentMergeResult::Succeeded => Some((DispatchStatus::Coalesced, ReasonCode::Coalesced)),
        CommentMergeResult::AttributionBlocked => {
            Some((DispatchStatus::Blocked, ReasonCode::AttributionBlocked))
        }
        CommentMergeResult::Error => Some((DispatchStatus::Blocked, ReasonCode::InternalError)),
        CommentMergeResult::NoPendingTask => None,
    }
}

/// The writes the ladder performs, one method per branch. Bound to one
/// (issue, agent, thread, head) scope by the implementation.
#[async_trait]
pub trait CommentEnqueueStore: Sync {
    /// Any non-terminal run for this (issue, agent) in this thread. The error
    /// must fail closed — "cannot confirm" is never "no run".
    async fn has_active(&self, agent_id: Uuid) -> Result<bool, ()>;
    /// Fold the comment into a same-head pre-claim task, re-attributing the run.
    async fn merge(&self, trigger: &CommentAgentTrigger) -> CommentMergeResult;
    /// Durably register the comment as a planned (undelivered) input on a
    /// same-head claim-receipt task.
    async fn register_planned(&self, agent_id: Uuid) -> Result<bool, ()>;
    /// Create a fresh run for this trigger.
    async fn enqueue_fresh(&self, trigger: &CommentAgentTrigger) -> Result<(), EnqueueError>;
}

/// Bounded retry count. A concurrent-churn backstop, not a prediction budget:
/// each duplicate is followed by a merge/register attempt, so the loop only
/// spins while a sibling keeps flipping state.
const MAX_ATTEMPTS: usize = 4;

/// Resolve ONE trigger into its final dispatch outcome.
///
/// Branch order is the contract; reordering changes behaviour, not just style.
pub async fn resolve_comment_trigger_enqueue(
    store: &impl CommentEnqueueStore,
    trigger: &CommentAgentTrigger,
) -> (DispatchStatus, ReasonCode) {
    let mut pending = trigger.already_pending;
    let mut lost_race = false;
    for _ in 0..MAX_ATTEMPTS {
        if pending {
            if let Some((status, reason)) =
                comment_merge_terminal_outcome(store.merge(trigger).await)
            {
                return (status, reason);
            }
            // Merge missed: there is no same-head pre-claim row to fold into.
            if !lost_race && !trigger.non_leader_agent_reply {
                // AlreadyPending path: the comment arrived AFTER its task, so the
                // task's completion reconcile covers it by timestamp. Defer to
                // that run, or enqueue fresh if it has since finished.
                let (outcome, enqueue_fresh) =
                    decide_post_merge_miss(store.has_active(trigger.agent_id).await);
                if let Some((status, reason)) = outcome {
                    return (status, reason);
                }
                debug_assert!(enqueue_fresh);
            } else {
                // A worker reply needs a RECORDED obligation: unlike an explicit
                // mention, a timestamp-only agent reply is not replayed (loop
                // safety). Same call covers a lost insert race, where the losing
                // comment can predate the winner and so sits outside the
                // completion reconcile window.
                match store.register_planned(trigger.agent_id).await {
                    Err(()) => return (DispatchStatus::Blocked, ReasonCode::InternalError),
                    Ok(true) => return (DispatchStatus::Deferred, ReasonCode::Deferred),
                    // No same-head claim-receipt task either. Fall through to a
                    // fresh enqueue — never defer off a snapshot of active tasks,
                    // because a snapshot proves state only NOW.
                    Ok(false) => {}
                }
            }
        }
        match store.enqueue_fresh(trigger).await {
            Ok(()) => return (DispatchStatus::Queued, ReasonCode::Queued),
            Err(EnqueueError::DuplicatePendingTask) => {
                // A sibling exists now. Re-resolve as pending so the next
                // attempt folds into it (queued) or registers on it (claimed).
                pending = true;
                lost_race = true;
                continue;
            }
            Err(err) => return (DispatchStatus::Blocked, comment_enqueue_failure_reason(err)),
        }
    }
    // Non-convergence under sustained churn: surface a truthful non-success
    // rather than a fabricated `deferred` that would drop the comment silently.
    (DispatchStatus::Blocked, ReasonCode::InternalError)
}

/// Resolve every trigger for this comment and key each result by executing
/// agent, in routing order. Several explicit mentions may resolve to the same
/// agent; the fan-out later gives each its own outcome from this one entry, so
/// coalescing never drops a named target.
pub async fn enqueue_comment_agent_triggers(
    store: &impl CommentEnqueueStore,
    triggers: &[CommentAgentTrigger],
) -> Vec<(Uuid, EnqueueResult)> {
    let mut results = Vec::with_capacity(triggers.len());
    for trigger in triggers {
        let (status, reason) = resolve_comment_trigger_enqueue(store, trigger).await;
        results.push((
            trigger.agent_id,
            EnqueueResult {
                status,
                reason,
                // The squad whose leader context the run actually carries, which
                // is what lets a DIFFERENT squad sharing this leader report
                // `coalesced` instead of claiming its own context ran.
                exec_squad_id: trigger.squad.as_ref().map(|squad| squad.id),
            },
        ));
    }
    results
}

/// Which enqueue entry point a trigger uses. `Named` skips the Triage guard: an
/// explicit mention is a direct request and runs even on a Triage issue, whereas
/// a derived run (the issue assignee reacting) does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOrigin {
    Derived,
    Named,
}

/// The issue columns the enqueue paths read.
#[derive(Clone, Debug)]
pub struct EnqueueIssue {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub assignee_id: Option<Uuid>,
    pub priority: String,
    pub creator_type: String,
    pub creator_id: Option<Uuid>,
    pub origin_type: Option<String>,
    pub origin_id: Option<Uuid>,
    pub triage_state: Option<String>,
}

impl EnqueueIssue {
    fn attribution_facts(&self) -> IssueFacts {
        IssueFacts {
            id: self.id,
            workspace_id: self.workspace_id,
            creator_type: self.creator_type.clone(),
            creator_id: self.creator_id,
            origin_type: self.origin_type.clone(),
            origin_id: self.origin_id,
        }
    }
}

/// `urgent 4, high 3, medium 2, low 1, everything else 0`.
pub fn priority_to_int(priority: &str) -> i32 {
    match priority {
        "urgent" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

/// Go's `isDuplicatePendingTaskErr`: the unique-slot race, recognised only when
/// the violation names one of the three pending-task indexes. A bare 23505 could
/// come from anywhere, and treating an unrelated conflict as benign would drop
/// the comment.
pub fn is_duplicate_pending_task_err(err: &tokio_postgres::Error) -> bool {
    let Some(db) = err.as_db_error() else {
        return false;
    };
    db.code() == &SqlState::UNIQUE_VIOLATION
        && matches!(
            db.constraint(),
            Some(
                "idx_one_pending_task_per_issue_agent"
                    | "idx_one_pending_task_per_issue_agent_v2"
                    | "idx_one_pending_task_per_issue_agent_thread"
            )
        )
}

/// The reviewed HEAD of the issue's linked PR, or `None` when there is no linked
/// PR. Keys both the head-scoped merge and the planned registration so a PR
/// pushed mid-review invalidates both together (TEN-356).
pub async fn resolve_issue_review_sha(db: &impl GenericClient, issue_id: Uuid) -> Option<String> {
    db.query_opt(HEAD_SHA_SQL, &[&issue_id])
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<_, String>(0))
        .filter(|sha| !sha.is_empty())
}

const HEAD_SHA_SQL: &str = "\
SELECT head_sha FROM (
    SELECT pr.head_sha AS head_sha, pr.state AS state, pr.pr_updated_at AS pr_updated_at
    FROM github_pull_request pr
    JOIN issue_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1 AND pr.head_sha <> ''
    UNION ALL
    SELECT pr.head_sha AS head_sha, pr.state AS state, pr.pr_updated_at AS pr_updated_at
    FROM vcs_pull_request pr
    JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1 AND pr.head_sha <> ''
) combined
ORDER BY (state IN ('open', 'draft')) DESC, pr_updated_at DESC
LIMIT 1";

/// `guardIssueNotInTriage`. A derived run on a Triage issue is refused: the
/// assignee there is a proposal, not an owner. A missing issue reads as "not in
/// triage" (the later insert then fails on its own terms), while a failed read is
/// an error rather than a permissive pass.
pub async fn guard_issue_not_in_triage(
    db: &impl GenericClient,
    issue_id: Option<Uuid>,
    origin: RunOrigin,
) -> Result<(), EnqueueError> {
    if origin == RunOrigin::Named {
        return Ok(());
    }
    let Some(issue_id) = issue_id else {
        return Ok(());
    };
    // A failed read is never a pass: it surfaces as the generic create failure
    // rather than silently permitting the write.
    let row = db
        .query_opt("SELECT triage_state FROM issue WHERE id=$1", &[&issue_id])
        .await
        .map_err(|_| EnqueueError::CreateTask)?;
    // A missing issue is not "in triage" — the insert then fails on its own terms
    // with a truthful create error instead of a misleading refusal.
    match row.and_then(|row| row.get::<_, Option<String>>("triage_state")) {
        Some(_) => Err(EnqueueError::IssueInTriage),
        None => Ok(()),
    }
}

/// One run's insert parameters, shared by the issue-assignee and mention paths.
struct FreshTask<'a> {
    agent_id: Uuid,
    runtime_id: Uuid,
    priority: i32,
    trigger_comment_id: Option<Uuid>,
    trigger_summary: Option<String>,
    force_fresh_session: bool,
    is_leader_task: bool,
    handoff_note: Option<&'a str>,
    squad_id: Option<Uuid>,
    head_sha: Option<String>,
    originator_user_id: Option<Uuid>,
    accountable_user_id: Option<Uuid>,
    originator_source: &'a str,
    delegated_from_task_id: Option<Uuid>,
    rule_version_id: Option<Uuid>,
    rerun_of_task_id: Option<Uuid>,
    trigger_evidence_kind: Option<&'a str>,
    trigger_evidence_ref_id: Option<Uuid>,
    id: Uuid,
}

const CREATE_AGENT_TASK: &str = r#"
INSERT INTO agent_task_queue (
    agent_id, runtime_id, issue_id, status, priority, trigger_comment_id,
    coalesced_comment_ids, trigger_summary, force_fresh_session, is_leader_task, handoff_note,
    squad_id, context, originator_user_id, accountable_user_id, runtime_mcp_overlay, runtime_connected_apps,
    originator_source, delegated_from_task_id, rule_version_id, rerun_of_task_id, trigger_evidence_kind, trigger_evidence_ref_id,
    workflow_step, id
)
SELECT
    $1, $2, $3, 'queued', $4, $5::uuid,
    COALESCE($6::uuid[], '{}'),
    $7::text,
    COALESCE($8::boolean, FALSE),
    COALESCE($9::boolean, FALSE),
    $10::text,
    $11::uuid,
    NULLIF(CASE
        WHEN COALESCE($12::text, '') <> ''
        THEN jsonb_build_object('head_sha', $12::text)
        ELSE '{}'::jsonb
    END || CASE
        WHEN COALESCE($13::boolean, FALSE)
        THEN '{"fresh_rerun":true}'::jsonb
        ELSE '{}'::jsonb
    END, '{}'::jsonb),
    $14::uuid,
    $15::uuid, $16::jsonb, $17::jsonb,
    $18::text, $19::uuid, $20::uuid, $21::uuid, $22::text, $23::uuid,
    (SELECT i.status FROM issue i WHERE i.id = $3),
    COALESCE($24::uuid, gen_random_uuid())
WHERE lock_task_owner_rows($1, $3, $2)
RETURNING *"#;

const MERGE_COMMENT_INTO_PENDING: &str = r#"
UPDATE agent_task_queue
SET coalesced_comment_ids = (
        SELECT COALESCE(array_agg(DISTINCT e), '{}')
        FROM unnest(array_append(coalesced_comment_ids, trigger_comment_id)) AS e
        WHERE e IS NOT NULL AND e <> $3::uuid
    ),
    trigger_comment_id = $3::uuid,
    trigger_summary = COALESCE($11::text, trigger_summary),
    originator_user_id = $4::uuid,
    accountable_user_id = $5::uuid,
    originator_source = $6::text,
    delegated_from_task_id = $7::uuid,
    rule_version_id = $8::uuid,
    trigger_evidence_kind = $9::text,
    trigger_evidence_ref_id = $10::uuid,
    runtime_mcp_overlay = $12::jsonb,
    runtime_connected_apps = $13::jsonb
WHERE id = (
    SELECT t.id FROM agent_task_queue t
    WHERE t.context->>'wakeup_id' IS NULL AND t.issue_id = $1
      AND t.agent_id = $2
      AND t.comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($3::uuid)
      AND (
          t.status = 'queued'
          OR (t.status = 'deferred' AND t.context->>'channel_issue_media_pending' = 'true')
      )
      AND (
          COALESCE($14::text, '') = ''
          OR t.context->>'head_sha' = $14::text
      )
    ORDER BY t.created_at DESC
    LIMIT 1
)
RETURNING *"#;

const REGISTER_PLANNED_COMMENT: &str = r#"
UPDATE agent_task_queue
SET coalesced_comment_ids = (
        SELECT COALESCE(array_agg(DISTINCT e), '{}')
        FROM unnest(array_append(coalesced_comment_ids, $1::uuid)) AS e
        WHERE e IS NOT NULL
    )
WHERE id = (
    SELECT t.id FROM agent_task_queue t
    WHERE t.context->>'wakeup_id' IS NULL AND t.issue_id = $2
      AND t.agent_id = $3
      AND t.comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($1::uuid)
      AND t.status IN ('dispatched', 'running', 'waiting_local_directory')
      AND (
          COALESCE($4::text, '') = ''
          OR t.context->>'head_sha' = $4::text
      )
    ORDER BY t.created_at DESC
    LIMIT 1
)
AND status IN ('dispatched', 'running', 'waiting_local_directory')
RETURNING id, coalesced_comment_ids"#;

/// PostgreSQL half of the ladder, bound to one issue and one trigger comment.
pub struct PgCommentEnqueue<'a> {
    state: &'a ApiState,
    issue: EnqueueIssue,
    trigger_comment_id: Option<Uuid>,
    actor_user_id: Option<Uuid>,
    head_sha: OnceCell<Option<String>>,
}

impl<'a> PgCommentEnqueue<'a> {
    pub fn new(
        state: &'a ApiState,
        issue: EnqueueIssue,
        trigger_comment_id: Option<Uuid>,
        actor_user_id: Option<Uuid>,
    ) -> Self {
        Self {
            state,
            issue,
            trigger_comment_id,
            actor_user_id,
            head_sha: OnceCell::new(),
        }
    }

    fn pool(&self) -> &Arc<Pool> {
        &self.state.pool
    }

    /// Resolved at most once: the common non-pending path never needs it, so it
    /// stays off the hot path, while merge and register share one value.
    async fn head_sha(&self) -> Option<String> {
        if let Some(cached) = self.head_sha.get() {
            return cached.clone();
        }
        let loaded = match self.pool().get().await {
            Ok(client) => resolve_issue_review_sha(&**client, self.issue.id).await,
            Err(_) => None,
        };
        let _ = self.head_sha.set(loaded.clone());
        loaded
    }

    /// The issue-assignee path: the executor is the issue's assignee.
    async fn enqueue_issue_task(&self) -> Result<(), EnqueueError> {
        let Some(assignee_id) = self.issue.assignee_id else {
            return Err(EnqueueError::IssueHasNoAssignee);
        };
        let client = self
            .pool()
            .get()
            .await
            .map_err(|_| EnqueueError::CreateTask)?;
        let db = &**client;
        guard_issue_not_in_triage(db, Some(self.issue.id), RunOrigin::Derived).await?;
        let agent = load_runnable_agent(db, assignee_id).await?;

        let attr = attribution_db::attribution_for_issue_task(
            db,
            &self.issue.attribution_facts(),
            self.trigger_comment_id,
            Source::CommentSource,
            self.actor_user_id,
        )
        .await;
        let attr = attribution_db::apply_attribution_fallback(
            db,
            attr,
            agent.workspace_id,
            agent.owner_id,
        )
        .await
        .map_err(|AttributionFailClosed| EnqueueError::AttributionFailClosed)?;
        let columns = super::attribution::attribution_columns(&attr);
        let task = FreshTask {
            agent_id: assignee_id,
            runtime_id: agent.runtime_id,
            priority: priority_to_int(&self.issue.priority),
            trigger_comment_id: self.trigger_comment_id,
            trigger_summary: attribution_db::build_comment_trigger_summary(
                db,
                self.issue.workspace_id,
                self.trigger_comment_id,
            )
            .await,
            force_fresh_session: false,
            is_leader_task: false,
            handoff_note: None,
            squad_id: None,
            head_sha: self.head_sha().await,
            originator_user_id: columns.originator_user_id,
            accountable_user_id: columns.accountable_user_id,
            originator_source: columns.originator_source,
            delegated_from_task_id: columns.delegated_from_task_id,
            rule_version_id: columns.rule_version_id,
            rerun_of_task_id: None,
            trigger_evidence_kind: columns.trigger_evidence_kind,
            trigger_evidence_ref_id: columns.trigger_evidence_ref_id,
            id: Uuid::now_v7(),
        };
        let row = insert_task(db, &self.issue, &task).await?;
        attribution_db::record_human_delegation(db, &attr, member_activity_enabled()).await;
        self.announce_queued(&row);
        Ok(())
    }

    /// The mention / thread-parent / squad-leader path: the executor is named by
    /// the trigger, so the assignee guard and the Triage guard differ.
    async fn enqueue_mention_task(
        &self,
        agent_id: Uuid,
        squad_id: Option<Uuid>,
        is_leader_task: bool,
        origin: RunOrigin,
    ) -> Result<(), EnqueueError> {
        let client = self
            .pool()
            .get()
            .await
            .map_err(|_| EnqueueError::CreateTask)?;
        let db = &**client;
        guard_issue_not_in_triage(db, Some(self.issue.id), origin).await?;
        let agent = load_runnable_agent(db, agent_id).await?;

        let attr = attribution_db::attribution_for_issue_task(
            db,
            &self.issue.attribution_facts(),
            self.trigger_comment_id,
            Source::Delegation,
            self.actor_user_id,
        )
        .await;
        let attr = attribution_db::apply_attribution_fallback(
            db,
            attr,
            agent.workspace_id,
            agent.owner_id,
        )
        .await
        .map_err(|AttributionFailClosed| EnqueueError::AttributionFailClosed)?;
        let columns = super::attribution::attribution_columns(&attr);
        let task = FreshTask {
            agent_id,
            runtime_id: agent.runtime_id,
            priority: priority_to_int(&self.issue.priority),
            trigger_comment_id: self.trigger_comment_id,
            trigger_summary: attribution_db::build_comment_trigger_summary(
                db,
                self.issue.workspace_id,
                self.trigger_comment_id,
            )
            .await,
            force_fresh_session: false,
            is_leader_task,
            handoff_note: None,
            squad_id,
            head_sha: self.head_sha().await,
            originator_user_id: columns.originator_user_id,
            accountable_user_id: columns.accountable_user_id,
            originator_source: columns.originator_source,
            delegated_from_task_id: columns.delegated_from_task_id,
            rule_version_id: columns.rule_version_id,
            rerun_of_task_id: None,
            trigger_evidence_kind: columns.trigger_evidence_kind,
            trigger_evidence_ref_id: columns.trigger_evidence_ref_id,
            id: Uuid::now_v7(),
        };
        let row = insert_task(db, &self.issue, &task).await?;
        attribution_db::record_human_delegation(db, &attr, member_activity_enabled()).await;
        self.announce_queued(&row);
        Ok(())
    }

    /// Order matters: broadcast first, wake the daemon second. The claim path
    /// emits its own `task:dispatch`, so publishing the queued event afterwards
    /// risks a dispatch reaching clients before the queued one.
    fn announce_queued(&self, row: &Row) {
        self.state.events.publish(system_event(
            "task:queued",
            self.issue.workspace_id,
            None,
            None,
            task_payload(row),
        ));
        self.state
            .wake_daemon(Some(row.get("runtime_id")), Some(row.get("id")));
    }
}

/// A run's announce payload, matching Go's `taskEvent`: the four core fields plus
/// `chat_session_id` when the task carries one (issue tasks do not).
fn task_payload(row: &Row) -> serde_json::Value {
    let mut payload = json!({
        "task_id": row.get::<_, Uuid>("id"),
        "agent_id": row.get::<_, Uuid>("agent_id"),
        "issue_id": row.get::<_, Option<Uuid>>("issue_id").map(|id| id.to_string()).unwrap_or_default(),
        "status": row.get::<_, String>("status"),
    });
    if let Some(session) = row.get::<_, Option<Uuid>>("chat_session_id") {
        payload["chat_session_id"] = json!(session);
    }
    payload
}

/// The `(workspace_id, owner_id)` pair the attribution fallback judges an agent
/// by. `(None, None)` on an unreadable or missing row, which makes the fallback
/// fail closed.
async fn load_agent_owner(db: &impl GenericClient, agent_id: Uuid) -> (Option<Uuid>, Option<Uuid>) {
    db.query_opt(
        "SELECT workspace_id, owner_id FROM agent WHERE id=$1",
        &[&agent_id],
    )
    .await
    .ok()
    .flatten()
    .map(|row| (row.get("workspace_id"), row.get("owner_id")))
    .unwrap_or((None, None))
}

/// Load the executor and prove it can run. Go checks exactly these two columns on
/// every enqueue: archived agents and agents without a runtime are refused, and
/// the workspace scope is NOT applied — a stale assignee still resolves, then
/// fails the run rather than silently enqueuing nothing.
struct RunnableAgent {
    runtime_id: Uuid,
    workspace_id: Option<Uuid>,
    owner_id: Option<Uuid>,
}

async fn load_runnable_agent(
    db: &impl GenericClient,
    agent_id: Uuid,
) -> Result<RunnableAgent, EnqueueError> {
    let Some(row) = db
        .query_opt("SELECT * FROM agent WHERE id=$1", &[&agent_id])
        .await
        .map_err(|_| EnqueueError::AgentLoad)?
    else {
        return Err(EnqueueError::AgentLoad);
    };
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(EnqueueError::AgentArchived);
    }
    let runtime_id = row
        .get::<_, Option<Uuid>>("runtime_id")
        .ok_or(EnqueueError::AgentNoRuntime)?;
    Ok(RunnableAgent {
        runtime_id,
        workspace_id: row.get("workspace_id"),
        owner_id: row.get("owner_id"),
    })
}

async fn insert_task(
    db: &impl GenericClient,
    issue: &EnqueueIssue,
    task: &FreshTask<'_>,
) -> Result<Row, EnqueueError> {
    let coalesced: Option<Vec<Uuid>> = None;
    let force_fresh: Option<bool> = Some(task.force_fresh_session);
    let is_leader: Option<bool> = Some(task.is_leader_task);
    let fresh_rerun: Option<bool> = Some(false);
    let row = db
        .query_opt(
            CREATE_AGENT_TASK,
            &[
                &task.agent_id,
                &task.runtime_id,
                &issue.id,
                &task.priority,
                &task.trigger_comment_id,
                &coalesced,
                &task.trigger_summary,
                &force_fresh,
                &is_leader,
                &task.handoff_note,
                &task.squad_id,
                &task.head_sha,
                &fresh_rerun,
                &task.originator_user_id,
                &task.accountable_user_id,
                // No Composio equivalent: both overlay columns stay NULL, which
                // is exactly what Go writes when the integration is absent.
                &None::<serde_json::Value>,
                &None::<serde_json::Value>,
                &task.originator_source,
                &task.delegated_from_task_id,
                &task.rule_version_id,
                &task.rerun_of_task_id,
                &task.trigger_evidence_kind,
                &task.trigger_evidence_ref_id,
                &task.id,
            ],
        )
        .await;
    match row {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(EnqueueError::CreateTask),
        Err(err) => {
            if is_duplicate_pending_task_err(&err) {
                Err(EnqueueError::DuplicatePendingTask)
            } else {
                Err(EnqueueError::CreateTask)
            }
        }
    }
}

/// The self-host telemetry switch, read once per enqueue from the environment.
fn member_activity_enabled() -> bool {
    attribution_db::member_activity_recording_enabled(std::env::var("DO_NOT_TRACK").ok().as_deref())
}

#[async_trait]
impl CommentEnqueueStore for PgCommentEnqueue<'_> {
    async fn has_active(&self, agent_id: Uuid) -> Result<bool, ()> {
        let client = self.pool().get().await.map_err(|_| ())?;
        let row = client
            .query_one(
                super::comment_routing_db::HAS_ACTIVE_SQL,
                &[&self.issue.id, &agent_id, &self.trigger_comment_id],
            )
            .await
            .map_err(|_| ())?;
        Ok(row.get(0))
    }

    async fn merge(&self, trigger: &CommentAgentTrigger) -> CommentMergeResult {
        // An issue-assignee reaction re-attributes as `comment_source`; every
        // other hop is a delegation.
        let is_mention = trigger.source != TriggerSource::IssueAssignee;
        let Ok(client) = self.pool().get().await else {
            return CommentMergeResult::Error;
        };
        let db = &**client;
        // The merge RE-STAMPS the run, so the fallback gate needs the target
        // agent's owner and workspace — the same pair the fresh-enqueue path
        // passes — not the trigger's. An unreadable agent row leaves the fallback
        // with no workspace, which fails closed rather than re-stamping a queued
        // run to an owner nobody can name.
        let (agent_workspace, agent_owner) = load_agent_owner(db, trigger.agent_id).await;
        let attr = attribution_db::attribution_for_merged_comment(
            db,
            self.issue.workspace_id,
            self.trigger_comment_id,
            is_mention,
            agent_workspace,
            agent_owner,
        )
        .await;
        let attr = match attr {
            Ok(attr) => attr,
            Err(AttributionFailClosed) => return CommentMergeResult::AttributionBlocked,
        };
        let columns = super::attribution::attribution_columns(&attr);
        let summary = attribution_db::build_comment_trigger_summary(
            db,
            self.issue.workspace_id,
            self.trigger_comment_id,
        )
        .await;
        let head_sha = self.head_sha().await;
        let row = db
            .query_opt(
                MERGE_COMMENT_INTO_PENDING,
                &[
                    &self.issue.id,
                    &trigger.agent_id,
                    &self.trigger_comment_id,
                    &columns.originator_user_id,
                    &columns.accountable_user_id,
                    &columns.originator_source,
                    &columns.delegated_from_task_id,
                    &columns.rule_version_id,
                    &columns.trigger_evidence_kind,
                    &columns.trigger_evidence_ref_id,
                    &summary,
                    &None::<serde_json::Value>,
                    &None::<serde_json::Value>,
                    &head_sha,
                ],
            )
            .await;
        match row {
            Ok(Some(row)) => {
                attribution_db::record_human_delegation(db, &attr, member_activity_enabled()).await;
                // The run's status is unchanged, so no lifecycle event would
                // otherwise tell clients it now answers this comment.
                self.state.events.publish(system_event(
                    "task:coalesced",
                    self.issue.workspace_id,
                    None,
                    None,
                    task_payload(&row),
                ));
                CommentMergeResult::Succeeded
            }
            // No pre-claim task to fold into any more.
            Ok(None) => CommentMergeResult::NoPendingTask,
            // Unknown failure: the pending task most likely still exists, so do
            // NOT risk a duplicate — but the merge did not happen, so this is not
            // a success.
            Err(_) => CommentMergeResult::Error,
        }
    }

    async fn register_planned(&self, agent_id: Uuid) -> Result<bool, ()> {
        let comment_id = self.trigger_comment_id.ok_or(())?;
        let client = self.pool().get().await.map_err(|_| ())?;
        let head_sha = self.head_sha().await;
        let row = client
            .query_opt(
                REGISTER_PLANNED_COMMENT,
                &[&comment_id, &self.issue.id, &agent_id, &head_sha],
            )
            .await
            .map_err(|_| ())?;
        Ok(row.is_some())
    }

    async fn enqueue_fresh(&self, trigger: &CommentAgentTrigger) -> Result<(), EnqueueError> {
        match trigger.source {
            TriggerSource::IssueAssignee => match &trigger.squad {
                Some(squad) => {
                    self.enqueue_mention_task(
                        trigger.agent_id,
                        Some(squad.id),
                        true,
                        RunOrigin::Derived,
                    )
                    .await
                }
                None => self.enqueue_issue_task().await,
            },
            // Routing guarantees a squad for this source; without one, fall back
            // to a plain named mention rather than fabricating a leader task with
            // no squad context.
            TriggerSource::MentionSquadLeader => match &trigger.squad {
                Some(squad) => {
                    self.enqueue_mention_task(
                        trigger.agent_id,
                        Some(squad.id),
                        true,
                        RunOrigin::Named,
                    )
                    .await
                }
                None => {
                    self.enqueue_mention_task(trigger.agent_id, None, false, RunOrigin::Named)
                        .await
                }
            },
            TriggerSource::MentionAgent => {
                self.enqueue_mention_task(trigger.agent_id, None, false, RunOrigin::Named)
                    .await
            }
            // The squad is set on these two only when routing already proved a
            // leader role to continue.
            TriggerSource::ThreadParent | TriggerSource::ConversationContinuation => {
                match &trigger.squad {
                    Some(squad) => {
                        self.enqueue_mention_task(
                            trigger.agent_id,
                            Some(squad.id),
                            true,
                            RunOrigin::Named,
                        )
                        .await
                    }
                    None => {
                        self.enqueue_mention_task(trigger.agent_id, None, false, RunOrigin::Named)
                            .await
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn agent() -> Uuid {
        Uuid::from_u128(0xa1)
    }

    fn trigger(source: TriggerSource, pending: bool) -> CommentAgentTrigger {
        CommentAgentTrigger {
            agent_id: agent(),
            source,
            squad: None,
            already_pending: pending,
            non_leader_agent_reply: false,
        }
    }

    /// Records the calls the ladder made, so the branch order is observable.
    #[derive(Default)]
    struct Calls {
        merged: usize,
        registered: usize,
        enqueued: usize,
    }

    struct Fake {
        merge: Vec<CommentMergeResult>,
        active: Result<bool, ()>,
        register: Vec<Result<bool, ()>>,
        enqueue: Vec<Result<(), EnqueueError>>,
        calls: Mutex<Calls>,
    }

    impl Fake {
        fn new() -> Self {
            Self {
                merge: Vec::new(),
                active: Ok(false),
                register: Vec::new(),
                enqueue: Vec::new(),
                calls: Mutex::new(Calls::default()),
            }
        }
    }

    #[async_trait]
    impl CommentEnqueueStore for Fake {
        async fn has_active(&self, _agent_id: Uuid) -> Result<bool, ()> {
            self.active
        }

        async fn merge(&self, _trigger: &CommentAgentTrigger) -> CommentMergeResult {
            let mut calls = self.calls.lock().unwrap();
            let index = calls.merged;
            calls.merged += 1;
            self.merge
                .get(index)
                .copied()
                .unwrap_or(CommentMergeResult::NoPendingTask)
        }

        async fn register_planned(&self, _agent_id: Uuid) -> Result<bool, ()> {
            let mut calls = self.calls.lock().unwrap();
            let index = calls.registered;
            calls.registered += 1;
            self.register.get(index).copied().unwrap_or(Ok(false))
        }

        async fn enqueue_fresh(&self, _trigger: &CommentAgentTrigger) -> Result<(), EnqueueError> {
            let mut calls = self.calls.lock().unwrap();
            let index = calls.enqueued;
            calls.enqueued += 1;
            self.enqueue.get(index).copied().unwrap_or(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_pending_merge_success_is_coalesced() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::Succeeded];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Coalesced, ReasonCode::Coalesced)
        );
        let calls = fake.calls.lock().unwrap();
        assert_eq!((calls.merged, calls.enqueued), (1, 0));
    }

    #[tokio::test]
    async fn a_refused_merge_is_attribution_blocked_not_success() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::AttributionBlocked];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Blocked, ReasonCode::AttributionBlocked)
        );
        assert_eq!(fake.calls.lock().unwrap().enqueued, 0);
    }

    #[tokio::test]
    async fn a_failed_merge_is_internal_error_not_success() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::Error];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Blocked, ReasonCode::InternalError)
        );
        assert_eq!(fake.calls.lock().unwrap().enqueued, 0);
    }

    #[tokio::test]
    async fn a_merge_miss_with_an_active_run_defers() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.active = Ok(true);
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Deferred, ReasonCode::Deferred)
        );
    }

    #[tokio::test]
    async fn a_merge_miss_whose_lookup_fails_fails_closed() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.active = Err(());
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Blocked, ReasonCode::InternalError)
        );
        // Never enqueue a possibly-colliding duplicate on an unknown state.
        assert_eq!(fake.calls.lock().unwrap().enqueued, 0);
    }

    #[tokio::test]
    async fn a_merge_miss_with_no_active_run_enqueues_fresh() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.active = Ok(false);
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Queued, ReasonCode::Queued)
        );
    }

    #[tokio::test]
    async fn a_worker_reply_registers_a_planned_input_instead_of_deferring_by_timestamp() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.register = vec![Ok(true)];
        let mut reply = trigger(TriggerSource::ThreadParent, true);
        reply.non_leader_agent_reply = true;
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &reply).await,
            (DispatchStatus::Deferred, ReasonCode::Deferred)
        );
        // It must not consult the active-task snapshot at all.
        assert_eq!(fake.calls.lock().unwrap().registered, 1);
    }

    #[tokio::test]
    async fn a_failed_planned_registration_is_internal_error() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.register = vec![Err(())];
        let mut reply = trigger(TriggerSource::ThreadParent, true);
        reply.non_leader_agent_reply = true;
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &reply).await,
            (DispatchStatus::Blocked, ReasonCode::InternalError)
        );
    }

    #[tokio::test]
    async fn an_unregistered_worker_reply_falls_through_to_a_fresh_enqueue() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.register = vec![Ok(false)];
        let mut reply = trigger(TriggerSource::ThreadParent, true);
        reply.non_leader_agent_reply = true;
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &reply).await,
            (DispatchStatus::Queued, ReasonCode::Queued)
        );
    }

    #[tokio::test]
    async fn losing_the_insert_race_reloops_and_coalesces() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::Succeeded];
        fake.enqueue = vec![Err(EnqueueError::DuplicatePendingTask)];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::MentionAgent, false))
                .await,
            (DispatchStatus::Coalesced, ReasonCode::Coalesced)
        );
        let calls = fake.calls.lock().unwrap();
        assert_eq!((calls.enqueued, calls.merged), (1, 1));
    }

    #[tokio::test]
    async fn a_lost_race_registers_a_planned_input_on_a_claimed_winner() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask];
        fake.register = vec![Ok(true)];
        fake.enqueue = vec![Err(EnqueueError::DuplicatePendingTask)];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::MentionAgent, false))
                .await,
            (DispatchStatus::Deferred, ReasonCode::Deferred)
        );
    }

    #[tokio::test]
    async fn sustained_duplicates_report_non_convergence_not_a_fake_deferral() {
        let mut fake = Fake::new();
        fake.merge = vec![CommentMergeResult::NoPendingTask; MAX_ATTEMPTS];
        fake.active = Ok(false);
        fake.enqueue = vec![Err(EnqueueError::DuplicatePendingTask); MAX_ATTEMPTS];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::IssueAssignee, true))
                .await,
            (DispatchStatus::Blocked, ReasonCode::InternalError)
        );
        assert_eq!(fake.calls.lock().unwrap().enqueued, MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn a_fail_closed_enqueue_is_attribution_blocked() {
        let mut fake = Fake::new();
        fake.enqueue = vec![Err(EnqueueError::AttributionFailClosed)];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::MentionAgent, false))
                .await,
            (DispatchStatus::Blocked, ReasonCode::AttributionBlocked)
        );
    }

    #[tokio::test]
    async fn an_unclassified_enqueue_failure_is_internal_error() {
        let mut fake = Fake::new();
        fake.enqueue = vec![Err(EnqueueError::IssueInTriage)];
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::MentionAgent, false))
                .await,
            (DispatchStatus::Blocked, ReasonCode::InternalError)
        );
    }

    #[tokio::test]
    async fn a_non_pending_trigger_enqueues_without_touching_the_queue() {
        let fake = Fake::new();
        assert_eq!(
            resolve_comment_trigger_enqueue(&fake, &trigger(TriggerSource::MentionAgent, false))
                .await,
            (DispatchStatus::Queued, ReasonCode::Queued)
        );
        let calls = fake.calls.lock().unwrap();
        assert_eq!((calls.merged, calls.registered), (0, 0));
    }

    #[test]
    fn merge_results_map_to_their_honest_outcome() {
        assert_eq!(
            comment_merge_terminal_outcome(CommentMergeResult::Succeeded),
            Some((DispatchStatus::Coalesced, ReasonCode::Coalesced))
        );
        assert_eq!(
            comment_merge_terminal_outcome(CommentMergeResult::Error),
            Some((DispatchStatus::Blocked, ReasonCode::InternalError))
        );
        // The one non-terminal result: the caller still has to decide.
        assert_eq!(
            comment_merge_terminal_outcome(CommentMergeResult::NoPendingTask),
            None
        );
    }

    #[test]
    fn priorities_match_the_go_scale() {
        assert_eq!(priority_to_int("urgent"), 4);
        assert_eq!(priority_to_int("high"), 3);
        assert_eq!(priority_to_int("medium"), 2);
        assert_eq!(priority_to_int("low"), 1);
        assert_eq!(priority_to_int("none"), 0);
        assert_eq!(priority_to_int(""), 0);
    }
}
