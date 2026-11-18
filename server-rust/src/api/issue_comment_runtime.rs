//! Post-commit comment side effects: trigger routing, invocation admission,
//! coalescing, steering, cancellation fanout and survivor replay.
//!
//! Everything here runs AFTER the comment transaction committed, so none of it
//! may fail the request. The split matches Go exactly:
//!
//! * routing, steering and enqueue problems degrade to per-target outcomes;
//! * the durable refusal notices log and drop on error, because the refusal
//!   itself already happened and stays correct either way;
//! * a steer that loses its race keeps the trigger's ordinary queued/coalesced
//!   handling rather than dropping or misdirecting the comment.

use super::{
    ApiState,
    agent_invoke::{self, InvokeTarget, Invoker},
    agent_ready,
    attribution::Source,
    attribution_db,
    comment_dispatch::{DispatchStatus, ReasonCode},
    comment_enqueue::{self, EnqueueIssue, PgCommentEnqueue},
    comment_mention, comment_routing,
    comment_routing::{
        CommentAgentTrigger, CommentFacts, CommentMentionTarget, EnqueueResult, IssueFacts,
        RoutingOptions,
    },
    comment_routing_db::DbRouting,
    files_ops_sprint,
    issue_write::{
        CommentCancelledBatch, CommentCancelledTask, CommentChangedAction, CommentCreatedAction,
        CommentFeedback, CommentReplayInput, CommentSupplement, CommentTriggerOutcome,
        IssueCommentChangedEvent, IssueCommentCreatedEvent, IssueCommentLegacyRuntime,
        IssueCommentRuntime, IssueTaskCancelledEvent,
    },
    issue_write_resolution, runtime_chat_daemon, runtime_chat_delivery, timestamp,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

/// Agent-authored steering is capped per running turn — in total, and per
/// authoring agent — so two agents replying to each other mid-run can only
/// append a bounded number of messages. Past the cap the comment is an ordinary
/// trigger. Member steering is not capped.
const MAX_AGENT_BINDINGS: i32 = 10;
const MAX_BINDINGS_PER_AGENT: i32 = 3;

/// The issue columns this runtime needs: routing facts plus enqueue facts.
const ISSUE_SQL: &str = "\
SELECT id,workspace_id,assignee_type,assignee_id,priority,creator_type,creator_id,\
       origin_type,origin_id,triage_state,title,status
FROM issue WHERE id=$1 AND workspace_id=$2";

const LOCK_TASK_FOR_SUPPLEMENT_BIND_SQL: &str =
    "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE";

/// Go's `BindCommentTaskSupplement`. The lock above serializes against terminal
/// transitions; this statement then sees every binding an earlier lock holder
/// committed, so the per-turn caps cannot be raced past.
const BIND_COMMENT_TASK_SUPPLEMENT_SQL: &str = "\
WITH locked_task AS MATERIALIZED (
    SELECT t.id, t.issue_id, t.runtime_id
    FROM agent_task_queue t
    JOIN agent_runtime r ON r.id = t.runtime_id
    JOIN task_supplement_capability cap ON cap.task_id = t.id
    WHERE t.id=$1
      AND t.issue_id=$2
      AND t.agent_id=$3
      AND r.workspace_id=$4
      AND t.status='running'
      AND cap.capability='task-supplement-v1'
      AND r.provider IN ('codex','claude','grok','cursor','kiro')
      AND ($5::text <> 'agent' OR (
          (SELECT count(*) FROM task_supplement s
           JOIN comment c ON c.id = s.comment_id
           WHERE s.task_id = t.id AND c.author_type = 'agent') < $6::int
          AND (SELECT count(*) FROM task_supplement s
               JOIN comment c ON c.id = s.comment_id
               WHERE s.task_id = t.id AND c.author_type = 'agent'
                 AND s.author_id = $7) < $8::int
      ))
    FOR UPDATE OF t
), inserted AS (
    INSERT INTO task_supplement (
        task_id, workspace_id, issue_id, comment_id, author_id,
        client_request_id, status
    )
    SELECT t.id, $4, t.issue_id, $9, $7, $10, 'pending'
    FROM locked_task t
    RETURNING *
)
SELECT inserted.*, locked_task.runtime_id
FROM inserted
JOIN locked_task ON locked_task.id = inserted.task_id";

const RUNNING_TASK_FOR_AGENT_SQL: &str = "\
SELECT id FROM agent_task_queue
WHERE issue_id=$1 AND agent_id=$2 AND status='running'
ORDER BY started_at DESC NULLS LAST, id DESC
LIMIT 1";

const STEER_TASK_SQL: &str =
    "SELECT id,agent_id,issue_id FROM agent_task_queue WHERE id=ANY($1::uuid[])";

/// Root facts for a parent comment, projected like the routing lookup's row.
const PARENT_COMMENT_SQL: &str = "\
SELECT id,author_type,author_id,deleted_at,source_task_id,content FROM comment \
WHERE id=$1 AND workspace_id=$2";

/// A task author lends its originator only while the run is live (MUL-6951).
const TASK_ORIGINATOR_SQL: &str =
    "SELECT status,originator_user_id FROM agent_task_queue WHERE id=$1";

/// The cancelled batch re-read by id. Go hands its in-memory rows straight to
/// `BroadcastCancelledTasks`; Rust also needs `runtime_id` for the finish wake.
const CANCELLED_SQL: &str = "\
SELECT id,agent_id,runtime_id,issue_id,trigger_comment_id,coalesced_comment_ids \
FROM agent_task_queue WHERE id=ANY($1::uuid[])";

/// Go's `retriggerCancelledTaskSurvivors` selects live inputs of the batch in
/// `created_at`, then string-id, order — so `id::text` is the faithful tie-break.
const SURVIVOR_SQL: &str = "\
SELECT id,parent_id,author_type,author_id,source_task_id,content,suppressed_agent_ids,type \
FROM comment \
WHERE issue_id=$1 AND id=ANY($2::uuid[]) AND deleted_at IS NULL \
ORDER BY created_at, id::text";

const COMMENT_UPDATED_AT_SQL: &str =
    "SELECT updated_at FROM comment WHERE id=$1 AND workspace_id=$2";

const AGENT_INVOKE_FACTS_SQL: &str =
    "SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2";

const SUPPLEMENTS_SQL: &str = "\
SELECT s.task_id,s.status,s.failure_reason,s.delivered_at,t.agent_id \
FROM task_supplement s LEFT JOIN agent_task_queue t ON t.id=s.task_id \
WHERE s.workspace_id=$1 AND s.comment_id=$2 \
ORDER BY s.created_at,s.task_id";

/// Go's `util.MentionRe`, reproduced raw. The denial notice names a refused
/// target from the SUBMITTED TEXT only, and Go reads it with a plain regex —
/// including inside code fences, which the AST-based trigger parser ignores.
static GO_MENTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[@?(.+?)\]\(mention://(member|agent|squad|issue|all)/([0-9a-fA-F-]+|all)\)")
        .expect("go mention regex")
});

/// The comment type a delegated-failure recovery carries in Go
/// (`delegatedFailureRecoveryCommentType`).
const DELEGATED_RECOVERY_TYPE: &str = "progress_update";

pub struct PgCommentRuntime {
    state: ApiState,
}

impl PgCommentRuntime {
    pub fn new(state: ApiState) -> Self {
        Self { state }
    }

    pub fn shared(state: ApiState) -> Arc<Self> {
        Arc::new(Self::new(state))
    }

    async fn issue_row(&self, workspace_id: Uuid, issue_id: Uuid) -> Option<Row> {
        let client = self.state.pool.get().await.ok()?;
        client
            .query_opt(ISSUE_SQL, &[&issue_id, &workspace_id])
            .await
            .ok()
            .flatten()
    }
}

fn routing_issue(row: &Row) -> IssueFacts {
    IssueFacts {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        assignee_type: row.get("assignee_type"),
        assignee_id: row.get("assignee_id"),
        triage_state: row.get("triage_state"),
    }
}

fn enqueue_issue(row: &Row) -> EnqueueIssue {
    EnqueueIssue {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        assignee_id: row.get("assignee_id"),
        priority: row.get("priority"),
        creator_type: row.get("creator_type"),
        creator_id: row.get("creator_id"),
        origin_type: row.get("origin_type"),
        origin_id: row.get("origin_id"),
        triage_state: row.get("triage_state"),
    }
}

fn comment_facts(row: &Row) -> CommentFacts {
    CommentFacts {
        id: row.get("id"),
        author_type: row.get("author_type"),
        author_id: row.get("author_id"),
        deleted: row.get::<_, Option<DateTime<Utc>>>("deleted_at").is_some(),
        source_task_id: row.get("source_task_id"),
        content: row.get("content"),
    }
}

/// Go's `filterSuppressedCommentAgentTriggers`: the composer's explicit opt-out
/// wins over every routing branch, including the issue assignee.
fn filter_suppressed(
    triggers: Vec<CommentAgentTrigger>,
    suppressed: &[Uuid],
) -> Vec<CommentAgentTrigger> {
    if triggers.is_empty() || suppressed.is_empty() {
        return triggers;
    }
    let suppressed: HashSet<Uuid> = suppressed.iter().copied().collect();
    triggers
        .into_iter()
        .filter(|trigger| !suppressed.contains(&trigger.agent_id))
        .collect()
}

/// Resolve the parent best-effort: Go swallows a failed read and routes without
/// a parent rather than refusing the comment.
async fn parent_facts(
    db: &impl GenericClient,
    workspace_id: Uuid,
    parent: Option<Uuid>,
) -> Option<CommentFacts> {
    let parent = parent?;
    db.query_opt(PARENT_COMMENT_SQL, &[&parent, &workspace_id])
        .await
        .ok()
        .flatten()
        .map(|row| comment_facts(&row))
}

/// Go's `invokeOriginatorFromRequest`: a member is their own originator; an
/// agent inherits the originator of the live task named by `X-Task-ID`, and a
/// TERMINAL task lends nothing (MUL-6951).
async fn originator_from_action(
    db: &impl GenericClient,
    actor_type: &str,
    actor_id: Uuid,
    source_task_id: Option<Uuid>,
) -> Option<Uuid> {
    if actor_type == "member" {
        return Some(actor_id);
    }
    let task_id = source_task_id?;
    let row = db
        .query_opt(TASK_ORIGINATOR_SQL, &[&task_id])
        .await
        .ok()
        .flatten()?;
    if matches!(
        row.get::<_, String>("status").as_str(),
        "completed" | "failed" | "cancelled"
    ) {
        return None;
    }
    row.get("originator_user_id")
}

async fn load_supplements(
    db: &impl GenericClient,
    workspace_id: Uuid,
    comment_id: Uuid,
) -> Vec<CommentSupplement> {
    let Ok(rows) = db
        .query(SUPPLEMENTS_SQL, &[&workspace_id, &comment_id])
        .await
    else {
        return vec![];
    };
    rows.iter()
        .map(|row| CommentSupplement {
            task_id: row.get("task_id"),
            agent_id: row.get("agent_id"),
            status: row.get("status"),
            failure_reason: row.get("failure_reason"),
            delivered_at: row
                .get::<_, Option<DateTime<Utc>>>("delivered_at")
                .map(timestamp),
        })
        .collect()
}

/// Mention labels keyed `kind:id`, first occurrence winning, read from the
/// submitted text only. Resolving the target's real name here would disclose
/// information the caller may not have.
fn mention_labels(content: &str) -> HashMap<String, String> {
    let mut labels: HashMap<String, String> = HashMap::new();
    for caps in GO_MENTION_RE.captures_iter(content) {
        labels
            .entry(format!("{}:{}", &caps[2], &caps[3]))
            .or_insert_with(|| caps[1].to_string());
    }
    labels
}

/// Go's `escapeMarkdownInline`. `>` is deliberately absent: Go's replacer list
/// stops at `<`, and escaping more would change the written notice.
fn escape_markdown_inline(name: &str) -> String {
    let mut escaped = String::with_capacity(name.len());
    for c in name.chars() {
        if matches!(c, '\\' | '`' | '*' | '_' | '~' | '[' | ']' | '<') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

struct BindCommentSteer<'a> {
    task_id: Uuid,
    issue_id: Uuid,
    agent_id: Uuid,
    workspace_id: Uuid,
    author_type: &'a str,
    author_id: Uuid,
    comment_id: Uuid,
}

/// Go's `bindCommentSteer`: lock the turn, then bind, in one transaction.
/// `None` means nothing was bound — a terminal or unsupported turn, or a cap
/// refusal — and the caller must fall back to ordinary enqueue handling.
async fn bind_comment_steer(
    db: &mut tokio_postgres::Client,
    bind: BindCommentSteer<'_>,
) -> Option<Option<Uuid>> {
    let tx = db.transaction().await.ok()?;
    if tx
        .query_opt(LOCK_TASK_FOR_SUPPLEMENT_BIND_SQL, &[&bind.task_id])
        .await
        .ok()?
        .is_none()
    {
        return None;
    }
    let row = tx
        .query_opt(
            BIND_COMMENT_TASK_SUPPLEMENT_SQL,
            &[
                &bind.task_id,
                &bind.issue_id,
                &bind.agent_id,
                &bind.workspace_id,
                &bind.author_type,
                &MAX_AGENT_BINDINGS,
                &bind.author_id,
                &MAX_BINDINGS_PER_AGENT,
                &bind.comment_id,
                &bind.comment_id,
            ],
        )
        .await
        .ok()??;
    tx.commit().await.ok()?;
    Some(row.get::<_, Option<Uuid>>("runtime_id"))
}

/// Lock a live parent comment while pinning it to one issue. A parent that is
/// deleted, gone, or in another issue yields no row, which Go maps to
/// `pgx.ErrNoRows`.
async fn lock_live_comment(
    tx: &tokio_postgres::Transaction<'_>,
    comment: Uuid,
    workspace_id: Uuid,
) -> Result<Row, tokio_postgres::Error> {
    tx.query_one(
        "WITH locked_issue AS MATERIALIZED (
            SELECT issue.id FROM issue
            JOIN comment ON comment.issue_id = issue.id AND comment.workspace_id = issue.workspace_id
            WHERE comment.id=$1 AND comment.workspace_id=$2
            FOR NO KEY UPDATE OF issue
        ), issue_fence AS MATERIALIZED (
            SELECT count(*) AS locked_count FROM locked_issue
        )
        SELECT comment.* FROM comment CROSS JOIN issue_fence
        WHERE comment.id=$1 AND comment.workspace_id=$2
          AND comment.deleted_at IS NULL AND issue_fence.locked_count >= 0
        FOR NO KEY UPDATE OF comment",
        &[&comment, &workspace_id],
    )
    .await
}

/// One cancelled run, as the post-commit fanout needs it.
struct CancelledRow {
    id: Uuid,
    agent_id: Option<Uuid>,
    runtime_id: Option<Uuid>,
    issue_id: Option<Uuid>,
    trigger_comment_id: Option<Uuid>,
    coalesced_comment_ids: Vec<Uuid>,
}

impl CancelledRow {
    fn snapshot(&self) -> CommentCancelledTask {
        CommentCancelledTask {
            id: self.id,
            agent_id: self.agent_id,
            trigger_comment_id: self.trigger_comment_id,
            coalesced_comment_ids: self.coalesced_comment_ids.clone(),
        }
    }
}

/// Go's `targetsByComment`: which agents the cancelled batch was carrying for
/// each input. The excluded comment is the one just edited or deleted, so it is
/// never replayed against its own new text, and an agentless row carries no
/// receiver to retrigger.
fn survivor_targets(
    tasks: &[CommentCancelledTask],
    excluded: Option<Uuid>,
) -> BTreeMap<Uuid, BTreeSet<Uuid>> {
    let mut targets: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
    for task in tasks {
        let Some(agent_id) = task.agent_id else {
            continue;
        };
        for comment_id in task
            .coalesced_comment_ids
            .iter()
            .copied()
            .chain(task.trigger_comment_id)
        {
            if comment_id.is_nil() || Some(comment_id) == excluded {
                continue;
            }
            targets.entry(comment_id).or_default().insert(agent_id);
        }
    }
    targets
}

fn is_delegated_recovery(author_type: &str, kind: &str, source_task_id: Option<Uuid>) -> bool {
    author_type == "system" && kind == DELEGATED_RECOVERY_TYPE && source_task_id.is_some()
}

/// Everything one routing pass needs, kept in one struct so the Go call chain
/// stays readable at the call site.
struct Route<'a> {
    issue: &'a Row,
    workspace_id: Uuid,
    issue_id: Uuid,
    comment_id: Uuid,
    parent: Option<CommentFacts>,
    actor_type: &'a str,
    actor_id: Uuid,
    originator: Option<Uuid>,
    /// The run a task-authored comment came from. Routing uses it to resolve
    /// the authoring task's role; it is never the editor's own authority.
    source_task_id: Option<Uuid>,
    content: &'a str,
    suppressed: &'a [Uuid],
    steer_task_ids: &'a [Uuid],
    steer_all: bool,
}

impl PgCommentRuntime {
    /// Go's `noteBlockedRuntimeTargets`: one durable note per refused agent,
    /// deduped by agent id in target order.
    async fn note_blocked_runtime_targets(&self, issue: &Row, targets: &[CommentMentionTarget]) {
        let mut noted: HashSet<Uuid> = HashSet::new();
        for target in targets {
            let Some(notice) = target.unusable.as_ref() else {
                continue;
            };
            if !noted.insert(notice.agent_id) {
                continue;
            }
            let content = agent_ready::runtime_unusable_notice(&notice.agent_name, &notice.verdict);
            self.note_trigger_failure(issue, content, None).await;
        }
    }

    /// Go's `noteCommentInvocationDenied`: only "you may not invoke this" gets a
    /// note, because that is what the composer chip cannot explain. The
    /// timestamp is the stored comment's `updated_at`, exactly the row Go reads
    /// back after the write.
    async fn note_comment_invocation_denied(
        &self,
        issue: &Row,
        content: &str,
        comment_id: Uuid,
        outcomes: &[CommentTriggerOutcome],
    ) {
        let denied: Vec<&CommentTriggerOutcome> = outcomes
            .iter()
            .filter(|outcome| {
                outcome.status == DispatchStatus::Blocked.as_str()
                    && outcome.reason_code == ReasonCode::InvocationNotAllowed.as_str()
            })
            .collect();
        if denied.is_empty() {
            return;
        }
        let Ok(client) = self.state.pool.get().await else {
            return;
        };
        let Ok(Some(row)) = client
            .query_opt(
                COMMENT_UPDATED_AT_SQL,
                &[&comment_id, &issue.get::<_, Uuid>("workspace_id")],
            )
            .await
        else {
            return;
        };
        let at: DateTime<Utc> = row.get("updated_at");
        let labels = mention_labels(content);
        let mut body = format!(
            "These mentions did not start when the parent comment was submitted on {}:\n",
            at.format("%b %-d, %Y at %H:%M:%S UTC")
        );
        for outcome in denied {
            let label = labels
                .get(&format!("{}:{}", outcome.target_type, outcome.target_id))
                .cloned()
                .unwrap_or_else(|| format!("{} {}", outcome.target_type, outcome.target_id));
            body.push_str(&format!(
                "\n- **{}**: Target not found or permission not granted.",
                escape_markdown_inline(&label)
            ));
        }
        self.note_trigger_failure(issue, body, Some(comment_id))
            .await;
    }

    /// Go's `noteTriggerFailure`: a system comment written OUTSIDE the trigger
    /// pipeline, so the note itself starts no run. Failures log and drop — the
    /// refusal it explains already stands.
    async fn note_trigger_failure(&self, issue: &Row, content: String, parent: Option<Uuid>) {
        let issue_id: Uuid = issue.get("id");
        let workspace_id: Uuid = issue.get("workspace_id");
        let id = Uuid::now_v7();
        let params = [
            &issue_id as &(dyn tokio_postgres::types::ToSql + Sync),
            &workspace_id,
            &id,
            &"system",
            &Uuid::nil(),
            &content,
            &"system",
            &parent,
            &Option::<Uuid>::None,
            &Option::<Vec<Uuid>>::None,
        ];
        let Ok(mut client) = self.state.pool.get().await else {
            return;
        };
        let created = match parent {
            None => client
                .query_one(include_str!("issue_write_comment_create.sql"), &params)
                .await
                .ok(),
            Some(parent) => {
                let Ok(tx) = client.transaction().await else {
                    return;
                };
                let created = match lock_live_comment(&tx, parent, workspace_id).await {
                    // A parent that is gone, deleted, or in another issue is not
                    // a place to attach a notice.
                    Ok(row) if row.get::<_, Uuid>("issue_id") != issue_id => None,
                    Ok(_) => tx
                        .query_one(include_str!("issue_write_comment_create.sql"), &params)
                        .await
                        .ok(),
                    Err(_) => None,
                };
                match created {
                    Some(_) if tx.commit().await.is_err() => None,
                    other => other,
                }
            }
        };
        let Some(created) = created else {
            return;
        };
        let Some(sink) = self.state.issue_events.as_ref() else {
            return;
        };
        sink.publish_comment_created(IssueCommentCreatedEvent {
            workspace_id,
            actor_type: "system",
            actor_id: Uuid::nil(),
            payload: json!({
                "comment": issue_write_resolution::base_response(&created),
                "issue_title": issue.get::<_, String>("title"),
                "issue_assignee_type": issue.get::<_, Option<String>>("assignee_type"),
                "issue_assignee_id": issue.get::<_, Option<Uuid>>("assignee_id"),
                "issue_status": issue.get::<_, String>("status"),
                "issue_revision": created.get::<_, i64>("issue_revision"),
            }),
        });
    }

    /// Go's `steerCommentAgentTriggers`. A bound comment leaves the enqueue list:
    /// that turn receives it instead of a follow-up run. A chosen turn that has
    /// ended, has not started, or cannot take more input is never swapped for
    /// another turn of the same agent — the trigger keeps its ordinary handling,
    /// so losing the race never drops or misdirects the comment.
    async fn steer_agent_triggers(
        &self,
        issue: &Row,
        comment_id: Uuid,
        author_type: &str,
        author_id: Uuid,
        originator: Option<Uuid>,
        triggers: Vec<CommentAgentTrigger>,
        steer_task_ids: &[Uuid],
        steer_all: bool,
    ) -> (Vec<CommentAgentTrigger>, Vec<(Uuid, EnqueueResult)>) {
        if (steer_task_ids.is_empty() && !steer_all)
            || triggers.is_empty()
            || !matches!(author_type, "member" | "agent")
        {
            return (triggers, vec![]);
        }
        let issue_id: Uuid = issue.get("id");
        let workspace_id: Uuid = issue.get("workspace_id");
        let Ok(mut client) = self.state.pool.get().await else {
            return (triggers, vec![]);
        };
        let mut chosen: HashMap<Uuid, Uuid> = HashMap::new();
        if !steer_task_ids.is_empty() {
            let Ok(rows) = client
                .query(STEER_TASK_SQL, &[&steer_task_ids.to_vec()])
                .await
            else {
                return (triggers, vec![]);
            };
            for row in rows {
                let Some(agent_id) = row.get::<_, Option<Uuid>>("agent_id") else {
                    continue;
                };
                // Only a turn of THIS issue counts: a task id from elsewhere in
                // the workspace must not receive a comment it was never given.
                if row.get::<_, Option<Uuid>>("issue_id") == Some(issue_id) {
                    chosen.insert(agent_id, row.get::<_, Uuid>("id"));
                }
            }
        }
        let mut kept = Vec::with_capacity(triggers.len());
        let mut steered: Vec<(Uuid, EnqueueResult)> = vec![];
        for trigger in triggers {
            let task_id = match chosen.get(&trigger.agent_id).copied() {
                Some(task) => Some(task),
                None if steer_all => client
                    .query_opt(RUNNING_TASK_FOR_AGENT_SQL, &[&issue_id, &trigger.agent_id])
                    .await
                    .ok()
                    .flatten()
                    .map(|row| row.get::<_, Uuid>("id")),
                None => None,
            };
            let Some(task_id) = task_id else {
                kept.push(trigger);
                continue;
            };
            // Some routes (a worker's reply reaching its assigned leader) never
            // pass the invoke gate, so an agent author is re-checked here.
            if author_type == "agent"
                && !self
                    .agent_may_steer(
                        &**client,
                        trigger.agent_id,
                        author_id,
                        originator,
                        workspace_id,
                    )
                    .await
            {
                kept.push(trigger);
                continue;
            }
            let bound = bind_comment_steer(
                &mut client,
                BindCommentSteer {
                    task_id,
                    issue_id,
                    agent_id: trigger.agent_id,
                    workspace_id,
                    author_type,
                    author_id,
                    comment_id,
                },
            )
            .await;
            let Some(runtime_id) = bound else {
                kept.push(trigger);
                continue;
            };
            steered.push((
                trigger.agent_id,
                EnqueueResult {
                    status: DispatchStatus::Steered,
                    reason: ReasonCode::Steered,
                    // A steer starts no run, so no squad context executed and
                    // none may be reported.
                    exec_squad_id: None,
                },
            ));
            // Go notifies only through the daemon hub here; there is no
            // empty-claim fallback on this path.
            if let (Some(hub), Some(runtime)) = (&self.state.daemon_wake, runtime_id) {
                hub.notify_task_supplement_available(runtime, task_id);
            }
        }
        if !steered.is_empty() {
            self.publish_supplement_update(workspace_id, comment_id)
                .await;
        }
        (kept, steered)
    }

    /// Go's `agentMaySteer`: an agent may steer only a DIFFERENT agent it is
    /// allowed to invoke. Any read failure denies.
    async fn agent_may_steer(
        &self,
        db: &impl GenericClient,
        target_id: Uuid,
        actor_id: Uuid,
        originator: Option<Uuid>,
        workspace_id: Uuid,
    ) -> bool {
        if actor_id.is_nil() || target_id == actor_id {
            return false;
        }
        let Ok(Some(row)) = db
            .query_opt(AGENT_INVOKE_FACTS_SQL, &[&target_id, &workspace_id])
            .await
        else {
            return false;
        };
        agent_invoke::can_invoke(
            db,
            workspace_id,
            InvokeTarget {
                id: target_id,
                owner_id: row.get("owner_id"),
                permission_mode: &row.get::<_, String>("permission_mode"),
            },
            Invoker {
                actor_type: "agent",
                actor_id,
                originator,
            },
        )
        .await
        .unwrap_or(false)
    }

    /// Go's `publishCommentSupplementUpdate`: one `comment:updated` carrying the
    /// committed receipts, published only after a bind succeeded.
    async fn publish_supplement_update(&self, workspace_id: Uuid, comment_id: Uuid) {
        let Some(sink) = self.state.issue_events.as_ref() else {
            return;
        };
        let Ok(client) = self.state.pool.get().await else {
            return;
        };
        let Some(comment) =
            runtime_chat_delivery::comment_snapshot(&**client, workspace_id, comment_id).await
        else {
            return;
        };
        sink.publish_comment_changed(IssueCommentChangedEvent {
            event_type: "comment:updated",
            workspace_id,
            actor_type: "system",
            actor_id: Uuid::nil(),
            payload: json!({"comment": comment}),
        });
    }

    /// Go's `triggerTasksForComment`, in the same order: route, drop suppressed
    /// triggers, leave runtime refusals on the issue, steer, enqueue, fold the
    /// steered results over the enqueue results, then explain the invocation
    /// denials.
    async fn route_and_enqueue(&self, req: Route<'_>) -> CommentFeedback {
        if comment_mention::is_note_comment(req.content) {
            return CommentFeedback::default();
        }
        let plan = comment_routing::compute_comment_agent_triggers(
            &DbRouting::new(
                Arc::clone(&self.state.pool),
                req.workspace_id,
                req.issue_id,
                Some(req.comment_id),
                Some(req.comment_id),
            ),
            &routing_issue(req.issue),
            req.content,
            req.parent.as_ref(),
            req.actor_type,
            req.actor_id,
            RoutingOptions {
                thread_comment_id: Some(req.comment_id),
                exclude_trigger_comment_id: Some(req.comment_id),
                authoring_task_id: req.source_task_id,
                originator_user_id: req.originator,
            },
        )
        .await;
        let triggers = filter_suppressed(plan.triggers, req.suppressed);
        self.note_blocked_runtime_targets(req.issue, &plan.targets)
            .await;
        let (kept, steered) = self
            .steer_agent_triggers(
                req.issue,
                req.comment_id,
                req.actor_type,
                req.actor_id,
                req.originator,
                triggers,
                req.steer_task_ids,
                req.steer_all,
            )
            .await;
        let store = PgCommentEnqueue::new(
            &self.state,
            enqueue_issue(req.issue),
            Some(req.comment_id),
            None,
        );
        let mut enqueued = comment_enqueue::enqueue_comment_agent_triggers(&store, &kept).await;
        for (agent_id, result) in steered {
            match enqueued.iter_mut().find(|(id, _)| *id == agent_id) {
                Some(slot) => slot.1 = result,
                None => enqueued.push((agent_id, result)),
            }
        }
        let outcomes = comment_routing::comment_trigger_outcomes(&plan.targets, &enqueued);
        self.note_comment_invocation_denied(req.issue, req.content, req.comment_id, &outcomes)
            .await;
        CommentFeedback {
            trigger_outcomes: outcomes,
            supplements: vec![],
        }
    }

    async fn load_cancelled(&self, task_ids: &[Uuid]) -> Vec<CancelledRow> {
        if task_ids.is_empty() {
            return vec![];
        }
        let Ok(client) = self.state.pool.get().await else {
            return vec![];
        };
        let Ok(rows) = client.query(CANCELLED_SQL, &[&task_ids.to_vec()]).await else {
            return vec![];
        };
        rows.iter()
            .map(|row| CancelledRow {
                id: row.get("id"),
                agent_id: row.get("agent_id"),
                runtime_id: row.get("runtime_id"),
                issue_id: row.get("issue_id"),
                trigger_comment_id: row.get("trigger_comment_id"),
                coalesced_comment_ids: row
                    .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// Go's `BroadcastCancelledTasks`: revoke the run's tokens, publish one
    /// `task:cancelled`, reconcile each distinct agent (emitting `agent:status`
    /// only when the stored status actually moved), then wake each runtime once
    /// so released capacity is re-claimed. Go's task-terminal metrics have no
    /// Rust counterpart yet; the token revocation is the durable part.
    async fn fanout_cancelled(&self, workspace_id: Uuid, cancelled: &[CancelledRow]) {
        if cancelled.is_empty() {
            return;
        }
        let ids: Vec<Uuid> = cancelled.iter().map(|task| task.id).collect();
        if let Ok(client) = self.state.pool.get().await
            && let Err(err) = client
                .execute(
                    "DELETE FROM task_token WHERE task_id=ANY($1::uuid[])",
                    &[&ids],
                )
                .await
        {
            // Go logs and continues: the token's own expiry is the backstop.
            eprintln!("cancel task: failed to revoke task tokens: {err}");
        }
        let mut agents: BTreeSet<Uuid> = BTreeSet::new();
        for task in cancelled {
            let Some(agent_id) = task.agent_id else {
                continue;
            };
            if let Some(sink) = self.state.task_cancel_events.as_ref() {
                sink.publish_task_cancelled(IssueTaskCancelledEvent {
                    workspace_id,
                    task_id: task.id,
                    agent_id,
                    issue_id: task.issue_id,
                });
            }
            agents.insert(agent_id);
        }
        for agent_id in agents {
            let Ok(client) = self.state.pool.get().await else {
                continue;
            };
            // The SQL returns a row only when the status actually changed, so a
            // no-op reconcile publishes nothing.
            let Ok(Some(row)) = runtime_chat_daemon::reconcile_agent(&**client, agent_id).await
            else {
                continue;
            };
            self.state.events.publish(runtime_chat_daemon::system_event(
                "agent:status",
                row.get("workspace_id"),
                None,
                None,
                json!({"agent": runtime_chat_daemon::agent_snapshot(&row)}),
            ));
        }
        // Go's `notifyTasksFinished`: one wake per runtime, without a task id so
        // the daemon refreshes its schedule instead of claiming one row.
        let mut seen: HashSet<Uuid> = HashSet::new();
        for task in cancelled {
            if let Some(runtime) = task.runtime_id.filter(|id| seen.insert(*id)) {
                self.state.wake_daemon(Some(runtime), None);
            }
        }
    }

    /// The live, ordered survivors of a cancelled batch, scoped to the agents
    /// that were actually carrying each input.
    async fn load_survivors(
        &self,
        issue: &Row,
        targets: &BTreeMap<Uuid, BTreeSet<Uuid>>,
    ) -> Result<Vec<CommentReplayInput>, ()> {
        if targets.is_empty() {
            return Ok(vec![]);
        }
        let issue_id: Uuid = issue.get("id");
        let client = self.state.pool.get().await.map_err(|_| ())?;
        let ids: Vec<Uuid> = targets.keys().copied().collect();
        let rows = client
            .query(SURVIVOR_SQL, &[&issue_id, &ids])
            .await
            .map_err(|_| ())?;
        Ok(rows
            .iter()
            .filter_map(|row| {
                let comment_id: Uuid = row.get("id");
                let target_agent_ids = targets.get(&comment_id)?.iter().copied().collect();
                let author_type: String = row.get("author_type");
                let source_task_id: Option<Uuid> = row.get("source_task_id");
                Some(CommentReplayInput {
                    comment_id,
                    parent_id: row.get("parent_id"),
                    author_type,
                    author_id: row.get("author_id"),
                    source_task_id,
                    content: row.get("content"),
                    suppressed_agent_ids: row
                        .get::<_, Option<Vec<Uuid>>>("suppressed_agent_ids")
                        .unwrap_or_default(),
                    target_agent_ids,
                    delegated_failure_recovery: is_delegated_recovery(
                        &row.get::<_, String>("author_type"),
                        &row.get::<_, String>("type"),
                        source_task_id,
                    ),
                })
            })
            .collect())
    }

    /// Go's `retriggerCancelledTaskSurvivors` for the claim path: a task
    /// cancelled because its comment plan was stale still had queued work
    /// behind it, and dropping that work silently would lose the obligation the
    /// cancelled run was carrying. Survivors are recomputed exactly as the
    /// comment-mutation path does — live, ordered, scoped to the agents the
    /// cancelled batch was carrying — and `excluded` is nil here because the
    /// claim path cancels for a reason unrelated to a specific input.
    pub(super) async fn retrigger_cancelled_task_survivors(
        &self,
        workspace_id: Uuid,
        issue_id: Uuid,
        tasks: &[CommentCancelledTask],
        excluded: Option<Uuid>,
    ) {
        let targets = survivor_targets(tasks, excluded);
        if targets.is_empty() {
            return;
        }
        let Some(issue) = self.issue_row(workspace_id, issue_id).await else {
            eprintln!(
                "retrigger cancelled comment batch: issue {issue_id} vanished from workspace {workspace_id}"
            );
            return;
        };
        let inputs = match self.load_survivors(&issue, &targets).await {
            Ok(inputs) => inputs,
            Err(()) => {
                eprintln!(
                    "retrigger cancelled comment batch: failed to load survivors for issue {issue_id}"
                );
                return;
            }
        };
        self.replay_inputs(&issue, inputs).await;
    }

    /// Go's `retriggerCancelledTaskSurvivors`: each survivor is routed with ITS
    /// OWN stored authority — never the editor's — and only the agents that were
    /// carrying it are re-enqueued.
    async fn replay_inputs(&self, issue: &Row, inputs: Vec<CommentReplayInput>) {
        if inputs.is_empty() {
            return;
        }
        let workspace_id: Uuid = issue.get("workspace_id");
        let issue_id: Uuid = issue.get("id");
        let Ok(client) = self.state.pool.get().await else {
            return;
        };
        let db = &**client;
        for input in inputs {
            if comment_mention::is_note_comment(&input.content) {
                continue;
            }
            if input.delegated_failure_recovery {
                // Go routes these through its durable delegated-failure recovery
                // path, which Rust does not have yet. Skipping silently would
                // swallow the obligation the cancelled run was carrying, so say
                // plainly that this replay did not happen.
                eprintln!(
                    "delegated-failure recovery replay not implemented: comment {} on issue {}",
                    input.comment_id, issue_id
                );
                continue;
            }
            let parent = parent_facts(db, workspace_id, input.parent_id).await;
            let originator = if input.author_type == "member" {
                Some(input.author_id)
            } else {
                attribution_db::attribution_from_trigger_comment(
                    db,
                    workspace_id,
                    Some(input.comment_id),
                    Source::CommentSource,
                )
                .await
                .user_id
            };
            let plan = comment_routing::compute_comment_agent_triggers(
                &DbRouting::new(
                    Arc::clone(&self.state.pool),
                    workspace_id,
                    issue_id,
                    Some(input.comment_id),
                    Some(input.comment_id),
                ),
                &routing_issue(issue),
                &input.content,
                parent.as_ref(),
                &input.author_type,
                input.author_id,
                RoutingOptions {
                    thread_comment_id: Some(input.comment_id),
                    exclude_trigger_comment_id: Some(input.comment_id),
                    authoring_task_id: input.source_task_id,
                    originator_user_id: originator,
                },
            )
            .await;
            let scoped: Vec<CommentAgentTrigger> = plan
                .triggers
                .into_iter()
                .filter(|trigger| input.target_agent_ids.contains(&trigger.agent_id))
                .collect();
            if scoped.is_empty() {
                continue;
            }
            let store = PgCommentEnqueue::new(
                &self.state,
                enqueue_issue(issue),
                Some(input.comment_id),
                None,
            );
            comment_enqueue::enqueue_comment_agent_triggers(&store, &scoped).await;
        }
    }
}

#[async_trait]
impl IssueCommentRuntime for PgCommentRuntime {
    async fn created(&self, action: CommentCreatedAction) -> CommentFeedback {
        let Some(issue) = self.issue_row(action.workspace_id, action.issue_id).await else {
            return CommentFeedback::default();
        };
        let Ok(client) = self.state.pool.get().await else {
            return CommentFeedback::default();
        };
        let parent = parent_facts(&**client, action.workspace_id, action.parent_id).await;
        // Only human/cloud authors supply an originator; a task author inherits
        // one from its source task and never from the issue creator.
        let originator = match action.member_originator {
            Some(user) => Some(user),
            None => {
                originator_from_action(
                    &**client,
                    action.actor_type,
                    action.actor_id,
                    action.source_task_id,
                )
                .await
            }
        };
        let mut feedback = self
            .route_and_enqueue(Route {
                issue: &issue,
                workspace_id: action.workspace_id,
                issue_id: action.issue_id,
                comment_id: action.comment_id,
                parent,
                actor_type: action.actor_type,
                actor_id: action.actor_id,
                originator,
                source_task_id: action.source_task_id,
                content: &action.content,
                suppressed: &action.suppressed_agent_ids,
                steer_task_ids: &action.steer_task_ids,
                steer_all: action.steer_all,
            })
            .await;
        // Go reads the receipts whenever the request asked to steer at all, even
        // if every bind lost its race.
        if !action.steer_task_ids.is_empty() || action.steer_all {
            feedback.supplements =
                load_supplements(&**client, action.workspace_id, action.comment_id).await;
        }
        feedback
    }

    async fn changed(&self, action: CommentChangedAction) -> CommentFeedback {
        let comment = &action.comment;
        let Some(issue) = self.issue_row(comment.workspace_id, comment.issue_id).await else {
            return CommentFeedback::default();
        };
        let cancelled = self.load_cancelled(&action.cancelled_task_ids).await;
        self.fanout_cancelled(comment.workspace_id, &cancelled)
            .await;
        // Cancellation released the one-pending-task slot, so every OTHER input
        // the batch was carrying is replayed against its own stored authority.
        if !cancelled.is_empty() {
            let snapshots: Vec<CommentCancelledTask> =
                cancelled.iter().map(CancelledRow::snapshot).collect();
            let targets = survivor_targets(&snapshots, Some(comment.comment_id));
            match self.load_survivors(&issue, &targets).await {
                Ok(inputs) => self.replay_inputs(&issue, inputs).await,
                Err(()) => eprintln!(
                    "failed to select cancelled batch survivors for comment {}",
                    comment.comment_id
                ),
            }
        }
        if action.deleted {
            // The attachment rows are already gone; the ledger entry the delete
            // trigger queued is drained here because nothing else does.
            if !action.deleted_attachment_urls.is_empty() {
                let limit = action.deleted_attachment_urls.len().clamp(1, 100) as i64;
                if let Err(err) = files_ops_sprint::reconcile_cleanup(&self.state, limit).await {
                    eprintln!("comment delete: object cleanup failed: {err}");
                }
            }
            return CommentFeedback::default();
        }
        let Ok(client) = self.state.pool.get().await else {
            return CommentFeedback::default();
        };
        let parent = parent_facts(&**client, comment.workspace_id, comment.parent_id).await;
        self.route_and_enqueue(Route {
            issue: &issue,
            workspace_id: comment.workspace_id,
            issue_id: comment.issue_id,
            comment_id: comment.comment_id,
            parent,
            actor_type: comment.actor_type,
            actor_id: comment.actor_id,
            originator: comment.member_originator,
            source_task_id: comment.source_task_id,
            content: &comment.content,
            suppressed: &comment.suppressed_agent_ids,
            steer_task_ids: &[],
            steer_all: false,
        })
        .await
    }

    fn legacy_batches(&self) -> Option<&dyn IssueCommentLegacyRuntime> {
        Some(self)
    }
}

#[async_trait]
impl IssueCommentLegacyRuntime for PgCommentRuntime {
    async fn cancelled(&self, batch: &CommentCancelledBatch) {
        let cancelled = self.load_cancelled(&batch.task_ids).await;
        self.fanout_cancelled(batch.workspace_id, &cancelled).await;
    }

    async fn replay(
        &self,
        batch: &CommentCancelledBatch,
        excluded_comment_id: Option<Uuid>,
        inputs: Vec<CommentReplayInput>,
    ) {
        let Some(issue) = self.issue_row(batch.workspace_id, batch.issue_id).await else {
            return;
        };
        // The caller pre-selects the live survivors; the runtime still recomputes
        // each one's authority and intersects the triggers with the agents that
        // were actually carrying it.
        let allowed: HashSet<Uuid> = survivor_targets(&batch.tasks, excluded_comment_id)
            .into_keys()
            .collect();
        let inputs: Vec<CommentReplayInput> = inputs
            .into_iter()
            .filter(|input| allowed.contains(&input.comment_id))
            .collect();
        self.replay_inputs(&issue, inputs).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mention_labels_are_first_wins_and_raw() {
        let content = "\
[@Alice](mention://member/11111111-1111-1111-1111-111111111111) and \
[@Bob](mention://agent/22222222-2222-2222-2222-222222222222) then \
[@Alice again](mention://member/11111111-1111-1111-1111-111111111111)
```
[@Fenced](mention://squad/33333333-3333-3333-3333-333333333333)
```";
        let labels = mention_labels(content);
        assert_eq!(
            labels.get("member:11111111-1111-1111-1111-111111111111"),
            Some(&"Alice".to_string()),
            "the first occurrence wins"
        );
        assert_eq!(
            labels.get("agent:22222222-2222-2222-2222-222222222222"),
            Some(&"Bob".to_string())
        );
        // Go reads the submitted text with a plain regex, so a fenced mention
        // still contributes a label.
        assert_eq!(
            labels.get("squad:33333333-3333-3333-3333-333333333333"),
            Some(&"Fenced".to_string())
        );
        assert_eq!(labels.len(), 3);
    }

    #[test]
    fn escape_markdown_inline_matches_go_replacer() {
        assert_eq!(
            escape_markdown_inline(r"a\b`c*d_e~f[g]h<i>j"),
            r"a\\b\`c\*d\_e\~f\[g\]h\<i>j"
        );
        // `>` and `|` are not in Go's replacer list.
        assert_eq!(escape_markdown_inline("a>b|c"), "a>b|c");
    }

    #[test]
    fn survivor_targets_skip_excluded_and_agentless_tasks() {
        let (kept, excluded) = (Uuid::new_v4(), Uuid::new_v4());
        let agent = Uuid::new_v4();
        let tasks = vec![
            CommentCancelledTask {
                id: Uuid::new_v4(),
                agent_id: None,
                trigger_comment_id: Some(kept),
                coalesced_comment_ids: vec![],
            },
            CommentCancelledTask {
                id: Uuid::new_v4(),
                agent_id: Some(agent),
                trigger_comment_id: Some(excluded),
                coalesced_comment_ids: vec![kept, Uuid::nil()],
            },
            CommentCancelledTask {
                id: Uuid::new_v4(),
                agent_id: Some(agent),
                trigger_comment_id: Some(kept),
                coalesced_comment_ids: vec![],
            },
        ];
        let targets = survivor_targets(&tasks, Some(excluded));
        assert_eq!(targets.len(), 1, "only the surviving input remains");
        assert_eq!(
            targets.get(&kept).map(|agents| agents.len()),
            Some(1),
            "the same agent carrying two rows is one target"
        );
        assert_eq!(targets.get(&kept).unwrap().iter().next(), Some(&agent));
        assert!(!targets.contains_key(&excluded));
    }
}
