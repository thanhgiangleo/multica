//! Comment → agent routing decision tree.
//!
//! This is the pure decision half of Go's `computeCommentAgentTriggers`
//! (`server/internal/handler/comment.go`). It answers exactly one question —
//! which agents does this comment trigger, and what is the outcome of every
//! target the author named explicitly — given a read-only snapshot supplied by
//! [`CommentRoutingLookup`].
//!
//! Keeping the decision separate from the enqueue ladder matters for parity: the
//! ladder performs writes (merge, planned registration, fresh insert) and can
//! only be exercised against a database, while the branch order below decides
//! *which* of those writes ever happens. Every fail-closed rule lives here:
//!
//! * an `@agent`/`@squad` mention wins over `@all`, and `@all` only suppresses
//!   the implicit fallbacks (assignee, thread parent, conversation);
//! * the invocation gate is evaluated BEFORE archived/runtime state is read, so
//!   a caller who cannot invoke a private target learns only the generic
//!   `invocation_not_allowed` and cannot enumerate the target's state;
//! * a malformed mention id is `target_unavailable`, a well-formed but
//!   unresolvable agent id is `invocation_not_allowed`;
//! * agent actors never take the member conversation routes, and a member's
//!   plain reply to another member never wakes the assignee;
//! * Triage issues have no derived executor (explicit mentions still run);
//! * a squad leader is not re-triggered by its own comment unless its latest
//!   task on the issue was a same-squad worker turn.
//!
//! The `deferred`/`blocked` honesty rules for a suppressed leader
//! ([`decide_suppressed_leader_outcome`]) and for a merge miss
//! ([`decide_post_merge_miss`]) are pure too, and are pinned by unit tests.

use super::agent_ready::AgentVerdict;
use super::comment_dispatch::{DispatchStatus, ReasonCode};
use super::comment_mention::{Mention, has_agent_or_squad, has_all, has_member, parse_mentions};
use async_trait::async_trait;
use uuid::Uuid;

/// Trigger origin. The value is internal: it selects which enqueue entry point
/// the ladder uses, and whether a coalescing merge is a delegation hop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerSource {
    IssueAssignee,
    MentionAgent,
    MentionSquadLeader,
    ThreadParent,
    ConversationContinuation,
}

impl TriggerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IssueAssignee => "issue_assignee",
            Self::MentionAgent => "mention_agent",
            Self::MentionSquadLeader => "mention_squad_leader",
            Self::ThreadParent => "thread_parent",
            Self::ConversationContinuation => "conversation_continuation",
        }
    }

    /// An issue-assignee reaction re-attributes the run as `comment_source`;
    /// every other hop is a delegation.
    pub fn is_delegation(self) -> bool {
        self != Self::IssueAssignee
    }
}

#[derive(Clone, Debug)]
pub struct AgentFacts {
    pub id: Uuid,
    /// Only the blocked-runtime notice needs this: the notice names the agent
    /// it refused, and the target row already carries its id.
    pub name: String,
    pub runtime_id: Option<Uuid>,
    pub archived: bool,
    /// Needed by the readiness verdict, which judges the *agent owner* against
    /// the machine's share list rather than the caller.
    pub owner_id: Option<Uuid>,
    /// `private` | `public_to`. Carried on the facts so the invocation gate and
    /// the readiness verdict never re-read the row the caller already loaded.
    pub permission_mode: String,
}

impl AgentFacts {
    /// Runnable target: bound to a runtime and not archived. Go checks exactly
    /// these two columns on every non-mention route.
    fn runnable(&self) -> bool {
        self.runtime_id.is_some() && !self.archived
    }
}

#[derive(Clone, Debug)]
pub struct SquadFacts {
    pub id: Uuid,
    pub leader_id: Uuid,
    pub archived: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TaskFacts {
    pub agent_id: Option<Uuid>,
    pub issue_id: Option<Uuid>,
    pub is_leader_task: bool,
    pub squad_id: Option<Uuid>,
    pub trigger_comment_id: Option<Uuid>,
    pub coalesced_comment_ids: Vec<Uuid>,
}

impl TaskFacts {
    /// Whether the run already consumed this comment as its trigger or as a
    /// coalesced input.
    pub fn covers(&self, comment_id: Uuid) -> bool {
        self.trigger_comment_id == Some(comment_id)
            || self.coalesced_comment_ids.contains(&comment_id)
    }
}

#[derive(Clone, Debug)]
pub struct CommentFacts {
    pub id: Uuid,
    pub author_type: String,
    pub author_id: Option<Uuid>,
    pub deleted: bool,
    pub source_task_id: Option<Uuid>,
    pub content: String,
}

#[derive(Clone, Copy, Debug)]
pub struct TaskRole {
    pub is_leader_task: bool,
    pub squad_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug)]
pub struct RootOwner {
    pub agent_id: Uuid,
    pub squad_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct IssueFacts {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<Uuid>,
    /// Present when the issue sits in Triage, where the assignee is a proposal
    /// rather than an owner.
    pub triage_state: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RoutingOptions {
    /// The thread a new comment will belong to; a persisted comment's own id
    /// (see [`RoutingOptions::for_new_comment`]).
    pub thread_comment_id: Option<Uuid>,
    /// The comment being created. Excluded from its own dedup and thread-owner
    /// lookups.
    pub exclude_trigger_comment_id: Option<Uuid>,
    /// Server-trusted task that wrote an agent comment (X-Task-ID).
    pub authoring_task_id: Option<Uuid>,
    /// Top-of-chain human for A2A invocation. Members are their own originator,
    /// so this may be absent for member-authored triggers.
    pub originator_user_id: Option<Uuid>,
}

impl RoutingOptions {
    /// Options for the comment being created: it is its own thread head until a
    /// parent is supplied.
    pub fn for_new_comment(comment_id: Uuid) -> Self {
        Self {
            thread_comment_id: Some(comment_id),
            exclude_trigger_comment_id: Some(comment_id),
            ..Self::default()
        }
    }
}

/// Read-only facts the routing decision needs. Every method is workspace-scoped
/// where Go is, so a mention can never resolve an entity from another workspace.
#[async_trait]
pub trait CommentRoutingLookup: Sync {
    async fn agent(&self, workspace_id: Uuid, agent_id: Uuid) -> Option<AgentFacts>;
    async fn squad(&self, workspace_id: Uuid, squad_id: Uuid) -> Option<SquadFacts>;
    async fn can_invoke(
        &self,
        agent: &AgentFacts,
        actor_type: &str,
        actor_id: Uuid,
        originator: Option<Uuid>,
    ) -> bool;
    /// The shared readiness verdict. `None` means the run may proceed — an
    /// offline runtime still queues, so only a genuinely blocked target answers
    /// `Some`. The full verdict travels with the refusal because the durable
    /// notice is built from the daemon's own repair detail; reducing it to a
    /// code here would strand that text.
    async fn readiness_block(&self, agent: &AgentFacts) -> Option<AgentVerdict>;
    /// Pending (pre-claim) task for this issue+agent in the relevant thread.
    async fn has_pending(&self, agent_id: Uuid) -> Result<bool, ()>;
    /// Non-terminal task; the error must fail closed, never read as "no run".
    async fn has_active(&self, agent_id: Uuid, thread_comment_id: Option<Uuid>)
    -> Result<bool, ()>;
    async fn latest_task_role(&self, agent_id: Uuid) -> Result<TaskRole, ()>;
    async fn task(&self, task_id: Uuid) -> Option<TaskFacts>;
    async fn thread_root(&self, comment_id: Uuid) -> Option<CommentFacts>;
    async fn conversation_root_owners(&self, root_id: Uuid) -> Result<Vec<RootOwner>, ()>;
}

#[derive(Clone, Debug)]
pub struct CommentAgentTrigger {
    pub agent_id: Uuid,
    pub source: TriggerSource,
    pub squad: Option<SquadFacts>,
    pub already_pending: bool,
    /// An agent comment routed back to a squad leader from worker context. Only
    /// a recorded planned input may replay such a comment, which is why the
    /// ladder treats it differently from an explicit mention.
    pub non_leader_agent_reply: bool,
}

/// One explicitly named `@agent`/`@squad`, with either the executing agent or a
/// terminal non-executing outcome. Several mentions may resolve to the same
/// executing agent and still each need their own outcome.
#[derive(Clone, Debug)]
pub struct CommentMentionTarget {
    pub target_type: &'static str,
    pub target_id: String,
    pub exec_agent_id: Option<Uuid>,
    pub status: Option<DispatchStatus>,
    pub reason: Option<ReasonCode>,
    /// Set only for the verdicts that need a durable trace, so the trigger path
    /// can leave the explanation on the issue. The resolver runs for the
    /// composer PREVIEW too, which is why recording the refusal and writing the
    /// notice are separate steps.
    pub unusable: Option<BlockedRuntimeNotice>,
}

/// A refusal worth leaving on the issue: which agent, and the verdict that
/// names the repair.
#[derive(Clone, Debug)]
pub struct BlockedRuntimeNotice {
    /// The refused agent. Go dedupes the issue notice by agent id, so two
    /// mentions resolving to the same blocked agent leave one note.
    pub agent_id: Uuid,
    pub agent_name: String,
    pub verdict: AgentVerdict,
}

impl CommentMentionTarget {
    fn executing(target_type: &'static str, target_id: String, agent_id: Uuid) -> Self {
        Self {
            target_type,
            target_id,
            exec_agent_id: Some(agent_id),
            status: None,
            reason: None,
            unusable: None,
        }
    }

    fn terminal(
        target_type: &'static str,
        target_id: String,
        status: DispatchStatus,
        reason: ReasonCode,
    ) -> Self {
        Self {
            target_type,
            target_id,
            exec_agent_id: None,
            status: Some(status),
            reason: Some(reason),
            unusable: None,
        }
    }

    /// A refused mention whose verdict needs a durable trace. Every author gets
    /// it, including a human: the chip carries the reason code but not the
    /// repair command, and an agent-authored mention has nobody watching a
    /// response at all.
    fn blocked_unusable(
        target_type: &'static str,
        target_id: String,
        agent: &AgentFacts,
        verdict: AgentVerdict,
    ) -> Self {
        let notice = super::agent_ready::runtime_blocked_needs_notice(
            verdict
                .blocked_reason()
                .unwrap_or(ReasonCode::InternalError),
        )
        .then(|| BlockedRuntimeNotice {
            agent_id: agent.id,
            agent_name: agent.name.clone(),
            verdict: verdict.clone(),
        });
        Self {
            target_type,
            target_id,
            exec_agent_id: None,
            status: Some(DispatchStatus::Blocked),
            reason: verdict.blocked_reason(),
            unusable: notice,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct RoutingPlan {
    pub triggers: Vec<CommentAgentTrigger>,
    pub targets: Vec<CommentMentionTarget>,
}

/// Resolve every agent a comment triggers, plus one outcome per explicitly named
/// target. Mirrors Go's branch order exactly; reordering the checks changes the
/// contract, not just the code.
pub async fn compute_comment_agent_triggers(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    content: &str,
    parent: Option<&CommentFacts>,
    actor_type: &str,
    actor_id: Uuid,
    mut options: RoutingOptions,
) -> RoutingPlan {
    // A persisted comment determines its thread; only a preview, which has no
    // row of its own yet, falls back to the parent.
    options.thread_comment_id = options.exclude_trigger_comment_id;
    if options.thread_comment_id.is_none() {
        options.thread_comment_id = parent.map(|comment| comment.id);
    }
    if super::comment_mention::is_note_comment(content) {
        return RoutingPlan::default();
    }

    let mentions = parse_mentions(content);

    // An explicit mention is a direct request and wins over `@all`, which only
    // suppresses the implicit fallbacks below.
    if has_agent_or_squad(&mentions) {
        return resolve_mentioned_triggers(
            lookup, issue, &mentions, actor_type, actor_id, &options,
        )
        .await;
    }
    if has_all(&mentions) || has_member(&mentions) {
        return RoutingPlan::default();
    }

    if actor_type != "member" {
        // Agent-authored comments do not take the member conversation routes or
        // the member assignee fallback, but the leader→worker→leader loop must
        // keep working: the assigned squad comes from the issue, a guest squad
        // from the exact covered delegation.
        if actor_type == "agent"
            && let Some(triggers) =
                route_guest_squad_leader_fallback(lookup, issue, parent, actor_id, &options).await
        {
            return RoutingPlan {
                triggers,
                targets: Vec::new(),
            };
        }
        if issue.assignee_type.as_deref() == Some("squad")
            && let Some(mut trigger) =
                route_assigned_squad_leader_fallback(lookup, issue, actor_type, actor_id, &options)
                    .await
        {
            trigger.non_leader_agent_reply = actor_type == "agent" && actor_id != trigger.agent_id;
            return RoutingPlan {
                triggers: vec![trigger],
                targets: Vec::new(),
            };
        }
        return RoutingPlan::default();
    }

    // A deleted parent no longer speaks for its agent author.
    if let Some(parent) = parent
        && parent.author_type == "agent"
        && !parent.deleted
    {
        return match route_reply_to_parent_author(
            lookup, issue, parent, actor_type, actor_id, &options,
        )
        .await
        {
            Some(trigger) => RoutingPlan {
                triggers: vec![trigger],
                targets: Vec::new(),
            },
            None => RoutingPlan::default(),
        };
    }

    if let Some(parent) = parent {
        if let Some(triggers) =
            route_thread_root_owners(lookup, issue, parent, actor_id, &options).await
        {
            return RoutingPlan {
                triggers,
                targets: Vec::new(),
            };
        }
        // A plain member-to-member reply must not start the issue assignee just
        // because the thread has no agent owner; the same holds under a deleted
        // comment, which is still a reply rather than a new request.
        if parent.author_type == "member" || parent.deleted {
            return RoutingPlan::default();
        }
    }

    match route_assignee_fallback(lookup, issue, actor_type, actor_id, &options).await {
        Some(trigger) => RoutingPlan {
            triggers: vec![trigger],
            targets: Vec::new(),
        },
        None => RoutingPlan::default(),
    }
}

async fn resolve_mentioned_triggers(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    mentions: &[Mention],
    actor_type: &str,
    actor_id: Uuid,
    options: &RoutingOptions,
) -> RoutingPlan {
    let mut triggers: Vec<CommentAgentTrigger> = Vec::new();
    // Dedup by EXECUTING agent: two mentions resolving to the same agent enqueue
    // one task. A squad-leader mention upgrades an already-added plain @agent
    // trigger for that agent, because the leader run is a strict superset.
    let mut seen: Vec<(Uuid, usize)> = Vec::new();
    let mut add = |trigger: CommentAgentTrigger| {
        if let Some(index) = seen
            .iter()
            .find(|(id, _)| *id == trigger.agent_id)
            .map(|(_, index)| *index)
        {
            if triggers[index].source != TriggerSource::MentionSquadLeader
                && trigger.source == TriggerSource::MentionSquadLeader
            {
                triggers[index] = trigger;
            }
            return;
        }
        seen.push((trigger.agent_id, triggers.len()));
        triggers.push(trigger);
    };
    let mut targets: Vec<CommentMentionTarget> = Vec::new();
    let mut target_seen: Vec<String> = Vec::new();
    let mut add_target = |target: CommentMentionTarget| {
        let key = format!("{}:{}", target.target_type, target.target_id);
        if target_seen.contains(&key) {
            return;
        }
        target_seen.push(key);
        targets.push(target);
    };

    for mention in mentions {
        if mention.kind == "squad" {
            let Some(squad_id) = parse_mention_id(&mention.id) else {
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::TargetUnavailable,
                ));
                continue;
            };
            let Some(squad) = lookup.squad(issue.workspace_id, squad_id).await else {
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::TargetUnavailable,
                ));
                continue;
            };
            // A2A self-suppression: do not re-fire a leader from its own
            // mention. The outcome must reflect reality, so `deferred` only
            // when a real non-terminal task is still active.
            if actor_type == "agent"
                && actor_id == squad.leader_id
                && should_suppress_squad_leader_self_trigger(lookup, squad.leader_id, &squad).await
            {
                let active = lookup
                    .has_active(squad.leader_id, options.thread_comment_id)
                    .await;
                let (status, reason) = decide_suppressed_leader_outcome(active);
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    status,
                    reason,
                ));
                continue;
            }
            let Some(agent) = lookup.agent(issue.workspace_id, squad.leader_id).await else {
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::TargetUnavailable,
                ));
                continue;
            };
            // Invocation gate before archived/runtime state: enumeration-safe.
            if !lookup
                .can_invoke(&agent, actor_type, actor_id, options.originator_user_id)
                .await
            {
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::InvocationNotAllowed,
                ));
                continue;
            }
            if agent.archived {
                add_target(CommentMentionTarget::terminal(
                    "squad",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::TargetUnavailable,
                ));
                continue;
            }
            if let Some(verdict) = blocked_readiness(lookup, &agent).await {
                add_target(CommentMentionTarget::blocked_unusable(
                    "squad",
                    mention.id.clone(),
                    &agent,
                    verdict,
                ));
                continue;
            }
            let pending = match lookup.has_pending(squad.leader_id).await {
                Ok(pending) => pending,
                Err(()) => {
                    add_target(CommentMentionTarget::terminal(
                        "squad",
                        mention.id.clone(),
                        DispatchStatus::Blocked,
                        ReasonCode::InternalError,
                    ));
                    continue;
                }
            };
            let leader_id = squad.leader_id;
            add(CommentAgentTrigger {
                agent_id: agent.id,
                source: TriggerSource::MentionSquadLeader,
                squad: Some(squad),
                already_pending: pending,
                non_leader_agent_reply: false,
            });
            add_target(CommentMentionTarget::executing(
                "squad",
                mention.id.clone(),
                leader_id,
            ));
            continue;
        }
        if mention.kind != "agent" {
            continue;
        }
        let Some(agent_id) = parse_mention_id(&mention.id) else {
            // A string that is not a UUID cannot name an entity in any
            // workspace, so there is no existence to conceal.
            add_target(CommentMentionTarget::terminal(
                "agent",
                mention.id.clone(),
                DispatchStatus::Blocked,
                ReasonCode::TargetUnavailable,
            ));
            continue;
        };
        // Workspace-scoped load: a mention must not resolve an agent from
        // another workspace, whose roles would be judged against this one.
        let Some(agent) = lookup.agent(issue.workspace_id, agent_id).await else {
            // Do not reveal whether the id exists — it may be a private agent
            // in another workspace.
            add_target(CommentMentionTarget::terminal(
                "agent",
                mention.id.clone(),
                DispatchStatus::Blocked,
                ReasonCode::InvocationNotAllowed,
            ));
            continue;
        };
        if !lookup
            .can_invoke(&agent, actor_type, actor_id, options.originator_user_id)
            .await
        {
            add_target(CommentMentionTarget::terminal(
                "agent",
                mention.id.clone(),
                DispatchStatus::Blocked,
                ReasonCode::InvocationNotAllowed,
            ));
            continue;
        }
        if agent.archived {
            add_target(CommentMentionTarget::terminal(
                "agent",
                mention.id.clone(),
                DispatchStatus::Blocked,
                ReasonCode::TargetUnavailable,
            ));
            continue;
        }
        // Only a BLOCKED readiness verdict refuses the mention: an offline
        // runtime keeps queueing, because that wait ends by itself.
        if let Some(verdict) = blocked_readiness(lookup, &agent).await {
            add_target(CommentMentionTarget::blocked_unusable(
                "agent",
                mention.id.clone(),
                &agent,
                verdict,
            ));
            continue;
        }
        let pending = match lookup.has_pending(agent.id).await {
            Ok(pending) => pending,
            Err(()) => {
                add_target(CommentMentionTarget::terminal(
                    "agent",
                    mention.id.clone(),
                    DispatchStatus::Blocked,
                    ReasonCode::InternalError,
                ));
                continue;
            }
        };
        add(CommentAgentTrigger {
            agent_id: agent.id,
            source: TriggerSource::MentionAgent,
            squad: None,
            already_pending: pending,
            non_leader_agent_reply: false,
        });
        add_target(CommentMentionTarget::executing(
            "agent",
            mention.id.clone(),
            agent_id,
        ));
    }

    RoutingPlan { triggers, targets }
}

/// Readiness is a runtime/agent-ownership question, not a permission one. The
/// trait exposes the verdict through the same codes the send path uses; an
/// agent with no runtime is not refused here because Go's readiness verdict
/// treats "offline" as queueable and "unbound" as blocked. The lookup answers
/// with `None` when the run may proceed.
async fn blocked_readiness(
    lookup: &impl CommentRoutingLookup,
    agent: &AgentFacts,
) -> Option<AgentVerdict> {
    lookup.readiness_block(agent).await
}

async fn route_reply_to_parent_author(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    parent: &CommentFacts,
    actor_type: &str,
    actor_id: Uuid,
    options: &RoutingOptions,
) -> Option<CommentAgentTrigger> {
    if parent.author_type != "agent" {
        return None;
    }
    let author_id = parent.author_id?;
    let agent = lookup.agent(issue.workspace_id, author_id).await?;
    if !agent.runnable() {
        return None;
    }
    if !lookup
        .can_invoke(&agent, actor_type, actor_id, options.originator_user_id)
        .await
    {
        return None;
    }
    let pending = lookup.has_pending(author_id).await.ok()?;
    let squad = squad_leader_role_of_authoring_task(lookup, issue, parent, &agent).await;
    Some(CommentAgentTrigger {
        agent_id: agent.id,
        source: TriggerSource::ThreadParent,
        squad,
        already_pending: pending,
        non_leader_agent_reply: false,
    })
}

/// The squad a reply should continue under, when the replied-to comment was
/// written by that squad's leader acting IN the leader role. Read from the
/// comment's own authoring run, not the agent's latest task: an agent that is
/// both leader and worker posts in both roles, and a reply continues the role of
/// the comment it answers. Every uncertainty fails closed to direct-agent
/// routing.
async fn squad_leader_role_of_authoring_task(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    parent: &CommentFacts,
    agent: &AgentFacts,
) -> Option<SquadFacts> {
    let task = lookup.task(parent.source_task_id?).await?;
    if !task.is_leader_task {
        return None;
    }
    let squad = lookup.squad(issue.workspace_id, task.squad_id?).await?;
    if squad.archived || squad.leader_id != agent.id {
        return None;
    }
    Some(squad)
}

/// Close the leader→worker→leader loop when the squad was mentioned as a guest
/// instead of assigned to the issue. `Some(triggers)` means the guest path
/// claimed the comment — including the fail-closed shape where the historical
/// leader role is now invalid and nothing must fall through to an unrelated
/// assigned squad.
async fn route_guest_squad_leader_fallback(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    parent: Option<&CommentFacts>,
    author_id: Uuid,
    options: &RoutingOptions,
) -> Option<Vec<CommentAgentTrigger>> {
    let parent = parent?;
    // A tombstone preserves the reply tree, not the delegation's routing
    // authority. The issue assignment remains an independent source.
    if parent.deleted {
        return None;
    }
    let worker_task = lookup.task(options.authoring_task_id?).await?;
    if worker_task.is_leader_task
        || worker_task.agent_id != Some(author_id)
        || worker_task.issue_id != Some(issue.id)
        || !worker_task.covers(parent.id)
    {
        return None;
    }
    let leader_task = lookup.task(parent.source_task_id?).await?;
    if !leader_task.is_leader_task
        || leader_task.squad_id.is_none()
        || leader_task.agent_id != parent.author_id
    {
        return None;
    }
    // Preserve the established assigned-squad path when this delegation came
    // from that same squad: this fallback is only for a distinct guest squad.
    if issue.assignee_type.as_deref() == Some("squad") && issue.assignee_id == leader_task.squad_id
    {
        return None;
    }
    match route_reply_to_parent_author(lookup, issue, parent, "agent", author_id, options).await {
        Some(mut trigger) if trigger.squad.is_some() => {
            trigger.non_leader_agent_reply = true;
            Some(vec![trigger])
        }
        // Claimed but unroutable: nothing runs, and no other route may run.
        _ => Some(Vec::new()),
    }
}

async fn route_thread_root_owners(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    parent: &CommentFacts,
    member_id: Uuid,
    options: &RoutingOptions,
) -> Option<Vec<CommentAgentTrigger>> {
    let root = lookup.thread_root(parent.id).await?;
    if root.author_type != "member" {
        return None;
    }
    route_conversation_owners_for_root(lookup, issue, &root, member_id, options).await
}

async fn route_conversation_owners_for_root(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    root: &CommentFacts,
    member_id: Uuid,
    options: &RoutingOptions,
) -> Option<Vec<CommentAgentTrigger>> {
    let (trigger, has_explicit_owner, _) =
        route_first_explicit_root_mention_owner(lookup, issue, root, member_id).await;
    if has_explicit_owner {
        // Claimed by an explicit root mention: even when it cannot run, no
        // other route may run instead.
        return Some(trigger.into_iter().collect());
    }
    if options.exclude_trigger_comment_id == Some(root.id) {
        return None;
    }
    let owners = lookup.conversation_root_owners(root.id).await.ok()?;
    if owners.is_empty() {
        return None;
    }
    let mut triggers = Vec::new();
    for owner in owners {
        if let Some(trigger) = route_conversation_continuation_to_agent(
            lookup,
            issue,
            owner.agent_id,
            owner.squad_id,
            member_id,
        )
        .await
        {
            triggers.push(trigger);
        }
    }
    Some(triggers)
}

/// Returns `(trigger, has_explicit_owner, ok)`. The first explicit
/// `@agent`/`@squad` in the root decides the continuation, even when it cannot
/// run.
async fn route_first_explicit_root_mention_owner(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    root: &CommentFacts,
    member_id: Uuid,
) -> (Option<CommentAgentTrigger>, bool, bool) {
    for mention in parse_mentions(&root.content) {
        match mention.kind.as_str() {
            "agent" => {
                let Some(agent_id) = parse_mention_id(&mention.id) else {
                    return (None, true, false);
                };
                let trigger = route_conversation_continuation_to_agent(
                    lookup, issue, agent_id, None, member_id,
                )
                .await;
                let ok = trigger.is_some();
                return (trigger, true, ok);
            }
            "squad" => {
                let Some(squad_id) = parse_mention_id(&mention.id) else {
                    return (None, true, false);
                };
                let Some(squad) = lookup.squad(issue.workspace_id, squad_id).await else {
                    return (None, true, false);
                };
                let trigger = route_conversation_continuation_to_agent(
                    lookup,
                    issue,
                    squad.leader_id,
                    Some(squad_id),
                    member_id,
                )
                .await;
                let ok = trigger.is_some();
                return (trigger, true, ok);
            }
            _ => {}
        }
    }
    (None, false, false)
}

async fn route_conversation_continuation_to_agent(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    agent_id: Uuid,
    squad_id: Option<Uuid>,
    member_id: Uuid,
) -> Option<CommentAgentTrigger> {
    let agent = lookup.agent(issue.workspace_id, agent_id).await?;
    if !agent.runnable() {
        return None;
    }
    // A member is its own originator, exactly as Go passes memberID twice.
    if !lookup
        .can_invoke(&agent, "member", member_id, Some(member_id))
        .await
    {
        return None;
    }
    let pending = lookup.has_pending(agent_id).await.ok()?;
    let squad = match squad_id {
        Some(squad_id) => lookup.squad(issue.workspace_id, squad_id).await,
        None => None,
    };
    Some(CommentAgentTrigger {
        agent_id: agent.id,
        source: TriggerSource::ConversationContinuation,
        squad,
        already_pending: pending,
        non_leader_agent_reply: false,
    })
}

async fn route_assignee_fallback(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    actor_type: &str,
    actor_id: Uuid,
    options: &RoutingOptions,
) -> Option<CommentAgentTrigger> {
    let assignee_type = issue.assignee_type.as_deref()?;
    issue.assignee_id?;
    // In Triage the assignee is a proposal, not an owner, so there is nobody to
    // fall back TO. This route and the squad-leader one below are the two that
    // derive an executor from the issue.
    if issue.triage_state.is_some() {
        return None;
    }
    match assignee_type {
        "agent" => route_assignee_agent(lookup, issue, actor_type, actor_id, options).await,
        "squad" => {
            route_assigned_squad_leader_fallback(lookup, issue, actor_type, actor_id, options).await
        }
        _ => None,
    }
}

async fn route_assignee_agent(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    actor_type: &str,
    actor_id: Uuid,
    options: &RoutingOptions,
) -> Option<CommentAgentTrigger> {
    let agent_id = issue.assignee_id?;
    let agent = lookup.agent(issue.workspace_id, agent_id).await?;
    if !agent.runnable() {
        return None;
    }
    if !lookup
        .can_invoke(&agent, actor_type, actor_id, options.originator_user_id)
        .await
    {
        return None;
    }
    let pending = lookup.has_pending(agent_id).await.ok()?;
    Some(CommentAgentTrigger {
        agent_id,
        source: TriggerSource::IssueAssignee,
        squad: None,
        already_pending: pending,
        non_leader_agent_reply: false,
    })
}

async fn route_assigned_squad_leader_fallback(
    lookup: &impl CommentRoutingLookup,
    issue: &IssueFacts,
    actor_type: &str,
    actor_id: Uuid,
    options: &RoutingOptions,
) -> Option<CommentAgentTrigger> {
    // Checked here as well as in the assignee fallback: an agent-authored
    // comment reaches this one directly.
    if issue.triage_state.is_some() {
        return None;
    }
    let squad = lookup.squad(issue.workspace_id, issue.assignee_id?).await?;
    if actor_type == "agent"
        && actor_id == squad.leader_id
        && should_suppress_squad_leader_self_trigger(lookup, squad.leader_id, &squad).await
    {
        return None;
    }
    let agent = lookup.agent(issue.workspace_id, squad.leader_id).await?;
    if !agent.runnable() {
        return None;
    }
    if !lookup
        .can_invoke(&agent, actor_type, actor_id, options.originator_user_id)
        .await
    {
        return None;
    }
    let pending = lookup.has_pending(squad.leader_id).await.ok()?;
    Some(CommentAgentTrigger {
        agent_id: squad.leader_id,
        source: TriggerSource::IssueAssignee,
        squad: Some(squad),
        already_pending: pending,
        non_leader_agent_reply: false,
    })
}

/// Whether a leader's own comment must not re-enqueue that leader. The only
/// leader-authored non-leader task allowed to wake its leader is a same-squad
/// worker task; direct mentions and thread-parent replies are not worker-role
/// proof. A failed lookup is not suppression.
async fn should_suppress_squad_leader_self_trigger(
    lookup: &impl CommentRoutingLookup,
    agent_id: Uuid,
    squad: &SquadFacts,
) -> bool {
    let Ok(latest) = lookup.latest_task_role(agent_id).await else {
        return false;
    };
    if latest.is_leader_task {
        return true;
    }
    latest.squad_id != Some(squad.id)
}

/// Map a suppressed leader's active-task check to an honest outcome: a query
/// failure is never success, and `deferred` is only claimed when a real run is
/// still active to cover the comment.
pub fn decide_suppressed_leader_outcome(active: Result<bool, ()>) -> (DispatchStatus, ReasonCode) {
    match active {
        Err(()) => (DispatchStatus::Blocked, ReasonCode::InternalError),
        Ok(true) => (DispatchStatus::Deferred, ReasonCode::AlreadyActive),
        Ok(false) => (DispatchStatus::Blocked, ReasonCode::SelfTriggerSuppressed),
    }
}

/// Decide what a comment merge miss means for a target that was pre-flagged
/// pending. A failed lookup fails closed: never enqueue a possibly-colliding
/// duplicate, and never report success. Only a confirmed-none enqueues fresh.
pub fn decide_post_merge_miss(
    active: Result<bool, ()>,
) -> (Option<(DispatchStatus, ReasonCode)>, bool) {
    match active {
        Err(()) => (
            Some((DispatchStatus::Blocked, ReasonCode::InternalError)),
            false,
        ),
        Ok(true) => (
            Some((DispatchStatus::Deferred, ReasonCode::Deferred)),
            false,
        ),
        Ok(false) => (None, true),
    }
}

/// Enqueue result for one executing agent, keyed by agent id later.
#[derive(Clone, Debug)]
pub struct EnqueueResult {
    pub status: DispatchStatus,
    pub reason: ReasonCode,
    /// The squad whose leader context the run actually carries, so a DIFFERENT
    /// squad sharing this leader is reported honestly as coalesced.
    pub exec_squad_id: Option<Uuid>,
}

/// Fan each explicit mention target out to its executing agent's result. A
/// target whose agent was suppressed by the composer yields no outcome — the
/// user opted out deliberately.
pub fn comment_trigger_outcomes(
    targets: &[CommentMentionTarget],
    enqueued: &[(Uuid, EnqueueResult)],
) -> Vec<super::issue_write_comment::CommentTriggerOutcome> {
    let mut outcomes = Vec::with_capacity(targets.len());
    for target in targets {
        let Some(exec_agent_id) = target.exec_agent_id else {
            outcomes.push(super::issue_write_comment::CommentTriggerOutcome {
                target_type: target.target_type.to_string(),
                target_id: target.target_id.clone(),
                status: target
                    .status
                    .unwrap_or(DispatchStatus::Blocked)
                    .as_str()
                    .to_string(),
                reason_code: target
                    .reason
                    .unwrap_or(ReasonCode::InternalError)
                    .as_str()
                    .to_string(),
            });
            continue;
        };
        let Some((_, result)) = enqueued.iter().find(|(id, _)| *id == exec_agent_id) else {
            continue;
        };
        let (mut status, mut reason) = (result.status, result.reason);
        // A @squad whose shared leader ran under a DIFFERENT squad's context did
        // not get its own leader briefing injected; the single leader run merely
        // folds it in, so report coalesced rather than claim this squad's
        // context executed.
        if target.target_type == "squad"
            && result.exec_squad_id.is_some()
            && result.exec_squad_id.map(|id| id.to_string()) != Some(target.target_id.clone())
            && status == DispatchStatus::Queued
        {
            status = DispatchStatus::Coalesced;
            reason = ReasonCode::Coalesced;
        }
        outcomes.push(super::issue_write_comment::CommentTriggerOutcome {
            target_type: target.target_type.to_string(),
            target_id: target.target_id.clone(),
            status: status.as_str().to_string(),
            reason_code: reason.as_str().to_string(),
        });
    }
    outcomes
}

/// Mention ids come from untrusted comment text and the canonical regex accepts
/// any hex/dash run, so parsing must be fallible rather than panicking (the
/// comment row is already committed by the time routing runs).
fn parse_mention_id(raw: &str) -> Option<Uuid> {
    Uuid::parse_str(raw).ok()
}

#[cfg(test)]
mod tests;
