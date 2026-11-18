//! Branch-order tests for the routing decision tree.
//!
//! Each case pins one rule from Go's `computeCommentAgentTriggers`. They use an
//! in-memory lookup so a reordering of the checks fails a test instead of
//! silently changing which runs a comment creates.

use super::*;
use crate::api::agent_ready::AgentAvailability;
use std::collections::HashMap;

const WS: Uuid = Uuid::nil();

fn id(n: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[15] = n;
    Uuid::from_bytes(bytes)
}

#[derive(Default)]
struct Fake {
    agents: HashMap<Uuid, AgentFacts>,
    squads: HashMap<Uuid, SquadFacts>,
    /// Overrides `can_invoke`; default is "everything is invocable".
    denied: Vec<Uuid>,
    readiness: HashMap<Uuid, ReasonCode>,
    pending: HashMap<Uuid, bool>,
    pending_error: bool,
    active: HashMap<Uuid, bool>,
    active_error: bool,
    roles: HashMap<Uuid, TaskRole>,
    role_error: bool,
    tasks: HashMap<Uuid, TaskFacts>,
    roots: HashMap<Uuid, CommentFacts>,
    owners: HashMap<Uuid, Vec<RootOwner>>,
}

impl Fake {
    fn agent(mut self, n: u8) -> Self {
        self.agents.insert(
            id(n),
            AgentFacts {
                id: id(n),
                name: format!("agent-{n}"),
                runtime_id: Some(id(200)),
                archived: false,
                owner_id: None,
                permission_mode: "public_to".into(),
            },
        );
        self
    }

    fn agent_with(mut self, n: u8, runtime: bool, archived: bool) -> Self {
        self.agents.insert(
            id(n),
            AgentFacts {
                id: id(n),
                name: format!("agent-{n}"),
                runtime_id: if runtime { Some(id(200)) } else { None },
                archived,
                owner_id: None,
                permission_mode: "public_to".into(),
            },
        );
        self
    }

    fn squad(mut self, n: u8, leader: u8) -> Self {
        self.squads.insert(
            id(n),
            SquadFacts {
                id: id(n),
                leader_id: id(leader),
                archived: false,
            },
        );
        self
    }

    fn denied(mut self, n: u8) -> Self {
        self.denied.push(id(n));
        self
    }

    fn readiness(mut self, n: u8, reason: ReasonCode) -> Self {
        self.readiness.insert(id(n), reason);
        self
    }

    fn pending(mut self, n: u8, value: bool) -> Self {
        self.pending.insert(id(n), value);
        self
    }

    fn active(mut self, n: u8, value: bool) -> Self {
        self.active.insert(id(n), value);
        self
    }

    fn role(mut self, n: u8, role: TaskRole) -> Self {
        self.roles.insert(id(n), role);
        self
    }
}

#[async_trait]
impl CommentRoutingLookup for Fake {
    async fn agent(&self, _: Uuid, agent_id: Uuid) -> Option<AgentFacts> {
        self.agents.get(&agent_id).cloned()
    }
    async fn squad(&self, _: Uuid, squad_id: Uuid) -> Option<SquadFacts> {
        self.squads.get(&squad_id).cloned()
    }
    async fn can_invoke(&self, agent: &AgentFacts, _: &str, _: Uuid, _: Option<Uuid>) -> bool {
        !self.denied.contains(&agent.id)
    }
    async fn readiness_block(&self, agent: &AgentFacts) -> Option<AgentVerdict> {
        self.readiness
            .get(&agent.id)
            .copied()
            .map(|reason| AgentVerdict {
                availability: AgentAvailability::Blocked,
                reason: Some(reason),
                repair: None,
                detail: String::new(),
            })
    }
    async fn has_pending(&self, agent_id: Uuid) -> Result<bool, ()> {
        if self.pending_error {
            return Err(());
        }
        Ok(self.pending.get(&agent_id).copied().unwrap_or(false))
    }
    async fn has_active(&self, agent_id: Uuid, _: Option<Uuid>) -> Result<bool, ()> {
        if self.active_error {
            return Err(());
        }
        Ok(self.active.get(&agent_id).copied().unwrap_or(false))
    }
    async fn latest_task_role(&self, agent_id: Uuid) -> Result<TaskRole, ()> {
        if self.role_error {
            return Err(());
        }
        Ok(self.roles.get(&agent_id).copied().unwrap_or(TaskRole {
            is_leader_task: false,
            squad_id: None,
        }))
    }
    async fn task(&self, task_id: Uuid) -> Option<TaskFacts> {
        self.tasks.get(&task_id).cloned()
    }
    async fn thread_root(&self, comment_id: Uuid) -> Option<CommentFacts> {
        self.roots.get(&comment_id).cloned()
    }
    async fn conversation_root_owners(&self, root_id: Uuid) -> Result<Vec<RootOwner>, ()> {
        Ok(self.owners.get(&root_id).cloned().unwrap_or_default())
    }
}

fn issue(assignee: Option<(&str, u8)>) -> IssueFacts {
    IssueFacts {
        id: id(100),
        workspace_id: WS,
        assignee_type: assignee.map(|(kind, _)| kind.to_string()),
        assignee_id: assignee.map(|(_, n)| id(n)),
        triage_state: None,
    }
}

fn mention(kind: &str, n: u8) -> String {
    format!("[@T](mention://{kind}/{})", id(n))
}

fn run<'a>(
    fake: &'a Fake,
    issue: &'a IssueFacts,
    content: &'a str,
    parent: Option<&'a CommentFacts>,
    actor_type: &'a str,
    actor_id: u8,
) -> RoutingPlan {
    futures_lite_block_on(compute_comment_agent_triggers(
        fake,
        issue,
        content,
        parent,
        actor_type,
        id(actor_id),
        RoutingOptions::for_new_comment(id(1)),
    ))
}

/// The crate is async; tests are not in an async runtime, so drive the future
/// with a minimal single-poll executor. Every future here is ready on first
/// poll because the fake lookup never awaits real I/O.
fn futures_lite_block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fake lookup must never yield"),
    }
}

#[test]
fn explicit_agent_mention_wins_over_all_and_enqueues_once() {
    let fake = Fake::default().agent(2);
    let plan = run(
        &fake,
        &issue(None),
        &format!("{} [@all](mention://all/all)", mention("agent", 2)),
        None,
        "member",
        9,
    );
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].agent_id, id(2));
    assert_eq!(plan.triggers[0].source, TriggerSource::MentionAgent);
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].exec_agent_id, Some(id(2)));
}

#[test]
fn all_mention_suppresses_implicit_routes_but_not_member_mention() {
    let fake = Fake::default().agent(2);
    assert!(
        run(
            &fake,
            &issue(Some(("agent", 2))),
            "[@all](mention://all/all)",
            None,
            "member",
            9
        )
        .triggers
        .is_empty()
    );
    let member = format!("[@M](mention://member/{})", id(9));
    assert!(
        run(
            &fake,
            &issue(Some(("agent", 2))),
            &member,
            None,
            "member",
            9
        )
        .triggers
        .is_empty()
    );
}

#[test]
fn note_comment_never_triggers() {
    let fake = Fake::default().agent(2);
    let plan = run(
        &fake,
        &issue(None),
        &format!("/note {}", mention("agent", 2)),
        None,
        "member",
        9,
    );
    assert!(plan.triggers.is_empty());
    assert!(plan.targets.is_empty());
}

#[test]
fn malformed_mention_id_is_target_unavailable_not_a_panic() {
    let fake = Fake::default();
    let plan = run(
        &fake,
        &issue(None),
        "[@A](mention://agent/-)",
        None,
        "member",
        9,
    );
    assert!(plan.triggers.is_empty());
    assert_eq!(plan.targets.len(), 1);
    assert_eq!(plan.targets[0].status, Some(DispatchStatus::Blocked));
    assert_eq!(plan.targets[0].reason, Some(ReasonCode::TargetUnavailable));
}

#[test]
fn unresolvable_well_formed_agent_is_invocation_not_allowed() {
    let fake = Fake::default();
    let plan = run(
        &fake,
        &issue(None),
        &mention("agent", 77),
        None,
        "member",
        9,
    );
    assert_eq!(
        plan.targets[0].reason,
        Some(ReasonCode::InvocationNotAllowed)
    );
}

#[test]
fn invocation_gate_runs_before_archived_and_readiness_state() {
    // The agent is archived AND blocked on readiness, but the caller may not
    // invoke it: the only observable reason must be the generic one.
    let fake = Fake::default()
        .agent_with(2, true, true)
        .denied(2)
        .readiness(2, ReasonCode::RuntimeUnusable);
    let plan = run(&fake, &issue(None), &mention("agent", 2), None, "member", 9);
    assert_eq!(
        plan.targets[0].reason,
        Some(ReasonCode::InvocationNotAllowed)
    );
}

#[test]
fn archived_agent_after_permission_is_target_unavailable() {
    let fake = Fake::default().agent_with(2, true, true);
    let plan = run(&fake, &issue(None), &mention("agent", 2), None, "member", 9);
    assert_eq!(plan.targets[0].reason, Some(ReasonCode::TargetUnavailable));
}

#[test]
fn runtime_blocked_mention_reports_the_verdict_code() {
    let fake = Fake::default()
        .agent(2)
        .readiness(2, ReasonCode::RuntimeProfileMissing);
    let plan = run(&fake, &issue(None), &mention("agent", 2), None, "member", 9);
    assert_eq!(
        plan.targets[0].reason,
        Some(ReasonCode::RuntimeProfileMissing)
    );
    assert!(plan.triggers.is_empty());
}

#[test]
fn pending_lookup_failure_blocks_with_internal_error() {
    let mut fake = Fake::default().agent(2);
    fake.pending_error = true;
    let plan = run(&fake, &issue(None), &mention("agent", 2), None, "member", 9);
    assert!(plan.triggers.is_empty());
    assert_eq!(plan.targets[0].reason, Some(ReasonCode::InternalError));
}

#[test]
fn mentioned_squad_enqueues_its_leader_once_and_upgrades_duplicate() {
    // @Agent(3) then @Squad(4) whose leader is Agent(3): one run, upgraded to
    // the leader context regardless of mention order.
    let fake = Fake::default().agent(3).squad(4, 3);
    let content = format!("{} {}", mention("agent", 3), mention("squad", 4));
    let plan = run(&fake, &issue(None), &content, None, "member", 9);
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].source, TriggerSource::MentionSquadLeader);
    assert_eq!(plan.triggers[0].squad.as_ref().map(|s| s.id), Some(id(4)));
    assert_eq!(plan.targets.len(), 2);
    assert_eq!(plan.targets[0].exec_agent_id, Some(id(3)));
    assert_eq!(plan.targets[1].exec_agent_id, Some(id(3)));
}

#[test]
fn leader_self_mention_is_suppressed_unless_a_same_squad_worker_ran() {
    let suppressed = Fake::default()
        .agent(3)
        .squad(4, 3)
        .role(
            3,
            TaskRole {
                is_leader_task: true,
                squad_id: Some(id(4)),
            },
        )
        .active(3, false);
    let plan = run(
        &suppressed,
        &issue(None),
        &mention("squad", 4),
        None,
        "agent",
        3,
    );
    assert!(plan.triggers.is_empty());
    assert_eq!(
        plan.targets[0].reason,
        Some(ReasonCode::SelfTriggerSuppressed)
    );

    // Suppressed again, now with a run genuinely in flight: `deferred` is only
    // honest when that live run will actually cover the comment.
    let deferred = Fake::default()
        .agent(3)
        .squad(4, 3)
        .role(
            3,
            TaskRole {
                is_leader_task: false,
                squad_id: Some(id(5)),
            },
        )
        .active(3, true);
    let plan = run(
        &deferred,
        &issue(None),
        &mention("squad", 4),
        None,
        "agent",
        3,
    );
    assert_eq!(plan.targets[0].status, Some(DispatchStatus::Deferred));
    assert_eq!(plan.targets[0].reason, Some(ReasonCode::AlreadyActive));
}

#[test]
fn same_squad_worker_role_allows_the_leader_to_fire_again() {
    // Latest task was a worker turn OF THIS squad: the leader→worker→leader
    // hand-off is legitimate, so the mention fires.
    let fake = Fake::default().agent(3).squad(4, 3).role(
        3,
        TaskRole {
            is_leader_task: false,
            squad_id: Some(id(4)),
        },
    );
    let plan = run(&fake, &issue(None), &mention("squad", 4), None, "agent", 3);
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].source, TriggerSource::MentionSquadLeader);
}

#[test]
fn assignee_fallback_is_used_for_a_plain_member_comment() {
    let fake = Fake::default().agent(2).pending(2, true);
    let plan = run(
        &fake,
        &issue(Some(("agent", 2))),
        "thanks",
        None,
        "member",
        9,
    );
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].source, TriggerSource::IssueAssignee);
    assert!(plan.triggers[0].already_pending);
    assert!(plan.targets.is_empty());
}

#[test]
fn triage_issue_has_no_derived_executor_but_explicit_mentions_still_run() {
    let mut facts = issue(Some(("agent", 2)));
    facts.triage_state = Some("triage".into());
    let fake = Fake::default().agent(2);
    assert!(
        run(&fake, &facts, "look at this", None, "member", 9)
            .triggers
            .is_empty()
    );
    assert_eq!(
        run(&fake, &facts, &mention("agent", 2), None, "member", 9)
            .triggers
            .len(),
        1
    );
}

#[test]
fn plain_member_reply_never_wakes_the_assignee() {
    let fake = Fake::default().agent(2);
    let parent = CommentFacts {
        id: id(50),
        author_type: "member".into(),
        author_id: Some(id(8)),
        deleted: false,
        source_task_id: None,
        content: "hello".into(),
    };
    let plan = run(
        &fake,
        &issue(Some(("agent", 2))),
        "me too",
        Some(&parent),
        "member",
        9,
    );
    assert!(plan.triggers.is_empty());
}

#[test]
fn reply_to_agent_parent_wakes_that_agent_not_the_assignee() {
    let fake = Fake::default().agent(2).agent(3);
    let parent = CommentFacts {
        id: id(50),
        author_type: "agent".into(),
        author_id: Some(id(3)),
        deleted: false,
        source_task_id: None,
        content: "done".into(),
    };
    let plan = run(
        &fake,
        &issue(Some(("agent", 2))),
        "next please",
        Some(&parent),
        "member",
        9,
    );
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].agent_id, id(3));
    assert_eq!(plan.triggers[0].source, TriggerSource::ThreadParent);
}

#[test]
fn deleted_agent_parent_does_not_speak_for_its_author() {
    let fake = Fake::default().agent(2).agent(3);
    let parent = CommentFacts {
        id: id(50),
        author_type: "agent".into(),
        author_id: Some(id(3)),
        deleted: true,
        source_task_id: None,
        content: "gone".into(),
    };
    let plan = run(
        &fake,
        &issue(Some(("agent", 2))),
        "next please",
        Some(&parent),
        "member",
        9,
    );
    assert!(
        plan.triggers.is_empty(),
        "deleted parent is a reply, not a request"
    );
}

#[test]
fn thread_root_owners_continue_the_conversation() {
    let fake = Fake {
        owners: HashMap::from([(
            id(50),
            vec![RootOwner {
                agent_id: id(3),
                squad_id: None,
            }],
        )]),
        roots: HashMap::from([(
            id(50),
            CommentFacts {
                id: id(50),
                author_type: "member".into(),
                author_id: Some(id(8)),
                deleted: false,
                source_task_id: None,
                content: "root".into(),
            },
        )]),
        ..Fake::default()
    }
    .agent(3);
    let parent = CommentFacts {
        id: id(50),
        author_type: "member".into(),
        author_id: Some(id(8)),
        deleted: false,
        source_task_id: None,
        content: "root".into(),
    };
    let plan = run(
        &fake,
        &issue(Some(("agent", 2))),
        "still there?",
        Some(&parent),
        "member",
        9,
    );
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(
        plan.triggers[0].source,
        TriggerSource::ConversationContinuation
    );
}

#[test]
fn explicit_root_mention_decides_the_continuation_even_when_it_cannot_run() {
    let root = CommentFacts {
        id: id(50),
        author_type: "member".into(),
        author_id: Some(id(8)),
        deleted: false,
        source_task_id: None,
        content: mention("agent", 3),
    };
    let fake = Fake {
        roots: HashMap::from([(id(50), root.clone())]),
        // A different owner exists but must not be consulted.
        owners: HashMap::from([(
            id(50),
            vec![RootOwner {
                agent_id: id(4),
                squad_id: None,
            }],
        )]),
        ..Fake::default()
    }
    .agent(4)
    .denied(3)
    .agent(3);
    let plan = run(&fake, &issue(None), "reply", Some(&root), "member", 9);
    assert!(plan.triggers.is_empty());
}

#[test]
fn agent_actors_use_the_assigned_squad_route_not_member_conversation() {
    let fake = Fake::default().agent(3).squad(4, 3);
    let mut facts = issue(Some(("squad", 4)));
    facts.triage_state = None;
    let plan = run(&fake, &facts, "worker result", None, "agent", 5);
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].source, TriggerSource::IssueAssignee);
    assert_eq!(plan.triggers[0].squad.as_ref().map(|s| s.id), Some(id(4)));
    // A different agent author is a non-leader reply, which only a recorded
    // planned input may replay.
    assert!(plan.triggers[0].non_leader_agent_reply);
}

#[test]
fn leader_authoring_on_the_assigned_squad_is_not_a_self_trigger() {
    let fake = Fake::default().agent(3).squad(4, 3).role(
        3,
        TaskRole {
            is_leader_task: true,
            squad_id: Some(id(4)),
        },
    );
    let plan = run(
        &fake,
        &issue(Some(("squad", 4))),
        "status",
        None,
        "agent",
        3,
    );
    assert!(plan.triggers.is_empty());
}

#[test]
fn agent_without_a_runtime_is_not_routed() {
    let fake = Fake::default().agent_with(2, false, false);
    assert!(
        run(&fake, &issue(Some(("agent", 2))), "hi", None, "member", 9)
            .triggers
            .is_empty()
    );
}

#[test]
fn guest_squad_fallback_requires_a_covering_worker_task() {
    let mut fake = Fake::default().agent(3).squad(4, 3);
    let parent = CommentFacts {
        id: id(50),
        author_type: "agent".into(),
        author_id: Some(id(3)),
        deleted: false,
        source_task_id: Some(id(60)),
        content: "delegate".into(),
    };
    fake.tasks.insert(
        id(60),
        TaskFacts {
            agent_id: Some(id(3)),
            is_leader_task: true,
            squad_id: Some(id(4)),
            ..TaskFacts::default()
        },
    );
    fake.tasks.insert(
        id(70),
        TaskFacts {
            agent_id: Some(id(5)),
            issue_id: Some(id(100)),
            trigger_comment_id: Some(id(50)),
            ..TaskFacts::default()
        },
    );
    // Worker task that covers the delegation comment → leader hand-off.
    let mut options = RoutingOptions::for_new_comment(id(1));
    options.authoring_task_id = Some(id(70));
    let plan = futures_lite_block_on(compute_comment_agent_triggers(
        &fake,
        &issue(None),
        "worker output",
        Some(&parent),
        "agent",
        id(5),
        options,
    ));
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].agent_id, id(3));
    assert!(plan.triggers[0].non_leader_agent_reply);

    // The same comment without the trusted authoring task is not a guest loop.
    let plan = run(
        &fake,
        &issue(None),
        "worker output",
        Some(&parent),
        "agent",
        5,
    );
    assert!(plan.triggers.is_empty());
}

#[test]
fn assigned_same_squad_delegation_keeps_the_assigned_route() {
    let mut fake = Fake::default().agent(3).squad(4, 3);
    let parent = CommentFacts {
        id: id(50),
        author_type: "agent".into(),
        author_id: Some(id(3)),
        deleted: false,
        source_task_id: Some(id(60)),
        content: "delegate".into(),
    };
    fake.tasks.insert(
        id(60),
        TaskFacts {
            agent_id: Some(id(3)),
            is_leader_task: true,
            squad_id: Some(id(4)),
            ..TaskFacts::default()
        },
    );
    fake.tasks.insert(
        id(70),
        TaskFacts {
            agent_id: Some(id(5)),
            issue_id: Some(id(100)),
            coalesced_comment_ids: vec![id(50)],
            ..TaskFacts::default()
        },
    );
    let mut options = RoutingOptions::for_new_comment(id(1));
    options.authoring_task_id = Some(id(70));
    let plan = futures_lite_block_on(compute_comment_agent_triggers(
        &fake,
        &issue(Some(("squad", 4))),
        "worker output",
        Some(&parent),
        "agent",
        id(5),
        options,
    ));
    assert_eq!(plan.triggers.len(), 1);
    assert_eq!(plan.triggers[0].source, TriggerSource::IssueAssignee);
}

#[test]
fn suppressed_composer_targets_produce_no_outcome() {
    let targets = vec![CommentMentionTarget::executing(
        "agent",
        id(2).to_string(),
        id(2),
    )];
    assert!(comment_trigger_outcomes(&targets, &[]).is_empty());
}

#[test]
fn terminal_targets_carry_their_own_outcome() {
    let targets = vec![CommentMentionTarget::terminal(
        "squad",
        id(4).to_string(),
        DispatchStatus::Blocked,
        ReasonCode::TargetUnavailable,
    )];
    let outcomes = comment_trigger_outcomes(&targets, &[]);
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].status, "blocked");
    assert_eq!(outcomes[0].reason_code, "target_unavailable");
}

#[test]
fn a_shared_leader_running_another_squad_is_reported_coalesced() {
    let targets = vec![CommentMentionTarget::executing(
        "squad",
        id(4).to_string(),
        id(3),
    )];
    let enqueued = vec![(
        id(3),
        EnqueueResult {
            status: DispatchStatus::Queued,
            reason: ReasonCode::Queued,
            exec_squad_id: Some(id(5)),
        },
    )];
    let outcomes = comment_trigger_outcomes(&targets, &enqueued);
    assert_eq!(outcomes[0].status, "coalesced");
    assert_eq!(outcomes[0].reason_code, "coalesced");
}

#[test]
fn the_executing_squad_keeps_its_queued_outcome() {
    let targets = vec![CommentMentionTarget::executing(
        "squad",
        id(4).to_string(),
        id(3),
    )];
    let enqueued = vec![(
        id(3),
        EnqueueResult {
            status: DispatchStatus::Queued,
            reason: ReasonCode::Queued,
            exec_squad_id: Some(id(4)),
        },
    )];
    assert_eq!(
        comment_trigger_outcomes(&targets, &enqueued)[0].status,
        "queued"
    );
}

#[test]
fn post_merge_miss_fails_closed_on_a_failed_lookup() {
    assert_eq!(
        decide_post_merge_miss(Err(())),
        (
            Some((DispatchStatus::Blocked, ReasonCode::InternalError)),
            false
        )
    );
    assert_eq!(
        decide_post_merge_miss(Ok(true)),
        (
            Some((DispatchStatus::Deferred, ReasonCode::Deferred)),
            false
        )
    );
    assert_eq!(decide_post_merge_miss(Ok(false)), (None, true));
}

#[test]
fn suppressed_leader_outcome_never_fabricates_deferred() {
    assert_eq!(
        decide_suppressed_leader_outcome(Err(())),
        (DispatchStatus::Blocked, ReasonCode::InternalError)
    );
    assert_eq!(
        decide_suppressed_leader_outcome(Ok(true)),
        (DispatchStatus::Deferred, ReasonCode::AlreadyActive)
    );
    assert_eq!(
        decide_suppressed_leader_outcome(Ok(false)),
        (DispatchStatus::Blocked, ReasonCode::SelfTriggerSuppressed)
    );
}

#[test]
fn task_covers_reports_trigger_and_coalesced_inputs() {
    let task = TaskFacts {
        trigger_comment_id: Some(id(10)),
        coalesced_comment_ids: vec![id(11), id(12)],
        ..TaskFacts::default()
    };
    assert!(task.covers(id(10)));
    assert!(task.covers(id(12)));
    assert!(!task.covers(id(13)));
}
