//! Database reads behind [`super::attribution`].
//!
//! Go keeps these on `TaskService`; the split is the same one: this module
//! gathers facts and hands them to the pure classifiers, so the rules stay
//! unit-tested and this file is the only place that knows the SQL.
//!
//! Two invariants the write boundary protects:
//!
//! * a trigger comment is read **workspace-scoped**, so a foreign comment UUID
//!   cannot resolve a human from another tenant.
//! * the autopilot trigger principal is read **bound to its autopilot and
//!   workspace** and re-checks that the member is still in the workspace, so
//!   removing someone actually revokes what their triggers can do. Every failure
//!   path returns "no principal" — never a substitute human.

use super::attribution::{
    self, AttributionResult, CommentAttributionFacts, DirectFacts, EvidenceKind, Source,
    TRIGGER_SUMMARY_MAX_RUNES,
};
use chrono::{DateTime, Utc};
use tokio_postgres::GenericClient;
use uuid::Uuid;

/// Magic marker the enqueue paths surface when a run resolved to no precise
/// accountable human and the enqueue is REFUSED rather than started. It covers
/// three cases, all meaning "we cannot guarantee an accountable human": the
/// workspace opted into fail-closed, the policy could not be read (so we cannot
/// confirm fallback is allowed), or `owner_fallback` has no agent owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttributionFailClosed;

/// The issue columns attribution reads.
#[derive(Clone, Debug)]
pub struct IssueFacts {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub creator_type: String,
    pub creator_id: Option<Uuid>,
    /// `quick_create` | `agent_create` | `autopilot` | other.
    pub origin_type: Option<String>,
    pub origin_id: Option<Uuid>,
}

/// Load the attribution-relevant columns of one issue, workspace-scoped.
pub async fn load_issue_facts(
    db: &impl GenericClient,
    workspace_id: Uuid,
    issue_id: Uuid,
) -> Option<IssueFacts> {
    let row = db
        .query_opt(
            "SELECT id,workspace_id,creator_type,creator_id,origin_type,origin_id \
             FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&issue_id, &workspace_id],
        )
        .await
        .ok()??;
    Some(IssueFacts {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        creator_type: row.get("creator_type"),
        creator_id: row.get("creator_id"),
        origin_type: row.get("origin_type"),
        origin_id: row.get("origin_id"),
    })
}

/// Resolve attribution for a comment-triggered run. Performs the reads and hands
/// the facts to [`attribution::classify_comment`].
///
/// The returned originator is byte-identical to the pre-attribution originator
/// resolution, so authorization behavior is unchanged.
pub async fn attribution_from_trigger_comment(
    db: &impl GenericClient,
    workspace_id: Uuid,
    comment_id: Option<Uuid>,
    agent_authored_source: Source,
) -> AttributionResult {
    let Some(comment_id) = comment_id else {
        return AttributionResult {
            source: Some(Source::Unattributed),
            ..AttributionResult::default()
        };
    };
    let Ok(Some(row)) = db
        .query_opt(
            "SELECT id,author_type,author_id,created_at,source_task_id FROM comment \
             WHERE id=$1 AND workspace_id=$2",
            &[&comment_id, &workspace_id],
        )
        .await
    else {
        return AttributionResult {
            source: Some(Source::Unattributed),
            ..AttributionResult::default()
        };
    };
    attribution_from_comment(db, &row, agent_authored_source).await
}

/// Classify a run from an already-loaded trigger comment, so a caller that
/// already has the row (e.g. to inspect `author_type`) does not re-read it.
pub async fn attribution_from_comment(
    db: &impl GenericClient,
    comment: &tokio_postgres::Row,
    agent_authored_source: Source,
) -> AttributionResult {
    let author_type: String = comment.get("author_type");
    let mut facts = CommentAttributionFacts {
        comment_id: comment.get("id"),
        author_type: author_type.clone(),
        author_id: comment.get("author_id"),
        created_at: comment
            .get::<_, Option<DateTime<Utc>>>("created_at")
            .or_else(|| Some(Utc::now())),
        ..CommentAttributionFacts::default()
    };
    // For an agent-authored comment, walk comment.source_task_id → parent task →
    // parent.originator_user_id. A NULL/missing source task leaves the parent
    // fields unset, which `classify_comment` maps to unattributed.
    if author_type == "agent"
        && let Some(source_task_id) = comment.get::<_, Option<Uuid>>("source_task_id")
    {
        facts.source_task_id = Some(source_task_id);
        if let Ok(Some(parent)) = db
            .query_opt(
                "SELECT originator_user_id,accountable_user_id FROM agent_task_queue WHERE id=$1",
                &[&source_task_id],
            )
            .await
        {
            facts.parent_originator = parent.get("originator_user_id");
            facts.parent_accountable = parent.get("accountable_user_id");
        }
    }
    attribution::classify_comment(&facts, agent_authored_source)
}

/// Resolve the full attribution for an issue-backed enqueue.
///
/// Order matters and is Go's (MUL-4302 §4/§5): a direct member action first,
/// because a manual rerun may INHERIT a trigger comment for the daemon's prompt
/// context but must still attribute to the member who clicked rerun. Then the
/// trigger comment, then the autopilot origin, then the issue's own provenance.
pub async fn attribution_for_issue_task(
    db: &impl GenericClient,
    issue: &IssueFacts,
    trigger_comment_id: Option<Uuid>,
    agent_authored_source: Source,
    actor_user_id: Option<Uuid>,
) -> AttributionResult {
    if let Some(actor) = actor_user_id {
        return attribution::classify_direct(&DirectFacts {
            issue_id: Some(issue.id),
            actor_user_id: Some(actor),
            ..DirectFacts::default()
        });
    }
    if let Some(comment_id) = trigger_comment_id {
        // Workspace-scoped so a foreign comment UUID cannot resolve a human from
        // another tenant.
        if let Ok(Some(comment)) = db
            .query_opt(
                "SELECT id,author_type,author_id,created_at,source_task_id FROM comment \
                 WHERE id=$1 AND workspace_id=$2",
                &[&comment_id, &issue.workspace_id],
            )
            .await
        {
            // A SYSTEM-authored comment — historically the Stage-completion
            // child-done comment, which woke the parent assignee and threaded no
            // actor — carries no human and is not part of any delegation chain.
            // Classifying it would degrade straight to owner_fallback (the agent's
            // own owner), which is wrong for a Stage cascade: the woken run should
            // be accountable to whoever caused the PARENT issue to exist. So skip
            // it and fall through to the parent issue's own provenance.
            if comment.get::<_, String>("author_type") != "system" {
                return attribution_from_comment(db, &comment, agent_authored_source).await;
            }
        }
    }
    // An autopilot-origin issue (origin_id is the autopilot id) from a schedule /
    // webhook trigger attributes to the firing trigger's persisted created_by
    // principal, degrading to the audit-only rule publisher when the trigger has
    // none. The issue only stores the autopilot id, so bridge issue → latest run
    // → trigger_id, including terminal runs so later handoffs retain the
    // principal.
    if issue.origin_type.as_deref() == Some("autopilot")
        && let Some(autopilot_id) = issue.origin_id
    {
        let trigger_id = db
            .query_opt(
                "SELECT r.trigger_id FROM autopilot_run r JOIN autopilot a ON a.id=r.autopilot_id \
                 WHERE r.issue_id=$1 AND r.autopilot_id=$2 AND a.workspace_id=$3 \
                 ORDER BY r.created_at DESC, r.id DESC LIMIT 1",
                &[&issue.id, &autopilot_id, &issue.workspace_id],
            )
            .await
            .ok()
            .flatten()
            .and_then(|row| row.get::<_, Option<Uuid>>("trigger_id"));
        return trigger_owner_attribution(
            db,
            trigger_id,
            issue.workspace_id,
            Some(autopilot_id),
            EvidenceKind::IssueAssignment,
            Some(issue.id),
        )
        .await;
    }
    let mut facts = DirectFacts {
        issue_id: Some(issue.id),
        creator_type: issue.creator_type.clone(),
        creator_id: issue.creator_id,
        ..DirectFacts::default()
    };
    // Member-created issues resolve without a DB read. Only origin-linked
    // agent-created issues (quick_create, agent_create) need the origin task to
    // inherit its human. Both stamp origin_id with the agent_task_queue row that
    // created the issue.
    if !(issue.creator_type == "member" && issue.creator_id.is_some())
        && matches!(
            issue.origin_type.as_deref(),
            Some("quick_create") | Some("agent_create")
        )
        && let Some(origin_task_id) = issue.origin_id
    {
        facts.origin_type = issue.origin_type.clone().unwrap_or_default();
        facts.origin_task_id = Some(origin_task_id);
        if let Ok(Some(task)) = db
            .query_opt(
                "SELECT originator_user_id,accountable_user_id FROM agent_task_queue WHERE id=$1",
                &[&origin_task_id],
            )
            .await
        {
            facts.origin_originator = task.get("originator_user_id");
            facts.origin_accountable = task.get("accountable_user_id");
        }
    }
    attribution::classify_direct(&facts)
}

/// Resolve an autopilot schedule/webhook run to the firing trigger's dispatch
/// principal (`created_by`). No principal degrades to [`rule_owner_attribution`],
/// which is audit-only — the run then carries no originator and the invoke gate
/// fails closed. Never errors: attribution must not fail an enqueue.
pub async fn trigger_owner_attribution(
    db: &impl GenericClient,
    trigger_id: Option<Uuid>,
    workspace_id: Uuid,
    autopilot_id: Option<Uuid>,
    evidence_kind: EvidenceKind,
    evidence_ref_id: Option<Uuid>,
) -> AttributionResult {
    if let Some(principal) =
        resolve_autopilot_trigger_principal(db, trigger_id, autopilot_id, workspace_id).await
    {
        return AttributionResult::trigger_owner(Some(principal), evidence_kind, evidence_ref_id);
    }
    rule_owner_attribution(
        db,
        workspace_id,
        autopilot_id,
        evidence_kind,
        evidence_ref_id,
    )
    .await
}

/// The human a schedule/webhook dispatch ACTS AS — the trigger's `created_by` —
/// or `None` when there is none, in which case every caller must fail closed
/// rather than substitute a different human.
///
/// Three conditions, all required, all fail-closed:
///
/// * the trigger row is fetched BOUND to this autopilot AND its workspace, so a
///   trigger id from another autopilot, or from another tenant, cannot select the
///   principal. The membership check is not a substitute: it proves the resolved
///   human is in the workspace passed in, which a member of two workspaces
///   satisfies even when the trigger came from the other one;
/// * `created_by` names a member — a legacy trigger that no backfill could fill
///   resolves nobody;
/// * that member is STILL in the autopilot's workspace, re-checked on every
///   dispatch, so removing someone actually revokes what their triggers can do.
pub async fn resolve_autopilot_trigger_principal(
    db: &impl GenericClient,
    trigger_id: Option<Uuid>,
    autopilot_id: Option<Uuid>,
    workspace_id: Uuid,
) -> Option<Uuid> {
    let (Some(trigger_id), Some(autopilot_id)) = (trigger_id, autopilot_id) else {
        return None;
    };
    let trigger = db
        .query_opt(
            "SELECT t.created_by_type,t.created_by_id FROM autopilot_trigger t \
             JOIN autopilot a ON a.id=t.autopilot_id \
             WHERE t.id=$1 AND t.autopilot_id=$2 AND a.workspace_id=$3",
            &[&trigger_id, &autopilot_id, &workspace_id],
        )
        .await
        .ok()??;
    if trigger
        .get::<_, Option<String>>("created_by_type")
        .as_deref()
        != Some("member")
    {
        return None;
    }
    let creator: Option<Uuid> = trigger.get("created_by_id");
    let creator = creator?;
    db.query_opt(
        "SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2",
        &[&creator, &workspace_id],
    )
    .await
    .ok()?
    .map(|_| creator)
}

/// Resolve the `rule_owner` attribution for an autopilot from its active (latest)
/// rule version snapshot. Only the audit side is set, to the version's member
/// publisher. A missing version or a non-member/absent publisher degrades to
/// unattributed rather than fabricating a human.
pub async fn rule_owner_attribution(
    db: &impl GenericClient,
    workspace_id: Uuid,
    autopilot_id: Option<Uuid>,
    evidence_kind: EvidenceKind,
    evidence_ref_id: Option<Uuid>,
) -> AttributionResult {
    let Some(autopilot_id) = autopilot_id else {
        return AttributionResult::rule_owner(None, None, evidence_kind, evidence_ref_id);
    };
    let Ok(Some(version)) = db
        .query_opt(
            "SELECT id,published_by_type,published_by_id FROM autopilot_rule_version \
             WHERE workspace_id=$1 AND autopilot_id=$2 ORDER BY created_at DESC LIMIT 1",
            &[&workspace_id, &autopilot_id],
        )
        .await
    else {
        return AttributionResult::rule_owner(None, None, evidence_kind, evidence_ref_id);
    };
    let publisher = if version.get::<_, String>("published_by_type") == "member" {
        version.get::<_, Option<Uuid>>("published_by_id")
    } else {
        None
    };
    AttributionResult::rule_owner(
        publisher,
        Some(version.get::<_, Uuid>("id")),
        evidence_kind,
        evidence_ref_id,
    )
}

/// The fail-closed gate every enqueue path runs after classification:
///
/// * already precise → pass through;
/// * workspace opted into fail-closed → REFUSE;
/// * workspace policy unreadable → REFUSE (we cannot confirm fallback is allowed);
/// * otherwise → `owner_fallback` (accountable = agent owner, audit-only,
///   originator untouched). With no agent owner it stays unattributed → REFUSE
///   rather than enqueue a NULL-accountable task.
///
/// Keeping this at the enqueue boundary (not inside the pure classifiers) means
/// `owner_fallback` needs the agent owner, which every enqueue path has in hand.
pub async fn apply_attribution_fallback(
    db: &impl GenericClient,
    attr: AttributionResult,
    agent_workspace_id: Option<Uuid>,
    agent_owner_id: Option<Uuid>,
) -> Result<AttributionResult, AttributionFailClosed> {
    if attr.source != Some(Source::Unattributed) {
        return Ok(attr);
    }
    let Some(workspace_id) = agent_workspace_id else {
        return Err(AttributionFailClosed);
    };
    let Ok(row) = db
        .query_one(
            "SELECT attribution_fail_closed FROM workspace WHERE id=$1",
            &[&workspace_id],
        )
        .await
    else {
        // Cannot confirm the workspace allows fallback → fail closed rather than
        // silently run an unattributable task.
        return Err(AttributionFailClosed);
    };
    if row.get::<_, bool>("attribution_fail_closed") {
        return Err(AttributionFailClosed);
    }
    let fallback = attr.owner_fallback(agent_owner_id);
    if fallback.source == Some(Source::Unattributed) {
        // No agent owner to attribute to: refuse rather than enqueue a
        // NULL-accountable task.
        return Err(AttributionFailClosed);
    }
    Ok(fallback)
}

/// The identical gate the fresh-enqueue path uses, for a comment being MERGED
/// into an already-queued task. A merge re-stamps the run to the new comment, so
/// "the enqueue already checked" does not carry over: a failed merge keeps the
/// original precisely-attributed snapshot rather than re-stamping a queued run to
/// a degraded `owner_fallback`.
pub async fn attribution_for_merged_comment(
    db: &impl GenericClient,
    workspace_id: Uuid,
    comment_id: Option<Uuid>,
    is_mention: bool,
    agent_workspace_id: Option<Uuid>,
    agent_owner_id: Option<Uuid>,
) -> Result<AttributionResult, AttributionFailClosed> {
    let agent_authored_source = if is_mention {
        Source::Delegation
    } else {
        Source::CommentSource
    };
    let attr =
        attribution_from_trigger_comment(db, workspace_id, comment_id, agent_authored_source).await;
    apply_attribution_fallback(db, attr, agent_workspace_id, agent_owner_id).await
}

/// One-line trigger snapshot for the task row, workspace-scoped. `None` when the
/// comment cannot be read or flattens to nothing.
pub async fn build_comment_trigger_summary(
    db: &impl GenericClient,
    workspace_id: Uuid,
    comment_id: Option<Uuid>,
) -> Option<String> {
    let comment_id = comment_id?;
    let row = db
        .query_opt(
            "SELECT content FROM comment WHERE id=$1 AND workspace_id=$2",
            &[&comment_id, &workspace_id],
        )
        .await
        .ok()??;
    let content: String = row.get("content");
    let summary = attribution::truncate_for_summary(&content, TRIGGER_SUMMARY_MAX_RUNES);
    (!summary.is_empty()).then_some(summary)
}

/// Note that this member personally started an agent run, for the self-host
/// telemetry "delegating member" counts.
///
/// Call it once the run row (or the fold into a queued row) is committed, with
/// the attribution that row was written with. Runs that only inherit a human
/// (retries, system wakeups, delegation, autopilot) carry `human_action=false`
/// and are skipped.
///
/// Best-effort and outside any caller transaction: a failed write costs one
/// activity mark, never the run.
pub async fn record_human_delegation(
    db: &impl GenericClient,
    attr: &AttributionResult,
    activity_recording_enabled: bool,
) {
    if !activity_recording_enabled || !attr.human_action {
        return;
    }
    let Some(user_id) = attr.user_id else {
        return;
    };
    let acted_at = attr.human_acted_at;
    let _ = db
        .execute(
            "INSERT INTO member_delegation_day (user_id, activity_date) VALUES ($1, \
             (COALESCE($2::timestamptz, CURRENT_TIMESTAMP) AT TIME ZONE 'UTC')::date) \
             ON CONFLICT DO NOTHING",
            &[&user_id, &acted_at],
        )
        .await;
}

/// The self-host telemetry switch, from the raw `DO_NOT_TRACK` value. Default-on:
/// only `1` or `true` (case-insensitive, trimmed) disable the activity ledger;
/// every other value — including empty — leaves it on.
pub fn member_activity_recording_enabled(raw: Option<&str>) -> bool {
    let raw = raw.unwrap_or_default().trim();
    !(raw == "1" || raw.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_is_on_unless_do_not_track_says_otherwise() {
        assert!(member_activity_recording_enabled(None));
        assert!(member_activity_recording_enabled(Some("")));
        assert!(member_activity_recording_enabled(Some("0")));
        assert!(member_activity_recording_enabled(Some("yes")));
        assert!(!member_activity_recording_enabled(Some("1")));
        assert!(!member_activity_recording_enabled(Some(" 1 ")));
        assert!(!member_activity_recording_enabled(Some("TRUE")));
    }
}
