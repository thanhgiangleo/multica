//! Accountable-human resolution for agent task runs.
//!
//! Port of Go's `internal/attribution` package: the vocabulary (`Source`,
//! `EvidenceKind`) and the PURE classification rules. The database reads that
//! gather the facts live in [`super::attribution_db`], exactly as Go keeps them
//! in `service.TaskService`, so the rules here stay side-effect-free and fully
//! unit-testable.
//!
//! Three invariants the shape protects, all copied verbatim from Go:
//!
//! * `finalize` enforces `originator_user_id IS NOT NULL ⟹ accountable_user_id =
//!   originator_user_id`. Every constructor flows through it, so no caller can
//!   stamp a row whose two person columns disagree.
//! * the source label is provenance only. No permission decision reads it — the
//!   *originator* is the authorization value, and it is byte-identical to what
//!   the pre-attribution originator resolution produced.
//! * a degraded label (`owner_fallback`, `backfill`, `unattributed`) is never
//!   silently promoted to a precise one, and `owner_fallback` never fabricates a
//!   human: it needs a real agent owner or it stays unattributed.

use uuid::Uuid;

/// The waterfall level that resolved the accountable human. Stored verbatim in
/// `agent_task_queue.originator_source`; free strings in the database (no CHECK)
/// so a newly-modeled trigger path can introduce a source without a migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// A member's own action enqueued the run. The member IS the accountable
    /// human.
    DirectHuman,
    /// An agent running on behalf of a human caused the enqueue. The parent
    /// task's accountable human is copied, not chained, so delegation cycles
    /// stay harmless.
    Delegation,
    /// The standing assignee reacted to an agent/system-authored comment; the
    /// human is resolved through `comment.source_task_id`.
    CommentSource,
    /// An autopilot schedule/webhook trigger enqueued the run; the human is that
    /// trigger's persisted `created_by` principal.
    TriggerOwner,
    /// An autopilot trigger with no `created_by` principal: the ACCOUNTABLE human
    /// degrades to the publisher of the rule's active version, while
    /// authorization carries none. Precise for audit, deliberately powerless.
    RuleOwner,
    /// Nothing above resolved a human, so attribution degrades to the agent
    /// owner. Degraded, not compliance-grade, and surfaced distinctly.
    OwnerFallback,
    /// A historical row attributed after the fact by the backfill command.
    Backfill,
    /// No human could be resolved and no fallback was applied. Distinct from a
    /// NULL (pre-migration) source: an explicit "we looked and found nobody".
    Unattributed,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DirectHuman => "direct_human",
            Self::Delegation => "delegation",
            Self::CommentSource => "comment_source",
            Self::TriggerOwner => "trigger_owner",
            Self::RuleOwner => "rule_owner",
            Self::OwnerFallback => "owner_fallback",
            Self::Backfill => "backfill",
            Self::Unattributed => "unattributed",
        }
    }

    /// Whether this is a compliance-grade (non-degraded) attribution.
    pub fn precise(self) -> bool {
        matches!(
            self,
            Self::DirectHuman
                | Self::Delegation
                | Self::CommentSource
                | Self::TriggerOwner
                | Self::RuleOwner
        )
    }
}

/// Tags the direct cause of a run so every attribution can jump to its evidence
/// row. Free strings paired with an evidence ref id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceKind {
    Comment,
    IssueAssignment,
    AutopilotRun,
    RuleVersion,
    Rerun,
    /// Points at the terminal worker task that handed control back to its source
    /// coordinator.
    DelegatedFailure,
    /// The chat session that triggered the run.
    Chat,
}

impl EvidenceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Comment => "comment",
            Self::IssueAssignment => "issue_assignment",
            Self::AutopilotRun => "autopilot_run",
            Self::RuleVersion => "rule_version",
            Self::Rerun => "rerun",
            Self::DelegatedFailure => "delegated_failure",
            Self::Chat => "chat",
        }
    }
}

/// A resolved attribution.
///
/// `user_id` is the originator and the authorization value; `accountable_user_id`
/// is the human on the hook for audit. They are kept equal whenever the
/// originator resolved, because [`finalize`] mirrors one onto the other. The only
/// divergence point is a run whose originator is NULL but whose accountable
/// human is known — `rule_owner` (rule publisher) and `owner_fallback` (agent
/// owner) name somebody for audit while authorization correctly carries nobody.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AttributionResult {
    pub user_id: Option<Uuid>,
    pub accountable_user_id: Option<Uuid>,
    pub source: Option<Source>,
    pub delegated_from_task_id: Option<Uuid>,
    pub rule_version_id: Option<Uuid>,
    pub retry_of_task_id: Option<Uuid>,
    pub rerun_of_task_id: Option<Uuid>,
    pub evidence_kind: Option<EvidenceKind>,
    pub evidence_ref_id: Option<Uuid>,
    /// True only when the originator's own deliberate action caused this enqueue
    /// (a member comment, an assign/promote/manual trigger/rerun, a chat message,
    /// a quick-create). Activity reporting only — never consulted for
    /// authorization.
    pub human_action: bool,
    /// When that action happened, when it is known to differ from the enqueue
    /// time (a member comment folded or re-enqueued later). `None` means "now".
    pub human_acted_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl AttributionResult {
    pub fn unattributed(evidence_kind: EvidenceKind, evidence_ref_id: Option<Uuid>) -> Self {
        finalize(Self {
            source: Some(Source::Unattributed),
            evidence_kind: Some(evidence_kind),
            evidence_ref_id,
            ..Self::default()
        })
    }

    pub fn direct_human(
        user_id: Option<Uuid>,
        kind: EvidenceKind,
        reference: Option<Uuid>,
    ) -> Self {
        let Some(user_id) = user_id else {
            return Self::unattributed(kind, reference);
        };
        finalize(Self {
            user_id: Some(user_id),
            source: Some(Source::DirectHuman),
            evidence_kind: Some(kind),
            evidence_ref_id: reference,
            human_action: true,
            ..Self::default()
        })
    }

    /// `rule_owner`: audit-accountable only, originator stays NULL so authorization
    /// fails closed. A missing publisher degrades to unattributed rather than
    /// fabricating a human.
    pub fn rule_owner(
        publisher: Option<Uuid>,
        rule_version_id: Option<Uuid>,
        kind: EvidenceKind,
        reference: Option<Uuid>,
    ) -> Self {
        let mut result = Self {
            rule_version_id,
            evidence_kind: Some(kind),
            evidence_ref_id: reference,
            ..Self::default()
        };
        match publisher {
            Some(publisher) => {
                result.source = Some(Source::RuleOwner);
                result.accountable_user_id = Some(publisher);
            }
            None => result.source = Some(Source::Unattributed),
        }
        finalize(result)
    }

    /// `trigger_owner`: the trigger's `created_by` principal is BOTH originator
    /// and accountable, because arming a trigger authorizes the run it fires the
    /// same way clicking "run now" does. No principal → unattributed.
    pub fn trigger_owner(
        creator: Option<Uuid>,
        kind: EvidenceKind,
        reference: Option<Uuid>,
    ) -> Self {
        let mut result = Self {
            evidence_kind: Some(kind),
            evidence_ref_id: reference,
            ..Self::default()
        };
        match creator {
            Some(creator) => {
                result.source = Some(Source::TriggerOwner);
                result.user_id = Some(creator);
            }
            None => result.source = Some(Source::Unattributed),
        }
        finalize(result)
    }

    /// Degrade an UNATTRIBUTED result to `owner_fallback`. Audit-only — the
    /// originator stays NULL, so authorization is unaffected. A precise result, or
    /// no agent owner to fall back to, passes through unchanged so a human is
    /// never fabricated.
    pub fn owner_fallback(self, owner: Option<Uuid>) -> Self {
        let Some(owner) = owner else {
            return self;
        };
        if self.source != Some(Source::Unattributed) {
            return self;
        }
        Self {
            source: Some(Source::OwnerFallback),
            accountable_user_id: Some(owner),
            ..self
        }
    }
}

/// Enforce the one-way accountability invariant: a resolved originator IS the
/// accountable human. Every `AttributionResult` flows through here.
fn finalize(mut result: AttributionResult) -> AttributionResult {
    if let Some(user_id) = result.user_id {
        result.accountable_user_id = Some(user_id);
    }
    result
}

/// Already-fetched facts about a trigger comment.
#[derive(Clone, Debug, Default)]
pub struct CommentAttributionFacts {
    pub comment_id: Option<Uuid>,
    /// `member` | `agent` | `system` | other.
    pub author_type: String,
    pub author_id: Option<Uuid>,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    /// For agent-authored comments: `comment.source_task_id`, and that task's
    /// `originator_user_id` / `accountable_user_id`.
    pub source_task_id: Option<Uuid>,
    pub parent_originator: Option<Uuid>,
    /// Lets an autopilot-rooted chain — where the parent has NO authorizing human
    /// but IS accountable to someone — copy that responsible human down the
    /// delegation instead of dropping the chain root to unattributed.
    pub parent_accountable: Option<Uuid>,
}

/// Resolve attribution for a comment-triggered run from already-fetched facts.
/// `agent_authored_source` labels the agent-authored case: `CommentSource` for
/// the issue-assignee-reacting path, `Delegation` for an explicit mention /
/// thread-parent / squad-leader path.
pub fn classify_comment(
    facts: &CommentAttributionFacts,
    agent_authored_source: Source,
) -> AttributionResult {
    match facts.author_type.as_str() {
        "member" => finalize(AttributionResult {
            user_id: facts.author_id,
            source: Some(Source::DirectHuman),
            evidence_kind: Some(EvidenceKind::Comment),
            evidence_ref_id: facts.comment_id,
            human_action: facts.author_id.is_some(),
            human_acted_at: facts.created_at,
            ..AttributionResult::default()
        }),
        "agent" => {
            let mut result = AttributionResult {
                evidence_kind: Some(EvidenceKind::Comment),
                evidence_ref_id: facts.comment_id,
                ..AttributionResult::default()
            };
            let Some(source_task_id) = facts.source_task_id else {
                // Agent comment with no source task: cannot walk the chain.
                result.source = Some(Source::Unattributed);
                return finalize(result);
            };
            result.delegated_from_task_id = Some(source_task_id);
            if let Some(parent) = facts.parent_originator {
                result.user_id = Some(parent);
                result.source = Some(agent_authored_source);
            } else if let Some(accountable) = facts.parent_accountable {
                // Autopilot-rooted chain: originator stays NULL so authorization
                // is unchanged, but the responsibility chain root stays stable at
                // any depth.
                result.accountable_user_id = Some(accountable);
                result.source = Some(agent_authored_source);
            } else {
                // Source task exists but has no human at its own top of chain.
                result.source = Some(Source::Unattributed);
            }
            finalize(result)
        }
        _ => finalize(AttributionResult {
            source: Some(Source::Unattributed),
            evidence_kind: Some(EvidenceKind::Comment),
            evidence_ref_id: facts.comment_id,
            ..AttributionResult::default()
        }),
    }
}

/// Facts for a run with no trigger comment: a direct assignment/creation, or an
/// agent-created issue with a quick-create origin.
#[derive(Clone, Debug, Default)]
pub struct DirectFacts {
    pub issue_id: Option<Uuid>,
    pub creator_type: String,
    pub creator_id: Option<Uuid>,
    /// The member who PERFORMED the action that enqueued this run. The person who
    /// acted — not whoever happened to file the issue — is on the hook.
    pub actor_user_id: Option<Uuid>,
    /// `quick_create` | `agent_create` | empty.
    pub origin_type: String,
    pub origin_task_id: Option<Uuid>,
    pub origin_originator: Option<Uuid>,
    pub origin_accountable: Option<Uuid>,
}

/// Resolve attribution for a run with no trigger comment.
pub fn classify_direct(facts: &DirectFacts) -> AttributionResult {
    // A member who directly assigned/promoted the issue is the accountable human,
    // ahead of the issue's creator.
    if let Some(actor) = facts.actor_user_id {
        return finalize(AttributionResult {
            user_id: Some(actor),
            source: Some(Source::DirectHuman),
            evidence_kind: Some(EvidenceKind::IssueAssignment),
            evidence_ref_id: facts.issue_id,
            human_action: true,
            ..AttributionResult::default()
        });
    }
    if facts.creator_type == "member"
        && let Some(creator) = facts.creator_id
    {
        return finalize(AttributionResult {
            user_id: Some(creator),
            source: Some(Source::DirectHuman),
            evidence_kind: Some(EvidenceKind::IssueAssignment),
            evidence_ref_id: facts.issue_id,
            ..AttributionResult::default()
        });
    }
    match facts.origin_type.as_str() {
        "quick_create" | "agent_create" => {
            let mut result = AttributionResult {
                delegated_from_task_id: facts.origin_task_id,
                evidence_kind: Some(EvidenceKind::IssueAssignment),
                evidence_ref_id: facts.issue_id,
                ..AttributionResult::default()
            };
            if let Some(originator) = facts.origin_originator {
                result.user_id = Some(originator);
                result.source = Some(Source::Delegation);
            } else if let Some(accountable) = facts.origin_accountable {
                result.accountable_user_id = Some(accountable);
                result.source = Some(Source::Delegation);
            } else {
                result.source = Some(Source::Unattributed);
            }
            finalize(result)
        }
        _ => finalize(AttributionResult {
            source: Some(Source::Unattributed),
            evidence_kind: Some(EvidenceKind::IssueAssignment),
            evidence_ref_id: facts.issue_id,
            ..AttributionResult::default()
        }),
    }
}

/// The provenance columns a fresh `agent_task_queue` insert carries.
/// `originator_source` is always stamped (never NULL for a new row); delegation
/// lineage and evidence are stamped only when present.
#[derive(Clone, Copy, Debug, Default)]
pub struct AttributionColumns {
    pub originator_user_id: Option<Uuid>,
    pub accountable_user_id: Option<Uuid>,
    pub originator_source: &'static str,
    pub delegated_from_task_id: Option<Uuid>,
    pub rule_version_id: Option<Uuid>,
    pub trigger_evidence_kind: Option<&'static str>,
    pub trigger_evidence_ref_id: Option<Uuid>,
}

pub fn attribution_columns(attr: &AttributionResult) -> AttributionColumns {
    AttributionColumns {
        originator_user_id: attr.user_id,
        accountable_user_id: attr.accountable_user_id,
        originator_source: attr.source.unwrap_or(Source::Unattributed).as_str(),
        delegated_from_task_id: attr.delegated_from_task_id,
        rule_version_id: attr.rule_version_id,
        trigger_evidence_kind: attr.evidence_kind.map(EvidenceKind::as_str),
        trigger_evidence_ref_id: attr.evidence_ref_id,
    }
}

/// Caps the trigger summary so the row stays cheap to transmit (it ends up in
/// every task list response). Go uses the same 200-rune budget.
pub const TRIGGER_SUMMARY_MAX_RUNES: usize = 200;

/// Flatten and shorten a comment body into the one-line trigger snapshot. Newlines
/// and tabs become spaces so a leading newline does not waste the budget, and the
/// cut is on runes so multibyte characters count as one each.
pub fn truncate_for_summary(text: &str, max_runes: usize) -> String {
    let flattened: String = text
        .chars()
        .map(|c| match c {
            '\n' | '\r' | '\t' => ' ',
            other => other,
        })
        .collect();
    let trimmed: Vec<char> = flattened.trim().chars().collect();
    if trimmed.len() <= max_runes {
        return trimmed.into_iter().collect();
    }
    let mut out: String = trimmed[..max_runes].iter().collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn a_member_comment_is_a_direct_human_run() {
        let attr = classify_comment(
            &CommentAttributionFacts {
                comment_id: Some(id(1)),
                author_type: "member".into(),
                author_id: Some(id(2)),
                ..Default::default()
            },
            Source::CommentSource,
        );
        assert_eq!(attr.source, Some(Source::DirectHuman));
        assert_eq!(attr.user_id, Some(id(2)));
        assert_eq!(attr.accountable_user_id, Some(id(2)));
        assert!(attr.human_action);
        assert_eq!(attr.evidence_kind, Some(EvidenceKind::Comment));
    }

    #[test]
    fn an_agent_comment_copies_the_parent_human_as_a_delegation() {
        let attr = classify_comment(
            &CommentAttributionFacts {
                comment_id: Some(id(1)),
                author_type: "agent".into(),
                author_id: Some(id(3)),
                source_task_id: Some(id(4)),
                parent_originator: Some(id(5)),
                ..Default::default()
            },
            Source::Delegation,
        );
        assert_eq!(attr.source, Some(Source::Delegation));
        assert_eq!(attr.user_id, Some(id(5)));
        assert_eq!(attr.delegated_from_task_id, Some(id(4)));
        // Inheriting a human is not that human acting.
        assert!(!attr.human_action);
    }

    #[test]
    fn an_agent_comment_with_no_human_anywhere_is_unattributed() {
        // No source task at all.
        let missing = classify_comment(
            &CommentAttributionFacts {
                author_type: "agent".into(),
                source_task_id: None,
                ..Default::default()
            },
            Source::Delegation,
        );
        assert_eq!(missing.source, Some(Source::Unattributed));
        // Source task present but with no human at its own top of chain.
        let barren = classify_comment(
            &CommentAttributionFacts {
                author_type: "agent".into(),
                source_task_id: Some(id(4)),
                ..Default::default()
            },
            Source::Delegation,
        );
        assert_eq!(barren.source, Some(Source::Unattributed));
        assert_eq!(barren.delegated_from_task_id, Some(id(4)));
    }

    #[test]
    fn an_autopilot_rooted_chain_copies_accountable_without_authorizing() {
        let attr = classify_comment(
            &CommentAttributionFacts {
                author_type: "agent".into(),
                source_task_id: Some(id(4)),
                parent_originator: None,
                parent_accountable: Some(id(6)),
                ..Default::default()
            },
            Source::Delegation,
        );
        assert_eq!(attr.source, Some(Source::Delegation));
        assert_eq!(attr.user_id, None, "no human authorized this run");
        assert_eq!(attr.accountable_user_id, Some(id(6)));
    }

    #[test]
    fn a_system_comment_carries_no_human() {
        let attr = classify_comment(
            &CommentAttributionFacts {
                author_type: "system".into(),
                ..Default::default()
            },
            Source::CommentSource,
        );
        assert_eq!(attr.source, Some(Source::Unattributed));
        assert_eq!(attr.user_id, None);
    }

    #[test]
    fn an_actor_outranks_the_issue_creator() {
        let attr = classify_direct(&DirectFacts {
            issue_id: Some(id(1)),
            creator_type: "member".into(),
            creator_id: Some(id(2)),
            actor_user_id: Some(id(3)),
            ..Default::default()
        });
        assert_eq!(attr.user_id, Some(id(3)));
        assert_eq!(attr.accountable_user_id, Some(id(3)));
        assert!(attr.human_action);
    }

    #[test]
    fn a_member_created_issue_attributes_to_its_creator_without_acting() {
        let attr = classify_direct(&DirectFacts {
            issue_id: Some(id(1)),
            creator_type: "member".into(),
            creator_id: Some(id(2)),
            ..Default::default()
        });
        assert_eq!(attr.source, Some(Source::DirectHuman));
        assert_eq!(attr.user_id, Some(id(2)));
        // Filing an issue is not the action that enqueued this run.
        assert!(!attr.human_action);
    }

    #[test]
    fn an_agent_created_issue_inherits_its_origin_task() {
        let attr = classify_direct(&DirectFacts {
            issue_id: Some(id(1)),
            creator_type: "agent".into(),
            creator_id: Some(id(2)),
            origin_type: "agent_create".into(),
            origin_task_id: Some(id(7)),
            origin_originator: Some(id(8)),
            ..Default::default()
        });
        assert_eq!(attr.source, Some(Source::Delegation));
        assert_eq!(attr.user_id, Some(id(8)));
        assert_eq!(attr.delegated_from_task_id, Some(id(7)));

        let rooted = classify_direct(&DirectFacts {
            issue_id: Some(id(1)),
            creator_type: "agent".into(),
            origin_type: "quick_create".into(),
            origin_task_id: Some(id(7)),
            origin_accountable: Some(id(9)),
            ..Default::default()
        });
        assert_eq!(rooted.source, Some(Source::Delegation));
        assert_eq!(rooted.user_id, None);
        assert_eq!(rooted.accountable_user_id, Some(id(9)));
    }

    #[test]
    fn an_issue_with_no_human_anywhere_is_unattributed() {
        let attr = classify_direct(&DirectFacts {
            issue_id: Some(id(1)),
            creator_type: "system".into(),
            ..Default::default()
        });
        assert_eq!(attr.source, Some(Source::Unattributed));
        assert_eq!(attr.user_id, None);
    }

    #[test]
    fn owner_fallback_only_degrades_an_unattributed_result() {
        let degraded = AttributionResult {
            source: Some(Source::Unattributed),
            ..Default::default()
        }
        .owner_fallback(Some(id(9)));
        assert_eq!(degraded.source, Some(Source::OwnerFallback));
        assert_eq!(degraded.accountable_user_id, Some(id(9)));
        assert_eq!(degraded.user_id, None, "audit-only, no authorization");
        assert!(!degraded.source.unwrap().precise());

        // A precise result passes through untouched.
        let precise = AttributionResult {
            source: Some(Source::Delegation),
            user_id: Some(id(1)),
            ..Default::default()
        };
        assert_eq!(precise.owner_fallback(Some(id(9))), precise);

        // No owner to fall back to: stay unattributed rather than fabricate one.
        let barren = AttributionResult {
            source: Some(Source::Unattributed),
            ..Default::default()
        }
        .owner_fallback(None);
        assert_eq!(barren.source, Some(Source::Unattributed));
    }

    #[test]
    fn evaluate_the_source_labels_the_columns_carry() {
        let attr = AttributionResult {
            user_id: Some(id(1)),
            source: Some(Source::CommentSource),
            evidence_kind: Some(EvidenceKind::Comment),
            evidence_ref_id: Some(id(2)),
            ..Default::default()
        };
        let columns = attribution_columns(&attr);
        assert_eq!(columns.originator_source, "comment_source");
        assert_eq!(columns.trigger_evidence_kind, Some("comment"));
        assert_eq!(columns.trigger_evidence_ref_id, Some(id(2)));
        // An empty source still stamps a non-NULL label.
        let empty = attribution_columns(&AttributionResult::default());
        assert_eq!(empty.originator_source, "unattributed");
        assert_eq!(empty.trigger_evidence_kind, None);
    }

    #[test]
    fn rule_owner_is_audit_only_and_trigger_owner_authorizes() {
        let rule = AttributionResult::rule_owner(
            Some(id(3)),
            Some(id(4)),
            EvidenceKind::RuleVersion,
            Some(id(5)),
        );
        assert_eq!(rule.source, Some(Source::RuleOwner));
        assert_eq!(rule.user_id, None, "a rule publisher authorized nothing");
        assert_eq!(rule.accountable_user_id, Some(id(3)));
        assert_eq!(rule.rule_version_id, Some(id(4)));

        let trigger =
            AttributionResult::trigger_owner(Some(id(6)), EvidenceKind::AutopilotRun, Some(id(7)));
        assert_eq!(trigger.source, Some(Source::TriggerOwner));
        assert_eq!(trigger.user_id, Some(id(6)));
        assert_eq!(trigger.accountable_user_id, Some(id(6)));

        // No principal degrades rather than fabricating one.
        assert_eq!(
            AttributionResult::trigger_owner(None, EvidenceKind::AutopilotRun, Some(id(7))).source,
            Some(Source::Unattributed)
        );
    }

    #[test]
    fn summaries_flatten_newlines_and_cut_on_runes() {
        assert_eq!(truncate_for_summary("  hi\nthere\t!", 200), "hi there !");
        let long = "á".repeat(300);
        let cut = truncate_for_summary(&long, 200);
        assert_eq!(cut.chars().count(), 201);
        assert!(cut.ends_with('…'));
        // Exactly at the cap: no ellipsis.
        assert_eq!(truncate_for_summary("abcde", 5), "abcde");
    }
}
