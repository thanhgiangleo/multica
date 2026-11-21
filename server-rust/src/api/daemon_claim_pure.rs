//! Pure claim-time rules that need no database: issue-state snapshots, resume
//! poisoning, comment-delivery budgeting, and the wire shapes they feed.
//!
//! Go spreads these across `issue_snapshot.go`, `task.go` and `daemon.go`.
//! Keeping them here is what makes the claim builder testable without a
//! disposable PostgreSQL, and it pins the byte-level formats (SHA-256 hex,
//! RFC3339Nano generation stamps, the legacy comment bundle) that the daemon
//! contract depends on.
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Shape version of `agent_task_queue.issue_snapshot`. A stored snapshot with
/// any other version is UNKNOWN, never coerced: an old row that lacks a newly
/// compared field would otherwise read as "unchanged" and silently skip a read.
pub(super) const ISSUE_SNAPSHOT_VERSION: i64 = 1;

pub(super) const ISSUE_FIELD_TITLE: &str = "title";
pub(super) const ISSUE_FIELD_DESCRIPTION: &str = "description";

/// Only title and description are compared: they are the issue body, and
/// nothing but a read can deliver them. Status/assignee/priority/labels/parent/
/// due date/stage/project/metadata are shipped as current values or do not
/// change what the agent does, and an issue whose only change is one of them is
/// reported as unchanged.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct IssueStateSnapshot {
    pub v: i64,
    #[serde(default)]
    pub title_sha256: String,
    #[serde(default)]
    pub description_sha256: String,
}

pub(super) fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

pub(super) fn build_issue_state_snapshot(title: &str, description: &str) -> IssueStateSnapshot {
    IssueStateSnapshot {
        v: ISSUE_SNAPSHOT_VERSION,
        title_sha256: sha256_hex(title),
        description_sha256: sha256_hex(description),
    }
}

/// Changed field names in the fixed reporting order. Empty means every compared
/// field matched — and says nothing about the fields outside the set.
pub(super) fn changed_fields_since(
    current: &IssueStateSnapshot,
    prev: &IssueStateSnapshot,
) -> Vec<&'static str> {
    let mut changed = Vec::new();
    if current.title_sha256 != prev.title_sha256 {
        changed.push(ISSUE_FIELD_TITLE);
    }
    if current.description_sha256 != prev.description_sha256 {
        changed.push(ISSUE_FIELD_DESCRIPTION);
    }
    changed
}

/// `None` for every state that means "this claim cannot say what changed":
/// absent column, malformed JSON, or a different shape version. Callers must
/// map `None` to "not compared" and never to "unchanged".
pub(super) fn decode_issue_state_snapshot(raw: Option<&[u8]>) -> Option<IssueStateSnapshot> {
    let raw = raw.filter(|raw| !raw.is_empty())?;
    let snap: IssueStateSnapshot = serde_json::from_slice(raw).ok()?;
    (snap.v == ISSUE_SNAPSHOT_VERSION).then_some(snap)
}

/// The run whose context this claim continues. Both deltas are measured from
/// THIS row, not from whichever run started last: `status` says the run actually
/// delivered its prompt, and a `failed`/`cancelled` row may have died before the
/// agent saw the issue.
#[derive(Clone, Debug)]
pub(super) struct ResumedRunAnchor {
    pub started_at: DateTime<Utc>,
    pub snapshot: Option<IssueStateSnapshot>,
}

/// Go returns nil unless the run completed AND carries a start time: a run that
/// never reached the provider has no prompt context to continue.
pub(super) fn new_resumed_run_anchor(
    status: &str,
    started_at: Option<DateTime<Utc>>,
    snapshot: Option<IssueStateSnapshot>,
) -> Option<ResumedRunAnchor> {
    let started_at = started_at.filter(|_| status == "completed")?;
    Some(ResumedRunAnchor {
        started_at,
        snapshot,
    })
}

/// One comment's edit-relevant timestamps.
#[derive(Clone, Debug)]
pub(super) struct CommentEditFacts {
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// True when a comment was edited after the run that the claim resumes started.
///
/// A comment created after that point is NEW, not edited, so it does not count.
/// The provably resolve-only case — revision 2, a resolve timestamp, and that
/// timestamp equal to `updated_at` — is excluded because resolving is not an
/// edit to the text the agent already read.
pub(super) fn trigger_comment_edited_since(
    comment: &CommentEditFacts,
    run_started_at: Option<DateTime<Utc>>,
) -> bool {
    let (Some(run_started_at), Some(created_at), Some(updated_at)) =
        (run_started_at, comment.created_at, comment.updated_at)
    else {
        return false;
    };
    if comment.revision <= 1 {
        return false;
    }
    if !(created_at < run_started_at && updated_at > run_started_at) {
        return false;
    }
    if comment.revision == 2
        && comment
            .resolved_at
            .is_some_and(|resolved| resolved == updated_at)
    {
        return false;
    }
    true
}

/// Issue-annotation scope for counting comments added since a resumed run.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Ported with the resumed-run counter; its caller is not wired yet.
pub(super) struct CommentCountScope {
    pub anchor_id: uuid::Uuid,
    pub issue_id: uuid::Uuid,
    pub workspace_id: uuid::Uuid,
}

// Failure reasons that poison the agent CONVERSATION (not the workdir): resuming
// the same session would immediately replay the stuck or oversized state.
// Kept in sync with the poison filters in the resume queries.
const RESUME_UNSAFE_REASONS: &[&str] = &[
    "iteration_limit",
    "agent_fallback_message",
    "api_invalid_request",
    "codex_semantic_inactivity",
    "agent_error.context_overflow",
    "codex_resume_oversized",
    "opencode_clock_skew",
];

pub(super) fn resume_unsafe_failure_reason(reason: &str) -> bool {
    RESUME_UNSAFE_REASONS.contains(&reason)
}

/// Whether a failed task's session must NOT be resumed.
///
/// The reason set above is the primary signal; the text guards are
/// defense-in-depth for rows an older daemon wrote before classification
/// existed. They must stay aligned with the SQL resume queries, which apply the
/// same predicates.
pub(super) fn resume_unsafe_failure(failure_reason: &str, error_text: &str) -> bool {
    if resume_unsafe_failure_reason(failure_reason) {
        return true;
    }
    let lower = error_text.to_ascii_lowercase();
    if lower.contains("400") && lower.contains("invalid_request_error") {
        return true;
    }
    if lower.contains("could not resolve authentication method") {
        return true;
    }
    if lower.contains("invalid conversation history") {
        return true;
    }
    empty_content_guard(&lower)
}

/// Provider-agnostic "an empty message is baked into the history" shape. Both
/// halves must match: the content complaint and the locator naming where the
/// empty message sits.
fn empty_content_guard(lower: &str) -> bool {
    let content = [
        "must not be empty",
        "must be non-empty",
        "must be nonempty",
        "must have non-empty",
        "must have nonempty",
        "non-empty content",
        "nonempty content",
        "cannot be empty",
        "should not be empty",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    let locator = [
        "assistant message",
        "message at position",
        "messages.",
        "messages[",
        "role:assistant",
        "role: assistant",
        "role=assistant",
        "role = assistant",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
        || role_assistant_pattern(lower);
    content && locator
}

/// The Go regex is `role[^a-z0-9]{0,2}assistant`: up to two non-alphanumerics
/// between the two words. Checked directly rather than by carrying a regex
/// dependency for one predicate.
fn role_assistant_pattern(lower: &str) -> bool {
    const ROLE: &str = "role";
    const ASSISTANT: &str = "assistant";
    // Compared as chars, not bytes: Go's `[^a-z0-9]` counts one rune per gap
    // slot, so a multi-byte symbol must consume exactly one slot here too.
    let chars: Vec<char> = lower.chars().collect();
    let role: Vec<char> = ROLE.chars().collect();
    let assistant: Vec<char> = ASSISTANT.chars().collect();
    let mut from = 0usize;
    while from + role.len() <= chars.len() {
        if chars[from..from + role.len()] != role[..] {
            from += 1;
            continue;
        }
        let start = from + role.len();
        for gap in 0..=2usize {
            let end = start + gap + assistant.len();
            if end <= chars.len()
                && chars[start..start + gap]
                    .iter()
                    .all(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
                && chars[start + gap..end] == assistant[..]
            {
                return true;
            }
        }
        from = start;
    }
    false
}

/// `fresh_rerun` in the task context: a manual rerun that explicitly discards
/// continuity. The lineage stays intact for attribution and history.
pub(super) fn is_fresh_rerun(context: Option<&Value>) -> bool {
    context.is_some_and(|context| context["fresh_rerun"] == true)
}

/// The worktree-mode refusal reason, or `None` when this runtime may proceed.
///
/// Decided on the CAPABILITY the daemon advertised on this request, never on a
/// version string: a daemon without the implementation does not merely lose
/// concurrency, it ignores `execution_mode` and edits the working copy the user
/// asked to isolate. Only resources bound to this runtime's own daemon count.
pub(super) fn worktree_claim_block_reason(
    resources: &[Value],
    runtime_daemon_id: Option<&str>,
    has_worktree_capability: bool,
) -> Option<String> {
    let daemon_id = runtime_daemon_id.filter(|id| !id.is_empty())?;
    if has_worktree_capability {
        return None;
    }
    for resource in resources {
        if resource["resource_type"] != "local_directory" {
            continue;
        }
        let Some(reference) = resource["resource_ref"].as_object() else {
            continue;
        };
        if reference.get("execution_mode").and_then(Value::as_str) != Some("worktree") {
            continue;
        }
        if reference.get("daemon_id").and_then(Value::as_str) != Some(daemon_id) {
            continue;
        }
        let path = reference
            .get("local_path")
            .and_then(Value::as_str)
            .unwrap_or("");
        return Some(format!(
            "This machine's Multica runtime does not support parallel (worktree) mode, which {path:?} is set to use. \
             Update the Multica app on that machine to the latest version, then re-run this task. \
             Refusing to run rather than falling back to editing the directory directly, which is what this mode exists to prevent."
        ));
    }
    None
}

/// `agent.model_override` lifted out of the stored runtime_config, for an
/// `openclaw` runtime. Empty when the column is empty, `null`, or has no string
/// override — the daemon then uses its own default.
pub(super) fn openclaw_model_override(raw: Option<&Value>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    raw["model_override"]
        .as_str()
        .unwrap_or("")
        .trim()
        .to_owned()
}

/// `DisabledRuntimeSkill` entries that apply to this runtime row.
///
/// The stored column is shared by every runtime an agent runs on, and a plugin
/// set disabled under one provider means nothing to another, so both the
/// runtime id and the provider must match.
pub(super) fn disabled_runtime_skills_for(
    raw: Option<&Value>,
    runtime_id: uuid::Uuid,
    provider: &str,
) -> Value {
    let entries = raw.and_then(Value::as_array).cloned().unwrap_or_default();
    let runtime_id = runtime_id.to_string();
    Value::Array(
        entries
            .into_iter()
            .filter(|entry| {
                entry["runtime_id"].as_str() == Some(runtime_id.as_str())
                    && entry["provider"].as_str() == Some(provider)
            })
            .collect(),
    )
}

/// Go's `uuidStringsOrEmpty`: a marshalled empty slice is `[]`, never `null`.
#[allow(dead_code)] // Kept for the claim response field that will marshal through it.
pub(super) fn uuid_strings_or_empty(ids: &[uuid::Uuid]) -> Value {
    Value::Array(ids.iter().map(|id| json!(id)).collect())
}

/// A loaded comment as the claim sees it, before the delivery budget runs.
#[derive(Clone, Debug)]
pub(super) struct ClaimComment {
    pub id: uuid::Uuid,
    pub thread_id: uuid::Uuid,
    pub content: String,
    pub author_type: String,
    pub author_name: String,
    pub created_at: String,
    pub created_at_value: Option<DateTime<Utc>>,
    pub revision: i64,
    pub updated_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl ClaimComment {
    pub(super) fn entry_size(&self, legacy: bool) -> usize {
        if legacy {
            escaped_json_string_content_size(&format_legacy_comment_entry(self))
        } else {
            serde_json::to_vec(&self.wire()).map_or(usize::MAX, |b| b.len() + 1)
        }
    }

    /// Go's `CoalescedCommentData` at the moment `buildCoalescedCommentData`
    /// builds it: `edited_at`/`revision` are zero and therefore omitted. The
    /// builder adds them later for comments edited since the resumed run, so
    /// the delivery budget measures the pre-edit shape exactly like Go does.
    pub(super) fn wire(&self) -> Value {
        let mut out = serde_json::Map::new();
        out.insert("id".into(), json!(self.id));
        if !self.thread_id.is_nil() {
            out.insert("thread_id".into(), json!(self.thread_id));
        }
        for (key, value) in [
            ("author_type", self.author_type.clone()),
            ("author_name", self.author_name.clone()),
        ] {
            if !value.is_empty() {
                out.insert(key.into(), json!(value));
            }
        }
        out.insert("content".into(), json!(self.content));
        if !self.created_at.is_empty() {
            out.insert("created_at".into(), json!(self.created_at));
        }
        Value::Object(out)
    }

    /// The same entry after the builder stamps an edit: Go assigns
    /// `EditedAt`/`Revision` onto the struct post-construction, and both carry
    /// `omitempty`, so an unedited comment serializes byte-identically to
    /// [`Self::wire`].
    pub(super) fn wire_edited(&self, edited_at: &str) -> Value {
        let mut value = self.wire();
        if let Value::Object(map) = &mut value {
            map.insert("edited_at".into(), json!(edited_at));
            if self.revision != 0 {
                map.insert("revision".into(), json!(self.revision));
            }
        }
        value
    }
}

/// `len(json.Marshal(v)) - 2`: the bytes an escaped JSON string adds inside a
/// larger document. `max + 1` on an encode error, matching Go's fail-closed
/// over-estimate.
fn escaped_json_string_content_size(value: &str) -> usize {
    serde_json::to_string(value).map_or(usize::MAX, |encoded| encoded.len().saturating_sub(2))
}

pub(super) const LEGACY_COMMENT_BUNDLE_HEADER: &str = "This run covers multiple distinct issue comments. Address every comment below in chronological order; do not treat this bundle as one rewritten comment.\n";

/// Go's `formatLegacyCommentEntry`: every optional segment is conditional, so a
/// comment with no thread, author, or creation stamp yields a shorter entry.
pub(super) fn format_legacy_comment_entry(comment: &ClaimComment) -> String {
    let mut out = format!("\n--- comment {}", comment.id);
    if !comment.thread_id.is_nil() {
        out.push_str(&format!(" [thread {}]", comment.thread_id));
    }
    if !comment.author_type.is_empty() || !comment.author_name.is_empty() {
        out.push_str(&format!(" [author {}", comment.author_type));
        if !comment.author_name.is_empty() {
            out.push_str(&format!(": {}", comment.author_name));
        }
        out.push(']');
    }
    if !comment.created_at.is_empty() {
        out.push_str(&format!(" [created {}]", comment.created_at));
    }
    out.push_str(" ---\n");
    out.push_str(&comment.content);
    out.push_str(&format!("\n--- end comment {} ---\n", comment.id));
    out
}

pub(super) fn format_legacy_comment_bundle(comments: &[ClaimComment]) -> String {
    let mut out = String::from(LEGACY_COMMENT_BUNDLE_HEADER);
    for comment in comments {
        out.push_str(&format_legacy_comment_entry(comment));
    }
    out.trim().to_owned()
}

/// The deterministic comment-delivery budget.
///
/// `trigger_id` names the mandatory comment — the one this task was queued for —
/// or, when the trigger row is gone, the newest loaded comment. Everything else
/// is admitted oldest-first while it fits under `limit`; `limit == 0` means no
/// budget. The returned ids preserve the INPUT order, and an over-budget comment
/// stops admission rather than being skipped, so the daemon never sees a
/// chronological gap.
pub(super) fn select_comment_delivery(
    comments: &[ClaimComment],
    trigger_id: Option<uuid::Uuid>,
    legacy: bool,
    limit: usize,
) -> Vec<&ClaimComment> {
    if comments.is_empty() {
        return Vec::new();
    }
    let mandatory = trigger_id
        .and_then(|id| comments.iter().find(|comment| comment.id == id))
        .unwrap_or_else(|| &comments[comments.len() - 1]);

    let base = if legacy {
        escaped_json_string_content_size(LEGACY_COMMENT_BUNDLE_HEADER)
    } else {
        2
    };
    let mut used = base.saturating_add(mandatory.entry_size(legacy));
    let mut admitted: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
    admitted.insert(mandatory.id);
    for comment in comments {
        if comment.id == mandatory.id {
            continue;
        }
        let cost = comment.entry_size(legacy);
        // An over-budget entry stops admission rather than being skipped: the
        // daemon must never receive a chronological gap.
        if limit > 0 && used.saturating_add(cost) > limit {
            break;
        }
        used = used.saturating_add(cost);
        admitted.insert(comment.id);
    }
    comments
        .iter()
        .filter(|comment| admitted.contains(&comment.id))
        .collect()
}

/// RFC3339 with nanosecond precision, matching Go's `time.RFC3339Nano` for a
/// generation stamp. Second-resolution display timestamps cannot distinguish
/// claims reclaimed inside the same second.
#[allow(dead_code)] // Generation stamps move to nanosecond precision with the caller.
pub(super) fn rfc3339_nano(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// `OffsetDateTime`-free second-precision stamp used by the edit notices, which
/// only need to tell the agent that a comment moved.
pub(super) fn rfc3339_secs(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn at(raw: &str) -> Option<DateTime<Utc>> {
        raw.parse().ok()
    }

    #[test]
    fn snapshot_round_trip_and_version_gate() {
        let snap = build_issue_state_snapshot("Fix it", "Body text");
        assert_eq!(snap.v, 1);
        assert_eq!(snap.title_sha256.len(), 64);
        let encoded = serde_json::to_vec(&snap).unwrap();
        assert_eq!(
            decode_issue_state_snapshot(Some(&encoded)),
            Some(snap.clone())
        );

        // Absent, empty, malformed and wrong-version all read as "not compared".
        assert_eq!(decode_issue_state_snapshot(None), None);
        assert_eq!(decode_issue_state_snapshot(Some(b"")), None);
        assert_eq!(decode_issue_state_snapshot(Some(b"{")), None);
        assert_eq!(
            decode_issue_state_snapshot(Some(br#"{"v":2,"title_sha256":"x"}"#)),
            None
        );
    }

    #[test]
    fn changed_fields_report_only_title_and_description() {
        let base = build_issue_state_snapshot("a", "b");
        assert!(changed_fields_since(&base, &base).is_empty());
        let title = build_issue_state_snapshot("A", "b");
        assert_eq!(changed_fields_since(&title, &base), vec!["title"]);
        let both = build_issue_state_snapshot("A", "B");
        assert_eq!(
            changed_fields_since(&both, &base),
            vec!["title", "description"]
        );
    }

    #[test]
    fn resumed_anchor_requires_completed_run() {
        let started = at("2026-01-02T03:04:05Z").unwrap();
        assert!(new_resumed_run_anchor("completed", Some(started), None).is_some());
        assert!(new_resumed_run_anchor("failed", Some(started), None).is_none());
        assert!(new_resumed_run_anchor("cancelled", Some(started), None).is_none());
        assert!(new_resumed_run_anchor("completed", None, None).is_none());
    }

    #[test]
    fn comment_edit_notice_rules() {
        let run = at("2026-01-02T00:00:00Z");
        let edited = CommentEditFacts {
            created_at: at("2026-01-01T00:00:00Z"),
            updated_at: at("2026-01-03T00:00:00Z"),
            revision: 3,
            resolved_at: None,
        };
        assert!(trigger_comment_edited_since(&edited, run));

        // Created after the run: new, not edited.
        let later = CommentEditFacts {
            created_at: at("2026-01-04T00:00:00Z"),
            updated_at: at("2026-01-05T00:00:00Z"),
            revision: 4,
            resolved_at: None,
        };
        assert!(!trigger_comment_edited_since(&later, run));

        // Revision 1 has never been edited.
        let never = CommentEditFacts {
            revision: 1,
            ..edited.clone()
        };
        assert!(!trigger_comment_edited_since(&never, run));

        // Revision 2 whose only change was a resolve is not a text edit.
        let resolved = CommentEditFacts {
            revision: 2,
            resolved_at: at("2026-01-03T00:00:00Z"),
            ..edited.clone()
        };
        assert!(!trigger_comment_edited_since(&resolved, run));

        // No resumed run means nothing to compare against.
        assert!(!trigger_comment_edited_since(&edited, None));
    }

    #[test]
    fn resume_poison_set_and_text_guards() {
        for reason in RESUME_UNSAFE_REASONS {
            assert!(resume_unsafe_failure(reason, ""));
        }
        assert!(!resume_unsafe_failure("agent_error", "boom"));
        assert!(resume_unsafe_failure(
            "agent_error",
            "got 400 invalid_request_error"
        ));
        assert!(resume_unsafe_failure(
            "agent_error",
            "Could not resolve authentication method"
        ));
        assert!(resume_unsafe_failure(
            "agent_error",
            "invalid conversation history received"
        ));
        assert!(resume_unsafe_failure(
            "agent_error",
            "messages.3: content must not be empty"
        ));
        assert!(resume_unsafe_failure(
            "agent_error",
            "role:assistant message must not be empty"
        ));
        // Content complaint alone is not enough.
        assert!(!resume_unsafe_failure(
            "agent_error",
            "content must not be empty"
        ));
        // Locator alone is not enough.
        assert!(!resume_unsafe_failure(
            "agent_error",
            "assistant message is odd"
        ));
    }

    #[test]
    fn worktree_gate_needs_capability_and_matching_daemon() {
        let resource = json!({
            "resource_type": "local_directory",
            "resource_ref": {"local_path": "/home/u/repo", "execution_mode": "worktree", "daemon_id": "d1"}
        });
        assert!(
            worktree_claim_block_reason(std::slice::from_ref(&resource), None, false).is_none()
        );
        assert!(
            worktree_claim_block_reason(std::slice::from_ref(&resource), Some(""), false).is_none()
        );
        assert!(
            worktree_claim_block_reason(std::slice::from_ref(&resource), Some("d1"), true)
                .is_none()
        );
        assert!(
            worktree_claim_block_reason(std::slice::from_ref(&resource), Some("other"), false)
                .is_none()
        );
        let reason = worktree_claim_block_reason(&[resource], Some("d1"), false).unwrap();
        assert!(reason.contains("/home/u/repo"));
        assert!(reason.contains("worktree"));

        let in_place = json!({
            "resource_type": "local_directory",
            "resource_ref": {"local_path": "/x", "execution_mode": "in_place", "daemon_id": "d1"}
        });
        assert!(worktree_claim_block_reason(&[in_place], Some("d1"), false).is_none());
    }

    #[test]
    fn openclaw_override_and_disabled_skills() {
        assert_eq!(openclaw_model_override(None), "");
        assert_eq!(
            openclaw_model_override(Some(&json!({"model_override":"  "}))),
            ""
        );
        assert_eq!(
            openclaw_model_override(Some(&json!({"model_override":" gpt-5 "}))),
            "gpt-5"
        );
        let runtime = Uuid::new_v4();
        let other = Uuid::new_v4();
        let raw = json!([
            {"runtime_id": runtime, "provider": "openclaw", "root": "/r", "key": "k"},
            {"runtime_id": other, "provider": "openclaw", "root": "/r", "key": "k"},
            {"runtime_id": runtime, "provider": "codex", "root": "/r", "key": "k"},
        ]);
        let filtered = disabled_runtime_skills_for(Some(&raw), runtime, "openclaw");
        assert_eq!(filtered.as_array().unwrap().len(), 1);
        assert_eq!(filtered[0]["runtime_id"], json!(runtime));
        // Malformed or absent stores degrade to an empty list, never an error.
        assert_eq!(
            disabled_runtime_skills_for(None, runtime, "openclaw"),
            json!([])
        );
        assert_eq!(
            disabled_runtime_skills_for(Some(&json!("nope")), runtime, "openclaw"),
            json!([])
        );
    }

    fn comment(id: Uuid, content: &str, created: &str) -> ClaimComment {
        ClaimComment {
            id,
            thread_id: id,
            content: content.into(),
            author_type: "member".into(),
            author_name: "Ada".into(),
            created_at: created.into(),
            created_at_value: at(created),
            revision: 1,
            updated_at: at(created),
            resolved_at: None,
        }
    }

    #[test]
    fn legacy_entry_format_is_pinned() {
        let id = Uuid::parse_str("0190a000-0000-7000-8000-000000000001").unwrap();
        let entry = format_legacy_comment_entry(&comment(id, "hello", "2026-01-02T03:04:05Z"));
        assert_eq!(
            entry,
            "\n--- comment 0190a000-0000-7000-8000-000000000001 [thread 0190a000-0000-7000-8000-000000000001] [author member: Ada] [created 2026-01-02T03:04:05Z] ---\nhello\n--- end comment 0190a000-0000-7000-8000-000000000001 ---\n"
        );
        let bundle = format_legacy_comment_bundle(&[comment(id, "  hi  ", "2026-01-02T03:04:05Z")]);
        assert!(bundle.starts_with(LEGACY_COMMENT_BUNDLE_HEADER.trim_end()));
        assert!(bundle.ends_with("--- end comment 0190a000-0000-7000-8000-000000000001 ---"));

        // An unnamed author omits the colon.
        let mut anonymous = comment(id, "x", "2026-01-02T03:04:05Z");
        anonymous.author_name.clear();
        assert!(format_legacy_comment_entry(&anonymous).contains("[author member]"));

        // Every optional segment is conditional in Go, so a bare comment keeps
        // only the framing.
        let mut bare = comment(id, "x", "");
        bare.thread_id = Uuid::nil();
        bare.author_type.clear();
        bare.author_name.clear();
        assert_eq!(
            format_legacy_comment_entry(&bare),
            "\n--- comment 0190a000-0000-7000-8000-000000000001 ---\nx\n--- end comment 0190a000-0000-7000-8000-000000000001 ---\n"
        );
    }

    #[test]
    fn coalesced_wire_matches_go_omitempty() {
        let id = Uuid::parse_str("0190a000-0000-7000-8000-000000000001").unwrap();
        let full = comment(id, "hello", "2026-01-02T03:04:05Z");
        assert_eq!(
            full.wire(),
            json!({
                "id": id,
                "thread_id": id,
                "author_type": "member",
                "author_name": "Ada",
                "content": "hello",
                "created_at": "2026-01-02T03:04:05Z",
            })
        );
        // An unedited comment carries no edit stamp; the builder adds one only
        // for comments edited since the resumed run.
        assert!(full.wire().get("edited_at").is_none());
        assert!(full.wire().get("revision").is_none());

        let mut bare = comment(id, "", "");
        bare.thread_id = Uuid::nil();
        bare.author_type.clear();
        bare.author_name.clear();
        assert_eq!(bare.wire(), json!({ "id": id, "content": "" }));

        let edited = full.wire_edited("2026-01-03T00:00:00Z");
        assert_eq!(edited["edited_at"], "2026-01-03T00:00:00Z");
        assert_eq!(edited["revision"], 1);
        assert_eq!(edited["content"], "hello");
    }

    #[test]
    fn delivery_budget_keeps_trigger_and_input_order() {
        let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        let list = vec![
            comment(ids[0], "zero", "2026-01-01T00:00:00Z"),
            comment(ids[1], "one", "2026-01-01T00:00:01Z"),
            comment(ids[2], "two", "2026-01-01T00:00:02Z"),
            comment(ids[3], "three", "2026-01-01T00:00:03Z"),
        ];
        // No budget admits everything, in input order.
        let all = select_comment_delivery(&list, Some(ids[2]), false, 0);
        assert_eq!(all.iter().map(|c| c.id).collect::<Vec<_>>(), ids);
        // A missing trigger falls back to the newest loaded comment.
        let none = select_comment_delivery(&list, None, false, 0);
        assert!(none.iter().any(|c| c.id == ids[3]));
        assert!(!select_comment_delivery(&list, None, false, 0).is_empty());
        // Empty input is empty output, not a panic.
        assert!(select_comment_delivery(&[], None, false, 10).is_empty());
    }

    #[test]
    fn generation_and_edit_stamps_match_go_precision() {
        let value: DateTime<Utc> = "2026-01-02T03:04:05.123456789Z".parse().unwrap();
        assert_eq!(rfc3339_nano(value), "2026-01-02T03:04:05.123456789Z");
        assert_eq!(rfc3339_secs(value), "2026-01-02T03:04:05Z");
        let whole: DateTime<Utc> = "2026-01-02T03:04:05Z".parse().unwrap();
        assert_eq!(rfc3339_nano(whole), "2026-01-02T03:04:05.000000000Z");
    }

    #[test]
    fn fresh_rerun_and_uuid_lists() {
        assert!(!is_fresh_rerun(None));
        assert!(!is_fresh_rerun(Some(&json!({}))));
        assert!(is_fresh_rerun(Some(&json!({"fresh_rerun": true}))));
        assert_eq!(uuid_strings_or_empty(&[]), json!([]));
        let id = Uuid::new_v4();
        assert_eq!(uuid_strings_or_empty(&[id]), json!([id]));
    }
}
