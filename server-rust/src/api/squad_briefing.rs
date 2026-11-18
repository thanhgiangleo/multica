//! The system briefing a squad leader reads on every claim. Ported from Go's
//! `squad_briefing.go` so the protocol text lives in one place.
//!
//! The protocol is appended to the leader agent's instructions; it is not a
//! replacement for them. Responsibility 6 varies with the leader's authority
//! over the issue and with whether the issue follows a project workflow.
use tokio_postgres::GenericClient;
use uuid::Uuid;

const SQUAD_OPERATING_PROTOCOL_HEADER: &str = r#"## Squad Operating Protocol

**If you are reading this section, you have been activated as a squad LEADER
for this task — regardless of how the work reached you (direct assignment,
an @squad mention in a comment, quick-create, or autopilot).** Your job is to
**coordinate**, NOT to do the work yourself. Even if the task reads like a
direct request to "do X" (review this PR, fix this bug, write this code), you
must delegate X to the right squad member by @mention — doing it yourself
defeats the entire purpose of the squad and is a protocol violation.

Your responsibilities, in order:

1. **Read the issue** (title, description, latest comments, acceptance
   criteria) and decide which squad member is best suited to do the work.
   Match the task to each member's listed **skills** and role in the Squad
   Roster below — prefer the member whose skills cover the work.
2. **Delegate by @mention.** Post a single comment on this issue that
   @mentions the chosen member(s) and tells them what to do.
   - **Be terse.** Every Multica agent already has full context of the
     issue (title, description, all prior comments, attachments) and
     the surrounding workspace. Do NOT restate or summarise the
     issue body, prior discussion, or known facts in your delegation
     comment — they read it themselves.
   - Say only what cannot be inferred from the issue: who you're
     picking, why them (one short clause), and any *additional*
     constraints, hints, or sequencing you want them to follow.
     Two or three sentences is usually plenty.
   - Use the exact mention markdown shown in the Squad Roster below —
     typing a plain "@name" will not trigger anyone.
3. **Record your evaluation.** After every trigger — whether you delegated,
   decided no action is needed, or encountered an error — record it:
   `multica squad activity <issue-id> <outcome> --reason "<short reason>"`
   Outcome values: `action` (you delegated or acted),
   `no_action` (you evaluated and decided nothing is needed),
   `failed` (you hit an error).
   This is mandatory on every turn — it records your decision in the
   issue timeline so humans can see you evaluated the trigger.
   Record it against the issue THIS turn is running on (the issue id in
   your task context). It does not need to be assigned to your squad.
   If the call fails, make sure the turn still leaves a record, without
   breaking the one-comment-per-turn rule: post a short comment with the
   outcome and the error ONLY if you have not already commented this turn
   (the no_action case). If you already posted a delegation comment, that
   comment is the record — do not add a second one.
4. **Stop after dispatching.** Once your delegation comment is posted
   and evaluation recorded, end your turn. Do not continue working,
   do not write code, do not open files. You will be re-triggered
   automatically when:
   - a delegated member posts an update or asks you a question;
   - a delegated member finishes and the issue moves forward;
   - someone @mentions you again on this issue.
5. **Re-evaluate on each trigger.** When you wake up again, read the new
   activity and decide whether to delegate the next step, escalate to
   the human reporter, or close the loop. If no action is needed
   (e.g. a member posted a progress update that requires no response),
   record `no_action` and exit silently. Exiting silently
   means posting NO comment at all — not one announcing no_action, not
   one acknowledging another agent, not one saying you are exiting. The
   `squad activity` call IS the record; a comment on top of
   it is noise. That prohibition holds only while the call succeeds — if
   it errors, responsibility 3 applies and the turn leaves one short
   comment instead."#;

const SQUAD_PARENT_STATUS_OWNED: &str = r#"6. **Own the parent issue status.** This issue is assigned to your squad,
   so its status is yours to manage (unless Agent Identity forbids status
   changes). On the first assignment turn, move the parent to
   `in_progress` and keep it there while members work — a successful
   dispatch is not completion. On later turns, do not flip status for
   routine progress updates. When you confirm the overall goal is met, run
   `multica issue status <issue-id> in_review` — this responsibility is
   itself the standing instruction that authorizes that change, so do it even
   when no comment asked you to. Leave `done` to a human reviewer or
   existing integrations (for example a PR with close intent that merges)."#;

const SQUAD_PARENT_STATUS_OWNED_IN_WORKFLOW: &str = r#"6. **Own the parent issue status — through its project workflow.** This issue
   is assigned to your squad and follows its project workflow, so its status
   is yours to move (unless Agent Identity forbids status changes), using only
   the commands in `[Project workflow]`. The parent stays at its
   current step while members work — a successful dispatch is not completion.
   On later turns, do not move it for routine progress updates. When you
   confirm the overall goal is met, run that block's Step done command — this
   responsibility is itself the standing instruction that authorizes that
   change, so do it even when no comment asked you to."#;

const SQUAD_PARENT_STATUS_WORKFLOW_OVERRIDES_RUNTIME: &str = r#"
   This overrides the in_progress / in_review rules in your runtime
   instructions: never set either on this issue just to mark progress or
   delivery."#;

const SQUAD_PARENT_STATUS_NOT_OWNED: &str = r#"6. **Do NOT change this issue's status.** This issue is not assigned to your
   squad — you were pulled in by an @mention (or this is a quick-create turn,
   where the issue does not exist yet). Its status belongs to its own
   assignee. Answer, delegate, or escalate as usual, but never run
   `multica issue status` on it, no matter how complete the work looks
   to you."#;

const SQUAD_OPERATING_PROTOCOL_HARD_RULES: &str = r#"Hard rules:
- EVERY delegation MUST use the full mention markdown syntax
  `[@Name](mention://<type>/<UUID>)` exactly as shown in the Squad
  Roster. A plain "@name" or bare name does NOT trigger the agent —
  if you skip the mention link, the task is never delivered and the
  issue stalls. This is non-negotiable: no mention link = no delegation.
- Do NOT restate the issue body or prior comments in your delegation —
  the assignee already has them. Repeating context is noise that
  buries the actual instruction.
- Do NOT do the implementation work yourself unless the squad has no
  other suitable members. The squad exists so work is split — bypassing
  it defeats the point.
- Do NOT @mention members who don't appear in the Squad Roster below;
  they are not part of this squad.
- One delegation comment per turn is enough. Avoid spamming multiple
  near-identical comments.
- If the squad has no member capable of the task, post a comment
  explaining the gap (and @mention the issue's reporter if possible)
  rather than silently doing the work.
- ALWAYS call `multica squad activity` before ending your turn —
  even when the outcome is no_action. If it errors on a turn where you
  posted no comment, leave one short comment instead; never let an
  evaluation end with no record at all, and never post a second comment
  just to report the error.
- A child issue you create with `--status todo` and an agent assignee
  already fires that agent automatically — the assignment IS the trigger.
  If you also @mention the same agent on this parent issue for the same
  work, the agent runs twice in parallel (once from the mention, once
  from the assignment). Pick exactly one path: either delegate by
  @mention on this issue, or create a `todo` child issue assigned to
  them. Never both for the same work."#;

const SQUAD_STEER_RULE: &str = r#"- A worker already running on this issue gets a new comment only after its
  run ends. To correct it mid-run (wrong direction, a missing fact, a
  narrower scope), steer that run instead: add the comment with
  `--steer <run-id>` (run ids from `multica issue runs <issue-id> --active`), or
  `--steer-active` for every running worker you may steer. Only a run that
  accepts additional messages (`can_supplement: true` in
  `multica issue runs <issue-id> --active --output json`) can be steered, and
  the command reports per run whether it was. New work is still a normal
  delegation."#;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::api) enum LeaderWorkflow {
    None,
    /// The runtime brief points at the project workflow and states no
    /// built-in moves.
    CurrentDaemon,
    /// The runtime brief still says to set in_progress first and in_review on
    /// delivery, so responsibility 6 has to rule those moves out.
    LegacyDaemon,
}

/// The protocol, selecting the parent-status responsibility that matches this
/// leader's authority over the issue.
pub(super) fn operating_protocol_for(owns_issue_status: bool, workflow: LeaderWorkflow) -> String {
    let status = match (owns_issue_status, workflow) {
        (true, LeaderWorkflow::CurrentDaemon) => SQUAD_PARENT_STATUS_OWNED_IN_WORKFLOW.to_string(),
        (true, LeaderWorkflow::LegacyDaemon) => format!(
            "{SQUAD_PARENT_STATUS_OWNED_IN_WORKFLOW}{SQUAD_PARENT_STATUS_WORKFLOW_OVERRIDES_RUNTIME}"
        ),
        (true, LeaderWorkflow::None) => SQUAD_PARENT_STATUS_OWNED.to_string(),
        _ => SQUAD_PARENT_STATUS_NOT_OWNED.to_string(),
    };
    format!("{SQUAD_OPERATING_PROTOCOL_HEADER}\n{status}\n\n{SQUAD_OPERATING_PROTOCOL_HARD_RULES}")
}

pub(in crate::api) struct Squad {
    pub id: Uuid,
    pub name: String,
    pub leader_id: Uuid,
    pub instructions: String,
}

/// The full system briefing appended to a squad leader's instructions:
/// protocol, roster with pasteable mention markdown, and the squad's own
/// instructions when it has any.
///
/// `owns_issue_status` must be true only when the issue this task is bound to
/// is assigned to this very squad: the briefing is injected on guest paths too,
/// and the flag is what keeps status authority from leaking along with the
/// roster.
pub(in crate::api) async fn build_leader_briefing(
    db: &impl GenericClient,
    squad: &Squad,
    owns_issue_status: bool,
    workflow: LeaderWorkflow,
    steer: bool,
) -> String {
    let mut out = operating_protocol_for(owns_issue_status, workflow);
    if steer {
        out.push('\n');
        out.push_str(SQUAD_STEER_RULE);
    }
    out.push_str("\n\n");
    out.push_str(&build_roster(db, squad).await);
    let instructions = squad.instructions.trim();
    if !instructions.is_empty() {
        out.push_str(&format!(
            "\n\n## Squad Instructions ({})\n\n{instructions}",
            squad.name
        ));
    }
    out
}

fn mention(name: &str, kind: &str, id: &str) -> String {
    format!("[@{name}](mention://{kind}/{id})")
}

fn roster_row(name: &str, kind: &str, role: &str, skills: &str, mention: &str) -> String {
    let mut row = format!("- {name} — {kind}");
    if !role.is_empty() {
        row.push_str(&format!(", role: \"{role}\""));
    }
    if !skills.is_empty() {
        row.push_str(&format!(" — {skills}"));
    }
    row.push_str(&format!(" — `{mention}`\n"));
    row
}

async fn build_roster(db: &impl GenericClient, squad: &Squad) -> String {
    let leader_id = squad.leader_id.to_string();
    let leader_name: String = db
        .query_opt("SELECT name FROM agent WHERE id=$1", &[&squad.leader_id])
        .await
        .ok()
        .flatten()
        .map(|row| row.get(0))
        .unwrap_or_else(|| "Leader".into());
    let mut out = String::from("## Squad Roster\n\n");
    out.push_str("Leader (you):\n");
    out.push_str(&format!(
        "- {leader_name} — agent — `{}`\n",
        mention(&leader_name, "agent", &leader_id)
    ));

    let members = db
        .query(
            "SELECT member_type,member_id,role FROM squad_member WHERE squad_id=$1 ORDER BY created_at ASC",
            &[&squad.id],
        )
        .await
        .unwrap_or_default();
    let skill_names = member_skill_names(db, &members, &leader_id).await;

    let mut rows = Vec::with_capacity(members.len());
    for member in &members {
        let member_type: String = member.get("member_type");
        let member_id: Uuid = member.get("member_id");
        // The leader may also be in the member list; it is shown above and
        // must not delegate to itself.
        if member_type == "agent" && member_id.to_string() == leader_id {
            continue;
        }
        let role: String = member.get::<_, Option<String>>("role").unwrap_or_default();
        let role = role.trim();
        let id = member_id.to_string();
        let row = match member_type.as_str() {
            "agent" => match db
                .query_opt(
                    "SELECT name,archived_at FROM agent WHERE id=$1",
                    &[&member_id],
                )
                .await
            {
                Ok(Some(agent))
                    if agent
                        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
                        .is_none() =>
                {
                    let name: String = agent.get("name");
                    roster_row(
                        &name,
                        "agent",
                        role,
                        &skill_segment(&skill_names, &id),
                        &mention(&name, "agent", &id),
                    )
                }
                // An archived agent or a failed read has nobody to delegate to.
                _ => String::new(),
            },
            // Humans are mentioned by their user id, as everywhere else in
            // the product, and carry no Multica skills.
            "member" => match db
                .query_opt("SELECT name FROM \"user\" WHERE id=$1", &[&member_id])
                .await
            {
                Ok(Some(user)) => {
                    let name: String = user.get("name");
                    roster_row(
                        &name,
                        "member (human)",
                        role,
                        "",
                        &mention(&name, "member", &id),
                    )
                }
                _ => String::new(),
            },
            _ => String::new(),
        };
        if !row.is_empty() {
            rows.push(row);
        }
    }

    if rows.is_empty() {
        out.push_str("\nMembers: (none — you are the only member of this squad)\n");
        return out;
    }
    out.push_str("\nMembers:\n");
    for row in rows {
        out.push_str(&row);
    }
    out
}

/// Skill display names per agent member, or `None` when the lookup failed: a
/// transient error degrades to the plain name+role row rather than asserting
/// a misleading "no skills".
async fn member_skill_names(
    db: &impl GenericClient,
    members: &[tokio_postgres::Row],
    leader_id: &str,
) -> Option<std::collections::HashMap<String, Vec<String>>> {
    let mut ids: Vec<Uuid> = Vec::new();
    for member in members {
        let member_type: String = member.get("member_type");
        let member_id: Uuid = member.get("member_id");
        if member_type != "agent" || member_id.to_string() == leader_id || ids.contains(&member_id)
        {
            continue;
        }
        ids.push(member_id);
    }
    if ids.is_empty() {
        return Some(std::collections::HashMap::new());
    }
    let rows = db
        .query(
            "SELECT ask.agent_id, s.name FROM agent_skill ask JOIN skill s ON s.id = ask.skill_id WHERE ask.agent_id = ANY($1::uuid[]) AND ask.enabled = TRUE ORDER BY ask.agent_id, s.name ASC",
            &[&ids],
        )
        .await
        .ok()?;
    let mut by_agent: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in rows {
        let agent_id: Uuid = row.get("agent_id");
        let name: String = row.get("name");
        by_agent.entry(agent_id.to_string()).or_default().push(name);
    }
    Some(by_agent)
}

fn skill_segment(
    names: &Option<std::collections::HashMap<String, Vec<String>>>,
    agent_id: &str,
) -> String {
    let Some(names) = names else {
        return String::new();
    };
    match names.get(agent_id) {
        Some(list) if !list.is_empty() => format!("skills: {}", list.join(", ")),
        _ => "no skills assigned".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owning_leader_moves_the_parent_through_its_workflow() {
        let current = operating_protocol_for(true, LeaderWorkflow::CurrentDaemon);
        assert!(current.contains("Own the parent issue status — through its project workflow."));
        assert!(!current.contains("replaces the in_progress / in_review rules"));
        let legacy = operating_protocol_for(true, LeaderWorkflow::LegacyDaemon);
        assert!(legacy.contains("never set either on this issue just to mark progress"));
        let plain = operating_protocol_for(true, LeaderWorkflow::None);
        assert!(plain.contains("multica issue status <issue-id> in_review"));
    }

    #[test]
    fn guest_leader_must_not_touch_the_status() {
        let guest = operating_protocol_for(false, LeaderWorkflow::CurrentDaemon);
        assert!(guest.contains("**Do NOT change this issue's status.**"));
        assert!(!guest.contains("Own the parent issue status"));
        // The protocol always states the mention syntax and the activity call.
        assert!(guest.starts_with("## Squad Operating Protocol"));
        assert!(guest.contains("multica squad activity <issue-id> <outcome>"));
        assert!(guest.ends_with("Never both for the same work."));
    }

    #[test]
    fn roster_rows_carry_pasteable_mentions() {
        let row = roster_row(
            "Lin",
            "agent",
            "reviewer",
            "skills: a, b",
            "[@Lin](mention://agent/x)",
        );
        assert_eq!(
            row,
            "- Lin — agent, role: \"reviewer\" — skills: a, b — `[@Lin](mention://agent/x)`\n"
        );
        assert_eq!(
            roster_row("Sam", "member (human)", "", "", "m"),
            "- Sam — member (human) — `m`\n"
        );
    }

    #[test]
    fn steer_rule_names_the_flag() {
        assert!(SQUAD_STEER_RULE.contains("`--steer <run-id>`"));
        assert!(SQUAD_STEER_RULE.contains("`can_supplement: true`"));
    }
}
