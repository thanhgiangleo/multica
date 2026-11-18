//! Project workflows (MUL-7420): resolution, handoff resolution and the agent
//! brief. Ported from Go's `internal/issueworkflow` so the claim payload and the
//! workflow editor render the same text from the same rules.
//!
//! A workflow selects and orders statuses from the workspace catalog and says
//! who each column hands an issue to. `todo`, `done` and `cancelled` are
//! required; `backlog` and `blocked` are optional; only positions are fixed.
use super::workflow::{Handler, Step, decode};
use deadpool_postgres::GenericClient;
use uuid::Uuid;

pub(super) const HANDLER_NONE: &str = "none";
pub(super) const HANDLER_AGENT: &str = "agent";
pub(super) const HANDLER_SQUAD: &str = "squad";
pub(super) const HANDLER_MEMBER: &str = "member";
pub(super) const HANDLER_PROJECT_LEAD: &str = "project_lead";
pub(super) const HANDLER_CREATOR: &str = "creator";

pub(super) const DEFAULT_NAME: &str = "Default";
/// Where a new issue lands unless its creator picks a status.
pub(super) const START_STATUS: &str = "todo";

const FIXED_HEAD: [&str; 2] = ["backlog", "todo"];
const FIXED_TAIL: [&str; 3] = ["blocked", "done", "cancelled"];

/// Every workflow must list the status: new issues start on todo, and done and
/// cancelled are how work ends.
pub(super) fn is_required(key: &str) -> bool {
    matches!(key, "todo" | "done" | "cancelled")
}

/// The status has a fixed place when listed.
pub(super) fn is_fixed(key: &str) -> bool {
    FIXED_HEAD.contains(&key) || FIXED_TAIL.contains(&key)
}

/// Entering the status can hand an issue off. Parking, finishing and cancelling
/// never reassign.
pub(super) fn accepts_handoff(key: &str) -> bool {
    !matches!(key, "backlog" | "done" | "cancelled")
}

/// Puts steps in board order: fixed statuses it lists in their places around
/// its own steps, with the required ones added when missing. A repeated fixed
/// status keeps its first step.
pub(super) fn arrange(steps: Vec<Step>) -> Vec<Step> {
    let mut fixed: Vec<(String, Step)> = Vec::new();
    let mut middle: Vec<Step> = Vec::new();
    let capacity = steps.len() + FIXED_HEAD.len() + FIXED_TAIL.len();
    for step in steps {
        if !is_fixed(&step.status_key) {
            middle.push(step);
        } else if !fixed.iter().any(|(key, _)| *key == step.status_key) {
            fixed.push((step.status_key.clone(), step));
        }
    }
    let place = |out: &mut Vec<Step>, keys: &[&str]| {
        for key in keys {
            if let Some((_, step)) = fixed.iter().find(|(fixed, _)| fixed == key) {
                out.push(step.clone());
            } else if is_required(key) {
                out.push(none_step(key));
            }
        }
    };
    let mut out = Vec::with_capacity(capacity);
    place(&mut out, &FIXED_HEAD);
    out.append(&mut middle);
    place(&mut out, &FIXED_TAIL);
    out
}

fn none_step(key: &str) -> Step {
    Step {
        status_key: key.into(),
        handler: Handler {
            r#type: HANDLER_NONE.into(),
            id: String::new(),
        },
        instructions: String::new(),
        next_status_key: String::new(),
        back_status_key: String::new(),
    }
}

pub(super) fn steps_for(keys: &[String]) -> Vec<Step> {
    keys.iter().map(|key| none_step(key)).collect()
}

pub(super) fn keys_of(steps: &[Step]) -> Vec<String> {
    steps.iter().map(|step| step.status_key.clone()).collect()
}

pub(super) fn arrange_keys(keys: &[String]) -> Vec<String> {
    keys_of(&arrange(steps_for(keys)))
}

/// Default before a workspace stores its own.
pub(super) fn built_in_default_keys() -> Vec<String> {
    arrange_keys(&[
        "backlog".to_string(),
        "in_progress".into(),
        "in_review".into(),
        "blocked".into(),
    ])
}

impl Step {
    /// Entering the step reassigns the issue.
    pub(super) fn hands_off(&self) -> bool {
        matches!(
            self.handler.r#type.as_str(),
            HANDLER_AGENT | HANDLER_SQUAD | HANDLER_MEMBER | HANDLER_PROJECT_LEAD | HANDLER_CREATOR
        )
    }
}

pub(super) struct Definition {
    pub name: String,
    pub is_default: bool,
    pub steps: Vec<Step>,
}

impl Definition {
    pub fn has(&self, key: &str) -> bool {
        self.step(key).is_some()
    }

    pub fn step(&self, key: &str) -> Option<&Step> {
        self.steps.iter().find(|step| step.status_key == key)
    }

    pub fn status_keys(&self) -> Vec<String> {
        keys_of(&self.steps)
    }

    pub fn hands_off_anywhere(&self) -> bool {
        self.steps.iter().any(Step::hands_off)
    }

    /// Where a step's handler moves the issue when done: the step's own choice,
    /// or else the next status on the board that is not blocked.
    pub fn next_status(&self, key: &str) -> Option<String> {
        let step = self.step(key)?;
        if !step.next_status_key.is_empty() {
            return Some(step.next_status_key.clone());
        }
        let index = self.steps.iter().position(|step| step.status_key == key)?;
        self.steps[index + 1..]
            .iter()
            .find(|next| next.status_key != "blocked")
            .map(|next| next.status_key.clone())
    }

    /// The stretch of work an assignee owns from a step that has no handler:
    /// entering such steps reassigns nothing and starts no run, so the same
    /// assignee carries the issue until a step that hands off.
    fn handlerless_span(&self, key: &str) -> HandlerlessSpan {
        let Some(current) = self.step(key) else {
            return HandlerlessSpan::default();
        };
        if current.hands_off() || matches!(key, "blocked" | "done" | "cancelled") {
            return HandlerlessSpan::default();
        }
        let mut span = HandlerlessSpan {
            ok: true,
            ..HandlerlessSpan::default()
        };
        let mut passed: Vec<String> = Vec::new();
        let mut seen = vec![key.to_string()];
        let mut at = key.to_string();
        loop {
            let Some(next) = self.next_status(&at) else {
                break;
            };
            if seen.contains(&next) {
                break;
            }
            seen.push(next.clone());
            let step = self.step(&next);
            if step.is_some_and(Step::hands_off) || next == "done" || next == "cancelled" {
                span.until = Some(next);
                break;
            }
            passed.push(next.clone());
            at = next;
        }
        if passed.iter().any(|key| key == "in_progress") {
            span.start = Some("in_progress".into());
        }
        if let Some(until) = &span.until
            && self.step(until).is_some_and(Step::hands_off)
        {
            span.deliver = span.until.clone();
            return span;
        }
        if let Some(key) = passed.iter().rev().find(|key| *key != "in_progress") {
            span.deliver = Some(key.clone());
            return span;
        }
        // Nothing but done lies ahead. From a step where work starts, done is
        // the only way out; from a delivery status such as in_review the work
        // is already handed in, and done stays a human's call.
        if !matches!(key, "backlog" | "todo" | "in_progress") {
            return span;
        }
        span.deliver = span.until.clone();
        span
    }
}

#[derive(Default)]
struct HandlerlessSpan {
    /// Set when the current step has no handler and the span applies.
    ok: bool,
    /// The status that marks the work as started, when the span passes it.
    start: Option<String>,
    /// The status to set once all of the work is done.
    deliver: Option<String>,
    /// Where the span ends: the next step that hands off, done or cancelled.
    until: Option<String>,
}

pub(super) struct ProjectRef {
    pub title: String,
    pub lead_type: Option<String>,
    pub lead_id: Option<Uuid>,
    workflow_id: Option<Uuid>,
    workspace_id: Uuid,
}

fn decode_row(row: &tokio_postgres::Row) -> Result<Definition, String> {
    Ok(Definition {
        name: row.get("name"),
        is_default: row.get("is_default"),
        steps: decode(row.get("steps")).map_err(|_| "decode workflow steps".to_string())?,
    })
}

const WORKFLOW_COLUMNS: &str = "id,workspace_id,name,is_default,steps";

/// The workspace Default workflow, or the built-in one when the workspace has
/// not stored its own yet.
pub(super) async fn default_for(
    db: &impl GenericClient,
    workspace_id: Uuid,
) -> Result<Definition, String> {
    let row = db
        .query_opt(
            &format!(
                "SELECT {WORKFLOW_COLUMNS} FROM issue_workflow WHERE workspace_id=$1 AND is_default"
            ),
            &[&workspace_id],
        )
        .await
        .map_err(|_| "get default workflow".to_string())?;
    match row {
        Some(row) => decode_row(&row),
        None => Ok(Definition {
            name: DEFAULT_NAME.into(),
            is_default: true,
            steps: steps_for(&built_in_default_keys()),
        }),
    }
}

/// The workflow a project uses, with the project row the brief names. `None`
/// means the issue is outside any project, or its reference is stale.
pub(super) async fn for_project(
    db: &impl GenericClient,
    workspace_id: Uuid,
    project_id: Option<Uuid>,
) -> Result<Option<(Definition, ProjectRef)>, String> {
    let Some(project_id) = project_id else {
        return Ok(None);
    };
    let Some(project) = db
        .query_opt(
            "SELECT id,workspace_id,title,lead_type,lead_id,workflow_id FROM project WHERE id=$1 AND workspace_id=$2",
            &[&project_id, &workspace_id],
        )
        .await
        .map_err(|_| "get project".to_string())?
    else {
        return Ok(None);
    };
    let project = ProjectRef {
        title: project.get("title"),
        lead_type: project.get("lead_type"),
        lead_id: project.get("lead_id"),
        workflow_id: project.get("workflow_id"),
        workspace_id: project.get("workspace_id"),
    };
    let definition = for_project_row(db, &project).await?;
    Ok(Some((definition, project)))
}

/// A dangling workflow reference falls back to Default.
async fn for_project_row(
    db: &impl GenericClient,
    project: &ProjectRef,
) -> Result<Definition, String> {
    if let Some(workflow_id) = project.workflow_id {
        let row = db
            .query_opt(
                &format!(
                    "SELECT {WORKFLOW_COLUMNS} FROM issue_workflow WHERE id=$1 AND workspace_id=$2"
                ),
                &[&workflow_id, &project.workspace_id],
            )
            .await
            .map_err(|_| "get workflow".to_string())?;
        if let Some(row) = row {
            return decode_row(&row);
        }
    }
    default_for(db, project.workspace_id).await
}

/// Turns a step's handler into a concrete assignee for one issue. `None` when
/// the step does not hand off, or a relative handler has nobody to resolve to.
pub(super) fn resolve_handler(
    step: &Step,
    project: &ProjectRef,
    creator_type: Option<&str>,
    creator_id: Option<Uuid>,
) -> Option<(String, Uuid)> {
    match step.handler.r#type.as_str() {
        HANDLER_AGENT | HANDLER_SQUAD | HANDLER_MEMBER => {
            let id = Uuid::parse_str(&step.handler.id).ok()?;
            Some((step.handler.r#type.clone(), id))
        }
        HANDLER_PROJECT_LEAD => Some((project.lead_type.clone()?, project.lead_id?)),
        HANDLER_CREATOR => Some((creator_type?.to_string(), creator_id?)),
        _ => None,
    }
}

/// What the handoff brief names.
pub(super) struct BriefInput<'a> {
    pub workflow: &'a Definition,
    pub project_name: &'a str,
    pub status_key: &'a str,
    pub issue_identifier: &'a str,
    /// Resolves a catalog key to its display name.
    pub status_name: Option<&'a (dyn Fn(&str) -> String + Sync)>,
    /// Describes who a step hands off to, e.g. "Sentinel" or "Lin (project
    /// lead)". Empty when the step does not hand off.
    pub handler_name: Option<&'a (dyn Fn(&Step) -> String + Sync)>,
    /// The brief for an agent working on the issue without being its assignee.
    pub observer: bool,
    /// The issue's assignee in an observer's brief; empty means unassigned.
    pub assignee_name: &'a str,
    /// Renders for a daemon whose runtime brief still states the built-in
    /// in_progress / in_review rules (MUL-8145).
    pub legacy_runtime_rules: bool,
}

fn name_of(input: &BriefInput<'_>, key: &str) -> String {
    let Some(resolve) = input.status_name else {
        return key.into();
    };
    let line = brief_line(&resolve(key));
    if line.is_empty() { key.into() } else { line }
}

/// What an agent reads about the issue's project workflow: the statuses, who
/// each step hands off to, and — for the assignee — the current step's
/// instructions and the commands for the next move.
pub(super) fn brief(input: &BriefInput<'_>) -> String {
    let workflow = input.workflow;
    let current = workflow.step(input.status_key);
    let mut out = String::new();
    let mut title = brief_line(&workflow.name);
    if !brief_line(input.project_name).is_empty() {
        title = format!("{title} ({})", brief_line(input.project_name));
    }
    out.push_str(&format!("This project's workflow — {title}\n"));

    // Each move names who the target step hands off to, so the list below needs
    // no preamble about handoffs.
    let hands_off_to = |key: &str| -> String {
        let Some(target) = workflow.step(key) else {
            return String::new();
        };
        let Some(resolve) = input.handler_name else {
            return String::new();
        };
        let who = brief_line(&resolve(target));
        if who.is_empty() {
            String::new()
        } else {
            format!(" — hands off to {who}")
        }
    };

    // An older daemon still states the built-in rules itself, so its brief keeps
    // the per-step wording rather than add a second in_progress rule.
    let span = if input.observer || input.legacy_runtime_rules {
        HandlerlessSpan::default()
    } else {
        workflow.handlerless_span(input.status_key)
    };

    if input.legacy_runtime_rules {
        out.push_str("This workflow replaces the status rules in your runtime instructions: use only the status keys below");
        if !workflow.has("in_review") {
            out.push_str(", never `in_review`");
        }
        out.push_str(".\n");
    }
    if input.observer {
        let assignee = if brief_line(input.assignee_name).is_empty() {
            "is unassigned".to_string()
        } else {
            format!("is assigned to {}", brief_line(input.assignee_name))
        };
        out.push_str(&format!(
            "{} is at the {:?} step and {assignee}. You are not its assignee: leave the status to them unless you are explicitly asked to change it.\n",
            input.issue_identifier,
            name_of(input, input.status_key)
        ));
    } else if span.ok {
        // A step without a handler is a progress marker, not a stage: naming it
        // "the step you handle" made agents split one task across columns.
        out.push_str(&format!(
            "You are handling {}. Its {:?} step has no handler, so the work the issue asks for is yours ",
            input.issue_identifier,
            name_of(input, input.status_key)
        ));
        match span.until.as_deref() {
            Some(until) if workflow.step(until).is_some_and(Step::hands_off) => {
                out.push_str(&format!(
                    "until it reaches the {:?} step{}.\n",
                    name_of(input, until),
                    hands_off_to(until)
                ));
            }
            _ => out.push_str("to finish.\n"),
        }
    } else {
        out.push_str(&format!(
            "You are handling the {:?} step of {}.\n",
            name_of(input, input.status_key),
            input.issue_identifier
        ));
        if let Some(step) = current {
            let instructions = brief_text(&step.instructions);
            if !instructions.is_empty() {
                out.push_str("\nStep instructions:\n");
                for line in instructions.split('\n') {
                    out.push_str("> ");
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
    }
    out.push_str("\nStatuses in this workflow:\n");
    for step in &workflow.steps {
        out.push_str(&format!("- `{}`", step.status_key));
        let name = name_of(input, &step.status_key);
        if name != step.status_key {
            out.push_str(&format!(" {name}"));
        }
        if let Some(resolve) = input.handler_name {
            let who = brief_line(&resolve(step));
            if !who.is_empty() {
                out.push_str(&format!(" — hands off to {who}"));
            }
        }
        if step.status_key == input.status_key {
            out.push_str("   ← current");
        }
        out.push('\n');
    }
    if input.observer {
        out.push_str("\nMoving the issue into a step that hands off reassigns it to that step's handler and starts their run.\n");
        return out;
    }
    out.push_str("\nYour moves:\n");
    if span.ok {
        if let Some(start) = &span.start {
            out.push_str(&format!(
                "Starting work  → multica issue status {} {start}\n",
                input.issue_identifier
            ));
        }
        match &span.deliver {
            Some(deliver) => out.push_str(&format!(
                "Step done      → multica issue status {} {deliver}{}\n",
                input.issue_identifier,
                hands_off_to(deliver)
            )),
            None => out.push_str(&format!(
                "Step done      → leave the issue at `{}`; `done` is set by a human\n",
                input.status_key
            )),
        }
    } else if let Some(next) = workflow.next_status(input.status_key) {
        out.push_str(&format!(
            "Step done      → multica issue status {} {next}{}\n",
            input.issue_identifier,
            hands_off_to(&next)
        ));
    }
    let back = current
        .map(|step| step.back_status_key.as_str())
        .unwrap_or("");
    if !back.is_empty() {
        out.push_str(&format!(
            "Needs changes  → multica issue status {} {back}{}\n",
            input.issue_identifier,
            hands_off_to(back)
        ));
    }
    if workflow.has("blocked") {
        out.push_str(&format!(
            "Can't proceed  → multica issue status {} blocked, then comment why\n",
            input.issue_identifier
        ));
    } else if !back.is_empty() {
        out.push_str("Can't proceed  → comment why, then run Needs changes\n");
    } else {
        out.push_str("Can't proceed  → comment why and leave the status as it is\n");
    }
    if span.ok {
        out.push_str("Steps without a handler hand nothing off and start no run: finish all of the issue's work in this run before Step done.\n");
    }
    if input.legacy_runtime_rules {
        out.push_str("These commands are the only status moves for your step: set no status to mark that you started, and none to mark delivery — no separate `in_progress` or `in_review`.\n");
        out.push_str("Before your run ends, either run one of them or leave the issue at this step on purpose because its work continues. The system does not advance the status when your run ends.\n");
    }
    out.push_str("When the issue enters a step you handle, you get a run automatically — do not add a wakeup for that. Wakeups for anything else (a time, a PR, another issue, a step someone else handles) are fine.\n");
    out
}

/// The workspace's effective issue prefix. Go falls back to the FROZEN
/// name-based derivation for rows that predate the column, and an identifier
/// computed today must keep matching one computed years ago — so this
/// deliberately does not use the slug-derived rule new workspaces follow.
pub(super) fn issue_prefix_for_workspace(issue_prefix: &str, name: &str) -> String {
    if !issue_prefix.is_empty() {
        return issue_prefix.to_owned();
    }
    legacy_issue_prefix_from_name(name)
}

/// `legacyIssuePrefixFromName`: the first three ASCII letters of a workspace
/// name, uppercased, `"WS"` when it has none. FROZEN — every historical issue
/// identifier in a workspace without a stored prefix is derived from this.
pub(super) fn legacy_issue_prefix_from_name(name: &str) -> String {
    let letters: String = name.chars().filter(|ch| ch.is_ascii_alphabetic()).collect();
    if letters.is_empty() {
        return "WS".into();
    }
    letters.chars().take(3).collect::<String>().to_uppercase()
}

/// Whether a run works on the issue as its assignee: the assigned agent's own
/// run, or a run made for the assigned squad. Any other run gets the observer
/// brief, so it does not move a step it does not own.
pub(super) fn task_runs_for_assignee(
    assignee_type: Option<&str>,
    assignee_id: Option<Uuid>,
    agent_id: Uuid,
    squad_id: Option<Uuid>,
) -> bool {
    let Some(assignee_id) = assignee_id else {
        return false;
    };
    match assignee_type {
        Some(HANDLER_AGENT) => agent_id == assignee_id,
        Some(HANDLER_SQUAD) => squad_id == Some(assignee_id),
        _ => false,
    }
}

/// Names an agent, squad or member for a brief. A lookup failure returns the
/// actor type, so an unreadable row never blocks a handoff.
async fn actor_display_name(
    db: &impl GenericClient,
    workspace_id: Uuid,
    actor_type: &str,
    id: Uuid,
) -> String {
    let sql = match actor_type {
        HANDLER_AGENT => "SELECT name FROM agent WHERE id=$1 AND workspace_id=$2",
        HANDLER_SQUAD => "SELECT name FROM squad WHERE id=$1 AND workspace_id=$2",
        HANDLER_MEMBER => "SELECT name FROM \"user\" WHERE id=$1",
        _ => return actor_type.to_owned(),
    };
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = if actor_type == HANDLER_MEMBER {
        vec![&id]
    } else {
        vec![&id, &workspace_id]
    };
    db.query_opt(sql, &params)
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<_, String>("name"))
        .unwrap_or_else(|| actor_type.to_owned())
}

/// Describes who a step hands the issue to. A relative handler with nobody to
/// resolve to still says so, because "the project lead (none set)" tells the
/// agent to ask a human while an empty string only leaves it guessing.
async fn step_handler_name(
    db: &impl GenericClient,
    workspace_id: Uuid,
    project: &ProjectRef,
    step: &Step,
    creator_type: Option<&str>,
    creator_id: Option<Uuid>,
) -> String {
    let Some((actor_type, id)) = resolve_handler(step, project, creator_type, creator_id) else {
        return match step.handler.r#type.as_str() {
            HANDLER_PROJECT_LEAD => "the project lead (none set)".into(),
            HANDLER_CREATOR => "the issue creator".into(),
            _ => String::new(),
        };
    };
    let name = actor_display_name(db, workspace_id, &actor_type, id).await;
    match step.handler.r#type.as_str() {
        HANDLER_PROJECT_LEAD => format!("{name} (project lead)"),
        HANDLER_CREATOR => format!("{name} (issue creator)"),
        _ => name,
    }
}

/// Everything a claim's workflow brief needs from the issue row.
pub(super) struct ClaimWorkflowIssue<'a> {
    pub workspace_id: Uuid,
    pub project_id: Option<Uuid>,
    pub status: &'a str,
    pub number: i64,
    pub assignee_type: Option<&'a str>,
    pub assignee_id: Option<Uuid>,
    pub creator_type: Option<&'a str>,
    pub creator_id: Option<Uuid>,
}

/// Go `claimProjectWorkflow`: the issue's project workflow rendered for a
/// claim, or `None` when there is nothing to say beyond the runtime's own
/// status rules — no project, or Default with no handoffs.
///
/// The brief's two callbacks are resolved up front: Go's `BriefInput` takes
/// closures that query lazily, and the Rust port cannot await inside them.
/// Pre-resolving reads the same rows exactly once each, so the text matches.
pub(super) async fn claim_project_workflow(
    db: &impl GenericClient,
    issue: &ClaimWorkflowIssue<'_>,
    agent_id: Uuid,
    squad_id: Option<Uuid>,
    legacy_runtime_rules: bool,
) -> Option<String> {
    let (definition, project) = match for_project(db, issue.workspace_id, issue.project_id).await {
        Ok(Some(found)) => found,
        Ok(None) => return None,
        Err(_) => {
            eprintln!(
                "task claim: failed to load project workflow for brief injection on workspace {}",
                issue.workspace_id
            );
            return None;
        }
    };
    if definition.is_default && !definition.hands_off_anywhere() {
        return None;
    }

    // Custom status display names, one catalog read. A built-in key has no name
    // on purpose — clients localize those from the key.
    let mut names: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    match super::daemon_claim_load::issue_statuses(db, issue.workspace_id).await {
        Ok(rows) => {
            for (key, name, _, _, _) in rows {
                names.insert(key, name);
            }
        }
        Err(_) => {
            eprintln!("task claim: failed to load the status catalog for a workflow brief");
        }
    }
    let status_name = move |key: &str| -> String {
        if super::catalog::is_builtin_status(key) {
            return String::new();
        }
        names.get(key).cloned().unwrap_or_default()
    };

    let mut handler_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for step in definition.steps.iter().filter(|step| step.hands_off()) {
        let name = step_handler_name(
            db,
            issue.workspace_id,
            &project,
            step,
            issue.creator_type,
            issue.creator_id,
        )
        .await;
        handler_names.insert(step.status_key.clone(), name);
    }
    let handler_name = move |step: &Step| -> String {
        handler_names
            .get(&step.status_key)
            .cloned()
            .unwrap_or_default()
    };

    let workspace = super::daemon_claim_load::workspace(db, issue.workspace_id)
        .await
        .ok()
        .flatten();
    let prefix = workspace
        .map(|row| issue_prefix_for_workspace(&row.issue_prefix, &row.name))
        .unwrap_or_default();
    let identifier = format!("{prefix}-{}", issue.number);

    let mut input = BriefInput {
        workflow: &definition,
        project_name: &project.title,
        status_key: issue.status,
        issue_identifier: &identifier,
        status_name: Some(&status_name),
        handler_name: Some(&handler_name),
        observer: false,
        assignee_name: "",
        legacy_runtime_rules,
    };
    let mut assignee_name = String::new();
    if !task_runs_for_assignee(issue.assignee_type, issue.assignee_id, agent_id, squad_id) {
        input.observer = true;
        if let (Some(assignee_type), Some(assignee_id)) = (issue.assignee_type, issue.assignee_id) {
            assignee_name =
                actor_display_name(db, issue.workspace_id, assignee_type, assignee_id).await;
        }
    }
    input.assignee_name = &assignee_name;
    Some(brief(&input))
}

/// Keeps a user-authored name on one line of a brief.
fn brief_line(value: &str) -> String {
    brief_text(value)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Drops the control characters of user-authored text, keeping its line breaks,
/// so it cannot reshape the brief around it.
fn brief_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.replace("\r\n", "\n").chars() {
        if ch == '\n' || ch == '\t' {
            out.push(ch);
        } else if ch < ' ' || ch == '\u{7f}' || ('\u{80}'..'\u{a0}').contains(&ch) {
            continue;
        } else if ch == '\u{2028}' || ch == '\u{2029}' {
            continue;
        } else {
            out.push(ch);
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(key: &str, handler: (&str, &str)) -> Step {
        Step {
            status_key: key.into(),
            handler: Handler {
                r#type: handler.0.into(),
                id: handler.1.into(),
            },
            instructions: String::new(),
            next_status_key: String::new(),
            back_status_key: String::new(),
        }
    }

    fn definition(steps: Vec<Step>) -> Definition {
        Definition {
            name: "Delivery".into(),
            is_default: false,
            steps,
        }
    }

    #[test]
    fn arrange_places_fixed_statuses_around_own_steps() {
        let steps = steps_for(&[
            "done".into(),
            "in_review".into(),
            "todo".into(),
            "backlog".into(),
            "in_progress".into(),
        ]);
        // Middle steps keep their own order; only the fixed ones move.
        assert_eq!(
            keys_of(&arrange(steps)),
            vec![
                "backlog",
                "todo",
                "in_review",
                "in_progress",
                "done",
                "cancelled"
            ]
        );
        assert_eq!(
            built_in_default_keys(),
            vec![
                "backlog",
                "todo",
                "in_progress",
                "in_review",
                "blocked",
                "done",
                "cancelled"
            ]
        );
    }

    #[test]
    fn next_status_falls_back_to_the_next_column() {
        let def = definition(steps_for(&[
            "todo".into(),
            "in_progress".into(),
            "blocked".into(),
            "in_review".into(),
            "done".into(),
        ]));
        assert_eq!(def.next_status("todo").as_deref(), Some("in_progress"));
        // blocked is skipped: it parks work, so it is never an automatic move.
        assert_eq!(def.next_status("in_progress").as_deref(), Some("in_review"));
        assert_eq!(def.next_status("missing"), None);
    }

    #[test]
    fn handlerless_span_reaches_the_next_handoff() {
        let mut steps = steps_for(&["todo".into(), "in_progress".into(), "in_review".into()]);
        steps[2] = agent(
            "in_review",
            (HANDLER_AGENT, "00000000-0000-0000-0000-000000000001"),
        );
        let def = definition(steps);
        let span = def.handlerless_span("todo");
        assert!(span.ok);
        assert_eq!(span.start.as_deref(), Some("in_progress"));
        assert_eq!(span.until.as_deref(), Some("in_review"));
        assert_eq!(span.deliver.as_deref(), Some("in_review"));
        // A step that already hands off owns its own command.
        assert!(!def.handlerless_span("in_review").ok);
    }

    #[test]
    fn brief_names_step_commands_and_handlers() {
        let mut steps = steps_for(&["todo".into(), "in_progress".into(), "in_review".into()]);
        steps[1] = agent(
            "in_progress",
            (HANDLER_AGENT, "00000000-0000-0000-0000-000000000001"),
        );
        let def = definition(steps);
        let name = |key: &str| match key {
            "in_progress" => "In Progress".to_string(),
            other => other.to_string(),
        };
        let handler = |step: &Step| -> String {
            if step.hands_off() {
                "Sentinel".into()
            } else {
                String::new()
            }
        };
        let text = brief(&BriefInput {
            workflow: &def,
            project_name: "Launch",
            status_key: "in_progress",
            issue_identifier: "MUL-7",
            status_name: Some(&name),
            handler_name: Some(&handler),
            observer: false,
            assignee_name: "",
            legacy_runtime_rules: false,
        });
        assert!(text.starts_with("This project's workflow — Delivery (Launch)\n"));
        assert!(text.contains("You are handling the \"In Progress\" step of MUL-7."));
        assert!(text.contains("- `in_progress` In Progress — hands off to Sentinel   ← current"));
        assert!(text.contains("Step done      → multica issue status MUL-7 in_review"));
        // No blocked status in this workflow: the brief cannot send it there.
        assert!(text.contains("Can't proceed  → comment why and leave the status as it is"));
    }

    #[test]
    fn brief_overrides_runtime_rules_only_for_legacy_daemons() {
        let def = definition(steps_for(&["todo".into(), "done".into()]));
        let render = |legacy| {
            brief(&BriefInput {
                workflow: &def,
                project_name: "",
                status_key: "todo",
                issue_identifier: "MUL-7",
                status_name: None,
                handler_name: None,
                observer: false,
                assignee_name: "",
                legacy_runtime_rules: legacy,
            })
        };
        assert!(!render(false).contains("replaces the status rules"));
        assert!(render(true).contains("replaces the status rules in your runtime instructions"));
        // A workflow without in_review must rule the key out explicitly.
        assert!(render(true).contains(", never `in_review`."));
    }

    #[test]
    fn brief_for_observer_leaves_the_step_to_its_assignee() {
        let def = definition(steps_for(&["todo".into(), "in_progress".into()]));
        let text = brief(&BriefInput {
            workflow: &def,
            project_name: "",
            status_key: "in_progress",
            issue_identifier: "MUL-7",
            status_name: None,
            handler_name: None,
            observer: true,
            assignee_name: "Lin",
            legacy_runtime_rules: false,
        });
        assert!(text.contains("MUL-7 is at the \"in_progress\" step and is assigned to Lin."));
        assert!(!text.contains("Your moves:"));
    }

    #[test]
    fn brief_drops_control_characters_from_user_text() {
        assert_eq!(brief_text("a\u{0}b\r\nc\t"), "ab\nc\t".trim_end());
        assert_eq!(brief_line("  two\nlines\there  "), "two lines here");
        assert_eq!(
            brief_text("keep\u{2028}separators out"),
            "keepseparators out"
        );
    }

    #[test]
    fn resolve_handler_covers_relative_handlers() {
        let project = ProjectRef {
            title: String::new(),
            lead_type: Some("agent".into()),
            lead_id: Some(Uuid::nil()),
            workflow_id: None,
            workspace_id: Uuid::nil(),
        };
        let concrete = agent(
            "todo",
            (HANDLER_AGENT, "00000000-0000-0000-0000-000000000002"),
        );
        assert_eq!(
            resolve_handler(&concrete, &project, None, None),
            Some((
                HANDLER_AGENT.into(),
                Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
            ))
        );
        let lead = agent("todo", (HANDLER_PROJECT_LEAD, ""));
        assert_eq!(
            resolve_handler(&lead, &project, None, None),
            Some((HANDLER_AGENT.into(), Uuid::nil()))
        );
        let creator = agent("todo", (HANDLER_CREATOR, ""));
        assert_eq!(resolve_handler(&creator, &project, None, None), None);
        assert_eq!(
            resolve_handler(&creator, &project, Some("member"), Some(Uuid::nil())),
            Some(("member".into(), Uuid::nil()))
        );
        let squad = agent("todo", (HANDLER_SQUAD, ""));
        // A squad handler needs its own id; an empty one resolves to nobody.
        assert_eq!(resolve_handler(&squad, &project, None, None), None);
    }
}
