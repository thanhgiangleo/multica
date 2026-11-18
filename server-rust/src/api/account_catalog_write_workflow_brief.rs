//! Read-only editor preview of the frozen workflow brief; never saves the draft.
use super::*;
use std::fmt::Write;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issue-workflows/{id}/brief", post(preview))
}

#[derive(Default, Deserialize)]
struct PreviewRequest {
    #[serde(flatten)]
    step: StepInput,
    #[serde(default, deserialize_with = "decode::null_default")]
    project_id: String,
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap_or_default()
}
fn kind(step: &Value) -> &str {
    text(&step["handler"], "type")
}
fn hands_off(step: &Value) -> bool {
    matches!(
        kind(step),
        "agent" | "squad" | "member" | "project_lead" | "creator"
    )
}
fn brief_text(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .chars()
        .filter(|&c| {
            matches!(c, '\n' | '\t')
                || !(c < '\u{20}'
                    || ('\u{7f}'..='\u{9f}').contains(&c)
                    || matches!(c, '\u{2028}' | '\u{2029}'))
        })
        .collect::<String>()
        .trim()
        .to_owned()
}
fn brief_line(value: &str) -> String {
    brief_text(value)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
fn quote_line(value: &str) -> String {
    let mut output = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            _ if c.escape_debug().to_string().starts_with("\\u{") => {
                // Go %q uses fixed-width Unicode escapes, not Rust's brace syntax.
                if (c as u32) <= 0xffff {
                    write!(output, "\\u{:04x}", c as u32).unwrap();
                } else {
                    write!(output, "\\U{:08x}", c as u32).unwrap();
                }
            }
            _ => output.push(c),
        }
    }
    output.push('"');
    output
}
fn next_status<'a>(steps: &'a [Value], key: &str) -> &'a str {
    let Some(index) = steps.iter().position(|s| s["status_key"] == key) else {
        return "";
    };
    let explicit = text(&steps[index], "next_status_key");
    if !explicit.is_empty() {
        return explicit;
    }
    steps[index + 1..]
        .iter()
        .find(|s| s["status_key"] != "blocked")
        .map_or("", |s| text(s, "status_key"))
}

// This is the editor/assignee rendering, without legacy daemon or observer rules.
fn render(
    workflow: &str,
    project: &str,
    key: &str,
    identifier: &str,
    steps: &[Value],
    status_names: &HashMap<String, String>,
    handler_names: &HashMap<(String, String), String>,
) -> String {
    let current = steps.iter().find(|s| s["status_key"] == key).unwrap();
    let name = |key: &str| {
        status_names
            .get(key)
            .map(|s| brief_line(s))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| key.to_owned())
    };
    let handler = |step: &Value| match kind(step) {
        "project_lead" => "the project lead".to_owned(),
        "creator" => "the issue creator".to_owned(),
        "agent" | "squad" | "member" => brief_line(
            handler_names
                .get(&(
                    kind(step).to_owned(),
                    text(&step["handler"], "id").to_owned(),
                ))
                .map_or(kind(step), String::as_str),
        ),
        _ => String::new(),
    };
    let suffix = |key: &str| {
        steps
            .iter()
            .find(|s| s["status_key"] == key)
            .map(&handler)
            .filter(|s| !s.is_empty())
            .map_or(String::new(), |s| format!(" — hands off to {s}"))
    };
    let span = !hands_off(current) && !matches!(key, "blocked" | "done" | "cancelled");
    let mut start = "";
    let mut deliver = "";
    let mut until = "";
    if span {
        let mut passed = Vec::new();
        let mut seen = HashSet::from([key]);
        let mut at = key;
        loop {
            let next = next_status(steps, at);
            if next.is_empty() || !seen.insert(next) {
                break;
            }
            if steps
                .iter()
                .any(|s| s["status_key"] == next && hands_off(s))
                || matches!(next, "done" | "cancelled")
            {
                until = next;
                break;
            }
            passed.push(next);
            at = next;
        }
        if passed.contains(&"in_progress") {
            start = "in_progress";
        }
        if steps
            .iter()
            .any(|s| s["status_key"] == until && hands_off(s))
        {
            deliver = until;
        } else if let Some(last) = passed.iter().rev().find(|&&s| s != "in_progress") {
            deliver = last;
        } else if matches!(key, "backlog" | "todo" | "in_progress") {
            deliver = until;
        }
    }
    let mut title = brief_line(workflow);
    let project = brief_line(project);
    if !project.is_empty() {
        write!(title, " ({project})").unwrap();
    }
    let mut output = format!("This project's workflow — {title}\n");
    if span {
        write!(output, "You are handling {identifier}. Its {} step has no handler, so the work the issue asks for is yours ", quote_line(&name(key))).unwrap();
        if steps
            .iter()
            .any(|s| s["status_key"] == until && hands_off(s))
        {
            writeln!(
                output,
                "until it reaches the {} step{}.",
                quote_line(&name(until)),
                suffix(until)
            )
            .unwrap();
        } else {
            output.push_str("to finish.\n");
        }
    } else {
        writeln!(
            output,
            "You are handling the {} step of {identifier}.",
            quote_line(&name(key))
        )
        .unwrap();
        let instructions = brief_text(text(current, "instructions"));
        if !instructions.is_empty() {
            output.push_str("\nStep instructions:\n");
            for line in instructions.split('\n') {
                writeln!(output, "> {line}").unwrap();
            }
        }
    }
    output.push_str("\nStatuses in this workflow:\n");
    for step in steps {
        let status = text(step, "status_key");
        write!(output, "- `{status}`").unwrap();
        let display = name(status);
        if display != status {
            write!(output, " {display}").unwrap();
        }
        let who = handler(step);
        if !who.is_empty() {
            write!(output, " — hands off to {who}").unwrap();
        }
        if status == key {
            output.push_str("   ← current");
        }
        output.push('\n');
    }
    output.push_str("\nYour moves:\n");
    if span {
        if !start.is_empty() {
            writeln!(
                output,
                "Starting work  → multica issue status {identifier} {start}"
            )
            .unwrap();
        }
        if !deliver.is_empty() {
            writeln!(
                output,
                "Step done      → multica issue status {identifier} {deliver}{}",
                suffix(deliver)
            )
            .unwrap();
        } else {
            writeln!(
                output,
                "Step done      → leave the issue at `{key}`; `done` is set by a human"
            )
            .unwrap();
        }
    } else {
        let next = next_status(steps, key);
        if !next.is_empty() {
            writeln!(
                output,
                "Step done      → multica issue status {identifier} {next}{}",
                suffix(next)
            )
            .unwrap();
        }
    }
    let back = text(current, "back_status_key");
    if !back.is_empty() {
        writeln!(
            output,
            "Needs changes  → multica issue status {identifier} {back}{}",
            suffix(back)
        )
        .unwrap();
    }
    if steps.iter().any(|s| s["status_key"] == "blocked") {
        writeln!(
            output,
            "Can't proceed  → multica issue status {identifier} blocked, then comment why"
        )
        .unwrap();
    } else if !back.is_empty() {
        output.push_str("Can't proceed  → comment why, then run Needs changes\n");
    } else {
        output.push_str("Can't proceed  → comment why and leave the status as it is\n");
    }
    if span {
        output.push_str("Steps without a handler hand nothing off and start no run: finish all of the issue's work in this run before Step done.\n");
    }
    output.push_str("When the issue enters a step you handle, you get a run automatically — do not add a wakeup for that. Wakeups for anything else (a time, a PR, another issue, a step someone else handles) are fine.\n");
    output
}

async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
    body: Result<Json<PreviewRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<super::super::GoJson<Value>, Failure> {
    let db = state.pool.get().await.map_err(|_| save_failure())?;
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| save_failure())?
        .is_none()
    {
        return Err(fail(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let id =
        parse_go_uuid(&raw).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workflow id"))?;
    let Json(body) = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let row = db
        .query_opt(
            "SELECT name,steps FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| save_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workflow not found"))?;
    let mut steps = handoff::normalized_steps(row.get("steps"))?;
    let key = body.step.status_key.trim();
    let request = handoff::HandoffRequest {
        handler: body.step.handler.unwrap_or_default(),
        instructions: body.step.instructions,
        next_status_key: body.step.next_status_key,
        back_status_key: body.step.back_status_key,
        ..Default::default()
    };
    let draft = handoff::step_value(key, &request);
    if !steps.iter().any(|s| s["status_key"] == key) {
        return Err(fail(
            StatusCode::BAD_REQUEST,
            "status_key is not a column of this workflow",
        ));
    }
    let handler_kind = kind(&draft);
    let target = text(&draft["handler"], "id");
    if matches!(handler_kind, "agent" | "squad" | "member")
        && !steps.iter().any(|s| s["handler"] == draft["handler"])
    {
        let target_id = parse_go_uuid(target)
            .ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid handler id"))?;
        if handler_kind != "member" {
            // Assignment authorization is a shared contract, not a local admin bypass.
            return Err(fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "workflow handler authorization integration unavailable",
            ));
        }
        if db
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &target_id],
            )
            .await
            .map_err(|_| save_failure())?
            .is_none()
        {
            return Err(fail(
                StatusCode::BAD_REQUEST,
                "assignee_id does not refer to a member of this workspace",
            ));
        }
    }
    for step in &mut steps {
        if step["status_key"] == key {
            *step = draft.clone();
        }
    }
    let project = if let Some(project) = parse_go_uuid(&body.project_id) {
        db.query_opt(
            "SELECT title FROM project WHERE id=$1 AND workspace_id=$2",
            &[&project, &ws],
        )
        .await
        .ok()
        .flatten()
        .map(|r| r.get::<_, String>(0))
        .unwrap_or_default()
    } else {
        String::new()
    };
    let mut status_names = HashMap::new();
    for row in db
        .query(
            "SELECT key,name FROM issue_status WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .map_err(|_| save_failure())?
    {
        let key: String = row.get(0);
        if !matches!(
            key.as_str(),
            "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
        ) {
            status_names.insert(key, row.get::<_, String>(1));
        }
    }
    let mut handler_names = HashMap::new();
    for step in &steps {
        let handler_kind = kind(step);
        let raw = text(&step["handler"], "id");
        if let Some(id) = parse_go_uuid(raw) {
            let sql = match handler_kind {
                "member" => {
                    "SELECT u.name FROM \"user\" u JOIN member m ON m.user_id=u.id WHERE u.id=$1 AND m.workspace_id=$2"
                }
                "agent" => "SELECT name FROM agent WHERE id=$1 AND workspace_id=$2",
                "squad" => "SELECT name FROM squad WHERE id=$1 AND workspace_id=$2",
                _ => continue,
            };
            if let Ok(Some(row)) = db.query_opt(sql, &[&id, &ws]).await {
                handler_names.insert((handler_kind.to_owned(), raw.to_owned()), row.get(0));
            }
        }
    }
    let workspace = db
        .query_one(
            "SELECT name,issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .map_err(|_| save_failure())?;
    let mut prefix: String = workspace.get("issue_prefix");
    if prefix.is_empty() {
        prefix = super::super::squad::legacy_prefix(workspace.get("name"));
    }
    Ok(super::super::GoJson(
        json!({"brief": render(row.get("name"), &project, key, &format!("{prefix}-123"), &steps, &status_names, &handler_names)}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_go_renderer_oracle_matches_editor() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_brief_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let steps = case["workflow"]["steps"].as_array().unwrap();
            let statuses: HashMap<String, String> =
                serde_json::from_value(case["status_names"].clone()).unwrap();
            let handlers: HashMap<String, String> =
                serde_json::from_value(case["handler_names"].clone()).unwrap();
            let handlers = handlers
                .into_iter()
                .map(|(key, name)| {
                    let (kind, id) = key.split_once(':').unwrap();
                    ((kind.to_owned(), id.to_owned()), name)
                })
                .collect();
            let actual = render(
                case["workflow"]["name"].as_str().unwrap(),
                case["project"].as_str().unwrap(),
                case["status_key"].as_str().unwrap(),
                case["identifier"].as_str().unwrap(),
                steps,
                &statuses,
                &handlers,
            );
            assert_eq!(
                actual,
                case["brief"].as_str().unwrap(),
                "case {}",
                case["name"]
            );
        }
    }

    #[test]
    fn names_and_instructions_cannot_reshape_brief() {
        assert_eq!(
            brief_line("  One\r\nTwo\t\u{1b}\u{85}\u{2028}Three  "),
            "One Two Three"
        );
        assert_eq!(brief_text(" x\r\ny\t\u{7f}\u{9f}\u{2029}z "), "x\ny\tz");
    }

    #[test]
    fn handlerless_span_delivers_once_and_leaves_done_to_human() {
        let steps = vec![
            step("todo"),
            step("in_progress"),
            step("in_review"),
            step("blocked"),
            step("done"),
            step("cancelled"),
        ];
        let brief = render(
            "Flow",
            "",
            "todo",
            "TEST-123",
            &steps,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(brief.contains("Starting work  → multica issue status TEST-123 in_progress\n"));
        assert!(brief.contains("Step done      → multica issue status TEST-123 in_review\n"));
        assert!(!brief.contains("status TEST-123 done"));
        let brief = render(
            "Flow",
            "",
            "in_review",
            "TEST-123",
            &steps,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(brief.contains("leave the issue at `in_review`; `done` is set by a human"));
    }

    #[test]
    fn explicit_cycles_terminate_and_handoff_uses_sanitized_names() {
        let steps = vec![
            json!({"status_key":"todo","handler":{"type":"none"},"next_status_key":"review"}),
            json!({"status_key":"review","handler":{"type":"member","id":"member"},"instructions":"Check\nall","back_status_key":"todo","next_status_key":"todo"}),
        ];
        let names = HashMap::from([(
            ("member".to_owned(), "member".to_owned()),
            "Sam\nReviewer".to_owned(),
        )]);
        let brief = render(
            "Flow",
            "Campaign",
            "todo",
            "TEST-123",
            &steps,
            &HashMap::new(),
            &names,
        );
        assert!(
            brief.contains("until it reaches the \"review\" step — hands off to Sam Reviewer.")
        );
        assert!(brief.contains(
            "Step done      → multica issue status TEST-123 review — hands off to Sam Reviewer\n"
        ));
        let brief = render(
            "Flow",
            "",
            "review",
            "TEST-123",
            &steps,
            &HashMap::new(),
            &names,
        );
        assert!(brief.contains("\nStep instructions:\n> Check\n> all\n"));
        assert!(brief.contains("Needs changes  → multica issue status TEST-123 todo\n"));
    }
}
