//! Pure taskToResponse projection for daemon lifecycle responses, not claims.
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

fn id(row: &Row, key: &str) -> String {
    row.get::<_, Option<Uuid>>(key)
        .map(|v| v.to_string())
        .unwrap_or_default()
}
fn text(row: &Row, key: &str) -> String {
    row.get::<_, Option<String>>(key).unwrap_or_default()
}
fn time(row: &Row, key: &str) -> Value {
    row.get::<_, Option<DateTime<Utc>>>(key)
        .map(|v| json!(v.to_rfc3339_opts(SecondsFormat::AutoSi, true)))
        .unwrap_or(Value::Null)
}
fn optional(out: &mut Value, key: &str, value: String) {
    if !value.is_empty() {
        out[key] = json!(value);
    }
}

pub(super) fn response(row: &Row, workspace: Uuid) -> Value {
    let context: Value = row
        .get::<_, Option<Value>>("context")
        .unwrap_or(Value::Null);
    let task = id(row, "id");
    let issue = id(row, "issue_id");
    let chat = id(row, "chat_session_id");
    let autopilot = id(row, "autopilot_run_id");
    let trigger = id(row, "trigger_comment_id");
    let status: String = row.get("status");
    let kind = if !chat.is_empty() {
        "chat"
    } else if !autopilot.is_empty() {
        "autopilot"
    } else if context["type"] == "quick_create" || issue.is_empty() {
        "quick_create"
    } else if !trigger.is_empty() {
        "comment"
    } else {
        "direct"
    };
    let mut out = json!({
        "id":task,"agent_id":id(row,"agent_id"),"runtime_id":id(row,"runtime_id"),"issue_id":issue,"workspace_id":workspace,
        "status":status,"priority":row.get::<_,i32>("priority"),"dispatched_at":time(row,"dispatched_at"),"started_at":time(row,"started_at"),"completed_at":time(row,"completed_at"),
        "result":row.get::<_,Option<Value>>("result"),"error":row.get::<_,Option<String>>("error"),"attempt":row.get::<_,i32>("attempt"),"max_attempts":row.get::<_,i32>("max_attempts"),
        "created_at":time(row,"created_at"),"delivered_comment_ids":row.get::<_,Vec<Uuid>>("delivered_comment_ids"),"kind":kind,
    });
    if let Some(summary) = row.get::<_, Option<String>>("trigger_summary") {
        out["trigger_summary"] = json!(summary);
    }
    for key in ["parent_task_id", "trigger_comment_id"] {
        optional(&mut out, key, id(row, key));
    }
    for key in [
        "failure_reason",
        "branch_name",
        "handoff_note",
        "work_dir",
        "durable_work_dir",
    ] {
        optional(&mut out, key, text(row, key));
    }
    optional(
        &mut out,
        "relative_work_dir",
        relative_work_dir(&text(row, "work_dir"), &workspace.to_string(), &task),
    );
    optional(
        &mut out,
        "relative_durable_work_dir",
        relative_work_dir(&text(row, "durable_work_dir"), "", ""),
    );
    optional(&mut out, "chat_session_id", chat);
    optional(&mut out, "autopilot_run_id", autopilot);
    for (target, source) in [
        ("wakeup_id", "wakeup_id"),
        ("wakeup_system_rule", "wakeup_system"),
    ] {
        optional(
            &mut out,
            target,
            context[source].as_str().unwrap_or("").to_owned(),
        );
    }
    let joined = context["wakeup_joined"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|j| j["note"].as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n")
        })
        .unwrap_or_default();
    optional(&mut out, "wakeup_joined", joined);
    if row.get::<_, bool>("is_leader_task") {
        out["is_leader_task"] = json!(true);
    }
    let coalesced: Vec<Uuid> = row.get("coalesced_comment_ids");
    if !coalesced.is_empty() {
        out["coalesced_comment_ids"] = json!(coalesced);
    }
    if status == "cancelled" {
        if context["comment_change_cancelled_task_id"].as_str() == Some(&task) {
            out["cancelled_by_comment_change"] = json!(true);
        }
        let actor_type = text(row, "cancelled_by_type");
        if !actor_type.is_empty() {
            let mut actor = json!({"type":actor_type,"id":id(row,"cancelled_by_id")});
            optional(&mut actor, "name", text(row, "cancelled_by_name"));
            out["cancelled_by"] = actor;
        }
    }
    let source = text(row, "originator_source");
    let source = if source.is_empty() {
        "unattributed"
    } else {
        &source
    };
    let mut attribution = json!({"source":source,"precise":matches!(source,"direct_human"|"delegation"|"comment_source"|"trigger_owner"|"rule_owner")});
    for (target, column) in [
        ("initiator", "accountable_user_id"),
        ("originator", "originator_user_id"),
    ] {
        let user = id(row, column);
        if !user.is_empty() {
            attribution[target] = json!({"id":user});
        }
    }
    for key in [
        "rule_version_id",
        "delegated_from_task_id",
        "retry_of_task_id",
        "rerun_of_task_id",
    ] {
        optional(&mut attribution, key, id(row, key));
    }
    let evidence = text(row, "trigger_evidence_kind");
    if !evidence.is_empty() {
        attribution["evidence"] =
            json!({"kind":evidence,"ref_id":id(row,"trigger_evidence_ref_id")});
    }
    out["attribution"] = attribution;
    out
}

fn matches_segment(segment: &str, id: &str, workspace: bool) -> bool {
    let segment = segment.to_ascii_lowercase();
    let id = id.to_ascii_lowercase();
    let compact = id.replace('-', "");
    let legacy = &compact[..compact.len().min(8)];
    let current = &compact[compact.len().saturating_sub(12)..];
    (workspace && segment == id)
        || (!workspace && (segment == legacy || segment == current))
        || segment.ends_with(&format!("-{legacy}"))
        || segment.ends_with(&format!("-{current}"))
}

fn relative_work_dir(path: &str, workspace: &str, task: &str) -> String {
    let normalized = path.replace('\\', "/");
    let parts: Vec<_> = normalized.split('/').collect();
    if !workspace.is_empty() && !task.is_empty() {
        for i in 0..parts.len().saturating_sub(1) {
            if matches_segment(parts[i], workspace, true)
                && matches_segment(parts[i + 1], task, false)
            {
                return parts[i..].join("/");
            }
        }
    }
    let bytes = normalized.as_bytes();
    let without_drive = if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        &normalized[2..]
    } else {
        &normalized
    };
    let home: Vec<_> = without_drive.splitn(4, '/').collect();
    if home.len() >= 3
        && home[0].is_empty()
        && (home[1].eq_ignore_ascii_case("home") || home[1].eq_ignore_ascii_case("users"))
        && !home[2].is_empty()
    {
        return home.get(3).unwrap_or(&"").to_string();
    }
    normalized
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn relative_paths_remove_account_names_and_keep_id_verified_layouts() {
        for (path, expected) in [
            ("/home/person/repos/app", "repos/app"),
            (r"C:\Users\person\repo", "repo"),
            ("/Users/person", ""),
            ("/srv/private/repo/", "repo"),
            ("/", ""),
            ("", ""),
            ("/home//repo", "repo"),
        ] {
            assert_eq!(relative_work_dir(path, "", ""), expected);
        }
        let ws = "11111111-2222-3333-4444-555555555555";
        let task = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        assert_eq!(
            relative_work_dir(&format!("/private/{ws}/aaaaaaaa/work"), ws, task),
            format!("{ws}/aaaaaaaa/work")
        );
        assert_eq!(
            relative_work_dir(
                "/private/team-555555555555/issue-eeeeeeeeeeee/work",
                ws,
                task
            ),
            "team-555555555555/issue-eeeeeeeeeeee/work"
        );
        assert_eq!(
            relative_work_dir("/private/team-555555555555/wrong/work", ws, task),
            "work"
        );
    }
}
