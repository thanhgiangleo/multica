//! Search sync projection shapes from the frozen Go API, not detail responses.
use super::{squad::legacy_prefix, timestamp};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) const ISSUE_SELECT: &str = "SELECT i.*, \
    w.name AS workspace_name, w.issue_prefix, \
    st.category AS custom_category, st.name AS custom_name, \
    original.id AS original_id, original.number AS original_number, \
    original.title AS original_title, original.status AS original_status \
    FROM issue i JOIN workspace w ON w.id=i.workspace_id \
    LEFT JOIN issue_status st ON st.workspace_id=i.workspace_id AND st.key=i.status \
    LEFT JOIN issue original ON i.status='cancelled' AND original.id=i.duplicate_of_issue_id \
      AND original.workspace_id=i.workspace_id";

pub(super) fn nano(t: DateTime<Utc>) -> String {
    // RFC3339Nano trims insignificant zeros rather than rounding to milliseconds.
    let mut raw = t.to_rfc3339_opts(SecondsFormat::Nanos, true);
    if let Some(dot) = raw.find('.') {
        let end = raw.len() - 1;
        let significant = raw[dot + 1..end].trim_end_matches('0').len();
        raw.truncate(if significant == 0 {
            dot
        } else {
            dot + 1 + significant
        });
        raw.push('Z');
    }
    raw
}

fn object_or_empty(value: Value) -> Value {
    if value.is_object() { value } else { json!({}) }
}

fn builtin(status: &str) -> bool {
    matches!(
        status,
        "backlog" | "todo" | "in_progress" | "in_review" | "done" | "blocked" | "cancelled"
    )
}

pub(super) fn issue(row: &Row) -> Value {
    let stored: String = row.get("issue_prefix");
    let prefix = if stored.is_empty() {
        legacy_prefix(row.get("workspace_name"))
    } else {
        stored
    };
    let status: &str = row.get("status");
    let category = if builtin(status) {
        status
    } else {
        match row.get::<_, Option<&str>>("custom_category") {
            Some("unstarted") => "todo",
            Some("started") => "in_progress",
            Some("done") => "done",
            Some("closed") => "cancelled",
            _ => "",
        }
    };
    let duplicate = row.get::<_,Option<Uuid>>("original_id").map(|id|json!({
        "id":id, "identifier":format!("{prefix}-{}",row.get::<_,i32>("original_number")),
        "title":row.get::<_,String>("original_title"), "status":row.get::<_,String>("original_status"),
    }));
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "number":row.get::<_,i32>("number"), "identifier":format!("{prefix}-{}",row.get::<_,i32>("number")),
        "title":row.get::<_,String>("title"), "description":row.get::<_,Option<String>>("description"),
        "status":status, "status_name":if builtin(status) { "" } else { row.get::<_,Option<&str>>("custom_name").unwrap_or("") },
        "priority":row.get::<_,String>("priority"), "assignee_type":row.get::<_,Option<String>>("assignee_type"),
        "assignee_id":row.get::<_,Option<Uuid>>("assignee_id"), "creator_type":row.get::<_,String>("creator_type"),
        "creator_id":row.get::<_,Uuid>("creator_id"), "parent_issue_id":row.get::<_,Option<Uuid>>("parent_issue_id"),
        "duplicate_of":duplicate, "project_id":row.get::<_,Option<Uuid>>("project_id"), "position":row.get::<_,f64>("position"),
        "stage":row.get::<_,Option<i32>>("stage"),
        "start_date":row.get::<_,Option<NaiveDate>>("start_date").map(|d|d.to_string()),
        "due_date":row.get::<_,Option<NaiveDate>>("due_date").map(|d|d.to_string()),
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
        "search_updated_at":nano(row.get::<_,DateTime<Utc>>("updated_at")),
        "revision":row.get::<_,i64>("revision"),
        "last_activity_at":row.get::<_,Option<DateTime<Utc>>>("last_activity_at").map(nano),
        "metadata":object_or_empty(row.get("metadata")), "properties":object_or_empty(row.get("properties")),
    });
    if !category.is_empty() {
        value["status_category"] = json!(category);
    }
    value
}

pub(super) fn project(row: &Row) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "title":row.get::<_,String>("title"), "description":row.get::<_,Option<String>>("description"),
        "icon":row.get::<_,Option<String>>("icon"), "status":row.get::<_,String>("status"),
        "priority":row.get::<_,String>("priority"), "lead_type":row.get::<_,Option<String>>("lead_type"),
        "lead_id":row.get::<_,Option<Uuid>>("lead_id"),
        "start_date":row.get::<_,Option<NaiveDate>>("start_date").map(|d|d.to_string()),
        "due_date":row.get::<_,Option<NaiveDate>>("due_date").map(|d|d.to_string()),
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
        "search_updated_at":nano(row.get::<_,DateTime<Utc>>("updated_at")),
        "issue_count":0, "done_count":0, "resource_count":0,
        "workflow_id":row.get::<_,Option<Uuid>>("workflow_id"),
    })
}

pub(super) fn comment(row: &Row) -> Value {
    json!({"id":row.get::<_,Uuid>("id"),"issue_id":row.get::<_,Uuid>("issue_id"),
        "content":row.get::<_,String>("content"),"created_at":nano(row.get("created_at"))})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nanos_match_go_rfc3339nano() {
        for (raw, want) in [
            ("2026-11-02T12:00:00.000000000Z", "2026-11-02T12:00:00Z"),
            ("2026-11-02T12:00:00.120000000Z", "2026-11-02T12:00:00.12Z"),
            (
                "2026-11-02T12:00:00.000001000Z",
                "2026-11-02T12:00:00.000001Z",
            ),
        ] {
            assert_eq!(nano(raw.parse().unwrap()), want);
        }
    }
}
