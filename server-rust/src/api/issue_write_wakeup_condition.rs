//! Go validateCondition and registration baseline for Save(existingID).
use super::{ApiError, error};

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::Transaction;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Condition {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    field: String,
    value: Option<Value>,
    #[serde(default)]
    assignee_type: String,
    #[serde(default)]
    assignee_id: String,
    #[serde(default)]
    label_id: String,
    #[serde(default)]
    property_id: String,
    stage: Option<i32>,
    #[serde(default)]
    each_stage: bool,
    #[serde(default)]
    event: String,
    #[serde(default)]
    issue_id: String,
    #[serde(default)]
    state: String,
    #[serde(default, rename = "identifier")]
    _identifier: String,
}
fn bad(message: &str) -> ApiError {
    error(
        StatusCode::BAD_REQUEST,
        format!("invalid wakeup: invalid condition: {message}"),
    )
}
fn fail() -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup")
}
fn uuid(raw: &str, field: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad(&format!("{field} must be a UUID")))
}
async fn exists(tx: &Transaction<'_>, sql: &str, a: Uuid, b: Uuid) -> Result<bool, ApiError> {
    Ok(tx
        .query_one(sql, &[&a, &b])
        .await
        .map_err(|_| fail())?
        .get(0))
}
const BUILTINS: &[&str] = &[
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "done",
    "blocked",
    "cancelled",
];

pub(super) async fn normalize(
    tx: &Transaction<'_>,
    issue: Uuid,
    ws: Uuid,
    raw: &Value,
) -> Result<(Value, Vec<String>), ApiError> {
    let c: Condition =
        serde_json::from_value(raw.clone()).map_err(|_| bad("unreadable condition"))?;
    let normalized;
    let mut events = Vec::new();
    match c.kind.as_str() {
        "issue_field" => {
            normalized = match c.field.as_str() {
                "status" => {
                    let value = c
                        .value
                        .as_ref()
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| bad("status value must be a status key"))?;
                    if !BUILTINS.contains(&value) {
                        let found: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM issue_status WHERE workspace_id=$1 AND key=$2)", &[&ws, &value]).await.map_err(|_| fail())?.get(0);
                        if !found {
                            return Err(bad("unknown status"));
                        }
                    }
                    events.push("issue.status_changed".to_owned());
                    json!({"type":"issue_field","field":"status","value":value})
                }
                "assignee" => {
                    let id = uuid(&c.assignee_id, "assignee_id")?;
                    let found = match c.assignee_type.as_str() {
                        "member" => exists(tx, "SELECT EXISTS(SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2)", id, ws).await?,
                        "agent" => exists(tx, "SELECT EXISTS(SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2)", id, ws).await?,
                        "squad" => exists(tx, "SELECT EXISTS(SELECT 1 FROM squad WHERE id=$1 AND workspace_id=$2)", id, ws).await?,
                        _ => return Err(bad("assignee_type must be member, agent or squad")),
                    };
                    if !found {
                        return Err(bad("unknown assignee"));
                    }
                    events.push("issue.assignee_changed".to_owned());
                    json!({"type":"issue_field","field":"assignee","assignee_type":c.assignee_type,"assignee_id":id})
                }
                "label" => {
                    let id = uuid(&c.label_id, "label_id")?;
                    if !exists(
                        tx,
                        "SELECT EXISTS(SELECT 1 FROM issue_label WHERE id=$1 AND workspace_id=$2)",
                        id,
                        ws,
                    )
                    .await?
                    {
                        return Err(bad("unknown label"));
                    }
                    events.push("issue.labels_changed".to_owned());
                    json!({"type":"issue_field","field":"label","label_id":id})
                }
                "property" => {
                    let id = uuid(&c.property_id, "property_id")?;
                    if !exists(tx, "SELECT EXISTS(SELECT 1 FROM issue_property WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL)", id, ws).await? {
                        return Err(bad("unknown property"));
                    }
                    let value = c
                        .value
                        .filter(|v| !v.is_null())
                        .ok_or_else(|| bad("property value must be a JSON value within 1 KB"))?;
                    if serde_json::to_vec(&value).map_err(|_| fail())?.len() > 1024 {
                        return Err(bad("property value must be a JSON value within 1 KB"));
                    }
                    events.push("issue.properties_changed".to_owned());
                    json!({"type":"issue_field","field":"property","property_id":id,"value":value})
                }
                _ => return Err(bad("field must be status, assignee, label or property")),
            };
        }
        "children_done" => {
            if c.each_stage {
                return Err(bad(
                    "each_stage is reserved for the platform's sub-issue rule",
                ));
            }
            if c.stage.is_some_and(|s| !(1..=1000).contains(&s)) {
                return Err(bad("stage must be a positive number"));
            }
            normalized = match c.stage {
                Some(s) => json!({"type":"children_done","stage":s}),
                None => json!({"type":"children_done"}),
            };
        }
        "pull_request" => {
            if !matches!(c.event.as_str(), "checks_finished" | "merged" | "conflicts") {
                return Err(bad("event must be checks_finished, merged or conflicts"));
            }
            normalized = json!({"type":"pull_request","event":c.event});
        }
        "other_issue" => {
            let id = uuid(&c.issue_id, "issue_id")?;
            if id == issue {
                return Err(bad("watch another issue, not this one"));
            }
            if !matches!(c.state.as_str(), "done" | "ended" | "in_review" | "blocked") {
                return Err(bad("state must be done, ended, in_review or blocked"));
            }
            let ident = tx.query_opt("SELECT ws.issue_prefix||'-'||i.number FROM issue i JOIN workspace ws ON ws.id=i.workspace_id WHERE i.workspace_id=$1 AND i.id=$2", &[&ws, &id]).await.map_err(|_| fail())?
                .ok_or_else(|| bad("unknown issue"))?.get::<_, String>(0);
            normalized =
                json!({"type":"other_issue","issue_id":id,"state":c.state,"identifier":ident});
        }
        _ => {
            return Err(bad(
                "type must be issue_field, children_done, pull_request or other_issue",
            ));
        }
    }
    Ok((normalized, events))
}

// A checks_finished rule ignores PR snapshots already complete at registration;
// all other condition kinds fire immediately when a true fact is observed.
pub(super) async fn baseline(
    tx: &Transaction<'_>,
    id: Uuid,
    issue: Uuid,
    _ws: Uuid,
    condition: &Value,
    now: DateTime<Utc>,
    github_snapshots_enabled: bool,
) -> Result<(), ApiError> {
    let event = condition["event"].as_str().unwrap_or("");
    debug_assert_eq!(event, "checks_finished");
    let mut keys = Vec::<String>::new();
    let mut github_linked = 0;
    let mut missing_snapshot = false;
    for row in tx.query("SELECT pr.pr_number,pr.state,COALESCE(pr.snapshot_head_sha,''),COALESCE(pr.checks_rollup_state,''),pr.head_sha,COALESCE(pr.api_mergeable,''),pr.id::text FROM github_pull_request pr JOIN issue_pull_request ipr ON ipr.pull_request_id=pr.id WHERE ipr.issue_id=$1", &[&issue]).await.map_err(|_| fail())? {
        github_linked += 1;
        let number: i32 = row.get(0);
        let head: String = row.get(2);
        let checks: String = row.get(3);
        if github_snapshots_enabled {
            missing_snapshot |= head.is_empty();
            if !head.is_empty() && matches!(checks.as_str(), "SUCCESS" | "FAILURE" | "ERROR") {
                keys.push(format!("{number}@{head}:{checks}"));
            }
        }
    }
    let mut vcs_linked = 0;
    for row in tx.query(
        "SELECT pr.id,pr.pr_number,pr.provider,pr.state,pr.head_sha,COALESCE(SUM(CASE WHEN cs.state='failed' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN cs.state='pending' THEN 1 ELSE 0 END),0),COUNT(cs.state) FROM vcs_pull_request pr JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id=pr.id LEFT JOIN vcs_commit_status cs ON cs.connection_id=pr.connection_id AND cs.sha=pr.head_sha AND pr.head_sha<>'' WHERE ipr.issue_id=$1 GROUP BY pr.id", &[&issue]
    ).await.map_err(|_| fail())? {
        vcs_linked += 1;
        let pr_id: Uuid = row.get(0);
        let head: String = row.get(4);
        let failed: i64 = row.get(5);
        let pending: i64 = row.get(6);
        let total: i64 = row.get(7);
        let checks = if failed > 0 { "FAILURE" } else if total > 0 && pending == 0 { "SUCCESS" } else { "" };
        if !head.is_empty() && !checks.is_empty() { keys.push(format!("vcs:{pr_id}@{head}:{checks}")); }
    }
    let diagnostic = if keys.is_empty() {
        if !github_snapshots_enabled && (github_linked > 0 || vcs_linked == 0) {
            Some(
                "PR checks cannot be evaluated: GitHub snapshot refresh is unavailable on this server; ask the server administrator to configure the GitHub App snapshot client",
            )
        } else if github_linked == 0 && vcs_linked == 0 {
            Some(
                "No pull request is linked to this issue; link the PR to this issue (a parent issue's PR is not included), or wait for the linking webhook",
            )
        } else if missing_snapshot {
            Some(
                "PR check snapshot is not available yet; inspect this issue's pull-requests snapshot_available field and ask the server administrator to check snapshot refresh if it persists",
            )
        } else {
            None
        }
    } else {
        None
    };
    let last_error = diagnostic.map(|msg| format!("Condition pending: {msg}"));
    keys.sort();
    let fingerprint = if keys.is_empty() {
        String::new()
    } else {
        format!("pr:{}", keys.join(","))
    };
    let next = now + Duration::seconds(30);
    tx.execute("UPDATE issue_wakeup SET last_error=$2,condition_state=$3,next_fire_at=$4,updated_at=clock_timestamp() WHERE id=$1", &[&id,&last_error,&fingerprint,&next]).await.map_err(|_| fail())?;
    Ok(())
}
