//! Issue-linked PR read model. Provider refresh admission belongs to external_cloud.
use super::{
    ApiError, ApiState, GoJson, catalog::WorkspaceId, error, issue_read::load_issue,
    squad::legacy_prefix, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::{Arc, LazyLock};
use tokio_postgres::{IsolationLevel, Row, Transaction};
use uuid::Uuid;

/// Authorized mirrored row only; no credentials or provider I/O in this module.
/// The adapter MUST validate installation ownership again before provider access.
#[derive(Clone, Debug)]
pub struct PullRequestView {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub pull_request_id: Uuid,
    pub installation_id: i64,
    pub repo_owner: String,
    pub repo_name: String,
    pub number: i32,
    pub snapshot_fetched_at: Option<DateTime<Utc>>,
    pub current_snapshot: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshAdmission {
    Queued,
    NotNeeded,
    Saturated,
    Unavailable,
}

/// Synchronous, nonblocking queue admission only. Never fetch from a provider
/// here, spawn detached request-owned work, or silently register a no-op sink.
/// Disabled is an explicit deployment configuration, not a missing adapter.
pub trait PullRequestRefresh: Send + Sync {
    fn enabled(&self) -> bool;
    fn maybe_enqueue_on_view(&self, view: PullRequestView) -> RefreshAdmission;
}
pub type SharedPullRequestRefresh = Arc<dyn PullRequestRefresh>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/pull-requests", get(list))
}
fn db_error(err: tokio_postgres::Error) -> ApiError {
    if err.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) {
        error(StatusCode::GATEWAY_TIMEOUT, "pull request query timed out")
    } else {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list pull requests",
        )
    }
}
fn unavailable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "pull request refresh policy unavailable",
    )
}
fn lower(value: Option<String>) -> Option<String> {
    value.filter(|s| !s.is_empty()).map(|s| s.to_lowercase())
}
fn conclusion(
    rollup: Option<&str>,
    failed: i64,
    running: i64,
    passed: i64,
) -> Option<&'static str> {
    match rollup?.to_uppercase().as_str() {
        "" => None,
        "FAILURE" | "ERROR" => Some("failed"),
        "PENDING" | "EXPECTED" => Some("pending"),
        "SUCCESS" => Some("passed"),
        _ => counts_conclusion(failed, running, passed),
    }
}
fn counts_conclusion(failed: i64, running: i64, passed: i64) -> Option<&'static str> {
    if failed > 0 {
        Some("failed")
    } else if running > 0 {
        Some("pending")
    } else if passed > 0 {
        Some("passed")
    } else {
        None
    }
}
fn snapshot_current(row: &Row) -> bool {
    let head: &str = row.get("snapshot_head_sha");
    row.get::<_, Option<DateTime<Utc>>>("snapshot_fetched_at")
        .is_some()
        && !head.is_empty()
        && head == row.get::<_, &str>("head_sha")
}
fn snapshot_stale(state: &str, fetched: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    fetched.is_some_and(|at| {
        matches!(state, "open" | "draft")
            && now.signed_duration_since(at) > chrono::Duration::minutes(30)
    })
}
fn projection(
    row: &Row,
    github: bool,
    enabled: bool,
    now: DateTime<Utc>,
    identifier: &str,
) -> Value {
    let snapshot = github && enabled && snapshot_current(row);
    let checks = !github || snapshot;
    let count = |key| if checks { row.get::<_, i64>(key) } else { 0 };
    let (total, failed, passed, running) = (
        count("checks_total"),
        count("checks_failed"),
        count("checks_passed"),
        count("checks_running"),
    );
    let fetched: Option<DateTime<Utc>> = if snapshot {
        row.get("snapshot_fetched_at")
    } else {
        None
    };
    let state: &str = row.get("state");
    let title: &str = row.get("title");
    let branch: Option<String> = row.get("branch");
    let rollup: Option<String> = if snapshot {
        row.get("checks_rollup_state")
    } else {
        None
    };
    let mut value = json!({
        "id": row.get::<_,Uuid>("id"), "provider": if github { "github" } else { row.get::<_,&str>("provider") },
        "workspace_id": row.get::<_,Uuid>("workspace_id"),
        "repo_owner":row.get::<_,&str>("repo_owner"), "repo_name":row.get::<_,&str>("repo_name"),
        "number":row.get::<_,i32>("pr_number"), "title":title, "state":state, "html_url":row.get::<_,&str>("html_url"),
        "branch":branch, "author_login":row.get::<_,Option<String>>("author_login"), "author_avatar_url":row.get::<_,Option<String>>("author_avatar_url"),
        "merged_at":row.get::<_,Option<DateTime<Utc>>>("merged_at").map(timestamp), "closed_at":row.get::<_,Option<DateTime<Utc>>>("closed_at").map(timestamp),
        "pr_created_at":timestamp(row.get("pr_created_at")), "pr_updated_at":timestamp(row.get("pr_updated_at")),
        "link_source":link_source(row.get("linked_by_type"), identifier, title, branch.as_deref().unwrap_or_default()),
        "mergeable_state":if github { row.get::<_,Option<String>>("mergeable_state") } else { None },
        "mergeable":if snapshot { lower(row.get("api_mergeable")) } else { None },
        "merge_state_status":if snapshot { lower(row.get("api_merge_state_status")) } else { None },
        "checks_rollup":lower(rollup.clone()),
        "checks_conclusion":if github { conclusion(rollup.as_deref(),failed,running,passed) } else if total == 0 { None } else { counts_conclusion(failed,running,passed) },
        "checks_total":total, "checks_passed":passed, "checks_failed":failed, "checks_running":running, "checks_pending":running,
        "failed_check_names":if snapshot { row.get::<_,Vec<String>>("failed_check_names") } else { Vec::new() },
        "snapshot_stale":snapshot_stale(state,fetched,now),
        "snapshot_fetched_at":fetched.map(timestamp), "additions":row.get::<_,i32>("additions"), "deletions":row.get::<_,i32>("deletions"), "changed_files":row.get::<_,i32>("changed_files"),
    });
    if github {
        value["snapshot_available"] = json!(snapshot);
    }
    value
}
fn link_source(linked: &str, identifier: &str, title: &str, branch: &str) -> &'static str {
    // Go regexp \b is ASCII, unlike regex crate's default Unicode boundary.
    static IDS: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"(?i-u)\b([a-z][a-z0-9]{0,9})-([0-9]+)\b").unwrap());
    let contains = |text: &str| {
        IDS.find_iter(text)
            .any(|m| m.as_str().eq_ignore_ascii_case(identifier))
    };
    if linked == "member" {
        "manual"
    } else if contains(title) {
        "title"
    } else if contains(branch) {
        "branch"
    } else {
        "auto"
    }
}
fn builtin_category(key: &str) -> Option<&'static str> {
    match key {
        "backlog" | "todo" => Some("unstarted"),
        "in_progress" | "in_review" | "blocked" => Some("started"),
        "done" => Some("done"),
        "cancelled" => Some("closed"),
        _ => None,
    }
}
fn target_setting(settings: &Value) -> String {
    // Go accepts a null map, but rejects scalar/array settings blobs.
    if !settings.is_object() && !settings.is_null() {
        return "none".into();
    }
    if settings.get("pr_merge_status").is_none_or(Value::is_null) {
        return if settings
            .get("pr_auto_complete_enabled")
            .is_none_or(|v| v.is_null() || v == true)
        {
            "done"
        } else {
            "none"
        }
        .into();
    }
    settings["pr_merge_status"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("none")
        .to_lowercase()
}
// Match Go's json.Unmarshal into []storedStep: null steps/entries and null
// string fields decode as zero values; unknown fields are ignored. Wrong field
// types anywhere (including handoff fields unused by this read) reject the row.
fn decode_workflow_keys(steps: &Value) -> Result<Vec<&str>, ApiError> {
    let invalid = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list pull requests",
        )
    };
    if steps.is_null() {
        return Ok(Vec::new());
    }
    let steps = steps.as_array().ok_or_else(invalid)?;
    let mut keys = Vec::with_capacity(steps.len());
    for step in steps {
        if step.is_null() {
            keys.push("");
            continue;
        }
        let step = step.as_object().ok_or_else(invalid)?;
        for name in [
            "status_key",
            "instructions",
            "next_status_key",
            "back_status_key",
        ] {
            if step
                .get(name)
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                return Err(invalid());
            }
        }
        if let Some(handler) = step.get("handler").filter(|handler| !handler.is_null()) {
            let handler = handler.as_object().ok_or_else(invalid)?;
            for name in ["type", "id"] {
                if handler
                    .get(name)
                    .is_some_and(|value| !value.is_null() && !value.is_string())
                {
                    return Err(invalid());
                }
            }
        }
        keys.push(step.get("status_key").and_then(Value::as_str).unwrap_or(""));
    }
    Ok(keys)
}

async fn workflow_allows(
    tx: &Transaction<'_>,
    ws: Uuid,
    project: Option<Uuid>,
    target: &str,
) -> Result<bool, ApiError> {
    let Some(project) = project else {
        return Ok(true);
    };
    let Some(project) = tx
        .query_opt(
            "SELECT workflow_id FROM project WHERE id=$1 AND workspace_id=$2",
            &[&project, &ws],
        )
        .await
        .map_err(db_error)?
    else {
        return Ok(true);
    };
    let id: Option<Uuid> = project.get(0);
    let mut workflow = if let Some(id) = id {
        tx.query_opt(
            "SELECT is_default,steps FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(db_error)?
    } else {
        None
    };
    if workflow.is_none() {
        workflow = tx
            .query_opt(
                "SELECT is_default,steps FROM issue_workflow WHERE workspace_id=$1 AND is_default",
                &[&ws],
            )
            .await
            .map_err(db_error)?;
    }
    let Some(workflow) = workflow else {
        return Ok(true);
    };
    // Decode even default workflows: corrupt stored definitions are not permission.
    let steps: Value = workflow.get("steps");
    let keys = decode_workflow_keys(&steps)?;
    Ok(workflow.get::<_, bool>("is_default") || keys.contains(&target))
}
async fn auto_complete(
    tx: &Transaction<'_>,
    ws: Uuid,
    id: Uuid,
    prs: &[Value],
    settings: &Value,
) -> Result<Value, ApiError> {
    let row = tx
        .query_one(
            "SELECT status,triage_state,project_id FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(db_error)?;
    let status: String = row.get("status");
    let disabled: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM issue_pr_automation WHERE issue_id=$1 AND workspace_id=$2 AND auto_complete_disabled)", &[&id,&ws]).await.map_err(db_error)?.get(0);
    let mut target = target_setting(settings);
    let catalog = if builtin_category(&target).is_none() && target != "none"
        || !prs.is_empty() && builtin_category(&status).is_none()
    {
        tx.query("SELECT key,category,archived_at IS NOT NULL AS archived FROM issue_status WHERE workspace_id=$1", &[&ws]).await.map_err(db_error)?
    } else {
        Vec::new()
    };
    let category = builtin_category(&target).or_else(|| {
        catalog
            .iter()
            .find(|r| r.get::<_, &str>("key") == target && !r.get::<_, bool>("archived"))
            .map(|r| r.get::<_, &str>("category"))
    });
    if target == "blocked" || !matches!(category, Some("started" | "done")) {
        target = "none".into();
    }
    let effective = if builtin_category(&status).is_some() {
        status.as_str()
    } else {
        match catalog
            .iter()
            .find(|r| r.get::<_, &str>("key") == status)
            .map(|r| r.get::<_, &str>("category"))
        {
            Some("done") => "done",
            Some("closed") => "cancelled",
            _ => &status,
        }
    };
    let mut ids = Vec::new();
    let state = if prs.is_empty() {
        "none"
    } else if matches!(effective, "done" | "cancelled") {
        "terminal"
    } else if row.get::<_, Option<String>>("triage_state").is_some() {
        "triage"
    } else if target == "none" {
        "workspace_disabled"
    } else if disabled {
        "issue_disabled"
    } else if !workflow_allows(tx, ws, row.get("project_id"), &target).await? {
        "not_in_workflow"
    } else if status == target {
        "at_target"
    } else {
        let mut sorted: Vec<&Value> = prs.iter().collect();
        sorted.sort_by_key(|v| v["number"].as_i64());
        let open: Vec<_> = sorted
            .iter()
            .filter(|v| matches!(v["state"].as_str(), Some("open" | "draft")))
            .collect();
        let closed: Vec<_> = sorted.iter().filter(|v| v["state"] == "closed").collect();
        if !open.is_empty() {
            ids = open.iter().map(|v| v["id"].clone()).collect();
            "waiting"
        } else if !closed.is_empty() {
            ids = closed.iter().map(|v| v["id"].clone()).collect();
            "not_merged"
        } else {
            ids = sorted.iter().map(|v| v["id"].clone()).collect();
            "all_merged"
        }
    };
    Ok(
        json!({"state":state,"pull_request_ids":ids,"issue_disabled":disabled,"workspace_enabled":target!="none","target_status":target}),
    )
}
// Use the same read-only snapshot for every page: sorting by timestamp alone
// can drop rows when many PRs share a timestamp at a page boundary.
async fn read_pr_pages(
    tx: &Transaction<'_>,
    sql: &str,
    issue: Uuid,
    ws: Uuid,
) -> Result<Vec<Row>, ApiError> {
    let mut rows = Vec::new();
    let mut cursor: Option<(DateTime<Utc>, Uuid)> = None;
    loop {
        let page = tx
            .query(
                sql,
                &[
                    &issue,
                    &ws,
                    &cursor.is_some(),
                    &cursor.as_ref().map(|c| c.0),
                    &cursor.as_ref().map(|c| c.1),
                ],
            )
            .await
            .map_err(db_error)?;
        let size = page.len();
        if let Some(last) = page.last() {
            cursor = Some((last.get("pr_created_at"), last.get("id")));
        }
        rows.extend(page);
        if size < 512 {
            break;
        }
    }
    Ok(rows)
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    if Uuid::parse_str(&raw).is_err()
        && !raw.rsplit_once('-').is_some_and(|(prefix, number)| {
            !prefix.is_empty()
                && !number.is_empty()
                && number.bytes().all(|b| b.is_ascii_digit())
                && number.parse::<i32>().is_ok_and(|n| n > 0)
        })
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list pull requests",
        )
    })?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(db_error)?;
    tx.batch_execute("SET LOCAL statement_timeout=8000")
        .await
        .map_err(db_error)?;
    if tx
        .query_opt(
            "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(db_error)?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let workspace = tx
        .query_one(
            "SELECT name,issue_prefix,settings FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .map_err(db_error)?;
    let number: i32 = tx
        .query_one(
            "SELECT number FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(db_error)?
        .get(0);
    let prefix: String = workspace.get("issue_prefix");
    let identifier = format!(
        "{}-{number}",
        if prefix.is_empty() {
            legacy_prefix(workspace.get("name"))
        } else {
            prefix
        }
    );
    let github = read_pr_pages(
        &tx,
        include_str!("queries_sprint_pull_requests.sql"),
        id,
        ws,
    )
    .await?;
    let vcs = read_pr_pages(
        &tx,
        include_str!("queries_sprint_pull_requests_vcs.sql"),
        id,
        ws,
    )
    .await?;
    let refresh = if github.is_empty() {
        None
    } else {
        Some(
            state
                .pull_request_refresh
                .as_ref()
                .ok_or_else(unavailable)?,
        )
    };
    let enabled = refresh.is_some_and(|r| r.enabled());
    let now = Utc::now();
    let mut out: Vec<Value> = github
        .iter()
        .map(|row| projection(row, true, enabled, now, &identifier))
        .chain(
            vcs.iter()
                .map(|row| projection(row, false, false, now, &identifier)),
        )
        .collect();
    // Frozen Go compares serialized second-precision timestamps, stably retaining
    // provider order (GitHub before VCS) at ties, not global subsecond DB order.
    out.sort_by(|a, b| {
        b["pr_created_at"]
            .as_str()
            .cmp(&a["pr_created_at"].as_str())
    });
    // Keep foreign/provider-mismatched connections out of the cards, but do not
    // mistake a hidden linked PR for an all-merged (or unlinked) issue. The PR
    // row itself must still belong to this workspace; only the connection is
    // invalid. Go's merge decision reads linked states independently of cards.
    let hidden = read_pr_pages(
        &tx,
        include_str!("queries_sprint_pull_requests_hidden.sql"),
        id,
        ws,
    )
    .await?;
    // The decision only needs identity, number and state. Avoid copying every
    // card's checks, author and snapshot payload into a second large JSON tree.
    let linked: Vec<Value> = github
        .iter()
        .chain(vcs.iter())
        .chain(hidden.iter())
        .map(|row| {
            json!({
                "id": row.get::<_, Uuid>("id"),
                "number": row.get::<_, i32>("pr_number"),
                "state": row.get::<_, &str>("state"),
            })
        })
        .collect();
    let decision =
        auto_complete(&tx, ws, id, &linked, &workspace.get::<_, Value>("settings")).await?;
    tx.commit().await.map_err(db_error)?;
    // Admission follows successful reads/commit. No enqueue after failed read.
    if enabled {
        let refresh = refresh.unwrap();
        for row in &github {
            let view = PullRequestView {
                workspace_id: ws,
                issue_id: id,
                pull_request_id: row.get("id"),
                installation_id: row.get("installation_id"),
                repo_owner: row.get("repo_owner"),
                repo_name: row.get("repo_name"),
                number: row.get("pr_number"),
                snapshot_fetched_at: row.get("snapshot_fetched_at"),
                current_snapshot: snapshot_current(row),
            };
            if refresh.maybe_enqueue_on_view(view) == RefreshAdmission::Unavailable {
                return Err(unavailable());
            }
        }
    }
    Ok(GoJson(
        json!({"pull_requests":out,"auto_complete":decision}),
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn workflow_json_null_slice_and_fields_match_frozen_go() {
        use super::{decode_workflow_keys, json};
        assert_eq!(
            decode_workflow_keys(&json!(null)).unwrap(),
            Vec::<&str>::new()
        );
        assert_eq!(
            decode_workflow_keys(&json!([null, {"status_key":null}])).unwrap(),
            ["", ""]
        );
        assert!(
            decode_workflow_keys(&json!([{"status_key":"done", "handler":{"type":false}}]))
                .is_err()
        );
    }
    use super::*;
    #[test]
    fn frozen_go_helpers() {
        for (text, source) in [
            ("fix abc-12", "title"),
            ("abc-123", "auto"),
            ("_ABC-12", "auto"),
            ("ABCDEFGHIJK-12", "auto"),
            ("éABC-12é", "title"),
        ] {
            assert_eq!(link_source("system", "ABC-12", text, ""), source);
        }
        assert_eq!(
            link_source("member", "ABC-12", "ABC-12", "ABC-12"),
            "manual"
        );
        assert_eq!(link_source("system", "ABC-12", "", "feat/ABC-12"), "branch");
        for (settings, target) in [
            (json!({}), "done"),
            (json!({"pr_auto_complete_enabled":false}), "none"),
            (json!({"pr_auto_complete_enabled":1}), "none"),
            (json!({"pr_merge_status":" In_Review "}), "in_review"),
            (json!({"pr_merge_status":false}), "none"),
            (json!({"pr_merge_status":null}), "done"),
        ] {
            assert_eq!(target_setting(&settings), target);
        }
        assert_eq!(conclusion(None, 1, 0, 0), None);
        assert_eq!(conclusion(Some(""), 1, 0, 0), None);
        assert_eq!(conclusion(Some("SUCCESS"), 1, 0, 0), Some("passed"));
        assert_eq!(conclusion(Some("UNKNOWN"), 1, 1, 1), Some("failed"));
        assert_eq!(conclusion(Some("EXPECTED"), 0, 0, 1), Some("pending"));
        for settings in [json!([]), json!(true), json!("invalid")] {
            assert_eq!(target_setting(&settings), "none");
        }
        assert_eq!(target_setting(&Value::Null), "done");
        let now = Utc::now();
        let boundary = now - chrono::Duration::minutes(30);
        assert!(!snapshot_stale("open", Some(boundary), now));
        assert!(snapshot_stale(
            "open",
            Some(boundary - chrono::Duration::nanoseconds(1)),
            now
        ));
        assert!(!snapshot_stale(
            "open",
            Some(now + chrono::Duration::seconds(1)),
            now
        ));
        for (state, fetched_minutes) in [
            ("open", Some(29)),
            ("open", Some(31)),
            ("draft", Some(31)),
            ("merged", Some(31)),
            ("closed", None),
        ] {
            let fetched = fetched_minutes.map(|m| now - chrono::Duration::minutes(m));
            assert_eq!(
                snapshot_stale(state, fetched, now),
                fetched_minutes.is_some() && state != "merged" && fetched_minutes.unwrap() > 30
            );
        }
    }
}
