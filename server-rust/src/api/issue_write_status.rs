//! Status and duplicate transitions for non-workflow, member/unassigned issues.
//! This module changes issue values, not wakeup definitions or agent config.
use super::super::{ApiError, GoJson, error, issue_write::IssueTaskCancelledEvent};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

const BUILT_IN: [&str; 7] = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "cancelled",
];

fn fail() -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue")
}

pub(super) fn duplicate(
    raw: &Value,
    req: &mut super::Request,
    issue: Uuid,
) -> Result<Option<Uuid>, ApiError> {
    if raw.get("duplicate_of_issue_id").is_none() {
        return Ok(None);
    }
    let target = req.duplicate_of_issue_id.as_deref()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "duplicate_of_issue_id cannot be null; change the status to remove a duplicate mark"))?
        .parse::<Uuid>()
        .ok()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid duplicate_of_issue_id"))?;
    if target == issue {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "an issue cannot be a duplicate of itself",
        ));
    }
    if req.status.as_deref().is_some_and(|s| s != "cancelled") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "a duplicate is cancelled; status must be cancelled or omitted",
        ));
    }
    if req.description.is_some()
        || req.title_base.is_some()
        || req.description_base.is_some()
        || req.attachment_ids.as_ref().is_some_and(|v| !v.is_empty())
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "duplicate_of_issue_id cannot be combined with description or attachment changes",
        ));
    }
    req.status = Some("cancelled".to_owned());
    Ok(Some(target))
}

pub(in crate::api) async fn preflight(
    db: &Client,
    ws: Uuid,
    status: Option<&str>,
) -> Result<(), Response> {
    let Some(status) = status else {
        return Ok(());
    };
    if BUILT_IN.contains(&status) {
        return Ok(());
    }
    let row = db
        .query_opt(
            "SELECT archived_at FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&ws, &status],
        )
        .await
        .map_err(|_| fail().into_response())?;
    if row.is_some_and(|r| {
        r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
            .is_none()
    }) {
        return Ok(());
    }
    let rows = db.query(
        "SELECT key,name,is_system FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END,position,CASE WHEN is_system THEN 0 ELSE 1 END,CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END,key", &[&ws]
    ).await.map_err(|_| fail().into_response())?;
    let mut seen = HashSet::new();
    let mut allowed: Vec<String> = rows
        .iter()
        .map(|row| {
            let key: String = row.get("key");
            let name: String = row.get("name");
            seen.insert(key.clone());
            if row.get::<_, bool>("is_system") || name.trim().is_empty() {
                key
            } else {
                format!("{key} ({name})")
            }
        })
        .collect();
    for key in BUILT_IN {
        if !seen.contains(key) {
            allowed.push(key.to_owned());
        }
    }
    Err((StatusCode::BAD_REQUEST, GoJson(json!({"error":format!("invalid status {status:?}; valid values: {}", allowed.join(", "))}))).into_response())
}

/// A project move can choose a custom status only after reading the locked
/// issue/workflow. Reserve its catalog before owner rows to keep lock order.
pub(in crate::api) async fn lock_namespace(tx: &Transaction<'_>, ws: Uuid) -> Result<(), ApiError> {
    tx.execute(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| fail())?;
    Ok(())
}

/// Lock custom catalog before attachment/issue rows, as frozen Go does.
pub(in crate::api) async fn lock_catalog(
    tx: &Transaction<'_>,
    ws: Uuid,
    status: Option<&str>,
) -> Result<(), ApiError> {
    let Some(status) = status.filter(|s| !BUILT_IN.contains(s)) else {
        return Ok(());
    };
    lock_namespace(tx, ws).await?;
    let row = tx
        .query_opt(
            "SELECT archived_at FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&ws, &status],
        )
        .await
        .map_err(|_| fail())?;
    if row.is_none_or(|r| {
        r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
            .is_some()
    }) {
        return Err(error(
            StatusCode::CONFLICT,
            "the target status was archived while this request was in flight; reload the status list and retry",
        ));
    }
    Ok(())
}

pub(in crate::api) async fn check_duplicate(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    target: Uuid,
) -> Result<(), Response> {
    // Must precede locking the owner alone. Sorted row locks also serialize
    // inverse marks with frozen Go's LockIssuesForDuplicateMark path.
    let rows = tx.query("SELECT i.id,(i.status='cancelled' AND EXISTS(SELECT 1 FROM issue original WHERE original.id=i.duplicate_of_issue_id AND original.workspace_id=i.workspace_id)) AS is_duplicate FROM issue i WHERE i.workspace_id=$1 AND i.id=ANY($2::uuid[]) ORDER BY i.id FOR UPDATE OF i", &[&ws, &vec![issue,target]]).await.map_err(|_| fail().into_response())?;
    let row = rows
        .iter()
        .find(|r| r.get::<_, Uuid>("id") == target)
        .ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "duplicate target not found in this workspace",
            )
            .into_response()
        })?;
    if row.get::<_, bool>("is_duplicate") {
        return Err((StatusCode::CONFLICT,GoJson(json!({"code":"duplicate_target_is_duplicate","error":"the target issue is itself a duplicate; mark this issue as a duplicate of its original instead"}))).into_response());
    }
    let has: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM issue WHERE workspace_id=$1 AND duplicate_of_issue_id=$2 AND status='cancelled')", &[&ws,&issue]).await.map_err(|_| fail().into_response())?.get(0);
    if has {
        return Err((StatusCode::CONFLICT,GoJson(json!({"code":"issue_has_duplicates","error":"other issues are marked as duplicates of this issue; remove those marks first"}))).into_response());
    }
    Ok(())
}

pub(in crate::api) fn dispatch_supported(
    current: &Row,
    assignee: Option<&str>,
) -> Result<(), ApiError> {
    if matches!(
        current.get::<_, Option<String>>("assignee_type").as_deref(),
        Some("agent" | "squad")
    ) || matches!(assignee, Some("agent" | "squad"))
    {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue status dispatch runtime unavailable",
        ));
    }
    Ok(())
}

/// Frozen StopClosedIssueWakeups, part of the issue status transaction. System
/// rules only rest while closed; running/dispatched/started tasks are not stopped.
pub(in crate::api) async fn stop_closed(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    status: Option<&str>,
) -> Result<Vec<IssueTaskCancelledEvent>, ApiError> {
    let Some(status) = status else {
        return Ok(Vec::new());
    };
    let closed = if BUILT_IN.contains(&status) {
        matches!(status, "done" | "cancelled")
    } else {
        tx.query_opt(
            "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&ws, &status],
        )
        .await
        .map_err(|_| fail())?
        .is_some_and(|r| matches!(r.get::<_, String>(0).as_str(), "done" | "closed"))
    };
    if !closed {
        return Ok(Vec::new());
    }
    tx.execute("UPDATE issue_wakeup SET enabled=false,disabled_at=clock_timestamp(),updated_at=clock_timestamp() WHERE issue_id=$1 AND workspace_id=$2 AND disabled_at IS NULL AND system_rule IS NULL", &[&issue,&ws]).await.map_err(|_| fail())?;
    let rows = tx.query("UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error='Issue closed; wakeup disabled' WHERE issue_id=$1 AND context->>'wakeup_id' IS NOT NULL AND status IN ('queued','deferred') AND started_at IS NULL RETURNING id,agent_id,issue_id", &[&issue]).await.map_err(|_| fail())?;
    Ok(rows
        .iter()
        .map(|r| IssueTaskCancelledEvent {
            workspace_id: ws,
            task_id: r.get("id"),
            agent_id: r.get("agent_id"),
            issue_id: r.get("issue_id"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_presence_and_conflicts() {
        let issue = Uuid::new_v4();
        let target = Uuid::new_v4();
        let mut req = super::super::Request::default();
        assert_eq!(duplicate(&json!({}), &mut req, issue).unwrap(), None);
        req.duplicate_of_issue_id = Some(target.to_string());
        // Presence uses exact raw key, but value uses Go's ordered/folded struct.
        assert_eq!(
            duplicate(&json!({"duplicate_of_issue_id":null}), &mut req, issue).unwrap(),
            Some(target)
        );
        assert_eq!(req.status.as_deref(), Some("cancelled"));
        assert_eq!(
            duplicate(&json!({"DUPLICATE_OF_ISSUE_ID":target}), &mut req, issue).unwrap(),
            None
        );
        for value in [None, Some("bad".into()), Some(issue.to_string())] {
            req.duplicate_of_issue_id = value;
            assert!(duplicate(&json!({"duplicate_of_issue_id":target}), &mut req, issue).is_err());
        }
        req.duplicate_of_issue_id = Some(target.to_string());
        req.status = Some("done".into());
        assert!(duplicate(&json!({"duplicate_of_issue_id":target}), &mut req, issue).is_err());
        for flags in [
            (true, false, false, false),
            (false, true, false, false),
            (false, false, true, false),
            (false, false, false, true),
        ] {
            req.status = None;
            req.description = flags.0.then(String::new);
            req.title_base = flags.1.then(String::new);
            req.description_base = flags.2.then(String::new);
            req.attachment_ids = flags.3.then(|| vec![target.to_string()]);
            assert!(duplicate(&json!({"duplicate_of_issue_id":target}), &mut req, issue).is_err());
        }
    }
}
