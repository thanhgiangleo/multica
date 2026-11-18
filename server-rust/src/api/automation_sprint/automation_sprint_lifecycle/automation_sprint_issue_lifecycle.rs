//! create_issue settlement mirrors frozen Go's dispatch root + automatic retry lineage.
//! Hooks observe committed writers; they never enqueue, cancel, or mutate issue/task rows.
use super::*;
use deadpool_postgres::Transaction;
use std::collections::HashSet;
use tokio_postgres::Row;
use uuid::Uuid;

impl RunLifecycle {
    /// Observe a committed task outcome for a create_issue run. Mention/comment
    /// tasks and manual reruns outside the recorded retry chain do not settle it.
    /// Failed attempts wait while any attempt in that chain is still active.
    pub async fn linked_issue_task_terminal(
        &self,
        workspace_id: Uuid,
        task_id: Uuid,
    ) -> Result<Vec<RunTransition>, &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "lifecycle database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "lifecycle transaction failed")?;
        let task = tx
            .query_opt(
                "SELECT issue_id FROM agent_task_queue WHERE id=$1 AND autopilot_run_id IS NULL \
             AND issue_id IS NOT NULL AND status IN ('completed','failed')",
                &[&task_id],
            )
            .await
            .map_err(|_| "lifecycle task lookup failed")?;
        let Some(task) = task else {
            return Ok(Vec::new());
        };
        let issue_id: Uuid = task.get("issue_id");
        let Some(issue) = linked_issue(&tx, workspace_id, issue_id).await? else {
            return Ok(Vec::new());
        };
        let runs = linked_runs(&tx, workspace_id, issue_id).await?;
        let mut changes = Vec::new();
        for run in runs {
            let chain = dispatch_chain(&tx, &run, &issue, issue_id).await?;
            let Some(task) = chain.iter().find(|t| t.get::<_, Uuid>("id") == task_id) else {
                continue;
            };
            // Frozen Go waits while any attempt in the chain is still active: a
            // non-retryable failure settles at once, a retryable one only once its
            // auto-retry chain is exhausted. The callback attempt is the one that
            // settles, exactly as the reference does.
            if task.get::<_, String>("status") == "failed" && chain_active(&chain) {
                continue;
            }
            if let Some(change) =
                settle_dispatch_task(&tx, workspace_id, &run, task, self.events.as_deref()).await?
            {
                changes.push(change);
            }
        }
        tx.commit().await.map_err(|_| "lifecycle commit failed")?;
        Ok(changes)
    }

    /// Observe the current issue status, not an event payload. Delivery waits for
    /// an active/pending agent dispatch, and a committed task failure wins over an
    /// early done/in_review. Custom statuses only map terminal done/closed categories.
    pub async fn issue_changed(
        &self,
        workspace_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Vec<RunTransition>, &'static str> {
        self.sync_issue(workspace_id, issue_id, false, None).await
    }

    pub(super) async fn sync_issue(
        &self,
        workspace_id: Uuid,
        issue_id: Uuid,
        cancellation_only: bool,
        recovery_run: Option<Uuid>,
    ) -> Result<Vec<RunTransition>, &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "lifecycle database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "lifecycle transaction failed")?;
        let Some(issue) = linked_issue(&tx, workspace_id, issue_id).await? else {
            return Ok(Vec::new());
        };
        let status: String = issue.get("status");
        let outcome = issue_outcome(&tx, workspace_id, &status).await?;
        if outcome == IssueOutcome::Ignore && recovery_run.is_none()
            || cancellation_only && outcome != IssueOutcome::Cancel
        {
            return Ok(Vec::new());
        }
        let runs = linked_runs_for_recovery(&tx, workspace_id, issue_id, recovery_run).await?;
        let mut changes = Vec::new();
        for run in runs {
            let change = if outcome == IssueOutcome::Cancel {
                settle(
                    &tx,
                    workspace_id,
                    &run,
                    false,
                    Some(&format!("issue {status}")),
                    None,
                    self.events.as_deref(),
                )
                .await?
            } else {
                let chain = dispatch_chain(&tx, &run, &issue, issue_id).await?;
                if chain_active(&chain) {
                    continue;
                }
                if let Some(task) = chain.last().filter(|t| {
                    matches!(
                        t.get::<_, String>("status").as_str(),
                        "completed" | "failed"
                    )
                }) {
                    settle_dispatch_task(&tx, workspace_id, &run, task, self.events.as_deref())
                        .await?
                } else {
                    if outcome == IssueOutcome::Ignore {
                        continue;
                    }
                    let assigned = issue.get::<_, Option<String>>("assignee_type");
                    if chain.is_empty() && matches!(assigned.as_deref(), Some("agent" | "squad")) {
                        continue;
                    }
                    settle(
                        &tx,
                        workspace_id,
                        &run,
                        true,
                        None,
                        None,
                        self.events.as_deref(),
                    )
                    .await?
                }
            };
            if let Some(change) = change {
                changes.push(change);
            }
        }
        tx.commit().await.map_err(|_| "lifecycle commit failed")?;
        Ok(changes)
    }
}

// All create_issue hooks lock issue then runs in the same order. Task/issue
// writers must commit before calling; automatic retries must be inserted before
// announcing failure, as in frozen Go. This is not a shared dispatch admission lock.
async fn linked_issue(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    issue_id: Uuid,
) -> Result<Option<Row>, &'static str> {
    tx.query_opt(
        "SELECT status,creator_type,creator_id,assignee_type FROM issue \
         WHERE id=$1 AND workspace_id=$2 AND origin_type='autopilot' FOR SHARE",
        &[&issue_id, &workspace_id],
    )
    .await
    .map_err(|_| "lifecycle issue lookup failed")
}

async fn linked_runs(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    issue_id: Uuid,
) -> Result<Vec<Row>, &'static str> {
    linked_runs_for_recovery(tx, workspace_id, issue_id, None).await
}

async fn linked_runs_for_recovery(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    issue_id: Uuid,
    run_id: Option<Uuid>,
) -> Result<Vec<Row>, &'static str> {
    tx.query(
        "SELECT r.id,r.autopilot_id,r.quota_reservation_id,r.task_id FROM autopilot_run r \
         JOIN autopilot a ON a.id=r.autopilot_id WHERE r.issue_id=$1 AND a.workspace_id=$2 \
         AND a.execution_mode='create_issue' AND r.status IN ('issue_created','running') \
         AND ($3::uuid IS NULL OR r.id=$3) ORDER BY r.id FOR UPDATE OF r",
        &[&issue_id, &workspace_id, &run_id],
    )
    .await
    .map_err(|_| "lifecycle run lookup failed")
}

async fn dispatch_chain(
    tx: &Transaction<'_>,
    run: &Row,
    issue: &Row,
    issue_id: Uuid,
) -> Result<Vec<Row>, &'static str> {
    let tasks = tx
        .query(
            "SELECT id,agent_id,retry_of_task_id,status,error,failure_reason FROM agent_task_queue \
         WHERE issue_id=$1 ORDER BY created_at,id",
            &[&issue_id],
        )
        .await
        .map_err(|_| "lifecycle dispatch chain lookup failed")?;
    let mut root: Option<Uuid> = run.get("task_id");
    if root.is_none() && issue.get::<_, String>("creator_type") == "agent" {
        let creator: Uuid = issue.get("creator_id");
        root = tasks
            .iter()
            .find(|t| {
                t.get::<_, Option<Uuid>>("retry_of_task_id").is_none()
                    && t.get::<_, Uuid>("agent_id") == creator
            })
            .map(|t| t.get("id"));
        if let Some(root) = root {
            let run_id: Uuid = run.get("id");
            tx.execute(
                "UPDATE autopilot_run SET task_id=$2 WHERE id=$1 AND task_id IS NULL",
                &[&run_id, &root],
            )
            .await
            .map_err(|_| "lifecycle dispatch root update failed")?;
        }
    }
    let Some(root) = root else {
        return Ok(Vec::new());
    };
    let mut seen = HashSet::new();
    Ok(tasks
        .into_iter()
        .filter(|t| {
            let id: Uuid = t.get("id");
            let parent: Option<Uuid> = t.get("retry_of_task_id");
            if id == root || parent.is_some_and(|p| seen.contains(&p)) {
                seen.insert(id);
                true
            } else {
                false
            }
        })
        .collect())
}

fn chain_active(chain: &[Row]) -> bool {
    chain.iter().any(|t| {
        !matches!(
            t.get::<_, String>("status").as_str(),
            "completed" | "failed" | "cancelled"
        )
    })
}

async fn settle_dispatch_task(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    run: &Row,
    task: &Row,
    events: Option<&dyn DurableLifecycleEvents>,
) -> Result<Option<RunTransition>, &'static str> {
    let status: String = task.get("status");
    if !matches!(status.as_str(), "completed" | "failed") {
        return Ok(None);
    }
    let reason = (status == "failed").then(|| task_failure_reason(task));
    // create_issue exposes the linked issue, never task output.
    settle(
        tx,
        workspace_id,
        run,
        status == "completed",
        reason.as_deref(),
        None,
        events,
    )
    .await
}

#[derive(PartialEq, Eq)]
enum IssueOutcome {
    Ignore,
    Deliver,
    Cancel,
}

async fn issue_outcome(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    status: &str,
) -> Result<IssueOutcome, &'static str> {
    Ok(match status {
        "done" | "in_review" => IssueOutcome::Deliver,
        "cancelled" => IssueOutcome::Cancel,
        "backlog" | "todo" | "in_progress" | "blocked" => IssueOutcome::Ignore,
        _ => {
            let row = tx
                .query_opt(
                    "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
                    &[&workspace_id, &status],
                )
                .await
                .map_err(|_| "lifecycle issue status lookup failed")?;
            match row
                .as_ref()
                .map(|r| r.get::<_, String>("category"))
                .as_deref()
            {
                Some("done") => IssueOutcome::Deliver,
                Some("closed") => IssueOutcome::Cancel,
                _ => IssueOutcome::Ignore,
            }
        }
    })
}
