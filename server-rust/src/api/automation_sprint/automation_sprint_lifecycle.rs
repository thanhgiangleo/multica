//! Internal lifecycle hooks. Call only after the task/issue writer commits;
//! the workspace and terminal state are re-read from PostgreSQL, not trusted from an event.
mod automation_sprint_issue_lifecycle;
mod automation_sprint_quota_recovery;
mod automation_sprint_reconcile;
pub use automation_sprint_quota_recovery::{QuotaRecoveryFailure, QuotaRecoveryPage};
pub use automation_sprint_reconcile::{ReconcileFailure, ReconcilePage};

use super::*;
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTransition {
    pub workspace_id: Uuid,
    pub autopilot_id: Uuid,
    pub run_id: Uuid,
    pub status: &'static str,
}

/// The runtime owns the durable event transport. Persist the transition in
/// this transaction; a post-commit publish alone loses events on process death.
#[async_trait]
pub trait DurableLifecycleEvents: Send + Sync {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        transition: &RunTransition,
    ) -> Result<(), &'static str>;
}

#[derive(Clone)]
pub struct RunLifecycle {
    pool: Arc<Pool>,
    events: Option<Arc<dyn DurableLifecycleEvents>>,
}

impl RunLifecycle {
    /// Hooks without an event writer return transitions to the caller, but do
    /// not guarantee delivery. Recovery refuses to run until the runtime
    /// supplies a transactional event writer.
    pub fn new(pool: Arc<Pool>) -> Self {
        Self { pool, events: None }
    }

    /// The writer enqueues in the same transaction as status/quota settlement.
    /// Returned transitions are observations, not a second publish instruction.
    pub fn with_events(pool: Arc<Pool>, events: Arc<dyn DurableLifecycleEvents>) -> Self {
        Self {
            pool,
            events: Some(events),
        }
    }

    /// Settle a run_only run from its own terminal task. No task writes happen here.
    /// A transition is returned only by the winning terminal CAS after commit.
    /// When configured, the durable event writer participates before commit.
    pub async fn task_terminal(
        &self,
        workspace_id: Uuid,
        task_id: Uuid,
    ) -> Result<Option<RunTransition>, &'static str> {
        self.task_terminal_for_run(workspace_id, task_id, None)
            .await
    }

    async fn task_terminal_for_run(
        &self,
        workspace_id: Uuid,
        task_id: Uuid,
        run_id: Option<Uuid>,
    ) -> Result<Option<RunTransition>, &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "lifecycle database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "lifecycle transaction failed")?;
        let row = tx.query_opt(
            "SELECT r.id,r.autopilot_id,r.quota_reservation_id,q.status,q.result,q.error,q.failure_reason \
             FROM agent_task_queue q JOIN autopilot_run r ON r.id=q.autopilot_run_id \
             JOIN autopilot a ON a.id=r.autopilot_id \
             WHERE q.id=$1 AND r.task_id=q.id AND a.workspace_id=$2 AND a.execution_mode='run_only' \
               AND q.status IN ('completed','failed','cancelled') \
               AND ($3::uuid IS NULL OR r.id=$3) FOR UPDATE OF r",
            &[&task_id, &workspace_id, &run_id],
        ).await.map_err(|_| "lifecycle task lookup failed")?;
        let Some(row) = row else { return Ok(None) };
        let status: String = row.get("status");
        let reason = if status == "completed" {
            None
        } else {
            Some(task_failure_reason(&row))
        };
        let result: Option<Value> = if status == "completed" {
            row.get("result")
        } else {
            None
        };
        let transition = settle(
            &tx,
            workspace_id,
            &row,
            status == "completed",
            reason.as_deref(),
            result.as_ref(),
            self.events.as_deref(),
        )
        .await?;
        tx.commit().await.map_err(|_| "lifecycle commit failed")?;
        Ok(transition)
    }

    /// Call inside the authorized issue deletion transaction, before clearing links.
    /// This hook never deletes issues/tasks. Its savepoint prevents a caller from
    /// committing partial settlement after a rejected event. Outer rollback also
    /// rolls back status, quota and events; returned changes are not committed yet.
    pub async fn issue_deleting_in_tx(
        &self,
        tx: &mut Transaction<'_>,
        workspace_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Vec<RunTransition>, &'static str> {
        let events = self
            .events
            .as_deref()
            .ok_or("lifecycle durable event writer required")?;
        let savepoint = tx
            .transaction()
            .await
            .map_err(|_| "lifecycle savepoint failed")?;
        let result = async {
            let issue = savepoint
                .query_opt(
                    "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
                    &[&issue_id, &workspace_id],
                )
                .await
                .map_err(|_| "lifecycle issue lookup failed")?;
            if issue.is_none() {
                return Err("lifecycle issue missing");
            }
            let runs = savepoint
                .query(
                    "SELECT r.id,r.autopilot_id,r.quota_reservation_id FROM autopilot_run r \
             JOIN autopilot a ON a.id=r.autopilot_id WHERE r.issue_id=$1 AND a.workspace_id=$2 \
             AND r.status IN ('issue_created','running') ORDER BY r.id FOR UPDATE OF r",
                    &[&issue_id, &workspace_id],
                )
                .await
                .map_err(|_| "lifecycle run lookup failed")?;
            let mut changes = Vec::new();
            for run in runs {
                if let Some(change) = settle(
                    &savepoint,
                    workspace_id,
                    &run,
                    false,
                    Some("linked issue was deleted"),
                    None,
                    Some(events),
                )
                .await?
                {
                    changes.push(change);
                }
            }
            Ok::<_, &'static str>(changes)
        }
        .await;
        match result {
            Ok(changes) => {
                savepoint
                    .commit()
                    .await
                    .map_err(|_| "lifecycle savepoint commit failed")?;
                Ok(changes)
            }
            Err(message) => {
                savepoint
                    .rollback()
                    .await
                    .map_err(|_| "lifecycle savepoint rollback failed")?;
                Err(message)
            }
        }
    }

    /// Called after issue cancellation. A later issue status cannot reopen or
    /// flip a run already settled by its own task.
    pub async fn issue_cancelled(
        &self,
        workspace_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Vec<RunTransition>, &'static str> {
        self.sync_issue(workspace_id, issue_id, true, None).await
    }
}

fn task_failure_reason(task: &Row) -> String {
    task.get::<_, Option<String>>("error")
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            task.get::<_, Option<String>>("failure_reason")
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| "task failed".to_owned())
}

async fn settle(
    tx: &Transaction<'_>,
    workspace_id: Uuid,
    run: &Row,
    completed: bool,
    reason: Option<&str>,
    result: Option<&Value>,
    events: Option<&dyn DurableLifecycleEvents>,
) -> Result<Option<RunTransition>, &'static str> {
    let run_id: Uuid = run.get("id");
    let autopilot_id: Uuid = run.get("autopilot_id");
    let reservation_id: Option<Uuid> = run.get("quota_reservation_id");
    let status = if completed { "completed" } else { "failed" };
    let updated = tx
        .execute(
            "UPDATE autopilot_run SET status=$2,completed_at=now(), \
         result=CASE WHEN $2='completed' THEN $4 ELSE result END, \
         failure_reason=CASE WHEN $2='failed' THEN $3 ELSE failure_reason END \
         WHERE id=$1 AND autopilot_id=$5 AND status IN ('issue_created','running')",
            &[&run_id, &status, &reason, &result, &autopilot_id],
        )
        .await
        .map_err(|_| "lifecycle run update failed")?;
    if updated == 0 {
        return Ok(None);
    }
    if let Some(reservation_id) = reservation_id {
        let reserved = tx.query_opt(
            "SELECT workspace_id,period_start,period_end,state FROM autopilot_quota_reservation \
             WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&reservation_id, &workspace_id],
        ).await.map_err(|_| "lifecycle reservation lookup failed")?
            .ok_or("lifecycle reservation missing")?;
        if reserved.get::<_, String>("state") == "reserved" {
            let start: DateTime<Utc> = reserved.get("period_start");
            let end: DateTime<Utc> = reserved.get("period_end");
            let affected = tx
                .execute(
                    "UPDATE autopilot_quota_period SET reserved_count=reserved_count-1, \
                 used_count=used_count+$4,updated_at=now() WHERE workspace_id=$1 \
                 AND period_start=$2 AND period_end=$3 AND reserved_count>0",
                    &[&workspace_id, &start, &end, &i64::from(completed)],
                )
                .await
                .map_err(|_| "lifecycle quota update failed")?;
            if affected != 1 {
                return Err("lifecycle quota period missing");
            }
            tx.execute(
                "UPDATE autopilot_quota_reservation SET state=$2,finalized_at=now() WHERE id=$1 AND state='reserved'",
                &[&reservation_id, &if completed { "consumed" } else { "released" }],
            ).await.map_err(|_| "lifecycle reservation update failed")?;
        }
    }
    let transition = RunTransition {
        workspace_id,
        autopilot_id,
        run_id,
        status,
    };
    if let Some(events) = events {
        events.enqueue(tx, &transition).await?;
    }
    Ok(Some(transition))
}
