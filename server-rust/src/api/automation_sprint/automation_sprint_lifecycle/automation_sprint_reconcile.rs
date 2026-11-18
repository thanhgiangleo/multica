//! Bounded recovery of missed lifecycle callbacks from authoritative rows.
//! The caller schedules full sweeps; no runtime service is started here.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileFailure {
    pub run_id: Uuid,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilePage {
    pub scanned: usize,
    pub next_after: Option<Uuid>,
    pub transitions: Vec<RunTransition>,
    pub failures: Vec<ReconcileFailure>,
}

impl RunLifecycle {
    /// Sweep at most 100 open runs in one workspace. Restart with `None` after
    /// `next_after` is empty; UUID ordering is pagination, not execution order.
    /// Per-run errors do not starve later runs, and the next sweep retries them.
    /// Requires a transactional event writer before it can change any run.
    pub async fn reconcile_page(
        &self,
        workspace_id: Uuid,
        after: Option<Uuid>,
        limit: u16,
    ) -> Result<ReconcilePage, &'static str> {
        if self.events.is_none() {
            return Err("lifecycle durable event writer required");
        }
        if !(1..=100).contains(&limit) {
            return Err("lifecycle page limit out of range");
        }
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "lifecycle database unavailable")?;
        let rows = db
            .query(
                "SELECT r.id,r.task_id,r.issue_id,a.execution_mode FROM autopilot_run r \
                 JOIN autopilot a ON a.id=r.autopilot_id WHERE a.workspace_id=$1 \
                 AND r.status IN ('issue_created','running') AND ($2::uuid IS NULL OR r.id>$2) \
                 ORDER BY r.id LIMIT $3",
                &[&workspace_id, &after, &i64::from(limit)],
            )
            .await
            .map_err(|_| "lifecycle recovery scan failed")?;
        let scanned = rows.len();
        let next_after = if scanned == usize::from(limit) {
            rows.last().map(|row| row.get("id"))
        } else {
            None
        };
        // Drop the scan connection before invoking hooks. A one-slot pool can
        // still process pages, and no shared snapshot lock spans the sweep.
        drop(db);
        let mut transitions = Vec::new();
        let mut failures = Vec::new();
        for row in rows {
            let run_id: Uuid = row.get("id");
            let result = match row.get::<_, String>("execution_mode").as_str() {
                "run_only" => match row.get::<_, Option<Uuid>>("task_id") {
                    Some(task_id) => self
                        .task_terminal_for_run(workspace_id, task_id, Some(run_id))
                        .await
                        .map(|change| change.into_iter().collect()),
                    None => Ok(Vec::new()),
                },
                "create_issue" => match row.get::<_, Option<Uuid>>("issue_id") {
                    Some(issue_id) => {
                        self.sync_issue(workspace_id, issue_id, false, Some(run_id))
                            .await
                    }
                    None => Ok(Vec::new()),
                },
                _ => Ok(Vec::new()),
            };
            match result {
                Ok(changes) => transitions.extend(changes),
                Err(message) => failures.push(ReconcileFailure { run_id, message }),
            }
        }
        Ok(ReconcilePage {
            scanned,
            next_after,
            transitions,
            failures,
        })
    }
}
