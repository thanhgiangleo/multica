//! Recover reserved quota after crashes, without touching live dispatch or external providers.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRecoveryFailure {
    pub reservation_id: Uuid,
    pub message: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRecoveryPage {
    pub scanned: usize,
    pub next_after: Option<Uuid>,
    pub settled: Vec<Uuid>,
    pub transitions: Vec<RunTransition>,
    pub failures: Vec<QuotaRecoveryFailure>,
}

impl RunLifecycle {
    /// Scan one workspace in UUID pages; restart at None on the next sweep.
    /// The caller owns cadence and must use an event writer for partial-run failures.
    /// Task/issue dispatch writers must lock the run before linking a side effect.
    /// Creating a run from a reservation must lock that reservation and recheck
    /// its reserved state before insertion, so orphan recovery cannot refund it.
    pub async fn reconcile_quota_page(
        &self,
        workspace_id: Uuid,
        after: Option<Uuid>,
        limit: u16,
    ) -> Result<QuotaRecoveryPage, &'static str> {
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
            .map_err(|_| "quota database unavailable")?;
        let rows = db.query(
            "SELECT q.id FROM autopilot_quota_reservation q \
             LEFT JOIN autopilot_run r ON r.quota_reservation_id=q.id \
             WHERE q.workspace_id=$1 AND q.state='reserved' AND q.id>$2 \
             AND ((q.created_at < now()-interval '10 minutes' \
                   AND (r.id IS NULL OR r.status IN ('completed','failed','skipped'))) \
                  OR (q.created_at < now()-interval '6 hours' \
                      AND r.source IN ('manual','api') \
                      AND r.status IN ('pending','issue_created','running') \
                      AND r.issue_id IS NULL AND r.task_id IS NULL \
                      AND NOT EXISTS (SELECT 1 FROM agent_task_queue t WHERE t.autopilot_run_id=r.id))) \
             ORDER BY q.id LIMIT $3",
            &[&workspace_id, &after.unwrap_or(Uuid::nil()), &i64::from(limit)],
        ).await.map_err(|_| "quota recovery scan failed")?;
        let scanned = rows.len();
        let next_after = if scanned == usize::from(limit) {
            rows.last().map(|row| row.get("id"))
        } else {
            None
        };
        drop(db);
        let mut page = QuotaRecoveryPage {
            scanned,
            next_after,
            settled: Vec::new(),
            transitions: Vec::new(),
            failures: Vec::new(),
        };
        for row in rows {
            let reservation_id: Uuid = row.get("id");
            match self.recover_reservation(workspace_id, reservation_id).await {
                Ok(Some(transition)) => {
                    page.settled.push(reservation_id);
                    if let Some(transition) = transition {
                        page.transitions.push(transition);
                    }
                }
                Ok(None) => {}
                Err(message) => page.failures.push(QuotaRecoveryFailure {
                    reservation_id,
                    message,
                }),
            }
        }
        Ok(page)
    }

    async fn recover_reservation(
        &self,
        workspace_id: Uuid,
        reservation_id: Uuid,
    ) -> Result<Option<Option<RunTransition>>, &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "quota database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "quota transaction failed")?;
        // Same lock order as lifecycle settlement: run, reservation, period.
        let runs = tx
            .query(
                "SELECT r.id,r.autopilot_id,r.source,r.status,r.issue_id,r.task_id,a.workspace_id \
             FROM autopilot_run r LEFT JOIN autopilot a ON a.id=r.autopilot_id \
             WHERE r.quota_reservation_id=$1 FOR UPDATE OF r",
                &[&reservation_id],
            )
            .await
            .map_err(|_| "quota run lookup failed")?;
        if runs.len() > 1
            || runs
                .iter()
                .any(|r| r.get::<_, Option<Uuid>>("workspace_id") != Some(workspace_id))
        {
            return Err("quota reservation run workspace mismatch");
        }
        let reserved = tx
            .query_opt(
                "SELECT period_start,period_end,created_at FROM autopilot_quota_reservation \
             WHERE id=$1 AND workspace_id=$2 AND state='reserved' FOR UPDATE",
                &[&reservation_id, &workspace_id],
            )
            .await
            .map_err(|_| "quota reservation lookup failed")?;
        let Some(reserved) = reserved else {
            return Ok(None);
        };
        // A creator may have committed after the run lookup but before this lock.
        // Do not lock that new run in reverse order; leave it for the next sweep.
        if runs.is_empty()
            && tx
                .query_opt(
                    "SELECT id FROM autopilot_run WHERE quota_reservation_id=$1 LIMIT 1",
                    &[&reservation_id],
                )
                .await
                .map_err(|_| "quota run recheck failed")?
                .is_some()
        {
            return Ok(None);
        }
        let created_at: DateTime<Utc> = reserved.get("created_at");
        let now: DateTime<Utc> = tx
            .query_one("SELECT now()", &[])
            .await
            .map_err(|_| "quota clock unavailable")?
            .get(0);
        let terminal_old = created_at < now - chrono::Duration::minutes(10);
        let partial_old = created_at < now - chrono::Duration::hours(6);
        let mut transition = None;
        let consume = if let Some(run) = runs.first() {
            let status: String = run.get("status");
            if terminal_old && matches!(status.as_str(), "completed" | "failed" | "skipped") {
                status == "completed"
            } else if partial_old
                && matches!(run.get::<_, String>("source").as_str(), "manual" | "api")
                && matches!(status.as_str(), "pending" | "issue_created" | "running")
                && run.get::<_, Option<Uuid>>("issue_id").is_none()
                && run.get::<_, Option<Uuid>>("task_id").is_none()
            {
                let run_id: Uuid = run.get("id");
                let linked_task = tx
                    .query_opt(
                        "SELECT 1 FROM agent_task_queue WHERE autopilot_run_id=$1 LIMIT 1",
                        &[&run_id],
                    )
                    .await
                    .map_err(|_| "quota task lookup failed")?;
                if linked_task.is_some() {
                    return Ok(None);
                }
                let changed = tx.execute(
                    "UPDATE autopilot_run SET status='failed',completed_at=now(), \
                     failure_reason='recovered partial dispatch (crashed before downstream creation)', \
                     reason_code='internal_error',planned_at=NULL \
                     WHERE id=$1 AND status IN ('pending','issue_created','running') \
                     AND issue_id IS NULL AND task_id IS NULL",
                    &[&run_id],
                ).await.map_err(|_| "quota partial run update failed")?;
                if changed != 1 {
                    return Ok(None);
                }
                transition = Some(RunTransition {
                    workspace_id,
                    autopilot_id: run.get("autopilot_id"),
                    run_id,
                    status: "failed",
                });
                false
            } else {
                return Ok(None);
            }
        } else if terminal_old {
            false
        } else {
            return Ok(None);
        };
        let start: DateTime<Utc> = reserved.get("period_start");
        let end: DateTime<Utc> = reserved.get("period_end");
        let changed = tx
            .execute(
                "UPDATE autopilot_quota_period SET reserved_count=reserved_count-1, \
             used_count=used_count+$4,updated_at=now() WHERE workspace_id=$1 \
             AND period_start=$2 AND period_end=$3 AND reserved_count>0",
                &[&workspace_id, &start, &end, &i64::from(consume)],
            )
            .await
            .map_err(|_| "quota period update failed")?;
        if changed != 1 {
            return Err("quota period missing");
        }
        let changed = tx
            .execute(
                "UPDATE autopilot_quota_reservation SET state=$2,finalized_at=now() \
             WHERE id=$1 AND state='reserved'",
                &[
                    &reservation_id,
                    &if consume { "consumed" } else { "released" },
                ],
            )
            .await
            .map_err(|_| "quota reservation update failed")?;
        if changed != 1 {
            return Err("quota reservation changed");
        }
        if let Some(change) = &transition {
            self.events
                .as_ref()
                .ok_or("lifecycle durable event writer required")?
                .enqueue(&tx, change)
                .await?;
        }
        tx.commit().await.map_err(|_| "quota commit failed")?;
        Ok(Some(transition))
    }
}
