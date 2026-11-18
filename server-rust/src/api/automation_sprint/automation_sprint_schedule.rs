//! Durable schedule planning and leases. Runtime admission remains injected.
use super::{automation_sprint_cron::Schedule, *};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

const JOB: &str = "autopilot_schedule_dispatch";
const SCOPE: &str = "autopilot_trigger";

/// Must atomically authorize the current trigger publisher, enforce quota,
/// and admit/resume downstream work idempotently on (trigger_id, planned_at).
/// Partial-run repair and task dispatch belong to the runtime, not this worker.
#[async_trait]
pub trait ScheduleDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, job: ScheduleJob) -> Result<Uuid, super::DispatchFailure>;
}

#[derive(Clone, Debug)]
pub struct ScheduleJob {
    pub workspace_id: Uuid,
    pub autopilot_id: Uuid,
    pub trigger_id: Uuid,
    pub planned_at: DateTime<Utc>,
    pub idempotency_key: String,
}

#[derive(Clone)]
pub struct ScheduleWorker {
    pool: Arc<Pool>,
    dispatcher: Arc<dyn ScheduleDispatcher>,
    runner_id: String,
}

#[derive(Default)]
struct Latest {
    plan: Option<DateTime<Utc>>,
    retry: bool,
}

struct DisplayAdvance {
    trigger: Uuid,
    expression: Option<String>,
    zone: Option<String>,
    next: Option<DateTime<Utc>>,
}

fn plan(
    expression: &str,
    zone: &str,
    created: DateTime<Utc>,
    fired: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    latest: &Latest,
) -> Result<Option<DateTime<Utc>>, &'static str> {
    if latest.retry {
        return Ok(latest.plan);
    }
    let schedule = Schedule::parse(expression, if zone.is_empty() { "UTC" } else { zone })?;
    let mut after = latest.plan.or(fired).unwrap_or(created);
    let floor = now - chrono::Duration::minutes(5);
    if after < floor {
        after = floor - chrono::Duration::nanoseconds(1);
    }
    let mut due = None;
    // Five-field cron has at most six occurrences in the inclusive window.
    for _ in 0..7 {
        let Some(next) = schedule.next(after)? else {
            break;
        };
        if next > now {
            break;
        }
        due = Some(next);
        after = next;
    }
    Ok(due)
}

impl ScheduleWorker {
    pub fn new(pool: Arc<Pool>, dispatcher: Arc<dyn ScheduleDispatcher>) -> Self {
        Self {
            pool,
            dispatcher,
            runner_id: format!("rust-automation-{}", Uuid::new_v4()),
        }
    }

    /// Not started automatically: the integrator must provide real admission.
    /// Dropping an in-flight tick leaves its lease recoverable on restart.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                result = self.tick() => if let Err(error) = result {
                    eprintln!("autopilot schedule: {error}");
                },
                _ = shutdown.changed() => break,
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(30)) => {},
                _ = shutdown.changed() => break,
            }
        }
    }

    /// One DB-clock tick. Planning ignores display-only next_run_at.
    pub async fn tick(&self) -> Result<u64, &'static str> {
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        let now: DateTime<Utc> = db
            .query_one("SELECT now()", &[])
            .await
            .map_err(|_| "schedule clock failed")?
            .get(0);
        db.execute("UPDATE sys_cron_executions SET status='FAILED',finished_at=$2,error_code='stale_timeout',error_msg='lease expired without heartbeat',updated_at=$2 WHERE job_name=$1 AND scope_kind='autopilot_trigger' AND status='RUNNING' AND stale_after<$2", &[&JOB,&now]).await.map_err(|_| "schedule stale sweep failed")?;
        let scopes = db.query(
            "SELECT t.*,a.workspace_id,e.plan_time AS latest_plan,e.status AS latest_status,e.attempt AS latest_attempt,e.max_attempts AS latest_max,e.next_retry_at AS latest_retry FROM autopilot_trigger t JOIN autopilot a ON a.id=t.autopilot_id JOIN workspace w ON w.id=a.workspace_id LEFT JOIN LATERAL (SELECT plan_time,status,attempt,max_attempts,next_retry_at FROM sys_cron_executions WHERE job_name=$1 AND scope_kind=$2 AND scope_id=t.id::text ORDER BY plan_time DESC LIMIT 1) e ON TRUE WHERE t.kind='schedule' AND t.enabled AND a.status='active' AND COALESCE(t.cron_expression,'')<>'' ORDER BY t.id",
            &[&JOB,&SCOPE],
        ).await.map_err(|_| "schedule scope read failed")?;
        drop(db);
        let mut dispatched = 0;
        for row in scopes {
            let latest = Latest {
                plan: row.get("latest_plan"),
                retry: row.get::<_, Option<String>>("latest_status").as_deref() == Some("FAILED")
                    && row
                        .get::<_, Option<i32>>("latest_attempt")
                        .zip(row.get::<_, Option<i32>>("latest_max"))
                        .is_some_and(|(a, m)| a < m)
                    && row
                        .get::<_, Option<DateTime<Utc>>>("latest_retry")
                        .is_none_or(|t| t <= now),
            };
            let expression: Option<String> = row.get("cron_expression");
            let zone: Option<String> = row.get("timezone");
            let planned = plan(
                expression.as_deref().unwrap_or(""),
                zone.as_deref().unwrap_or("UTC"),
                row.get("created_at"),
                row.get("last_fired_at"),
                now,
                &latest,
            );
            let planned = match planned {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                // One corrupt trigger cannot starve every other workspace.
                Err(error) => {
                    eprintln!("autopilot schedule plan: {error}");
                    continue;
                }
            };
            let trigger: Uuid = row.get("id");
            if let Some(lease) = self.claim(trigger, planned, now).await? {
                self.execute(
                    lease,
                    ScheduleJob {
                        workspace_id: row.get("workspace_id"),
                        autopilot_id: row.get("autopilot_id"),
                        trigger_id: trigger,
                        planned_at: planned,
                        idempotency_key: format!(
                            "schedule:{trigger}:{}",
                            planned.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
                        ),
                    },
                )
                .await?;
                dispatched += 1;
            }
        }
        Ok(dispatched)
    }

    async fn claim(
        &self,
        trigger: Uuid,
        planned: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<(Uuid, Uuid)>, &'static str> {
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        let scope = trigger.to_string();
        let row = db.query_opt("INSERT INTO sys_cron_executions(job_name,scope_kind,scope_id,plan_time,status,attempt,max_attempts,runner_id,heartbeat_at,stale_after,started_at,updated_at) VALUES ($1,$2,$3,$4,'RUNNING',1,3,$5,$6,$6::timestamptz+interval '5 minutes',$6,$6) ON CONFLICT ON CONSTRAINT uq_sys_cron_execution DO UPDATE SET status='RUNNING',attempt=sys_cron_executions.attempt+1,runner_id=EXCLUDED.runner_id,lease_token=gen_random_uuid(),heartbeat_at=EXCLUDED.heartbeat_at,stale_after=EXCLUDED.stale_after,started_at=EXCLUDED.started_at,finished_at=NULL,duration_ms=NULL,next_retry_at=NULL,error_code=NULL,error_msg=NULL,updated_at=EXCLUDED.updated_at WHERE sys_cron_executions.attempt<sys_cron_executions.max_attempts AND ((sys_cron_executions.status='FAILED' AND COALESCE(sys_cron_executions.next_retry_at,$6)<=$6) OR (sys_cron_executions.status='RUNNING' AND sys_cron_executions.stale_after<$6)) RETURNING id,lease_token", &[&JOB,&SCOPE,&scope,&planned,&self.runner_id,&now]).await.map_err(|error| { eprintln!("schedule claim: {error:?}"); "schedule claim failed" })?;
        Ok(row.map(|r| (r.get(0), r.get(1))))
    }

    async fn execute(&self, lease: (Uuid, Uuid), job: ScheduleJob) -> Result<(), &'static str> {
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        let state = db.query_opt("SELECT t.cron_expression,t.timezone FROM autopilot_trigger t JOIN autopilot a ON a.id=t.autopilot_id JOIN workspace w ON w.id=a.workspace_id WHERE t.id=$1 AND a.id=$2 AND a.workspace_id=$3 AND t.enabled AND t.kind='schedule' AND a.status='active'", &[&job.trigger_id,&job.autopilot_id,&job.workspace_id]).await.map_err(|_| "schedule state read failed")?;
        let Some(state) = state else {
            drop(db);
            return self
                .finish(lease, None, Some("trigger_inactive"), None)
                .await;
        };
        let expression: Option<String> = state.get(0);
        let zone: Option<String> = state.get(1);
        drop(db);
        // No DB client is held across runtime calls or heartbeat waits.
        let dispatch = self.dispatcher.dispatch(job.clone());
        tokio::pin!(dispatch);
        let deadline = tokio::time::sleep(Duration::from_secs(120));
        tokio::pin!(deadline);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        heartbeat.tick().await;
        let outcome = loop {
            tokio::select! {
                result = &mut dispatch => break result,
                _ = &mut deadline => return self.failure(lease,"dispatch_timeout").await,
                _ = heartbeat.tick() => {
                    let db = self.pool.get().await.map_err(|_| "schedule database unavailable")?;
                    let changed = db.execute("UPDATE sys_cron_executions SET heartbeat_at=now(),stale_after=now()+interval '5 minutes',updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.0,&lease.1]).await.map_err(|_| "schedule heartbeat failed")?;
                    if changed==0 { return Ok(()); }
                }
            }
        };
        let run = match outcome {
            Ok(run) => run,
            Err(super::DispatchFailure::Retry) => {
                return self.failure(lease, "dispatch_error").await;
            }
            Err(super::DispatchFailure::QuotaExceeded) => {
                return self.failure(lease, "quota_exceeded").await;
            }
        };
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        // A fabricated, unrelated or partial run is not a successful dispatch.
        let run = db.query_opt("SELECT status,issue_id,task_id FROM autopilot_run WHERE id=$1 AND autopilot_id=$2 AND trigger_id=$3 AND planned_at=$4 AND source='schedule'", &[&run,&job.autopilot_id,&job.trigger_id,&job.planned_at]).await.map_err(|_| "schedule run verification failed")?.map(|r| (run,r));
        let complete = run.as_ref().is_some_and(|(_, r)| {
            let status: String = r.get("status");
            matches!(status.as_str(), "completed" | "failed" | "skipped")
                || (status == "issue_created" && r.get::<_, Option<Uuid>>("issue_id").is_some())
                || (status == "running" && r.get::<_, Option<Uuid>>("task_id").is_some())
        });
        let now: DateTime<Utc> = db
            .query_one("SELECT now()", &[])
            .await
            .map_err(|_| "schedule clock failed")?
            .get(0);
        drop(db);
        if !complete {
            return self.failure(lease, "incomplete_dispatch").await;
        }
        let next = Schedule::parse(
            expression.as_deref().unwrap_or(""),
            zone.as_deref().filter(|s| !s.is_empty()).unwrap_or("UTC"),
        )
        .ok()
        .and_then(|s| s.next(now.max(job.planned_at)).ok().flatten());
        self.finish(
            lease,
            Some(run.unwrap().0),
            None,
            Some(DisplayAdvance {
                trigger: job.trigger_id,
                expression,
                zone,
                next,
            }),
        )
        .await
    }

    async fn failure(&self, lease: (Uuid, Uuid), code: &str) -> Result<(), &'static str> {
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        db.execute("UPDATE sys_cron_executions SET status='FAILED',finished_at=now(),error_code=$3,error_msg=$3,next_retry_at=CASE WHEN attempt<max_attempts THEN now()+CASE WHEN attempt=1 THEN interval '1 minute' ELSE interval '5 minutes' END ELSE NULL END,duration_ms=LEAST(2147483647,GREATEST(0,EXTRACT(EPOCH FROM (now()-started_at))*1000))::integer,updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.0,&lease.1,&code]).await.map_err(|_| "schedule failure write failed")?;
        Ok(())
    }

    async fn finish(
        &self,
        lease: (Uuid, Uuid),
        run: Option<Uuid>,
        skipped: Option<&str>,
        advance: Option<DisplayAdvance>,
    ) -> Result<(), &'static str> {
        let mut db = self
            .pool
            .get()
            .await
            .map_err(|_| "schedule database unavailable")?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| "schedule transaction failed")?;
        // Lease row is fenced before display writes; lost leases never touch triggers.
        let owned = tx.query_opt("SELECT id FROM sys_cron_executions WHERE id=$1 AND lease_token=$2 AND status='RUNNING' FOR UPDATE", &[&lease.0,&lease.1]).await.map_err(|_| "schedule lease read failed")?.is_some();
        if !owned {
            return Ok(());
        }
        let result = match run {
            Some(id) => json!({"run_id":id}),
            None => json!({"skipped_reason":skipped.unwrap_or("trigger_inactive")}),
        };
        let affected: i64 = i64::from(run.is_some());
        if let Some(DisplayAdvance {
            trigger,
            expression,
            zone,
            next,
        }) = advance
        {
            // A concurrent rule edit owns its new display value; never overwrite it.
            tx.execute("UPDATE autopilot_trigger SET last_fired_at=now(),next_run_at=COALESCE($4,next_run_at) WHERE id=$1 AND cron_expression IS NOT DISTINCT FROM $2 AND timezone IS NOT DISTINCT FROM $3", &[&trigger,&expression,&zone,&next]).await.map_err(|_| "schedule display write failed")?;
        }
        tx.execute("UPDATE sys_cron_executions SET status='SUCCESS',finished_at=now(),rows_affected=$3,result=$4,error_code=NULL,error_msg=NULL,duration_ms=LEAST(2147483647,GREATEST(0,EXTRACT(EPOCH FROM (now()-started_at))*1000))::integer,updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.0,&lease.1,&affected,&result]).await.map_err(|_| "schedule completion failed")?;
        tx.commit().await.map_err(|_| "schedule commit failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    #[test]
    fn automation_sprint_schedule_latest_lateness_restart_and_retry() {
        let now = Utc.with_ymd_and_hms(2026, 11, 2, 14, 0, 0).unwrap();
        let old = now - chrono::Duration::days(90);
        assert_eq!(
            plan("* * * * *", "UTC", old, None, now, &Latest::default()).unwrap(),
            Some(now)
        );
        assert_eq!(
            plan("55 13 * * *", "UTC", old, None, now, &Latest::default()).unwrap(),
            Some(now - chrono::Duration::minutes(5))
        );
        assert_eq!(
            plan("54 13 * * *", "UTC", old, None, now, &Latest::default()).unwrap(),
            None
        );
        assert_eq!(
            plan("* * * * *", "UTC", old, Some(now), now, &Latest::default()).unwrap(),
            None
        );
        assert_eq!(
            plan(
                "* * * * *",
                "UTC",
                old,
                None,
                now,
                &Latest {
                    plan: Some(now),
                    retry: false
                }
            )
            .unwrap(),
            None
        );
        assert_eq!(
            plan(
                "* * * * *",
                "UTC",
                old,
                None,
                now,
                &Latest {
                    plan: Some(old),
                    retry: true
                }
            )
            .unwrap(),
            Some(old)
        );
        assert_eq!(
            plan("* * * * *", "UTC", now, None, now, &Latest::default()).unwrap(),
            None
        );
        assert_eq!(
            plan("0 0 31 2 *", "UTC", old, None, now, &Latest::default()).unwrap(),
            None
        );
        assert!(plan("broken", "UTC", old, None, now, &Latest::default()).is_err());
    }
}
