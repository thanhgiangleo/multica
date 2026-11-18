//! PostgreSQL-backed periodic execution leases and registered jobs.
//! `sys_cron_executions` is shared with Go; its unique plan key and lease token
//! fence concurrent replicas and stale handlers. Never use a local process lock.
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use uuid::Uuid;

pub const PRUNE_JOB: &str = "prune_search_index_changes";
pub use crate::scheduler_issue_wakeup::{ISSUE_WAKEUP_JOB, issue_wakeup_job};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Scope {
    pub kind: String,
    pub id: String,
}
impl Scope {
    pub fn global() -> Self {
        Self {
            kind: "global".into(),
            id: "global".into(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum CatchUp {
    LatestOnly,
    EveryPlan,
}

#[derive(Clone)]
pub struct JobSpec {
    pub name: &'static str,
    pub cadence: Duration,
    pub schedule_delay: Duration,
    pub catch_up: CatchUp,
    pub catch_up_window: Duration,
    pub max_plans_per_tick: usize,
    pub run_timeout: Duration,
    pub stale_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub allow_stale_reentry: bool,
    pub max_attempts: i32,
    pub retry_backoff: Vec<Duration>,
    pub scopes: Vec<Scope>,
    pub handler: Arc<dyn JobHandler>,
}
impl JobSpec {
    fn validate(&self) -> std::result::Result<(), String> {
        if self.name.trim().is_empty()
            || self.cadence.is_zero()
            || self.run_timeout.is_zero()
            || self.stale_timeout <= self.run_timeout
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_interval >= self.stale_timeout
            || self.max_attempts < 1
            || self.scopes.is_empty()
            || matches!(self.catch_up, CatchUp::EveryPlan) && self.max_plans_per_tick == 0
        {
            return Err(format!("invalid scheduler spec {}", self.name));
        }
        Ok(())
    }
    fn retry_delay(&self, attempt: i32) -> Duration {
        self.retry_backoff
            .get((attempt - 1).max(0) as usize)
            .or_else(|| self.retry_backoff.last())
            .copied()
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Lease {
    pub id: Uuid,
    pub token: Uuid,
    pub attempt: i32,
}
#[derive(Debug)]
pub enum Claim {
    Won(Lease),
    Stole(Lease),
    Conflict,
}
#[derive(Debug)]
pub enum SchedulerError {
    Database(tokio_postgres::Error),
    Pool(deadpool_postgres::PoolError),
    LeaseLost,
    Invalid(String),
}
impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(e) => write!(f, "scheduler database: {e}"),
            Self::Pool(e) => write!(f, "scheduler pool: {e}"),
            Self::LeaseLost => write!(f, "scheduler lease lost"),
            Self::Invalid(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for SchedulerError {}
impl From<tokio_postgres::Error> for SchedulerError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Database(e)
    }
}
impl From<deadpool_postgres::PoolError> for SchedulerError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Self::Pool(e)
    }
}
type Result<T> = std::result::Result<T, SchedulerError>;

#[derive(Debug)]
pub struct HandlerResult {
    pub rows_affected: i64,
    pub result: Value,
}
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn handle(
        &self,
        pool: &Pool,
        plan: DateTime<Utc>,
        lease: Lease,
        stale: Duration,
    ) -> std::result::Result<HandlerResult, String>;
}

#[derive(Clone)]
pub struct Manager {
    pool: Arc<Pool>,
    runner: String,
    jobs: HashMap<&'static str, JobSpec>,
    tick_interval: Duration,
}
impl Manager {
    pub fn new(pool: Arc<Pool>) -> Self {
        Self {
            pool,
            runner: Uuid::new_v4().to_string(),
            jobs: HashMap::new(),
            tick_interval: Duration::from_secs(30),
        }
    }
    pub fn register(&mut self, job: JobSpec) -> std::result::Result<(), String> {
        job.validate()?;
        if self.jobs.contains_key(job.name) {
            return Err(format!("duplicate scheduler job {}", job.name));
        }
        self.jobs.insert(job.name, job);
        Ok(())
    }
    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.tick_interval); // first tick immediately
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                _ = ticker.tick() => if let Err(error) = self.run_once().await { eprintln!("scheduler tick: {error}"); },
            }
            if *shutdown.borrow() {
                break;
            }
        }
    }
    pub async fn run_once(&self) -> Result<()> {
        let now = self.db_now().await?;
        for job in self.jobs.values() {
            if let Err(error) = self.run_job(job, now).await {
                eprintln!("scheduler job {}: {error}", job.name);
            }
        }
        Ok(())
    }
    pub async fn db_now(&self) -> Result<DateTime<Utc>> {
        Ok(self
            .pool
            .get()
            .await?
            .query_one("SELECT now()", &[])
            .await?
            .get(0))
    }
    async fn run_job(&self, job: &JobSpec, now: DateTime<Utc>) -> Result<()> {
        // Same unconditional sweep as Go: FAILED with NULL retry is immediately eligible.
        self.mark_stale(job.name, now).await?;
        for scope in &job.scopes {
            let plans = self.plans(job, scope, now).await?;
            for plan in plans {
                match self.try_claim(job, scope, plan, now).await? {
                    Claim::Won(lease) | Claim::Stole(lease) => {
                        self.run_claimed(job, plan, lease).await
                    }
                    Claim::Conflict => (),
                }
            }
        }
        Ok(())
    }
    async fn plans(
        &self,
        job: &JobSpec,
        scope: &Scope,
        now: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>> {
        let cadence = i64::try_from(job.cadence.as_micros())
            .map_err(|_| SchedulerError::Invalid("cadence too large".into()))?;
        let eligible = now - duration(job.schedule_delay)?;
        let latest = DateTime::from_timestamp_micros(
            eligible.timestamp_micros().div_euclid(cadence) * cadence,
        )
        .ok_or_else(|| SchedulerError::Invalid("plan time out of range".into()))?;
        if matches!(job.catch_up, CatchUp::LatestOnly) {
            return Ok(vec![latest]);
        }
        let db = self.pool.get().await?;
        let row = db.query_opt("SELECT plan_time,status,attempt,max_attempts,next_retry_at FROM sys_cron_executions WHERE job_name=$1 AND scope_kind=$2 AND scope_id=$3 ORDER BY plan_time DESC LIMIT 1", &[&job.name, &scope.kind, &scope.id]).await?;
        let start = if let Some(row) = row {
            let plan: DateTime<Utc> = row.get(0);
            let status: String = row.get(1);
            let attempts: i32 = row.get(2);
            let max: i32 = row.get(3);
            let retry: Option<DateTime<Utc>> = row.get(4);
            if status == "FAILED" && attempts < max && retry.is_none_or(|t| t <= now) {
                plan
            } else {
                plan + duration(job.cadence)?
            }
        } else {
            latest
        };
        let oldest = if job.catch_up_window.is_zero() {
            latest
        } else {
            now - duration(job.catch_up_window)?
        };
        let mut start = start;
        if start < oldest {
            start = DateTime::from_timestamp_micros(
                oldest.timestamp_micros().div_euclid(cadence) * cadence,
            )
            .ok_or_else(|| SchedulerError::Invalid("plan time out of range".into()))?;
            if start < oldest {
                start += duration(job.cadence)?;
            }
        }
        let mut plans = vec![];
        while start <= latest && plans.len() < job.max_plans_per_tick {
            plans.push(start);
            start += duration(job.cadence)?;
        }
        Ok(plans)
    }
    pub async fn try_claim(
        &self,
        job: &JobSpec,
        scope: &Scope,
        plan: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Claim> {
        let db = self.pool.get().await?;
        let secs = job.stale_timeout.as_secs().max(1).min(i32::MAX as u64) as i32;
        let id = Uuid::now_v7();
        let row = db.query_opt("INSERT INTO sys_cron_executions(job_name,scope_kind,scope_id,plan_time,status,attempt,max_attempts,runner_id,lease_token,heartbeat_at,stale_after,started_at,updated_at,id) VALUES ($1,$2,$3,$4,'RUNNING',1,$5,$6,gen_random_uuid(),$7,$7::timestamptz+make_interval(secs=>$8::int),$7,$7,$9) ON CONFLICT ON CONSTRAINT uq_sys_cron_execution DO NOTHING RETURNING id,lease_token,attempt", &[&job.name,&scope.kind,&scope.id,&plan,&job.max_attempts,&self.runner,&now,&secs,&id]).await?;
        if let Some(row) = row {
            return Ok(Claim::Won(Lease {
                id: row.get(0),
                token: row.get(1),
                attempt: row.get(2),
            }));
        }
        let row = db.query_opt("UPDATE sys_cron_executions SET status='RUNNING',attempt=attempt+1,runner_id=$1,lease_token=gen_random_uuid(),heartbeat_at=$2,stale_after=$2::timestamptz+make_interval(secs=>$3::int),started_at=$2,finished_at=NULL,duration_ms=NULL,next_retry_at=NULL,error_code=NULL,error_msg=NULL,updated_at=$2 WHERE job_name=$4 AND scope_kind=$5 AND scope_id=$6 AND plan_time=$7 AND attempt<max_attempts AND ((status='FAILED' AND COALESCE(next_retry_at,$2)<=$2) OR (status='RUNNING' AND stale_after<$2 AND $8)) RETURNING id,lease_token,attempt", &[&self.runner,&now,&secs,&job.name,&scope.kind,&scope.id,&plan,&job.allow_stale_reentry]).await?;
        Ok(match row {
            Some(row) => Claim::Stole(Lease {
                id: row.get(0),
                token: row.get(1),
                attempt: row.get(2),
            }),
            None => Claim::Conflict,
        })
    }
    pub async fn mark_stale(&self, name: &str, now: DateTime<Utc>) -> Result<u64> {
        Ok(self.pool.get().await?.execute("UPDATE sys_cron_executions SET status='FAILED',finished_at=$2,error_code='stale_timeout',error_msg='lease expired without heartbeat',updated_at=$2 WHERE job_name=$1 AND status='RUNNING' AND stale_after<$2", &[&name,&now]).await?)
    }
    pub async fn heartbeat(&self, lease: Lease, stale: Duration) -> Result<()> {
        let secs = stale.as_secs().max(1).min(i32::MAX as u64) as i32;
        let affected = self.pool.get().await?.execute("UPDATE sys_cron_executions SET heartbeat_at=now(),stale_after=now()+make_interval(secs=>$3::int),updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.id,&lease.token,&secs]).await?;
        if affected == 0 {
            Err(SchedulerError::LeaseLost)
        } else {
            Ok(())
        }
    }
    pub async fn finish_success(
        &self,
        lease: Lease,
        finished: DateTime<Utc>,
        elapsed: Duration,
        result: HandlerResult,
    ) -> Result<()> {
        let encoded = serde_json::to_string(&result.result)
            .map_err(|e| SchedulerError::Invalid(e.to_string()))?;
        if encoded.len() > 16 * 1024 {
            return Err(SchedulerError::Invalid("result payload too large".into()));
        }
        let affected = self.pool.get().await?.execute("UPDATE sys_cron_executions SET status='SUCCESS',finished_at=$3,duration_ms=$4,rows_affected=$5,result=$6::jsonb,error_code=NULL,error_msg=NULL,updated_at=$3 WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.id,&lease.token,&finished,&millis(elapsed),&result.rows_affected,&result.result]).await?;
        if affected == 0 {
            Err(SchedulerError::LeaseLost)
        } else {
            Ok(())
        }
    }
    pub async fn finish_failure(
        &self,
        lease: Lease,
        finished: DateTime<Utc>,
        elapsed: Duration,
        code: &str,
        message: &str,
        next: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let message: String = message.chars().take(4000).collect();
        let affected = self.pool.get().await?.execute("UPDATE sys_cron_executions SET status='FAILED',finished_at=$3,duration_ms=$4,next_retry_at=$5,error_code=$6,error_msg=$7,updated_at=$3 WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.id,&lease.token,&finished,&millis(elapsed),&next,&code,&message]).await?;
        if affected == 0 {
            Err(SchedulerError::LeaseLost)
        } else {
            Ok(())
        }
    }
    async fn run_claimed(&self, job: &JobSpec, plan: DateTime<Utc>, lease: Lease) {
        let start = std::time::Instant::now();
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let heartbeat = self.clone();
        let stale = job.stale_timeout;
        let interval = job.heartbeat_interval;
        let hb = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = ticker.tick() => {
                        match tokio::time::timeout(Duration::from_secs(5), heartbeat.heartbeat(lease, stale)).await {
                            Ok(Err(SchedulerError::LeaseLost)) => break,
                            Ok(Err(e)) => eprintln!("scheduler heartbeat: {e}"),
                            Err(_) => eprintln!("scheduler heartbeat timed out"),
                            Ok(Ok(())) => (),
                        }
                    }
                }
            }
        });
        let outcome = tokio::time::timeout(
            job.run_timeout,
            job.handler.handle(&self.pool, plan, lease, stale),
        )
        .await;
        let _ = stop_tx.send(true);
        let _ = hb.await;
        let finished = match self.db_now().await {
            Ok(t) => t,
            Err(e) => {
                eprintln!("scheduler terminal clock: {e}");
                Utc::now()
            }
        };
        let elapsed = start.elapsed();
        let terminal = match outcome {
            Ok(Ok(result)) => self.finish_success(lease, finished, elapsed, result).await,
            other => {
                let (code, message) = match other {
                    Err(_) => ("run_timeout", "scheduler handler timed out".to_string()),
                    Ok(Err(msg)) => ("handler_error", msg),
                    Ok(Ok(_)) => unreachable!(),
                };
                let next = if lease.attempt < job.max_attempts {
                    Some(finished + duration(job.retry_delay(lease.attempt)).unwrap_or_default())
                } else {
                    None
                };
                self.finish_failure(lease, finished, elapsed, code, &message, next)
                    .await
            }
        };
        if let Err(e) = terminal {
            eprintln!("scheduler terminal {}: {e}", job.name);
        }
    }
}
fn duration(value: Duration) -> Result<TimeDelta> {
    TimeDelta::from_std(value).map_err(|_| SchedulerError::Invalid("duration out of range".into()))
}
fn millis(value: Duration) -> i32 {
    value.as_millis().min(i32::MAX as u128) as i32
}

struct PruneSearchIndex;
#[async_trait]
impl JobHandler for PruneSearchIndex {
    async fn handle(
        &self,
        pool: &Pool,
        plan: DateTime<Utc>,
        lease: Lease,
        stale: Duration,
    ) -> std::result::Result<HandlerResult, String> {
        let cutoff = plan - TimeDelta::days(30);
        let mut total = 0;
        for _ in 0..200 {
            // Keep SQL identical to Go's PruneSearchIndexChanges: delete and mark
            // the highest removed xid in one statement, skipping locked rows.
            let sql = "WITH doomed AS (SELECT sic.entity_type,sic.entity_id FROM search_index_change sic WHERE sic.changed_at<$1 ORDER BY sic.changed_at LIMIT $2 FOR UPDATE SKIP LOCKED), deleted AS (DELETE FROM search_index_change c USING doomed d WHERE c.entity_type=d.entity_type AND c.entity_id=d.entity_id AND c.changed_at<$1 RETURNING c.change_xid), marked AS (INSERT INTO search_index_prune_mark(singleton,pruned_through_xid) SELECT TRUE,deleted.change_xid FROM deleted ORDER BY deleted.change_xid DESC LIMIT 1 ON CONFLICT(singleton) DO UPDATE SET pruned_through_xid=GREATEST(search_index_prune_mark.pruned_through_xid,EXCLUDED.pruned_through_xid) RETURNING 1) SELECT count(*)::bigint AS deleted_count FROM deleted";
            let db = pool.get().await.map_err(|e| e.to_string())?;
            let count: i64 = db
                .query_one(sql, &[&cutoff, &5000_i64])
                .await
                .map_err(|e| e.to_string())?
                .get(0);
            drop(db);
            total += count;
            if count < 5000 {
                break;
            }
            let secs = stale.as_secs().max(1).min(i32::MAX as u64) as i32;
            let updated = pool.get().await.map_err(|e| e.to_string())?.execute("UPDATE sys_cron_executions SET heartbeat_at=now(),stale_after=now()+make_interval(secs=>$3::int),updated_at=now() WHERE id=$1 AND lease_token=$2 AND status='RUNNING'", &[&lease.id,&lease.token,&secs]).await.map_err(|e| e.to_string())?;
            if updated == 0 {
                return Err("lease lost".into());
            }
        }
        Ok(HandlerResult {
            rows_affected: total,
            result: json!({"cutoff": cutoff.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)}),
        })
    }
}
pub fn prune_search_index_changes() -> JobSpec {
    JobSpec {
        name: PRUNE_JOB,
        cadence: Duration::from_secs(3600),
        schedule_delay: Duration::ZERO,
        catch_up: CatchUp::LatestOnly,
        catch_up_window: Duration::from_secs(86400),
        max_plans_per_tick: 0,
        run_timeout: Duration::from_secs(600),
        stale_timeout: Duration::from_secs(900),
        heartbeat_interval: Duration::from_secs(30),
        allow_stale_reentry: true,
        max_attempts: 3,
        retry_backoff: vec![Duration::from_secs(60), Duration::from_secs(300)],
        scopes: vec![Scope::global()],
        handler: Arc::new(PruneSearchIndex),
    }
}
