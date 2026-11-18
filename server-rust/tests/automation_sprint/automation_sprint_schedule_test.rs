//! Synthetic scheduler integration; no task/provider or real agent is invoked.
use super::*;
use multica_server::api::{DispatchFailure, ScheduleDispatcher, ScheduleJob, ScheduleWorker};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Mode {
    Skipped,
    Bogus,
    Partial,
    Steal,
    Quota,
}
struct FakeSchedule {
    pool: Arc<Pool>,
    calls: Arc<AtomicUsize>,
    mode: Mode,
}
#[async_trait::async_trait]
impl ScheduleDispatcher for FakeSchedule {
    async fn dispatch(&self, job: ScheduleJob) -> Result<Uuid, DispatchFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            job.idempotency_key,
            format!(
                "schedule:{}:{}",
                job.trigger_id,
                job.planned_at
                    .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
            )
        );
        let db = self.pool.get().await.unwrap();
        assert_eq!(
            db.query_one(
                "SELECT workspace_id FROM autopilot WHERE id=$1",
                &[&job.autopilot_id]
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
            job.workspace_id
        );
        match self.mode {
            Mode::Quota => return Err(DispatchFailure::QuotaExceeded),
            Mode::Bogus => return Ok(Uuid::new_v4()),
            _ => {}
        }
        let status = if matches!(self.mode, Mode::Partial) {
            "running"
        } else {
            "skipped"
        };
        let run: Uuid=db.query_one("INSERT INTO autopilot_run(autopilot_id,trigger_id,source,status,planned_at) VALUES ($1,$2,'schedule',$3,$4) ON CONFLICT (trigger_id,planned_at) WHERE trigger_id IS NOT NULL AND planned_at IS NOT NULL DO UPDATE SET status=EXCLUDED.status RETURNING id", &[&job.autopilot_id,&job.trigger_id,&status,&job.planned_at]).await.unwrap().get(0);
        if matches!(self.mode, Mode::Steal) {
            db.execute("UPDATE sys_cron_executions SET lease_token=gen_random_uuid(),stale_after=now()-interval '1 second' WHERE job_name='autopilot_schedule_dispatch' AND scope_id=$1 AND plan_time=$2", &[&job.trigger_id.to_string(),&job.planned_at]).await.unwrap();
        }
        Ok(run)
    }
}
struct BlockingSchedule(Arc<tokio::sync::Notify>);
#[async_trait::async_trait]
impl ScheduleDispatcher for BlockingSchedule {
    async fn dispatch(&self, _: ScheduleJob) -> Result<Uuid, DispatchFailure> {
        self.0.notify_one();
        std::future::pending().await
    }
}

fn worker(pool: &Arc<Pool>, mode: Mode, calls: &Arc<AtomicUsize>) -> ScheduleWorker {
    ScheduleWorker::new(
        pool.clone(),
        Arc::new(FakeSchedule {
            pool: pool.clone(),
            calls: calls.clone(),
            mode,
        }),
    )
}
async fn reset(db: &tokio_postgres::Client, trigger: Uuid, expression: &str) {
    db.execute("DELETE FROM sys_cron_executions WHERE job_name='autopilot_schedule_dispatch' AND scope_id=$1", &[&trigger.to_string()]).await.unwrap();
    db.execute("DELETE FROM autopilot_run WHERE trigger_id=$1", &[&trigger])
        .await
        .unwrap();
    db.execute("UPDATE autopilot_trigger SET last_fired_at=NULL,next_run_at=NULL,enabled=true,created_at=now()-interval '1 hour',cron_expression=$2,timezone='UTC' WHERE id=$1", &[&trigger,&expression]).await.unwrap();
}
async fn row(db: &tokio_postgres::Client, trigger: Uuid) -> tokio_postgres::Row {
    db.query_one("SELECT * FROM sys_cron_executions WHERE job_name='autopilot_schedule_dispatch' AND scope_id=$1 ORDER BY plan_time DESC LIMIT 1", &[&trigger.to_string()]).await.unwrap()
}
async fn runs(db: &tokio_postgres::Client, trigger: Uuid) -> i64 {
    db.query_one(
        "SELECT count(*) FROM autopilot_run WHERE trigger_id=$1",
        &[&trigger],
    )
    .await
    .unwrap()
    .get(0)
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ws: Uuid,
    agent: Uuid,
    creator: Uuid,
) {
    let ap:Uuid=db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Synthetic schedule',$2,'active','run_only','member',$3) RETURNING id",&[&ws,&agent,&creator]).await.unwrap().get(0);
    // Fix one daily occurrence two minutes behind the DB clock. All restart
    // and retry assertions stay stable across minute boundaries.
    let due: chrono::DateTime<chrono::Utc> = db
        .query_one("SELECT now()-interval '2 minutes'", &[])
        .await
        .unwrap()
        .get(0);
    let expression = due.format("%M %H * * *").to_string();
    let trigger:Uuid=db.query_one("INSERT INTO autopilot_trigger(autopilot_id,kind,cron_expression,timezone,created_at) VALUES ($1,'schedule',$2,'UTC',now()-interval '1 hour') RETURNING id",&[&ap,&expression]).await.unwrap().get(0);
    let calls = Arc::new(AtomicUsize::new(0));
    let first = worker(pool, Mode::Skipped, &calls);
    let second = worker(pool, Mode::Skipped, &calls);
    // Two replicas race one occurrence; the unique lease admits exactly one.
    let (a, b) = tokio::join!(first.tick(), second.tick());
    assert_eq!(a.unwrap() + b.unwrap(), 1, "one lease per occurrence");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let done = row(db, trigger).await;
    assert_eq!(done.get::<_, String>("status"), "SUCCESS");
    assert_eq!(done.get::<_, i32>("attempt"), 1);
    assert_eq!(done.get::<_, i64>("rows_affected"), 1);
    let planned: chrono::DateTime<chrono::Utc> = done.get("plan_time");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot_run WHERE trigger_id=$1 AND planned_at=$2",
            &[&trigger, &planned]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let display = db
        .query_one(
            "SELECT last_fired_at,next_run_at FROM autopilot_trigger WHERE id=$1",
            &[&trigger],
        )
        .await
        .unwrap();
    assert!(
        display
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_fired_at")
            .is_some()
    );
    assert!(display.get::<_, chrono::DateTime<chrono::Utc>>("next_run_at") > planned);
    assert_eq!(
        first.tick().await.unwrap(),
        0,
        "restart cannot replay the same occurrence"
    );
    // A callback cannot acknowledge a fabricated or still-partial run.
    reset(db, trigger, &expression).await;
    assert_eq!(worker(pool, Mode::Bogus, &calls).tick().await.unwrap(), 1);
    let failed = row(db, trigger).await;
    assert_eq!(failed.get::<_, String>("status"), "FAILED");
    assert_eq!(failed.get::<_, String>("error_code"), "incomplete_dispatch");
    assert!(
        failed
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_retry_at")
            .is_some()
    );
    assert_eq!(
        first.tick().await.unwrap(),
        0,
        "backoff must block an immediate retry"
    );
    // A retry returns the exact stored plan even beyond catch-up lateness.
    db.execute("UPDATE sys_cron_executions SET plan_time=plan_time-interval '1 hour',next_retry_at=now()-interval '1 second' WHERE id=$1", &[&failed.get::<_, Uuid>("id")]).await.unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    let retry = row(db, trigger).await;
    assert_eq!(retry.get::<_, i32>("attempt"), 2);
    assert_eq!(retry.get::<_, String>("status"), "SUCCESS");
    assert_eq!(
        retry.get::<_, chrono::DateTime<chrono::Utc>>("plan_time"),
        planned - chrono::Duration::hours(1)
    );
    // A partial run reserved by an earlier attempt fails, then its runtime
    // adapter can repair the same slot on retry without duplicating the run.
    reset(db, trigger, &expression).await;
    assert_eq!(worker(pool, Mode::Partial, &calls).tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "FAILED");
    assert_eq!(runs(db, trigger).await, 1);
    db.execute(
        "UPDATE sys_cron_executions SET next_retry_at=now()-interval '1 second' WHERE scope_id=$1",
        &[&trigger.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "SUCCESS");
    assert_eq!(runs(db, trigger).await, 1, "retry reuses the planned run");
    // A stolen lease cannot write success or advance the trigger display.
    reset(db, trigger, &expression).await;
    assert_eq!(worker(pool, Mode::Steal, &calls).tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "RUNNING");
    assert!(
        db.query_one(
            "SELECT last_fired_at FROM autopilot_trigger WHERE id=$1",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
        .is_none()
    );
    db.execute(
        "UPDATE sys_cron_executions SET stale_after=now()-interval '1 second' WHERE scope_id=$1",
        &[&trigger.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "SUCCESS");
    assert_eq!(runs(db, trigger).await, 1, "stale retry stays idempotent");
    // Three failed attempts exhaust the envelope; the fourth claim conflicts.
    reset(db, trigger, &expression).await;
    let quota = worker(pool, Mode::Quota, &calls);
    for attempt in 1..=3 {
        assert_eq!(quota.tick().await.unwrap(), 1);
        let r = row(db, trigger).await;
        assert_eq!(r.get::<_, i32>("attempt"), attempt);
        assert_eq!(r.get::<_, String>("error_code"), "quota_exceeded");
        if attempt < 3 {
            db.execute("UPDATE sys_cron_executions SET next_retry_at=now()-interval '1 second' WHERE id=$1", &[&r.get::<_, Uuid>("id")]).await.unwrap();
        }
    }
    assert!(
        row(db, trigger)
            .await
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_retry_at")
            .is_none()
    );
    assert_eq!(quota.tick().await.unwrap(), 0, "attempts are exhausted");
    assert_eq!(runs(db, trigger).await, 0);
    // Disabled triggers, paused/archived autopilots and never-firing
    // expressions are all skipped without writing an attempt row.
    reset(db, trigger, &expression).await;
    db.execute(
        "UPDATE autopilot_trigger SET enabled=false WHERE id=$1",
        &[&trigger],
    )
    .await
    .unwrap();
    assert_eq!(first.tick().await.unwrap(), 0);
    db.execute(
        "UPDATE autopilot_trigger SET enabled=true WHERE id=$1",
        &[&trigger],
    )
    .await
    .unwrap();
    db.execute("UPDATE autopilot SET status='archived' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    assert_eq!(first.tick().await.unwrap(), 0);
    db.execute("UPDATE autopilot SET status='active' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    reset(db, trigger, "0 0 31 2 *").await;
    assert_eq!(first.tick().await.unwrap(), 0, "never-firing cron is quiet");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM sys_cron_executions WHERE scope_id=$1",
            &[&trigger.to_string()]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    // A dense cron resumes at the most recent due slot after a long pause; its
    // own pending retry with a future backoff does not block the new slot.
    reset(db, trigger, "*/5 * * * *").await;
    db.execute("INSERT INTO sys_cron_executions(job_name,scope_kind,scope_id,plan_time,status,attempt,max_attempts,next_retry_at,finished_at,error_code) VALUES ('autopilot_schedule_dispatch','autopilot_trigger',$1,now()-interval '1 hour','FAILED',3,3,now()+interval '1 hour',now(),'quota_exceeded')", &[&trigger.to_string()]).await.unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    let resumed = row(db, trigger).await;
    assert_eq!(resumed.get::<_, String>("status"), "SUCCESS");
    assert!(
        resumed.get::<_, chrono::DateTime<chrono::Utc>>("plan_time")
            >= chrono::Utc::now() - chrono::Duration::minutes(5)
    );
    // A terminal completion failure rolls back the trigger display write.
    reset(db, trigger, &expression).await;
    db.batch_execute("CREATE FUNCTION automation_sprint_schedule_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='SUCCESS' THEN RAISE EXCEPTION 'synthetic schedule completion failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER automation_sprint_schedule_reject BEFORE UPDATE ON sys_cron_executions FOR EACH ROW EXECUTE FUNCTION automation_sprint_schedule_reject();").await.unwrap();
    assert!(first.tick().await.is_err());
    assert!(
        db.query_one(
            "SELECT last_fired_at FROM autopilot_trigger WHERE id=$1",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
        .is_none()
    );
    db.batch_execute("DROP TRIGGER automation_sprint_schedule_reject ON sys_cron_executions; DROP FUNCTION automation_sprint_schedule_reject();").await.unwrap();
    db.execute(
        "UPDATE sys_cron_executions SET stale_after=now()-interval '1 second' WHERE scope_id=$1",
        &[&trigger.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "SUCCESS");
    // Shutdown while the runtime call is pending leaves a recoverable lease and
    // must not hang the worker; the next instance resumes that occurrence.
    reset(db, trigger, &expression).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let blocked = ScheduleWorker::new(pool.clone(), Arc::new(BlockingSchedule(entered.clone())));
    let (stop, receiver) = tokio::sync::watch::channel(false);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(blocked.run(receiver), async {
            entered.notified().await;
            stop.send(true).unwrap();
        });
    })
    .await
    .expect("schedule shutdown must not hang on a pending adapter");
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "RUNNING");
    db.execute(
        "UPDATE sys_cron_executions SET stale_after=now()-interval '1 second' WHERE scope_id=$1",
        &[&trigger.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(first.tick().await.unwrap(), 1);
    assert_eq!(row(db, trigger).await.get::<_, String>("status"), "SUCCESS");
    db.execute("UPDATE autopilot SET status='archived' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    println!(
        "automation_sprint schedule: replica claim, latest-only planning, retry/backoff/exhaustion, lease CAS, display rollback and shutdown recovery passed; runtime admission remains a fake adapter"
    );
}
