//! Disposable PostgreSQL contract test for the scheduler claim/lease core.
//! Run through `sprint-gate.py --kind db -- bash tests/scheduler_db_verify.sh`.
use deadpool_postgres::{Manager as PgManager, Pool};
use chrono::TimeDelta;
use multica_server::scheduler::*;
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

async fn database() -> Option<Arc<Pool>> {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").ok()?;
    let config = Config::from_str(&url).unwrap();
    assert!(
        config
            .get_dbname()
            .unwrap_or_default()
            .starts_with("multica_")
    );
    let pool = Arc::new(
        Pool::builder(PgManager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    db.batch_execute(include_str!("../migrations/113_sys_cron_executions.up.sql"))
        .await
        .unwrap();
    db.batch_execute("CREATE TABLE search_index_change(entity_type text NOT NULL,entity_id uuid NOT NULL,workspace_id uuid NOT NULL,change_xid xid8 NOT NULL,changed_at timestamptz NOT NULL,PRIMARY KEY(entity_type,entity_id)); CREATE TABLE search_index_prune_mark(singleton boolean PRIMARY KEY,pruned_through_xid xid8 NOT NULL)").await.unwrap();
    drop(db);
    Some(pool)
}

#[tokio::test]
#[ignore = "requires disposable RUST_API_TEST_DATABASE_URL; run via sprint-gate DB script"]
async fn postgres_claim_recovery_and_registered_prune_job() {
    let Some(pool) = database().await else {
        panic!("disposable DB required")
    };
    let mut first = Manager::new(pool.clone());
    let mut second = Manager::new(pool.clone());
    let job = prune_search_index_changes();
    first.register(job.clone()).unwrap();
    second.register(job.clone()).unwrap();
    assert!(second.register(job.clone()).is_err());
    let now = first.db_now().await.unwrap();
    let plan = now - TimeDelta::hours(2);
    let scope = Scope::global();
    let (a, b) = tokio::join!(
        first.try_claim(&job, &scope, plan, now),
        second.try_claim(&job, &scope, plan, now)
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|c| matches!(c, Claim::Won(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|c| matches!(c, Claim::Conflict))
            .count(),
        1
    );
    let first_lease = outcomes
        .into_iter()
        .find_map(|c| if let Claim::Won(l) = c { Some(l) } else { None })
        .unwrap();
    assert_eq!(first_lease.id.get_version_num(), 7);
    assert_eq!(first_lease.token.get_version_num(), 4);
    assert!(
        first
            .heartbeat(first_lease, job.stale_timeout)
            .await
            .is_ok()
    );
    // A recently heartbeating holder cannot be stolen, even by another replica.
    assert!(matches!(
        second.try_claim(&job, &scope, plan, now).await.unwrap(),
        Claim::Conflict
    ));
    let db = pool.get().await.unwrap();
    db.execute(
        "UPDATE sys_cron_executions SET stale_after=$2 WHERE id=$1",
        &[&first_lease.id, &(now - TimeDelta::seconds(1))],
    )
    .await
    .unwrap();
    drop(db);
    assert_eq!(second.mark_stale(PRUNE_JOB, now).await.unwrap(), 1);
    let stolen = match second.try_claim(&job, &scope, plan, now).await.unwrap() {
        Claim::Stole(l) => l,
        _ => panic!("stale plan must be retryable"),
    };
    assert_eq!(stolen.id, first_lease.id);
    assert_eq!(stolen.attempt, 2);
    assert_ne!(stolen.token, first_lease.token);
    assert!(matches!(
        first.heartbeat(first_lease, job.stale_timeout).await,
        Err(SchedulerError::LeaseLost)
    ));
    assert!(matches!(
        first
            .finish_success(
                first_lease,
                now,
                Duration::ZERO,
                HandlerResult {
                    rows_affected: 0,
                    result: json!({})
                }
            )
            .await,
        Err(SchedulerError::LeaseLost)
    ));
    let retry = now + TimeDelta::minutes(1);
    second
        .finish_failure(
            stolen,
            now,
            Duration::from_millis(2),
            "handler_error",
            "synthetic",
            Some(retry),
        )
        .await
        .unwrap();
    assert!(matches!(
        first.try_claim(&job, &scope, plan, now).await.unwrap(),
        Claim::Conflict
    ));
    let third = match first.try_claim(&job, &scope, plan, retry).await.unwrap() {
        Claim::Stole(l) => l,
        _ => panic!("due retry"),
    };
    assert_eq!(third.attempt, 3);
    first
        .finish_failure(
            third,
            retry,
            Duration::ZERO,
            "handler_error",
            "exhausted",
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        second.try_claim(&job, &scope, plan, retry).await.unwrap(),
        Claim::Conflict
    ));
    // The registered job uses a plan-time cutoff, prunes the bounded SQL batch,
    // and persists a SUCCESS audit row and the highest deleted xid.
    let db = pool.get().await.unwrap();
    let old = now - TimeDelta::days(31);
    let fresh = now - TimeDelta::days(2);
    for changed in [old, fresh] {
        db.execute("INSERT INTO search_index_change(entity_type,entity_id,workspace_id,change_xid,changed_at) VALUES('issue',$1,$2,pg_current_xact_id(),$3)", &[&Uuid::new_v4(),&Uuid::new_v4(),&changed]).await.unwrap();
    }
    drop(db);
    first.run_once().await.unwrap();
    let db = pool.get().await.unwrap();
    let row = db.query_one("SELECT status,attempt,rows_affected,result FROM sys_cron_executions WHERE job_name=$1 AND plan_time=date_trunc('hour',now())", &[&PRUNE_JOB]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "SUCCESS");
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i64>(2), 1);
    let result: Value = row.get(3);
    assert!(result["cutoff"].is_string());
    let remaining: i64 = db
        .query_one("SELECT count(*) FROM search_index_change", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 1);
    let mark: i64 = db
        .query_one("SELECT count(*) FROM search_index_prune_mark", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(mark, 1);
    first.run_once().await.unwrap();
    let attempts: i32 = db.query_one("SELECT attempt FROM sys_cron_executions WHERE job_name=$1 AND plan_time=date_trunc('hour',now())", &[&PRUNE_JOB]).await.unwrap().get(0);
    assert_eq!(attempts, 1);
}
