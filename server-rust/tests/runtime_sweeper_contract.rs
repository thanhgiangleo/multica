//! Ignored disposable-PG contract for the DB-only runtime sweeper slice.
use deadpool_postgres::{Manager, Pool};
use multica_server::api::p0_daemon_outcomes::{mark_stale_runtime_candidates, sweep_runtime_once};
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

async fn database() -> Pool {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").expect("disposable PG URL required");
    let config = Config::from_str(&url).unwrap();
    assert!(config.get_dbname().unwrap_or_default().starts_with("multica_"));
    let pool = Pool::builder(Manager::new(config, NoTls)).max_size(8).build().unwrap();
    pool.get().await.unwrap().batch_execute("\
        CREATE TABLE agent_runtime (id uuid PRIMARY KEY, status text NOT NULL, last_seen_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now());\
        CREATE TABLE agent_task_queue (id uuid PRIMARY KEY, runtime_id uuid NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz, error text, failure_reason text, wait_reason text, delivered_comment_ids uuid[] NOT NULL DEFAULT '{}');\
        CREATE TABLE task_supplement (task_id uuid NOT NULL, status text NOT NULL, failure_reason text, updated_at timestamptz NOT NULL DEFAULT now());\
        CREATE TABLE comment (id uuid PRIMARY KEY, author_type text NOT NULL, type text NOT NULL, source_task_id uuid, recovery_settled_at timestamptz);\
    ").await.unwrap();
    pool
}

async fn runtime(pool: &Pool, status: &str, age_seconds: i32, heartbeated: bool) -> Uuid {
    let id = Uuid::new_v4();
    let db = pool.get().await.unwrap();
    db.execute("INSERT INTO agent_runtime(id,status,last_seen_at,updated_at) VALUES ($1,$2,CASE WHEN $4 THEN now()-make_interval(secs=>$3::int) END,now()-make_interval(secs=>$3::int))", &[&id,&status,&age_seconds,&heartbeated]).await.unwrap();
    id
}
async fn task(pool: &Pool, runtime: Uuid, status: &str, receipt: Option<Uuid>) -> Uuid {
    let id = Uuid::new_v4();
    let db = pool.get().await.unwrap();
    let receipts: Vec<Uuid> = receipt.into_iter().collect();
    db.execute("INSERT INTO agent_task_queue(id,runtime_id,status,delivered_comment_ids) VALUES($1,$2,$3,$4)", &[&id,&runtime,&status,&receipts]).await.unwrap();
    id
}
async fn status(pool: &Pool, id: Uuid) -> String {
    pool.get().await.unwrap().query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&id]).await.unwrap().get(0)
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; use runtime_sweeper_verify.sh via sprint-gate"]
async fn stale_recheck_grace_and_atomic_terminal_settlement() {
    let pool = database().await;
    let stale = runtime(&pool, "online", 151, true).await;
    let refreshed = runtime(&pool, "online", 151, true).await;
    let fresh = runtime(&pool, "online", 149, true).await;
    let db = pool.get().await.unwrap();
    db.execute("UPDATE agent_runtime SET last_seen_at=now() WHERE id=$1", &[&refreshed]).await.unwrap();
    drop(db);
    // Candidate ids came from an earlier SELECT; heartbeat wins before UPDATE.
    assert_eq!(mark_stale_runtime_candidates(&pool, &[refreshed]).await.unwrap(), 0);
    let first = sweep_runtime_once(&pool, 10800).await.unwrap();
    assert_eq!(first.offline_runtimes, 1);
    assert_eq!(first.failed_tasks, 0);
    let db = pool.get().await.unwrap();
    assert_eq!(db.query_one("SELECT status FROM agent_runtime WHERE id=$1", &[&stale]).await.unwrap().get::<_, String>(0), "offline");
    for id in [refreshed, fresh] {
        assert_eq!(db.query_one("SELECT status FROM agent_runtime WHERE id=$1", &[&id]).await.unwrap().get::<_, String>(0), "online");
    }
    drop(db);

    let old = runtime(&pool, "offline", 10810, true).await;
    let never_heartbeated = runtime(&pool, "offline", 10810, false).await;
    let grace = runtime(&pool, "offline", 10790, true).await;
    // A runtime that came back heartbeats fresh, so its work is never reaped.
    let recovered = runtime(&pool, "online", 0, true).await;
    let failed = task(&pool, old, "running", None).await;
    let dispatched = task(&pool, never_heartbeated, "dispatched", None).await;
    let waiting = task(&pool, old, "waiting_local_directory", None).await;
    let within_grace = task(&pool, grace, "running", None).await;
    let reconnected = task(&pool, recovered, "running", None).await;
    let queued = task(&pool, old, "queued", None).await;
    let completed = task(&pool, old, "completed", None).await;
    let delivered = Uuid::new_v4();
    let planned_only = Uuid::new_v4();
    let with_receipt = task(&pool, old, "running", Some(delivered)).await;
    let db = pool.get().await.unwrap();
    for (id, current_status) in [(failed,"pending"),(dispatched,"delivering"),(with_receipt,"pending")] {
        db.execute("INSERT INTO task_supplement(task_id,status) VALUES($1,$2)", &[&id,&current_status]).await.unwrap();
    }
    for (id, source) in [(delivered,failed),(planned_only,failed)] {
        db.execute("INSERT INTO comment(id,author_type,type,source_task_id) VALUES($1,'system','progress_update',$2)", &[&id,&source]).await.unwrap();
    }
    drop(db);
    let second = sweep_runtime_once(&pool, 10800).await.unwrap();
    assert_eq!(second.failed_tasks, 4);
    for id in [failed,dispatched,waiting,with_receipt] {
        assert_eq!(status(&pool,id).await,"failed");
        let db = pool.get().await.unwrap();
        let row=db.query_one("SELECT error,failure_reason,completed_at IS NOT NULL,wait_reason FROM agent_task_queue WHERE id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_,String>(0),"runtime went offline");
        assert_eq!(row.get::<_,String>(1),"runtime_offline");
        assert!(row.get::<_,bool>(2));
        assert_eq!(row.get::<_,Option<String>>(3),None);
    }
    for (id, expected) in [(within_grace,"running"),(reconnected,"running"),(queued,"queued"),(completed,"completed")] { assert_eq!(status(&pool,id).await,expected); }
    let db=pool.get().await.unwrap();
    for id in [failed,dispatched,with_receipt] {
        let row=db.query_one("SELECT status,failure_reason FROM task_supplement WHERE task_id=$1", &[&id]).await.unwrap();
        assert_eq!(row.get::<_,String>(0),"failed");
        assert_eq!(row.get::<_,String>(1),"turn_ended");
    }
    assert!(db.query_one("SELECT recovery_settled_at IS NOT NULL FROM comment WHERE id=$1", &[&delivered]).await.unwrap().get::<_,bool>(0));
    assert!(!db.query_one("SELECT recovery_settled_at IS NOT NULL FROM comment WHERE id=$1", &[&planned_only]).await.unwrap().get::<_,bool>(0));
    drop(db);
    assert_eq!(sweep_runtime_once(&pool,10800).await.unwrap().failed_tasks,0);

    // Settlement failure must abort the failed status, not leave a terminal
    // receipt that the recovery outbox can no longer replay.
    let rollback_task=task(&pool,old,"running",None).await;
    let db=pool.get().await.unwrap();
    db.execute("INSERT INTO task_supplement(task_id,status) VALUES($1,'pending')", &[&rollback_task]).await.unwrap();
    db.batch_execute("DROP TABLE comment").await.unwrap();
    drop(db);
    assert!(sweep_runtime_once(&pool,10800).await.is_err());
    assert_eq!(status(&pool,rollback_task).await,"running");
    let db=pool.get().await.unwrap();
    assert_eq!(db.query_one("SELECT status FROM task_supplement WHERE task_id=$1", &[&rollback_task]).await.unwrap().get::<_,String>(0),"pending");
}
