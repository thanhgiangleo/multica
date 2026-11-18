//! Transactional transport test double lives only in the disposable test DB.
use super::*;
use multica_server::api::{DurableLifecycleEvents, ReconcilePage, RunTransition};
use std::sync::Mutex;

#[derive(Default)]
struct TestEvents {
    reject: Mutex<Option<Uuid>>,
}

#[async_trait::async_trait]
impl DurableLifecycleEvents for TestEvents {
    async fn enqueue(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        event: &RunTransition,
    ) -> Result<(), &'static str> {
        tx.execute(
            "INSERT INTO automation_sprint_lifecycle_events_test(run_id,workspace_id,status) VALUES ($1,$2,$3)",
            &[&event.run_id, &event.workspace_id, &event.status],
        )
        .await
        .map_err(|_| "synthetic durable event insert failed")?;
        if *self.reject.lock().unwrap() == Some(event.run_id) {
            return Err("synthetic durable event rejection");
        }
        Ok(())
    }
}

#[derive(Default)]
struct PausedEvents {
    entered: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl DurableLifecycleEvents for PausedEvents {
    async fn enqueue(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        event: &RunTransition,
    ) -> Result<(), &'static str> {
        TestEvents::default().enqueue(tx, event).await?;
        self.entered.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
}

async fn sweep(hooks: &RunLifecycle, ws: Uuid, limit: u16) -> Vec<ReconcilePage> {
    let mut pages = Vec::new();
    let mut after = None;
    for _ in 0..300 {
        let page = hooks.reconcile_page(ws, after, limit).await.unwrap();
        assert!(page.scanned <= usize::from(limit));
        if let (Some(previous), Some(next)) = (after, page.next_after) {
            assert!(
                next > previous,
                "cursor must advance past deferred/failed rows"
            );
        }
        after = page.next_after;
        pages.push(page);
        if after.is_none() {
            return pages;
        }
    }
    panic!("bounded synthetic sweep did not finish");
}

async fn event_count(db: &tokio_postgres::Client, run: Uuid) -> i64 {
    db.query_one(
        "SELECT count(*) FROM automation_sprint_lifecycle_events_test WHERE run_id=$1",
        &[&run],
    )
    .await
    .unwrap()
    .get(0)
}

pub(super) async fn verify(original: &Fixture<'_>, other: Uuid, pool: &Arc<Pool>) {
    let db = original.db;
    db.batch_execute(
        "CREATE TABLE automation_sprint_lifecycle_events_test(run_id uuid PRIMARY KEY,workspace_id uuid NOT NULL,status text NOT NULL)",
    ).await.unwrap();
    let ap: Uuid = db.query_one(
        "INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Synthetic lifecycle recovery',$2,'active','run_only','member',$3) RETURNING id",
        &[&original.ws, &original.agent, &original.user],
    ).await.unwrap().get(0);
    let start = original.start + chrono::Duration::days(2);
    let end = original.end + chrono::Duration::days(2);
    db.execute(
        "INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end) VALUES ($1,$2,$3)",
        &[&original.ws, &start, &end],
    ).await.unwrap();
    let f = Fixture {
        db,
        ap,
        start,
        end,
        ..*original
    };
    let events = Arc::new(TestEvents::default());
    let hooks = RunLifecycle::with_events(pool.clone(), events.clone());
    let (success, task, success_res) = f.run("completed").await;
    let (failure, failed_task, failure_res) = f.run("failed").await;
    let (active, _, active_res) = f.run("running").await;

    assert_eq!(
        RunLifecycle::new(pool.clone())
            .reconcile_page(f.ws, None, 10)
            .await
            .unwrap_err(),
        "lifecycle durable event writer required"
    );
    for limit in [0, 101] {
        assert_eq!(
            hooks.reconcile_page(f.ws, None, limit).await.unwrap_err(),
            "lifecycle page limit out of range"
        );
    }
    assert_eq!(f.counters().await, (0, 3));
    assert!(
        hooks
            .reconcile_page(other, None, 100)
            .await
            .unwrap()
            .transitions
            .is_empty()
    );
    assert_eq!(f.counters().await, (0, 3));

    // Transport failure rolls back run status, quota and event together. A later
    // sweep repairs the missed callback; one poisoned row cannot starve peers.
    *events.reject.lock().unwrap() = Some(success);
    assert_eq!(
        hooks.task_terminal(f.ws, task).await.unwrap_err(),
        "synthetic durable event rejection"
    );
    assert_eq!(
        f.state(success, success_res).await,
        ("running".into(), "reserved".into())
    );
    assert_eq!(event_count(db, success).await, 0);
    let pages = sweep(&hooks, f.ws, 1).await;
    assert!(
        pages
            .iter()
            .any(|p| p.failures.iter().any(|e| e.run_id == success))
    );
    assert!(
        pages
            .iter()
            .any(|p| p.transitions.iter().any(|e| e.run_id == failure))
    );
    assert_eq!(
        f.state(failure, failure_res).await,
        ("failed".into(), "released".into())
    );
    assert_eq!(
        f.state(active, active_res).await,
        ("running".into(), "reserved".into())
    );
    assert_eq!(event_count(db, failure).await, 1);
    assert_eq!(f.counters().await, (0, 2));
    *events.reject.lock().unwrap() = None;

    // Recovery and duplicate task callbacks race for the same terminal CAS.
    let (pages, callback) = tokio::join!(sweep(&hooks, f.ws, 1), hooks.task_terminal(f.ws, task));
    let recovered = pages
        .iter()
        .flat_map(|p| &p.transitions)
        .filter(|e| e.run_id == success)
        .count();
    assert_eq!(recovered + usize::from(callback.unwrap().is_some()), 1);
    assert_eq!(event_count(db, success).await, 1);
    assert_eq!(
        f.state(success, success_res).await,
        ("completed".into(), "consumed".into())
    );
    assert_eq!(f.counters().await, (1, 1));
    assert!(
        hooks
            .task_terminal(f.ws, failed_task)
            .await
            .unwrap()
            .is_none()
    );
    let repeat = sweep(&hooks, f.ws, 100).await;
    assert!(repeat.iter().all(|p| {
        p.transitions
            .iter()
            .all(|e| ![success, failure].contains(&e.run_id))
    }));

    // Cancelling a hook while its event writer is paused aborts the whole
    // transaction. The next instance can recover that same committed task.
    let (cancelled, cancelled_task, cancelled_res) = f.run("completed").await;
    let paused = Arc::new(PausedEvents::default());
    let paused_hooks = RunLifecycle::with_events(pool.clone(), paused.clone());
    let ws = f.ws;
    let running = tokio::spawn(async move { paused_hooks.task_terminal(ws, cancelled_task).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        paused.entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(event_count(db, cancelled).await, 0);
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    assert_eq!(
        f.state(cancelled, cancelled_res).await,
        ("running".into(), "reserved".into())
    );
    assert_eq!(event_count(db, cancelled).await, 0);
    let fresh = RunLifecycle::with_events(pool.clone(), events.clone());
    let recovered = sweep(&fresh, f.ws, 100).await;
    assert!(
        recovered
            .iter()
            .any(|p| p.transitions.iter().any(|e| e.run_id == cancelled))
    );
    assert_eq!(event_count(db, cancelled).await, 1);
    assert_eq!(
        f.state(cancelled, cancelled_res).await,
        ("completed".into(), "consumed".into())
    );

    // A process can die after the source task commits without any callback.
    // create_issue recovery observes its chain even while issue is in_progress.
    db.execute(
        "UPDATE autopilot SET execution_mode='create_issue' WHERE id=$1",
        &[&ap],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one(
        "INSERT INTO issue(workspace_id,number,title,status,creator_type,creator_id,origin_type,origin_id,assignee_type,assignee_id) VALUES ($1,(SELECT COALESCE(max(number),0)+1 FROM issue WHERE workspace_id=$1),'Synthetic lost callback','in_progress','agent',$2,'autopilot',$3,'agent',$2) RETURNING id",
        &[&f.ws, &f.agent, &ap],
    ).await.unwrap().get(0);
    let dispatch: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,status,issue_id,originator_user_id,accountable_user_id,result) VALUES ($1,$2,'completed',$3,$4,$4,$5) RETURNING id",
        &[&f.agent, &f.runtime, &issue, &f.user, &json!({"private_task_output":true})],
    ).await.unwrap().get(0);
    let reservation = f.reservation().await;
    let run: Uuid = db.query_one(
        "INSERT INTO autopilot_run(autopilot_id,source,status,issue_id,quota_reservation_id) VALUES ($1,'manual','issue_created',$2,$3) RETURNING id",
        &[&ap, &issue, &reservation],
    ).await.unwrap().get(0);
    *events.reject.lock().unwrap() = Some(run);
    let failed = sweep(&hooks, f.ws, 100).await;
    assert!(
        failed
            .iter()
            .any(|p| p.failures.iter().any(|e| e.run_id == run))
    );
    let root: Option<Uuid> = db
        .query_one("SELECT task_id FROM autopilot_run WHERE id=$1", &[&run])
        .await
        .unwrap()
        .get(0);
    assert_eq!(root, None, "root backfill also rolls back on event failure");
    assert_eq!(
        f.state(run, reservation).await,
        ("issue_created".into(), "reserved".into())
    );
    *events.reject.lock().unwrap() = None;
    let pages = sweep(&hooks, f.ws, 1).await;
    assert_eq!(
        pages
            .iter()
            .flat_map(|p| &p.transitions)
            .filter(|e| e.run_id == run)
            .count(),
        1
    );
    assert_eq!(event_count(db, run).await, 1);
    let row = db
        .query_one(
            "SELECT task_id,result FROM autopilot_run WHERE id=$1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), Some(dispatch));
    assert_eq!(row.get::<_, Option<Value>>(1), None);
    assert_eq!(
        f.state(run, reservation).await,
        ("completed".into(), "consumed".into())
    );
    assert!(
        hooks
            .linked_issue_task_terminal(f.ws, dispatch)
            .await
            .unwrap()
            .is_empty()
    );

    // Cancellation callback uses the same transactional event contract.
    let pending_res = f.reservation().await;
    let pending: Uuid = db.query_one(
        "INSERT INTO autopilot_run(autopilot_id,source,status,issue_id,quota_reservation_id) VALUES ($1,'manual','issue_created',$2,$3) RETURNING id",
        &[&ap, &issue, &pending_res],
    ).await.unwrap().get(0);
    db.execute("UPDATE issue SET status='cancelled' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert_eq!(hooks.issue_changed(f.ws, issue).await.unwrap().len(), 1);
    assert_eq!(event_count(db, pending).await, 1);
    assert_eq!(
        f.state(pending, pending_res).await,
        ("failed".into(), "released".into())
    );
    // A separate one-slot pool proves the scan connection is released before
    // recovery hooks acquire their per-run transactions.
    let single_res = f.reservation().await;
    let single_run: Uuid = db.query_one(
        "INSERT INTO autopilot_run(autopilot_id,source,status,issue_id,quota_reservation_id) VALUES ($1,'manual','issue_created',$2,$3) RETURNING id",
        &[&ap, &issue, &single_res],
    ).await.unwrap().get(0);
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    let single = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(1)
            .build()
            .unwrap(),
    );
    let single_hooks = RunLifecycle::with_events(single, events);
    let pages = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        sweep(&single_hooks, f.ws, 100),
    )
    .await
    .unwrap();
    assert!(
        pages
            .iter()
            .any(|p| p.transitions.iter().any(|e| e.run_id == single_run))
    );
    assert_eq!(event_count(db, single_run).await, 1);
    assert_eq!(
        f.state(single_run, single_res).await,
        ("failed".into(), "released".into())
    );
    assert_eq!(
        f.state(active, active_res).await,
        ("running".into(), "reserved".into()),
        "missing issue link is deferred, not failed"
    );
    println!(
        "automation_sprint recovery: bounded cursor sweep, poison isolation, missing-callback repair, transactional event/quota/root rollback, cancellation rollback, one-slot pool, duplicate CAS and workspace guards passed; production event adapter/startup remain unwired"
    );
}
