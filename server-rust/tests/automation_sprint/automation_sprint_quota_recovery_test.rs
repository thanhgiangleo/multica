//! Crash recovery and deletion contracts against synthetic PostgreSQL rows.
use super::*;
use multica_server::api::{DurableLifecycleEvents, QuotaRecoveryPage, RunTransition};
use std::sync::Mutex;

#[derive(Default)]
struct Events {
    reject: Mutex<Option<Uuid>>,
}
#[async_trait::async_trait]
impl DurableLifecycleEvents for Events {
    async fn enqueue(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        event: &RunTransition,
    ) -> Result<(), &'static str> {
        tx.execute(
            "INSERT INTO automation_sprint_recovery_events_test(run_id,workspace_id,status) VALUES ($1,$2,$3)",
            &[&event.run_id, &event.workspace_id, &event.status],
        ).await.map_err(|_| "synthetic recovery event insert failed")?;
        if *self.reject.lock().unwrap() == Some(event.run_id) {
            return Err("synthetic recovery event rejected");
        }
        Ok(())
    }
}

async fn age(f: &Fixture<'_>, reservation: Uuid, minutes: i64) {
    f.db.execute(
        "UPDATE autopilot_quota_reservation SET created_at=now()-$2::bigint*interval '1 minute' WHERE id=$1",
        &[&reservation, &minutes],
    ).await.unwrap();
}

async fn run(f: &Fixture<'_>, source: &str, status: &str, minutes: i64) -> (Uuid, Uuid) {
    let reservation = f.reservation().await;
    age(f, reservation, minutes).await;
    let run = f.db.query_one(
        "INSERT INTO autopilot_run(autopilot_id,source,status,quota_reservation_id,planned_at) VALUES ($1,$2,$3,$4,now()) RETURNING id",
        &[&f.ap, &source, &status, &reservation],
    ).await.unwrap().get(0);
    (run, reservation)
}

async fn sweep(hooks: &RunLifecycle, ws: Uuid, limit: u16) -> Vec<QuotaRecoveryPage> {
    let mut pages = Vec::new();
    let mut after = None;
    for _ in 0..300 {
        let page = hooks.reconcile_quota_page(ws, after, limit).await.unwrap();
        assert!(page.scanned <= usize::from(limit));
        if let (Some(old), Some(new)) = (after, page.next_after) {
            assert!(new > old);
        }
        after = page.next_after;
        pages.push(page);
        if after.is_none() {
            return pages;
        }
    }
    panic!("synthetic quota sweep did not finish");
}

async fn event_count(f: &Fixture<'_>, run: Uuid) -> i64 {
    f.db.query_one(
        "SELECT count(*) FROM automation_sprint_recovery_events_test WHERE run_id=$1",
        &[&run],
    )
    .await
    .unwrap()
    .get(0)
}

async fn reservation_state(f: &Fixture<'_>, reservation: Uuid) -> String {
    f.db.query_one(
        "SELECT state FROM autopilot_quota_reservation WHERE id=$1",
        &[&reservation],
    )
    .await
    .unwrap()
    .get(0)
}

async fn issue(f: &Fixture<'_>) -> Uuid {
    f.db.query_one(
        "INSERT INTO issue(workspace_id,number,title,status,creator_type,creator_id) VALUES ($1,(SELECT COALESCE(max(number),0)+1 FROM issue WHERE workspace_id=$1),'Synthetic deletion','in_progress','member',$2) RETURNING id",
        &[&f.ws, &f.user],
    ).await.unwrap().get(0)
}

async fn link(f: &Fixture<'_>, run: Uuid, issue: Uuid) {
    f.db.execute(
        "UPDATE autopilot_run SET issue_id=$2 WHERE id=$1",
        &[&run, &issue],
    )
    .await
    .unwrap();
}

pub(super) async fn verify(original: &Fixture<'_>, other: Uuid, pool: &Arc<Pool>) {
    let db = original.db;
    db.batch_execute("CREATE TABLE automation_sprint_recovery_events_test(run_id uuid PRIMARY KEY,workspace_id uuid NOT NULL,status text NOT NULL)").await.unwrap();
    let ap: Uuid = db.query_one(
        "INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Synthetic quota recovery',$2,'active','run_only','member',$3) RETURNING id",
        &[&original.ws, &original.agent, &original.user],
    ).await.unwrap().get(0);
    let start = original.start + chrono::Duration::days(8);
    let end = original.end + chrono::Duration::days(8);
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end) VALUES ($1,$2,$3)", &[&original.ws,&start,&end]).await.unwrap();
    let f = Fixture {
        db,
        ap,
        start,
        end,
        ..*original
    };
    let events = Arc::new(Events::default());
    let hooks = RunLifecycle::with_events(pool.clone(), events.clone());
    assert_eq!(
        RunLifecycle::new(pool.clone())
            .reconcile_quota_page(f.ws, None, 10)
            .await
            .unwrap_err(),
        "lifecycle durable event writer required"
    );
    for limit in [0, 101] {
        assert_eq!(
            hooks
                .reconcile_quota_page(f.ws, None, limit)
                .await
                .unwrap_err(),
            "lifecycle page limit out of range"
        );
    }

    let orphan = f.reservation().await;
    age(&f, orphan, 11).await;
    let (completed, complete_res) = run(&f, "manual", "completed", 11).await;
    let (failed, failed_res) = run(&f, "api", "failed", 11).await;
    let (skipped, skipped_res) = run(&f, "schedule", "skipped", 11).await;
    let mut partial = Vec::new();
    // Frozen schema rejects pending, although Go recovery SQL retains that branch.
    for status in ["issue_created", "running"] {
        partial.push(run(&f, "manual", status, 361).await);
        partial.push(run(&f, "api", status, 361).await);
    }
    let mut protected = Vec::new();
    for (source, status, minutes) in [
        ("schedule", "issue_created", 999),
        ("webhook", "running", 999),
        ("manual", "issue_created", 359),
        ("api", "completed", 9),
    ] {
        protected.push(run(&f, source, status, minutes).await);
    }
    // An unlinked downstream task (even a terminal one) proves side effects exist.
    let (task_run, task, task_res) = f.run("completed").await;
    age(&f, task_res, 999).await;
    db.execute(
        "UPDATE autopilot_run SET task_id=NULL WHERE id=$1",
        &[&task_run],
    )
    .await
    .unwrap();
    protected.push((task_run, task_res));
    let linked_issue = issue(&f).await;
    let (issue_run, issue_res) = run(&f, "manual", "issue_created", 999).await;
    link(&f, issue_run, linked_issue).await;
    protected.push((issue_run, issue_res));
    let initial = f.counters().await;
    assert!(
        hooks
            .reconcile_quota_page(other, None, 100)
            .await
            .unwrap()
            .settled
            .is_empty()
    );
    assert_eq!(f.counters().await, initial);

    // Event rejection leaves the failed partial run and accounting unchanged.
    *events.reject.lock().unwrap() = Some(partial[0].0);
    let pages = sweep(&hooks, f.ws, 1).await;
    assert!(
        pages
            .iter()
            .any(|p| p.failures.iter().any(|e| e.reservation_id == partial[0].1))
    );
    assert_eq!(
        f.state(partial[0].0, partial[0].1).await,
        ("issue_created".into(), "reserved".into())
    );
    assert_eq!(event_count(&f, partial[0].0).await, 0);
    assert_eq!(reservation_state(&f, orphan).await, "released");
    assert_eq!(
        f.state(completed, complete_res).await,
        ("completed".into(), "consumed".into())
    );
    assert_eq!(
        f.state(failed, failed_res).await,
        ("failed".into(), "released".into())
    );
    assert_eq!(
        f.state(skipped, skipped_res).await,
        ("skipped".into(), "released".into())
    );
    // Already-terminal repair is accounting-only, not a duplicate lifecycle event.
    for terminal in [completed, failed, skipped] {
        assert_eq!(event_count(&f, terminal).await, 0);
    }
    *events.reject.lock().unwrap() = None;
    let (a, b) = tokio::join!(sweep(&hooks, f.ws, 1), sweep(&hooks, f.ws, 1));
    assert_eq!(
        a.iter()
            .chain(&b)
            .flat_map(|p| &p.settled)
            .filter(|&&id| id == partial[0].1)
            .count(),
        1
    );
    for (run, res) in &partial {
        assert_eq!(
            f.state(*run, *res).await,
            ("failed".into(), "released".into())
        );
        assert_eq!(event_count(&f, *run).await, 1);
        let row = db.query_one("SELECT planned_at IS NULL,reason_code,failure_reason FROM autopilot_run WHERE id=$1", &[run]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        assert_eq!(row.get::<_, String>(1), "internal_error");
        assert_eq!(
            row.get::<_, String>(2),
            "recovered partial dispatch (crashed before downstream creation)"
        );
    }
    for (run, res) in &protected {
        assert_eq!(reservation_state(&f, *res).await, "reserved");
        assert_eq!(event_count(&f, *run).await, 0);
    }
    assert_eq!(f.counters().await, (1, protected.len() as i64));
    assert!(
        sweep(&hooks, f.ws, 100)
            .await
            .iter()
            .all(|p| p.settled.is_empty())
    );
    // A terminal callback cannot consume a task whose run is not linked back.
    assert!(hooks.task_terminal(f.ws, task).await.unwrap().is_none());

    // Missing periods and foreign workspace links fail closed, without hiding peers.
    let broken_start = start + chrono::Duration::days(2);
    let broken_end = end + chrono::Duration::days(2);
    let broken = Fixture {
        start: broken_start,
        end: broken_end,
        ..f
    };
    let (bad_run, bad_res) = run(&broken, "manual", "issue_created", 999).await;
    let (good_run, good_res) = run(&f, "api", "issue_created", 999).await;
    let pages = sweep(&hooks, f.ws, 1).await;
    assert!(pages.iter().any(|p| {
        p.failures
            .iter()
            .any(|e| e.reservation_id == bad_res && e.message == "quota period missing")
    }));
    assert_eq!(
        f.state(bad_run, bad_res).await,
        ("issue_created".into(), "reserved".into())
    );
    assert_eq!(event_count(&f, bad_run).await, 0);
    assert_eq!(
        f.state(good_run, good_res).await,
        ("failed".into(), "released".into())
    );
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end,reserved_count) VALUES ($1,$2,$3,1)", &[&f.ws,&broken_start,&broken_end]).await.unwrap();
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&bad_res, &other],
    )
    .await
    .unwrap();
    let pages = sweep(&hooks, other, 100).await;
    assert!(pages.iter().any(|p| {
        p.failures.iter().any(|e| {
            e.reservation_id == bad_res && e.message == "quota reservation run workspace mismatch"
        })
    }));
    assert_eq!(
        f.state(bad_run, bad_res).await,
        ("issue_created".into(), "reserved".into())
    );
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&bad_res, &f.ws],
    )
    .await
    .unwrap();
    sweep(&hooks, f.ws, 100).await;
    assert_eq!(
        f.state(bad_run, bad_res).await,
        ("failed".into(), "released".into())
    );
    // Moving a run to another workspace cannot refund its original reservation.
    let foreign_ap: Uuid = db.query_one(
        "INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Synthetic foreign quota',$2,'active','run_only','member',$3) RETURNING id",
        &[&other, &f.agent, &f.user],
    ).await.unwrap().get(0);
    let (foreign_run, foreign_res) = run(&f, "manual", "running", 999).await;
    db.execute(
        "UPDATE autopilot_run SET autopilot_id=$2 WHERE id=$1",
        &[&foreign_run, &foreign_ap],
    )
    .await
    .unwrap();
    let pages = sweep(&hooks, f.ws, 100).await;
    assert!(pages.iter().any(|p| {
        p.failures.iter().any(|e| {
            e.reservation_id == foreign_res
                && e.message == "quota reservation run workspace mismatch"
        })
    }));
    assert_eq!(reservation_state(&f, foreign_res).await, "reserved");
    db.execute(
        "UPDATE autopilot_run SET autopilot_id=$2 WHERE id=$1",
        &[&foreign_run, &f.ap],
    )
    .await
    .unwrap();
    sweep(&hooks, f.ws, 100).await;
    assert_eq!(reservation_state(&f, foreign_res).await, "released");
    orphan_creation_race(&f, pool, &hooks).await;
    deletion(&f, other, pool, &hooks, &events).await;
    println!(
        "automation_sprint quota recovery/deletion: age/source/link guards, bounded pages, concurrent CAS, atomic accounting/events, savepoint and outer rollback passed; production writers/sweep remain unregistered"
    );
}

async fn orphan_creation_race(f: &Fixture<'_>, pool: &Arc<Pool>, hooks: &RunLifecycle) {
    let reservation = f.reservation().await;
    age(f, reservation, 11).await;
    let before = f.counters().await;
    let mut creator = pool.get().await.unwrap();
    let tx = creator.transaction().await.unwrap();
    let pid: i32 = tx
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    tx.query_one(
        "SELECT id FROM autopilot_quota_reservation WHERE id=$1 AND state='reserved' FOR UPDATE",
        &[&reservation],
    )
    .await
    .unwrap();
    let (page, ()) = tokio::join!(hooks.reconcile_quota_page(f.ws, None, 100), async {
        // Wait for recovery's pre-creation lookup to finish and its quota lock to block.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let blocked: bool = f.db.query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
                    &[&pid],
                ).await.unwrap().get(0);
                if blocked {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.expect("recovery did not reach reservation lock");
        tx.execute(
            "INSERT INTO autopilot_run(autopilot_id,source,status,quota_reservation_id) VALUES ($1,'manual','running',$2)",
            &[&f.ap, &reservation],
        ).await.unwrap();
        tx.commit().await.unwrap();
    });
    let page = page.unwrap();
    assert!(!page.settled.contains(&reservation));
    assert!(page.failures.is_empty());
    assert_eq!(reservation_state(f, reservation).await, "reserved");
    assert_eq!(f.counters().await, before);
}

async fn deletion(
    f: &Fixture<'_>,
    other: Uuid,
    pool: &Arc<Pool>,
    hooks: &RunLifecycle,
    events: &Arc<Events>,
) {
    let issue = issue(f).await;
    let (r1, _, q1) = f.run("running").await;
    let (r2, _, q2) = f.run("running").await;
    let (done, done_task, done_res) = f.run("completed").await;
    link(f, r1, issue).await;
    link(f, r2, issue).await;
    link(f, done, issue).await;
    // Consumed create_issue accounting cannot be refunded by deletion.
    f.db.execute(
        "UPDATE autopilot_quota_reservation SET state='consumed',finalized_at=now() WHERE id=$1",
        &[&q2],
    )
    .await
    .unwrap();
    f.db.execute("UPDATE autopilot_quota_period SET used_count=used_count+1,reserved_count=reserved_count-1 WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&f.ws,&f.start,&f.end]).await.unwrap();
    assert!(
        hooks
            .task_terminal(f.ws, done_task)
            .await
            .unwrap()
            .is_some()
    );
    let before = f.counters().await;
    let mut db = pool.get().await.unwrap();
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        hooks
            .issue_deleting_in_tx(&mut tx, other, issue)
            .await
            .unwrap_err(),
        "lifecycle issue missing"
    );
    assert_eq!(
        RunLifecycle::new(pool.clone())
            .issue_deleting_in_tx(&mut tx, f.ws, issue)
            .await
            .unwrap_err(),
        "lifecycle durable event writer required"
    );
    tx.commit().await.unwrap();
    assert_eq!(f.counters().await, before);

    // A rejected writer after earlier runs were settled rolls back the savepoint,
    // even when its caller incorrectly commits the outer transaction.
    *events.reject.lock().unwrap() = Some(if r1 > r2 { r1 } else { r2 });
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        hooks
            .issue_deleting_in_tx(&mut tx, f.ws, issue)
            .await
            .unwrap_err(),
        "synthetic recovery event rejected"
    );
    tx.commit().await.unwrap();
    assert_eq!(f.state(r1, q1).await, ("running".into(), "reserved".into()));
    assert_eq!(f.state(r2, q2).await, ("running".into(), "consumed".into()));
    assert_eq!(event_count(f, r1).await, 0);
    assert_eq!(event_count(f, r2).await, 0);
    assert_eq!(f.counters().await, before);
    *events.reject.lock().unwrap() = None;

    // Successful hook is still provisional until the authorized writer commits.
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        hooks
            .issue_deleting_in_tx(&mut tx, f.ws, issue)
            .await
            .unwrap()
            .len(),
        2
    );
    tx.execute(
        "UPDATE autopilot_run SET issue_id=NULL WHERE issue_id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    tx.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(f.state(r1, q1).await, ("running".into(), "reserved".into()));
    assert_eq!(f.counters().await, before);
    assert_eq!(event_count(f, r1).await, 0);
    let mut tx = db.transaction().await.unwrap();
    let changes = hooks
        .issue_deleting_in_tx(&mut tx, f.ws, issue)
        .await
        .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(
        changes
            .iter()
            .all(|c| c.workspace_id == f.ws && c.status == "failed")
    );
    assert!(
        hooks
            .issue_deleting_in_tx(&mut tx, f.ws, issue)
            .await
            .unwrap()
            .is_empty()
    );
    tx.execute(
        "UPDATE autopilot_run SET issue_id=NULL WHERE issue_id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    tx.execute("DELETE FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(f.counters().await, (before.0, before.1 - 1));
    assert_eq!(f.state(r1, q1).await, ("failed".into(), "released".into()));
    assert_eq!(f.state(r2, q2).await, ("failed".into(), "consumed".into()));
    assert_eq!(
        f.state(done, done_res).await,
        ("completed".into(), "consumed".into())
    );
    for run in [r1, r2] {
        assert_eq!(event_count(f, run).await, 1);
        assert_eq!(
            f.db.query_one(
                "SELECT failure_reason FROM autopilot_run WHERE id=$1",
                &[&run]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "linked issue was deleted"
        );
    }
    let mut tx = db.transaction().await.unwrap();
    assert_eq!(
        hooks
            .issue_deleting_in_tx(&mut tx, f.ws, issue)
            .await
            .unwrap_err(),
        "lifecycle issue missing"
    );
    tx.commit().await.unwrap();
}
