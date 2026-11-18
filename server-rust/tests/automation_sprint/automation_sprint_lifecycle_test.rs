//! Synthetic lifecycle/quota contracts; no task executor or external runtime.
use super::*;
use multica_server::api::RunLifecycle;

#[path = "automation_sprint_issue_lifecycle_test.rs"]
mod automation_sprint_issue_lifecycle_test;
#[path = "automation_sprint_quota_recovery_test.rs"]
mod automation_sprint_quota_recovery_test;
#[path = "automation_sprint_reconcile_test.rs"]
mod automation_sprint_reconcile_test;

struct Fixture<'a> {
    db: &'a tokio_postgres::Client,
    ws: Uuid,
    ap: Uuid,
    agent: Uuid,
    runtime: Uuid,
    user: Uuid,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
}
impl Fixture<'_> {
    async fn reservation(&self) -> Uuid {
        let reservation: Uuid = self.db.query_one(
            "INSERT INTO autopilot_quota_reservation(workspace_id,period_start,period_end,policy_revision,subscription_version,source,idempotency_key) VALUES ($1,$2,$3,1,1,'manual',$4) RETURNING id",
            &[&self.ws,&self.start,&self.end,&Uuid::new_v4().to_string()],
        ).await.unwrap().get(0);
        self.db.execute("UPDATE autopilot_quota_period SET reserved_count=reserved_count+1 WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&self.ws,&self.start,&self.end]).await.unwrap();
        reservation
    }
    async fn run(&self, task_status: &str) -> (Uuid, Uuid, Uuid) {
        let reservation = self.reservation().await;
        let task: Uuid = self.db.query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id,result) VALUES ($1,$2,$3,$4,$4,$5) RETURNING id",
            &[&self.agent,&self.runtime,&task_status,&self.user,&json!({"synthetic":"result"})],
        ).await.unwrap().get(0);
        let run: Uuid = self.db.query_one(
            "INSERT INTO autopilot_run(autopilot_id,source,status,task_id,quota_reservation_id) VALUES ($1,'manual','running',$2,$3) RETURNING id",
            &[&self.ap,&task,&reservation],
        ).await.unwrap().get(0);
        self.db
            .execute(
                "UPDATE agent_task_queue SET autopilot_run_id=$2 WHERE id=$1",
                &[&task, &run],
            )
            .await
            .unwrap();
        (run, task, reservation)
    }
    async fn counters(&self) -> (i64, i64) {
        let row=self.db.query_one("SELECT used_count,reserved_count FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&self.ws,&self.start,&self.end]).await.unwrap();
        (row.get(0), row.get(1))
    }
    async fn state(&self, run: Uuid, reservation: Uuid) -> (String, String) {
        let row=self.db.query_one("SELECT r.status,q.state FROM autopilot_run r JOIN autopilot_quota_reservation q ON q.id=r.quota_reservation_id WHERE r.id=$1 AND q.id=$2", &[&run,&reservation]).await.unwrap();
        (row.get(0), row.get(1))
    }
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ws: Uuid,
    other: Uuid,
    agent: Uuid,
    runtime: Uuid,
    user: Uuid,
) {
    let ap: Uuid=db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) VALUES ($1,'Lifecycle synthetic',$2,'active','run_only','member',$3) RETURNING id", &[&ws,&agent,&user]).await.unwrap().get(0);
    // Unique synthetic period avoids interfering with admission fixtures.
    let start = chrono::Utc::now() + chrono::Duration::days(600);
    let end = start + chrono::Duration::days(1);
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end) VALUES ($1,$2,$3)", &[&ws,&start,&end]).await.unwrap();
    // PostgreSQL precision is microseconds; use the returned canonical timestamps.
    let row=db.query_one("SELECT period_start,period_end FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&start,&end]).await.unwrap();
    let f = Fixture {
        db,
        ws,
        ap,
        agent,
        runtime,
        user,
        start: row.get(0),
        end: row.get(1),
    };
    let hooks = RunLifecycle::new(pool.clone());
    let (run, task, res) = f.run("completed").await;
    assert!(hooks.task_terminal(other, task).await.unwrap().is_none());
    assert_eq!(f.counters().await, (0, 1));
    let (a, b) = tokio::join!(hooks.task_terminal(ws, task), hooks.task_terminal(ws, task));
    let changes: Vec<_> = [a.unwrap(), b.unwrap()].into_iter().flatten().collect();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        (
            changes[0].workspace_id,
            changes[0].autopilot_id,
            changes[0].run_id,
            changes[0].status
        ),
        (ws, ap, run, "completed")
    );
    assert_eq!(
        f.state(run, res).await,
        ("completed".to_owned(), "consumed".to_owned())
    );
    assert_eq!(f.counters().await, (1, 0));
    let row = db
        .query_one(
            "SELECT result,completed_at IS NOT NULL FROM autopilot_run WHERE id=$1",
            &[&run],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Value>(0), json!({"synthetic":"result"}));
    assert!(row.get::<_, bool>(1));
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_none());
    // A late conflicting terminal callback cannot flip the winning result.
    db.execute(
        "UPDATE agent_task_queue SET status='failed',error='late failure' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_none());
    assert_eq!(f.counters().await, (1, 0));

    for (status, error, fallback, expected) in [
        (
            "failed",
            Some("  primary failure  "),
            Some("fallback"),
            "  primary failure  ",
        ),
        (
            "cancelled",
            Some(" \t"),
            Some("cancel reason"),
            "cancel reason",
        ),
        ("failed", None, Some(" \n"), "task failed"),
    ] {
        let (run, task, res) = f.run(status).await;
        db.execute(
            "UPDATE agent_task_queue SET error=$2,failure_reason=$3 WHERE id=$1",
            &[&task, &error, &fallback],
        )
        .await
        .unwrap();
        assert_eq!(
            hooks.task_terminal(ws, task).await.unwrap().unwrap().status,
            "failed"
        );
        assert_eq!(
            f.state(run, res).await,
            ("failed".to_owned(), "released".to_owned())
        );
        let row = db
            .query_one(
                "SELECT failure_reason,result FROM autopilot_run WHERE id=$1",
                &[&run],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), expected);
        assert_eq!(row.get::<_, Option<Value>>(1), None);
        assert!(hooks.task_terminal(ws, task).await.unwrap().is_none());
        assert_eq!(f.counters().await, (1, 0));
    }

    // Nonterminal state and one-sided/corrupt task links never settle a run.
    let (run, task, res) = f.run("running").await;
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_none());
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute("UPDATE autopilot_run SET task_id=NULL WHERE id=$1", &[&run])
        .await
        .unwrap();
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_none());
    db.execute(
        "UPDATE autopilot_run SET task_id=$2 WHERE id=$1",
        &[&run, &task],
    )
    .await
    .unwrap();
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_some());
    assert_eq!(
        f.state(run, res).await,
        ("completed".to_owned(), "consumed".to_owned())
    );
    assert_eq!(f.counters().await, (2, 0));

    // Different runs sharing one period must serialize counter changes, not lose updates.
    let (r1, t1, q1) = f.run("completed").await;
    let (r2, t2, q2) = f.run("failed").await;
    let (a, b) = tokio::join!(hooks.task_terminal(ws, t1), hooks.task_terminal(ws, t2));
    assert!(a.unwrap().is_some());
    assert!(b.unwrap().is_some());
    assert_eq!(
        f.state(r1, q1).await,
        ("completed".to_owned(), "consumed".to_owned())
    );
    assert_eq!(
        f.state(r2, q2).await,
        ("failed".to_owned(), "released".to_owned())
    );
    assert_eq!(f.counters().await, (3, 0));

    // Missing accounting rolls back the run mutation too; replay can repair after restore.
    let (run, task, res) = f.run("completed").await;
    db.execute("DELETE FROM autopilot_quota_period WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&f.start,&f.end]).await.unwrap();
    assert_eq!(
        hooks.task_terminal(ws, task).await.unwrap_err(),
        "lifecycle quota period missing"
    );
    assert_eq!(
        f.state(run, res).await,
        ("running".to_owned(), "reserved".to_owned())
    );
    db.execute("INSERT INTO autopilot_quota_period(workspace_id,period_start,period_end,used_count,reserved_count) VALUES ($1,$2,$3,3,1)", &[&ws,&f.start,&f.end]).await.unwrap();
    // A foreign reservation is not accepted through its UUID alone.
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&res, &other],
    )
    .await
    .unwrap();
    assert_eq!(
        hooks.task_terminal(ws, task).await.unwrap_err(),
        "lifecycle reservation missing"
    );
    assert_eq!(
        f.state(run, res).await,
        ("running".to_owned(), "reserved".to_owned())
    );
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&res, &ws],
    )
    .await
    .unwrap();
    assert!(hooks.task_terminal(ws, task).await.unwrap().is_some());
    assert_eq!(f.counters().await, (4, 0));

    db.execute(
        "UPDATE autopilot SET execution_mode='create_issue' WHERE id=$1",
        &[&ap],
    )
    .await
    .unwrap();
    let issue: Uuid=db.query_one("INSERT INTO issue(workspace_id,title,status,creator_type,creator_id,origin_type,origin_id) VALUES ($1,'Synthetic cancelled issue','cancelled','member',$2,'autopilot',$3) RETURNING id", &[&ws,&user,&ap]).await.unwrap().get(0);
    let (run, task, res) = f.run("completed").await;
    db.execute(
        "UPDATE autopilot_run SET status='issue_created',issue_id=$2 WHERE id=$1",
        &[&run, &issue],
    )
    .await
    .unwrap();
    assert!(
        hooks.task_terminal(ws, task).await.unwrap().is_none(),
        "create_issue requires dispatch-chain hook, not direct settlement"
    );
    assert!(
        hooks
            .issue_cancelled(other, issue)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute("UPDATE issue SET origin_type=NULL WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert!(hooks.issue_cancelled(ws, issue).await.unwrap().is_empty());
    db.execute(
        "UPDATE issue SET origin_type='autopilot',status='in_progress' WHERE id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    assert!(hooks.issue_cancelled(ws, issue).await.unwrap().is_empty());
    db.execute("UPDATE issue SET status='cancelled' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        hooks.issue_cancelled(ws, issue),
        hooks.issue_cancelled(ws, issue)
    );
    assert_eq!(a.unwrap().len() + b.unwrap().len(), 1);
    assert_eq!(
        f.state(run, res).await,
        ("failed".to_owned(), "released".to_owned())
    );
    assert_eq!(f.counters().await, (4, 0));

    // create_issue quota consumed at admission remains consumed after cancellation.
    let (run, _, res) = f.run("running").await;
    db.execute(
        "UPDATE autopilot_run SET status='issue_created',issue_id=$2 WHERE id=$1",
        &[&run, &issue],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE autopilot_quota_reservation SET state='consumed',finalized_at=now() WHERE id=$1",
        &[&res],
    )
    .await
    .unwrap();
    db.execute("UPDATE autopilot_quota_period SET used_count=used_count+1,reserved_count=reserved_count-1 WHERE workspace_id=$1 AND period_start=$2 AND period_end=$3", &[&ws,&f.start,&f.end]).await.unwrap();
    assert_eq!(hooks.issue_cancelled(ws, issue).await.unwrap().len(), 1);
    assert_eq!(
        f.state(run, res).await,
        ("failed".to_owned(), "consumed".to_owned())
    );
    assert_eq!(f.counters().await, (5, 0));
    assert!(hooks.issue_cancelled(ws, issue).await.unwrap().is_empty());
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cancelled"
    );
    automation_sprint_issue_lifecycle_test::verify(&f, other, &hooks).await;
    automation_sprint_reconcile_test::verify(&f, other, pool).await;
    automation_sprint_quota_recovery_test::verify(&f, other, pool).await;
    println!(
        "automation_sprint lifecycle: terminal CAS, scope/link guards, result/error mapping, concurrent quota settlement, rollback and consumed-quota monotonicity passed; hooks are not wired to task/issue writers or durable event transport"
    );
}
