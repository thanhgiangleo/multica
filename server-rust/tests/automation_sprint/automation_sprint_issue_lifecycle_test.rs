//! Frozen Go autopilot.go SyncRunFromIssue / SyncRunFromLinkedIssueTask contracts.
use super::*;

impl Fixture<'_> {
    async fn issue(&self, status: &str, assignee: Option<&str>) -> Uuid {
        self.db.query_one(
            "INSERT INTO issue(workspace_id,number,title,status,creator_type,creator_id,origin_type,origin_id,assignee_type,assignee_id) \
             VALUES ($1,(SELECT COALESCE(max(number),0)+1 FROM issue WHERE workspace_id=$1),'Synthetic dispatch chain',$2,'agent',$3,'autopilot',$4,$5,$6) RETURNING id",
            &[&self.ws, &status, &self.agent, &self.ap, &assignee, &assignee.map(|_| self.agent)],
        ).await.unwrap().get(0)
    }

    async fn linked_run(&self, issue: Uuid, task: Option<Uuid>) -> (Uuid, Uuid) {
        let reservation = self.reservation().await;
        let run = self.db.query_one(
            "INSERT INTO autopilot_run(autopilot_id,source,status,issue_id,task_id,quota_reservation_id) \
             VALUES ($1,'manual','issue_created',$2,$3,$4) RETURNING id",
            &[&self.ap, &issue, &task, &reservation],
        ).await.unwrap().get(0);
        (run, reservation)
    }

    async fn task(
        &self,
        issue: Uuid,
        agent: Uuid,
        status: &str,
        retry: Option<Uuid>,
        sequence: i32,
    ) -> Uuid {
        self.db.query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,status,issue_id,originator_user_id,accountable_user_id,retry_of_task_id,created_at,result) \
             VALUES ($1,$2,$3,$4,$5,$5,$6,'2044-01-01T00:00:00Z'::timestamptz+($7::integer*interval '1 second'),$8) RETURNING id",
            &[&agent, &self.runtime, &status, &issue, &self.user, &retry, &sequence, &json!({"private_task_output":true})],
        ).await.unwrap().get(0)
    }

    async fn task_status(&self, task: Uuid, status: &str) {
        self.db
            .execute(
                "UPDATE agent_task_queue SET status=$2 WHERE id=$1",
                &[&task, &status],
            )
            .await
            .unwrap();
    }

    async fn issue_status(&self, issue: Uuid, status: &str) {
        self.db
            .execute("UPDATE issue SET status=$2 WHERE id=$1", &[&issue, &status])
            .await
            .unwrap();
    }

    async fn run_task(&self, run: Uuid) -> Option<Uuid> {
        self.db
            .query_one("SELECT task_id FROM autopilot_run WHERE id=$1", &[&run])
            .await
            .unwrap()
            .get(0)
    }

    async fn failure(&self, run: Uuid) -> Option<String> {
        self.db
            .query_one(
                "SELECT failure_reason FROM autopilot_run WHERE id=$1",
                &[&run],
            )
            .await
            .unwrap()
            .get(0)
    }
}

pub(super) async fn verify(f: &Fixture<'_>, other: Uuid, hooks: &RunLifecycle) {
    let ws = f.ws;
    let db = f.db;
    let foreign: Uuid = db.query_one(
        "INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Synthetic mentioned agent','cloud',$2,$3) RETURNING id",
        &[&ws, &f.user, &f.runtime],
    ).await.unwrap().get(0);
    let before = f.counters().await;

    // Fallback root is the oldest creator-agent nonretry task, not an earlier
    // mention for a different agent or a later same-agent manual rerun.
    let issue = f.issue("in_review", Some("agent")).await;
    let (run, res) = f.linked_run(issue, None).await;
    assert!(
        hooks.issue_changed(ws, issue).await.unwrap().is_empty(),
        "dispatch not enqueued yet"
    );
    let mention = f.task(issue, foreign, "completed", None, -1).await;
    let root = f.task(issue, f.agent, "failed", None, 0).await;
    let manual = f.task(issue, f.agent, "completed", None, 1).await;
    let unrelated_retry = f.task(issue, foreign, "completed", Some(mention), 2).await;
    let retry = f.task(issue, f.agent, "failed", Some(root), 3).await;
    let final_retry = f.task(issue, f.agent, "queued", Some(retry), 4).await;
    for task in [mention, manual, unrelated_retry, root, retry] {
        assert!(
            hooks
                .linked_issue_task_terminal(other, task)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            hooks
                .linked_issue_task_terminal(ws, task)
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(f.run_task(run).await, Some(root));
    assert!(
        hooks.issue_changed(ws, issue).await.unwrap().is_empty(),
        "active retry beats early delivery"
    );
    assert_eq!(
        f.state(run, res).await,
        ("issue_created".to_owned(), "reserved".to_owned())
    );
    f.task_status(final_retry, "completed").await;
    let (a, b) = tokio::join!(
        hooks.linked_issue_task_terminal(ws, final_retry),
        hooks.issue_changed(ws, issue),
    );
    assert_eq!(
        a.unwrap().len() + b.unwrap().len(),
        1,
        "task/issue callbacks settle once"
    );
    assert_eq!(
        f.state(run, res).await,
        ("completed".to_owned(), "consumed".to_owned())
    );
    assert_eq!(f.counters().await, (before.0 + 1, before.1));
    let result: Option<Value> = db
        .query_one("SELECT result FROM autopilot_run WHERE id=$1", &[&run])
        .await
        .unwrap()
        .get(0);
    assert_eq!(result, None, "create_issue does not expose task result");
    assert!(
        hooks
            .linked_issue_task_terminal(ws, root)
            .await
            .unwrap()
            .is_empty()
    );
    f.issue_status(issue, "cancelled").await;
    assert!(
        hooks.issue_changed(ws, issue).await.unwrap().is_empty(),
        "terminal winner cannot flip"
    );
    assert_eq!(f.state(run, res).await.0, "completed");

    // Issue delivery cannot mask a failure committed before its task event.
    for (error, fallback, expected) in [
        (
            Some(" primary failure "),
            Some("fallback"),
            " primary failure ",
        ),
        (Some(" \t"), Some("retry exhausted"), "retry exhausted"),
        (None, Some(" \n"), "task failed"),
    ] {
        let issue = f.issue("done", Some("agent")).await;
        let task = f.task(issue, f.agent, "failed", None, 0).await;
        db.execute(
            "UPDATE agent_task_queue SET error=$2,failure_reason=$3 WHERE id=$1",
            &[&task, &error, &fallback],
        )
        .await
        .unwrap();
        let (run, res) = f.linked_run(issue, Some(task)).await;
        assert_eq!(
            hooks.issue_changed(ws, issue).await.unwrap()[0].status,
            "failed"
        );
        assert_eq!(f.failure(run).await.as_deref(), Some(expected));
        assert_eq!(
            f.state(run, res).await,
            ("failed".to_owned(), "released".to_owned())
        );
        assert!(
            hooks
                .linked_issue_task_terminal(ws, task)
                .await
                .unwrap()
                .is_empty()
        );
    }

    // Only retry_of_task_id lineage counts. A queued mention on the same issue
    // must not hold a terminal dispatch failure open.
    let issue = f.issue("in_progress", Some("agent")).await;
    let task = f.task(issue, f.agent, "failed", None, 0).await;
    f.task(issue, foreign, "queued", None, 1).await;
    f.task(issue, f.agent, "queued", None, 2).await;
    let (run, res) = f.linked_run(issue, Some(task)).await;
    assert_eq!(
        hooks.linked_issue_task_terminal(ws, task).await.unwrap()[0].status,
        "failed"
    );
    assert_eq!(f.state(run, res).await.0, "failed");

    // A delayed failure callback for an earlier attempt cannot override the
    // successful retry that already settled the chain.
    let issue = f.issue("in_progress", Some("agent")).await;
    let stale = f.task(issue, f.agent, "running", None, 0).await;
    let recovered = f.task(issue, f.agent, "completed", Some(stale), 1).await;
    let (run, res) = f.linked_run(issue, Some(stale)).await;
    assert_eq!(
        hooks
            .linked_issue_task_terminal(ws, recovered)
            .await
            .unwrap()[0]
            .status,
        "completed"
    );
    f.task_status(stale, "failed").await;
    assert!(
        hooks
            .linked_issue_task_terminal(ws, stale)
            .await
            .unwrap()
            .is_empty(),
        "late attempt failure cannot reopen a completed retry"
    );
    assert_eq!(
        f.state(run, res).await,
        ("completed".to_owned(), "consumed".to_owned())
    );

    // A completed dispatch is successful even when its agent blocked the issue.
    // Status callbacks alone do not settle nonterminal issue keys.
    let issue = f.issue("blocked", Some("agent")).await;
    let task = f.task(issue, f.agent, "running", None, 0).await;
    let (run, res) = f.linked_run(issue, Some(task)).await;
    assert!(hooks.issue_changed(ws, issue).await.unwrap().is_empty());
    assert!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap()
            .is_empty(),
        "DB state authoritative"
    );
    f.task_status(task, "completed").await;
    assert_eq!(
        hooks.linked_issue_task_terminal(ws, task).await.unwrap()[0].status,
        "completed"
    );
    assert_eq!(f.state(run, res).await.0, "completed");

    // Cancelled dispatch tasks have no task-outcome settlement; delivery can
    // complete them, whereas issue cancellation always fails an active run.
    for (status, expected, quota) in [
        ("in_review", "completed", "consumed"),
        ("cancelled", "failed", "released"),
    ] {
        let issue = f.issue(status, Some("agent")).await;
        let task = f.task(issue, f.agent, "cancelled", None, 0).await;
        let (run, res) = f.linked_run(issue, Some(task)).await;
        assert!(
            hooks
                .linked_issue_task_terminal(ws, task)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            hooks.issue_changed(ws, issue).await.unwrap()[0].status,
            expected
        );
        assert_eq!(
            f.state(run, res).await,
            (expected.to_owned(), quota.to_owned())
        );
    }

    // Empty dispatch waits for both agent and squad assignees, but a human or
    // unassigned delivered issue has no expected enqueue and may complete.
    for assignee in [None, Some("member"), Some("agent"), Some("squad")] {
        let issue = f.issue("done", assignee).await;
        let (run, res) = f.linked_run(issue, None).await;
        assert!(
            hooks.issue_cancelled(ws, issue).await.unwrap().is_empty(),
            "cancellation-only hook stays narrow"
        );
        let changes = hooks.issue_changed(ws, issue).await.unwrap();
        if matches!(assignee, Some("agent" | "squad")) {
            assert!(changes.is_empty());
            db.execute(
                "UPDATE issue SET assignee_type=NULL,assignee_id=NULL WHERE id=$1",
                &[&issue],
            )
            .await
            .unwrap();
            assert_eq!(hooks.issue_changed(ws, issue).await.unwrap().len(), 1);
        } else {
            assert_eq!(changes.len(), 1);
        }
        assert_eq!(f.state(run, res).await.0, "completed");
    }

    // Custom categories resolve only within the issue workspace. Started is
    // not review; unresolved keys stay nonterminal. Audit reason keeps raw key.
    for (key, category) in [
        ("synthetic_delivered", "done"),
        ("synthetic_closed", "closed"),
        ("synthetic_review", "started"),
    ] {
        db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,$2,$2,$3,'#6b7280')", &[&ws, &key, &category]).await.unwrap();
    }
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES ($1,'foreign_only','Foreign only','closed','#6b7280')", &[&other]).await.unwrap();
    for (status, expected) in [
        ("synthetic_delivered", Some("completed")),
        ("synthetic_closed", Some("failed")),
        ("synthetic_review", None),
        ("foreign_only", None),
        ("unknown_status", None),
    ] {
        let issue = f.issue(status, None).await;
        let (run, res) = f.linked_run(issue, None).await;
        assert!(hooks.issue_changed(other, issue).await.unwrap().is_empty());
        let changes = hooks.issue_changed(ws, issue).await.unwrap();
        if let Some(expected) = expected {
            assert_eq!(changes[0].status, expected);
            assert_eq!(f.state(run, res).await.0, expected);
            if expected == "failed" {
                assert_eq!(
                    f.failure(run).await.as_deref(),
                    Some("issue synthetic_closed")
                );
            }
        } else {
            assert!(changes.is_empty());
            assert_eq!(f.state(run, res).await.0, "issue_created");
            f.issue_status(issue, "cancelled").await;
            assert_eq!(hooks.issue_changed(ws, issue).await.unwrap().len(), 1);
        }
    }

    // A bogus recorded root on a different issue cannot import its outcome.
    // A valid recorded root wins over older creator tasks on the same issue.
    let other_issue = f.issue("in_progress", Some("agent")).await;
    let alien_task = f.task(other_issue, f.agent, "completed", None, 0).await;
    let issue = f.issue("in_review", Some("agent")).await;
    let old = f.task(issue, f.agent, "failed", None, 0).await;
    let task = f.task(issue, f.agent, "completed", None, 1).await;
    let (run, res) = f.linked_run(issue, Some(alien_task)).await;
    assert!(hooks.issue_changed(ws, issue).await.unwrap().is_empty());
    assert!(
        hooks
            .linked_issue_task_terminal(ws, alien_task)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        hooks
            .linked_issue_task_terminal(ws, old)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute(
        "UPDATE autopilot_run SET task_id=$2 WHERE id=$1",
        &[&run, &task],
    )
    .await
    .unwrap();
    assert_eq!(
        hooks.issue_changed(ws, issue).await.unwrap()[0].status,
        "completed"
    );
    assert_eq!(f.state(run, res).await.0, "completed");

    // Missing origin, foreign autopilot workspace, or direct run_only task
    // cannot enter the linked-issue hook. No root backfill on rejected scope.
    let issue = f.issue("done", Some("agent")).await;
    let task = f.task(issue, f.agent, "completed", None, 0).await;
    let (run, res) = f.linked_run(issue, None).await;
    db.execute("UPDATE issue SET origin_type=NULL WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(hooks.issue_changed(ws, issue).await.unwrap().is_empty());
    assert_eq!(f.run_task(run).await, None);
    db.execute(
        "UPDATE issue SET origin_type='autopilot' WHERE id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE autopilot SET workspace_id=$2 WHERE id=$1",
        &[&f.ap, &other],
    )
    .await
    .unwrap();
    assert!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(hooks.issue_changed(ws, issue).await.unwrap().is_empty());
    db.execute(
        "UPDATE autopilot SET workspace_id=$2 WHERE id=$1",
        &[&f.ap, &ws],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_task_queue SET autopilot_run_id=$2 WHERE id=$1",
        &[&task, &run],
    )
    .await
    .unwrap();
    assert!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute(
        "UPDATE agent_task_queue SET autopilot_run_id=NULL WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(f.state(run, res).await.0, "completed");

    // Root backfill, terminal transition and quota settlement roll back together.
    let issue = f.issue("done", Some("agent")).await;
    let task = f.task(issue, f.agent, "completed", None, 0).await;
    let (run, res) = f.linked_run(issue, None).await;
    let counters = f.counters().await;
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&res, &other],
    )
    .await
    .unwrap();
    assert_eq!(
        hooks
            .linked_issue_task_terminal(ws, task)
            .await
            .unwrap_err(),
        "lifecycle reservation missing"
    );
    assert_eq!(f.run_task(run).await, None);
    assert_eq!(
        f.state(run, res).await,
        ("issue_created".to_owned(), "reserved".to_owned())
    );
    assert_eq!(f.counters().await, counters);
    db.execute(
        "UPDATE autopilot_quota_reservation SET workspace_id=$2 WHERE id=$1",
        &[&res, &ws],
    )
    .await
    .unwrap();
    assert_eq!(hooks.issue_changed(ws, issue).await.unwrap().len(), 1);
    assert_eq!(f.run_task(run).await, Some(task));
    assert_eq!(f.counters().await, (counters.0 + 1, counters.1 - 1));
    assert!(
        hooks
            .issue_changed(ws, Uuid::new_v4())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        hooks
            .linked_issue_task_terminal(ws, Uuid::new_v4())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(f.counters().await.1, before.1, "all reservations settled");
    println!(
        "automation_sprint create_issue lifecycle: dispatch-root/retry-lineage isolation, active retry wait, early-delivery failure precedence, custom terminal categories, workspace/origin guards, task/issue concurrent CAS and atomic root/quota rollback passed"
    );
}
