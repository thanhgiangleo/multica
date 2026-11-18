//! Failure monitor uses synthetic PostgreSQL only. No scheduler, external provider or live bus.
use super::*;
use async_trait::async_trait;
use deadpool_postgres::Transaction;
use multica_server::api::{
    DurablePauseEvents, FailureMonitor, FailureMonitorConfig, PauseTransition,
};

struct PauseEvents {
    reject: Option<Uuid>,
}

#[async_trait]
impl DurablePauseEvents for PauseEvents {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        change: &PauseTransition,
    ) -> Result<(), &'static str> {
        tx.execute(
            "INSERT INTO automation_sprint_pause_events(autopilot_id,workspace_id,recipients) VALUES ($1,$2,$3)",
            &[&change.autopilot_id, &change.workspace_id, &json!(change.notices.iter().map(|n| n.recipient_id).collect::<Vec<_>>())],
        ).await.map_err(|_| "test event insert failed")?;
        if self.reject == Some(change.autopilot_id) {
            return Err("test durable event rejected");
        }
        Ok(())
    }
}

async fn autopilot(
    db: &tokio_postgres::Client,
    ws: Uuid,
    assignee: Uuid,
    creator_type: &str,
    creator: Uuid,
    status: &str,
) -> Uuid {
    db.query_one(
        "INSERT INTO autopilot(workspace_id,title,assignee_id,status,execution_mode,created_by_type,created_by_id) \
         VALUES ($1,'Synthetic monitor',$2,$3,'run_only',$4,$5) RETURNING id",
        &[&ws, &assignee, &status, &creator_type, &creator],
    ).await.unwrap().get(0)
}

async fn runs(db: &tokio_postgres::Client, id: Uuid, statuses: &[&str]) {
    for status in statuses {
        db.execute(
            "INSERT INTO autopilot_run(autopilot_id,source,status) VALUES ($1,'manual',$2)",
            &[&id, status],
        )
        .await
        .unwrap();
    }
}

async fn sweep(monitor: &FailureMonitor) -> Vec<PauseTransition> {
    let mut cursor = None;
    let mut results = Vec::new();
    loop {
        let page = monitor.sweep_page(cursor, 100).await.unwrap();
        assert!(page.failures.is_empty(), "{:?}", page.failures);
        results.extend(page.paused);
        match page.next_after {
            None => break,
            next => cursor = next,
        }
    }
    results
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ws: Uuid,
    other: Uuid,
    agent: Uuid,
    owner: Uuid,
) {
    db.batch_execute("CREATE TABLE automation_sprint_pause_events(autopilot_id uuid,workspace_id uuid,recipients jsonb)").await.unwrap();
    let config = FailureMonitorConfig {
        min_runs: 4,
        fail_ratio: 0.75,
        ..Default::default()
    };
    for invalid in [
        FailureMonitorConfig {
            min_runs: 0,
            ..config
        },
        FailureMonitorConfig {
            lookback_seconds: -1,
            ..config
        },
        FailureMonitorConfig {
            fail_ratio: f64::NAN,
            ..config
        },
        FailureMonitorConfig {
            fail_ratio: 1.1,
            ..config
        },
    ] {
        assert!(
            FailureMonitor::new(
                pool.clone(),
                Arc::new(PauseEvents { reject: None }),
                invalid
            )
            .is_err()
        );
    }
    let monitor =
        FailureMonitor::new(pool.clone(), Arc::new(PauseEvents { reject: None }), config).unwrap();
    assert!(monitor.sweep_page(None, 0).await.is_err());
    assert!(monitor.sweep_page(None, 101).await.is_err());
    let passing = autopilot(db, ws, agent, "member", owner, "active").await;
    runs(
        db,
        passing,
        &[
            "failed",
            "failed",
            "failed",
            "completed",
            "skipped",
            "issue_created",
            "running",
        ],
    )
    .await;
    db.execute("INSERT INTO autopilot_run(autopilot_id,source,status,created_at) VALUES ($1,'manual','failed',now()-interval '8 days')", &[&passing]).await.unwrap();
    let below = autopilot(db, ws, agent, "member", owner, "active").await;
    runs(db, below, &["failed", "failed", "completed", "completed"]).await;
    let skipped = autopilot(db, ws, agent, "member", owner, "active").await;
    runs(db, skipped, &["skipped", "skipped", "skipped", "skipped"]).await;
    let archived = autopilot(db, ws, agent, "member", owner, "archived").await;
    runs(db, archived, &["failed", "failed", "failed", "failed"]).await;
    // Former creator must not receive an inbox item. Workspace manager fallback stays local.
    let former = autopilot(db, ws, agent, "member", Uuid::new_v4(), "active").await;
    runs(db, former, &["failed", "failed", "failed", "failed"]).await;
    let foreign = autopilot(db, other, agent, "member", owner, "active").await;
    runs(db, foreign, &["failed", "failed", "failed", "failed"]).await;
    // A rejected event must roll back pause, rule version and inbox rows.
    let rejected = autopilot(db, ws, agent, "member", owner, "active").await;
    runs(db, rejected, &["failed", "failed", "failed", "failed"]).await;
    let reject_monitor = FailureMonitor::new(
        pool.clone(),
        Arc::new(PauseEvents {
            reject: Some(rejected),
        }),
        config,
    )
    .unwrap();
    let mut cursor = None;
    let mut saw_rejection = false;
    loop {
        let page = reject_monitor.sweep_page(cursor, 100).await.unwrap();
        saw_rejection |= page
            .failures
            .contains(&(rejected, "test durable event rejected"));
        match page.next_after {
            None => break,
            next => cursor = next,
        }
    }
    assert!(saw_rejection);
    assert_eq!(
        db.query_one("SELECT status FROM autopilot WHERE id=$1", &[&rejected])
            .await
            .unwrap()
            .get::<_, String>(0),
        "active"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot_rule_version WHERE autopilot_id=$1",
            &[&rejected]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM automation_sprint_pause_events WHERE autopilot_id=$1",
            &[&rejected]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE details->>'autopilot_id'=$1",
            &[&rejected.to_string()]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let changes = sweep(&monitor).await;
    assert!(changes.iter().any(|c| c.autopilot_id == rejected));
    assert!(
        !changes
            .iter()
            .any(|c| [passing, former, foreign].contains(&c.autopilot_id))
    );
    assert!(
        !changes
            .iter()
            .any(|c| [below, skipped, archived].contains(&c.autopilot_id))
    );
    let first = db.query_one("SELECT a.status,v.published_by_type,v.published_by_id,v.config_summary FROM autopilot a JOIN autopilot_rule_version v ON v.autopilot_id=a.id WHERE a.id=$1", &[&passing]).await.unwrap();
    assert_eq!(first.get::<_, String>(0), "paused");
    assert_eq!(first.get::<_, String>(1), "system");
    assert_eq!(first.get::<_, Option<Uuid>>(2), None);
    assert_eq!(first.get::<_, Value>(3)["status"], "paused");
    let first_notice=db.query_one("SELECT recipient_id,severity,details FROM inbox_item WHERE details->>'autopilot_id'=$1", &[&passing.to_string()]).await.unwrap();
    assert_eq!(first_notice.get::<_, Uuid>(0), owner);
    assert_eq!(first_notice.get::<_, String>(1), "attention");
    assert_eq!(first_notice.get::<_, Value>(2)["total_runs"], 4);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE details->>'autopilot_id'=$1 AND recipient_id=$2",
            &[&former.to_string(), &owner]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE details->>'autopilot_id'=$1 AND workspace_id=$2",
            &[&foreign.to_string(), &ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(sweep(&monitor).await.is_empty());
    for id in [passing, former, foreign, rejected] {
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM automation_sprint_pause_events WHERE autopilot_id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );
    }
    // Competing sweeps serialize on the autopilot; only one transaction may
    // create the system publish, inbox item and durable event.
    let concurrent = autopilot(db, ws, agent, "agent", agent, "active").await;
    runs(db, concurrent, &["failed", "failed", "failed", "failed"]).await;
    let (left, right) = tokio::join!(sweep(&monitor), sweep(&monitor));
    let won: Vec<_> = left
        .into_iter()
        .chain(right)
        .filter(|c| c.autopilot_id == concurrent)
        .collect();
    assert_eq!(won.len(), 1);
    assert_eq!(won[0].notices.len(), 1);
    assert_eq!(won[0].notices[0].recipient_id, owner);
    assert_eq!(
        won[0].notices[0].item["details"]["reason"],
        "auto_paused_high_failure_rate"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot_rule_version WHERE autopilot_id=$1",
            &[&concurrent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM automation_sprint_pause_events WHERE autopilot_id=$1",
            &[&concurrent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE details->>'autopilot_id'=$1",
            &[&concurrent.to_string()]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    // Single-row keyset pages stay monotonic, including pages with no offender.
    let mut after = None;
    for n in 0..200 {
        let page = monitor.sweep_page(after, 1).await.unwrap();
        assert!(page.paused.is_empty());
        assert!(page.failures.is_empty());
        if let Some(next) = page.next_after {
            assert!(after.is_none_or(|previous| next > previous));
            after = Some(next);
            assert!(n < 199, "cursor never drained");
        } else {
            break;
        }
    }
    db.batch_execute("DROP TABLE automation_sprint_pause_events")
        .await
        .unwrap();
}
