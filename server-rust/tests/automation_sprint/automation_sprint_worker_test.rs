//! Disposable DB lease, adapter, recovery, and failure tests. No live runtime.
use super::*;
use multica_server::api::{DispatchFailure, WebhookDispatcher, WebhookJob, WebhookWorker};
use std::sync::atomic::{AtomicUsize, Ordering};

struct FakeDispatcher {
    pool: Arc<Pool>,
    calls: Arc<AtomicUsize>,
    mode: Mode,
}

#[derive(Clone, Copy)]
enum Mode {
    SkippedRun,
    Quota,
    BogusRun,
    StealLease,
}

#[async_trait::async_trait]
impl WebhookDispatcher for FakeDispatcher {
    async fn dispatch(&self, job: WebhookJob) -> Result<Uuid, DispatchFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(job.event.starts_with("github.pull_request"));
        assert!(job.payload.is_object());
        assert_eq!(job.payload["event"], json!(job.event));
        assert!(job.payload["eventPayload"].is_object());
        assert!(
            job.payload["request"]["receivedAt"]
                .as_str()
                .unwrap()
                .ends_with('Z')
        );
        assert_eq!(job.payload["request"]["contentType"], "application/json");
        assert!(job.received_at <= chrono::Utc::now());
        match self.mode {
            Mode::Quota => Err(DispatchFailure::QuotaExceeded),
            Mode::BogusRun => Ok(Uuid::new_v4()),
            Mode::SkippedRun | Mode::StealLease => {
                let db = self.pool.get().await.unwrap();
                let inserted = db.query_opt("INSERT INTO autopilot_run(autopilot_id,trigger_id,source,status,webhook_delivery_id,trigger_payload) VALUES ($1,$2,'webhook','skipped',$3,$4) ON CONFLICT (webhook_delivery_id) WHERE webhook_delivery_id IS NOT NULL DO NOTHING RETURNING id", &[&job.autopilot_id,&job.trigger_id,&job.delivery_id,&job.payload]).await.unwrap();
                let id = match inserted {
                    Some(row) => row.get(0),
                    None => db
                        .query_one(
                            "SELECT id FROM autopilot_run WHERE webhook_delivery_id=$1",
                            &[&job.delivery_id],
                        )
                        .await
                        .unwrap()
                        .get(0),
                };
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
                if matches!(self.mode, Mode::StealLease) {
                    db.execute("UPDATE webhook_delivery SET lease_token=gen_random_uuid(),lease_expires_at=now()-interval '1 second' WHERE id=$1", &[&job.delivery_id]).await.unwrap();
                }
                Ok(id)
            }
        }
    }
}

struct BlockingDispatcher(Arc<tokio::sync::Notify>);

#[async_trait::async_trait]
impl WebhookDispatcher for BlockingDispatcher {
    async fn dispatch(&self, _: WebhookJob) -> Result<Uuid, DispatchFailure> {
        self.0.notify_one();
        std::future::pending().await
    }
}

fn worker(pool: &Arc<Pool>, mode: Mode, calls: &Arc<AtomicUsize>) -> WebhookWorker {
    WebhookWorker::new(
        pool.clone(),
        Arc::new(FakeDispatcher {
            pool: pool.clone(),
            calls: calls.clone(),
            mode,
        }),
    )
}

pub(super) async fn verify(
    db: &tokio_postgres::Client,
    pool: &Arc<Pool>,
    ap: Uuid,
    trigger: Uuid,
    ws: Uuid,
) {
    // Isolate each candidate while keeping the rest of the fixture's queue intact.
    db.execute("UPDATE webhook_delivery SET available_at=now()+interval '1 hour' WHERE autopilot_id=$1 AND status='queued'", &[&ap]).await.unwrap();
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='signature-retry'", &[&trigger]).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let first = worker(pool, Mode::SkippedRun, &calls);
    let (a, b) = tokio::join!(first.process_next(), first.process_next());
    assert_eq!(u8::from(a.unwrap()) + u8::from(b.unwrap()), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let delivered = db.query_one("SELECT id,status,autopilot_run_id,lease_token,dispatch_attempts FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='signature-retry' AND status='dispatched'", &[&trigger]).await.unwrap();
    let run_id: Uuid = delivered.get("autopilot_run_id");
    let run = db
        .query_one(
            "SELECT status,webhook_delivery_id FROM autopilot_run WHERE id=$1",
            &[&run_id],
        )
        .await
        .unwrap();
    assert_eq!(run.get::<_, String>("status"), "skipped");
    assert_eq!(
        run.get::<_, Uuid>("webhook_delivery_id"),
        delivered.get::<_, Uuid>("id")
    );
    assert!(delivered.get::<_, Option<Uuid>>("lease_token").is_none());
    assert_eq!(delivered.get::<_, i32>("dispatch_attempts"), 1);
    assert!(
        db.query_one(
            "SELECT last_fired_at FROM autopilot_trigger WHERE id=$1",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
        .is_some()
    );
    // A fabricated run ID cannot turn a queued delivery into success.
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='parallel'", &[&trigger]).await.unwrap();
    assert!(
        worker(pool, Mode::BogusRun, &calls)
            .process_next()
            .await
            .unwrap()
    );
    let row = db.query_one("SELECT status,dispatch_attempts,lease_token FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='parallel'", &[&trigger]).await.unwrap();
    assert_eq!(row.get::<_, String>("status"), "queued");
    assert_eq!(row.get::<_, i32>("dispatch_attempts"), 1);
    assert!(row.get::<_, Option<Uuid>>("lease_token").is_none());
    // Fifth failed dispatch terminates retry loop.
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second',dispatch_attempts=4 WHERE trigger_id=$1 AND dedupe_key='parallel'", &[&trigger]).await.unwrap();
    assert!(
        worker(pool, Mode::BogusRun, &calls)
            .process_next()
            .await
            .unwrap()
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='parallel'",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='allowed-later-filter'", &[&trigger]).await.unwrap();
    assert!(
        worker(pool, Mode::Quota, &calls)
            .process_next()
            .await
            .unwrap()
    );
    let quota = db.query_one("SELECT status,reason_code,autopilot_run_id FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='allowed-later-filter'", &[&trigger]).await.unwrap();
    assert_eq!(quota.get::<_, String>("status"), "ignored");
    assert_eq!(quota.get::<_, String>("reason_code"), "quota_exceeded");
    assert!(quota.get::<_, Option<Uuid>>("autopilot_run_id").is_none());
    // Expired lease owner may create a run, but cannot acknowledge new owner's lease.
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='nat-0'", &[&trigger]).await.unwrap();
    assert!(
        worker(pool, Mode::StealLease, &calls)
            .process_next()
            .await
            .unwrap()
    );
    let stolen = db.query_one("SELECT id,status,lease_token FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='nat-0'", &[&trigger]).await.unwrap();
    assert_eq!(stolen.get::<_, String>("status"), "queued");
    assert!(stolen.get::<_, Option<Uuid>>("lease_token").is_some());
    db.execute("UPDATE autopilot SET status='paused' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    assert!(first.process_next().await.unwrap());
    let resumed = db
        .query_one(
            "SELECT status,autopilot_run_id FROM webhook_delivery WHERE id=$1",
            &[&stolen.get::<_, Uuid>("id")],
        )
        .await
        .unwrap();
    assert_eq!(resumed.get::<_, String>("status"), "dispatched");
    assert!(resumed.get::<_, Option<Uuid>>("autopilot_run_id").is_some());
    db.execute("UPDATE autopilot SET status='active' WHERE id=$1", &[&ap])
        .await
        .unwrap();
    assert_eq!(
        db.query_one(
            "SELECT workspace_id FROM webhook_delivery WHERE id=$1",
            &[&stolen.get::<_, Uuid>("id")]
        )
        .await
        .unwrap()
        .get::<_, Uuid>(0),
        ws
    );
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second',raw_body=$2 WHERE trigger_id=$1 AND dedupe_key='nat-1'", &[&trigger,&b"null".as_slice()]).await.unwrap();
    assert!(first.process_next().await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='nat-1'",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    // Shutdown while adapter is blocked leaves durable lease, then a new worker
    // recovers that row after expiry. No task is claimed during either phase.
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='nat-2'", &[&trigger]).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let blocked = WebhookWorker::new(pool.clone(), Arc::new(BlockingDispatcher(entered.clone())));
    let (stop, receiver) = tokio::sync::watch::channel(false);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(blocked.run(receiver), async {
            entered.notified().await;
            stop.send(true).unwrap();
        });
    })
    .await
    .expect("worker shutdown while adapter is pending");
    let stranded = db.query_one("SELECT status,lease_token FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='nat-2'", &[&trigger]).await.unwrap();
    assert_eq!(stranded.get::<_, String>("status"), "queued");
    assert!(stranded.get::<_, Option<Uuid>>("lease_token").is_some());
    db.execute("UPDATE webhook_delivery SET lease_expires_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='nat-2'", &[&trigger]).await.unwrap();
    assert!(first.process_next().await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='nat-2'",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "dispatched"
    );
    // Long-running adapter renews the DB lease; a replacement token fences the
    // old worker even if that adapter never returns.
    db.execute("UPDATE webhook_delivery SET available_at=now()-interval '1 second' WHERE trigger_id=$1 AND dedupe_key='nat-3'", &[&trigger]).await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let blocked = WebhookWorker::new(pool.clone(), Arc::new(BlockingDispatcher(entered.clone())));
    let processing = tokio::spawn(async move { blocked.process_next().await });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .expect("adapter receives claimed delivery");
    let before = db.query_one("SELECT id,lease_expires_at FROM webhook_delivery WHERE trigger_id=$1 AND dedupe_key='nat-3'", &[&trigger]).await.unwrap();
    let delivery: Uuid = before.get("id");
    let first_expiry: chrono::DateTime<chrono::Utc> = before.get("lease_expires_at");
    tokio::time::sleep(std::time::Duration::from_secs(32)).await;
    let renewed = db
        .query_one(
            "SELECT lease_expires_at FROM webhook_delivery WHERE id=$1",
            &[&delivery],
        )
        .await
        .unwrap();
    assert!(
        renewed.get::<_, chrono::DateTime<chrono::Utc>>(0)
            > first_expiry + chrono::Duration::seconds(20)
    );
    let successor = Uuid::new_v4();
    db.execute(
        "UPDATE webhook_delivery SET lease_token=$2 WHERE id=$1",
        &[&delivery, &successor],
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(35), processing)
            .await
            .expect("lost lease stops blocked adapter")
            .unwrap()
            .unwrap()
    );
    let fenced = db
        .query_one(
            "SELECT lease_token,status,dispatch_attempts FROM webhook_delivery WHERE id=$1",
            &[&delivery],
        )
        .await
        .unwrap();
    assert_eq!(fenced.get::<_, Uuid>("lease_token"), successor);
    assert_eq!(fenced.get::<_, String>("status"), "queued");
    assert_eq!(fenced.get::<_, i32>("dispatch_attempts"), 0);
    db.execute(
        "UPDATE webhook_delivery SET lease_expires_at=now()-interval '1 second' WHERE id=$1",
        &[&delivery],
    )
    .await
    .unwrap();
    assert!(first.process_next().await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT status FROM webhook_delivery WHERE id=$1",
            &[&delivery]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "dispatched"
    );
    println!(
        "automation_sprint worker: SKIP LOCKED, persisted run validation, quota, retry exhaustion, heartbeat, lease fencing, stale lease, crash recovery, blocked-adapter shutdown, corrupted body passed; no production adapter installed"
    );
}
