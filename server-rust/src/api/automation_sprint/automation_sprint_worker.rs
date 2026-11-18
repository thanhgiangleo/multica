//! Recoverable PostgreSQL delivery lease. Dispatch belongs to the injected task runtime.
use super::{automation_sprint_ingress::event_allowed, *};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};

const MAX_ATTEMPTS: i32 = 5;
const DISPATCH_TIMEOUT: Duration = Duration::from_secs(120);
const LEASE_HEARTBEAT: Duration = Duration::from_secs(30);

/// The runtime must durably admit or resume a run with a unique webhook_delivery_id.
/// A successful return is checked against the database before acknowledgement.
#[async_trait]
pub trait WebhookDispatcher: Send + Sync + 'static {
    async fn dispatch(&self, job: WebhookJob) -> Result<Uuid, DispatchFailure>;
}

#[derive(Clone, Copy, Debug)]
pub enum DispatchFailure {
    Retry,
    QuotaExceeded,
}

#[derive(Clone, Debug)]
pub struct WebhookJob {
    pub delivery_id: Uuid,
    pub workspace_id: Uuid,
    pub autopilot_id: Uuid,
    pub trigger_id: Uuid,
    pub event: String,
    /// Canonical Go-compatible {event,eventPayload,request} envelope.
    pub payload: Value,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug)]
enum GuardedDispatch {
    Complete(Result<Uuid, DispatchFailure>),
    TimedOut,
    LeaseLost,
}

async fn guard_dispatch<F, R>(
    dispatch: impl std::future::Future<Output = Result<Uuid, DispatchFailure>>,
    mut renew: F,
) -> Result<GuardedDispatch, &'static str>
where
    F: FnMut() -> R,
    R: std::future::Future<Output = Result<bool, &'static str>>,
{
    tokio::pin!(dispatch);
    let deadline = tokio::time::sleep(DISPATCH_TIMEOUT);
    tokio::pin!(deadline);
    let mut heartbeat = tokio::time::interval(LEASE_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            result = &mut dispatch => return Ok(GuardedDispatch::Complete(result)),
            _ = &mut deadline => return Ok(GuardedDispatch::TimedOut),
            _ = heartbeat.tick() => {
                // Bound pool/DB waits too: a stalled heartbeat must not keep the
                // dispatch alive past its deadline or hide a lost lease.
                let owned = tokio::select! {
                    result = renew() => result?,
                    result = &mut dispatch => return Ok(GuardedDispatch::Complete(result)),
                    _ = &mut deadline => return Ok(GuardedDispatch::TimedOut),
                };
                if !owned {
                    return Ok(GuardedDispatch::LeaseLost);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WebhookWorker {
    pool: Arc<Pool>,
    dispatcher: Arc<dyn WebhookDispatcher>,
    budget: Arc<super::automation_sprint_ingress::IngressLimits>,
}

impl WebhookWorker {
    pub fn new(pool: Arc<Pool>, dispatcher: Arc<dyn WebhookDispatcher>) -> Self {
        Self {
            pool,
            dispatcher,
            budget: Arc::default(),
        }
    }

    /// A bounded four-worker recovery sweeper. It is deliberately not started
    /// by this module: startup must supply a real dispatch/quota adapter.
    pub async fn run(self, shutdown: watch::Receiver<bool>) {
        let mut workers = JoinSet::new();
        for _ in 0..4 {
            let worker = self.clone();
            let mut stop = shutdown.clone();
            workers.spawn(async move {
                while !*stop.borrow() {
                    let worked = tokio::select! {
                        result = worker.process_next() => match result {
                            Ok(worked) => worked,
                            Err(err) => {
                                eprintln!("webhook worker: {err}");
                                false
                            }
                        },
                        _ = stop.changed() => break,
                    };
                    if worked {
                        continue;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                        _ = stop.changed() => break,
                    }
                }
            });
        }
        while workers.join_next().await.is_some() {}
    }

    /// Claim one durable row. A stale lease cannot change its successor's row.
    /// The adapter must be idempotent by delivery_id across process restarts.
    pub async fn process_next(&self) -> Result<bool, &'static str> {
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "worker database unavailable")?;
        let delivery = db.query_opt(
            "WITH candidate AS (SELECT id FROM webhook_delivery WHERE status='queued' AND available_at<=now() AND (lease_expires_at IS NULL OR lease_expires_at<=now()) ORDER BY available_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE webhook_delivery AS d SET lease_token=gen_random_uuid(),lease_expires_at=now()+interval '2 minutes' FROM candidate WHERE d.id=candidate.id RETURNING d.*",
            &[],
        ).await.map_err(|_| "worker claim failed")?;
        let Some(row) = delivery else {
            return Ok(false);
        };
        let id: Uuid = row.get("id");
        let lease: Uuid = row.get("lease_token");
        let trigger_id: Uuid = row.get("trigger_id");
        let ap_id: Uuid = row.get("autopilot_id");
        let ws: Uuid = row.get("workspace_id");
        let attempts: i32 = row.get("dispatch_attempts");
        if !self.budget.check(format!("trigger:{trigger_id}"), 60, true) {
            db.execute("UPDATE webhook_delivery SET available_at=now()+interval '1 second',lease_token=NULL,lease_expires_at=NULL WHERE id=$1 AND lease_token=$2 AND status='queued'", &[&id,&lease])
                .await.map_err(|_| "worker defer failed")?;
            return Ok(true);
        }
        let state = db.query_opt(
            "SELECT a.workspace_id,a.status,t.enabled,t.event_filters FROM autopilot a JOIN autopilot_trigger t ON t.autopilot_id=a.id WHERE a.id=$1 AND t.id=$2",
            &[&ap_id, &trigger_id],
        ).await;
        let state = match state {
            Ok(value) => value,
            Err(_) => {
                self.retry(&db, id, lease, attempts).await?;
                return Ok(true);
            }
        };
        let Some(state) = state else {
            self.complete(&db, id, lease, "failed", None, "delivery_owner_missing")
                .await?;
            return Ok(true);
        };
        if state.get::<_, Uuid>("workspace_id") != ws {
            self.complete(
                &db,
                id,
                lease,
                "failed",
                None,
                "delivery_ownership_mismatch",
            )
            .await?;
            return Ok(true);
        }
        let raw: Option<Vec<u8>> = row.get("raw_body");
        let selected: Value = row.get("selected_headers");
        let payload = raw.as_deref().and_then(|raw| {
            let clean = raw.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(raw);
            serde_json::from_slice::<Value>(clean)
                .ok()
                .filter(|v| v.is_object() || v.is_array())
        });
        let Some(payload) = payload else {
            self.complete(&db, id, lease, "failed", None, "invalid_stored_body")
                .await?;
            return Ok(true);
        };
        let event = super::automation_sprint_deliveries::normalized_event(
            raw.as_deref().unwrap(),
            &selected,
        )
        .map_err(|_| "worker normalization failed")?;
        let signature: String = row.get("signature_status");
        if !matches!(signature.as_str(), "valid" | "not_required") {
            self.complete(&db, id, lease, "failed", None, "invalid_stored_signature")
                .await?;
            return Ok(true);
        }
        let admitted = db.query_opt("SELECT id FROM autopilot_run WHERE webhook_delivery_id=$1 AND autopilot_id=$2 AND trigger_id=$3", &[&id,&ap_id,&trigger_id]).await;
        let admitted = match admitted {
            Ok(value) => value,
            Err(_) => {
                self.retry(&db, id, lease, attempts).await?;
                return Ok(true);
            }
        };
        if admitted.is_none() {
            let status: String = state.get("status");
            let reason = if !state.get::<_, bool>("enabled") {
                Some("trigger_disabled")
            } else if status == "archived" {
                Some("autopilot_archived")
            } else if status != "active" {
                Some("autopilot_paused")
            } else {
                let inner = payload
                    .get("eventPayload")
                    .filter(|_| {
                        payload
                            .get("event")
                            .and_then(Value::as_str)
                            .is_some_and(|event| !event.is_empty())
                    })
                    .unwrap_or(&payload);
                (!event_allowed(
                    state.get::<_, Option<Value>>("event_filters").as_ref(),
                    &event,
                    inner,
                ))
                .then_some("event_filtered")
            };
            if let Some(reason) = reason {
                self.complete(&db, id, lease, "ignored", None, reason)
                    .await?;
                return Ok(true);
            }
        }
        let received_at: DateTime<Utc> = row.get("received_at");
        let inner = payload
            .get("eventPayload")
            .filter(|_| {
                payload
                    .get("event")
                    .and_then(Value::as_str)
                    .is_some_and(|event| !event.is_empty())
            })
            .unwrap_or(&payload);
        let mut request =
            json!({"receivedAt":received_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)});
        if let Some(content) = row
            .get::<_, Option<String>>("content_type")
            .filter(|s| !s.is_empty())
        {
            request["contentType"] = json!(content);
        }
        let envelope = json!({"event":event,"eventPayload":inner,"request":request});
        let job = WebhookJob {
            delivery_id: id,
            workspace_id: ws,
            autopilot_id: ap_id,
            trigger_id,
            event,
            payload: envelope,
            received_at,
        };
        drop(db);
        let outcome = guard_dispatch(self.dispatcher.dispatch(job), || async {
            let db = self
                .pool
                .get()
                .await
                .map_err(|_| "worker database unavailable")?;
            let changed = db
                .execute(
                    "UPDATE webhook_delivery SET lease_expires_at=now()+interval '2 minutes' WHERE id=$1 AND lease_token=$2 AND status='queued' AND lease_expires_at>now()",
                    &[&id, &lease],
                )
                .await
                .map_err(|_| "worker heartbeat failed")?;
            Ok(changed == 1)
        })
        .await?;
        if matches!(outcome, GuardedDispatch::LeaseLost) {
            return Ok(true);
        }
        let db = self
            .pool
            .get()
            .await
            .map_err(|_| "worker database unavailable")?;
        match outcome {
            GuardedDispatch::LeaseLost => unreachable!(),
            GuardedDispatch::TimedOut => self.retry(&db, id, lease, attempts).await?,
            GuardedDispatch::Complete(outcome) => match outcome {
                Err(DispatchFailure::QuotaExceeded) => {
                    self.complete(&db, id, lease, "ignored", None, "quota_exceeded")
                        .await?;
                }
                Err(DispatchFailure::Retry) => {
                    self.retry(&db, id, lease, attempts).await?;
                }
                Ok(run_id) => {
                    // A callback cannot claim success using a fabricated or unrelated run.
                    // Incomplete runs are left queued for idempotent runtime repair.
                    let run = db.query_opt("SELECT status,issue_id,task_id FROM autopilot_run WHERE id=$1 AND webhook_delivery_id=$2 AND autopilot_id=$3 AND trigger_id=$4", &[&run_id,&id,&ap_id,&trigger_id]).await;
                    match run {
                        Err(_) => self.retry(&db, id, lease, attempts).await?,
                        Ok(None) => self.retry(&db, id, lease, attempts).await?,
                        Ok(Some(run)) => {
                            let status: String = run.get("status");
                            let complete =
                                matches!(status.as_str(), "completed" | "skipped" | "failed")
                                    || (status == "issue_created"
                                        && run.get::<_, Option<Uuid>>("issue_id").is_some())
                                    || (status == "running"
                                        && run.get::<_, Option<Uuid>>("task_id").is_some());
                            if !complete {
                                self.retry(&db, id, lease, attempts).await?;
                            } else {
                                let outcome = if status == "failed" {
                                    "failed"
                                } else {
                                    "dispatched"
                                };
                                self.complete(
                                    &db,
                                    id,
                                    lease,
                                    outcome,
                                    Some(run_id),
                                    if status == "failed" { "run_failed" } else { "" },
                                )
                                .await?;
                            }
                        }
                    }
                }
            },
        }
        Ok(true)
    }

    async fn complete(
        &self,
        db: &deadpool_postgres::Object,
        id: Uuid,
        lease: Uuid,
        status: &str,
        run: Option<Uuid>,
        reason: &str,
    ) -> Result<(), &'static str> {
        // One statement keeps acknowledgement and fired-at atomic without
        // borrowing a second pool connection while the first is held.
        db.execute("WITH completed AS (UPDATE webhook_delivery SET status=$3,autopilot_run_id=$4,dispatch_attempts=dispatch_attempts+1,error=NULLIF($5,''),reason_code=NULLIF($5,''),lease_token=NULL,lease_expires_at=NULL,last_attempt_at=now() WHERE id=$1 AND lease_token=$2 AND status='queued' RETURNING trigger_id) UPDATE autopilot_trigger SET last_fired_at=now() WHERE id IN (SELECT trigger_id FROM completed) AND $3='dispatched'", &[&id,&lease,&status,&run,&reason])
            .await.map_err(|_| "worker completion failed")?;
        Ok(())
    }

    async fn retry(
        &self,
        db: &deadpool_postgres::Object,
        id: Uuid,
        lease: Uuid,
        attempts: i32,
    ) -> Result<(), &'static str> {
        if attempts + 1 >= MAX_ATTEMPTS {
            return self
                .complete(db, id, lease, "failed", None, "dispatch_attempts_exhausted")
                .await;
        }
        let delay = 1_i32 << attempts.clamp(0, 6);
        db.execute("UPDATE webhook_delivery SET available_at=now()+($3::int * interval '1 second'),dispatch_attempts=dispatch_attempts+1,error='dispatch_retry',lease_token=NULL,lease_expires_at=NULL,last_attempt_at=now() WHERE id=$1 AND lease_token=$2 AND status='queued'", &[&id,&lease,&delay])
            .await.map_err(|_| "worker retry failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn automation_sprint_worker_stalled_dispatch_times_out() {
        let calls = AtomicUsize::new(0);
        let result = guard_dispatch(std::future::pending(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(true)
        })
        .await
        .unwrap();
        assert!(matches!(result, GuardedDispatch::TimedOut));
        assert!(calls.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test(start_paused = true)]
    async fn automation_sprint_worker_stalled_renewal_is_bounded() {
        let result = guard_dispatch(std::future::pending(), std::future::pending)
            .await
            .unwrap();
        assert!(matches!(result, GuardedDispatch::TimedOut));
    }

    #[tokio::test(start_paused = true)]
    async fn automation_sprint_worker_heartbeat_failure_is_not_success() {
        let result = guard_dispatch(std::future::pending(), || async {
            Err("worker heartbeat failed")
        })
        .await;
        assert_eq!(result.unwrap_err(), "worker heartbeat failed");
    }

    #[tokio::test(start_paused = true)]
    async fn automation_sprint_worker_completed_dispatch_needs_no_heartbeat() {
        let id = Uuid::new_v4();
        let result = guard_dispatch(async { Ok(id) }, || async {
            panic!("completed dispatch must not renew")
        })
        .await
        .unwrap();
        assert!(matches!(result, GuardedDispatch::Complete(Ok(run)) if run == id));
    }

    #[tokio::test(start_paused = true)]
    async fn automation_sprint_worker_stolen_lease_stops_dispatch() {
        let calls = AtomicUsize::new(0);
        let result = guard_dispatch(std::future::pending(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        })
        .await
        .unwrap();
        assert!(matches!(result, GuardedDispatch::LeaseLost));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
