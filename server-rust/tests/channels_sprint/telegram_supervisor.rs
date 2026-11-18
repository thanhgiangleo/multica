use super::*;
use multica_server::api::channels_sprint_telegram::{
    admission::AdmittedText,
    polling::{PrivateTaskEnqueue, supervise_private_text},
};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::watch;
use tokio_postgres::Transaction;

// Synthetic durable sink, not a scheduler implementation. Tests prove the
// admission/dedup/enqueue transaction boundary without modifying runtime code.
#[derive(Default)]
struct Enqueue {
    fail: AtomicBool,
    wrong_task: AtomicBool,
    calls: AtomicUsize,
    pause: Pause,
}
#[async_trait]
impl PrivateTaskEnqueue for Enqueue {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        admitted: &AdmittedText,
    ) -> Result<Uuid, ClaimError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.wrong_task.load(Ordering::SeqCst) {
            return Ok(Uuid::new_v4());
        }
        let runtime: Uuid = tx.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider) VALUES($1,'Synthetic supervisor runtime','local','synthetic') RETURNING id", &[&admitted.workspace]).await.map_err(|_| ClaimError::Database)?.get(0);
        let task: Uuid = tx.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,initiator_user_id,channel_context_revision) VALUES($1,$2,$3,$4,$5) RETURNING id", &[&admitted.agent,&runtime,&admitted.session,&admitted.sender,&admitted.context_revision]).await.map_err(|_| ClaimError::Database)?.get(0);
        tx.execute(
            "INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason,installation_id) VALUES('telegram','supervisor-enqueue','synthetic',$1)",
            &[&admitted.installation],
        ).await.map_err(|_| ClaimError::Database)?;
        let pause = self.pause.lock().unwrap().clone();
        if let Some((started, release)) = pause {
            started.notify_one();
            release.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(ClaimError::Database)
        } else {
            Ok(task)
        }
    }
}
async fn audit_count(f: &Fixture) -> i64 {
    f.pool.get().await.unwrap().query_one(
        "SELECT count(*) FROM channel_inbound_audit WHERE installation_id=$1 AND event_type='supervisor-enqueue'",
        &[&f.installation],
    ).await.unwrap().get(0)
}
async fn wait_for_audits(f: &Fixture, n: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while audit_count(f).await != n {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn wait_for_calls(f: &Fixture, n: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if f.fake.0.lock().unwrap().1.len() >= n {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_atomic_enqueue_failure_replay_and_product_drop() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    enqueue.fail.store(true, Ordering::SeqCst);
    f.updates(vec![private(1, 11, 731, "member text")]);
    let mut session = f.session().await;
    let updates = session.get_updates().await.unwrap();
    assert_eq!(
        session
            .process_private_text_with_enqueue(&updates[0], enqueue.clone())
            .await
            .err(),
        Some(PollError::Database)
    );
    assert_eq!(session.offset(), 0);
    assert_eq!(audit_count(&f).await, 0);
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_session WHERE workspace_id=$1",
            &[&f.workspace]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&f.installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    enqueue.fail.store(false, Ordering::SeqCst);
    enqueue.wrong_task.store(true, Ordering::SeqCst);
    assert_eq!(
        session
            .process_private_text_with_enqueue(&updates[0], enqueue.clone())
            .await
            .err(),
        Some(PollError::InvalidInstallation)
    );
    assert_eq!(audit_count(&f).await, 0);
    enqueue.wrong_task.store(false, Ordering::SeqCst);
    assert!(matches!(
        session
            .process_private_text_with_enqueue(&updates[0], enqueue.clone())
            .await
            .unwrap(),
        PollCommit::Committed(Admission::Persisted(_))
    ));
    assert_eq!(session.offset(), 2);
    assert_eq!(audit_count(&f).await, 1);
    let pinned = db.query_one("SELECT t.status,d.channel_message_id,d.zbs_guest_shadow_user_id FROM agent_task_queue t JOIN channel_task_delivery d ON d.task_id=t.id WHERE d.installation_id=$1", &[&f.installation]).await.unwrap();
    assert_eq!(pinned.get::<_, String>(0), "queued");
    assert_eq!(
        pinned.get::<_, Option<String>>(1).as_deref(),
        Some("731:11")
    );
    assert_eq!(pinned.get::<_, Option<Uuid>>(2), None);
    drop(session);
    let mut replay = f.session().await;
    let updates = replay.get_updates().await.unwrap();
    assert_eq!(
        replay
            .process_private_text_with_enqueue(&updates[0], enqueue.clone())
            .await
            .unwrap(),
        PollCommit::Replay
    );
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 3);
    f.updates(vec![private(2, 12, 999, "unbound text")]);
    let updates = replay.get_updates().await.unwrap();
    assert_eq!(
        replay
            .process_private_text_with_enqueue(&updates[0], enqueue.clone())
            .await
            .unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 3);
    assert_eq!(replay.offset(), 3);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_batch_commits_and_restarts_without_duplicate_enqueue() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    f.updates(vec![
        private(1, 11, 731, "member text"),
        private(2, 12, 733, "guest text"),
    ]);
    f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                wait_for_audits(&f, 2).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 2);
    assert_eq!(audit_count(&f).await, 2);
    // A fresh connection starts at zero and safely deduplicates its first batch.
    let calls = f.fake.0.lock().unwrap().1.len();
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                wait_for_calls(&f, calls + 1).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 2);
    assert_eq!(audit_count(&f).await, 2);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_busy_claim_retries_same_batch_without_ack() {
    use multica_server::api::channels_sprint_ingress::{
        ClaimResult, InboundKey, claim_inbound, release_inbound, telegram_poll_generation,
    };
    let f = Fixture::new().await;
    f.updates(vec![private(1, 11, 731, "member text")]);
    f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
    let key = InboundKey {
        workspace: f.workspace,
        installation: f.installation,
        channel: Channel::Telegram,
        message_id: "731:11".into(),
        generation: Some(
            telegram_poll_generation(&f.pool, f.workspace, f.installation)
                .await
                .unwrap(),
        ),
    };
    let claim = match claim_inbound(&f.pool, key).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        _ => panic!("fixture claim"),
    };
    let enqueue = Arc::new(Enqueue::default());
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                wait_for_calls(&f, 1).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(audit_count(&f).await, 0);
                assert_eq!(f.fake.0.lock().unwrap().1.len(), 1);
                assert!(release_inbound(&f.pool, &claim).await.unwrap());
                wait_for_audits(&f, 1).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_transient_backoff_recovers_and_rotation_stops() {
    let f = Fixture::new().await;
    f.fake.0.lock().unwrap().0 = json!({"ok":false,"error_code":503});
    let enqueue = Arc::new(Enqueue::default());
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                wait_for_calls(&f, 1).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(f.fake.0.lock().unwrap().1.len(), 1);
                f.updates(vec![private(1, 11, 731, "member text")]);
                f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
                wait_for_audits(&f, 1).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 1);
    // The same frozen epoch must survive retries; never reconnect with new credentials.
    f.fake.0.lock().unwrap().0 =
        json!({"ok":false,"error_code":429,"parameters":{"retry_after":1}});
    let before = f.fake.0.lock().unwrap().1.len();
    let (_stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            supervise_private_text(&f.installer, f.pool.clone(), f.workspace, f.installation, rx, enqueue.clone()),
            async {
                wait_for_calls(&f, before+1).await;
                f.pool.get().await.unwrap().execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($1::text)) WHERE id=$2", &[&Uuid::new_v4().to_string(),&f.installation]).await.unwrap();
            }
        )
    }).await.unwrap();
    assert_eq!(result, Err(PollError::InvalidInstallation));
    assert_eq!(f.fake.0.lock().unwrap().1.len(), before + 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_cancel_http_and_before_start() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    let (stop, rx) = watch::channel(true);
    assert_eq!(
        supervise_private_text(
            &f.installer,
            f.pool.clone(),
            f.workspace,
            f.installation,
            rx,
            enqueue.clone()
        )
        .await,
        Ok(())
    );
    assert!(f.fake.0.lock().unwrap().1.is_empty());
    drop(stop);
    f.updates(vec![private(1, 11, 731, "member text")]);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *f.fake.2.lock().unwrap() = Some((started.clone(), release.clone()));
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                started.notified().await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    release.notify_one();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 0);
    assert_eq!(audit_count(&f).await, 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_stops_conflict_credentials_and_unsupported_without_ack() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    for (response, expected) in [
        (json!({"ok":false,"error_code":409}), PollError::Conflict),
        (
            json!({"ok":false,"error_code":401}),
            PollError::CredentialsRejected,
        ),
        (
            json!({"ok":true,"result":[private(1, 11, 731, "/start")]}),
            PollError::UnsupportedUpdate,
        ),
    ] {
        f.fake.0.lock().unwrap().0 = response;
        let (_stop, rx) = watch::channel(false);
        let before = f.fake.0.lock().unwrap().1.len();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                supervise_private_text(
                    &f.installer,
                    f.pool.clone(),
                    f.workspace,
                    f.installation,
                    rx,
                    enqueue.clone()
                )
            )
            .await
            .unwrap(),
            Err(expected)
        );
        assert_eq!(f.fake.0.lock().unwrap().1.len(), before + 1);
    }
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 0);
    assert_eq!(audit_count(&f).await, 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_cancel_enqueue_rolls_back_and_expired_claim_recovers() {
    let f = Fixture::new().await;
    f.updates(vec![private(1, 11, 731, "member text")]);
    f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
    let enqueue = Arc::new(Enqueue::default());
    let started = Arc::new(tokio::sync::Notify::new());
    *enqueue.pause.lock().unwrap() = Some((started.clone(), Arc::new(tokio::sync::Notify::new())));
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                started.notified().await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(audit_count(&f).await, 0);
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_session WHERE workspace_id=$1",
            &[&f.workspace]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE agent_id=$1",
            &[&f.agent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let processed: bool = db.query_one("SELECT processed_at IS NOT NULL FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id='731:11'", &[&f.installation]).await.unwrap().get(0);
    assert!(!processed);
    // Simulate the existing 60-second abandoned claim lease, not production repair.
    db.execute("UPDATE channel_inbound_message_dedup SET received_at=now()-interval '61 seconds' WHERE installation_id=$1", &[&f.installation]).await.unwrap();
    *enqueue.pause.lock().unwrap() = None;
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue.clone()
            ),
            async {
                wait_for_audits(&f, 1).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE agent_id=$1",
            &[&f.agent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_cancel_rate_limit_backoff_and_closed_owner() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    for response in [
        json!({"ok":false,"error_code":429,"parameters":{"retry_after":9000}}),
        json!({"ok":false,"error_code":503}),
    ] {
        f.fake.0.lock().unwrap().0 = response;
        let before = f.fake.0.lock().unwrap().1.len();
        let (stop, rx) = watch::channel(false);
        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                supervise_private_text(
                    &f.installer,
                    f.pool.clone(),
                    f.workspace,
                    f.installation,
                    rx,
                    enqueue.clone()
                ),
                async {
                    wait_for_calls(&f, before + 1).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    drop(stop);
                }
            )
        })
        .await
        .unwrap();
        assert_eq!(result, Ok(()));
        assert_eq!(f.fake.0.lock().unwrap().1.len(), before + 1);
    }
    assert_eq!(enqueue.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_lease_blocks_other_replicas_and_releases_on_cancel() {
    let f = Fixture::new().await;
    let enqueue = Arc::new(Enqueue::default());
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *f.fake.2.lock().unwrap() = Some((started.clone(), release.clone()));
    let (stop, first_rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                first_rx,
                enqueue.clone(),
            ),
            async {
                started.notified().await;
                let calls = f.fake.0.lock().unwrap().1.len();
                let (_other_stop, other_rx) = watch::channel(false);
                let second = supervise_private_text(
                    &f.installer,
                    f.pool.clone(),
                    f.workspace,
                    f.installation,
                    other_rx,
                    enqueue.clone(),
                )
                .await;
                assert_eq!(second, Err(PollError::PollerActive));
                assert_eq!(f.fake.0.lock().unwrap().1.len(), calls);
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
    release.notify_one();
    *f.fake.2.lock().unwrap() = None;
    let (stop, rx) = watch::channel(false);
    let before = f.fake.0.lock().unwrap().1.len();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            supervise_private_text(
                &f.installer,
                f.pool.clone(),
                f.workspace,
                f.installation,
                rx,
                enqueue
            ),
            async {
                wait_for_calls(&f, before + 1).await;
                stop.send(true).unwrap();
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_reacquires_dead_lease_without_acknowledging_lost_response() {
    let f = Fixture::new().await;
    f.updates(vec![private(1, 11, 731, "member text")]);
    f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *f.fake.2.lock().unwrap() = Some((started.clone(), release.clone()));
    let enqueue = Arc::new(Enqueue::default());
    let (stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            supervise_private_text(&f.installer, f.pool.clone(), f.workspace, f.installation, rx, enqueue.clone()),
            async {
                started.notified().await;
                let db = f.pool.get().await.unwrap();
                let pid: i32 = db.query_one(
                    "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid()",
                    &[],
                ).await.unwrap().get(0);
                assert!(db.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await.unwrap().get::<_, bool>(0));
                *f.fake.2.lock().unwrap() = None;
                release.notify_one();
                wait_for_audits(&f, 1).await;
                assert_eq!(enqueue.calls.load(Ordering::SeqCst), 1);
                assert!(f.fake.0.lock().unwrap().1.len() >= 2);
                stop.send(true).unwrap();
            }
        )
    }).await.unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(audit_count(&f).await, 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_dead_lease_rejects_rotated_generation() {
    let f = Fixture::new().await;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *f.fake.2.lock().unwrap() = Some((started.clone(), release.clone()));
    let (_stop, rx) = watch::channel(false);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            supervise_private_text(&f.installer, f.pool.clone(), f.workspace, f.installation, rx, Arc::new(Enqueue::default())),
            async {
                started.notified().await;
                let db = f.pool.get().await.unwrap();
                let pid: i32 = db.query_one(
                    "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid()",
                    &[],
                ).await.unwrap().get(0);
                assert!(db.query_one("SELECT pg_terminate_backend($1)", &[&pid]).await.unwrap().get::<_, bool>(0));
                db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($1::text)) WHERE id=$2", &[&Uuid::new_v4().to_string(),&f.installation]).await.unwrap();
                *f.fake.2.lock().unwrap() = None;
                release.notify_one();
            }
        )
    }).await.unwrap();
    assert_eq!(result, Err(PollError::InvalidInstallation));
    assert_eq!(f.fake.0.lock().unwrap().1.len(), 1);
}
