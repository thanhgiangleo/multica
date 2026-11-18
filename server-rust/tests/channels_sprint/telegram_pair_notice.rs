use super::*;
use axum::response::{IntoResponse, Response};
use multica_server::api::channels_sprint_telegram::{
    admission::AdmittedText,
    pairing::PairingDelivery,
    polling::{PrivateTaskEnqueue, TelegramPollSession, supervise_private_text},
};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio_postgres::Transaction;

#[derive(Clone)]
struct NoticeFake {
    poll: FakePoll,
    requests: Arc<Mutex<Vec<Value>>>,
    response: Arc<Mutex<(StatusCode, Option<String>)>>,
    pause: Pause,
    pool: Arc<Pool>,
    installation: Uuid,
}
struct NoticeServer {
    fake: NoticeFake,
    installer: TelegramInstaller,
    handle: tokio::task::JoinHandle<()>,
}
impl NoticeServer {
    async fn new(f: &Fixture) -> Self {
        let fake = NoticeFake {
            poll: f.fake.clone(),
            requests: Arc::new(Mutex::new(vec![])),
            response: Arc::new(Mutex::new((StatusCode::OK, None))),
            pause: Arc::new(Mutex::new(None)),
            pool: f.pool.clone(),
            installation: f.installation,
        };
        async fn route(
            State(fake): State<NoticeFake>,
            uri: axum::http::Uri,
            Json(body): Json<Value>,
        ) -> Response {
            if uri.path() == fake.poll.1 {
                return fake_updates(State(fake.poll), uri, Json(body))
                    .await
                    .into_response();
            }
            assert_eq!(uri.path(), fake.poll.1.replace("getUpdates", "sendMessage"));
            // A different DB connection must already observe the durable marker.
            // The notice cannot run inside the redemption transaction.
            let chat = body["chat_id"].as_i64().unwrap().to_string();
            let committed = fake.pool.get().await.unwrap().query_one(
                "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id LIKE $2 AND processed_at IS NOT NULL",
                &[&fake.installation,&format!("{chat}:%")],
            ).await.unwrap().get::<_,i64>(0);
            assert!(committed > 0, "provider send must follow committed pairing");
            fake.requests.lock().unwrap().push(body.clone());
            let pause = fake.pause.lock().unwrap().clone();
            if let Some((started, release)) = pause {
                started.notify_one();
                release.notified().await;
            }
            let (status, response) = fake.response.lock().unwrap().clone();
            let response = response.unwrap_or_else(|| {
                json!({"ok":true,"result":{"message_id":9001,"chat":{"id":body["chat_id"]}}})
                    .to_string()
            });
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(response))
                .unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .fallback(post(route))
            .with_state(fake.clone());
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            fake,
            installer: TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            handle,
        }
    }
    async fn session(&self, f: &Fixture) -> TelegramPollSession<'_> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match self
                    .installer
                    .open_poll_session(f.pool.clone(), f.workspace, f.installation)
                    .await
                {
                    Ok(session) => return session,
                    Err(PollError::PollerActive) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) => panic!("unexpected notice poll error: {error:?}"),
                }
            }
        })
        .await
        .expect("prior detached connection must close")
    }
    async fn process(&self, f: &Fixture, update: i64, sender: i64, text: &str) -> PairingDelivery {
        f.updates(vec![private(update, update, sender, text)]);
        let mut session = self.session(f).await;
        let updates = session.get_updates().await.unwrap();
        let result = session
            .process_pairing_and_reply(&updates[0])
            .await
            .unwrap();
        assert_eq!(session.offset(), update + 1);
        result
    }
    fn count(&self) -> usize {
        self.fake.requests.lock().unwrap().len()
    }
    fn pause(&self) -> (Arc<Notify>, Arc<Notify>) {
        let pair = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        *self.fake.pause.lock().unwrap() = Some(pair.clone());
        pair
    }
}
impl Drop for NoticeServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_static_post_commit_and_replay_without_bearer_echo() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (id, code, _) = invite(&f).await;
    let text = format!("/pair {code}");
    assert_eq!(
        server.process(&f, 1, 740, &text).await,
        PairingDelivery::Delivered
    );
    assert!(consumed(&f, id).await);
    assert_eq!(
        server.fake.requests.lock().unwrap().as_slice(),
        &[json!({"chat_id":740,"text":PairingNotice::Paired.text().unwrap()})]
    );
    assert!(
        !serde_json::to_string(&*server.fake.requests.lock().unwrap())
            .unwrap()
            .contains(&code)
    );
    assert_eq!(
        server.process(&f, 1, 740, &text).await,
        PairingDelivery::Replay
    );
    assert_eq!(server.count(), 1);
    assert_eq!(attempts(&f, 740).await, 1);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_invalid_and_throttled_are_static_private_only() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    for n in 1..=6 {
        assert_eq!(
            server.process(&f, n, 740, "/pair broken").await,
            PairingDelivery::Delivered
        );
    }
    let requests = server.fake.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 6);
    for (index, request) in requests.iter().enumerate() {
        let notice = if index == 5 {
            PairingNotice::Unavailable
        } else {
            PairingNotice::Invalid
        };
        assert_eq!(
            *request,
            json!({"chat_id":740,"text":notice.text().unwrap()})
        );
    }
    assert_eq!(attempts(&f, 740).await, 6);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_group_silent_and_untrusted_update_never_sends() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let mut group = private(1, 1, 740, "/pair broken");
    group["message"]["chat"] = json!({"id":-740,"type":"supergroup"});
    let mut thread = private(2, 2, 740, "/pair broken");
    thread["message"]["message_thread_id"] = json!(9);
    f.updates(vec![group, thread, private(3, 3, 740, "/pair broken")]);
    let mut poll = server.session(&f).await;
    let items = poll.get_updates().await.unwrap();
    assert_eq!(
        poll.process_pairing_and_reply(&items[1]).await,
        Err(PollError::InvalidResponse)
    );
    assert!(matches!(
        server
            .installer
            .open_poll_session(f.pool.clone(), f.workspace, f.installation)
            .await,
        Err(PollError::PollerActive)
    ));
    assert_eq!(
        poll.process_pairing_and_reply(&items[0]).await.unwrap(),
        PairingDelivery::Silent
    );
    assert_eq!(
        poll.process_pairing_and_reply(&items[1]).await.unwrap(),
        PairingDelivery::Silent
    );
    assert_eq!(server.count(), 0);
    assert_eq!(attempts(&f, 740).await, 0);
    f.pool
        .get()
        .await
        .unwrap()
        .execute(
            "UPDATE channel_installation SET status='revoked' WHERE id=$1",
            &[&f.installation],
        )
        .await
        .unwrap();
    assert_eq!(
        poll.process_pairing_and_reply(&items[2]).await,
        Err(PollError::InvalidInstallation)
    );
    assert_eq!(poll.offset(), 3);
    assert_eq!(server.count(), 0);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_provider_failures_consume_once_and_never_retry() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let cases = [
        (
            StatusCode::FORBIDDEN,
            "provider secret",
            PairingDelivery::Refused,
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            "{\"ok\":false,\"parameters\":{\"retry_after\":1}}",
            PairingDelivery::Refused,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "provider secret",
            PairingDelivery::Unknown,
        ),
        (StatusCode::OK, "invalid JSON", PairingDelivery::Unknown),
        (
            StatusCode::OK,
            "{\"ok\":false,\"error_code\":403,\"description\":\"secret\"}",
            PairingDelivery::Refused,
        ),
        (
            StatusCode::OK,
            "{\"ok\":false,\"error_code\":500}",
            PairingDelivery::Unknown,
        ),
        (
            StatusCode::OK,
            "{\"ok\":true,\"result\":{\"message_id\":0,\"chat\":{\"id\":740}}}",
            PairingDelivery::Unknown,
        ),
        (
            StatusCode::OK,
            "{\"ok\":true,\"result\":{\"message_id\":99,\"chat\":{\"id\":741}}}",
            PairingDelivery::Unknown,
        ),
    ];
    for (n, (status, body, expected)) in cases.into_iter().enumerate() {
        *server.fake.response.lock().unwrap() = (status, Some(body.into()));
        let update = n as i64 + 1;
        assert_eq!(
            server.process(&f, update, 740, "/pair broken").await,
            expected
        );
        assert_eq!(
            server.process(&f, update, 740, "/pair broken").await,
            PairingDelivery::Replay
        );
        assert_eq!(server.count(), n + 1);
        let stored: String = f.pool.get().await.unwrap().query_one(
            "SELECT status FROM channel_pairing_notice WHERE installation_id=$1 AND message_id=$2",
            &[&f.installation,&format!("740:{update}")],
        ).await.unwrap().get(0);
        assert_eq!(
            stored,
            if expected == PairingDelivery::Refused {
                "refused"
            } else {
                "unknown"
            }
        );
    }
    *server.fake.response.lock().unwrap() = (StatusCode::OK, Some("x".repeat(65537)));
    assert_eq!(
        server.process(&f, 9, 740, "/pair broken").await,
        PairingDelivery::Unknown
    );
    assert_eq!(
        server.process(&f, 9, 740, "/pair broken").await,
        PairingDelivery::Replay
    );
    assert_eq!(server.count(), 9);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_commit_failure_never_sends_and_retry_is_safe() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let db = f.pool.get().await.unwrap();
    let function = format!("notice_fail_{}", f.installation.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.installation_id='{}'::uuid AND NEW.processed_at IS NOT NULL THEN RAISE EXCEPTION 'synthetic notice rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} BEFORE UPDATE ON channel_inbound_message_dedup FOR EACH ROW EXECUTE FUNCTION {function}()",f.installation)).await.unwrap();
    let (id, code, _) = invite(&f).await;
    f.updates(vec![private(1, 1, 740, &format!("/pair {code}"))]);
    let mut poll = server.session(&f).await;
    let items = poll.get_updates().await.unwrap();
    let result = poll.process_pairing_and_reply(&items[0]).await;
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON channel_inbound_message_dedup; DROP FUNCTION {function}()"
    ))
    .await
    .unwrap();
    assert_eq!(result, Err(PollError::Database));
    assert_eq!(poll.offset(), 0);
    assert_eq!(server.count(), 0);
    assert!(!consumed(&f, id).await);
    assert_eq!(attempts(&f, 740).await, 0);
    assert_eq!(
        poll.process_pairing_and_reply(&items[0]).await.unwrap(),
        PairingDelivery::Delivered
    );
    assert!(consumed(&f, id).await);
    assert_eq!(server.count(), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_concurrent_replay_cannot_send_twice() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (started, release) = server.pause();
    f.updates(vec![private(1, 1, 740, "/pair broken")]);
    let mut first = server.session(&f).await;
    let a = first.get_updates().await.unwrap();
    assert!(matches!(
        server
            .installer
            .open_poll_session(f.pool.clone(), f.workspace, f.installation)
            .await,
        Err(PollError::PollerActive)
    ));
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first.process_pairing_and_reply(&a[0]), async {
            started.notified().await;
            assert!(matches!(
                server
                    .installer
                    .open_poll_session(f.pool.clone(), f.workspace, f.installation)
                    .await,
                Err(PollError::PollerActive)
            ));
            assert_eq!(server.count(), 1);
            release.notify_one();
        })
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap(), PairingDelivery::Delivered);
    drop(first);
    assert_eq!(
        server.process(&f, 1, 740, "/pair broken").await,
        PairingDelivery::Replay
    );
    assert_eq!(server.count(), 1);
    assert_eq!(attempts(&f, 740).await, 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_holds_generation_and_archive_fences_through_http() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (started, release) = server.pause();
    f.updates(vec![private(1, 1, 740, "/pair broken")]);
    let mut poll = server.session(&f).await;
    let items = poll.get_updates().await.unwrap();
    let (result,())=tokio::time::timeout(Duration::from_secs(8),async {
        tokio::join!(poll.process_pairing_and_reply(&items[0]),async {
            started.notified().await;
            let mut db=f.pool.get().await.unwrap();
            for (sql,id) in [
                ("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb(gen_random_uuid()::text)) WHERE id=$1",f.installation),
                ("UPDATE channel_installation SET status='revoked' WHERE id=$1",f.installation),
                ("UPDATE agent SET archived_at=now() WHERE id=$1",f.agent),
            ] {
                let tx=db.transaction().await.unwrap();
                tx.batch_execute("SET LOCAL lock_timeout='100ms'").await.unwrap();
                assert_eq!(tx.execute(sql,&[&id]).await.unwrap_err().code(),Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE));
                tx.rollback().await.unwrap();
            }
            release.notify_one();
        })
    }).await.unwrap();
    assert_eq!(result.unwrap(), PairingDelivery::Delivered);
    f.pool.get().await.unwrap().execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb(gen_random_uuid()::text)) WHERE id=$1",&[&f.installation]).await.unwrap();
    assert_eq!(
        poll.get_updates().await.err(),
        Some(PollError::InvalidInstallation)
    );
    assert_eq!(server.count(), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_cancellation_after_commit_does_not_resend_on_restart() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (started, release) = server.pause();
    f.updates(vec![private(1, 1, 740, "/pair broken")]);
    let mut poll = server.session(&f).await;
    let items = poll.get_updates().await.unwrap();
    {
        let send = poll.process_pairing_and_reply(&items[0]);
        tokio::pin!(send);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _=started.notified()=>{},
                result=&mut send=>panic!("send should be held: {result:?}"),
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(poll.offset(), 2);
    release.notify_one();
    drop(poll);
    assert_eq!(
        server.process(&f, 1, 740, "/pair broken").await,
        PairingDelivery::Replay
    );
    assert_eq!(server.count(), 1);
    assert_eq!(attempts(&f, 740).await, 1);
}

struct NoticeEnqueue;
#[async_trait]
impl PrivateTaskEnqueue for NoticeEnqueue {
    async fn enqueue(
        &self,
        tx: &Transaction<'_>,
        admitted: &AdmittedText,
    ) -> Result<Uuid, ClaimError> {
        let runtime:Uuid=tx.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider) VALUES($1,'Synthetic notice runtime','local','synthetic') RETURNING id",&[&admitted.workspace]).await.map_err(|_|ClaimError::Database)?.get(0);
        Ok(tx.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,initiator_user_id,channel_context_revision) VALUES($1,$2,$3,$4,$5) RETURNING id",&[&admitted.agent,&runtime,&admitted.session,&admitted.sender,&admitted.context_revision]).await.map_err(|_|ClaimError::Database)?.get(0))
    }
}
#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_supervisor_pair_send_refusal_does_not_block_following_guest_turn() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (id, code, _) = invite(&f).await;
    *server.fake.response.lock().unwrap() =
        (StatusCode::FORBIDDEN, Some("synthetic blocked bot".into()));
    f.updates(vec![
        private(1, 1, 740, &format!("/pair {code}")),
        private(2, 2, 740, "synthetic guest text"),
    ]);
    f.fake.0.lock().unwrap().0["_respect_offset"] = json!(true);
    let (stop, rx) = watch::channel(false);
    let (result,())=tokio::time::timeout(Duration::from_secs(8),async {
        tokio::join!(supervise_private_text(&server.installer,f.pool.clone(),f.workspace,f.installation,rx,Arc::new(NoticeEnqueue)),async {
            loop {
                let row=f.pool.get().await.unwrap().query_opt("SELECT t.initiator_user_id,u.zbs_is_shadow,m.content FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id JOIN \"user\" u ON u.id=t.initiator_user_id JOIN chat_message m ON m.chat_session_id=t.chat_session_id WHERE a.workspace_id=$1",&[&f.workspace]).await.unwrap();
                if let Some(row)=row {
                    assert!(row.get::<_,bool>(1));
                    assert_eq!(row.get::<_,String>(2),"synthetic guest text");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            stop.send(true).unwrap();
        })
    }).await.unwrap();
    assert_eq!(result, Ok(()));
    assert!(consumed(&f, id).await);
    assert_eq!(server.count(), 1);
    assert_eq!(attempts(&f, 740).await, 1);
    assert_eq!(
        server.process(&f, 1, 740, &format!("/pair {code}")).await,
        PairingDelivery::Replay
    );
    assert_eq!(server.count(), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_recovers_unattempted_after_restart_without_bearer() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let (invite_id, code, _) = invite(&f).await;
    f.updates(vec![private(1, 1, 740, &format!("/pair {code}"))]);
    let mut first = server.session(&f).await;
    let items = first.get_updates().await.unwrap();
    assert_eq!(
        first.process_pairing(&items[0]).await.unwrap(),
        PollCommit::Committed(PairingNotice::Paired)
    );
    assert!(consumed(&f, invite_id).await);
    let db = f.pool.get().await.unwrap();
    let row = db.query_one("SELECT status,kind,chat_id FROM channel_pairing_notice WHERE installation_id=$1 AND message_id='740:1'", &[&f.installation]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, String>(1), "paired");
    assert_eq!(row.get::<_, i64>(2), 740);
    assert_eq!(server.count(), 0);
    drop(first);
    let second = server.session(&f).await;
    assert_eq!(second.recover_pairing_notices().await.unwrap(), 1);
    assert_eq!(second.recover_pairing_notices().await.unwrap(), 0);
    assert_eq!(server.count(), 1);
    assert_eq!(db.query_one("SELECT status FROM channel_pairing_notice WHERE installation_id=$1 AND message_id='740:1'", &[&f.installation]).await.unwrap().get::<_,String>(0), "delivered");
    assert!(
        !serde_json::to_string(&*server.fake.requests.lock().unwrap())
            .unwrap()
            .contains(&code)
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_attempted_and_rotated_never_recover() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    f.updates(vec![
        private(1, 1, 740, "/pair broken"),
        private(2, 2, 740, "/pair broken"),
    ]);
    let mut first = server.session(&f).await;
    let items = first.get_updates().await.unwrap();
    assert_eq!(
        first.process_pairing(&items[0]).await.unwrap(),
        PollCommit::Committed(PairingNotice::Invalid)
    );
    assert_eq!(
        first.process_pairing(&items[1]).await.unwrap(),
        PollCommit::Committed(PairingNotice::Invalid)
    );
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE channel_pairing_notice SET status='attempted' WHERE installation_id=$1 AND message_id='740:1'", &[&f.installation]).await.unwrap();
    drop(first);
    let second = server.session(&f).await;
    assert_eq!(second.recover_pairing_notices().await.unwrap(), 1);
    assert_eq!(server.count(), 1);
    drop(second);
    let original: String = db
        .query_one(
            "SELECT config->>'generation' FROM channel_installation WHERE id=$1",
            &[&f.installation],
        )
        .await
        .unwrap()
        .get(0);
    let fresh = Uuid::new_v4().to_string();
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&f.installation,&fresh]).await.unwrap();
    db.execute("UPDATE channel_pairing_notice SET status='pending' WHERE installation_id=$1 AND message_id='740:2'", &[&f.installation]).await.unwrap();
    let third = server.session(&f).await;
    assert_eq!(third.recover_pairing_notices().await.unwrap(), 0);
    assert_eq!(server.count(), 1);
    assert_ne!(original, fresh);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_notice_rollback_leaves_no_recoverable_notice() {
    let f = Fixture::new().await;
    let server = NoticeServer::new(&f).await;
    let db = f.pool.get().await.unwrap();
    let name = format!("notice_rollback_{}", f.installation.simple());
    db.batch_execute(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.installation_id='{}'::uuid AND NEW.processed_at IS NOT NULL THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {name} BEFORE UPDATE ON channel_inbound_message_dedup FOR EACH ROW EXECUTE FUNCTION {name}()",f.installation)).await.unwrap();
    f.updates(vec![private(1, 1, 740, "/pair broken")]);
    let mut session = server.session(&f).await;
    let items = session.get_updates().await.unwrap();
    assert_eq!(
        session.process_pairing(&items[0]).await,
        Err(PollError::Database)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_pairing_notice WHERE installation_id=$1",
            &[&f.installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {name} ON channel_inbound_message_dedup; DROP FUNCTION {name}()"
    ))
    .await
    .unwrap();
    assert_eq!(session.recover_pairing_notices().await.unwrap(), 0);
    assert_eq!(server.count(), 0);
}
