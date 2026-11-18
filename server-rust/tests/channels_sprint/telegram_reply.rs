use super::*;
use axum::response::Response;
use multica_server::api::channels_sprint_telegram::{
    admission::AdmittedText,
    reply::{ReplyError, ReplyOutcome, deliver_completed_private_text, snapshot_private_task},
};

#[derive(Clone)]
struct ReplyFake {
    requests: Arc<Mutex<Vec<Value>>>,
    response: Arc<Mutex<(StatusCode, Option<String>)>>,
    path: String,
    pause: Pause,
}
struct ReplyServer {
    fake: ReplyFake,
    installer: Arc<TelegramInstaller>,
    handle: tokio::task::JoinHandle<()>,
}
impl ReplyServer {
    async fn new(f: &Fixture) -> Self {
        let fake = ReplyFake {
            requests: Arc::new(Mutex::new(vec![])),
            response: Arc::new(Mutex::new((StatusCode::OK, None))),
            path: f.fake.1.replace("getUpdates", "sendMessage"),
            pause: Arc::new(Mutex::new(None)),
        };
        async fn send(
            State(fake): State<ReplyFake>,
            uri: axum::http::Uri,
            Json(body): Json<Value>,
        ) -> Response {
            assert_eq!(
                uri.path(),
                fake.path,
                "only synthetic sendMessage permitted"
            );
            let number = {
                let mut requests = fake.requests.lock().unwrap();
                requests.push(body.clone());
                requests.len() as i64
            };
            let pause = fake.pause.lock().unwrap().clone();
            if let Some((started, release)) = pause {
                started.notify_one();
                release.notified().await;
            }
            let (status, response) = fake.response.lock().unwrap().clone();
            let response = response.unwrap_or_else(|| json!({"ok":true,"result":{"message_id":9000+number,"chat":{"id":body["chat_id"]}}}).to_string());
            Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(response))
                .unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .fallback(post(send))
            .with_state(fake.clone());
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            fake,
            installer: Arc::new(TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap()),
            handle,
        }
    }
    async fn deliver(&self, f: &Fixture, task: Uuid) -> Result<ReplyOutcome, ReplyError> {
        deliver_completed_private_text(&self.installer, f.pool.clone(), task).await
    }
    fn count(&self) -> usize {
        self.fake.requests.lock().unwrap().len()
    }
}
impl Drop for ReplyServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
async fn queued(
    tx: &tokio_postgres::Transaction<'_>,
    f: &Fixture,
    admitted: &AdmittedText,
) -> Uuid {
    let runtime: Uuid = tx.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider) VALUES($1,'Synthetic reply runtime','local','synthetic') RETURNING id", &[&f.workspace]).await.unwrap().get(0);
    tx.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,chat_session_id,initiator_user_id,channel_context_revision) VALUES($1,$2,$3,$4,$5) RETURNING id", &[&f.agent,&runtime,&admitted.session,&admitted.sender,&admitted.context_revision]).await.unwrap().get(0)
}
async fn completed(f: &Fixture, update: i64, external: i64) -> (Uuid, AdmittedText) {
    let admitted = match f.admit(update, external).await {
        PollCommit::Committed(Admission::Persisted(text)) => text,
        _ => panic!("synthetic text must persist"),
    };
    let mut db = f.pool.get().await.unwrap();
    let tx = db.transaction().await.unwrap();
    let task = queued(&tx, f, &admitted).await;
    snapshot_private_task(&tx, task).await.unwrap();
    tx.commit().await.unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO chat_message(chat_session_id,role,task_id,content,channel_context_revision) VALUES($1,'assistant',$2,'synthetic final reply',$3)", &[&admitted.session,&task,&admitted.context_revision]).await.unwrap();
    (task, admitted)
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_freezes_trigger_delivers_once_and_records_provenance() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, admitted) = completed(&f, 101, 731).await;
    // A later same-context message must not change this task's reply anchor.
    f.admit(102, 731).await;
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Delivered
    );
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::AlreadySettled
    );
    assert_eq!(server.count(), 1);
    let requests = server.fake.requests.lock().unwrap().clone();
    assert_eq!(
        requests[0],
        json!({"chat_id":731,"text":"synthetic final reply","reply_parameters":{"message_id":101,"allow_sending_without_reply":true}})
    );
    let db = f.pool.get().await.unwrap();
    let row=db.query_one("SELECT phase,send_state,message_id,chunks_sent,owner_token,settled_reason FROM channel_reply_delivery WHERE turn_id=$1", &[&task]).await.unwrap();
    assert_eq!(row.get::<_, String>("phase"), "settled");
    assert_eq!(row.get::<_, String>("send_state"), "known");
    assert_eq!(row.get::<_, String>("message_id"), "9001");
    assert_eq!(row.get::<_, i32>("chunks_sent"), 1);
    assert_eq!(row.get::<_, Option<Uuid>>("owner_token"), None);
    let sent=db.query_one("SELECT binding_id,route_revision,channel_type FROM channel_outbound_message WHERE installation_id=$1 AND channel_message_id='9001'", &[&f.installation]).await.unwrap();
    assert_eq!(sent.get::<_, Uuid>(0), admitted.binding);
    assert_eq!(sent.get::<_, i64>(1), admitted.route_revision);
    assert_eq!(sent.get::<_, String>(2), "telegram");
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_fences_generation_route_membership_and_context() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, a) = completed(&f, 111, 731).await;
    let db = f.pool.get().await.unwrap();
    for (mutate, restore) in [
        (
            "UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb('00000000-0000-0000-0000-000000000073'::text)) WHERE id=$1",
            "",
        ),
        (
            "UPDATE channel_installation SET status='revoked' WHERE id=$1",
            "UPDATE channel_installation SET status='active' WHERE id=$1",
        ),
        (
            "UPDATE channel_installation SET workspace_id=gen_random_uuid() WHERE id=$1",
            "",
        ),
    ] {
        let old: Value = db
            .query_one(
                "SELECT config FROM channel_installation WHERE id=$1",
                &[&f.installation],
            )
            .await
            .unwrap()
            .get(0);
        db.execute(mutate, &[&f.installation]).await.unwrap();
        assert_eq!(
            server.deliver(&f, task).await.unwrap(),
            ReplyOutcome::Denied
        );
        db.execute(
            "UPDATE channel_installation SET config=$2,workspace_id=$3 WHERE id=$1",
            &[&f.installation, &old, &f.workspace],
        )
        .await
        .unwrap();
        if !restore.is_empty() {
            db.execute(restore, &[&f.installation]).await.unwrap();
        }
    }
    db.execute(
        "UPDATE channel_chat_session_binding SET route_revision=route_revision+1 WHERE id=$1",
        &[&a.binding],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE channel_chat_session_binding SET route_revision=route_revision-1 WHERE id=$1",
        &[&a.binding],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE channel_chat_context_generation SET pending_fresh=true WHERE chat_session_id=$1",
        &[&a.session],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE channel_chat_context_generation SET pending_fresh=false WHERE chat_session_id=$1",
        &[&a.session],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&f.workspace, &f.member],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&f.workspace, &f.member],
    )
    .await
    .unwrap();
    db.execute("UPDATE channel_user_binding SET multica_user_id=$2 WHERE installation_id=$1 AND channel_user_id='731'", &[&f.installation,&f.shadow]).await.unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    assert_eq!(server.count(), 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_reply_delivery WHERE turn_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_guest_repair_and_invocation_policy_fail_closed() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, _) = completed(&f, 121, 733).await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE zbs_telegram_guest_grant SET grant_generation=2 WHERE installation_id=$1",
        &[&f.installation],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE zbs_telegram_guest_grant SET grant_generation=1 WHERE installation_id=$1",
        &[&f.installation],
    )
    .await
    .unwrap();
    db.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&f.agent],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'member',$2)", &[&f.agent,&f.shadow]).await.unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'workspace',$2)", &[&f.agent,&f.workspace]).await.unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Delivered
    );
    assert_eq!(server.fake.requests.lock().unwrap()[0]["chat_id"], 733);
    assert_eq!(
        db.query_one("SELECT count(*) FROM member WHERE user_id=$1", &[&f.shadow])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_lost_response_and_abandoned_send_never_resend() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, _) = completed(&f, 131, 731).await;
    *server.fake.response.lock().unwrap() = (
        StatusCode::BAD_GATEWAY,
        Some("synthetic ambiguous response".into()),
    );
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Unknown
    );
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::AlreadySettled
    );
    assert_eq!(server.count(), 1);
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT send_state FROM channel_reply_delivery WHERE turn_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "unknown"
    );
    let (abandoned, a) = completed(&f, 132, 731).await;
    db.execute("INSERT INTO channel_reply_delivery(turn_id,task_id,binding_id,installation_id,channel_type,chat_id,phase,send_state,owner_token,owner_expires_at) VALUES($1,$1,$2,$3,'telegram','731','terminal','in_flight',$4,now()-interval '1 minute')", &[&abandoned,&a.binding,&f.installation,&Uuid::new_v4()]).await.unwrap();
    assert_eq!(
        server.deliver(&f, abandoned).await.unwrap(),
        ReplyOutcome::Unknown
    );
    assert_eq!(server.count(), 1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_outbound_message WHERE task_id IN ($1,$2)",
            &[&task, &abandoned]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_refusal_and_invalid_provider_payload_are_not_success() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let db = f.pool.get().await.unwrap();
    for (index, status, body, outcome) in [
        (
            141,
            StatusCode::TOO_MANY_REQUESTS,
            json!({"ok":false,"error_code":429,"description":"synthetic token echo"}).to_string(),
            ReplyOutcome::Refused,
        ),
        (
            142,
            StatusCode::OK,
            "bad JSON".into(),
            ReplyOutcome::Unknown,
        ),
        (
            143,
            StatusCode::OK,
            json!({"ok":true,"result":{"message_id":1,"chat":{"id":733}}}).to_string(),
            ReplyOutcome::Unknown,
        ),
        (
            144,
            StatusCode::OK,
            "x".repeat(65537),
            ReplyOutcome::Unknown,
        ),
    ] {
        let (task, _) = completed(&f, index, 731).await;
        *server.fake.response.lock().unwrap() = (status, Some(body));
        assert_eq!(server.deliver(&f, task).await.unwrap(), outcome);
        assert_eq!(
            server.deliver(&f, task).await.unwrap(),
            ReplyOutcome::AlreadySettled
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM channel_outbound_message WHERE task_id=$1",
                &[&task]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0
        );
    }
    assert_eq!(server.count(), 4);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_snapshot_rollback_missing_snapshot_and_retry_task_are_denied() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, a) = completed(&f, 151, 731).await;
    let mut db = f.pool.get().await.unwrap();
    db.execute(
        "DELETE FROM channel_task_delivery WHERE task_id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    let tx = db.transaction().await.unwrap();
    let rolled = queued(&tx, &f, &a).await;
    snapshot_private_task(&tx, rolled).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_task_delivery WHERE task_id=$1",
            &[&rolled]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let (retry, _) = completed(&f, 152, 731).await;
    db.execute(
        "UPDATE agent_task_queue SET retry_of_task_id=$2 WHERE id=$1",
        &[&retry, &task],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, retry).await.unwrap(),
        ReplyOutcome::Denied
    );
    assert_eq!(server.count(), 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_accepted_then_database_failure_stays_unknown() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (first, _) = completed(&f, 171, 731).await;
    assert_eq!(
        server.deliver(&f, first).await.unwrap(),
        ReplyOutcome::Delivered
    );
    let (task, _) = completed(&f, 172, 731).await;
    // Force provenance collision after the fake provider accepts: transaction
    // rolls back, but the previously committed in-flight marker must survive.
    *server.fake.response.lock().unwrap() = (
        StatusCode::OK,
        Some(json!({"ok":true,"result":{"message_id":9001,"chat":{"id":731}}}).to_string()),
    );
    assert_eq!(server.deliver(&f, task).await, Err(ReplyError::Database));
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT send_state FROM channel_reply_delivery WHERE turn_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "in_flight"
    );
    assert_eq!(server.deliver(&f, task).await.unwrap(), ReplyOutcome::Busy);
    db.execute("UPDATE channel_reply_delivery SET owner_expires_at=now()-interval '1 second' WHERE turn_id=$1", &[&task]).await.unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Unknown
    );
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::AlreadySettled
    );
    assert_eq!(server.count(), 2);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_outbound_message WHERE task_id=$1",
            &[&task]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_rejects_stale_answer_cancelled_task_and_multipart() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, admitted) = completed(&f, 181, 731).await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE chat_message SET channel_context_revision=NULL WHERE task_id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE chat_message SET channel_context_revision=$2 WHERE task_id=$1",
        &[&task, &admitted.context_revision],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent_task_queue SET status='cancelled' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE chat_message SET content=$2 WHERE task_id=$1",
        &[&task, &"x".repeat(4097)],
    )
    .await
    .unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    db.execute(
        "UPDATE chat_message SET content='synthetic first part' WHERE task_id=$1",
        &[&task],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO chat_message(chat_session_id,role,task_id,content,channel_context_revision) VALUES($1,'assistant',$2,'synthetic second part',$3)", &[&admitted.session,&task,&admitted.context_revision]).await.unwrap();
    assert_eq!(
        server.deliver(&f, task).await.unwrap(),
        ReplyOutcome::Denied
    );
    assert_eq!(server.count(), 0);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_reply_concurrent_owners_and_rotation_are_serialized() {
    let f = Fixture::new().await;
    let server = ReplyServer::new(&f).await;
    let (task, _) = completed(&f, 161, 731).await;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *server.fake.pause.lock().unwrap() = Some((started.clone(), release.clone()));
    let send = tokio::spawn({
        let installer = server.installer.clone();
        let pool = f.pool.clone();
        async move { deliver_completed_private_text(&installer, pool, task).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    // A competing caller cannot acquire the committed in-flight turn.
    let duplicate = tokio::spawn({
        let installer = server.installer.clone();
        let pool = f.pool.clone();
        async move { deliver_completed_private_text(&installer, pool, task).await }
    });
    let mut db = f.pool.get().await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.batch_execute("SET LOCAL lock_timeout='100ms'")
        .await
        .unwrap();
    let rotate=tx.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb(gen_random_uuid()::text)) WHERE id=$1", &[&f.installation]).await;
    assert_eq!(
        rotate.unwrap_err().code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE)
    );
    tx.rollback().await.unwrap();
    release.notify_one();
    assert_eq!(send.await.unwrap().unwrap(), ReplyOutcome::Delivered);
    let duplicate = duplicate.await.unwrap().unwrap();
    assert!(matches!(
        duplicate,
        ReplyOutcome::AlreadySettled | ReplyOutcome::Busy
    ));
    assert_eq!(server.count(), 1);
}
