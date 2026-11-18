use super::*;
use multica_server::api::channels_sprint_telegram::{CredentialBox, admission::Admission};
#[path = "telegram_pair.rs"]
mod pairing_tests;
#[path = "telegram_reply.rs"]
mod reply_tests;
#[path = "telegram_supervisor.rs"]
mod supervisor_tests;

struct Fixture {
    pool: Arc<Pool>,
    fake: FakePoll,
    server: tokio::task::JoinHandle<()>,
    installer: TelegramInstaller,
    workspace: Uuid,
    installation: Uuid,
    agent: Uuid,
    member: Uuid,
    shadow: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
        assert!(cfg.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
        ));
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(8)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let owner = insert_user(&db, "admit-owner", &suffix).await;
        let member = insert_user(&db, "admit-member", &suffix).await;
        let shadow = insert_user(&db, "admit-shadow", &suffix).await;
        db.execute(
            "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
            &[&shadow],
        )
        .await
        .unwrap();
        let workspace: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Admit test',$1) RETURNING id",
                &[&format!("admit-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        for (user, role) in [(owner, "admin"), (member, "member")] {
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
                &[&workspace, &user, &role],
            )
            .await
            .unwrap();
        }
        let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode) VALUES($1,'Admit agent','local',$2,'public_to') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
        db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'workspace',$2)", &[&agent,&workspace]).await.unwrap();
        let bot_id = (u64::from_str_radix(&suffix[..12], 16).unwrap() + 100000).to_string();
        let token = format!("{bot_id}:synthetic_secret");
        let fake = FakePoll(
            Arc::new(Mutex::new((json!({"ok":true,"result":[]}), vec![]))),
            format!("/bot{token}/getUpdates"),
            Arc::new(Mutex::new(None)),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn({
            let fake = fake.clone();
            async move {
                axum::serve(
                    listener,
                    axum::Router::new()
                        .fallback(post(fake_updates))
                        .with_state(fake),
                )
                .await
                .unwrap()
            }
        });
        let installer = TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
        let config = json!({"generation":Uuid::new_v4().to_string(),"app_id":bot_id,"bot_token_encrypted":CredentialBox::new(&[73;32]).unwrap().seal(token.as_bytes()).unwrap()});
        let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'telegram',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&config]).await.unwrap().get(0);
        db.execute("INSERT INTO channel_user_binding(workspace_id,installation_id,channel_type,channel_user_id,multica_user_id) VALUES($1,$2,'telegram','731',$3)", &[&workspace,&installation,&member]).await.unwrap();
        db.execute("INSERT INTO zbs_telegram_guest_grant(installation_id,workspace_id,agent_id,telegram_user_id,shadow_user_id,alias,invite_id) VALUES($1,$2,$3,'733',$4,'Synthetic guest',$5)", &[&installation,&workspace,&agent,&shadow,&Uuid::new_v4()]).await.unwrap();
        Self {
            pool,
            fake,
            server,
            installer,
            workspace,
            installation,
            agent,
            member,
            shadow,
        }
    }
    fn updates(&self, updates: Vec<Value>) {
        self.fake.0.lock().unwrap().0 = json!({"ok":true,"result":updates});
    }
    async fn session(
        &self,
    ) -> multica_server::api::channels_sprint_telegram::polling::TelegramPollSession<'_> {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match self
                    .installer
                    .open_poll_session(self.pool.clone(), self.workspace, self.installation)
                    .await
                {
                    Ok(session) => return session,
                    Err(PollError::PollerActive) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await
                    }
                    Err(error) => panic!("unexpected poll session error: {error:?}"),
                }
            }
        })
        .await
        .expect("prior detached connection must close")
    }
    async fn admit(&self, update: i64, sender: i64) -> PollCommit<Admission> {
        self.updates(vec![private(
            update,
            update,
            sender,
            "synthetic private text",
        )]);
        let mut poll = self.session().await;
        let items = poll.get_updates().await.unwrap();
        poll.process_private_text(&items[0]).await.unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn private(update: i64, message: i64, sender: i64, body: &str) -> Value {
    json!({"update_id":update,"message":{"message_id":message,"from":{"id":sender,"is_bot":false},"chat":{"id":sender,"type":"private"},"text":body}})
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_member_guest_generation_and_replay() {
    let fixture = Fixture::new().await;
    let db = fixture.pool.get().await.unwrap();
    fixture.updates(vec![
        private(1, 11, 731, "member hello"),
        private(2, 12, 733, "guest hello"),
        private(3, 13, 734, "unknown hello"),
        private(4, 14, 731, "member again"),
    ]);
    let mut poll = fixture.session().await;
    let updates = poll.get_updates().await.unwrap();
    let first = match poll.process_private_text(&updates[0]).await.unwrap() {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("member must persist"),
    };
    assert!(first.created_session);
    assert_eq!(first.sender, fixture.member);
    assert_eq!(first.guest_generation, None);
    let guest = match poll.process_private_text(&updates[1]).await.unwrap() {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("guest must persist"),
    };
    assert!(guest.created_session);
    assert_ne!(guest.session, first.session);
    assert_eq!(guest.sender, fixture.shadow);
    assert_eq!(guest.guest_generation, Some(1));
    assert_eq!(
        poll.process_private_text(&updates[2]).await.unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    let repeat = match poll.process_private_text(&updates[3]).await.unwrap() {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("member repeat must persist"),
    };
    assert!(!repeat.created_session);
    assert_eq!(
        (repeat.session, repeat.binding, repeat.context_revision),
        (first.session, first.binding, first.context_revision)
    );
    assert_eq!(poll.offset(), 5);
    let member_data = db.query_one("SELECT cs.creator_id, cs.zbs_has_guest_history, c.initiator_user_id, c.last_message_id, b.last_message_id, count(m.id) FROM chat_session cs JOIN channel_chat_session_binding b ON b.chat_session_id=cs.id JOIN channel_chat_context_generation c ON c.chat_session_id=cs.id AND c.revision=b.context_revision JOIN chat_message m ON m.chat_session_id=cs.id WHERE cs.id=$1 GROUP BY cs.creator_id,cs.zbs_has_guest_history,c.initiator_user_id,c.last_message_id,b.last_message_id", &[&first.session]).await.unwrap();
    assert_eq!(member_data.get::<_, Uuid>(0), fixture.member);
    assert!(!member_data.get::<_, bool>(1));
    assert_eq!(member_data.get::<_, Option<Uuid>>(2), Some(fixture.member));
    assert_eq!(
        member_data.get::<_, Option<String>>(3).as_deref(),
        Some("731:14")
    );
    assert_eq!(
        member_data.get::<_, Option<String>>(4).as_deref(),
        Some("731:14")
    );
    assert_eq!(member_data.get::<_, i64>(5), 2);
    let guest_data = db.query_one("SELECT cs.creator_id,cs.zbs_has_guest_history,c.initiator_user_id,c.zbs_guest_shadow_user_id,c.zbs_guest_grant_generation,m.zbs_sender_user_id,m.zbs_sender_external_id,m.content FROM chat_session cs JOIN channel_chat_session_binding b ON b.chat_session_id=cs.id JOIN channel_chat_context_generation c ON c.chat_session_id=cs.id AND c.revision=b.context_revision JOIN chat_message m ON m.chat_session_id=cs.id WHERE cs.id=$1", &[&guest.session]).await.unwrap();
    assert_eq!(guest_data.get::<_, Uuid>(0), fixture.shadow);
    assert!(guest_data.get::<_, bool>(1));
    assert_eq!(guest_data.get::<_, Option<Uuid>>(2), Some(fixture.shadow));
    assert_eq!(guest_data.get::<_, Option<Uuid>>(3), Some(fixture.shadow));
    assert_eq!(guest_data.get::<_, Option<i64>>(4), Some(1));
    assert_eq!(guest_data.get::<_, Option<Uuid>>(5), Some(fixture.shadow));
    assert_eq!(
        guest_data.get::<_, Option<String>>(6).as_deref(),
        Some("733")
    );
    assert_eq!(guest_data.get::<_, String>(7), "guest hello");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM member WHERE user_id=$1",
            &[&fixture.shadow]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_task_queue WHERE chat_session_id IN ($1,$2)",
            &[&first.session, &guest.session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    drop(poll);
    let mut replay = fixture.session().await;
    let duplicate = replay.get_updates().await.unwrap();
    for update in &duplicate {
        assert_eq!(
            replay.process_private_text(update).await.unwrap(),
            PollCommit::Replay
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id IN ($1,$2)",
            &[&first.session, &guest.session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        3
    );
    db.execute("UPDATE zbs_telegram_guest_grant SET revoked_at=now(),revoked_by_user_id=$2 WHERE installation_id=$1 AND telegram_user_id='733'", &[&fixture.installation,&fixture.member]).await.unwrap();
    fixture.updates(vec![private(5, 15, 733, "revoked guest")]);
    drop(replay);
    let mut revoked = fixture.session().await;
    let item = revoked.get_updates().await.unwrap();
    assert_eq!(
        revoked.process_private_text(&item[0]).await.unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute("UPDATE zbs_telegram_guest_grant SET revoked_at=NULL,revoked_by_user_id=NULL,grant_generation=2 WHERE installation_id=$1 AND telegram_user_id='733'", &[&fixture.installation]).await.unwrap();
    fixture.updates(vec![private(6, 16, 733, "renewed generation")]);
    drop(revoked);
    let mut renewed = fixture.session().await;
    let item = renewed.get_updates().await.unwrap();
    assert_eq!(
        renewed.process_private_text(&item[0]).await.unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute("INSERT INTO channel_user_binding(workspace_id,installation_id,channel_type,channel_user_id,multica_user_id) VALUES($1,$2,'telegram','733',$3)", &[&fixture.workspace,&fixture.installation,&fixture.member]).await.unwrap();
    fixture.updates(vec![private(7, 17, 733, "bound to another member")]);
    drop(renewed);
    let mut rebound = fixture.session().await;
    let item = rebound.get_updates().await.unwrap();
    assert_eq!(
        rebound.process_private_text(&item[0]).await.unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&fixture.workspace, &fixture.member],
    )
    .await
    .unwrap();
    fixture.updates(vec![private(8, 18, 731, "stale member")]);
    drop(rebound);
    let mut stale = fixture.session().await;
    let item = stale.get_updates().await.unwrap();
    assert_eq!(
        stale.process_private_text(&item[0]).await.unwrap(),
        PollCommit::Committed(Admission::Dropped)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id IN ($1,$2)",
            &[&first.session, &guest.session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        3
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NOT NULL", &[&fixture.installation]).await.unwrap().get::<_,i64>(0),8);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_failure_rolls_back_route_message_and_dedup() {
    let fixture = Fixture::new().await;
    let db = fixture.pool.get().await.unwrap();
    fixture.updates(vec![private(20, 21, 731, "retry after rollback")]);
    let mut poll = fixture.session().await;
    let updates = poll.get_updates().await.unwrap();
    let contexts_before: i64 = db
        .query_one("SELECT count(*) FROM channel_chat_context_generation", &[])
        .await
        .unwrap()
        .get(0);
    db.batch_execute("CREATE FUNCTION channels_sprint_admit_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic private admission failure'; END $$; CREATE TRIGGER channels_sprint_admit_failure BEFORE INSERT ON chat_message FOR EACH ROW EXECUTE FUNCTION channels_sprint_admit_failure();").await.unwrap();
    assert_eq!(
        poll.process_private_text(&updates[0]).await,
        Err(PollError::Database)
    );
    assert_eq!(poll.offset(), 0);
    for (table, sql) in [
        (
            "chat_session",
            "SELECT count(*) FROM chat_session WHERE agent_id=$1",
        ),
        (
            "channel_chat_session_binding",
            "SELECT count(*) FROM channel_chat_session_binding WHERE chat_session_id IN (SELECT id FROM chat_session WHERE agent_id=$1)",
        ),
        (
            "channel_chat_context_generation",
            "SELECT count(*) FROM channel_chat_context_generation WHERE chat_session_id IN (SELECT id FROM chat_session WHERE agent_id=$1)",
        ),
        (
            "chat_message",
            "SELECT count(*) FROM chat_message WHERE chat_session_id IN (SELECT id FROM chat_session WHERE agent_id=$1)",
        ),
    ] {
        let count: i64 = db.query_one(sql, &[&fixture.agent]).await.unwrap().get(0);
        assert_eq!(count, 0, "{table} must roll back");
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&fixture.installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM channel_chat_context_generation", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        contexts_before
    );
    db.batch_execute("DROP TRIGGER channels_sprint_admit_failure ON chat_message; DROP FUNCTION channels_sprint_admit_failure();").await.unwrap();
    assert!(matches!(
        poll.process_private_text(&updates[0]).await,
        Ok(PollCommit::Committed(Admission::Persisted(_)))
    ));
    assert_eq!(poll.offset(), 21);
    assert_eq!(db.query_one("SELECT count(*) FROM chat_message WHERE chat_session_id IN (SELECT id FROM chat_session WHERE agent_id=$1)", &[&fixture.agent]).await.unwrap().get::<_,i64>(0),1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_requires_current_invocation_permission_and_live_installation() {
    let fixture = Fixture::new().await;
    let db = fixture.pool.get().await.unwrap();
    let owner: Uuid = db
        .query_one("SELECT owner_id FROM agent WHERE id=$1", &[&fixture.agent])
        .await
        .unwrap()
        .get(0);
    assert!(matches!(
        fixture.admit(40, 731).await,
        PollCommit::Committed(Admission::Persisted(_))
    ));
    db.execute("INSERT INTO channel_user_binding(workspace_id,installation_id,channel_type,channel_user_id,multica_user_id) VALUES($1,$2,'telegram','732',$3)", &[&fixture.workspace,&fixture.installation,&owner]).await.unwrap();
    db.execute(
        "DELETE FROM agent_invocation_target WHERE agent_id=$1",
        &[&fixture.agent],
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.admit(41, 731).await,
        PollCommit::Committed(Admission::Dropped)
    );
    assert_eq!(
        fixture.admit(42, 733).await,
        PollCommit::Committed(Admission::Dropped)
    );
    assert!(matches!(
        fixture.admit(43, 732).await,
        PollCommit::Committed(Admission::Persisted(_))
    ));
    db.execute(
        "UPDATE agent SET permission_mode='private' WHERE id=$1",
        &[&fixture.agent],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&fixture.workspace, &fixture.member],
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.admit(44, 731).await,
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute(
        "UPDATE agent SET owner_id=NULL WHERE id=$1",
        &[&fixture.agent],
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.admit(45, 732).await,
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute(
        "UPDATE agent SET permission_mode='public_to' WHERE id=$1",
        &[&fixture.agent],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'member',$2)", &[&fixture.agent,&fixture.member]).await.unwrap();
    assert!(matches!(
        fixture.admit(46, 731).await,
        PollCommit::Committed(Admission::Persisted(_))
    ));
    assert_eq!(
        fixture.admit(47, 733).await,
        PollCommit::Committed(Admission::Dropped)
    );
    fixture.updates(vec![private(48, 48, 731, "revoked installation")]);
    let mut revoked = fixture.session().await;
    let updates = revoked.get_updates().await.unwrap();
    db.execute(
        "UPDATE channel_installation SET status='revoked' WHERE id=$1",
        &[&fixture.installation],
    )
    .await
    .unwrap();
    assert_eq!(
        revoked.process_private_text(&updates[0]).await,
        Err(PollError::InvalidInstallation)
    );
    assert_eq!(revoked.offset(), 0);
    assert_eq!(db.query_one("SELECT count(*) FROM chat_message WHERE chat_session_id IN (SELECT id FROM chat_session WHERE agent_id=$1)", &[&fixture.agent]).await.unwrap().get::<_,i64>(0),3);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_pending_context_and_invalid_binding_fail_closed() {
    let fixture = Fixture::new().await;
    let db = fixture.pool.get().await.unwrap();
    let admitted = match fixture.admit(60, 731).await {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("fixture admission"),
    };
    fixture.updates(vec![private(61, 61, 731, "pending context retry")]);
    let mut poll = fixture.session().await;
    let updates = poll.get_updates().await.unwrap();
    for change in [
        "UPDATE channel_chat_session_binding SET history_boundary_pending=true WHERE chat_session_id=$1",
        "UPDATE channel_chat_session_binding SET history_boundary_pending=false WHERE chat_session_id=$1; UPDATE channel_chat_context_generation SET pending_fresh=true WHERE chat_session_id=$1",
    ] {
        // Each statement is scoped to this disposable fixture's session.
        for statement in change.split(';') {
            db.execute(statement, &[&admitted.session]).await.unwrap();
        }
        assert_eq!(
            poll.process_private_text(&updates[0]).await,
            Err(PollError::InvalidInstallation)
        );
        assert_eq!(poll.offset(), 0);
    }
    db.execute("UPDATE channel_chat_context_generation SET pending_fresh=false,history_boundary_pending=true WHERE chat_session_id=$1",&[&admitted.session]).await.unwrap();
    assert_eq!(
        poll.process_private_text(&updates[0]).await,
        Err(PollError::InvalidInstallation)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id=$1",
            &[&admitted.session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id='731:61'",&[&fixture.installation]).await.unwrap().get::<_,i64>(0),0);
    db.execute("UPDATE channel_chat_context_generation SET history_boundary_pending=false WHERE chat_session_id=$1",&[&admitted.session]).await.unwrap();
    assert!(matches!(
        poll.process_private_text(&updates[0]).await,
        Ok(PollCommit::Committed(Admission::Persisted(_)))
    ));
    assert_eq!(poll.offset(), 62);
    drop(poll);
    db.execute("UPDATE zbs_telegram_guest_grant SET telegram_user_id='735' WHERE installation_id=$1 AND telegram_user_id='733'",&[&fixture.installation]).await.unwrap();
    db.execute("INSERT INTO channel_user_binding(workspace_id,installation_id,channel_type,channel_user_id,multica_user_id) VALUES($1,$2,'telegram','735',$3)",&[&Uuid::new_v4(),&fixture.installation,&fixture.member]).await.unwrap();
    assert_eq!(
        fixture.admit(62, 735).await,
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute("UPDATE channel_user_binding SET workspace_id=$2,channel_type='slack' WHERE installation_id=$1 AND channel_user_id='735'",&[&fixture.installation,&fixture.workspace]).await.unwrap();
    assert_eq!(
        fixture.admit(63, 735).await,
        PollCommit::Committed(Admission::Dropped)
    );
    db.execute("UPDATE channel_user_binding SET channel_type='telegram' WHERE installation_id=$1 AND channel_user_id='735'",&[&fixture.installation]).await.unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&fixture.workspace, &fixture.member],
    )
    .await
    .unwrap();
    assert_eq!(
        fixture.admit(64, 735).await,
        PollCommit::Committed(Admission::Dropped)
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_chat_session_binding WHERE installation_id=$1 AND channel_chat_id='735'",&[&fixture.installation]).await.unwrap().get::<_,i64>(0),0);
    db.execute(
        "DELETE FROM channel_user_binding WHERE installation_id=$1 AND channel_user_id='735'",
        &[&fixture.installation],
    )
    .await
    .unwrap();
    assert!(
        matches!(fixture.admit(65,735).await,PollCommit::Committed(Admission::Persisted(value)) if value.sender == fixture.shadow)
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_private_rejects_unsupported_and_serializes_first_messages() {
    let fixture = Fixture::new().await;
    let db = fixture.pool.get().await.unwrap();
    fixture.updates(vec![private(30, 31, 731, "   /new hidden command")]);
    let mut command = fixture.session().await;
    let item = command.get_updates().await.unwrap();
    assert_eq!(
        command.process_private_text(&item[0]).await,
        Err(PollError::UnsupportedUpdate)
    );
    assert_eq!(command.offset(), 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&fixture.installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    fixture.updates(vec![private(31, 32, 731, "first sequential")]);
    drop(command);
    let mut first = fixture.session().await;
    let one = first.get_updates().await.unwrap();
    assert!(matches!(
        fixture
            .installer
            .open_poll_session(
                fixture.pool.clone(),
                fixture.workspace,
                fixture.installation
            )
            .await,
        Err(PollError::PollerActive)
    ));
    let a = match first.process_private_text(&one[0]).await.unwrap() {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("first admission"),
    };
    fixture.updates(vec![private(32, 33, 731, "second sequential")]);
    let two = first.get_updates().await.unwrap();
    let b = match first.process_private_text(&two[0]).await.unwrap() {
        PollCommit::Committed(Admission::Persisted(value)) => value,
        _ => panic!("second admission"),
    };
    assert_eq!((a.session, a.binding), (b.session, b.binding));
    assert_ne!(a.created_session, b.created_session);
    assert_eq!(db.query_one("SELECT count(*) FROM channel_chat_session_binding WHERE installation_id=$1 AND channel_chat_id='731' AND retired_at IS NULL", &[&fixture.installation]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE chat_session_id=$1",
            &[&a.session]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_poll_lost_lease_fails_closed_before_provider_io() {
    let fixture = Fixture::new().await;
    let mut session = fixture.session().await;
    let db = fixture.pool.get().await.unwrap();
    let pid: i32 = db.query_one(
        "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid()",
        &[],
    ).await.unwrap().get(0);
    let terminated: bool = db
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
    assert!(matches!(
        session.get_updates().await,
        Err(PollError::LeaseLost)
    ));
    assert_eq!(session.offset(), 0);
    assert!(fixture.fake.0.lock().unwrap().1.is_empty());
    drop(session);
    let mut restarted = fixture.session().await;
    assert!(restarted.get_updates().await.unwrap().is_empty());
    assert_eq!(fixture.fake.0.lock().unwrap().1.len(), 1);
}
