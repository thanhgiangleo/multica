use super::*;
#[path = "telegram_pair_notice.rs"]
mod notice_tests;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use multica_server::api::channels_sprint_telegram::pairing::PairingNotice;
use sha2::{Digest, Sha256};

async fn invite(f: &Fixture) -> (Uuid, String, String) {
    let db = f.pool.get().await.unwrap();
    let id = Uuid::new_v4();
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).unwrap();
    let code = URL_SAFE_NO_PAD.encode(bytes);
    let hash = hex::encode(Sha256::digest(code.as_bytes()));
    let alias = format!("pair-{id}");
    db.execute("INSERT INTO zbs_telegram_guest_invite(id,workspace_id,installation_id,agent_id,code_hash,issued_by_user_id,alias,expires_at) SELECT $1,$2,$3,id,$4,owner_id,$5,now()+interval '1 hour' FROM agent WHERE id=$6", &[&id,&f.workspace,&f.installation,&hash,&alias,&f.agent]).await.unwrap();
    (id, code, alias)
}
async fn process(f: &Fixture, update: i64, sender: i64, text: &str) -> PollCommit<PairingNotice> {
    f.updates(vec![private(update, update, sender, text)]);
    let mut poll = f.session().await;
    let items = poll.get_updates().await.unwrap();
    let result = poll.process_pairing(&items[0]).await.unwrap();
    assert_eq!(poll.offset(), update + 1);
    result
}
async fn attempts(f: &Fixture, sender: i64) -> i64 {
    let scope = format!("sender:{}:{sender}", f.installation);
    let hash = hex::encode(Sha256::digest(scope.as_bytes()));
    f.pool.get().await.unwrap().query_one("SELECT coalesce(sum(attempts),0)::bigint FROM zbs_telegram_guest_attempt WHERE scope_hash=$1", &[&hash]).await.unwrap().get(0)
}
async fn no_content(f: &Fixture) {
    let db = f.pool.get().await.unwrap();
    for sql in [
        "SELECT count(*) FROM chat_session WHERE workspace_id=$1",
        "SELECT count(*) FROM chat_message m JOIN chat_session s ON s.id=m.chat_session_id WHERE s.workspace_id=$1",
        "SELECT count(*) FROM agent_task_queue q JOIN agent a ON a.id=q.agent_id WHERE a.workspace_id=$1",
        "SELECT count(*) FROM channel_chat_session_binding b JOIN channel_installation i ON i.id=b.installation_id WHERE i.workspace_id=$1",
    ] {
        assert_eq!(
            db.query_one(sql, &[&f.workspace])
                .await
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }
}
async fn consumed(f: &Fixture, id: Uuid) -> bool {
    f.pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT consumed_at IS NOT NULL FROM zbs_telegram_guest_invite WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_atomic_private_replay_and_subsequent_guest_admission() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE channel_installation SET config=config || '{\"bot_username\":\"Mine\"}'::jsonb WHERE id=$1", &[&f.installation]).await.unwrap();
    let (id, code, alias) = invite(&f).await;
    let text = format!("@mInE /start@Mine {code}");
    assert_eq!(
        process(&f, 1, 740, &text).await,
        PollCommit::Committed(PairingNotice::Paired)
    );
    assert!(consumed(&f, id).await);
    assert_eq!(attempts(&f, 740).await, 1);
    assert_eq!(process(&f, 1, 740, &text).await, PollCommit::Replay);
    assert_eq!(attempts(&f, 740).await, 1);
    let row = db.query_one("SELECT g.shadow_user_id,g.grant_generation,g.alias,u.zbs_is_shadow FROM zbs_telegram_guest_grant g JOIN \"user\" u ON u.id=g.shadow_user_id WHERE g.installation_id=$1 AND g.telegram_user_id='740'", &[&f.installation]).await.unwrap();
    let shadow: Uuid = row.get(0);
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(row.get::<_, String>(2), alias);
    assert!(row.get::<_, bool>(3));
    assert_eq!(
        db.query_one("SELECT count(*) FROM member WHERE user_id=$1", &[&shadow])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id='740:1' AND processed_at IS NOT NULL", &[&f.installation]).await.unwrap().get::<_,i64>(0),1);
    no_content(&f).await;
    match f.admit(2, 740).await {
        PollCommit::Committed(Admission::Persisted(value)) => {
            assert_eq!(value.sender, shadow);
            assert_eq!(value.guest_generation, Some(1));
        }
        _ => panic!("paired guest must admit without secret content"),
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_message WHERE content LIKE $1",
            &[&format!("%{code}%")]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_malformed_budget_and_sender_isolation() {
    let f = Fixture::new().await;
    let (id, code, _) = invite(&f).await;
    for (i, text) in [
        format!("please /pair {code}"),
        format!("/PAIR {code}"),
        format!("/pair {code} extra"),
        format!("https://t.me/Mine?start={code}"),
        "/pair broken".into(),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(
            process(&f, i as i64 + 1, 740, text).await,
            PollCommit::Committed(PairingNotice::Invalid)
        );
    }
    assert_eq!(
        process(&f, 6, 740, &format!("/pair {code}")).await,
        PollCommit::Committed(PairingNotice::Unavailable)
    );
    assert_eq!(attempts(&f, 740).await, 6);
    assert!(!consumed(&f, id).await);
    assert_eq!(
        process(&f, 7, 741, &format!("/start@Other {code}")).await,
        PollCommit::Committed(PairingNotice::Paired)
    );
    assert_eq!(attempts(&f, 741).await, 1);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_groups_silent_and_bare_commands_pending() {
    let f = Fixture::new().await;
    let (id, code, _) = invite(&f).await;
    let mut group = private(1, 1, 740, &format!("/pair {code}"));
    group["message"]["chat"] = json!({"id":-740,"type":"supergroup"});
    f.updates(vec![group]);
    let mut poll = f.session().await;
    let items = poll.get_updates().await.unwrap();
    assert_eq!(
        poll.process_pairing(&items[0]).await.unwrap(),
        PollCommit::Committed(PairingNotice::Silent)
    );
    assert_eq!(attempts(&f, 740).await, 0);
    assert!(!consumed(&f, id).await);
    drop(poll);
    for text in ["/start", "/pair", "/start pair", "ordinary text"] {
        f.updates(vec![private(2, 2, 740, text)]);
        let mut poll = f.session().await;
        let items = poll.get_updates().await.unwrap();
        assert_eq!(
            poll.process_pairing(&items[0]).await,
            Err(PollError::UnsupportedUpdate)
        );
        assert_eq!(poll.offset(), 0);
    }
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_member_active_guest_and_revoked_generation() {
    let f = Fixture::new().await;
    let (id, code, _) = invite(&f).await;
    for (update, sender) in [(1, 731), (2, 733)] {
        assert_eq!(
            process(&f, update, sender, &format!("/pair {code}")).await,
            PollCommit::Committed(PairingNotice::Invalid)
        );
        assert!(!consumed(&f, id).await);
    }
    let db = f.pool.get().await.unwrap();
    db.execute("UPDATE zbs_telegram_guest_grant SET revoked_at=now(),revoked_by_user_id=$2 WHERE installation_id=$1 AND telegram_user_id='733'", &[&f.installation,&f.member]).await.unwrap();
    assert_eq!(
        process(&f, 3, 733, &format!("/pair {code}")).await,
        PollCommit::Committed(PairingNotice::Paired)
    );
    let row = db.query_one("SELECT shadow_user_id,grant_generation FROM zbs_telegram_guest_grant WHERE installation_id=$1 AND telegram_user_id='733'", &[&f.installation]).await.unwrap();
    assert_eq!(row.get::<_, Uuid>(0), f.shadow);
    assert_eq!(row.get::<_, i64>(1), 2);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_rechecks_invite_scope_owner_membership_and_sharing() {
    for case in 0..7 {
        let f = Fixture::new().await;
        let (id, code, _) = invite(&f).await;
        let db = f.pool.get().await.unwrap();
        match case {
            0 => {
                db.execute("UPDATE zbs_telegram_guest_invite SET created_at=now()-interval '2 hours',expires_at=now()-interval '1 hour' WHERE id=$1", &[&id]).await.unwrap();
            }
            1 => {
                db.execute("UPDATE zbs_telegram_guest_invite SET revoked_at=now(),revoked_by_user_id=$2 WHERE id=$1", &[&id,&f.member]).await.unwrap();
            }
            2 => {
                db.execute(
                    "UPDATE zbs_telegram_guest_invite SET workspace_id=$2 WHERE id=$1",
                    &[&id, &Uuid::new_v4()],
                )
                .await
                .unwrap();
            }
            3 => {
                db.execute(
                    "UPDATE zbs_telegram_guest_invite SET agent_id=$2 WHERE id=$1",
                    &[&id, &Uuid::new_v4()],
                )
                .await
                .unwrap();
            }
            4 => {
                db.execute(
                    "UPDATE agent SET owner_id=$2 WHERE id=$1",
                    &[&f.agent, &f.member],
                )
                .await
                .unwrap();
            }
            5 => {
                db.execute("DELETE FROM member WHERE workspace_id=$1 AND user_id=(SELECT owner_id FROM agent WHERE id=$2)", &[&f.workspace,&f.agent]).await.unwrap();
            }
            _ => {
                db.execute(
                    "DELETE FROM agent_invocation_target WHERE agent_id=$1",
                    &[&f.agent],
                )
                .await
                .unwrap();
            }
        }
        assert_eq!(
            process(&f, 1, 740, &format!("/pair {code}")).await,
            PollCommit::Committed(PairingNotice::Invalid)
        );
        assert!(!consumed(&f, id).await);
        assert_eq!(attempts(&f, 740).await, 1);
        assert_eq!(db.query_one("SELECT count(*) FROM zbs_telegram_guest_grant WHERE installation_id=$1 AND telegram_user_id='740'", &[&f.installation]).await.unwrap().get::<_,i64>(0),0);
        no_content(&f).await;
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_connection_order_rotation_and_archive_fences() {
    for case in 0..3 {
        let f = Fixture::new().await;
        let (id, code, _) = invite(&f).await;
        f.updates(vec![
            private(1, 1, 740, &format!("/pair {code}")),
            private(2, 2, 741, &format!("/pair {code}")),
        ]);
        let mut poll = f.session().await;
        let items = poll.get_updates().await.unwrap();
        assert_eq!(
            poll.process_pairing(&items[1]).await,
            Err(PollError::InvalidResponse)
        );
        assert!(matches!(
            f.installer
                .open_poll_session(f.pool.clone(), f.workspace, f.installation)
                .await,
            Err(PollError::PollerActive)
        ));
        let db = f.pool.get().await.unwrap();
        match case {
            0 => {
                db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&f.installation,&Uuid::new_v4().to_string()]).await.unwrap();
            }
            1 => {
                db.execute(
                    "UPDATE channel_installation SET status='revoked' WHERE id=$1",
                    &[&f.installation],
                )
                .await
                .unwrap();
            }
            _ => {
                db.execute(
                    "UPDATE agent SET archived_at=now() WHERE id=$1",
                    &[&f.agent],
                )
                .await
                .unwrap();
            }
        }
        assert_eq!(
            poll.process_pairing(&items[0]).await,
            Err(PollError::InvalidInstallation)
        );
        assert_eq!(poll.offset(), 0);
        assert!(!consumed(&f, id).await);
        assert_eq!(attempts(&f, 740).await, 0);
        no_content(&f).await;
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_late_product_denial_rolls_back_shadow_and_grant_only() {
    let f = Fixture::new().await;
    let (id, code, alias) = invite(&f).await;
    let db = f.pool.get().await.unwrap();
    let name = format!("channels_sprint_pair_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; CREATE TRIGGER {name} BEFORE UPDATE OF consumed_at ON zbs_telegram_guest_invite FOR EACH ROW WHEN (NEW.id='{id}'::uuid) EXECUTE FUNCTION {name}();")).await.unwrap();
    let result = process(&f, 1, 740, &format!("/pair {code}")).await;
    db.batch_execute(&format!(
        "DROP TRIGGER {name} ON zbs_telegram_guest_invite; DROP FUNCTION {name}();"
    ))
    .await
    .unwrap();
    assert_eq!(result, PollCommit::Committed(PairingNotice::Invalid));
    assert_eq!(attempts(&f, 740).await, 1);
    assert!(!consumed(&f, id).await);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM \"user\" WHERE name=$1 AND zbs_is_shadow",
            &[&alias]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(db.query_one("SELECT count(*) FROM zbs_telegram_guest_grant WHERE installation_id=$1 AND telegram_user_id='740'", &[&f.installation]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(
        process(&f, 2, 740, &format!("/pair {code}")).await,
        PollCommit::Committed(PairingNotice::Paired)
    );
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_database_failure_rolls_back_marker_budget_and_retries() {
    let f = Fixture::new().await;
    let (id, code, alias) = invite(&f).await;
    let db = f.pool.get().await.unwrap();
    let name = format!("channels_sprint_pair_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic pairing failure'; END $$; CREATE TRIGGER {name} BEFORE UPDATE OF consumed_at ON zbs_telegram_guest_invite FOR EACH ROW WHEN (NEW.id='{id}'::uuid) EXECUTE FUNCTION {name}();")).await.unwrap();
    f.updates(vec![private(1, 1, 740, &format!("/pair {code}"))]);
    let mut poll = f.session().await;
    let items = poll.get_updates().await.unwrap();
    let result = poll.process_pairing(&items[0]).await;
    db.batch_execute(&format!(
        "DROP TRIGGER {name} ON zbs_telegram_guest_invite; DROP FUNCTION {name}();"
    ))
    .await
    .unwrap();
    assert_eq!(result, Err(PollError::Database));
    assert_eq!(poll.offset(), 0);
    assert_eq!(attempts(&f, 740).await, 0);
    assert!(!consumed(&f, id).await);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM \"user\" WHERE name=$1 AND zbs_is_shadow",
            &[&alias]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND processed_at IS NOT NULL", &[&f.installation]).await.unwrap().get::<_,i64>(0),0);
    assert_eq!(
        poll.process_pairing(&items[0]).await.unwrap(),
        PollCommit::Committed(PairingNotice::Paired)
    );
    assert_eq!(attempts(&f, 740).await, 1);
    no_content(&f).await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_pair_concurrent_invite_and_sender_contenders() {
    for same_sender in [false, true] {
        let f = Fixture::new().await;
        let (first, code, alias) = invite(&f).await;
        let (second, other_code, other_alias) = if same_sender {
            invite(&f).await
        } else {
            (first, code.clone(), alias.clone())
        };
        let sender = if same_sender { 740 } else { 741 };
        f.updates(vec![
            private(1, 1, 740, &format!("/pair {code}")),
            private(2, 2, sender, &format!("/pair {other_code}")),
        ]);
        let mut a = f.session().await;
        let aa = a.get_updates().await.unwrap();
        assert!(matches!(
            f.installer
                .open_poll_session(f.pool.clone(), f.workspace, f.installation)
                .await,
            Err(PollError::PollerActive)
        ));
        let left = a.process_pairing(&aa[0]).await;
        drop(a);
        f.updates(vec![private(2, 2, sender, &format!("/pair {other_code}"))]);
        let mut b = f.session().await;
        let bb = b.get_updates().await.unwrap();
        let right = b.process_pairing(&bb[0]).await;
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| **r == PollCommit::Committed(PairingNotice::Paired))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|r| **r == PollCommit::Committed(PairingNotice::Invalid))
                .count(),
            1
        );
        let db = f.pool.get().await.unwrap();
        assert_eq!(db.query_one("SELECT count(*) FROM zbs_telegram_guest_grant WHERE installation_id=$1 AND telegram_user_id IN ('740','741')", &[&f.installation]).await.unwrap().get::<_,i64>(0),1);
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM \"user\" WHERE name IN ($1,$2) AND zbs_is_shadow",
                &[&alias, &other_alias]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            1
        );
        assert!(consumed(&f, first).await || consumed(&f, second).await);
        if same_sender {
            assert_ne!(consumed(&f, first).await, consumed(&f, second).await);
        }
        no_content(&f).await;
    }
}
