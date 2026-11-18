use super::*;
#[path = "telegram_admit.rs"]
mod admission_tests;
use axum::{Json, extract::State, routing::post};
use multica_server::api::{
    channels_sprint_ingress::ClaimError,
    channels_sprint_telegram::{
        TelegramInstaller,
        polling::{PollCommit, PollError},
    },
};

type Pause = Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>;
#[derive(Clone)]
struct FakePoll(Arc<Mutex<(Value, Vec<Value>)>>, String, Pause);
async fn fake_updates(
    State(fake): State<FakePoll>,
    uri: axum::http::Uri,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert_eq!(uri.path(), fake.1);
    let pause = fake.2.lock().unwrap().clone();
    if let Some((started, release)) = pause {
        started.notify_one();
        release.notified().await;
    }
    let mut guard = fake.0.lock().unwrap();
    guard.1.push(body.clone());
    let mut response = guard.0.clone();
    if response["_respect_offset"] == true {
        let offset = body["offset"].as_i64().unwrap();
        response["result"]
            .as_array_mut()
            .unwrap()
            .retain(|item| item["update_id"].as_i64().unwrap() >= offset);
        response.as_object_mut().unwrap().remove("_respect_offset");
    }
    Json(response)
}
fn text(id: i64, message: i64) -> Value {
    json!({"update_id":id,"message":{"message_id":message,"from":{"id":731,"is_bot":false},"chat":{"id":731,"type":"private"},"text":"fake message"}})
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn telegram_poll_fences_batches_replay_rotation_and_failed_persistence() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "poll-owner", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Poll test',$1) RETURNING id",
            &[&format!("poll-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other poll test',$1) RETURNING id",
            &[&format!("poll-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Poll agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let bot_id = (u64::from_str_radix(&suffix[..12], 16).unwrap() + 100000).to_string();
    let token = format!("{bot_id}:synthetic_secret");
    let fake = FakePoll(
        Arc::new(Mutex::new((
            json!({"ok":true,"result":[text(11,71),text(12,72)]}),
            vec![],
        ))),
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
            .unwrap();
        }
    });
    let installer = TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
    let generation = Uuid::new_v4();
    let encrypted = multica_server::api::channels_sprint_telegram::CredentialBox::new(&[73; 32])
        .unwrap()
        .seal(token.as_bytes())
        .unwrap();
    let config = json!({"generation":generation.to_string(),"app_id":bot_id,"bot_token_encrypted":encrypted});
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'telegram',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&config]).await.unwrap().get(0);
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), foreign, installation)
            .await,
        Err(PollError::InvalidInstallation)
    ));
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), workspace, Uuid::new_v4())
            .await,
        Err(PollError::InvalidInstallation)
    ));
    let mut session = installer
        .open_poll_session(pool.clone(), workspace, installation)
        .await
        .unwrap();
    let updates = session.get_updates().await.unwrap();
    assert_eq!(session.offset(), 0);
    assert_eq!(updates.len(), 2);
    assert_eq!(
        (
            updates[0].sender_id(),
            updates[0].chat_id(),
            updates[0].text()
        ),
        (731, 731, "fake message")
    );
    assert!(matches!(
        session.get_updates().await,
        Err(PollError::Pending)
    ));
    assert!(matches!(
        session
            .process_update(&updates[1], |_| Box::pin(async { Ok::<_, ClaimError>(()) }))
            .await,
        Err(PollError::InvalidResponse)
    ));
    assert_eq!(session.offset(), 0);
    let failed = session.process_update(&updates[0], |tx| Box::pin(async move {
        tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','poll-rollback','test')",&[]).await.map_err(|_|ClaimError::Database)?;
        Err::<(), _>(ClaimError::Database)
    })).await;
    assert!(matches!(failed, Err(PollError::Database)));
    assert_eq!(session.offset(), 0);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='poll-rollback'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let committed = session.process_update(&updates[0], |tx| Box::pin(async move {
        tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','poll-committed','test')",&[]).await.map_err(|_|ClaimError::Database)?;
        Ok::<_,ClaimError>(71)
    })).await.unwrap();
    assert_eq!(committed, PollCommit::Committed(71));
    assert_eq!(session.offset(), 12);
    assert_eq!(
        session
            .process_update(&updates[1], |_| Box::pin(async { Ok::<_, ClaimError>(72) }))
            .await
            .unwrap(),
        PollCommit::Committed(72)
    );
    assert_eq!(session.offset(), 13);
    let calls = fake.0.lock().unwrap().1.clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        (
            calls[0]["offset"].as_i64(),
            calls[0]["timeout"].as_i64(),
            calls[0]["limit"].as_i64()
        ),
        (Some(0), Some(50), Some(100))
    );
    assert_eq!(calls[0]["allowed_updates"], json!(["message"]));

    // A direct poller blocks other sessions until its connection is closed.
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), workspace, installation)
            .await,
        Err(PollError::PollerActive)
    ));
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), foreign, installation)
            .await,
        Err(PollError::InvalidInstallation)
    ));
    drop(session);
    // Restart replays offset zero, but no callback fires for a processed message.
    let mut replay = installer
        .open_poll_session(pool.clone(), workspace, installation)
        .await
        .unwrap();
    let updates = replay.get_updates().await.unwrap();
    assert_eq!(
        replay
            .process_update(&updates[0], |_| Box::pin(async {
                panic!("replay must not persist");
                #[allow(unreachable_code)]
                Ok::<_, ClaimError>(0)
            }))
            .await
            .unwrap(),
        PollCommit::Replay
    );
    assert_eq!(replay.offset(), 12);
    assert_eq!(
        replay
            .process_update(&updates[1], |_| Box::pin(async {
                panic!("replay must not persist");
                #[allow(unreachable_code)]
                Ok::<_, ClaimError>(0)
            }))
            .await
            .unwrap(),
        PollCommit::Replay
    );
    for (body, expected) in [
        (
            json!({"ok":false,"error_code":401,"description":token}),
            PollError::CredentialsRejected,
        ),
        (
            json!({"ok":false,"error_code":409,"description":token}),
            PollError::Conflict,
        ),
        (
            json!({"ok":false,"error_code":429,"parameters":{"retry_after":17}}),
            PollError::RateLimited {
                retry_after_seconds: 17,
            },
        ),
        (
            json!({"ok":true,"result":[text(15,75),text(14,74)]}),
            PollError::InvalidResponse,
        ),
        (
            json!({"ok":true,"result":[{"update_id":13,"edited_message":{}}]}),
            PollError::UnsupportedUpdate,
        ),
        (
            json!({"ok":true,"result":"bad"}),
            PollError::InvalidResponse,
        ),
        (
            json!({"ok":true,"result":[] ,"padding":"x".repeat(1024*1024)}),
            PollError::InvalidResponse,
        ),
    ] {
        fake.0.lock().unwrap().0 = body;
        assert_eq!(replay.get_updates().await.err(), Some(expected));
        assert_eq!(replay.offset(), 13);
    }
    fake.0.lock().unwrap().0 = json!({"ok":true,"result":[text(13,73)]});
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *fake.2.lock().unwrap() = Some((started.clone(), release.clone()));
    let new_generation = Uuid::new_v4();
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(replay.get_updates(), async {
            started.notified().await;
            db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($1::text)) WHERE id=$2", &[&new_generation.to_string(),&installation]).await.unwrap();
            release.notify_one();
        })
    }).await.unwrap();
    *fake.2.lock().unwrap() = None;
    assert!(matches!(result, Err(PollError::InvalidInstallation)));
    assert_eq!(replay.offset(), 13);
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), workspace, installation)
            .await,
        Err(PollError::PollerActive)
    ));
    let old_update = &updates[0];
    assert!(matches!(
        replay
            .process_update(old_update, |_| Box::pin(async { Ok::<_, ClaimError>(()) }))
            .await,
        Err(PollError::InvalidResponse)
    ));
    *fake.0.lock().unwrap() = (
        json!({"ok":true,"result":[text(13,73),text(14,74)]}),
        vec![],
    );
    drop(replay);
    let mut rotated = installer
        .open_poll_session(pool.clone(), workspace, installation)
        .await
        .unwrap();
    let fresh = rotated.get_updates().await.unwrap();
    let pending_key = multica_server::api::channels_sprint_ingress::InboundKey {
        workspace,
        installation,
        channel: Channel::Telegram,
        generation: Some(new_generation),
        message_id: "731:73".into(),
    };
    let pending_claim =
        match multica_server::api::channels_sprint_ingress::claim_inbound(&pool, pending_key)
            .await
            .unwrap()
        {
            multica_server::api::channels_sprint_ingress::ClaimResult::Acquired(c) => c,
            _ => panic!("claim fixture"),
        };
    assert!(matches!(
        rotated
            .process_update(&fresh[0], |_| Box::pin(async { Ok::<_, ClaimError>(()) }))
            .await,
        Err(PollError::Pending)
    ));
    assert_eq!(rotated.offset(), 0);
    assert!(
        multica_server::api::channels_sprint_ingress::release_inbound(&pool, &pending_claim)
            .await
            .unwrap()
    );
    assert_eq!(
        rotated
            .process_update(&fresh[0], |_| Box::pin(async { Ok::<_, ClaimError>(()) }))
            .await
            .unwrap(),
        PollCommit::Committed(())
    );
    assert_eq!(rotated.offset(), 14);
    db.execute(
        "UPDATE channel_installation SET status='revoked' WHERE id=$1",
        &[&installation],
    )
    .await
    .unwrap();
    assert!(matches!(
        rotated
            .process_update(&fresh[1], |_| Box::pin(async { Ok::<_, ClaimError>(()) }))
            .await,
        Err(PollError::InvalidInstallation)
    ));
    assert_eq!(rotated.offset(), 14);
    drop(rotated);
    assert!(matches!(
        installer
            .open_poll_session(pool.clone(), workspace, installation)
            .await,
        Err(PollError::InvalidInstallation)
    ));
    server.abort();
}
