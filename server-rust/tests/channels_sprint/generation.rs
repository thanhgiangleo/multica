use super::*;
use multica_server::api::channels_sprint_ingress::{
    ClaimError, ClaimResult, InboundKey, claim_inbound, current_in_transaction,
    finish_in_transaction, process_inbound, release_inbound, zalo_poll_generation,
};

fn pool() -> Arc<Pool> {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn zalo_epoch_fences_claim_finish_release_and_queued_writes() {
    let pool = pool();
    let mut db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "epoch-owner", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Epoch test',$1) RETURNING id",
            &[&format!("epoch-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other epoch',$1) RETURNING id",
            &[&format!("epoch-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Epoch agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let first = Uuid::new_v4();
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'zalo',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"app_id":"synthetic-epoch-bot","generation":first.to_string(),"bot_token_encrypted":"synthetic-ciphertext"})]).await.unwrap().get(0);
    assert_eq!(
        zalo_poll_generation(&pool, workspace, installation).await,
        Ok(first)
    );
    assert_eq!(
        zalo_poll_generation(&pool, foreign, installation).await,
        Err(ClaimError::Invalid)
    );
    let key = InboundKey {
        workspace,
        installation,
        channel: Channel::Zalo,
        message_id: format!("epoch-message-{suffix}"),
        generation: Some(first),
    };
    assert_eq!(
        claim_inbound(
            &pool,
            InboundKey {
                generation: None,
                ..key.clone()
            }
        )
        .await,
        Err(ClaimError::Invalid)
    );
    assert_eq!(
        claim_inbound(
            &pool,
            InboundKey {
                workspace: foreign,
                ..key.clone()
            }
        )
        .await,
        Err(ClaimError::Invalid)
    );
    assert_eq!(
        claim_inbound(
            &pool,
            InboundKey {
                generation: Some(Uuid::new_v4()),
                ..key.clone()
            }
        )
        .await,
        Err(ClaimError::Invalid)
    );
    let claim = match claim_inbound(&pool, key.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("expected first claim"),
    };
    // A live token rotation preserves its generation and does not invalidate
    // already accepted turns.
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{bot_token_encrypted}','\"new-synthetic-ciphertext\"') WHERE id=$1", &[&installation]).await.unwrap();
    let tx = db.transaction().await.unwrap();
    assert!(current_in_transaction(&tx, &key).await.unwrap());
    tx.rollback().await.unwrap();
    assert!(release_inbound(&pool, &claim).await.unwrap());
    let claim = match claim_inbound(&pool, key.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("expected reacquired claim"),
    };
    // Emulate a reconnect's committed epoch change, without granting the
    // public reconnect route permission to run until delivery consumers wire
    // this guard. A queued callback must not write under its stale snapshot.
    let second = Uuid::new_v4();
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&installation,&second.to_string()]).await.unwrap();
    assert_eq!(
        zalo_poll_generation(&pool, workspace, installation).await,
        Ok(second)
    );
    assert_eq!(
        claim_inbound(&pool, key.clone()).await,
        Err(ClaimError::Invalid)
    );
    let tx = db.transaction().await.unwrap();
    assert!(!current_in_transaction(&tx, &key).await.unwrap());
    tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('zalo','stale-epoch-write','synthetic')", &[]).await.unwrap();
    assert!(!finish_in_transaction(&tx, &claim).await.unwrap());
    tx.rollback().await.unwrap();
    assert_eq!(
        process_inbound(&pool, &claim, |_| Box::pin(async {
            panic!("stale callback reached");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await,
        Ok(None)
    );
    assert!(!release_inbound(&pool, &claim).await.unwrap());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='stale-epoch-write'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute(
        "DELETE FROM channel_inbound_message_dedup WHERE installation_id=$1",
        &[&installation],
    )
    .await
    .unwrap();
    let fresh = InboundKey {
        generation: Some(second),
        ..key
    };
    let claim = match claim_inbound(&pool, fresh).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("new generation should not inherit old claim"),
    };
    assert_eq!(process_inbound(&pool, &claim, |tx| Box::pin(async move {
        tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('zalo','fresh-epoch-write','synthetic')", &[]).await.map_err(|_| ClaimError::Database)?;
        Ok(())
    })).await, Ok(Some(())));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='fresh-epoch-write'",
            &[]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    for generation in ["invalid-epoch".to_string(), second.simple().to_string()] {
        db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&installation,&generation]).await.unwrap();
        assert_eq!(
            zalo_poll_generation(&pool, workspace, installation).await,
            Err(ClaimError::Invalid)
        );
    }
    db.execute(
        "UPDATE channel_installation SET config=config-'generation' WHERE id=$1",
        &[&installation],
    )
    .await
    .unwrap();
    assert_eq!(
        zalo_poll_generation(&pool, workspace, installation).await,
        Err(ClaimError::Invalid)
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn zalo_revoke_drops_claims_and_pending_replies_atomically() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "revoke-owner", &suffix).await;
    let member = insert_user(&db, "revoke-member", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Revoke epoch test',$1) RETURNING id",
            &[&format!("epoch-revoke-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Revoke agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let first = Uuid::new_v4();
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'zalo',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"app_id":"synthetic-revoke-bot","generation":first.to_string()})]).await.unwrap().get(0);
    let key = InboundKey {
        workspace,
        installation,
        channel: Channel::Zalo,
        message_id: format!("revoke-epoch-{suffix}"),
        generation: Some(first),
    };
    let claim = match claim_inbound(&pool, key.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("expected claim"),
    };
    let pending = binding_token(&db, workspace, installation, "zalo", "synthetic-external").await;
    let task = Uuid::new_v4();
    let binding = Uuid::new_v4();
    db.execute("INSERT INTO channel_task_delivery(task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,route_revision) VALUES($1,$2,$3,'zalo','synthetic-chat','p2p',1)", &[&task,&binding,&installation]).await.unwrap();
    db.execute("INSERT INTO channel_outbound_message(installation_id,channel_type,channel_message_id,binding_id,route_revision,outbound_kind) VALUES($1,'zalo',$2,$3,1,'reply')", &[&installation,&format!("historical-{suffix}"),&binding]).await.unwrap();
    let rt = Arc::new(FakeRuntime::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()).with_channel_runtime(rt.clone()),
    );
    let path = format!("/api/workspaces/{workspace}/zalo/installations/{installation}");
    assert_eq!(
        call(&app, "DELETE", &path, &token(member), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "active"
    );
    db.batch_execute("CREATE FUNCTION channels_sprint_epoch_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic revoke rollback'; END $$; CREATE TRIGGER channels_sprint_epoch_abort BEFORE DELETE ON channel_task_delivery FOR EACH ROW EXECUTE FUNCTION channels_sprint_epoch_abort();").await.unwrap();
    let failed = call(&app, "DELETE", &path, &token(owner), Value::Null).await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert_eq!(rt.events.lock().unwrap().len(), 0);
    assert_eq!(
        db.query_one(
            "SELECT status FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "active"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.batch_execute("DROP TRIGGER channels_sprint_epoch_abort ON channel_task_delivery; DROP FUNCTION channels_sprint_epoch_abort();").await.unwrap();
    let mut guard_db = pool.get().await.unwrap();
    let guard = guard_db.transaction().await.unwrap();
    assert!(current_in_transaction(&guard, &key).await.unwrap());
    let app_revoke = app.clone();
    let path_revoke = path.clone();
    let mut revoke = tokio::spawn(async move {
        call(
            &app_revoke,
            "DELETE",
            &path_revoke,
            &token(owner),
            Value::Null,
        )
        .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut revoke)
            .await
            .is_err(),
        "revoke cannot overtake a generation-guarded write"
    );
    guard.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), revoke)
            .await
            .unwrap()
            .unwrap()
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    assert_eq!(
        db.query_one(
            "SELECT status FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "revoked"
    );
    assert_eq!(
        zalo_poll_generation(&pool, workspace, installation).await,
        Err(ClaimError::Invalid)
    );
    assert_eq!(claim_inbound(&pool, key).await, Err(ClaimError::Invalid));
    assert!(!release_inbound(&pool, &claim).await.unwrap());
    for (table, want) in [
        ("channel_inbound_message_dedup", 0),
        ("channel_task_delivery", 0),
        ("channel_outbound_message", 1),
    ] {
        let count: i64 = db
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE installation_id=$1"),
                &[&installation],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, want, "{table}");
    }
    let hash = hex::encode(Sha256::digest(pending.as_bytes()));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_binding_token WHERE installation_id=$1 AND token_hash=$2",
            &[&installation, &hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}
