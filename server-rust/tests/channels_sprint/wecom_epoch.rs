use super::*;
use multica_server::api::{
    channels_sprint_binding::{BindingMint, BindingMintError, mint_binding, mint_wecom_binding},
    channels_sprint_ingress::{
        ClaimError, ClaimResult, InboundKey, claim_inbound, current_in_transaction,
        finish_in_transaction, process_inbound, release_inbound, wecom_stream_generation,
    },
};

fn key(workspace: Uuid, installation: Uuid, generation: Uuid) -> InboundKey {
    InboundKey {
        workspace,
        installation,
        channel: Channel::Wecom,
        message_id: Uuid::new_v4().to_string(),
        generation: Some(generation),
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_epoch_rejects_wrong_workspace_missing_and_stale_streams() {
    let pool = pool();
    let mut db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let (foreign, _, _, _, _) = fixture(&db).await;
    let epoch = Uuid::new_v4();
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'wecom',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"app_id":format!("synthetic-bot-{agent}"),"generation":epoch.to_string()})]).await.unwrap().get(0);
    assert_eq!(
        wecom_stream_generation(&pool, workspace, installation).await,
        Ok(epoch)
    );
    assert_eq!(
        wecom_stream_generation(&pool, foreign, installation).await,
        Err(ClaimError::Invalid)
    );
    let current = key(workspace, installation, epoch);
    assert_eq!(
        claim_inbound(
            &pool,
            InboundKey {
                generation: None,
                ..current.clone()
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
                ..current.clone()
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
                ..current.clone()
            }
        )
        .await,
        Err(ClaimError::Invalid)
    );
    let claim = match claim_inbound(&pool, current.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("first message must acquire"),
    };
    let fresh = Uuid::new_v4();
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&installation,&fresh.to_string()]).await.unwrap();
    assert_eq!(
        claim_inbound(&pool, current.clone()).await,
        Err(ClaimError::Invalid)
    );
    let tx = db.transaction().await.unwrap();
    assert!(!current_in_transaction(&tx, &current).await.unwrap());
    assert!(!finish_in_transaction(&tx, &claim).await.unwrap());
    tx.rollback().await.unwrap();
    assert!(!release_inbound(&pool, &claim).await.unwrap());
    assert_eq!(
        process_inbound(&pool, &claim, |_| Box::pin(async {
            panic!("stale stream persisted a message");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await,
        Ok(None)
    );
    assert_eq!(
        wecom_stream_generation(&pool, workspace, installation).await,
        Ok(fresh)
    );
    for malformed in ["invalid".to_owned(), fresh.simple().to_string()] {
        db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&installation,&malformed]).await.unwrap();
        assert_eq!(
            wecom_stream_generation(&pool, workspace, installation).await,
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
        wecom_stream_generation(&pool, workspace, installation).await,
        Err(ClaimError::Invalid)
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_rotation_fences_old_claim_and_binding_token() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let (_, origin, server) = provider().await;
    let runtime = Arc::new(InstallRuntime {
        installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), runtime.clone());
    let url = format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}");
    let body = json!({"bot_id":format!("synthetic-bot-{agent}"),"secret":"synthetic-secret"});
    let (status, response) = call(&app, "POST", &url, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    let installation = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
    let epoch = wecom_stream_generation(&pool, workspace, installation)
        .await
        .unwrap();
    let sender = VerifiedPrivateSender {
        channel: Channel::Wecom,
        installation,
        external_user_id: "synthetic-private-user".into(),
        display_name: "Synthetic".into(),
    };
    assert!(matches!(
        mint_binding(&pool, workspace, &sender).await,
        Err(BindingMintError::InvalidSender)
    ));
    assert!(matches!(
        mint_wecom_binding(&pool, workspace, &sender, Uuid::new_v4()).await,
        Err(BindingMintError::InstallationUnavailable)
    ));
    let raw = match mint_wecom_binding(&pool, workspace, &sender, epoch)
        .await
        .unwrap()
    {
        BindingMint::Fresh { raw, .. } => raw,
        BindingMint::Throttled { .. } => panic!("first mint must succeed"),
    };
    let pending = key(workspace, installation, epoch);
    let claim = match claim_inbound(&pool, pending.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("first claim must succeed"),
    };
    let historical = key(workspace, installation, epoch);
    let processed = match claim_inbound(&pool, historical.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("first claim must succeed"),
    };
    assert_eq!(
        process_inbound(&pool, &processed, |_| Box::pin(async { Ok(()) })).await,
        Ok(Some(()))
    );
    let (status, rotated) = call(&app, "POST", &url, &token(owner), body).await;
    assert_eq!(status, StatusCode::OK, "{rotated:?}");
    assert_eq!(rotated["id"], response["id"]);
    let latest = wecom_stream_generation(&pool, workspace, installation)
        .await
        .unwrap();
    assert_ne!(latest, epoch);
    assert_eq!(
        claim_inbound(&pool, pending.clone()).await,
        Err(ClaimError::Invalid)
    );
    assert!(!release_inbound(&pool, &claim).await.unwrap());
    assert_eq!(
        process_inbound(&pool, &claim, |_| Box::pin(async {
            panic!("old stream wrote");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await,
        Ok(None)
    );
    assert!(matches!(
        mint_wecom_binding(&pool, workspace, &sender, epoch).await,
        Err(BindingMintError::InstallationUnavailable)
    ));
    let (redeem_status, _) = call(
        &app,
        "POST",
        "/api/wecom/binding/redeem",
        &token(owner),
        json!({"token":raw}),
    )
    .await;
    assert_eq!(redeem_status, StatusCode::GONE);
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id=$2", &[&installation,&historical.message_id]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(db.query_one("SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id=$2", &[&installation,&pending.message_id]).await.unwrap().get::<_,i64>(0),0);
    assert!(matches!(
        mint_wecom_binding(&pool, workspace, &sender, latest)
            .await
            .unwrap(),
        BindingMint::Fresh { .. }
    ));
    assert_eq!(runtime.events.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_revoke_clears_claims_replies_and_tokens_atomically() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, outsider) = fixture(&db).await;
    let epoch = Uuid::new_v4();
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'wecom',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"app_id":format!("synthetic-bot-{agent}"),"generation":epoch.to_string()})]).await.unwrap().get(0);
    let pending = key(workspace, installation, epoch);
    let claim = match claim_inbound(&pool, pending.clone()).await.unwrap() {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("first claim must succeed"),
    };
    let raw = binding_token(&db, workspace, installation, "wecom", "synthetic-private").await;
    let task = Uuid::new_v4();
    let binding = Uuid::new_v4();
    db.execute("INSERT INTO channel_task_delivery(task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,route_revision) VALUES($1,$2,$3,'wecom','synthetic-chat','p2p',1)", &[&task,&binding,&installation]).await.unwrap();
    db.execute("INSERT INTO channel_outbound_message(installation_id,channel_type,channel_message_id,binding_id,route_revision,outbound_kind) VALUES($1,'wecom',$2,$3,1,'reply')", &[&installation,&format!("historical-{task}"),&binding]).await.unwrap();
    let rt = Arc::new(FakeRuntime::default());
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/wecom/installations/{installation}");
    assert_eq!(
        call(&app, "DELETE", &path, &token(outsider), Value::Null)
            .await
            .0,
        // Go's workspace URL middleware 404s a non-member before revoke runs.
        StatusCode::NOT_FOUND
    );
    db.batch_execute("CREATE FUNCTION channels_sprint_wecom_epoch_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic revoke rollback'; END $$; CREATE TRIGGER channels_sprint_wecom_epoch_abort BEFORE DELETE ON channel_task_delivery FOR EACH ROW EXECUTE FUNCTION channels_sprint_wecom_epoch_abort();").await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &path, &token(owner), Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("DROP TRIGGER channels_sprint_wecom_epoch_abort ON channel_task_delivery; DROP FUNCTION channels_sprint_wecom_epoch_abort();").await.unwrap();
    assert_eq!(
        wecom_stream_generation(&pool, workspace, installation).await,
        Ok(epoch)
    );
    assert!(rt.events.lock().unwrap().is_empty());
    let mut guard_db = pool.get().await.unwrap();
    let guard = guard_db.transaction().await.unwrap();
    assert!(current_in_transaction(&guard, &pending).await.unwrap());
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
            .is_err()
    );
    guard.commit().await.unwrap();
    assert_eq!(revoke.await.unwrap().0, StatusCode::NO_CONTENT);
    assert_eq!(
        wecom_stream_generation(&pool, workspace, installation).await,
        Err(ClaimError::Invalid)
    );
    assert!(!release_inbound(&pool, &claim).await.unwrap());
    assert_eq!(
        process_inbound(&pool, &claim, |_| Box::pin(async {
            panic!("revoked stream wrote");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await,
        Ok(None)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_task_delivery WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_binding_token WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_outbound_message WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/wecom/binding/redeem",
            &token(owner),
            json!({"token":raw})
        )
        .await
        .0,
        StatusCode::GONE
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_failed_rotation_preserves_epoch_claim_and_token() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let (p, origin, server) = provider().await;
    let runtime = Arc::new(InstallRuntime {
        installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), runtime.clone());
    let path = format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}");
    let body = json!({"bot_id":format!("synthetic-bot-{agent}"),"secret":"synthetic-secret"});
    let (status, installed) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{installed:?}");
    let installation = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
    let epoch = wecom_stream_generation(&pool, workspace, installation)
        .await
        .unwrap();
    let raw = binding_token(&db, workspace, installation, "wecom", "synthetic-retained").await;
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    let pending = key(workspace, installation, epoch);
    assert!(matches!(
        claim_inbound(&pool, pending.clone()).await.unwrap(),
        ClaimResult::Acquired(_)
    ));
    let before: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    *p.code.lock().unwrap() = 40001;
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    *p.code.lock().unwrap() = 0;
    db.batch_execute("CREATE FUNCTION channels_sprint_wecom_rotation_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.channel_type='wecom' THEN RAISE EXCEPTION 'synthetic failed rotation'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_wecom_rotation_abort BEFORE UPDATE ON channel_installation FOR EACH ROW EXECUTE FUNCTION channels_sprint_wecom_rotation_abort();").await.unwrap();
    let failed = call(&app, "POST", &path, &token(owner), body).await;
    db.batch_execute("DROP TRIGGER channels_sprint_wecom_rotation_abort ON channel_installation; DROP FUNCTION channels_sprint_wecom_rotation_abort();").await.unwrap();
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert!(!failed.1.to_string().contains("synthetic failed rotation"));
    let after: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(after, before);
    assert_eq!(
        wecom_stream_generation(&pool, workspace, installation).await,
        Ok(epoch)
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_binding_token WHERE token_hash=$1",
            &[&hash]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        claim_inbound(&pool, pending).await,
        Ok(ClaimResult::Duplicate)
    );
    assert_eq!(runtime.events.lock().unwrap().len(), 1);
    server.abort();
}
