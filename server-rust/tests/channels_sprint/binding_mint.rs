use super::*;
use multica_server::api::channels_sprint_binding::{
    BindingMint, BindingMintError, mint_binding, mint_dingtalk_binding, mint_lark_binding,
    mint_slack_binding, mint_telegram_binding, mint_wecom_binding, mint_zalo_binding,
};

async fn mint_test(
    pool: &Arc<Pool>,
    workspace: Uuid,
    sender: &VerifiedPrivateSender,
) -> Result<BindingMint, BindingMintError> {
    assert!(matches!(
        mint_binding(pool, workspace, sender).await,
        Err(BindingMintError::InvalidSender)
    ));
    let db = pool.get().await.unwrap();
    let row = db
        .query_one(
            "SELECT config->>'generation' FROM channel_installation WHERE id=$1",
            &[&sender.installation],
        )
        .await
        .unwrap();
    let epoch = Uuid::parse_str(&row.get::<_, String>(0)).unwrap();
    match sender.channel {
        Channel::Lark => mint_lark_binding(pool, workspace, sender, epoch).await,
        Channel::DingTalk => mint_dingtalk_binding(pool, workspace, sender, epoch).await,
        Channel::Telegram => mint_telegram_binding(pool, workspace, sender, epoch).await,
        Channel::Slack => mint_slack_binding(pool, workspace, sender, epoch).await,
        Channel::Zalo => mint_zalo_binding(pool, workspace, sender, epoch).await,
        Channel::Wecom => mint_wecom_binding(pool, workspace, sender, epoch).await,
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn binding_mint_uses_hashes_private_sender_scope_throttle_and_rollback() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "mint-owner", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Binding mint',$1) RETURNING id",
            &[&format!("mint-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign mint',$1) RETURNING id",
            &[&format!("foreign-mint-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&workspace, &owner],
    )
    .await
    .unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Mint agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec())
            .with_channel_runtime(Arc::new(FakeRuntime::default())),
    );
    let channels = [
        Channel::Lark,
        Channel::Slack,
        Channel::Wecom,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Zalo,
    ];
    for channel in channels {
        let inst: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,$3,$4,$5) RETURNING id", &[&workspace,&agent,&channel.database_type(),&owner,&json!({"generation":Uuid::new_v4().to_string()})]).await.unwrap().get(0);
        let sender = VerifiedPrivateSender {
            channel,
            installation: inst,
            external_user_id: "private-sender".into(),
            display_name: "Fake".into(),
        };
        assert!(matches!(
            mint_test(&pool, foreign, &sender).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
        let (one, two) = tokio::join!(
            mint_test(&pool, workspace, &sender),
            mint_test(&pool, workspace, &sender)
        );
        let minted = [one.unwrap(), two.unwrap()];
        let expected = if channel == Channel::Wecom { 1 } else { 2 };
        assert_eq!(
            minted
                .iter()
                .filter(|v| matches!(v, BindingMint::Fresh { .. }))
                .count(),
            expected
        );
        let mut redeem_raw = None;
        for value in minted {
            match value {
                BindingMint::Fresh { raw, expires_at } => {
                    assert_eq!(raw.len(), 43);
                    assert!(expires_at > chrono::Utc::now());
                    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
                    assert_ne!(raw, hash);
                    let row = db.query_one("SELECT token_hash,channel_type,workspace_id,channel_user_id FROM channel_binding_token WHERE token_hash=$1", &[&hash]).await.unwrap();
                    assert_eq!(row.get::<_, String>(0), hash);
                    assert_eq!(row.get::<_, String>(1), channel.database_type());
                    assert_eq!(row.get::<_, Uuid>(2), workspace);
                    assert_eq!(row.get::<_, String>(3), sender.external_user_id);
                    if redeem_raw.is_none() {
                        redeem_raw = Some(raw);
                    }
                }
                BindingMint::Throttled { expires_at } => {
                    assert_eq!(channel, Channel::Wecom);
                    assert!(expires_at > chrono::Utc::now());
                }
            }
        }
        if channel == Channel::Wecom {
            assert!(matches!(
                mint_test(&pool, workspace, &sender).await.unwrap(),
                BindingMint::Throttled { .. }
            ));
        }
        let raw = redeem_raw.unwrap();
        let path = format!("/api/{}/binding/redeem", channel.path());
        let (status, _) = call(&app, "POST", &path, "invalid-session", json!({"token":raw})).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, body) = call(&app, "POST", &path, &token(owner), json!({"token":raw})).await;
        assert_eq!(status, StatusCode::OK, "{channel:?}: {body:?}");
        assert_eq!(body["workspace_id"], json!(workspace));
        assert_eq!(
            call(&app, "POST", &path, &token(owner), json!({"token":raw}))
                .await
                .0,
            StatusCode::GONE
        );
        assert!(matches!(
            mint_test(&pool, workspace, &sender).await,
            Err(BindingMintError::AlreadyBound)
        ));
        let bound = VerifiedPrivateSender {
            channel,
            installation: inst,
            external_user_id: "already-bound".into(),
            display_name: "Fake".into(),
        };
        db.execute("INSERT INTO channel_user_binding(workspace_id,installation_id,channel_type,channel_user_id,multica_user_id) VALUES($1,$2,$3,$4,$5)", &[&workspace,&inst,&channel.database_type(),&bound.external_user_id,&owner]).await.unwrap();
        assert!(matches!(
            mint_test(&pool, workspace, &bound).await,
            Err(BindingMintError::AlreadyBound)
        ));
        db.execute(
            "UPDATE channel_installation SET status='revoked' WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap();
        assert!(matches!(
            mint_test(&pool, workspace, &sender).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
    }
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Mint rollback agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let inst: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'wecom',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"generation":Uuid::new_v4().to_string()})]).await.unwrap().get(0);
    let sender = VerifiedPrivateSender {
        channel: Channel::Wecom,
        installation: inst,
        external_user_id: "rollback".into(),
        display_name: "Fake".into(),
    };
    db.batch_execute("CREATE FUNCTION channels_sprint_mint_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic mint rollback'; END $$; CREATE TRIGGER channels_sprint_mint_abort BEFORE INSERT ON channel_binding_token FOR EACH ROW EXECUTE FUNCTION channels_sprint_mint_abort();").await.unwrap();
    assert!(matches!(
        mint_test(&pool, workspace, &sender).await,
        Err(BindingMintError::Database)
    ));
    db.batch_execute("DROP TRIGGER channels_sprint_mint_abort ON channel_binding_token; DROP FUNCTION channels_sprint_mint_abort();").await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT COUNT(*) FROM channel_binding_token WHERE installation_id=$1",
            &[&inst]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert!(matches!(
        mint_test(&pool, workspace, &sender).await.unwrap(),
        BindingMint::Fresh { .. }
    ));
    let invalid = VerifiedPrivateSender {
        external_user_id: "\n".into(),
        ..sender
    };
    assert!(matches!(
        mint_test(&pool, workspace, &invalid).await,
        Err(BindingMintError::InvalidSender)
    ));
}
