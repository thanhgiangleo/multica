use super::*;
use multica_server::api::channels_sprint_ingress::{
    ClaimError, ClaimResult, InboundClaim, InboundKey, claim_inbound, finish_in_transaction,
    process_inbound, release_inbound,
};

fn acquired(result: ClaimResult) -> InboundClaim {
    match result {
        ClaimResult::Acquired(claim) => claim,
        ClaimResult::Duplicate => panic!("expected claim"),
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn inbound_claims_fence_replay_reclaim_revoke_and_failed_writes() {
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
    let mut db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "inbound-owner", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Inbound test',$1) RETURNING id",
            &[&format!("inbound-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Inbound foreign',$1) RETURNING id",
            &[&format!("inbound-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Inbound agent','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let generation = Uuid::new_v4();
    let installation: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'telegram',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"generation":generation.to_string()})]).await.unwrap().get(0);
    let key = InboundKey {
        workspace,
        installation,
        channel: Channel::Telegram,
        message_id: format!("message-{suffix}"),
        generation: Some(generation),
    };
    let invalid = InboundKey {
        workspace: foreign,
        ..key.clone()
    };
    assert_eq!(
        claim_inbound(&pool, invalid).await,
        Err(ClaimError::Invalid)
    );
    assert_eq!(
        claim_inbound(
            &pool,
            InboundKey {
                channel: Channel::Zalo,
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
                message_id: "\n".into(),
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
                message_id: "x".repeat(1025),
                ..key.clone()
            }
        )
        .await,
        Err(ClaimError::Invalid)
    );
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);

    let (a, b) = tokio::join!(
        claim_inbound(&pool, key.clone()),
        claim_inbound(&pool, key.clone())
    );
    let claims = [a.unwrap(), b.unwrap()];
    assert_eq!(
        claims
            .iter()
            .filter(|v| matches!(v, ClaimResult::Acquired(_)))
            .count(),
        1
    );
    let first = acquired(
        claims
            .into_iter()
            .find(|v| matches!(v, ClaimResult::Acquired(_)))
            .unwrap(),
    );
    assert_eq!(
        claim_inbound(&pool, key.clone()).await,
        Ok(ClaimResult::Duplicate)
    );
    db.execute("UPDATE channel_inbound_message_dedup SET received_at=now()-interval '61 seconds' WHERE installation_id=$1 AND message_id=$2", &[&installation,&key.message_id]).await.unwrap();
    let second = acquired(claim_inbound(&pool, key.clone()).await.unwrap());
    assert_ne!(first.token, second.token);
    assert!(!release_inbound(&pool, &first).await.unwrap());
    let tx = db.transaction().await.unwrap();
    tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','stale-attempt','test')", &[]).await.unwrap();
    assert!(!finish_in_transaction(&tx, &first).await.unwrap());
    tx.rollback().await.unwrap();
    let lost_write: i64 = db
        .query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='stale-attempt'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(lost_write, 0);

    let tx = db.transaction().await.unwrap();
    tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','committed-attempt','test')", &[]).await.unwrap();
    assert!(finish_in_transaction(&tx, &second).await.unwrap());
    tx.commit().await.unwrap();
    assert_eq!(
        claim_inbound(&pool, key.clone()).await,
        Ok(ClaimResult::Duplicate)
    );
    assert!(!release_inbound(&pool, &second).await.unwrap());
    let committed: i64 = db
        .query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='committed-attempt'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(committed, 1);

    let candidate = acquired(
        claim_inbound(
            &pool,
            InboundKey {
                message_id: format!("callback-{suffix}"),
                ..key.clone()
            },
        )
        .await
        .unwrap(),
    );
    let failed = process_inbound(&pool, &candidate, |tx| Box::pin(async move {
        tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','rollback-callback','test')", &[]).await.map_err(|_| ClaimError::Database)?;
        Err::<(), _>(ClaimError::Database)
    })).await;
    assert_eq!(failed, Err(ClaimError::Database));
    let processed: Option<chrono::DateTime<chrono::Utc>> = db.query_one("SELECT processed_at FROM channel_inbound_message_dedup WHERE installation_id=$1 AND message_id=$2", &[&installation,&candidate.key.message_id]).await.unwrap().get(0);
    assert!(processed.is_none());
    let rolled_back: i64 = db
        .query_one(
            "SELECT count(*) FROM channel_inbound_audit WHERE event_type='rollback-callback'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rolled_back, 0);
    let success = process_inbound(&pool, &candidate, |tx| Box::pin(async move {
        tx.execute("INSERT INTO channel_inbound_audit(channel_type,event_type,drop_reason) VALUES('telegram','success-callback','test')", &[]).await.map_err(|_| ClaimError::Database)?;
        Ok(73_i32)
    })).await;
    assert_eq!(success, Ok(Some(73)));
    assert_eq!(
        process_inbound(&pool, &candidate, |_| Box::pin(async {
            panic!("replay must not invoke persistence");
            #[allow(unreachable_code)]
            Ok(73_i32)
        }))
        .await,
        Ok(None)
    );

    for channel in [
        Channel::Lark,
        Channel::Slack,
        Channel::Wecom,
        Channel::DingTalk,
        Channel::Zalo,
    ] {
        let generation = Some(Uuid::new_v4());
        let config = generation.map_or_else(|| json!({}), |g| json!({"generation":g.to_string()}));
        let other: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,$3,$4,$5) RETURNING id", &[&workspace,&agent,&channel.database_type(),&owner,&config]).await.unwrap().get(0);
        let independent = acquired(
            claim_inbound(
                &pool,
                InboundKey {
                    installation: other,
                    channel,
                    generation,
                    ..key.clone()
                },
            )
            .await
            .unwrap(),
        );
        assert!(release_inbound(&pool, &independent).await.unwrap());
        let retry = acquired(claim_inbound(&pool, independent.key.clone()).await.unwrap());
        assert_ne!(independent.token, retry.token);
        assert!(!release_inbound(&pool, &independent).await.unwrap());
        assert_eq!(
            process_inbound(&pool, &retry, |_| Box::pin(async { Ok(()) })).await,
            Ok(Some(()))
        );
    }

    let revocable = acquired(
        claim_inbound(
            &pool,
            InboundKey {
                message_id: format!("revoke-{suffix}"),
                ..key
            },
        )
        .await
        .unwrap(),
    );
    db.execute(
        "UPDATE channel_installation SET status='revoked' WHERE id=$1",
        &[&installation],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    assert!(!finish_in_transaction(&tx, &revocable).await.unwrap());
    tx.rollback().await.unwrap();
    assert_eq!(
        claim_inbound(&pool, revocable.key.clone()).await,
        Err(ClaimError::Invalid)
    );
    assert!(!release_inbound(&pool, &revocable).await.unwrap());
}
