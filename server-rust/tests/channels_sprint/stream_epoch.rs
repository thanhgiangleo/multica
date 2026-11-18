use super::*;
use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use multica_server::api::{
    channels_sprint_binding::{
        BindingMint, BindingMintError, mint_binding, mint_dingtalk_binding, mint_lark_binding,
        mint_slack_binding, mint_telegram_binding,
    },
    channels_sprint_dingtalk::DingTalkInstaller,
    channels_sprint_ingress::{
        ClaimError, ClaimResult, InboundClaim, InboundKey, claim_inbound, current_in_transaction,
        dingtalk_stream_generation, lark_stream_generation, process_inbound, release_inbound,
        slack_stream_generation, telegram_poll_generation,
    },
    channels_sprint_lark::{DeviceCompletion, LarkInstaller, Region},
    channels_sprint_slack::SlackInstaller,
    channels_sprint_telegram::TelegramInstaller,
};

struct Runtime {
    ding: DingTalkInstaller,
    telegram: TelegramInstaller,
    slack: SlackInstaller,
    events: Mutex<Vec<ChannelEvent>>,
}
#[async_trait]
impl ChannelRuntime for Runtime {
    fn configured(&self, _: Channel) -> bool {
        true
    }
    fn publish(&self, event: ChannelEvent) {
        self.events.lock().unwrap().push(event);
    }
    async fn cancel_guest_tasks(&self, _: GuestCancellation) {}
    fn dingtalk_installer(&self) -> Option<&DingTalkInstaller> {
        Some(&self.ding)
    }
    fn telegram_installer(&self) -> Option<&TelegramInstaller> {
        Some(&self.telegram)
    }
    fn slack_installer(&self) -> Option<&SlackInstaller> {
        Some(&self.slack)
    }
}
async fn provider(State(reject): State<Arc<Mutex<bool>>>) -> (StatusCode, Json<Value>) {
    if *reject.lock().unwrap() {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"code":1,"message":"synthetic secret"})),
        )
    } else {
        (
            StatusCode::OK,
            Json(
                json!({"code":0,"tenant_access_token":"synthetic-token","expire":7200,"bot":{"open_id":"ou_synthetic_bot"}}),
            ),
        )
    }
}
async fn ding_provider(State(reject): State<Arc<Mutex<bool>>>) -> (StatusCode, Json<Value>) {
    if *reject.lock().unwrap() {
        (StatusCode::BAD_REQUEST, Json(json!({"code":"rejected"})))
    } else {
        (
            StatusCode::OK,
            Json(json!({"accessToken":"synthetic-token","expireIn":7200})),
        )
    }
}
type ProviderPause = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;

async fn connection_provider(
    State((reject, app_id, bot_id, pause)): State<(Arc<Mutex<bool>>, String, i64, ProviderPause)>,
    uri: axum::http::Uri,
) -> (StatusCode, Json<Value>) {
    if matches!(
        uri.path().rsplit('/').next(),
        Some("getWebhookInfo" | "apps.connections.open")
    ) {
        let blocked = { pause.lock().unwrap().take() };
        if let Some((entered, release)) = blocked {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
    if *reject.lock().unwrap() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"ok":false,"error_code":401})),
        );
    }
    let value = match uri.path().rsplit('/').next().unwrap() {
        "getMe" => {
            json!({"ok":true,"result":{"id":bot_id,"is_bot":true,"username":"SyntheticBot_73"}})
        }
        "getWebhookInfo" => json!({"ok":true,"result":{"url":""}}),
        "auth.test" => json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"}),
        "bots.info" => json!({"ok":true,"bot":{"id":"B73","app_id":app_id}}),
        "apps.connections.open" => json!({"ok":true,"url":"wss://synthetic.invalid/socket"}),
        _ => panic!("unexpected synthetic provider path: {uri}"),
    };
    (StatusCode::OK, Json(value))
}

struct Harness {
    pool: Arc<Pool>,
    state: ApiState,
    app: Router,
    rt: Arc<Runtime>,
    lark: LarkInstaller,
    workspace: Uuid,
    agent: Uuid,
    owner: Uuid,
    outsider: Uuid,
    app_id: String,
    slack_app: String,
    bot_id: i64,
    reject: Arc<Mutex<bool>>,
    pause: ProviderPause,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Harness {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
        );
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(8)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let owner = insert_user(&db, "stream-owner", &suffix).await;
        let outsider = insert_user(&db, "stream-outsider", &suffix).await;
        let workspace = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Stream epoch',$1) RETURNING id",
                &[&format!("stream-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
            &[&workspace, &owner],
        )
        .await
        .unwrap();
        let agent = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Stream synthetic','local',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let reject = Arc::new(Mutex::new(false));
        let app_id = format!("synthetic-{suffix}");
        let slack_app = format!("A{}", &suffix[..12].to_uppercase());
        let bot_id = (Uuid::new_v4().as_u128() % (i64::MAX as u128 - 1) + 1) as i64;
        let pause: ProviderPause = Arc::new(Mutex::new(None));
        let providers = Router::new()
            .route(
                "/open-apis/auth/v3/tenant_access_token/internal",
                post(provider),
            )
            .route("/open-apis/bot/v3/info", get(provider))
            .route("/v1.0/oauth2/accessToken", post(ding_provider))
            .with_state(reject.clone())
            .merge(Router::new().fallback(connection_provider).with_state((
                reject.clone(),
                slack_app.clone(),
                bot_id,
                pause.clone(),
            )));
        let server = tokio::spawn(async move {
            axum::serve(listener, providers).await.unwrap();
        });
        let rt = Arc::new(Runtime {
            ding: DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            telegram: TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            slack: SlackInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            events: Mutex::new(vec![]),
        });
        let state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec())
            .with_channel_runtime(rt.clone());
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state.clone(),
        );
        Self {
            pool,
            state,
            app,
            rt,
            lark: LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            workspace,
            agent,
            owner,
            outsider,
            app_id,
            slack_app,
            bot_id,
            reject,
            pause,
            server,
        }
    }
    async fn install(&self, channel: Channel) -> Result<Uuid, StatusCode> {
        if channel == Channel::Lark {
            return self
                .lark
                .complete_device_install(
                    &self.state,
                    DeviceCompletion {
                        workspace_id: self.workspace,
                        agent_id: self.agent,
                        initiator_id: self.owner,
                        app_id: &self.app_id,
                        app_secret: "synthetic-secret",
                        installer_open_id: "ou_installer",
                        region: Region::Lark,
                    },
                )
                .await
                .map(|v| Uuid::parse_str(v["id"].as_str().unwrap()).unwrap())
                .map_err(|e| e.status);
        }
        let (path, body) = match channel {
            Channel::DingTalk => (
                "dingtalk/install/byo",
                json!({"client_id":self.app_id,"client_secret":"synthetic-secret"}),
            ),
            Channel::Telegram => (
                "telegram/install",
                json!({"bot_token":format!("{}:synthetic-token",self.bot_id)}),
            ),
            Channel::Slack => (
                "slack/install/byo",
                json!({"bot_token":"xoxb-synthetic","app_token":format!("xapp-1-{}-73-synthetic",self.slack_app)}),
            ),
            _ => unreachable!(),
        };
        let (status, value) = call(
            &self.app,
            "POST",
            &format!(
                "/api/workspaces/{}/{path}?agent_id={}",
                self.workspace, self.agent
            ),
            &token(self.owner),
            body,
        )
        .await;
        if status == StatusCode::OK {
            Ok(Uuid::parse_str(value["id"].as_str().unwrap()).unwrap())
        } else {
            Err(status)
        }
    }
    async fn generation(
        &self,
        channel: Channel,
        workspace: Uuid,
        installation: Uuid,
    ) -> Result<Uuid, ClaimError> {
        match channel {
            Channel::Lark => lark_stream_generation(&self.pool, workspace, installation).await,
            Channel::DingTalk => {
                dingtalk_stream_generation(&self.pool, workspace, installation).await
            }
            Channel::Telegram => {
                telegram_poll_generation(&self.pool, workspace, installation).await
            }
            Channel::Slack => slack_stream_generation(&self.pool, workspace, installation).await,
            _ => unreachable!(),
        }
    }
    async fn revoke(&self, channel: Channel, installation: Uuid, user: Uuid) -> StatusCode {
        call(
            &self.app,
            "DELETE",
            &format!(
                "/api/workspaces/{}/{}/installations/{installation}",
                self.workspace,
                channel.path()
            ),
            &token(user),
            Value::Null,
        )
        .await
        .0
    }
}
fn key(h: &Harness, channel: Channel, installation: Uuid, epoch: Uuid) -> InboundKey {
    InboundKey {
        workspace: h.workspace,
        installation,
        channel,
        message_id: Uuid::new_v4().to_string(),
        generation: Some(epoch),
    }
}
async fn acquire(h: &Harness, key: InboundKey) -> InboundClaim {
    match claim_inbound(&h.pool, key).await.unwrap() {
        ClaimResult::Acquired(c) => c,
        ClaimResult::Duplicate => panic!("first claim must acquire"),
    }
}
async fn mint(
    h: &Harness,
    channel: Channel,
    installation: Uuid,
    epoch: Uuid,
) -> Result<BindingMint, BindingMintError> {
    let sender = VerifiedPrivateSender {
        channel,
        installation,
        external_user_id: "synthetic-private".into(),
        display_name: "Synthetic".into(),
    };
    assert!(matches!(
        mint_binding(&h.pool, h.workspace, &sender).await,
        Err(BindingMintError::InvalidSender)
    ));
    match channel {
        Channel::Lark => mint_lark_binding(&h.pool, h.workspace, &sender, epoch).await,
        Channel::DingTalk => mint_dingtalk_binding(&h.pool, h.workspace, &sender, epoch).await,
        Channel::Telegram => mint_telegram_binding(&h.pool, h.workspace, &sender, epoch).await,
        Channel::Slack => mint_slack_binding(&h.pool, h.workspace, &sender, epoch).await,
        _ => unreachable!(),
    }
}
async fn assert_stale(h: &Harness, claim: &InboundClaim) {
    assert_eq!(
        claim_inbound(&h.pool, claim.key.clone()).await,
        Err(ClaimError::Invalid)
    );
    assert!(!release_inbound(&h.pool, claim).await.unwrap());
    assert_eq!(
        process_inbound(&h.pool, claim, |_| Box::pin(async {
            panic!("stale callback ran");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await,
        Ok(None)
    );
}
async fn counts(h: &Harness, installation: Uuid) -> (i64, i64, i64) {
    let db = h.pool.get().await.unwrap();
    let r=db.query_one("SELECT (SELECT count(*) FROM channel_inbound_message_dedup WHERE installation_id=$1),(SELECT count(*) FROM channel_binding_token WHERE installation_id=$1 AND consumed_at IS NULL),(SELECT count(*) FROM channel_task_delivery WHERE installation_id=$1)",&[&installation]).await.unwrap();
    (r.get(0), r.get(1), r.get(2))
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_connection_epochs_scope_rotation_and_binding_cleanup() {
    for channel in [
        Channel::Lark,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Slack,
    ] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        assert_eq!(
            h.generation(channel, Uuid::new_v4(), installation).await,
            Err(ClaimError::Invalid)
        );
        let other = if channel == Channel::Lark {
            Channel::DingTalk
        } else {
            Channel::Lark
        };
        assert_eq!(
            h.generation(other, h.workspace, installation).await,
            Err(ClaimError::Invalid)
        );
        let pending = key(&h, channel, installation, epoch);
        for invalid in [
            InboundKey {
                generation: None,
                ..pending.clone()
            },
            InboundKey {
                generation: Some(Uuid::new_v4()),
                ..pending.clone()
            },
            InboundKey {
                workspace: Uuid::new_v4(),
                ..pending.clone()
            },
        ] {
            assert_eq!(
                claim_inbound(&h.pool, invalid).await,
                Err(ClaimError::Invalid)
            );
        }
        let claim = acquire(&h, pending).await;
        let processed = acquire(&h, key(&h, channel, installation, epoch)).await;
        assert_eq!(
            process_inbound(&h.pool, &processed, |_| Box::pin(async { Ok(()) })).await,
            Ok(Some(()))
        );
        let raw = match mint(&h, channel, installation, epoch).await.unwrap() {
            BindingMint::Fresh { raw, .. } => raw,
            _ => panic!("unexpected throttle"),
        };
        assert!(matches!(
            mint(&h, channel, installation, Uuid::new_v4()).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
        assert_eq!(h.install(channel).await, Ok(installation));
        let latest = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        assert_ne!(latest, epoch);
        assert_stale(&h, &claim).await;
        assert!(matches!(
            mint(&h, channel, installation, epoch).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
        assert_eq!(counts(&h, installation).await, (1, 0, 0));
        assert_eq!(
            claim_inbound(
                &h.pool,
                InboundKey {
                    generation: Some(latest),
                    ..processed.key.clone()
                }
            )
            .await,
            Ok(ClaimResult::Duplicate)
        );
        assert_eq!(
            call(
                &h.app,
                "POST",
                &format!("/api/{}/binding/redeem", channel.path()),
                &token(h.owner),
                json!({"token":raw})
            )
            .await
            .0,
            StatusCode::GONE
        );
        assert!(matches!(
            mint(&h, channel, installation, latest).await.unwrap(),
            BindingMint::Fresh { .. }
        ));
        let replacement = acquire(
            &h,
            InboundKey {
                generation: Some(latest),
                ..claim.key.clone()
            },
        )
        .await;
        assert!(!release_inbound(&h.pool, &claim).await.unwrap());
        assert_eq!(
            process_inbound(&h.pool, &replacement, |_| Box::pin(async { Ok(()) })).await,
            Ok(Some(()))
        );
        assert_eq!(h.rt.events.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_connection_rotation_failure_restores_claims_tokens_and_epoch() {
    for channel in [
        Channel::Lark,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Slack,
    ] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        let claim = acquire(&h, key(&h, channel, installation, epoch)).await;
        let _raw = mint(&h, channel, installation, epoch).await.unwrap();
        let db = h.pool.get().await.unwrap();
        let before: Value = db
            .query_one(
                "SELECT config FROM channel_installation WHERE id=$1",
                &[&installation],
            )
            .await
            .unwrap()
            .get(0);
        *h.reject.lock().unwrap() = true;
        assert!(h.install(channel).await.is_err());
        *h.reject.lock().unwrap() = false;
        // Failure after deletion must restore both cleanup and the installation update.
        db.batch_execute("CREATE FUNCTION channels_sprint_stream_rotation_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.consumed_at IS NULL THEN RAISE EXCEPTION 'synthetic cleanup rollback'; END IF; RETURN OLD; END $$; CREATE TRIGGER channels_sprint_stream_rotation_abort BEFORE DELETE ON channel_binding_token FOR EACH ROW EXECUTE FUNCTION channels_sprint_stream_rotation_abort();").await.unwrap();
        let failed = h.install(channel).await;
        db.batch_execute("DROP TRIGGER channels_sprint_stream_rotation_abort ON channel_binding_token; DROP FUNCTION channels_sprint_stream_rotation_abort();").await.unwrap();
        assert_eq!(failed, Err(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(counts(&h, installation).await, (1, 1, 0));
        let after: Value = db
            .query_one(
                "SELECT config FROM channel_installation WHERE id=$1",
                &[&installation],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(before, after);
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Ok(epoch)
        );
        assert_eq!(
            claim_inbound(&h.pool, claim.key.clone()).await,
            Ok(ClaimResult::Duplicate)
        );
        assert_eq!(h.rt.events.lock().unwrap().len(), 1);
        assert_eq!(
            process_inbound(&h.pool, &claim, |_| Box::pin(async { Ok(()) })).await,
            Ok(Some(()))
        );
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_connection_revoke_waits_for_admitted_transaction_and_rolls_back_cleanup() {
    for channel in [
        Channel::Lark,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Slack,
    ] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        let claim = acquire(&h, key(&h, channel, installation, epoch)).await;
        let _raw = mint(&h, channel, installation, epoch).await.unwrap();
        let db = h.pool.get().await.unwrap();
        let task = Uuid::new_v4();
        let binding = Uuid::new_v4();
        db.execute("INSERT INTO channel_task_delivery(task_id,binding_id,installation_id,channel_type,channel_chat_id,chat_type,route_revision) VALUES($1,$2,$3,$4,'synthetic-chat','p2p',1)",&[&task,&binding,&installation,&channel.database_type()]).await.unwrap();
        db.execute("INSERT INTO channel_outbound_message(installation_id,channel_type,channel_message_id,binding_id,route_revision,outbound_kind) VALUES($1,$2,$3,$4,1,'reply')",&[&installation,&channel.database_type(),&task.to_string(),&binding]).await.unwrap();
        assert_eq!(
            h.revoke(channel, installation, h.outsider).await,
            // The workspace middleware 404s a non-member before revoke runs.
            StatusCode::NOT_FOUND
        );
        db.batch_execute("CREATE FUNCTION channels_sprint_stream_revoke_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic revoke rollback'; END $$; CREATE TRIGGER channels_sprint_stream_revoke_abort BEFORE DELETE ON channel_task_delivery FOR EACH ROW EXECUTE FUNCTION channels_sprint_stream_revoke_abort();").await.unwrap();
        let failed = h.revoke(channel, installation, h.owner).await;
        db.batch_execute("DROP TRIGGER channels_sprint_stream_revoke_abort ON channel_task_delivery; DROP FUNCTION channels_sprint_stream_revoke_abort();").await.unwrap();
        assert_eq!(failed, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(counts(&h, installation).await, (1, 1, 1));
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Ok(epoch)
        );
        assert_eq!(h.rt.events.lock().unwrap().len(), 1);
        let mut guard_db = h.pool.get().await.unwrap();
        let guard = guard_db.transaction().await.unwrap();
        assert!(current_in_transaction(&guard, &claim.key).await.unwrap());
        let revoke = h.revoke(channel, installation, h.owner);
        tokio::pin!(revoke);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut revoke)
                .await
                .is_err()
        );
        guard.commit().await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), revoke)
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Err(ClaimError::Invalid)
        );
        assert_stale(&h, &claim).await;
        assert!(matches!(
            mint(&h, channel, installation, epoch).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
        assert_eq!(counts(&h, installation).await, (0, 0, 0));
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
        assert_eq!(h.rt.events.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_stale_provider_verification_cannot_restore_rotated_or_revoked_install() {
    for channel in [Channel::Telegram, Channel::Slack] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let original = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        let (entered, rx) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *h.pause.lock().unwrap() = Some((entered, wait));
        let delayed = h.install(channel);
        tokio::pin!(delayed);
        tokio::select! {
            _ = &mut delayed => panic!("provider did not pause"),
            done = rx => done.unwrap(),
        }
        assert_eq!(h.install(channel).await, Ok(installation));
        let latest = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        assert_ne!(latest, original);
        release.send(()).unwrap();
        assert_eq!(delayed.await, Err(StatusCode::CONFLICT));
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Ok(latest)
        );

        let (entered, rx) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *h.pause.lock().unwrap() = Some((entered, wait));
        let delayed = h.install(channel);
        tokio::pin!(delayed);
        tokio::select! {
            _ = &mut delayed => panic!("provider did not pause"),
            done = rx => done.unwrap(),
        }
        assert_eq!(
            h.revoke(channel, installation, h.owner).await,
            StatusCode::NO_CONTENT
        );
        release.send(()).unwrap();
        assert_eq!(delayed.await, Err(StatusCode::CONFLICT));
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Err(ClaimError::Invalid)
        );
        assert_eq!(h.install(channel).await, Ok(installation));
        let reconnected = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        assert_ne!(reconnected, latest);
        assert_eq!(h.rt.events.lock().unwrap().len(), 4);
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_rotation_waits_for_admitted_inbound_transaction() {
    for channel in [Channel::Telegram, Channel::Slack] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        let claim = acquire(&h, key(&h, channel, installation, epoch)).await;
        let mut db = h.pool.get().await.unwrap();
        let admitted = db.transaction().await.unwrap();
        assert!(current_in_transaction(&admitted, &claim.key).await.unwrap());
        let rotate = h.install(channel);
        tokio::pin!(rotate);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut rotate)
                .await
                .is_err()
        );
        admitted.commit().await.unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(5), rotate)
                .await
                .unwrap(),
            Ok(installation)
        );
        let latest = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        assert_ne!(latest, epoch);
        assert_stale(&h, &claim).await;
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_connection_legacy_malformed_and_archived_generations_fail_closed() {
    for channel in [
        Channel::Lark,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Slack,
    ] {
        let h = Harness::new().await;
        let installation = h.install(channel).await.unwrap();
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        let db = h.pool.get().await.unwrap();
        for malformed in [
            json!(null),
            json!("invalid"),
            json!(epoch.simple().to_string()),
            json!(epoch.to_string().to_uppercase()),
            json!(12),
        ] {
            db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',$2) WHERE id=$1",&[&installation,&malformed]).await.unwrap();
            assert_eq!(
                h.generation(channel, h.workspace, installation).await,
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
            h.generation(channel, h.workspace, installation).await,
            Err(ClaimError::Invalid)
        );
        assert_eq!(h.install(channel).await, Ok(installation));
        let epoch = h
            .generation(channel, h.workspace, installation)
            .await
            .unwrap();
        db.execute(
            "UPDATE agent SET archived_at=now() WHERE id=$1",
            &[&h.agent],
        )
        .await
        .unwrap();
        assert_eq!(
            h.generation(channel, h.workspace, installation).await,
            Err(ClaimError::Invalid)
        );
        assert_eq!(
            claim_inbound(&h.pool, key(&h, channel, installation, epoch)).await,
            Err(ClaimError::Invalid)
        );
        assert!(matches!(
            mint(&h, channel, installation, epoch).await,
            Err(BindingMintError::InstallationUnavailable)
        ));
    }
}
