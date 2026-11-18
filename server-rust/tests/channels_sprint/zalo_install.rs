use super::*;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use multica_server::api::{
    channels_sprint_telegram::CredentialBox, channels_sprint_zalo::ZaloInstaller,
};

struct InstallRuntime {
    installer: ZaloInstaller,
    events: Mutex<Vec<ChannelEvent>>,
}
#[async_trait]
impl ChannelRuntime for InstallRuntime {
    fn configured(&self, _: Channel) -> bool {
        true
    }
    fn publish(&self, event: ChannelEvent) {
        self.events.lock().unwrap().push(event);
    }
    async fn cancel_guest_tasks(&self, _: GuestCancellation) {}
    fn zalo_installer(&self) -> Option<&ZaloInstaller> {
        Some(&self.installer)
    }
}
type Pause = Arc<
    Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
>;
#[derive(Clone)]
struct Provider {
    calls: Arc<Mutex<Vec<String>>>,
    me: Arc<Mutex<Value>>,
    webhook: Arc<Mutex<Value>>,
    pause: Pause,
}
async fn provider_reply(
    State(provider): State<Provider>,
    Path((bot, method)): Path<(String, String)>,
) -> Json<Value> {
    provider
        .calls
        .lock()
        .unwrap()
        .push(format!("{bot}/{method}"));
    if method == "getWebhookInfo" {
        let pause = { provider.pause.lock().unwrap().take() };
        if let Some((entered, release)) = pause {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
    Json(match method.as_str() {
        "getMe" => provider.me.lock().unwrap().clone(),
        "getWebhookInfo" => provider.webhook.lock().unwrap().clone(),
        _ => json!({"ok":false,"error_code":404}),
    })
}
async fn fake_provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let provider = Provider {
        calls: Arc::new(Mutex::new(vec![])),
        me: Arc::new(Mutex::new(
            json!({"ok":true,"result":{"id":"123456789012345678901234567890","display_name":"Synthetic Bot","account_name":"Synthetic Account"}}),
        )),
        webhook: Arc::new(Mutex::new(json!({"ok":true,"result":{"url":""}}))),
        pause: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/bot{bot}/{method}", post(provider_reply))
        .with_state(provider.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (provider, origin, task)
}
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
async fn fixture(db: &deadpool_postgres::Object) -> (Uuid, Uuid, Uuid, Uuid, Uuid) {
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(db, "zalo-owner", &suffix).await;
    let member = insert_user(db, "zalo-member", &suffix).await;
    let outsider = insert_user(db, "zalo-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Zalo install',$1) RETURNING id",
            &[&format!("zalo-install-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Zalo fake','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    (workspace, agent, owner, member, outsider)
}
fn app(pool: Arc<Pool>, rt: Arc<dyn ChannelRuntime>) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool, SECRET.as_bytes().to_vec()).with_channel_runtime(rt),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_zalo_fake_provider_install_refresh_revoke_and_privacy() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, member, outsider) = fixture(&db).await;
    let (provider, origin, server) = fake_provider().await;
    *provider.me.lock().unwrap() = json!({"ok":true,"result":{"id":"123456789012345678901234567891","display_name":"Synthetic Bot","account_name":"Synthetic Account"}});
    let key = [73; 32];
    let rt = Arc::new(InstallRuntime {
        installer: ZaloInstaller::for_loopback_tests(&key, &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let disabled = app(pool.clone(), Arc::new(FakeRuntime::default()));
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/zalo/install?agent_id={agent}");
    let synthetic = "synthetic_private_zalo_token";
    let body = json!({"bot_token":synthetic});
    assert_eq!(
        call(&disabled, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    for jwt in ["invalid", &token(member), &token(outsider)] {
        assert_ne!(
            call(&app, "POST", &path, jwt, body.clone()).await.0,
            StatusCode::OK
        );
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            &token(owner),
            json!({"bot_token":"bad/path"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (foreign, foreign_agent, _, _, _) = fixture(&db).await;
    assert_ne!(foreign, workspace);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/zalo/install?agent_id={foreign_agent}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(provider.calls.lock().unwrap().is_empty());
    let (status, result) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{result:?}");
    assert_eq!(result["bot_id"], "123456789012345678901234567891");
    assert_eq!(result["bot_name"], "Synthetic Bot");
    assert_eq!(result["account_name"], "Synthetic Account");
    assert_eq!(result["share_link"], "");
    assert!(!result.to_string().contains(synthetic));
    let installation = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let config: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1 AND workspace_id=$2",
            &[&installation, &workspace],
        )
        .await
        .unwrap()
        .get(0);
    let encrypted = config["bot_token_encrypted"].as_str().unwrap();
    assert_ne!(encrypted, synthetic);
    assert_eq!(
        CredentialBox::new(&key).unwrap().open(encrypted).unwrap(),
        synthetic.as_bytes()
    );
    assert!(Uuid::parse_str(config["generation"].as_str().unwrap()).is_ok());
    assert_eq!(provider.calls.lock().unwrap().len(), 2);
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{share_link}','\"https://zalo.me/synthetic\"') WHERE id=$1", &[&installation]).await.unwrap();
    let (_, refreshed) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(refreshed["id"], result["id"]);
    assert_eq!(refreshed["share_link"], "https://zalo.me/synthetic");
    let config2: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(config2["generation"], config["generation"]);
    assert_ne!(
        config2["bot_token_encrypted"],
        config["bot_token_encrypted"]
    );
    let list = call(
        &app,
        "GET",
        &format!("/api/workspaces/{workspace}/zalo/installations"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["installations"][0]["bot_id"], result["bot_id"]);
    assert_eq!(list.1["install_supported"], false);
    assert!(!list.1.to_string().contains("encrypted"));
    assert!(!list.1.to_string().contains(synthetic));
    *provider.webhook.lock().unwrap() = json!({"ok":false,"error_code":404});
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::OK
    );
    *provider.webhook.lock().unwrap() =
        json!({"ok":true,"result":{"url":"https://example.invalid/webhook"}});
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    *provider.me.lock().unwrap() = json!({"ok":false,"error_code":401,"description":synthetic});
    let rejected = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST);
    assert!(!rejected.1.to_string().contains(synthetic));
    assert_eq!(rt.events.lock().unwrap().len(), 3);
    *provider.me.lock().unwrap() = json!({"ok":true,"result":{"id":"123456789012345678901234567891","display_name":"Synthetic Bot"}});
    *provider.webhook.lock().unwrap() = json!({"ok":true,"result":{"url":""}});
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}','\"invalid-epoch\"') WHERE id=$1", &[&installation]).await.unwrap();
    let invalid_epoch = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(invalid_epoch.0, StatusCode::CONFLICT, "{invalid_epoch:?}");
    assert_eq!(
        db.query_one(
            "SELECT config->>'generation' FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "invalid-epoch"
    );
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{generation}',to_jsonb($2::text)) WHERE id=$1", &[&installation,&config2["generation"].as_str().unwrap()]).await.unwrap();
    assert_eq!(rt.events.lock().unwrap().len(), 3);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{workspace}/zalo/installations/{installation}"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(rt.events.lock().unwrap().len(), 4);
    *provider.me.lock().unwrap() = json!({"ok":true,"result":{"id":"123456789012345678901234567891","display_name":"Synthetic Bot"}});
    *provider.webhook.lock().unwrap() = json!({"ok":true,"result":{"url":""}});
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body).await.0,
        StatusCode::CONFLICT,
        "reconnect cannot use stale generation while consumers are not fenced"
    );
    assert_eq!(rt.events.lock().unwrap().len(), 4);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_zalo_conflict_rollback_and_reauthorization() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let other: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Zalo rival','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let (provider, origin, server) = fake_provider().await;
    let rt = Arc::new(InstallRuntime {
        installer: ZaloInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/zalo/install?agent_id={agent}");
    let rival = format!("/api/workspaces/{workspace}/zalo/install?agent_id={other}");
    let body = json!({"bot_token":"synthetic_zalo_token_2"});
    let jwt = token(owner);
    let (first, second) = tokio::join!(
        call(&app, "POST", &path, &jwt, body.clone()),
        call(&app, "POST", &rival, &jwt, body.clone())
    );
    assert_eq!(
        [first.0, second.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1,
        "{first:?} {second:?}"
    );
    assert_eq!(
        [first.0, second.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    let winner = if first.0 == StatusCode::OK {
        &path
    } else {
        &rival
    };
    let loser = if first.0 == StatusCode::OK {
        &rival
    } else {
        &path
    };
    let installed = if first.0 == StatusCode::OK {
        &first.1
    } else {
        &second.1
    };
    let installation = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        call(&app, "POST", loser, &jwt, body.clone()).await.0,
        StatusCode::CONFLICT
    );
    *provider.me.lock().unwrap() = json!({"ok":true,"result":{"id":"987654321000000000000000000001","display_name":"Other Bot"}});
    assert_eq!(
        call(&app, "POST", winner, &jwt, body.clone()).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        db.query_one(
            "SELECT config->>'app_id' FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "123456789012345678901234567890"
    );
    db.batch_execute("CREATE FUNCTION channels_sprint_zalo_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic install rollback'; END $$; CREATE TRIGGER channels_sprint_zalo_abort BEFORE INSERT ON channel_installation FOR EACH ROW WHEN (NEW.channel_type='zalo' AND NEW.config->>'app_id'='987654321000000000000000000001') EXECUTE FUNCTION channels_sprint_zalo_abort();").await.unwrap();
    let failed = call(&app, "POST", loser, &jwt, body.clone()).await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert!(!failed.1.to_string().contains("synthetic install rollback"));
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    db.batch_execute("DROP TRIGGER channels_sprint_zalo_abort ON channel_installation; DROP FUNCTION channels_sprint_zalo_abort();").await.unwrap();
    let (new_ws, new_agent, new_owner, _, _) = fixture(&db).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let request_app = app.clone();
    let path2 = format!("/api/workspaces/{new_ws}/zalo/install?agent_id={new_agent}");
    let request = tokio::spawn(async move {
        call(
            &request_app,
            "POST",
            &path2,
            &token(new_owner),
            json!({"bot_token":"synthetic_zalo_token_3"}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1 AND workspace_id=$2",
        &[&new_agent, &new_ws],
    )
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(request.await.unwrap().0, StatusCode::NOT_FOUND);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_installation WHERE workspace_id=$1",
            &[&new_ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    server.abort();
    let _ = server.await;
}
