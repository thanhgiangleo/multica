use super::*;
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use multica_server::api::channels_sprint_telegram::{CredentialBox, TelegramInstaller};

struct InstallRuntime {
    installer: TelegramInstaller,
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
    fn telegram_installer(&self) -> Option<&TelegramInstaller> {
        Some(&self.installer)
    }
}

type ProviderPause = Option<(
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
)>;

#[derive(Clone)]
struct Provider {
    calls: Arc<Mutex<Vec<String>>>,
    me: Arc<Mutex<Value>>,
    webhook: Arc<Mutex<Value>>,
    pause: Arc<Mutex<ProviderPause>>,
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
    let response = match method.as_str() {
        "getMe" => provider.me.lock().unwrap().clone(),
        "getWebhookInfo" => provider.webhook.lock().unwrap().clone(),
        _ => json!({"ok":false,"error_code":404}),
    };
    Json(response)
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
    let owner = insert_user(db, "tg-owner", &suffix).await;
    let member = insert_user(db, "tg-member", &suffix).await;
    let outsider = insert_user(db, "tg-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Telegram install',$1) RETURNING id",
            &[&format!("telegram-install-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Telegram fake','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    (workspace, agent, owner, member, outsider)
}
async fn fake_provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let provider = Provider {
        calls: Arc::new(Mutex::new(Vec::new())),
        me: Arc::new(Mutex::new(
            json!({"ok":true,"result":{"id":123456,"is_bot":true,"username":"SyntheticBot"}}),
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
async fn channels_sprint_telegram_install_fake_provider_and_ciphertext() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, member, outsider) = fixture(&db).await;
    let (provider, origin, server) = fake_provider().await;
    let key = [73; 32];
    let rt = Arc::new(InstallRuntime {
        installer: TelegramInstaller::for_loopback_tests(&key, &origin).unwrap(),
        events: Mutex::new(Vec::new()),
    });
    let disabled = app(pool.clone(), Arc::new(FakeRuntime::default()));
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/telegram/install?agent_id={agent}");
    assert_eq!(
        call(
            &disabled,
            "POST",
            &path,
            &token(owner),
            json!({"bot_token":"123456:synthetic_TOKEN-73"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let synthetic = "123456:synthetic_TOKEN-73";
    let body = json!({"bot_token":synthetic});
    for jwt in ["invalid", &token(member), &token(outsider)] {
        let result = call(&app, "POST", &path, jwt, body.clone()).await;
        assert_ne!(result.0, StatusCode::OK, "{result:?}");
    }
    assert!(
        provider.calls.lock().unwrap().is_empty(),
        "auth failures must not contact provider"
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            &token(owner),
            json!({"bot_token":"123456:bad/path"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(provider.calls.lock().unwrap().is_empty());
    let (foreign, foreign_agent, _, _, _) = fixture(&db).await;
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/telegram/install?agent_id={foreign_agent}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_ne!(foreign, workspace);
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1 AND workspace_id=$2",
        &[&agent, &workspace],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "UPDATE agent SET archived_at=NULL WHERE id=$1 AND workspace_id=$2",
        &[&agent, &workspace],
    )
    .await
    .unwrap();
    assert!(provider.calls.lock().unwrap().is_empty());
    let (status, installed) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{installed:?}");
    assert_eq!(installed["bot_id"], "123456");
    assert_eq!(installed["bot_username"], "SyntheticBot");
    assert!(!installed.to_string().contains(synthetic));
    assert!(!installed.to_string().contains("bot_token_encrypted"));
    let installation = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
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
    assert_eq!(config["app_id"], "123456");
    assert_eq!(
        provider.calls.lock().unwrap().as_slice(),
        [
            format!("{synthetic}/getMe"),
            format!("{synthetic}/getWebhookInfo")
        ]
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    db.execute("INSERT INTO channel_pairing_notice(workspace_id,installation_id,generation,message_id,chat_id,kind) VALUES($1,$2,$3,'synthetic:rotation',740,'invalid')", &[&workspace,&installation,&Uuid::parse_str(config["generation"].as_str().unwrap()).unwrap()]).await.unwrap();
    let (_, reinstalled) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(reinstalled["id"], installed["id"]);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_pairing_notice WHERE installation_id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let refreshed: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(
        refreshed["bot_token_encrypted"],
        config["bot_token_encrypted"]
    );
    let list = call(
        &app,
        "GET",
        &format!("/api/workspaces/{workspace}/telegram/installations"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["installations"][0]["bot_id"], "123456");
    assert!(!list.1.to_string().contains(synthetic));
    assert!(!list.1.to_string().contains("bot_token_encrypted"));
    assert_eq!(
        list.1["install_supported"], false,
        "production runtime provider wiring is still absent"
    );
    *provider.webhook.lock().unwrap() =
        json!({"ok":true,"result":{"url":"https://example.invalid/webhook"}});
    let rejected = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    assert_eq!(
        db.query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, Value>(0),
        refreshed
    );
    *provider.webhook.lock().unwrap() = json!({"ok":true,"result":{"url":""}});
    *provider.me.lock().unwrap() = json!({"ok":false,"error_code":401,"description":synthetic});
    let rejected = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert!(!rejected.1.to_string().contains(synthetic));
    *provider.me.lock().unwrap() =
        json!({"ok":true,"result":{"id":999,"is_bot":true,"username":"OtherBot"}});
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    *provider.me.lock().unwrap() =
        json!({"ok":true,"result":{"id":123456,"is_bot":true,"username":"SyntheticBot"}});
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            &token(owner),
            json!({"bot_token":"654321:synthetic_other"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST,
        "mismatched getMe cannot swap bots"
    );
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_telegram_install_conflict_and_rollback() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let second: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Telegram rival','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let (provider, origin, server) = fake_provider().await;
    *provider.me.lock().unwrap() =
        json!({"ok":true,"result":{"id":123457,"is_bot":true,"username":"ConcurrentBot"}});
    let rt = Arc::new(InstallRuntime {
        installer: TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(Vec::new()),
    });
    let app = app(pool.clone(), rt.clone());
    let first = format!("/api/workspaces/{workspace}/telegram/install?agent_id={agent}");
    let rival = format!("/api/workspaces/{workspace}/telegram/install?agent_id={second}");
    let body = json!({"bot_token":"123457:synthetic_TOKEN-73"});
    let jwt = token(owner);
    let (a, b) = tokio::join!(
        call(&app, "POST", &first, &jwt, body.clone()),
        call(&app, "POST", &rival, &jwt, body.clone())
    );
    assert_eq!(
        [a.0, b.0]
            .into_iter()
            .filter(|s| *s == StatusCode::OK)
            .count(),
        1,
        "{a:?} {b:?}"
    );
    assert_eq!(
        [a.0, b.0]
            .into_iter()
            .filter(|s| *s == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    let winner = if a.0 == StatusCode::OK {
        &first
    } else {
        &rival
    };
    let alternate = if a.0 == StatusCode::OK {
        &rival
    } else {
        &first
    };
    let id = if a.0 == StatusCode::OK {
        &a.1["id"]
    } else {
        &b.1["id"]
    };
    let installation = Uuid::parse_str(id.as_str().unwrap()).unwrap();
    assert_eq!(
        call(&app, "POST", alternate, &jwt, body.clone()).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "POST",
            winner,
            &jwt,
            json!({"bot_token":"654321:synthetic_other"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    *provider.me.lock().unwrap() =
        json!({"ok":true,"result":{"id":654321,"is_bot":true,"username":"OtherBot"}});
    assert_eq!(
        call(
            &app,
            "POST",
            winner,
            &jwt,
            json!({"bot_token":"654321:synthetic_other"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let row = db
        .query_one(
            "SELECT id,config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Uuid>("id"), installation);
    assert_eq!(row.get::<_, Value>("config")["app_id"], "123457");
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    db.batch_execute("CREATE FUNCTION channels_sprint_telegram_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic install rollback'; END $$; CREATE TRIGGER channels_sprint_telegram_abort BEFORE INSERT ON channel_installation FOR EACH ROW WHEN (NEW.channel_type='telegram' AND NEW.config->>'app_id'='654321') EXECUTE FUNCTION channels_sprint_telegram_abort();").await.unwrap();
    let failed = call(
        &app,
        "POST",
        alternate,
        &jwt,
        json!({"bot_token":"654321:synthetic_other"}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert!(!failed.1.to_string().contains("synthetic install rollback"));
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND channel_type='telegram'", &[&workspace]).await.unwrap().get::<_,i64>(0), 1);
    db.batch_execute("DROP TRIGGER channels_sprint_telegram_abort ON channel_installation; DROP FUNCTION channels_sprint_telegram_abort();").await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_telegram_install_rechecks_authorization_after_provider() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (provider, origin, server) = fake_provider().await;
    let rt = Arc::new(InstallRuntime {
        installer: TelegramInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(Vec::new()),
    });
    let app = app(pool.clone(), rt.clone());
    for archive in [false, true] {
        let (workspace, agent, owner, _, _) = fixture(&db).await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
        let request_app = app.clone();
        let path = format!("/api/workspaces/{workspace}/telegram/install?agent_id={agent}");
        let request = tokio::spawn(async move {
            call(
                &request_app,
                "POST",
                &path,
                &token(owner),
                json!({"bot_token":"123456:synthetic_TOKEN-73"}),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        if archive {
            db.execute(
                "UPDATE agent SET archived_at=now() WHERE workspace_id=$1 AND id=$2",
                &[&workspace, &agent],
            )
            .await
            .unwrap();
        } else {
            db.execute(
                "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&workspace, &owner],
            )
            .await
            .unwrap();
        }
        release_tx.send(()).unwrap();
        let result = request.await.unwrap();
        assert_eq!(
            result.0,
            if archive {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::FORBIDDEN
            },
            "{result:?}"
        );
        assert_eq!(
            db.query_one(
                "SELECT count(*) FROM channel_installation WHERE workspace_id=$1",
                &[&workspace]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
            0
        );
    }
    assert!(rt.events.lock().unwrap().is_empty());
    server.abort();
    let _ = server.await;
}
