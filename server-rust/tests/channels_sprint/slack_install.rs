use super::*;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use multica_server::api::{
    channels_sprint_slack::SlackInstaller, channels_sprint_telegram::CredentialBox,
};

struct InstallRuntime {
    installer: SlackInstaller,
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
    fn slack_installer(&self) -> Option<&SlackInstaller> {
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
    auth: Arc<Mutex<Value>>,
    bot: Arc<Mutex<Value>>,
    socket: Arc<Mutex<Value>>,
    pause: Pause,
}
async fn provider_reply(
    State(provider): State<Provider>,
    headers: HeaderMap,
    request: axum::http::Request<axum::body::Body>,
) -> Json<Value> {
    assert_eq!(request.method(), "POST");
    let method = request
        .uri()
        .path()
        .strip_prefix("/api/")
        .unwrap()
        .to_string();
    let credential = headers.get("authorization").unwrap().to_str().unwrap();
    assert!(credential.starts_with("Bearer "));
    if method == "apps.connections.open" {
        assert!(credential.starts_with("Bearer xapp-"));
    } else {
        assert!(credential.starts_with("Bearer xoxb-"));
    }
    provider.calls.lock().unwrap().push(method.clone());
    if method == "apps.connections.open" {
        let pause = { provider.pause.lock().unwrap().take() };
        if let Some((entered, release)) = pause {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
    let result = match method.as_str() {
        "auth.test" => provider.auth.lock().unwrap().clone(),
        "bots.info" => provider.bot.lock().unwrap().clone(),
        "apps.connections.open" => provider.socket.lock().unwrap().clone(),
        _ => panic!("unexpected Slack provider operation"),
    };
    Json(result)
}
async fn fake_provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let provider = Provider {
        calls: Arc::new(Mutex::new(Vec::new())),
        auth: Arc::new(Mutex::new(
            json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"}),
        )),
        bot: Arc::new(Mutex::new(
            json!({"ok":true,"bot":{"id":"B73","app_id":"A73"}}),
        )),
        socket: Arc::new(Mutex::new(
            json!({"ok":true,"url":"wss://fake.invalid/socket?private=synthetic"}),
        )),
        pause: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .fallback(provider_reply)
        .with_state(provider.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (provider, origin, server)
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
    let owner = insert_user(db, "slack-owner", &suffix).await;
    let member = insert_user(db, "slack-member", &suffix).await;
    let outsider = insert_user(db, "slack-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Slack synthetic',$1) RETURNING id",
            &[&format!("slack-synthetic-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Slack synthetic','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
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
const BOT: &str = "xoxb-synthetic_73";
const APP: &str = "xapp-1-A73-1-synthetic_73";
fn install_body() -> Value {
    json!({"bot_token":BOT,"app_token":APP})
}
fn install_path(workspace: Uuid, agent: Uuid) -> String {
    format!("/api/workspaces/{workspace}/slack/install/byo?agent_id={agent}")
}
fn runtime(origin: &str) -> Arc<InstallRuntime> {
    Arc::new(InstallRuntime {
        installer: SlackInstaller::for_loopback_tests(&[73; 32], origin).unwrap(),
        events: Mutex::new(Vec::new()),
    })
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_slack_byo_install_ciphertext_and_revoke() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, member, outsider) = fixture(&db).await;
    let (provider, origin, server) = fake_provider().await;
    let rt = runtime(&origin);
    let path = install_path(workspace, agent);
    let disabled = app(pool.clone(), Arc::new(FakeRuntime::default()));
    assert_eq!(
        call(&disabled, "POST", &path, &token(owner), install_body())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let api = app(pool.clone(), rt.clone());
    for jwt in ["invalid", &token(member), &token(outsider)] {
        assert_ne!(
            call(&api, "POST", &path, jwt, install_body()).await.0,
            StatusCode::OK
        );
    }
    assert!(
        provider.calls.lock().unwrap().is_empty(),
        "unauthorized provider traffic"
    );
    let (_, other, _, _, _) = fixture(&db).await;
    assert_eq!(
        call(
            &api,
            "POST",
            &install_path(workspace, other),
            &token(owner),
            install_body()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        call(&api, "POST", &path, &token(owner), install_body())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE agent SET archived_at=NULL WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert!(provider.calls.lock().unwrap().is_empty());
    let (status, installed) = call(&api, "POST", &path, &token(owner), install_body()).await;
    assert_eq!(status, StatusCode::OK, "{installed:?}");
    assert_eq!(installed["team_id"], "T73");
    assert_eq!(installed["bot_user_id"], "U73");
    assert!(!installed.to_string().contains(BOT));
    assert!(!installed.to_string().contains(APP));
    let installation = Uuid::parse_str(installed["id"].as_str().unwrap()).unwrap();
    let config: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    let cipher = CredentialBox::new(&[73; 32]).unwrap();
    assert_eq!(
        cipher
            .open(config["bot_token_encrypted"].as_str().unwrap())
            .unwrap(),
        BOT.as_bytes()
    );
    assert_eq!(
        cipher
            .open(config["app_token_encrypted"].as_str().unwrap())
            .unwrap(),
        APP.as_bytes()
    );
    assert_ne!(config["bot_token_encrypted"], config["app_token_encrypted"]);
    assert_eq!(config["app_id"], "A73");
    assert_eq!(
        provider.calls.lock().unwrap().as_slice(),
        ["auth.test", "bots.info", "apps.connections.open"]
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    let (_, again) = call(&api, "POST", &path, &token(owner), install_body()).await;
    assert_eq!(again["id"], installed["id"]);
    assert_ne!(
        db.query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, Value>(0)["bot_token_encrypted"],
        config["bot_token_encrypted"]
    );
    let (status, list) = call(
        &api,
        "GET",
        &format!("/api/workspaces/{workspace}/slack/installations"),
        &token(member),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list["installations"][0]["team_id"], "T73");
    assert_eq!(list["install_supported"], false); // No production injector or socket supervisor.
    for secret in [
        BOT,
        APP,
        "bot_token_encrypted",
        "app_token_encrypted",
        "synthetic_73",
    ] {
        assert!(!list.to_string().contains(secret));
    }
    let binding = binding_token(&db, workspace, installation, "slack", "U73").await;
    let delete = format!("/api/workspaces/{workspace}/slack/installations/{installation}");
    assert_eq!(
        call(&api, "DELETE", &delete, &token(member), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&api, "DELETE", &delete, &token(owner), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(rt.events.lock().unwrap().len(), 3);
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
        call(
            &api,
            "POST",
            "/api/slack/binding/redeem",
            &token(member),
            json!({"token":binding})
        )
        .await
        .0,
        StatusCode::GONE
    );
    assert_eq!(
        call(&api, "POST", &path, &token(owner), install_body())
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(rt.events.lock().unwrap().len(), 4);
    *provider.auth.lock().unwrap() = json!({"ok":false,"error":format!("{BOT} {APP}")});
    let failed = call(&api, "POST", &path, &token(owner), install_body()).await;
    assert_eq!(failed.0, StatusCode::BAD_REQUEST);
    assert!(!failed.1.to_string().contains(BOT) && !failed.1.to_string().contains(APP));
    assert_eq!(rt.events.lock().unwrap().len(), 4);
    *provider.auth.lock().unwrap() =
        json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"});
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &member],
    )
    .await
    .unwrap();
    let (status, admin_install) = call(&api, "POST", &path, &token(member), install_body()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(admin_install["installer_user_id"], json!(member));
    assert_eq!(rt.events.lock().unwrap().len(), 5);
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&outsider],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'admin')",
        &[&workspace, &outsider],
    )
    .await
    .unwrap();
    let calls = provider.calls.lock().unwrap().len();
    assert_eq!(
        call(&api, "POST", &path, &token(outsider), install_body())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(provider.calls.lock().unwrap().len(), calls);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_slack_byo_conflict_mismatch_and_rollback() {
    fn install_body() -> Value {
        json!({"bot_token":BOT,"app_token":"xapp-1-A75-1-synthetic_73"})
    }
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let rival:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Slack rival','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let (provider, origin, server) = fake_provider().await;
    let rt = runtime(&origin);
    let api = app(pool.clone(), rt.clone());
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B73","app_id":"A75"}});
    let first = install_path(workspace, agent);
    let second = install_path(workspace, rival);
    let jwt = token(owner);
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B73","app_id":"A74"}});
    assert_eq!(
        call(&api, "POST", &first, &jwt, install_body()).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND channel_type='slack'", &[&workspace]).await.unwrap().get::<_,i64>(0),0);
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B73","app_id":"A75"}});
    let (a, b) = tokio::join!(
        call(&api, "POST", &first, &jwt, install_body()),
        call(&api, "POST", &second, &jwt, install_body())
    );
    assert_eq!(
        [a.0, b.0].iter().filter(|s| **s == StatusCode::OK).count(),
        1,
        "{a:?} {b:?}"
    );
    assert_eq!(
        [a.0, b.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    let winner = if a.0 == StatusCode::OK {
        &first
    } else {
        &second
    };
    let loser = if a.0 == StatusCode::OK {
        &second
    } else {
        &first
    };
    let installation = Uuid::parse_str(if a.0 == StatusCode::OK {
        a.1["id"].as_str().unwrap()
    } else {
        b.1["id"].as_str().unwrap()
    })
    .unwrap();
    let (foreign_workspace, foreign_agent, foreign_owner, _, _) = fixture(&db).await;
    let foreign = install_path(foreign_workspace, foreign_agent);
    assert_eq!(
        call(
            &api,
            "POST",
            &foreign,
            &token(foreign_owner),
            install_body()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(&api, "POST", loser, &jwt, install_body()).await.0,
        StatusCode::CONFLICT
    );
    *provider.auth.lock().unwrap() =
        json!({"ok":true,"team_id":"T74","user_id":"U74","bot_id":"B73"});
    assert_eq!(
        call(&api, "POST", winner, &jwt, install_body()).await.0,
        StatusCode::CONFLICT,
        "changed team/user on same app must not retain old bindings"
    );
    *provider.auth.lock().unwrap() =
        json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"});
    db.batch_execute("CREATE FUNCTION channels_sprint_slack_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback'; END $$; CREATE TRIGGER channels_sprint_slack_abort BEFORE INSERT ON channel_installation FOR EACH ROW WHEN (NEW.channel_type='slack' AND NEW.config->>'app_id'='A74') EXECUTE FUNCTION channels_sprint_slack_abort();").await.unwrap();
    *provider.auth.lock().unwrap() =
        json!({"ok":true,"team_id":"T73","user_id":"U74","bot_id":"B74"});
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B74","app_id":"A74"}});
    let other_app = json!({"bot_token":BOT,"app_token":"xapp-1-A74-1-synthetic"});
    let result = call(&api, "POST", loser, &jwt, other_app.clone()).await;
    assert_eq!(result.0, StatusCode::INTERNAL_SERVER_ERROR, "{result:?}");
    assert!(!result.1.to_string().contains("synthetic rollback"));
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND channel_type='slack'", &[&workspace]).await.unwrap().get::<_,i64>(0),1);
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    db.batch_execute("DROP TRIGGER channels_sprint_slack_abort ON channel_installation; DROP FUNCTION channels_sprint_slack_abort();").await.unwrap();
    assert_eq!(
        call(&api, "POST", winner, &jwt, other_app).await.0,
        StatusCode::CONFLICT,
        "bot swap requires fenced cleanup"
    );
    assert_eq!(
        db.query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, Value>(0)["app_id"],
        "A75"
    );
    assert_eq!(
        call(
            &api,
            "POST",
            loser,
            &jwt,
            json!({"bot_token":BOT,"app_token":"xapp-1-A74-1-synthetic"})
        )
        .await
        .0,
        StatusCode::OK,
        "different Slack apps may share one Slack team"
    );
    *provider.auth.lock().unwrap() =
        json!({"ok":true,"team_id":"T73","user_id":"U73","bot_id":"B73"});
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B73","app_id":"A75"}});
    let winning_agent: Uuid = db
        .query_one(
            "SELECT agent_id FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE agent SET archived_at=now() WHERE id=$1",
        &[&winning_agent],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &api,
            "POST",
            &foreign,
            &token(foreign_owner),
            install_body()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &api,
            "DELETE",
            &format!("/api/workspaces/{workspace}/slack/installations/{installation}"),
            &jwt,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &api,
            "POST",
            &foreign,
            &token(foreign_owner),
            install_body()
        )
        .await
        .0,
        StatusCode::CONFLICT,
        "revoked app routing slots require explicit cleanup before reassignment"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_slack_byo_rechecks_authorization_after_provider() {
    fn install_body() -> Value {
        json!({"bot_token":BOT,"app_token":"xapp-1-A76-1-synthetic_73"})
    }
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (provider, origin, server) = fake_provider().await;
    let rt = runtime(&origin);
    let api = app(pool.clone(), rt.clone());
    *provider.bot.lock().unwrap() = json!({"ok":true,"bot":{"id":"B73","app_id":"A76"}});
    for change in ["removed", "demoted", "archived", "shadow"] {
        let (workspace, agent, owner, _, _) = fixture(&db).await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
        let request_app = api.clone();
        let path = install_path(workspace, agent);
        let request = tokio::spawn(async move {
            call(&request_app, "POST", &path, &token(owner), install_body()).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        match change {
            "archived" => {
                db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
                    .await
                    .unwrap();
            }
            "demoted" => {
                db.execute(
                    "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
                    &[&workspace, &owner],
                )
                .await
                .unwrap();
            }
            "shadow" => {
                db.execute(
                    "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
                    &[&owner],
                )
                .await
                .unwrap();
            }
            _ => {
                db.execute(
                    "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&workspace, &owner],
                )
                .await
                .unwrap();
            }
        }
        release_tx.send(()).unwrap();
        let result = request.await.unwrap();
        assert_eq!(
            result.0,
            if change == "archived" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::FORBIDDEN
            },
            "{change}: {result:?}"
        );
        assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND channel_type='slack'", &[&workspace]).await.unwrap().get::<_,i64>(0),0);
    }
    assert!(rt.events.lock().unwrap().is_empty());
    server.abort();
    let _ = server.await;
}
