use super::*;
use axum::{Json, Router, extract::State, routing::post};
use multica_server::api::{
    channels_sprint_dingtalk::DingTalkInstaller, channels_sprint_telegram::CredentialBox,
};

struct InstallRuntime {
    installer: DingTalkInstaller,
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
    fn dingtalk_installer(&self) -> Option<&DingTalkInstaller> {
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
    calls: Arc<Mutex<usize>>,
    status: Arc<Mutex<StatusCode>>,
    pause: Pause,
}
async fn reply(
    State(provider): State<Provider>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert!(
        body["appKey"]
            .as_str()
            .unwrap()
            .starts_with("synthetic-app-")
    );
    assert_eq!(body["appSecret"], "synthetic-secret");
    *provider.calls.lock().unwrap() += 1;
    let paused = { provider.pause.lock().unwrap().take() };
    if let Some((entered, resume)) = paused {
        let _ = entered.send(());
        let _ = resume.await;
    }
    let status = *provider.status.lock().unwrap();
    (
        status,
        Json(if status == StatusCode::OK {
            json!({"accessToken":"synthetic-access","expireIn":7200})
        } else {
            json!({"code":"InvalidAuthentication","message":"synthetic-secret leaked by provider"})
        }),
    )
}
async fn fake_provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let provider = Provider {
        calls: Arc::new(Mutex::new(0)),
        status: Arc::new(Mutex::new(StatusCode::OK)),
        pause: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn({
        let provider = provider.clone();
        async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/v1.0/oauth2/accessToken", post(reply))
                    .with_state(provider),
            )
            .await
            .unwrap();
        }
    });
    (provider, origin, server)
}
fn pool() -> Arc<Pool> {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
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
    let owner = insert_user(db, "ding-owner", &suffix).await;
    let member = insert_user(db, "ding-member", &suffix).await;
    let outsider = insert_user(db, "ding-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Ding install',$1) RETURNING id",
            &[&format!("ding-install-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Ding fake','local',$2,'private','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
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
async fn channels_sprint_dingtalk_install_auth_rotation_conflict_and_revoke() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, member, outsider) = fixture(&db).await;
    let (provider, origin, server) = fake_provider().await;
    let key = [73; 32];
    let rt = Arc::new(InstallRuntime {
        installer: DingTalkInstaller::for_loopback_tests(&key, &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={agent}");
    let app_key = format!("synthetic-app-{agent}");
    let body = json!({"client_id":app_key,"client_secret":"synthetic-secret"});
    for jwt in ["invalid", &token(member), &token(outsider)] {
        assert_ne!(
            call(&app, "POST", &path, jwt, body.clone()).await.0,
            StatusCode::OK
        );
    }
    let (foreign, foreign_agent, _, _, _) = fixture(&db).await;
    assert_ne!(workspace, foreign);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={foreign_agent}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(*provider.calls.lock().unwrap(), 0);
    let (status, first) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first:?}");
    assert_eq!(first["agent_id"], json!(agent));
    let installation = Uuid::parse_str(first["id"].as_str().unwrap()).unwrap();
    let stored: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored["app_id"], app_key);
    assert_eq!(stored["robot_code"], app_key);
    let secret = stored["app_secret_encrypted"].as_str().unwrap();
    assert_eq!(
        CredentialBox::new(&key).unwrap().open(secret).unwrap(),
        b"synthetic-secret"
    );
    assert!(!first.to_string().contains("synthetic-secret"));
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    // Preserve server-managed config on a same-app key rotation.
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{ws_lease_token}','\"server-private\"') WHERE id=$1",&[&installation]).await.unwrap();
    let (status, rotated) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{rotated:?}");
    assert_eq!(rotated["id"], first["id"]);
    let stored2: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored2["ws_lease_token"], "server-private");
    assert_ne!(
        stored2["app_secret_encrypted"],
        stored["app_secret_encrypted"]
    );
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    let listing = call(
        &app,
        "GET",
        &format!("/api/workspaces/{workspace}/dingtalk/installations"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(listing.0, StatusCode::OK);
    assert!(!listing.1.to_string().contains("synthetic-secret"));
    assert!(!listing.1.to_string().contains("server-private"));
    *provider.status.lock().unwrap() = StatusCode::BAD_REQUEST;
    let (bad, reason) = call(&app, "POST", &path, &token(owner), body.clone()).await;
    assert_eq!(bad, StatusCode::BAD_REQUEST, "{reason:?}");
    assert!(!reason.to_string().contains("synthetic-secret"));
    *provider.status.lock().unwrap() = StatusCode::INTERNAL_SERVER_ERROR;
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    *provider.status.lock().unwrap() = StatusCode::OK;
    let (_, second_agent, _, _, _) = fixture(&db).await;
    let count = *provider.calls.lock().unwrap();
    // Wrong workspace owner cannot reach fake provider.
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={second_agent}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(*provider.calls.lock().unwrap(), count);
    let agent2:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Ding another','local',$2,'private','workspace') RETURNING id",&[&workspace,&owner]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={agent2}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE channel_type='dingtalk' AND config->>'app_id'=$1",&[&app_key]).await.unwrap().get::<_,i64>(0),1);
    let revoked = call(
        &app,
        "DELETE",
        &format!("/api/workspaces/{workspace}/dingtalk/installations/{installation}"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(revoked.0, StatusCode::NO_CONTENT, "{revoked:?}");
    assert_eq!(
        call(&app, "POST", &path, &token(owner), body).await.0,
        StatusCode::CONFLICT
    );
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_dingtalk_stale_verify_cannot_overwrite_rotation() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let (provider, origin, server) = fake_provider().await;
    let rt = Arc::new(InstallRuntime {
        installer: DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={agent}");
    let body =
        json!({"client_id":format!("synthetic-app-{agent}"),"client_secret":"synthetic-secret"});
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let pending = tokio::spawn({
        let app = app.clone();
        let path = path.clone();
        let body = body.clone();
        async move { call(&app, "POST", &path, &token(owner), body).await }
    });
    entered_rx.await.unwrap();
    let (status, second) = call(&app, "POST", &path, &token(owner), body).await;
    assert_eq!(status, StatusCode::OK, "{second:?}");
    release_tx.send(()).unwrap();
    assert_eq!(pending.await.unwrap().0, StatusCode::CONFLICT);
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_dingtalk_rechecks_authorization_and_revoke_after_http() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (provider, origin, server) = fake_provider().await;
    let rt = Arc::new(InstallRuntime {
        installer: DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), rt.clone());
    for revoke in [false, true] {
        let (workspace, agent, owner, _, _) = fixture(&db).await;
        let path = format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={agent}");
        let body = json!({"client_id":format!("synthetic-app-{agent}"),"client_secret":"synthetic-secret"});
        let mut installation = None;
        if revoke {
            let (status, result) = call(&app, "POST", &path, &token(owner), body.clone()).await;
            assert_eq!(status, StatusCode::OK, "{result:?}");
            installation = Some(result["id"].as_str().unwrap().to_owned());
        }
        let before = rt.events.lock().unwrap().len();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
        let pending = tokio::spawn({
            let app = app.clone();
            let path = path.clone();
            async move { call(&app, "POST", &path, &token(owner), body).await }
        });
        entered_rx.await.unwrap();
        if let Some(installation) = installation {
            assert_eq!(
                call(
                    &app,
                    "DELETE",
                    &format!("/api/workspaces/{workspace}/dingtalk/installations/{installation}"),
                    &token(owner),
                    Value::Null
                )
                .await
                .0,
                StatusCode::NO_CONTENT
            );
        } else {
            db.execute(
                "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&workspace, &owner],
            )
            .await
            .unwrap();
        }
        release_tx.send(()).unwrap();
        assert_eq!(
            pending.await.unwrap().0,
            if revoke {
                StatusCode::CONFLICT
            } else {
                StatusCode::FORBIDDEN
            }
        );
        let active:i64=db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND status='active'",&[&workspace,&agent]).await.unwrap().get(0);
        assert_eq!(active, 0);
        assert_eq!(
            rt.events.lock().unwrap().len(),
            before + usize::from(revoke)
        );
    }
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_dingtalk_agent_owner_and_atomic_rollback() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, _, member, _) = fixture(&db).await;
    db.execute(
        "UPDATE agent SET owner_id=$1 WHERE id=$2",
        &[&member, &agent],
    )
    .await
    .unwrap();
    let (_, origin, server) = fake_provider().await;
    let rt = Arc::new(InstallRuntime {
        installer: DingTalkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), rt.clone());
    let path = format!("/api/workspaces/{workspace}/dingtalk/install/byo?agent_id={agent}");
    let body =
        json!({"client_id":format!("synthetic-app-{agent}"),"client_secret":"synthetic-secret"});
    let (status, result) = call(&app, "POST", &path, &token(member), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{result:?}");
    let installation = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let original: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    let name = format!("channels_sprint_ding_{}", agent.simple());
    db.batch_execute(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic persistence failure'; END $$; CREATE TRIGGER {name} BEFORE UPDATE ON channel_installation FOR EACH ROW WHEN (NEW.id='{installation}'::uuid) EXECUTE FUNCTION {name}();")).await.unwrap();
    let (status, failure) = call(&app, "POST", &path, &token(member), body).await;
    db.batch_execute(&format!(
        "DROP TRIGGER {name} ON channel_installation; DROP FUNCTION {name}();"
    ))
    .await
    .unwrap();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{failure:?}");
    assert!(!failure.to_string().contains("synthetic persistence"));
    assert!(!failure.to_string().contains("synthetic-secret"));
    let after: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(original, after);
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    server.abort();
}
