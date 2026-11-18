use super::*;
use futures_util::{SinkExt, StreamExt};
use multica_server::api::{
    channels_sprint_telegram::CredentialBox, channels_sprint_wecom::WecomInstaller,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[path = "wecom_epoch.rs"]
mod epoch;

struct InstallRuntime {
    installer: WecomInstaller,
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
    fn wecom_installer(&self) -> Option<&WecomInstaller> {
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
    code: Arc<Mutex<i64>>,
    calls: Arc<Mutex<usize>>,
    pause: Pause,
}
async fn provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let p = Provider {
        code: Arc::new(Mutex::new(0)),
        calls: Arc::new(Mutex::new(0)),
        pause: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn({
        let p = p.clone();
        async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let p = p.clone();
                tokio::spawn(async move {
                    let mut ws = accept_async(stream).await.unwrap();
                    let frame = ws.next().await.unwrap().unwrap().into_text().unwrap();
                    let req: Value = serde_json::from_str(&frame).unwrap();
                    assert_eq!(req["cmd"], "aibot_subscribe");
                    assert!(
                        req["body"]["bot_id"]
                            .as_str()
                            .unwrap()
                            .starts_with("synthetic-bot-")
                    );
                    assert_eq!(req["body"]["secret"], "synthetic-secret");
                    *p.calls.lock().unwrap() += 1;
                    let pause = { p.pause.lock().unwrap().take() };
                    if let Some((entered, resume)) = pause {
                        let _ = entered.send(());
                        let _ = resume.await;
                    }
                    let code = *p.code.lock().unwrap();
                    let reply = json!({"cmd":"aibot_subscribe","headers":{"req_id":req["headers"]["req_id"]},"errcode":code,"errmsg":"synthetic-secret in provider error"});
                    let _ = ws.send(Message::Text(reply.to_string().into())).await;
                });
            }
        }
    });
    (p, origin, task)
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
            .max_size(10)
            .build()
            .unwrap(),
    )
}
async fn fixture(db: &deadpool_postgres::Object) -> (Uuid, Uuid, Uuid, Uuid, Uuid) {
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(db, "wecom-owner", &suffix).await;
    let admin = insert_user(db, "wecom-admin", &suffix).await;
    let outsider = insert_user(db, "wecom-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('WeCom install',$1) RETURNING id",
            &[&format!("wecom-install-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [(owner, "owner"), (admin, "admin")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let agent:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'WeCom fake','local',$2,'private','workspace') RETURNING id",&[&workspace,&owner]).await.unwrap().get(0);
    (workspace, agent, owner, admin, outsider)
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
async fn channels_sprint_wecom_proof_auth_rotation_and_collision() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, admin, outsider) = fixture(&db).await;
    let (p, origin, server) = provider().await;
    let key = [73; 32];
    let runtime = Arc::new(InstallRuntime {
        installer: WecomInstaller::for_loopback_tests(&key, &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), runtime.clone());
    let bot = format!("synthetic-bot-{agent}");
    let url = format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}");
    let body = json!({"bot_id":bot,"secret":"synthetic-secret","bot_name":"Synthetic Name"});
    for jwt in ["bad", &token(outsider)] {
        assert_ne!(
            call(&app, "POST", &url, jwt, body.clone()).await.0,
            StatusCode::OK
        );
    }
    let (foreign, foreign_agent, _, _, _) = fixture(&db).await;
    assert_ne!(workspace, foreign);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={foreign_agent}"),
            &token(owner),
            body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(*p.calls.lock().unwrap(), 0);
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &admin],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "POST", &url, &token(admin), body.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &admin],
    )
    .await
    .unwrap();
    assert_ne!(
        call(
            &app,
            "POST",
            &url,
            &token(admin),
            json!({"bot_id":bot,"secret":"s".repeat(17000)})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(*p.calls.lock().unwrap(), 0);
    let (status, response) = call(&app, "POST", &url, &token(admin), body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response["bot_id"], bot);
    assert!(!response.to_string().contains("synthetic-secret"));
    let installation = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
    let stored: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored["app_id"], bot);
    assert_eq!(
        CredentialBox::new(&key)
            .unwrap()
            .open(stored["secret_encrypted"].as_str().unwrap())
            .unwrap(),
        b"synthetic-secret"
    );
    assert_eq!(runtime.events.lock().unwrap().len(), 1);
    db.execute("UPDATE channel_installation SET config=jsonb_set(config,'{lease_tag}','\"server-data\"') WHERE id=$1",&[&installation]).await.unwrap();
    let rotation = json!({"bot_id":bot,"secret":"synthetic-secret"});
    let (status, again) = call(&app, "POST", &url, &token(owner), rotation.clone()).await;
    assert_eq!(status, StatusCode::OK, "{again:?}");
    assert_eq!(again["id"], response["id"]);
    let stored2: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored2["bot_display_name"], "Synthetic Name");
    assert_eq!(stored2["lease_tag"], "server-data");
    assert_ne!(stored2["secret_encrypted"], stored["secret_encrypted"]);
    assert_eq!(runtime.events.lock().unwrap().len(), 2);
    *p.code.lock().unwrap() = 40001;
    let (status, err) = call(&app, "POST", &url, &token(owner), rotation.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err:?}");
    assert!(!err.to_string().contains("synthetic-secret"));
    *p.code.lock().unwrap() = 45009;
    assert_eq!(
        call(&app, "POST", &url, &token(owner), rotation.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let unchanged: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&installation],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(unchanged, stored2);
    *p.code.lock().unwrap() = 0;
    let agent2:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'WeCom another','local',$2,'private','workspace') RETURNING id",&[&workspace,&owner]).await.unwrap().get(0);
    let calls = *p.calls.lock().unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent2}"),
            &token(owner),
            rotation.clone()
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        *p.calls.lock().unwrap(),
        calls,
        "a refused install must not displace live subscriber"
    );
    let (status, listing) = call(
        &app,
        "GET",
        &format!("/api/workspaces/{workspace}/wecom/installations"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!listing.to_string().contains("synthetic-secret"));
    assert!(!listing.to_string().contains("server-data"));
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{workspace}/wecom/installations/{installation}"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let calls = *p.calls.lock().unwrap();
    assert_eq!(
        call(&app, "POST", &url, &token(owner), rotation).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        *p.calls.lock().unwrap(),
        calls,
        "reconnect needs worker fence; no probe"
    );
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_permission_loss_during_probe_rolls_back() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, _owner, admin, _) = fixture(&db).await;
    let (p, origin, server) = provider().await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (resume_tx, resume) = tokio::sync::oneshot::channel();
    *p.pause.lock().unwrap() = Some((entered_tx, resume));
    let app = app(
        pool.clone(),
        Arc::new(InstallRuntime {
            installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            events: Mutex::new(vec![]),
        }),
    );
    let url = format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}");
    let request = tokio::spawn({
        let app = app.clone();
        async move {
            call(
                &app,
                "POST",
                &url,
                &token(admin),
                json!({"bot_id":format!("synthetic-bot-{agent}"),"secret":"synthetic-secret"}),
            )
            .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&workspace, &admin],
    )
    .await
    .unwrap();
    resume_tx.send(()).unwrap();
    let (status, _) = request.await.unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='wecom'",&[&workspace,&agent]).await.unwrap().get::<_,i64>(0),0);
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_simultaneous_claim_only_probes_winner() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let agent2:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'WeCom race','local',$2,'private','workspace') RETURNING id",&[&workspace,&owner]).await.unwrap().get(0);
    let (p, origin, server) = provider().await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (resume_tx, resume) = tokio::sync::oneshot::channel();
    *p.pause.lock().unwrap() = Some((entered_tx, resume));
    let app = app(
        pool.clone(),
        Arc::new(InstallRuntime {
            installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            events: Mutex::new(vec![]),
        }),
    );
    let body = json!({"bot_id":format!("synthetic-bot-{agent}"),"secret":"synthetic-secret"});
    let a = tokio::spawn({
        let app = app.clone();
        let body = body.clone();
        async move {
            call(
                &app,
                "POST",
                &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}"),
                &token(owner),
                body,
            )
            .await
            .0
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered)
        .await
        .unwrap()
        .unwrap();
    let b = tokio::spawn({
        let app = app.clone();
        async move {
            call(
                &app,
                "POST",
                &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent2}"),
                &token(owner),
                body,
            )
            .await
            .0
        }
    });
    resume_tx.send(()).unwrap();
    assert_eq!(a.await.unwrap(), StatusCode::OK);
    assert_eq!(b.await.unwrap(), StatusCode::CONFLICT);
    assert_eq!(*p.calls.lock().unwrap(), 1);
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_rejects_reserved_bot_without_provider_contact() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    let (other, other_agent, other_owner, _, _) = fixture(&db).await;
    let bot = format!("synthetic-bot-{other_agent}");
    let reserved:Uuid=db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,status,config) VALUES($1,$2,'wecom',$3,'revoked',$4) RETURNING id",&[&other,&other_agent,&other_owner,&json!({"app_id":bot,"bot_id":bot,"secret_encrypted":"sealed-other-workspace"})]).await.unwrap().get(0);
    let (p, origin, server) = provider().await;
    let app = app(
        pool.clone(),
        Arc::new(InstallRuntime {
            installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
            events: Mutex::new(vec![]),
        }),
    );
    let (status, reason) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}"),
        &token(owner),
        json!({"bot_id":bot,"secret":"synthetic-secret"}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{reason:?}");
    assert_eq!(*p.calls.lock().unwrap(), 0);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM channel_installation WHERE id=$1",
            &[&reserved]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    server.abort();
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_wecom_db_failure_after_probe_leaves_no_row_or_event() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (workspace, agent, owner, _, _) = fixture(&db).await;
    // Trigger is disposable-DB only. Inject failure after proof to cover rollback.
    db.batch_execute("CREATE FUNCTION zbs_channels_sprint_wecom_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.channel_type='wecom' THEN RAISE EXCEPTION 'synthetic database failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbs_channels_sprint_wecom_fail BEFORE INSERT ON channel_installation FOR EACH ROW EXECUTE FUNCTION zbs_channels_sprint_wecom_reject()") .await.unwrap();
    let (p, origin, server) = provider().await;
    let runtime = Arc::new(InstallRuntime {
        installer: WecomInstaller::for_loopback_tests(&[73; 32], &origin).unwrap(),
        events: Mutex::new(vec![]),
    });
    let app = app(pool.clone(), runtime.clone());
    let (status, reason) = call(
        &app,
        "POST",
        &format!("/api/workspaces/{workspace}/wecom/install/byo?agent_id={agent}"),
        &token(owner),
        json!({"bot_id":format!("synthetic-bot-{agent}"),"secret":"synthetic-secret"}),
    )
    .await;
    db.batch_execute("DROP TRIGGER zbs_channels_sprint_wecom_fail ON channel_installation; DROP FUNCTION zbs_channels_sprint_wecom_reject()") .await.unwrap();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{reason:?}");
    assert!(!reason.to_string().contains("synthetic database failure"));
    assert_eq!(*p.calls.lock().unwrap(), 1);
    assert!(runtime.events.lock().unwrap().is_empty());
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='wecom'",&[&workspace,&agent]).await.unwrap().get::<_,i64>(0),0);
    server.abort();
}
