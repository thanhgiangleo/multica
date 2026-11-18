use super::*;
use axum::{
    Json, Router,
    extract::State,
    routing::{get, post},
};
use multica_server::api::{
    channels_sprint_lark::{DeviceCompletion, LarkInstaller, Region},
    channels_sprint_telegram::CredentialBox,
};

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
    pause: Pause,
}
async fn auth(State(provider): State<Provider>, Json(body): Json<Value>) -> Json<Value> {
    assert!(body["app_id"].as_str().unwrap().starts_with("cli_"));
    assert!(
        body["app_secret"]
            .as_str()
            .unwrap()
            .starts_with("synthetic_")
    );
    provider.calls.lock().unwrap().push("auth".into());
    Json(json!({"code":0,"tenant_access_token":"synthetic_tenant_token","expire":7200}))
}
async fn bot(State(provider): State<Provider>, headers: axum::http::HeaderMap) -> Json<Value> {
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Bearer synthetic_tenant_token"
    );
    provider.calls.lock().unwrap().push("bot".into());
    let pause = provider.pause.lock().unwrap().take();
    if let Some((entered, release)) = pause {
        let _ = entered.send(());
        let _ = release.await;
    }
    Json(json!({"code":0,"bot":{"open_id":"ou_synthetic_bot"}}))
}
async fn provider() -> (Provider, String, tokio::task::JoinHandle<()>) {
    let p = Provider {
        calls: Arc::new(Mutex::new(vec![])),
        pause: Arc::new(Mutex::new(None)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route(
            "/open-apis/auth/v3/tenant_access_token/internal",
            post(auth),
        )
        .route("/open-apis/bot/v3/info", get(bot))
        .with_state(p.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (p, origin, server)
}
fn pool() -> Arc<Pool> {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi73"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1"))
    );
    Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    )
}
async fn fixture(db: &deadpool_postgres::Object) -> (Uuid, Uuid, Uuid, Uuid, Uuid) {
    let suffix = Uuid::new_v4().simple().to_string();
    let admin = insert_user(db, "lark-admin", &suffix).await;
    let owner = insert_user(db, "lark-owner", &suffix).await;
    let member = insert_user(db, "lark-member", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Lark synthetic',$1) RETURNING id",
            &[&format!("lark-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [(admin, "owner"), (owner, "member"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &user, &role],
        )
        .await
        .unwrap();
    }
    let agent:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Lark synthetic','local',$2,'private','workspace') RETURNING id",&[&workspace,&owner]).await.unwrap().get(0);
    (workspace, agent, admin, owner, member)
}
fn completion(
    workspace: Uuid,
    agent: Uuid,
    user: Uuid,
    app: &str,
    region: Region,
) -> DeviceCompletion<'_> {
    DeviceCompletion {
        workspace_id: workspace,
        agent_id: agent,
        initiator_id: user,
        app_id: app,
        app_secret: "synthetic_app_secret",
        installer_open_id: "ou_synthetic_installer",
        region,
    }
}
fn state(pool: Arc<Pool>, runtime: Arc<FakeRuntime>) -> ApiState {
    ApiState::new(pool, SECRET.as_bytes().to_vec()).with_channel_runtime(runtime)
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_lark_device_completion_binding_rotation_and_revoke() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (ws, agent, admin, owner, member) = fixture(&db).await;
    let (provider, origin, server) = provider().await;
    let installer = LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
    let rt = Arc::new(FakeRuntime::default());
    let state = state(pool.clone(), rt.clone());
    let app_id = format!("cli_{}", Uuid::new_v4().simple());
    let disabled = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec());
    assert_eq!(
        installer
            .complete_device_install(
                &disabled,
                completion(ws, agent, owner, &app_id, Region::Lark)
            )
            .await
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        installer
            .complete_device_install(&state, completion(ws, agent, member, &app_id, Region::Lark))
            .await
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    let (foreign, foreign_agent, _, foreign_owner, _) = fixture(&db).await;
    assert_eq!(
        installer
            .complete_device_install(
                &state,
                completion(ws, agent, foreign_owner, &app_id, Region::Lark)
            )
            .await
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        installer
            .complete_device_install(
                &state,
                completion(ws, foreign_agent, admin, &app_id, Region::Lark)
            )
            .await
            .unwrap_err()
            .status,
        StatusCode::NOT_FOUND
    );
    let mut malformed = completion(ws, agent, owner, &app_id, Region::Lark);
    malformed.app_secret = "";
    assert_eq!(
        installer
            .complete_device_install(&state, malformed)
            .await
            .unwrap_err()
            .status,
        StatusCode::BAD_REQUEST
    );
    assert!(provider.calls.lock().unwrap().is_empty());
    let result = installer
        .complete_device_install(&state, completion(ws, agent, owner, &app_id, Region::Lark))
        .await
        .unwrap();
    assert_eq!(result["region"], "lark");
    assert_eq!(result["bot_open_id"], "ou_synthetic_bot");
    assert!(!result.to_string().contains("secret"));
    let inst = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let config: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        CredentialBox::new(&[73; 32])
            .unwrap()
            .open(config["app_secret_encrypted"].as_str().unwrap())
            .unwrap(),
        b"synthetic_app_secret"
    );
    let binding:Uuid=db.query_one("SELECT multica_user_id FROM channel_user_binding WHERE installation_id=$1 AND channel_user_id='ou_synthetic_installer' AND channel_type='feishu'",&[&inst]).await.unwrap().get(0);
    assert_eq!(binding, owner);
    db.execute("UPDATE channel_installation SET config=config || '{\"server_marker\":\"preserved\"}' WHERE id=$1",&[&inst]).await.unwrap();
    let mut rotated = completion(ws, agent, owner, &app_id, Region::Lark);
    rotated.app_secret = "synthetic_rotated_secret";
    assert_eq!(
        installer
            .complete_device_install(&state, rotated)
            .await
            .unwrap()["id"],
        result["id"]
    );
    let updated: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(updated["server_marker"], "preserved");
    assert_ne!(
        updated["app_secret_encrypted"],
        config["app_secret_encrypted"]
    );
    assert_eq!(
        CredentialBox::new(&[73; 32])
            .unwrap()
            .open(updated["app_secret_encrypted"].as_str().unwrap())
            .unwrap(),
        b"synthetic_rotated_secret"
    );
    assert_eq!(
        *provider.calls.lock().unwrap(),
        vec!["auth", "bot", "auth", "bot"]
    );
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    assert_eq!(
        installer
            .complete_device_install(
                &state,
                completion(foreign, foreign_agent, foreign_owner, &app_id, Region::Lark)
            )
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    assert_eq!(
        installer
            .complete_device_install(
                &state,
                completion(ws, agent, owner, &app_id, Region::Feishu)
            )
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state.clone(),
    );
    let list = call(
        &app,
        "GET",
        &format!("/api/workspaces/{ws}/lark/installations"),
        &token(member),
        Value::Null,
    )
    .await;
    assert_eq!(list.0, StatusCode::OK);
    assert_eq!(list.1["install_supported"], false);
    assert!(!list.1.to_string().contains("encrypted"));
    // No invented public BYO route; device-code begin/status remain unwired.
    assert_ne!(
        call(
            &app,
            "POST",
            &format!("/api/workspaces/{ws}/lark/install/byo"),
            &token(owner),
            json!({})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{ws}/lark/installations/{inst}"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        installer
            .complete_device_install(&state, completion(ws, agent, owner, &app_id, Region::Lark))
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    assert_eq!(rt.events.lock().unwrap().len(), 3);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_lark_completion_conflict_concurrency_and_atomic_rollback() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (ws, agent, admin, owner, member) = fixture(&db).await;
    let (_, origin, server) = provider().await;
    let installer = LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap();
    let rt = Arc::new(FakeRuntime::default());
    let state = state(pool.clone(), rt.clone());
    let app_id = format!("cli_{}", Uuid::new_v4().simple());
    let second:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Lark second','local',$2,'private','workspace') RETURNING id",&[&ws,&owner]).await.unwrap().get(0);
    let (a, b) = tokio::join!(
        installer.complete_device_install(
            &state,
            completion(ws, agent, owner, &app_id, Region::Feishu)
        ),
        installer.complete_device_install(
            &state,
            completion(ws, second, owner, &app_id, Region::Feishu)
        )
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let (result, winner) = match (a, b) {
        (Ok(v), Err(e)) => {
            assert_eq!(e.status, StatusCode::CONFLICT);
            (v, agent)
        }
        (Err(e), Ok(v)) => {
            assert_eq!(e.status, StatusCode::CONFLICT);
            (v, second)
        }
        _ => panic!("one routing slot winner"),
    };
    let inst = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let before: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "UPDATE channel_user_binding SET multica_user_id=$2 WHERE installation_id=$1",
        &[&inst, &member],
    )
    .await
    .unwrap();
    let mut p = completion(ws, winner, owner, &app_id, Region::Feishu);
    p.app_secret = "synthetic_cannot_commit";
    assert_eq!(
        installer
            .complete_device_install(&state, p)
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
    let after: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    // Force failure after installation insert; transaction must leave neither row nor event.
    db.batch_execute("CREATE FUNCTION channels_sprint_lark_binding_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.channel_type='feishu' AND NEW.channel_user_id='ou_force_rollback' THEN RAISE EXCEPTION 'synthetic binding failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER channels_sprint_lark_binding_fail BEFORE INSERT ON channel_user_binding FOR EACH ROW EXECUTE FUNCTION channels_sprint_lark_binding_fail()").await.unwrap();
    let fresh = if winner == agent { second } else { agent };
    let new_app = format!("cli_{}", Uuid::new_v4().simple());
    let mut p = completion(ws, fresh, admin, &new_app, Region::Feishu);
    p.installer_open_id = "ou_force_rollback";
    assert_eq!(
        installer
            .complete_device_install(&state, p)
            .await
            .unwrap_err()
            .status,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(db.query_one("SELECT count(*) FROM channel_installation WHERE channel_type='feishu' AND config->>'app_id'=$1",&[&new_app]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute("DROP TRIGGER channels_sprint_lark_binding_fail ON channel_user_binding; DROP FUNCTION channels_sprint_lark_binding_fail()").await.unwrap();
    assert_eq!(rt.events.lock().unwrap().len(), 1);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_lark_rechecks_ownership_and_revoke_after_provider() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (ws, agent, admin, owner, member) = fixture(&db).await;
    let (provider, origin, server) = provider().await;
    let installer = Arc::new(LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap());
    let rt = Arc::new(FakeRuntime::default());
    let state = state(pool.clone(), rt.clone());
    let app_id = format!("cli_{}", Uuid::new_v4().simple());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let worker = {
        let installer = installer.clone();
        let state = state.clone();
        let app_id = app_id.clone();
        tokio::spawn(async move {
            installer
                .complete_device_install(
                    &state,
                    completion(ws, agent, owner, &app_id, Region::Lark),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &member],
    )
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        worker.await.unwrap().unwrap_err().status,
        StatusCode::FORBIDDEN
    );
    assert!(rt.events.lock().unwrap().is_empty());
    installer
        .complete_device_install(&state, completion(ws, agent, admin, &app_id, Region::Lark))
        .await
        .unwrap();
    let inst:Uuid=db.query_one("SELECT id FROM channel_installation WHERE workspace_id=$1 AND agent_id=$2 AND channel_type='feishu'",&[&ws,&agent]).await.unwrap().get(0);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let worker = {
        let installer = installer.clone();
        let state = state.clone();
        let app_id = app_id.clone();
        tokio::spawn(async move {
            installer
                .complete_device_install(
                    &state,
                    completion(ws, agent, admin, &app_id, Region::Lark),
                )
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state.clone(),
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/workspaces/{ws}/lark/installations/{inst}"),
            &token(admin),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    release_tx.send(()).unwrap();
    assert_eq!(
        worker.await.unwrap().unwrap_err().status,
        StatusCode::CONFLICT
    );
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_lark_stale_completion_cannot_overwrite_rotation() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let (ws, agent, _, owner, _) = fixture(&db).await;
    let (provider, origin, server) = provider().await;
    let installer = Arc::new(LarkInstaller::for_loopback_tests(&[73; 32], &origin).unwrap());
    let rt = Arc::new(FakeRuntime::default());
    let state = state(pool.clone(), rt.clone());
    let app_id = format!("cli_{}", Uuid::new_v4().simple());
    let result = installer
        .complete_device_install(&state, completion(ws, agent, owner, &app_id, Region::Lark))
        .await
        .unwrap();
    let inst = Uuid::parse_str(result["id"].as_str().unwrap()).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *provider.pause.lock().unwrap() = Some((entered_tx, release_rx));
    let worker = {
        let installer = installer.clone();
        let state = state.clone();
        let app_id = app_id.clone();
        tokio::spawn(async move {
            let mut p = completion(ws, agent, owner, &app_id, Region::Lark);
            p.app_secret = "synthetic_stale_secret";
            installer.complete_device_install(&state, p).await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    let mut newer = completion(ws, agent, owner, &app_id, Region::Lark);
    newer.app_secret = "synthetic_newer_secret";
    installer
        .complete_device_install(&state, newer)
        .await
        .unwrap();
    let before: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    release_tx.send(()).unwrap();
    assert_eq!(
        worker.await.unwrap().unwrap_err().status,
        StatusCode::CONFLICT
    );
    let after: Value = db
        .query_one(
            "SELECT config FROM channel_installation WHERE id=$1",
            &[&inst],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(after, before);
    assert_eq!(
        CredentialBox::new(&[73; 32])
            .unwrap()
            .open(after["app_secret_encrypted"].as_str().unwrap())
            .unwrap(),
        b"synthetic_newer_secret"
    );
    assert_eq!(rt.events.lock().unwrap().len(), 2);
    server.abort();
    let _ = server.await;
}
