//! P1 channel install gates: the not-configured contract, the workspace URL
//! middleware ordering, Lark device-flow begin/status, redeem conflicts and
//! guest-owner ordering. No provider or cloud call is made anywhere.
use super::*;
use multica_server::api::channels_sprint_lark_install::{
    BeginInstallResult, InstallSession, LarkRegistrar, SessionLookupError,
};
use std::collections::HashMap;

struct GatesRuntime {
    configured: Vec<Channel>,
    registrar: Option<FakeRegistrar>,
}

#[async_trait]
impl ChannelRuntime for GatesRuntime {
    fn configured(&self, channel: Channel) -> bool {
        self.configured.contains(&channel)
    }
    fn publish(&self, _: ChannelEvent) {}
    async fn cancel_guest_tasks(&self, _: GuestCancellation) {}
    fn lark_registrar(&self) -> Option<&dyn LarkRegistrar> {
        self.registrar.as_ref().map(|r| r as &dyn LarkRegistrar)
    }
}

#[derive(Default)]
struct FakeRegistrar {
    sessions: Mutex<HashMap<String, InstallSession>>,
    fail: Mutex<Option<String>>,
}

#[async_trait]
impl LarkRegistrar for FakeRegistrar {
    async fn begin_install(
        &self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
        region: &str,
    ) -> Result<BeginInstallResult, String> {
        if let Some(reason) = self.fail.lock().unwrap().clone() {
            return Err(reason);
        }
        Ok(BeginInstallResult {
            session_id: "synthetic-session".into(),
            qr_code_url: format!("https://fake.invalid/qr?region={region}"),
            expires_in_seconds: 300,
            poll_interval_seconds: 2,
        })
    }
    async fn install_session(
        &self,
        _: Uuid,
        session_id: &str,
    ) -> Result<InstallSession, SessionLookupError> {
        self.sessions
            .lock()
            .unwrap()
            .get(session_id)
            .cloned()
            .ok_or(SessionLookupError::NotFound)
    }
}

fn app_with(pool: Arc<Pool>, runtime: Option<Arc<dyn ChannelRuntime>>) -> axum::Router {
    let state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec());
    let state = match runtime {
        Some(runtime) => state.with_channel_runtime(runtime),
        None => state,
    };
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    )
}

struct Fixture {
    workspace: Uuid,
    agent: Uuid,
    owner: Uuid,
    member: Uuid,
    stranger: Uuid,
}

async fn fixture(db: &deadpool_postgres::Object) -> Fixture {
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(db, "gate-owner", &suffix).await;
    let member = insert_user(db, "gate-member", &suffix).await;
    let stranger = insert_user(db, "gate-stranger", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Channel gates',$1) RETURNING id",
            &[&format!("channel-gates-{suffix}")],
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
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Gate agent','local',$2,'public_to','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'workspace',$2)",
        &[&agent, &workspace],
    )
    .await
    .unwrap();
    Fixture {
        workspace,
        agent,
        owner,
        member,
        stranger,
    }
}

async fn installation(
    db: &deadpool_postgres::Object,
    fixture: &Fixture,
    channel: &str,
    config: Value,
) -> Uuid {
    installation_on(db, fixture, fixture.agent, channel, config).await
}

async fn installation_on(
    db: &deadpool_postgres::Object,
    fixture: &Fixture,
    agent: Uuid,
    channel: &str,
    config: Value,
) -> Uuid {
    db.query_one(
        "INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,$3,$4,$5) RETURNING id",
        &[&fixture.workspace, &agent, &channel, &fixture.owner, &config],
    )
    .await
    .unwrap()
    .get(0)
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

async fn binding_token(
    db: &deadpool_postgres::Object,
    workspace: Uuid,
    installation: Uuid,
    channel: &str,
    external: &str,
) -> String {
    let raw = Uuid::new_v4().to_string();
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    db.execute("INSERT INTO channel_binding_token(token_hash,workspace_id,installation_id,channel_type,channel_user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '10 minutes')", &[&hash,&workspace,&installation,&channel,&external]).await.unwrap();
    raw
}

fn install_path(workspace: Uuid, channel: &str) -> String {
    let suffix = if matches!(channel, "lark") {
        "install/begin".to_string()
    } else if matches!(channel, "slack" | "wecom" | "dingtalk") {
        "install/byo".to_string()
    } else {
        "install".to_string()
    };
    format!("/api/workspaces/{workspace}/{channel}/{suffix}")
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_unconfigured_contract() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let app = app_with(pool, None);
    let owner = token(fx.owner);

    for (channel, path, install_error, redeem_error) in [
        (
            "lark",
            "lark",
            "lark install not configured",
            "lark integration not configured",
        ),
        (
            "slack",
            "slack",
            "slack integration not enabled",
            "slack integration not configured",
        ),
        (
            "wecom",
            "wecom",
            "wecom integration not enabled",
            "wecom integration not configured",
        ),
        (
            "dingtalk",
            "dingtalk",
            "dingtalk integration not enabled",
            "dingtalk integration not configured",
        ),
        (
            "telegram",
            "telegram",
            "telegram integration not enabled",
            "telegram integration not configured",
        ),
        (
            "zalo",
            "zalo",
            "zalo integration not enabled",
            "zalo integration not configured",
        ),
    ] {
        // Listing a disabled integration is still a 200 with configured:false.
        let list = call(
            &app,
            "GET",
            &format!("/api/workspaces/{}/{path}/installations", fx.workspace),
            &owner,
            Value::Null,
        )
        .await;
        assert_eq!(list.0, StatusCode::OK, "{channel}: {list:?}");
        assert_eq!(list.1["configured"], json!(false), "{channel}");
        assert_eq!(list.1["install_supported"], json!(false), "{channel}");
        assert_eq!(list.1["installations"], json!([]), "{channel}");

        // Install is a coded 403, never a 503 stub.
        let body = if path == "lark" {
            json!({})
        } else {
            json!({"bot_token":"synthetic","app_token":"synthetic"})
        };
        let disabled = call(
            &app,
            "POST",
            &format!("{}?agent_id={}", install_path(fx.workspace, path), fx.agent),
            &owner,
            body,
        )
        .await;
        assert_eq!(disabled.0, StatusCode::FORBIDDEN, "{channel}: {disabled:?}");
        assert_eq!(disabled.1["error"], json!(install_error), "{channel}");
        assert_eq!(
            disabled.1["code"],
            json!(format!("{channel}_not_configured")),
            "{channel}"
        );

        // Redeem on the channel-level route.
        let redeem = call(
            &app,
            "POST",
            &format!("/api/{path}/binding/redeem"),
            &owner,
            json!({"token":"synthetic-token"}),
        )
        .await;
        assert_eq!(redeem.0, StatusCode::FORBIDDEN, "{channel}: {redeem:?}");
        assert_eq!(redeem.1["error"], json!(redeem_error), "{channel}");
        assert_eq!(
            redeem.1["code"],
            json!(format!("{channel}_not_configured")),
            "{channel}"
        );

        // Revoke on the install install-config-driven route.
        let revoke = call(
            &app,
            "DELETE",
            &format!(
                "/api/workspaces/{}/{path}/installations/{}",
                fx.workspace,
                Uuid::new_v4()
            ),
            &owner,
            Value::Null,
        )
        .await;
        assert_eq!(revoke.0, StatusCode::FORBIDDEN, "{channel}: {revoke:?}");
        assert_eq!(
            revoke.1["code"],
            json!(format!("{channel}_not_configured")),
            "{channel}"
        );
    }

    let lark_status = call(
        &app,
        "GET",
        &format!(
            "/api/workspaces/{}/lark/install/synthetic-session/status",
            fx.workspace
        ),
        &owner,
        Value::Null,
    )
    .await;
    assert_eq!(lark_status.0, StatusCode::FORBIDDEN, "{lark_status:?}");
    assert_eq!(lark_status.1["code"], json!("lark_not_configured"));

    for channel in ["telegram", "zalo"] {
        let guest = call(
            &app,
            "GET",
            &format!(
                "/api/workspaces/{}/{channel}/installations/{}/guest-invites",
                fx.workspace,
                Uuid::new_v4()
            ),
            &owner,
            Value::Null,
        )
        .await;
        assert_eq!(guest.0, StatusCode::FORBIDDEN, "{channel}: {guest:?}");
        assert_eq!(
            guest.1["error"],
            json!(format!("{channel} guest invitations are not configured")),
            "{channel}"
        );
        assert_eq!(guest.1["code"], json!(format!("{channel}_not_configured")));
    }
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_workspace_middleware_order() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let app = app_with(pool, None);

    // Invalid workspace id fails before any channel gate runs.
    let invalid = call(
        &app,
        "GET",
        "/api/workspaces/not-a-uuid/slack/installations",
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{invalid:?}");
    assert_eq!(invalid.1["error"], json!("invalid workspace_id"));

    // A non-member sees the generic workspace 404, not a channel message.
    let outsider = call(
        &app,
        "GET",
        &format!("/api/workspaces/{}/slack/installations", fx.workspace),
        &token(fx.stranger),
        Value::Null,
    )
    .await;
    assert_eq!(outsider.0, StatusCode::NOT_FOUND, "{outsider:?}");
    assert_eq!(outsider.1["error"], json!("workspace not found"));

    // Admin-only channel routes reject a plain member before the disabled gate.
    for channel in ["slack", "wecom", "telegram", "zalo"] {
        let denied = call(
            &app,
            "POST",
            &format!(
                "{}?agent_id={}",
                install_path(fx.workspace, channel),
                fx.agent
            ),
            &token(fx.member),
            json!({"bot_token":"synthetic","app_token":"synthetic"}),
        )
        .await;
        assert_eq!(denied.0, StatusCode::FORBIDDEN, "{channel}: {denied:?}");
        assert_eq!(
            denied.1["error"],
            json!("insufficient permissions"),
            "{channel}"
        );
    }

    // Lark begin is member-level in Go's router: a member reaches the coded 403.
    let lark = call(
        &app,
        "POST",
        &format!(
            "/api/workspaces/{}/lark/install/begin?agent_id={}",
            fx.workspace, fx.agent
        ),
        &token(fx.member),
        json!({}),
    )
    .await;
    assert_eq!(lark.0, StatusCode::FORBIDDEN, "{lark:?}");
    assert_eq!(lark.1["code"], json!("lark_not_configured"));
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_lark_begin_and_status() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let app = app_with(
        pool,
        Some(Arc::new(GatesRuntime {
            configured: vec![Channel::Lark],
            registrar: Some(FakeRegistrar::default()),
        })),
    );
    let root = format!("/api/workspaces/{}/lark", fx.workspace);
    let owner = token(fx.owner);

    let missing = call(
        &app,
        "POST",
        &format!("{root}/install/begin"),
        &owner,
        json!({}),
    )
    .await;
    assert_eq!(missing.0, StatusCode::BAD_REQUEST, "{missing:?}");
    assert_eq!(missing.1["error"], json!("agent_id is required"));

    let invalid_agent = call(
        &app,
        "POST",
        &format!("{root}/install/begin?agent_id=not-a-uuid"),
        &owner,
        json!({}),
    )
    .await;
    assert_eq!(invalid_agent.0, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_agent.1["error"], json!("invalid agent_id"));

    let bad_region = call(
        &app,
        "POST",
        &format!("{root}/install/begin?agent_id={}&region=aws", fx.agent),
        &owner,
        json!({}),
    )
    .await;
    assert_eq!(bad_region.0, StatusCode::BAD_REQUEST, "{bad_region:?}");
    assert_eq!(
        bad_region.1["error"],
        json!("region must be 'feishu' or 'lark'")
    );

    let unknown_agent = call(
        &app,
        "POST",
        &format!("{root}/install/begin?agent_id={}", Uuid::new_v4()),
        &owner,
        json!({}),
    )
    .await;
    assert_eq!(unknown_agent.0, StatusCode::NOT_FOUND, "{unknown_agent:?}");
    assert_eq!(
        unknown_agent.1["error"],
        json!("agent not found in this workspace")
    );

    // A member who is neither the agent owner nor an admin is refused.
    let not_owner = call(
        &app,
        "POST",
        &format!("{root}/install/begin?agent_id={}", fx.agent),
        &token(fx.member),
        json!({}),
    )
    .await;
    assert_eq!(not_owner.0, StatusCode::FORBIDDEN, "{not_owner:?}");
    assert_eq!(
        not_owner.1["error"],
        json!("only the agent owner can manage this agent")
    );

    let started = call(
        &app,
        "POST",
        &format!("{root}/install/begin?agent_id={}&region=lark", fx.agent),
        &owner,
        json!({}),
    )
    .await;
    assert_eq!(started.0, StatusCode::OK, "{started:?}");
    assert_eq!(started.1["session_id"], json!("synthetic-session"));
    assert_eq!(
        started.1["qr_code_url"],
        json!("https://fake.invalid/qr?region=lark")
    );
    assert_eq!(started.1["expires_in_seconds"], json!(300));
    assert_eq!(started.1["poll_interval_seconds"], json!(2));

    // Status is scoped to the initiator (or a workspace admin).
    let unknown_session = call(
        &app,
        "GET",
        &format!("{root}/install/synthetic-session/status"),
        &owner,
        Value::Null,
    )
    .await;
    assert_eq!(
        unknown_session.0,
        StatusCode::NOT_FOUND,
        "{unknown_session:?}"
    );
    assert_eq!(
        unknown_session.1["error"],
        json!("install session not found")
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_lark_begin_failure_is_502() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let registrar = FakeRegistrar::default();
    *registrar.fail.lock().unwrap() = Some("synthetic upstream unavailable".into());
    let app = app_with(
        pool,
        Some(Arc::new(GatesRuntime {
            configured: vec![Channel::Lark],
            registrar: Some(registrar),
        })),
    );
    let failed = call(
        &app,
        "POST",
        &format!(
            "/api/workspaces/{}/lark/install/begin?agent_id={}",
            fx.workspace, fx.agent
        ),
        &token(fx.owner),
        json!({}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::BAD_GATEWAY, "{failed:?}");
    assert_eq!(
        failed.1["error"],
        json!("failed to start install: synthetic upstream unavailable")
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_status_projection() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let installation = installation(&db, &fx, "feishu", json!({"app_id":"cli_synthetic"})).await;
    let registrar = FakeRegistrar::default();
    registrar.sessions.lock().unwrap().insert(
        "done-session".into(),
        InstallSession {
            status: "success".into(),
            installation_id: Some(installation),
            error_reason: String::new(),
            error_message: String::new(),
            initiator_id: fx.owner,
        },
    );
    registrar.sessions.lock().unwrap().insert(
        "failed-session".into(),
        InstallSession {
            status: "error".into(),
            installation_id: None,
            error_reason: "tenant_key_mismatch".into(),
            error_message: "synthetic mismatch".into(),
            initiator_id: fx.owner,
        },
    );
    let app = app_with(
        pool,
        Some(Arc::new(GatesRuntime {
            configured: vec![Channel::Lark],
            registrar: Some(registrar),
        })),
    );
    let root = format!("/api/workspaces/{}/lark/install", fx.workspace);

    let done = call(
        &app,
        "GET",
        &format!("{root}/done-session/status"),
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(done.0, StatusCode::OK, "{done:?}");
    assert_eq!(done.1["status"], json!("success"));
    assert_eq!(done.1["installation_id"], json!(installation));
    assert!(done.1.get("error_reason").is_none(), "{done:?}");

    let failed = call(
        &app,
        "GET",
        &format!("{root}/failed-session/status"),
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(failed.0, StatusCode::OK, "{failed:?}");
    assert_eq!(failed.1["status"], json!("error"));
    assert!(failed.1.get("installation_id").is_none(), "{failed:?}");
    assert_eq!(failed.1["error_reason"], json!("tenant_key_mismatch"));
    assert_eq!(failed.1["error_message"], json!("synthetic mismatch"));

    // Someone else's session id leaks exactly as much as an unknown one.
    let stranger = call(
        &app,
        "GET",
        &format!("{root}/done-session/status"),
        &token(fx.stranger),
        Value::Null,
    )
    .await;
    assert_eq!(stranger.0, StatusCode::NOT_FOUND, "{stranger:?}");
    assert_eq!(stranger.1["error"], json!("workspace not found"));

    let member = call(
        &app,
        "GET",
        &format!("{root}/done-session/status"),
        &token(fx.member),
        Value::Null,
    )
    .await;
    assert_eq!(member.0, StatusCode::NOT_FOUND, "{member:?}");

    // The workspace owner is an admin and may read the initiator's session.
    let admin = call(
        &app,
        "GET",
        &format!("{root}/done-session/status"),
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(admin.0, StatusCode::OK, "{admin:?}");
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_redeem_and_revoke_contracts() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let all = vec![
        Channel::Lark,
        Channel::Slack,
        Channel::Wecom,
        Channel::DingTalk,
        Channel::Telegram,
        Channel::Zalo,
    ];
    let app = app_with(
        pool.clone(),
        Some(Arc::new(GatesRuntime {
            configured: all.clone(),
            registrar: None,
        })),
    );
    let conflicts = [
        (
            Channel::Lark,
            "this Lark account is already bound to a different Multica user",
        ),
        (
            Channel::Slack,
            "this Slack account is already bound to a different Multica user",
        ),
        (
            Channel::Wecom,
            "this WeCom user is already bound to a different Multica user",
        ),
        (
            Channel::DingTalk,
            "this DingTalk account is already bound to a different Multica user",
        ),
        (
            Channel::Telegram,
            "this Telegram account is already bound to a different Multica user",
        ),
        (
            Channel::Zalo,
            "this Zalo account is already bound to a different Multica user",
        ),
    ];
    for (channel, conflict) in conflicts {
        let kind = channel.database_type();
        let inst = installation(&db, &fx, kind, json!({})).await;
        let external = format!("external-{}", Uuid::new_v4().simple());
        db.execute("INSERT INTO channel_user_binding(workspace_id,multica_user_id,installation_id,channel_type,channel_user_id) VALUES($1,$2,$3,$4,$5)", &[&fx.workspace,&fx.owner,&inst,&kind,&external]).await.unwrap();
        let raw = binding_token(&db, fx.workspace, inst, kind, &external).await;
        let redeem = call(
            &app,
            "POST",
            &format!("/api/{}/binding/redeem", channel.path()),
            &token(fx.member),
            json!({"token": raw}),
        )
        .await;
        assert_eq!(redeem.0, StatusCode::CONFLICT, "{kind}: {redeem:?}");
        assert_eq!(redeem.1["error"], json!(conflict), "{kind}");

        // Revoking an unknown installation names the channel.
        let missing = call(
            &app,
            "DELETE",
            &format!(
                "/api/workspaces/{}/{}/installations/{}",
                fx.workspace,
                channel.path(),
                Uuid::new_v4()
            ),
            &token(fx.owner),
            Value::Null,
        )
        .await;
        assert_eq!(missing.0, StatusCode::NOT_FOUND, "{kind}: {missing:?}");
        assert_eq!(
            missing.1["error"],
            json!(format!("{} installation not found", channel.path())),
            "{kind}"
        );
    }

    // A non-member's redemption is refused without consuming the token.
    let redeem_agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Gate redeem agent','local',$2) RETURNING id", &[&fx.workspace,&fx.owner]).await.unwrap().get(0);
    let inst = installation_on(&db, &fx, redeem_agent, "telegram", json!({})).await;
    let raw = binding_token(&db, fx.workspace, inst, "telegram", "outsider").await;
    let refused = call(
        &app,
        "POST",
        "/api/telegram/binding/redeem",
        &token(fx.stranger),
        json!({"token": raw.clone()}),
    )
    .await;
    assert_eq!(refused.0, StatusCode::FORBIDDEN, "{refused:?}");
    assert_eq!(
        refused.1["error"],
        json!("binding refused (are you a workspace member?)")
    );
    let consumed: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT consumed_at FROM channel_binding_token WHERE token_hash=$1",
            &[&hex::encode(Sha256::digest(raw.as_bytes()))],
        )
        .await
        .unwrap()
        .get(0);
    assert!(consumed.is_none(), "refused redemption must roll back");
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channels_sprint_install_gates_guest_owner_order() {
    let pool = pool();
    let db = pool.get().await.unwrap();
    let fx = fixture(&db).await;
    let app = app_with(
        pool,
        Some(Arc::new(GatesRuntime {
            configured: vec![Channel::Telegram, Channel::Zalo],
            registrar: None,
        })),
    );
    let inst = installation(&db, &fx, "telegram", json!({"bot_username":"GateBot_101"})).await;
    let invites = format!(
        "/api/workspaces/{}/telegram/installations/{inst}/guest-invites",
        fx.workspace
    );

    let around = call(
        &app,
        "GET",
        &format!(
            "/api/workspaces/{}/telegram/installations/{}/guest-invites",
            fx.workspace,
            Uuid::new_v4()
        ),
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(around.0, StatusCode::NOT_FOUND, "{around:?}");
    assert_eq!(around.1["error"], json!("telegram installation not found"));

    let zalo_only = call(
        &app,
        "GET",
        &format!(
            "/api/workspaces/{}/zalo/installations/{inst}/guest-invites",
            fx.workspace
        ),
        &token(fx.owner),
        Value::Null,
    )
    .await;
    assert_eq!(zalo_only.0, StatusCode::NOT_FOUND, "{zalo_only:?}");
    assert_eq!(zalo_only.1["error"], json!("zalo installation not found"));

    // The installation belongs to the owner's agent, so a plain member is not
    // the agent owner.
    let not_owner = call(&app, "GET", &invites, &token(fx.member), Value::Null).await;
    assert_eq!(not_owner.0, StatusCode::FORBIDDEN, "{not_owner:?}");
    assert_eq!(not_owner.1["error"], json!("agent owner required"));

    let owner = call(&app, "GET", &invites, &token(fx.owner), Value::Null).await;
    assert_eq!(owner.0, StatusCode::OK, "{owner:?}");
    assert_eq!(owner.1["invites"], json!([]));
}
