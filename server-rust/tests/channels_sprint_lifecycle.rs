//! Disposable-DB channel lifecycle, guest fencing, and secret projection.
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        channels_sprint::{Channel, ChannelEvent, ChannelRuntime, GuestCancellation},
        channels_sprint_guest::{guest_grant_active, lock_guest_grant},
        channels_sprint_notify::{Notification, fanout},
        channels_sprint_notify_targets::{
            DirectRecipient, ParsedMention, notify_direct, notify_mentions,
        },
        channels_sprint_pair::{GuestPairError, VerifiedPrivateSender, redeem_guest},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "channels_sprint/binding_mint.rs"]
mod binding_mint;
#[path = "channels_sprint/dingtalk_install.rs"]
mod dingtalk_install;
#[path = "channels_sprint/generation.rs"]
mod generation;
#[path = "channels_sprint/groups.rs"]
mod groups;
#[path = "channels_sprint/ingress.rs"]
mod ingress;
#[path = "channels_sprint/install_gates.rs"]
mod install_gates;
#[path = "channels_sprint/lark_install.rs"]
mod lark_install;
#[path = "channels_sprint/notify_event.rs"]
mod notify_event;
#[path = "channels_sprint/notify_targets.rs"]
mod notify_targets;
#[path = "channels_sprint/slack_install.rs"]
mod slack_install;
#[path = "channels_sprint/stream_epoch.rs"]
mod stream_epoch;
#[path = "channels_sprint/task_reaction.rs"]
mod task_reaction;
#[path = "channels_sprint/telegram_install.rs"]
mod telegram_install;
#[path = "channels_sprint/telegram_poll.rs"]
mod telegram_poll;
#[path = "channels_sprint/wecom_install.rs"]
mod wecom_install;
#[path = "channels_sprint/zalo_install.rs"]
mod zalo_install;

const SECRET: &str = "disposable-zbsi73-test-jwt";
#[derive(Default)]
struct FakeRuntime {
    cancels: Mutex<Vec<GuestCancellation>>,
    events: Mutex<Vec<ChannelEvent>>,
}
#[async_trait]
impl ChannelRuntime for FakeRuntime {
    fn configured(&self, _: Channel) -> bool {
        true
    }
    fn publish(&self, event: ChannelEvent) {
        self.events.lock().unwrap().push(event);
    }
    async fn cancel_guest_tasks(&self, cancel: GuestCancellation) {
        self.cancels.lock().unwrap().push(cancel);
    }
}
fn token(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user.to_string(),"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    jwt: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
async fn insert_user(db: &deadpool_postgres::Object, label: &str, suffix: &str) -> Uuid {
    db.query_one(
        "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
        &[&label, &format!("{label}-{suffix}@invalid.example")],
    )
    .await
    .unwrap()
    .get(0)
}
async fn binding_token(
    db: &deadpool_postgres::Object,
    workspace: Uuid,
    installation: Uuid,
    channel: &str,
    ext: &str,
) -> String {
    let raw = Uuid::new_v4().to_string();
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    db.execute("INSERT INTO channel_binding_token(token_hash,workspace_id,installation_id,channel_type,channel_user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '10 minutes')", &[&hash,&workspace,&installation,&channel,&ext]).await.unwrap();
    raw
}
#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn channel_binding_guest_revoke_and_privacy() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "owner", &suffix).await;
    let member = insert_user(&db, "member", &suffix).await;
    let stranger = insert_user(&db, "stranger", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Channels',$1) RETURNING id",
            &[&format!("channels-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Foreign',$1) RETURNING id",
            &[&format!("foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (u, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&workspace, &u, &role],
        )
        .await
        .unwrap();
    }
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,'Guest agent','local',$2,'public_to','workspace') RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'workspace',$2)", &[&agent,&workspace]).await.unwrap();
    let installation:Uuid=db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config) VALUES($1,$2,'telegram',$3,$4) RETURNING id", &[&workspace,&agent,&owner,&json!({"bot_id":"123","bot_username":"FakeBot_73","bot_token_encrypted":"encrypted:test-only","ws_lease_token":"private"})]).await.unwrap().get(0);
    let fake = Arc::new(FakeRuntime::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()).with_channel_runtime(fake.clone()),
    );
    let root = format!("/api/workspaces/{workspace}/telegram");
    for channel in [
        Channel::Lark,
        Channel::Slack,
        Channel::Wecom,
        Channel::DingTalk,
        Channel::Zalo,
    ] {
        let kind = channel.database_type();
        let inst: Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id) VALUES($1,$2,$3,$4) RETURNING id", &[&workspace,&agent,&kind,&owner]).await.unwrap().get(0);
        let raw = binding_token(&db, workspace, inst, kind, "verified-external-member").await;
        let path = format!("/api/{}/binding/redeem", channel.path());
        let result = call(&app, "POST", &path, &token(member), json!({"token":raw})).await;
        assert_eq!(result.0, StatusCode::OK, "channel {kind}: {result:?}");
        assert_eq!(result.1["workspace_id"], json!(workspace));
        assert_eq!(
            call(&app, "POST", &path, &token(member), json!({"token":raw}))
                .await
                .0,
            StatusCode::GONE
        );
    }
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{root}/installations"),
            "invalid-session",
            Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let list = call(
        &app,
        "GET",
        &format!("{root}/installations"),
        &token(member),
        Value::Null,
    )
    .await;
    assert_eq!(list.0, StatusCode::OK, "{list:?}");
    let public = list.1.to_string();
    assert!(
        !public.contains("bot_token_encrypted")
            && !public.contains("encrypted:test-only")
            && !public.contains("ws_lease_token")
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/workspaces/{foreign}/telegram/installations"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        // Go's workspace URL middleware answers a non-member before the handler.
        StatusCode::NOT_FOUND
    );
    let outsider = binding_token(&db, workspace, installation, "telegram", "sender-outsider").await;
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/telegram/binding/redeem",
            &token(stranger),
            json!({"token":outsider})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let consumed: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT consumed_at FROM channel_binding_token WHERE token_hash=$1",
            &[&hex::encode(Sha256::digest(outsider.as_bytes()))],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        consumed.is_none(),
        "denied redemption must roll back token consumption"
    );
    let shared = binding_token(&db, workspace, installation, "telegram", "sender-shared").await;
    let member_jwt = token(member);
    let (a, b) = tokio::join!(
        call(
            &app,
            "POST",
            "/api/telegram/binding/redeem",
            &member_jwt,
            json!({"token":shared})
        ),
        call(
            &app,
            "POST",
            "/api/telegram/binding/redeem",
            &member_jwt,
            json!({"token":shared})
        )
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
        call(
            &app,
            "POST",
            "/api/zalo/binding/redeem",
            &token(member),
            json!({"token":outsider})
        )
        .await
        .0,
        StatusCode::GONE
    );
    let stolen = binding_token(&db, workspace, installation, "telegram", "sender-shared").await;
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/telegram/binding/redeem",
            &token(owner),
            json!({"token":stolen})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let still_free: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one(
            "SELECT consumed_at FROM channel_binding_token WHERE token_hash=$1",
            &[&hex::encode(Sha256::digest(stolen.as_bytes()))],
        )
        .await
        .unwrap()
        .get(0);
    assert!(still_free.is_none());
    let invites = format!("{root}/installations/{installation}/guest-invites");
    assert_eq!(
        call(
            &app,
            "POST",
            &invites,
            &token(member),
            json!({"alias":"Guest"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, invitation) = call(
        &app,
        "POST",
        &invites,
        &token(owner),
        json!({"alias":"Guest"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{invitation:?}");
    let code = invitation["code"].as_str().unwrap().to_string();
    let all_invites = call(&app, "GET", &invites, &token(owner), Value::Null).await;
    assert!(!all_invites.1.to_string().contains(&code));
    let (hash, persisted): (String, i64) = {
        let row=db.query_one("SELECT code_hash,COUNT(*) OVER() FROM zbs_telegram_guest_invite WHERE installation_id=$1", &[&installation]).await.unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(persisted, 1);
    assert_eq!(hash, hex::encode(Sha256::digest(code.as_bytes())));
    let sender = VerifiedPrivateSender {
        channel: Channel::Telegram,
        installation,
        external_user_id: "guest-external-1".into(),
        display_name: "Fake guest".into(),
    };
    let first = redeem_guest(&pool, &sender, &code).await.unwrap();
    assert_eq!(first.workspace, workspace);
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{root}/installations"),
            &token(first.shadow_user),
            Value::Null
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let no_membership: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM member WHERE user_id=$1",
            &[&first.shadow_user],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        no_membership, 0,
        "guest shadow must never receive workspace membership"
    );
    assert_eq!(
        redeem_guest(&pool, &sender, &code).await,
        Err(GuestPairError::Invalid)
    );
    assert!(
        guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            first.shadow_user,
            first.grant_generation
        )
        .await
        .unwrap()
    );
    let guest_list = call(
        &app,
        "GET",
        &format!("{root}/installations/{installation}/guests"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(guest_list.0, StatusCode::OK);
    assert!(!guest_list.1.to_string().contains(&code));
    let revoke_guest = format!(
        "{root}/installations/{installation}/guests/{}",
        first.shadow_user
    );
    assert_eq!(
        call(&app, "DELETE", &revoke_guest, &token(member), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut fence_db = pool.get().await.unwrap();
    let fence_tx = fence_db.transaction().await.unwrap();
    assert!(
        lock_guest_grant(
            &fence_tx,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            first.shadow_user,
            first.grant_generation
        )
        .await
        .unwrap()
    );
    let owner_jwt = token(owner);
    let mut pending_revoke = Box::pin(call(&app, "DELETE", &revoke_guest, &owner_jwt, Value::Null));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut pending_revoke)
            .await
            .is_err(),
        "revocation must wait for admitted transaction"
    );
    fence_tx.commit().await.unwrap();
    assert_eq!(pending_revoke.await.0, StatusCode::NO_CONTENT);
    assert_eq!(
        call(&app, "DELETE", &revoke_guest, &owner_jwt, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(fake.cancels.lock().unwrap().len(), 1);
    assert!(
        !guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            first.shadow_user,
            first.grant_generation
        )
        .await
        .unwrap()
    );
    let second_code = call(
        &app,
        "POST",
        &invites,
        &token(owner),
        json!({"alias":"Renewed"}),
    )
    .await
    .1["code"]
        .as_str()
        .unwrap()
        .to_string();
    let second = redeem_guest(&pool, &sender, &second_code).await.unwrap();
    assert_eq!(second.shadow_user, first.shadow_user);
    assert_eq!(second.grant_generation, first.grant_generation + 1);
    assert!(
        !guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            first.shadow_user,
            first.grant_generation
        )
        .await
        .unwrap()
    );
    assert!(
        guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            second.shadow_user,
            second.grant_generation
        )
        .await
        .unwrap()
    );
    let pending = binding_token(&db, workspace, installation, "telegram", "pending-revoke").await;
    // A late real-DB failure must roll back installation, token, and guest writes.
    db.batch_execute("CREATE FUNCTION channels_sprint_forced_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic revoke rollback'; END $$; CREATE TRIGGER channels_sprint_forced_abort BEFORE UPDATE OF revoked_at ON zbs_telegram_guest_grant FOR EACH ROW EXECUTE FUNCTION channels_sprint_forced_abort();").await.unwrap();
    let failed = call(
        &app,
        "DELETE",
        &format!("{root}/installations/{installation}"),
        &token(owner),
        Value::Null,
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert_eq!(
        db.query_one(
            "SELECT status FROM channel_installation WHERE id=$1",
            &[&installation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "active"
    );
    assert_eq!(db.query_one("SELECT COUNT(*) FROM channel_binding_token WHERE token_hash=$1 AND consumed_at IS NULL", &[&hex::encode(Sha256::digest(pending.as_bytes()))]).await.unwrap().get::<_,i64>(0),1);
    assert!(
        guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            second.shadow_user,
            second.grant_generation
        )
        .await
        .unwrap()
    );
    assert!(fake.events.lock().unwrap().is_empty());
    assert_eq!(fake.cancels.lock().unwrap().len(), 1);
    db.batch_execute("DROP TRIGGER channels_sprint_forced_abort ON zbs_telegram_guest_grant; DROP FUNCTION channels_sprint_forced_abort();").await.unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{root}/installations/{installation}"),
            &token(member),
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{root}/installations/{installation}"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        !guest_grant_active(
            &**db,
            Channel::Telegram,
            workspace,
            installation,
            agent,
            second.shadow_user,
            second.grant_generation
        )
        .await
        .unwrap()
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/telegram/binding/redeem",
            &token(owner),
            json!({"token":pending})
        )
        .await
        .0,
        StatusCode::GONE
    );
    assert_eq!(fake.cancels.lock().unwrap().len(), 2);
    assert_eq!(fake.events.lock().unwrap().len(), 1);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("{root}/installations/{installation}"),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(fake.cancels.lock().unwrap().len(), 2);
    notification_privacy(
        &pool,
        &db,
        workspace,
        foreign,
        owner,
        member,
        stranger,
        first.shadow_user,
        &suffix,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn notification_privacy(
    pool: &Arc<Pool>,
    db: &deadpool_postgres::Object,
    workspace: Uuid,
    foreign: Uuid,
    owner: Uuid,
    member: Uuid,
    outsider: Uuid,
    shadow: Uuid,
    suffix: &str,
) {
    let watcher = insert_user(db, "parentwatch", suffix).await;
    let muted = insert_user(db, "muted", suffix).await;
    for u in [watcher, muted] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&workspace, &u],
        )
        .await
        .unwrap();
    }
    let parent: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id) VALUES ($1,1,'Parent','member',$2) RETURNING id", &[&workspace,&owner]).await.unwrap().get(0);
    let child: Uuid = db.query_one("INSERT INTO issue(workspace_id,number,title,creator_type,creator_id,status,parent_issue_id) VALUES ($1,2,'Child','member',$2,'in_progress',$3) RETURNING id", &[&workspace,&owner,&parent]).await.unwrap().get(0);
    for (issue, user, reason) in [
        (child, owner, "creator"),
        (child, member, "delegated"),
        (parent, member, "manual"),
        (parent, watcher, "manual"),
        (child, muted, "manual"),
        (child, outsider, "manual"),
        (child, shadow, "manual"),
    ] {
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES ($1,'member',$2,$3)", &[&issue,&user,&reason]).await.unwrap();
    }
    db.execute(
        "INSERT INTO notification_preference(workspace_id,user_id,preferences) VALUES ($1,$2,$3)",
        &[
            &workspace,
            &muted,
            &json!({"status_changes":"muted","comments":"muted"}),
        ],
    )
    .await
    .unwrap();
    let mut n = Notification {
        event_id: Uuid::new_v4(),
        workspace,
        issue: child,
        actor: Some(owner),
        actor_type: "member".into(),
        excluded: Default::default(),
        kind: "status_changed".into(),
        severity: "info".into(),
        title: "Child changed".into(),
        body: "Private test body".into(),
        details: json!({}),
    };
    let first = fanout(pool, &n).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].recipient, watcher,
        "delegated suppression must survive parent bubble"
    );
    assert_eq!(first[0].issue, child);
    assert!(
        fanout(pool, &n).await.unwrap().is_empty(),
        "same event must not duplicate inbox rows"
    );
    db.execute(
        "UPDATE issue SET status='in_review' WHERE id=$1 AND workspace_id=$2",
        &[&child, &workspace],
    )
    .await
    .unwrap();
    n.event_id = Uuid::new_v4();
    let handoff = fanout(pool, &n).await.unwrap();
    assert_eq!(handoff.len(), 2);
    let recipients: std::collections::HashSet<_> = handoff.iter().map(|d| d.recipient).collect();
    assert_eq!(recipients, [member, watcher].into_iter().collect());
    assert!(
        handoff
            .iter()
            .all(|d| d.workspace == workspace && d.issue_status == "in_review")
    );
    n.event_id = Uuid::new_v4();
    n.kind = "new_comment".into();
    assert!(
        fanout(pool, &n).await.unwrap().is_empty(),
        "comments must not bubble to parent or delegated users"
    );
    n.event_id = Uuid::new_v4();
    n.kind = "mentioned".into();
    let mentioned = fanout(pool, &n).await.unwrap();
    assert_eq!(mentioned.len(), 2);
    assert!(
        mentioned
            .iter()
            .all(|d| d.recipient == member || d.recipient == muted)
    );
    n.workspace = foreign;
    assert!(
        fanout(pool, &n).await.unwrap().is_empty(),
        "foreign workspace must not see issue subscribers"
    );
    n.workspace = workspace;
    db.execute(
        "UPDATE issue_subscriber SET unsubscribed_at=now() WHERE issue_id=$1 AND user_id=$2",
        &[&child, &member],
    )
    .await
    .unwrap();
    n.event_id = Uuid::new_v4();
    let unsubscribed = fanout(pool, &n).await.unwrap();
    assert_eq!(unsubscribed.len(), 1);
    assert_eq!(unsubscribed[0].recipient, muted);
    let forbidden: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1 AND recipient_id=ANY($2)",
            &[&child, &vec![owner, outsider, shadow]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        forbidden, 0,
        "actor, stale nonmember and guest shadows must never receive inbox rows"
    );
    db.batch_execute("CREATE FUNCTION channels_sprint_inbox_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic inbox rollback'; END $$; CREATE TRIGGER channels_sprint_inbox_abort BEFORE INSERT ON inbox_item FOR EACH ROW EXECUTE FUNCTION channels_sprint_inbox_abort();").await.unwrap();
    let before: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&child],
        )
        .await
        .unwrap()
        .get(0);
    n.event_id = Uuid::new_v4();
    assert!(fanout(pool, &n).await.is_err());
    let after: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&child],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
    db.batch_execute("DROP TRIGGER channels_sprint_inbox_abort ON inbox_item; DROP FUNCTION channels_sprint_inbox_abort();").await.unwrap();
    // Missing custom status catalogs cannot silently become human handoffs.
    db.execute(
        "UPDATE issue SET status='channels_missing' WHERE id=$1",
        &[&child],
    )
    .await
    .unwrap();
    n.event_id = Uuid::new_v4();
    n.kind = "status_changed".into();
    assert!(matches!(
        fanout(pool, &n).await,
        Err(multica_server::api::channels_sprint_notify::FanoutError::UnknownStatus)
    ));
    let after_missing: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM inbox_item WHERE issue_id=$1",
            &[&child],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after_missing);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,'channels_review','Channel review','started','#abc123')", &[&workspace]).await.unwrap();
    db.execute(
        "UPDATE issue SET status='channels_review' WHERE id=$1",
        &[&child],
    )
    .await
    .unwrap();
    let custom = fanout(pool, &n).await.unwrap();
    assert_eq!(custom.len(), 2);
    assert_eq!(
        custom
            .iter()
            .map(|r| r.recipient)
            .collect::<std::collections::HashSet<_>>(),
        [member, watcher].into_iter().collect()
    );
    assert!(custom.iter().all(|r| r.issue_status == "channels_review"));
}
