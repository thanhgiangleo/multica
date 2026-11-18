//! Synthetic membership departure HTTP/DB checks; no production resources or providers.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "synthetic-departure-secret";
fn token(user: Uuid, expired: bool) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+if expired {-300} else {3600}}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    credential: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {credential}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    if status == StatusCode::NO_CONTENT {
        assert!(bytes.is_empty());
    }
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Fixture {
    pool: Arc<Pool>,
    app: axum::Router,
    ws: Uuid,
    foreign: Uuid,
    owner: Uuid,
    admin: Uuid,
    member: Uuid,
    outsider: Uuid,
    member_id: Uuid,
    owner_id: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(cfg.get_hosts().iter().all(|host| matches!(host,
            tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1")));
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(8)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let mut users = Vec::new();
        for name in ["Owner", "Admin", "Member", "Outsider"] {
            users.push(
                db.query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[
                        &name,
                        &format!("departure-{}@invalid.example", Uuid::new_v4()),
                    ],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [owner, admin, member, outsider] = <[Uuid; 4]>::try_from(users).unwrap();
        let mut workspaces = Vec::new();
        for name in ["Departure", "Foreign"] {
            workspaces.push(
                db.query_one(
                    "INSERT INTO workspace(name,slug) VALUES($1,$2) RETURNING id",
                    &[&name, &format!("departure-{}", Uuid::new_v4())],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [ws, foreign] = <[Uuid; 2]>::try_from(workspaces).unwrap();
        db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin'),($1,$4,'member'),($5,$4,'owner')",
            &[&ws,&owner,&admin,&member,&foreign]).await.unwrap();
        let member_id = db
            .query_one(
                "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &member],
            )
            .await
            .unwrap()
            .get(0);
        let owner_id = db
            .query_one(
                "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &owner],
            )
            .await
            .unwrap()
            .get(0);
        let app = app(pool.clone(), false);
        Self {
            pool,
            app,
            ws,
            foreign,
            owner,
            admin,
            member,
            outsider,
            member_id,
            owner_id,
        }
    }
    fn remove(&self, member: Uuid) -> String {
        format!("/api/workspaces/{}/members/{member}", self.ws)
    }
    fn leave(&self) -> String {
        format!("/api/workspaces/{}/leave", self.ws)
    }
    async fn present(&self, user: Uuid) -> bool {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2)",
                &[&self.ws, &user],
            )
            .await
            .unwrap()
            .get(0)
    }
}
fn app(pool: Arc<Pool>, cloud: bool) -> axum::Router {
    let mut state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec());
    if cloud {
        state = state.with_cloud_url("http://127.0.0.1:1").unwrap();
    }
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    )
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn departure_auth_roles_and_scope() {
    let f = Fixture::new().await;
    let path = f.remove(f.member_id);
    assert_eq!(
        call(&f.app, "DELETE", &path, &token(f.owner, true)).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &token(f.outsider, false))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &token(f.member, false))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &f.remove(f.owner_id),
            &token(f.admin, false)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &f.remove(f.owner_id),
            &token(f.owner, false)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&f.app, "POST", &f.leave(), &token(f.owner, false))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let foreign_member: Uuid = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&f.foreign, &f.member],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &f.remove(foreign_member),
            &token(f.owner, false)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/members/bad", f.ws),
            &token(f.owner, false)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/members/bad", f.ws),
            &token(f.outsider, false)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/workspaces/bad/leave",
            &token(f.member, false)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    for (method, url) in [("DELETE", path.clone()), ("POST", f.leave())] {
        let response = f
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(url)
                    .header("cookie", format!("multica_auth={}", token(f.owner, false)))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let db = f.pool.get().await.unwrap();
    let pat = format!("mul_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked) VALUES($1,'revoked',$2,'mul_test',true)", &[&f.owner,&hash]).await.unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &pat).await.0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE personal_access_token SET revoked=false,expires_at=now()-interval '1 hour' WHERE token_hash=$1", &[&hash]).await.unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &pat).await.0,
        StatusCode::UNAUTHORIZED
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Task actor','cloud',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,$2,'cloud','synthetic','online') RETURNING id", &[&f.ws,&Uuid::new_v4().to_string()]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_departure_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &task_token).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&f.app, "POST", &f.leave(), &task_token).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/workspaces/{}/leave", f.foreign),
            &task_token
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE \"user\" SET zbs_is_shadow=true WHERE id=$1",
        &[&f.member],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.leave(), &token(f.member, false))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert!(f.present(f.member).await);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn departure_cleanup_is_atomic_scoped_and_preserves_public_furniture() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let mut private_view = Uuid::nil();
    let mut public_view = Uuid::nil();
    for ws in [f.ws, f.foreign] {
        let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Other owner agent','cloud',$2) RETURNING id", &[&ws,&f.owner]).await.unwrap().get(0);
        let installation = Uuid::new_v4();
        db.execute("INSERT INTO channel_user_binding(workspace_id,multica_user_id,installation_id,channel_type,channel_user_id) VALUES($1,$2,$3,'telegram','synthetic')", &[&ws,&f.member,&installation]).await.unwrap();
        db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'member',$2)", &[&agent,&f.member]).await.unwrap();
        for visibility in ["private", "public"] {
            db.execute("INSERT INTO quick_action(workspace_id,name,assignee_type,assignee_id,prompt,visibility,created_by_type,created_by_id) VALUES($1,'Action','agent',$2,'Synthetic',$3,'member',$4)", &[&ws,&agent,&visibility,&f.member]).await.unwrap();
        }
        for visibility in ["private", "workspace"] {
            let view:Uuid=db.query_one("INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,visibility,query) VALUES($1,$2,$3,'workspace',$4,'{}') RETURNING id", &[&ws,&f.member,&visibility,&visibility]).await.unwrap().get(0);
            db.execute("INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id) VALUES($1,$2,'view',$3)", &[&ws,&f.owner,&view]).await.unwrap();
            if ws == f.ws {
                if visibility == "private" {
                    private_view = view;
                } else {
                    public_view = view;
                }
            }
        }
        db.execute("INSERT INTO issue_view_preference(workspace_id,user_id,scope_type,scope_id) VALUES($1,$2,'workspace',$1)", &[&ws,&f.member]).await.unwrap();
        let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Subscriber fixture','member',$2) RETURNING id", &[&ws,&f.owner]).await.unwrap().get(0);
        db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&issue,&f.member]).await.unwrap();
        let autopilot:Uuid=db.query_one("INSERT INTO autopilot(workspace_id,title,assignee_id,created_by_type,created_by_id) VALUES($1,'Subscriber fixture',$2,'member',$3) RETURNING id", &[&ws,&agent,&f.owner]).await.unwrap().get(0);
        db.execute("INSERT INTO autopilot_subscriber(autopilot_id,user_type,user_id) VALUES($1,'member',$2)", &[&autopilot,&f.member]).await.unwrap();
        for table in ["zbs_telegram_guest_invite", "zbs_zalo_guest_invite"] {
            for consumed in [false, true] {
                db.execute(&format!("INSERT INTO {table}(id,workspace_id,installation_id,agent_id,code_hash,issued_by_user_id,alias,expires_at,consumed_at) VALUES($1,$2,$3,$4,$5,$6,'Synthetic',now()+interval '1 hour',CASE WHEN $7 THEN now() END)"),
                    &[&Uuid::new_v4(),&ws,&installation,&agent,&Uuid::new_v4().to_string(),&f.member,&consumed]).await.unwrap();
            }
        }
    }
    // Fault after every cleanup statement: nothing may survive a rollback.
    db.batch_execute("CREATE FUNCTION pg_temp.reject_departure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic member fault'; END $$; CREATE TRIGGER account_catalog_departure_fault BEFORE DELETE ON member FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_departure()").await.unwrap();
    let (status, body) = call(
        &f.app,
        "DELETE",
        &f.remove(f.member_id),
        &token(f.admin, false),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    db.batch_execute("DROP TRIGGER account_catalog_departure_fault ON member")
        .await
        .unwrap();
    assert!(f.present(f.member).await);
    let retained:bool=db.query_one("SELECT EXISTS(SELECT 1 FROM issue_view WHERE id=$1) AND EXISTS(SELECT 1 FROM channel_user_binding WHERE workspace_id=$2 AND multica_user_id=$3) AND NOT EXISTS(SELECT 1 FROM zbs_telegram_guest_invite WHERE workspace_id=$2 AND revoked_at IS NOT NULL)", &[&private_view,&f.ws,&f.member]).await.unwrap().get(0);
    assert!(retained);
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("{}/", f.remove(f.member_id)),
            &token(f.admin, false)
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(!f.present(f.member).await);
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &f.remove(f.member_id),
            &token(f.admin, false)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&f.app, "POST", &f.leave(), &token(f.member, false))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let gone:bool=db.query_one("SELECT NOT EXISTS(SELECT 1 FROM issue_view WHERE id=$1) AND NOT EXISTS(SELECT 1 FROM pinned_item WHERE item_id=$1) AND EXISTS(SELECT 1 FROM issue_view WHERE id=$2) AND EXISTS(SELECT 1 FROM pinned_item WHERE item_id=$2)", &[&private_view,&public_view]).await.unwrap().get(0);
    assert!(gone);
    for (sql, expected) in [
        (
            "SELECT count(*) FROM channel_user_binding WHERE workspace_id=$1 AND multica_user_id=$2",
            0,
        ),
        (
            "SELECT count(*) FROM agent_invocation_target t JOIN agent a ON a.id=t.agent_id WHERE a.workspace_id=$1 AND t.target_id=$2",
            0,
        ),
        (
            "SELECT count(*) FROM quick_action WHERE workspace_id=$1 AND created_by_id=$2 AND visibility='private'",
            0,
        ),
        (
            "SELECT count(*) FROM quick_action WHERE workspace_id=$1 AND created_by_id=$2 AND visibility='public'",
            1,
        ),
        (
            "SELECT count(*) FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2",
            0,
        ),
        (
            "SELECT count(*) FROM issue_subscriber s JOIN issue i ON i.id=s.issue_id WHERE i.workspace_id=$1 AND s.user_id=$2",
            0,
        ),
        (
            "SELECT count(*) FROM autopilot_subscriber s JOIN autopilot a ON a.id=s.autopilot_id WHERE a.workspace_id=$1 AND s.user_id=$2",
            0,
        ),
    ] {
        assert_eq!(
            db.query_one(sql, &[&f.ws, &f.member])
                .await
                .unwrap()
                .get::<_, i64>(0),
            expected,
            "{sql}"
        );
        assert_eq!(
            db.query_one(sql, &[&f.foreign, &f.member])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1,
            "foreign: {sql}"
        );
    }
    for table in ["zbs_telegram_guest_invite", "zbs_zalo_guest_invite"] {
        let revoked:i64=db.query_one(&format!("SELECT count(*) FROM {table} WHERE workspace_id=$1 AND issued_by_user_id=$2 AND revoked_at IS NOT NULL AND revoked_by_user_id=$2"), &[&f.ws,&f.member]).await.unwrap().get(0);
        assert_eq!(revoked, 1);
        let preserved:i64=db.query_one(&format!("SELECT count(*) FROM {table} WHERE workspace_id=$1 AND issued_by_user_id=$2 AND revoked_at IS NULL"), &[&f.foreign,&f.member]).await.unwrap().get(0);
        assert_eq!(preserved, 2);
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn departures_serialize_last_owner_and_double_remove() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE member SET role='owner' WHERE id=$1",
        &[&f.member_id],
    )
    .await
    .unwrap();
    let path = f.leave();
    let owner = token(f.owner, false);
    let second = token(f.member, false);
    let (a, b) = tokio::join!(
        call(&f.app, "POST", &path, &owner),
        call(&f.app, "POST", &path, &second)
    );
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [StatusCode::NO_CONTENT, StatusCode::BAD_REQUEST]);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM member WHERE workspace_id=$1 AND role='owner'",
            &[&f.ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    // Admin removal races resolve to one success, one absent target, never 500.
    let target: Uuid = db
        .query_one(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member') RETURNING id",
            &[&f.ws, &f.outsider],
        )
        .await
        .unwrap()
        .get(0);
    let path = f.remove(target);
    let admin = token(f.admin, false);
    let (a, b) = tokio::join!(
        call(&f.app, "DELETE", &path, &admin),
        call(&f.app, "DELETE", &path, &admin)
    );
    let mut codes = [a.0, b.0];
    codes.sort();
    assert_eq!(codes, [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND]);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn departure_subscriber_lock_and_cloud_fail_closed() {
    let f = Fixture::new().await;
    let cloud = app(f.pool.clone(), true);
    assert_eq!(
        call(&cloud, "POST", &f.leave(), &token(f.member, false))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.present(f.member).await);
    let mut db = f.pool.get().await.unwrap();
    let tx = db.transaction().await.unwrap();
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1::uuid::text),hashtext($2::uuid::text))",
        &[&f.ws, &f.member],
    )
    .await
    .unwrap();
    let path = format!("/api/workspaces/{}/leave", f.ws.to_string().to_uppercase());
    let member = token(f.member, false);
    let departure = call(&f.app, "POST", &path, &member);
    tokio::pin!(departure);
    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut departure)
            .await
            .is_err()
    );
    // Issue allocation touches workspace state. This would deadlock if the
    // departure took the workspace lock before LockSubscriberWrites.
    let issue:Uuid=tx.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Concurrent watch','member',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    tx.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&issue,&f.member]).await.unwrap();
    tx.commit().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), departure)
        .await
        .unwrap();
    assert_eq!(result.0, StatusCode::NO_CONTENT, "{}", result.1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_subscriber WHERE issue_id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn departure_runtime_and_agent_dependencies_never_partially_delete() {
    let f = Fixture::new().await;
    let mut db = f.pool.get().await.unwrap();
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status,owner_id) VALUES($1,$2,'cloud','synthetic','online',$3) RETURNING id", &[&f.ws,&Uuid::new_v4().to_string(),&f.member]).await.unwrap().get(0);
    let path = f.remove(f.member_id);
    let credential = token(f.owner, false);
    assert_eq!(
        call(&f.app, "DELETE", &path, &credential).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute("UPDATE agent_runtime SET owner_id=$2,visibility='private',shared_with=ARRAY[$3::uuid] WHERE id=$1", &[&runtime,&f.owner,&f.member]).await.unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &credential).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE agent_runtime SET shared_with='{}' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Owner guest dependency','cloud',$2) RETURNING id", &[&f.ws,&f.member]).await.unwrap().get(0);
    assert_eq!(
        call(&f.app, "DELETE", &path, &credential).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    db.execute(
        "UPDATE agent SET owner_id=$2 WHERE id=$1",
        &[&agent, &f.owner],
    )
    .await
    .unwrap();
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "UPDATE agent_runtime SET owner_id=$2 WHERE id=$1",
        &[&runtime, &f.member],
    )
    .await
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(8),
        call(&f.app, "DELETE", &path, &credential),
    )
    .await
    .unwrap();
    assert_eq!(result.0, StatusCode::SERVICE_UNAVAILABLE, "{}", result.1);
    assert_eq!(result.1["error"], "member revocation dependencies busy");
    tx.rollback().await.unwrap();
    assert!(f.present(f.member).await);
    assert_eq!(
        call(&f.app, "DELETE", &path, &credential).await.0,
        StatusCode::NO_CONTENT
    );
}
