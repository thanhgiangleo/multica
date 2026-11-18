//! Share-link mutations on a disposable, migrated PostgreSQL database.
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
        account_catalog_write_catalog_publish::{
            CatalogChange, CatalogPublication, CatalogPublisher,
        },
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &str = "synthetic-zbsi65-share-link-secret";
fn jwt(user: Uuid, seconds: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+seconds}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
fn database() -> Config {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(cfg.get_hosts().iter().all(
        |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
    ));
    cfg
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Observed {
    pool: Arc<Pool>,
    events: Mutex<Vec<CatalogPublication>>,
    trace: Mutex<Vec<&'static str>>,
    notified: Mutex<Vec<Uuid>>,
}
impl CatalogPublisher for Observed {
    fn disconnect_member<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("join must not disconnect members") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("join must not evict memberships") })
    }
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("join must not refresh runtime profiles") })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let db = tokio::time::timeout(std::time::Duration::from_secs(3), self.pool.get())
                .await
                .expect("handler kept DB connection during daemon notification")
                .unwrap();
            for user in &users {
                assert!(
                    db.query_one(
                        "SELECT onboarded_at IS NOT NULL FROM \"user\" WHERE id=$1",
                        &[user]
                    )
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
                );
            }
            self.trace.lock().unwrap().push("notify");
            self.notified.lock().unwrap().extend(users);
        })
    }
    fn publish<'a>(
        &'a self,
        event: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let db = tokio::time::timeout(std::time::Duration::from_secs(3), self.pool.get())
                .await
                .expect("handler kept DB connection during publication")
                .unwrap();
            let member = match &event.change {
                CatalogChange::MemberAdded {
                    member,
                    workspace_name,
                } => {
                    assert!(workspace_name.is_none());
                    member
                }
                CatalogChange::InvitationAccepted {
                    invitation_id,
                    member,
                } => {
                    let status: String = db
                        .query_one(
                            "SELECT status FROM workspace_invitation WHERE id=$1",
                            &[invitation_id],
                        )
                        .await
                        .unwrap()
                        .get(0);
                    assert_eq!(status, "accepted", "publication before invitation commit");
                    member
                }
                _ => panic!("unexpected event for share join"),
            };
            assert_eq!(member["user_id"], json!(event.actor_user_id));
            assert_eq!(member["workspace_id"], json!(event.workspace_id));
            let id = Uuid::parse_str(member["id"].as_str().unwrap()).unwrap();
            let role: String = db
                .query_one(
                    "SELECT role FROM member WHERE id=$1 AND workspace_id=$2 AND user_id=$3",
                    &[&id, &event.workspace_id, &event.actor_user_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(member["role"], role, "publication before membership commit");
            self.trace.lock().unwrap().push(event.change.event_type());
            self.events.lock().unwrap().push(event);
        })
    }
}
struct Fixture {
    app: axum::Router,
    pool: Arc<Pool>,
    observed: Arc<Observed>,
    ws: Uuid,
    owner: Uuid,
    admin: Uuid,
    member: Uuid,
    first: Uuid,
    second: Uuid,
}
impl Fixture {
    async fn new(cloud: bool) -> Self {
        Self::with_pool_size(cloud, 12).await
    }
    async fn with_pool_size(cloud: bool, max_size: usize) -> Self {
        let pool = Arc::new(
            Pool::builder(Manager::new(database(), NoTls))
                .max_size(max_size)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let mut users = Vec::new();
        for name in ["owner", "admin", "member", "first", "second"] {
            users.push(
                db.query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[&name, &format!("{name}-{suffix}@invalid.example")],
                )
                .await
                .unwrap()
                .get(0),
            );
        }
        let [owner, admin, member, first, second] = <[Uuid; 5]>::try_from(users).unwrap();
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Links',$1) RETURNING id",
                &[&format!("links-{suffix}")],
            )
            .await
            .unwrap()
            .get(0);
        for (user, role) in [(owner, "owner"), (admin, "admin"), (member, "member")] {
            db.execute(
                "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
                &[&ws, &user, &role],
            )
            .await
            .unwrap();
        }
        drop(db);
        let observed = Arc::new(Observed {
            pool: pool.clone(),
            events: Mutex::new(Vec::new()),
            trace: Mutex::new(Vec::new()),
            notified: Mutex::new(Vec::new()),
        });
        let mut state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec())
            .with_catalog_publisher(observed.clone());
        if cloud {
            state = state.with_cloud_url("https://example.invalid").unwrap();
        }
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        );
        Self {
            app,
            pool,
            observed,
            ws,
            owner,
            admin,
            member,
            first,
            second,
        }
    }
    fn path(&self) -> String {
        format!("/api/workspaces/{}/share-links", self.ws)
    }
    async fn create(&self, user: Uuid, body: Value) -> (StatusCode, Value) {
        call(&self.app, "POST", &self.path(), &jwt(user, 3600), body).await
    }
    async fn count(&self, id: Uuid) -> (i32, bool) {
        let row = self
            .pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT use_count,is_active FROM workspace_share_link WHERE id=$1",
                &[&id],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    }
    async fn member_role(&self, user: Uuid) -> Option<String> {
        self.pool
            .get()
            .await
            .unwrap()
            .query_opt(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&self.ws, &user],
            )
            .await
            .unwrap()
            .map(|row| row.get(0))
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn create_rotate_revoke_validate_auth_and_rollback() {
    let f = Fixture::new(false).await;
    for body in [
        json!({"role":"owner"}),
        json!({"expires_in":0}),
        json!({"expires_in":2562048}),
        json!({"max_uses":0}),
        json!({"max_uses":2147483648_i64}),
    ] {
        assert_eq!(f.create(f.owner, body).await.0, StatusCode::BAD_REQUEST);
    }
    assert_eq!(f.create(f.member, json!({})).await.0, StatusCode::FORBIDDEN);
    assert_eq!(
        call(&f.app, "POST", &f.path(), &jwt(f.owner, -3600), json!({}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/workspaces/{}/share-links", Uuid::new_v4()),
            &jwt(f.owner, 3600),
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(f.path())
        .header("cookie", format!("multica_auth={}", jwt(f.owner, 3600)))
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let (status, a) = f
        .create(
            f.owner,
            json!({"role":"ADMIN","expires_in":24,"max_uses":2}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{a}");
    let old = Uuid::parse_str(a["id"].as_str().unwrap()).unwrap();
    assert_eq!(a["role"], "admin");
    assert_eq!(a["max_uses"], 2);
    assert_eq!(a["code"].as_str().unwrap().len(), 24);
    assert_eq!(f.count(old).await, (0, true));
    // A failed rotation cannot deactivate the active link.
    assert_eq!(
        f.create(f.admin, json!({"role":"owner"})).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(f.count(old).await, (0, true));
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION zbsi65_share_fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rotation failure'; END $$; CREATE TRIGGER zbsi65_share_fail_insert BEFORE INSERT ON workspace_share_link FOR EACH ROW EXECUTE FUNCTION zbsi65_share_fail_insert()").await.unwrap();
    assert_eq!(
        f.create(f.admin, json!({})).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(f.count(old).await, (0, true));
    db.batch_execute("DROP TRIGGER zbsi65_share_fail_insert ON workspace_share_link; DROP FUNCTION zbsi65_share_fail_insert()").await.unwrap();
    let (status, b) = f.create(f.admin, json!({"role":null})).await;
    assert_eq!(status, StatusCode::CREATED, "{b}");
    assert_eq!(b["role"], "member");
    assert_eq!(f.count(old).await, (0, false));
    let next = Uuid::parse_str(b["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), next);
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.member, 3600), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("{}/bad", f.path()),
            &jwt(f.admin, 3600),
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.admin, 3600), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.admin, 3600), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(f.count(next).await, (0, false));
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn rotations_serialize_and_foreign_revocation_is_inert() {
    let f = Fixture::new(false).await;
    let other = Fixture::new(false).await;
    let (a, b) = tokio::join!(f.create(f.owner, json!({})), f.create(f.admin, json!({})));
    assert_eq!(a.0, StatusCode::CREATED, "{a:?}");
    assert_eq!(b.0, StatusCode::CREATED, "{b:?}");
    let db = f.pool.get().await.unwrap();
    let rows = db
        .query(
            "SELECT id,code FROM workspace_share_link WHERE workspace_id=$1 AND is_active=true",
            &[&f.ws],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id: Uuid = rows[0].get("id");
    let code: String = rows[0].get("code");
    let response = call(
        &other.app,
        "DELETE",
        &format!("{}/{}", other.path(), id),
        &jwt(other.owner, 3600),
        Value::Null,
    )
    .await;
    assert_eq!(response.0, StatusCode::NO_CONTENT);
    assert_eq!(f.count(id).await, (0, true));
    db.execute(
        "UPDATE workspace_share_link SET expires_at=now()-interval '1 second' WHERE id=$1",
        &[&id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &jwt(f.first, 3600),
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.count(id).await, (0, true));
    assert_eq!(f.member_role(f.first).await, None);
    assert!(f.observed.events.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn join_claims_once_settles_invitation_and_records_committed_event() {
    let f = Fixture::new(false).await;
    let (status, link) = f
        .create(f.owner, json!({"max_uses":1,"role":"admin"}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{link}");
    let id = Uuid::parse_str(link["id"].as_str().unwrap()).unwrap();
    let code = link["code"].as_str().unwrap();
    let email: String = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&f.first])
        .await
        .unwrap()
        .get(0);
    let invitation: Uuid = f.pool.get().await.unwrap().query_one(
        "INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES($1,$2,$3,$4,'member',now()+interval '1 day') RETURNING id",
        &[&f.ws,&f.owner,&email,&f.first]).await.unwrap().get(0);
    let path = "/api/share-links/join";
    assert_eq!(
        call(
            &f.app,
            "POST",
            path,
            &jwt(f.first, -3600),
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            path,
            &jwt(f.owner, 3600),
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            path,
            &jwt(f.first, 3600),
            json!({"code":"bogus"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.count(id).await, (0, true));
    let (status, response) = call(
        &f.app,
        "POST",
        path,
        &jwt(f.first, 3600),
        json!({"code":code}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["member"]["role"], "admin");
    assert_eq!(response["workspace_id"], f.ws.to_string());
    assert_eq!(f.count(id).await, (1, true));
    assert_eq!(f.member_role(f.first).await.as_deref(), Some("admin"));
    let db = f.pool.get().await.unwrap();
    let row = db
        .query_one(
            "SELECT status,role FROM workspace_invitation WHERE id=$1",
            &[&invitation],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "accepted");
    assert_eq!(row.get::<_, String>(1), "member");
    assert!(
        db.query_one(
            "SELECT onboarded_at IS NOT NULL FROM \"user\" WHERE id=$1",
            &[&f.first]
        )
        .await
        .unwrap()
        .get::<_, bool>(0)
    );
    {
        let events = f.observed.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(
            serde_json::to_value(&events[0].change).unwrap(),
            json!({"member":response["member"]})
        );
        assert_eq!(
            serde_json::to_value(&events[1].change).unwrap(),
            json!({"invitation_id":invitation,"member":response["member"]})
        );
        assert_eq!(
            *f.observed.trace.lock().unwrap(),
            ["member:added", "invitation:accepted", "notify"]
        );
        assert_eq!(*f.observed.notified.lock().unwrap(), [f.first]);
    }
    assert_eq!(
        call(
            &f.app,
            "POST",
            path,
            &jwt(f.second, 3600),
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.member_role(f.second).await, None);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn concurrent_joins_respect_max_uses_and_revoke() {
    let f = Fixture::new(false).await;
    let (status, link) = f.create(f.owner, json!({"max_uses":1})).await;
    assert_eq!(status, StatusCode::CREATED, "{link}");
    let code = link["code"].as_str().unwrap();
    let id = Uuid::parse_str(link["id"].as_str().unwrap()).unwrap();
    let body = json!({"code":code});
    let first_token = jwt(f.first, 3600);
    let second_token = jwt(f.second, 3600);
    let (a, b) = tokio::join!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &first_token,
            body.clone()
        ),
        call(&f.app, "POST", "/api/share-links/join", &second_token, body)
    );
    assert_eq!(
        [a.0, b.0]
            .into_iter()
            .filter(|s| *s == StatusCode::OK)
            .count(),
        1,
        "{a:?} {b:?}"
    );
    assert_eq!(f.count(id).await, (1, true));
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    let other = if a.0 == StatusCode::OK {
        f.second
    } else {
        f.first
    };
    assert_eq!(f.member_role(other).await, None);
    let (_, new_link) = f.create(f.owner, json!({"max_uses":2})).await;
    let new_code = new_link["code"].as_str().unwrap();
    let next = Uuid::parse_str(new_link["id"].as_str().unwrap()).unwrap();
    assert_eq!(f.count(id).await, (1, false));
    let path = format!("{}/{}", f.path(), next);
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &jwt(other, 3600),
            json!({"code":new_code})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(f.count(next).await, (0, false));
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn failed_onboarding_rolls_back_link_and_invitation() {
    let f = Fixture::new(false).await;
    let (status, link) = f.create(f.owner, json!({"max_uses":1})).await;
    assert_eq!(status, StatusCode::CREATED, "{link}");
    let code = link["code"].as_str().unwrap();
    let id = Uuid::parse_str(link["id"].as_str().unwrap()).unwrap();
    let email: String = f
        .pool
        .get()
        .await
        .unwrap()
        .query_one("SELECT email FROM \"user\" WHERE id=$1", &[&f.first])
        .await
        .unwrap()
        .get(0);
    let invitation: Uuid=f.pool.get().await.unwrap().query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,role,expires_at) VALUES($1,$2,$3,'member',now()+interval '1 day') RETURNING id",&[&f.ws,&f.owner,&email]).await.unwrap().get(0);
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION zbsi65_share_fail_onboarding() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback'; END $$; CREATE TRIGGER zbsi65_share_fail BEFORE UPDATE ON \"user\" FOR EACH ROW EXECUTE FUNCTION zbsi65_share_fail_onboarding()").await.unwrap();
    let response = call(
        &f.app,
        "POST",
        "/api/share-links/join",
        &jwt(f.first, 3600),
        json!({"code":code}),
    )
    .await;
    assert_eq!(
        response.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{response:?}"
    );
    assert_eq!(f.count(id).await, (0, true));
    assert_eq!(f.member_role(f.first).await, None);
    assert_eq!(
        db.query_one(
            "SELECT status FROM workspace_invitation WHERE id=$1",
            &[&invitation]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert!(f.observed.events.lock().unwrap().is_empty());
    db.batch_execute(
        "DROP TRIGGER zbsi65_share_fail ON \"user\"; DROP FUNCTION zbsi65_share_fail_onboarding()",
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn join_releases_single_connection_and_retry_does_not_republish() {
    let f = Fixture::with_pool_size(false, 1).await;
    let (status, link) = f.create(f.owner, json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{link}");
    let body = json!({"code":link["code"]});
    let token = jwt(f.first, 3600);
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &token,
            body.clone()
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        *f.observed.trace.lock().unwrap(),
        ["member:added", "notify"]
    );
    assert_eq!(*f.observed.notified.lock().unwrap(), [f.first]);
    assert_eq!(
        call(&f.app, "POST", "/api/share-links/join", &token, body)
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    assert_eq!(f.observed.trace.lock().unwrap().len(), 2);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn cloud_and_task_credentials_cannot_bypass_seat_or_workspace_guards() {
    let f = Fixture::new(true).await;
    let (status, link) = f.create(f.owner, json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{link}");
    let id = Uuid::parse_str(link["id"].as_str().unwrap()).unwrap();
    let code = link["code"].as_str().unwrap();
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &jwt(f.first, 3600),
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.count(id).await, (0, true));
    assert_eq!(f.member_role(f.first).await, None);
    let db = f.pool.get().await.unwrap();
    let agent: Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Share agent','cloud',$2) RETURNING id",&[&f.ws,&f.owner]).await.unwrap().get(0);
    let runtime: Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Share runtime','cloud','synthetic','online') RETURNING id",&[&f.ws]).await.unwrap().get(0);
    let task: Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id",&[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_share_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')",
        &[&hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.path(), &token, json!({})).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/share-links/join",
            &token,
            json!({"code":code})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.path(), &token, json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert!(f.observed.events.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn go_request_decoding_without_content_type_does_not_leak_failed_writes() {
    let f = Fixture::new(false).await;
    let send = |path: String, user: Uuid, raw: String| {
        let app = f.app.clone();
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header("authorization", format!("Bearer {}", jwt(user, 3600)))
                        .body(Body::from(raw))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    let (status, created) = send(f.path(), f.owner,
        r#"{"ROLE":"admin","role":null,"expires_in":12,"EXPIRES_IN":null,"max_uses":1,"MAX_USES":null} ignored"#.to_owned()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["role"], "admin");
    assert!(created.get("max_uses").is_none());
    assert!(created.get("expires_at").is_none());
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let code = created["code"].as_str().unwrap().to_owned();
    for raw in [r#"{"role":1}"#, r#"{"max_uses":1.2}"#, "[]"] {
        assert_eq!(
            send(f.path(), f.owner, raw.to_owned()).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let huge = format!(
        r#"{{"role":"member","ignored":"{}"}} trailing"#,
        "x".repeat(2_200_000)
    );
    assert_eq!(send(f.path(), f.owner, huge).await.0, StatusCode::CREATED);
    assert_eq!(f.count(id).await, (0, false));
    let new_code = send(f.path(), f.owner, "{}".into()).await.1["code"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        send(
            "/api/share-links/join".into(),
            f.first,
            format!(r#"{{"CODE":"{new_code}","code":null}} trailing"#)
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(f.member_role(f.first).await, Some("member".into()));
    assert_eq!(
        send(
            "/api/share-links/join".into(),
            f.second,
            r#"{"code":12}"#.into()
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(f.member_role(f.second).await, None);
    assert_ne!(code, new_code);
}
