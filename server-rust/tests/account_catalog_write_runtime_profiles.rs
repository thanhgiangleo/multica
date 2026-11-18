//! Runtime-profile mutations against isolated migrated PostgreSQL.
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

const SECRET: &str = "synthetic-zbsi65-runtime-profile-secret";
fn jwt(user: Uuid, duration: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+duration}),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}
fn database() -> Config {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(ip) if ip=="127.0.0.1"))
    );
    cfg
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
type Trace = Vec<(&'static str, Uuid, Uuid)>;
struct Observed {
    pool: Arc<Pool>,
    events: Mutex<Vec<CatalogPublication>>,
    trace: Mutex<Trace>,
}
impl CatalogPublisher for Observed {
    fn disconnect_member<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("profile mutation must not disconnect members") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("profile mutation must not evict memberships") })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        _: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {
            panic!("profile mutation must use profile refresh, not membership refresh")
        })
    }
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        workspace: Uuid,
        profile: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // A single free pool slot must suffice, even when the request caller
            // holds the other one. No callback may retain the transaction connection.
            let db = tokio::time::timeout(std::time::Duration::from_secs(3), self.pool.get())
                .await
                .expect("handler kept DB connection during daemon refresh")
                .unwrap();
            let stored = db
                .query_opt(
                    "SELECT workspace_id FROM runtime_profile WHERE id=$1",
                    &[&profile],
                )
                .await
                .unwrap();
            if let Some(stored) = stored {
                assert_eq!(stored.get::<_, Uuid>(0), workspace);
            }
            self.trace
                .lock()
                .unwrap()
                .push(("refresh", workspace, profile));
        })
    }
    fn publish<'a>(
        &'a self,
        event: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let (profile, deleted) = match &event.change {
                CatalogChange::RuntimeProfileChanged { runtime_profile_id } => {
                    (*runtime_profile_id, false)
                }
                CatalogChange::RuntimeProfileDeleted {
                    deleted_runtime_profile_id,
                } => (*deleted_runtime_profile_id, true),
                _ => panic!("unexpected event for profile mutation"),
            };
            assert_eq!(event.change.event_type(), "daemon:register");
            let db = tokio::time::timeout(std::time::Duration::from_secs(3), self.pool.get())
                .await
                .expect("handler kept DB connection during publication")
                .unwrap();
            let exists: bool = db
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM runtime_profile WHERE id=$1 AND workspace_id=$2)",
                    &[&profile, &event.workspace_id],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(exists, !deleted, "publication before successful commit");
            self.trace
                .lock()
                .unwrap()
                .push(("publish", event.workspace_id, profile));
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
}
impl Fixture {
    async fn new() -> Self {
        Self::with_pool_size(2).await
    }
    async fn with_pool_size(max_size: usize) -> Self {
        let pool = Arc::new(
            Pool::builder(Manager::new(database(), NoTls))
                .max_size(max_size)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let mut users = Vec::new();
        for name in ["owner", "admin", "member"] {
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
        let [owner, admin, member] = <[Uuid; 3]>::try_from(users).unwrap();
        let ws: Uuid = db
            .query_one(
                "INSERT INTO workspace(name,slug) VALUES('Profiles',$1) RETURNING id",
                &[&format!("profiles-{suffix}")],
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
        let observed = Arc::new(Observed {
            pool: pool.clone(),
            events: Mutex::new(Vec::new()),
            trace: Mutex::new(Vec::new()),
        });
        let state = ApiState::new(pool.clone(), SECRET.as_bytes().to_vec())
            .with_catalog_publisher(observed.clone());
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
        }
    }
    fn path(&self) -> String {
        format!("/api/workspaces/{}/runtime-profiles", self.ws)
    }
    async fn create(&self, user: Uuid, body: Value) -> (StatusCode, Value) {
        call(&self.app, "POST", &self.path(), &jwt(user, 3600), body).await
    }
    async fn stored(&self, id: Uuid) -> Option<(String, String)> {
        self.pool
            .get()
            .await
            .unwrap()
            .query_opt(
                "SELECT display_name,command_name FROM runtime_profile WHERE id=$1",
                &[&id],
            )
            .await
            .unwrap()
            .map(|row| (row.get(0), row.get(1)))
    }
}
fn valid(name: &str) -> Value {
    json!({"display_name":name,"protocol_family":"pi","runtime_type":"omp","command_name":"omp","fixed_args":["--json"]})
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn create_update_delete_real_rows_and_post_commit_events() {
    let f = Fixture::new().await;
    let (status, created) = f.create(f.owner, valid("Wrapper")).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    assert_eq!(created["protocol_family"], "pi");
    assert_eq!(created["runtime_type"], "omp");
    assert_eq!(created["visibility"], "workspace");
    assert_eq!(created["fixed_args"], json!(["--json"]));
    assert_eq!(f.stored(id).await, Some(("Wrapper".into(), "omp".into())));
    assert_eq!(
        f.create(f.admin, valid("Wrapper")).await.0,
        StatusCode::CONFLICT
    );
    let path = format!("{}/{}", f.path(), id);
    for method in ["PATCH", "PUT"] {
        let (status,updated)=call(&f.app,method,&path,&jwt(f.admin,3600),json!({"display_name":"Renamed","command_name":" /bin/wrapper ","fixed_args":[],"enabled":false,"description":null})).await;
        assert_eq!(status, StatusCode::OK, "{updated}");
        assert_eq!(updated["command_name"], "/bin/wrapper");
        assert_eq!(updated["fixed_args"], json!([]));
        assert_eq!(updated["enabled"], false);
    }
    assert_eq!(
        f.stored(id).await,
        Some(("Renamed".into(), "/bin/wrapper".into()))
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(f.stored(id).await, None);
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let events = f.observed.events.lock().unwrap();
    assert_eq!(events.len(), 4);
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event.workspace_id, f.ws);
        assert!([f.owner, f.admin].contains(&event.actor_user_id));
        let payload = serde_json::to_value(&event.change).unwrap();
        assert_eq!(event.change.event_type(), "daemon:register");
        assert_eq!(
            payload,
            if index == 3 {
                json!({"deleted_runtime_profile_id":id})
            } else {
                json!({"runtime_profile_id":id})
            }
        );
    }
    let trace = f.observed.trace.lock().unwrap();
    assert_eq!(trace.len(), 8);
    for step in trace.chunks_exact(2) {
        assert_eq!(step, [("refresh", f.ws, id), ("publish", f.ws, id)]);
    }
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn validation_auth_and_cross_workspace_guards_do_not_mutate() {
    let f = Fixture::new().await;
    let other = Fixture::new().await;
    for body in [
        json!({}),
        json!({"display_name":"  ","protocol_family":"pi","command_name":"pi"}),
        json!({"display_name":"Bad","protocol_family":"pi","runtime_type":"codex","command_name":"pi"}),
        json!({"display_name":"Bad","runtime_type":"unknown","command_name":"bad"}),
        json!({"display_name":"Bad","protocol_family":"pi","command_name":"pi --flag"}),
        json!({"display_name":"Bad","protocol_family":"pi","command_name":"pi","fixed_args":[""]}),
    ] {
        assert_eq!(f.create(f.owner, body).await.0, StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        f.create(f.member, valid("Denied")).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            &f.path(),
            &jwt(f.owner, -300),
            valid("Expired")
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            &other.path(),
            &jwt(f.owner, 3600),
            valid("Foreign")
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
        .body(Body::from(valid("Cookie").to_string()))
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let db = f.pool.get().await.unwrap();
    let pat = format!("mul_profile_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked) VALUES($1,'profile test',$2,'mul_profile',true)", &[&f.owner,&hash]).await.unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.path(), &pat, valid("Revoked"))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    db.execute("UPDATE personal_access_token SET revoked=false,expires_at=now()-interval '1 hour' WHERE token_hash=$1", &[&hash]).await.unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.path(), &pat, valid("Expired PAT"))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, created) = f.create(f.owner, valid("Valid")).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    for body in [
        json!({"runtime_type":null}),
        json!({"protocol_family":"pi"}),
        json!({"display_name":" "}),
        json!({"fixed_args":[" "]}),
        json!({"command_name":"pi --json"}),
    ] {
        // Go pointer field accepts JSON null as absent; only non-null immutable values are rejected.
        let wanted = if body["runtime_type"].is_null() && body.get("runtime_type").is_some() {
            StatusCode::OK
        } else {
            StatusCode::BAD_REQUEST
        };
        assert_eq!(
            call(&f.app, "PATCH", &path, &jwt(f.owner, 3600), body)
                .await
                .0,
            wanted
        );
    }
    assert_eq!(
        call(
            &f.app,
            "PATCH",
            &path,
            &jwt(f.member, 3600),
            json!({"display_name":"No"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &other.app,
            "PATCH",
            &format!("{}/{}", other.path(), id),
            &jwt(other.owner, 3600),
            json!({"display_name":"No"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &f.app,
            "PATCH",
            &format!("{}/bad", f.path()),
            &jwt(f.owner, 3600),
            json!({})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(f.stored(id).await, Some(("Valid".into(), "omp".into())));
    assert_eq!(f.observed.events.lock().unwrap().len(), 2);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn registered_instances_fail_closed_and_task_token_never_admin() {
    let f = Fixture::new().await;
    let (status, created) = f.create(f.owner, valid("Protected")).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    let db = f.pool.get().await.unwrap();
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Profile agent','cloud',$2) RETURNING id",&[&f.ws,&f.owner]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status,profile_id) VALUES($1,'Profile runtime','cloud','synthetic','online',$2) RETURNING id",&[&f.ws,&id]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id",&[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_profile_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')",&[&hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    assert_eq!(
        call(&f.app, "POST", &f.path(), &token, valid("Forbidden"))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&f.app, "PATCH", &path, &token, json!({"enabled":false}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&f.app, "DELETE", &path, &token, Value::Null).await.0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &token, Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, body) = call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(f.stored(id).await, Some(("Protected".into(), "omp".into())));
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn parallel_creates_are_unique_and_failed_writes_leave_no_event() {
    let f = Fixture::new().await;
    let (a, b) = tokio::join!(
        f.create(f.owner, valid("Concurrent")),
        f.create(f.admin, valid("Concurrent"))
    );
    let statuses = [a.0, b.0];
    assert!(statuses.contains(&StatusCode::CREATED), "{statuses:?}");
    assert!(statuses.contains(&StatusCode::CONFLICT), "{statuses:?}");
    let db = f.pool.get().await.unwrap();
    let count:i64=db.query_one("SELECT count(*) FROM runtime_profile WHERE workspace_id=$1 AND display_name='Concurrent'",&[&f.ws]).await.unwrap().get(0);
    assert_eq!(count, 1);
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    let id = Uuid::parse_str(if a.0 == StatusCode::CREATED {
        a.1["id"].as_str().unwrap()
    } else {
        b.1["id"].as_str().unwrap()
    })
    .unwrap();
    // Synthetic database failure rolls back any profile edit; callback cannot run on failure.
    db.batch_execute("CREATE FUNCTION zbsi65_profile_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$; CREATE TRIGGER zbsi65_profile_fail BEFORE UPDATE ON runtime_profile FOR EACH ROW EXECUTE FUNCTION zbsi65_profile_fail()").await.unwrap();
    let path = format!("{}/{}", f.path(), id);
    assert_eq!(
        call(
            &f.app,
            "PATCH",
            &path,
            &jwt(f.owner, 3600),
            json!({"display_name":"Changed"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        f.stored(id).await,
        Some(("Concurrent".into(), "omp".into()))
    );
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    db.batch_execute(
        "DROP TRIGGER zbsi65_profile_fail ON runtime_profile; DROP FUNCTION zbsi65_profile_fail()",
    )
    .await
    .unwrap();
    db.batch_execute("CREATE FUNCTION zbsi65_profile_delete_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic delete failure'; END $$; CREATE TRIGGER zbsi65_profile_delete_fail BEFORE DELETE ON runtime_profile FOR EACH ROW EXECUTE FUNCTION zbsi65_profile_delete_fail()").await.unwrap();
    assert_eq!(
        call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        f.stored(id).await,
        Some(("Concurrent".into(), "omp".into()))
    );
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    db.batch_execute("DROP TRIGGER zbsi65_profile_delete_fail ON runtime_profile; DROP FUNCTION zbsi65_profile_delete_fail()").await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn compatibility_defaults_visibility_and_disjoint_updates() {
    let f = Fixture::new().await;
    let (status,created)=f.create(f.owner,json!({"display_name":" Legacy ","protocol_family":"codex","command_name":"wrapper","visibility":"private","fixed_args":null,"enabled":null,"description":"Keep me"})).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["display_name"], "Legacy");
    assert_eq!(created["runtime_type"], "codex");
    assert_eq!(created["visibility"], "workspace");
    assert_eq!(created["fixed_args"], json!([]));
    assert_eq!(created["enabled"], true);
    let path = format!("{}/{}", f.path(), created["id"].as_str().unwrap());
    let token = jwt(f.owner, 3600);
    let (a, b) = tokio::join!(
        call(
            &f.app,
            "PATCH",
            &path,
            &token,
            json!({"display_name":"New name"})
        ),
        call(
            &f.app,
            "PUT",
            &path,
            &token,
            json!({"command_name":"new-command","description":null})
        )
    );
    assert_eq!(a.0, StatusCode::OK, "{a:?}");
    assert_eq!(b.0, StatusCode::OK, "{b:?}");
    let (status, loaded) = call(&f.app, "GET", &path, &token, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{loaded}");
    assert_eq!(loaded["display_name"], "New name");
    assert_eq!(loaded["command_name"], "new-command");
    assert_eq!(loaded["description"], "Keep me");
    assert_eq!(loaded["visibility"], "workspace");
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn profile_refresh_and_publication_release_pool_size_one_connection() {
    let f = Fixture::with_pool_size(1).await;
    let (status, created) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        f.create(f.owner, valid("One connection")),
    )
    .await
    .expect("create deadlocked in post-commit hooks");
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    let (status, updated) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        call(
            &f.app,
            "PUT",
            &path,
            &jwt(f.owner, 3600),
            json!({"enabled":false}),
        ),
    )
    .await
    .expect("update deadlocked in post-commit hooks");
    assert_eq!(status, StatusCode::OK, "{updated}");
    let (status, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        call(&f.app, "DELETE", &path, &jwt(f.owner, 3600), Value::Null),
    )
    .await
    .expect("delete deadlocked in post-commit hooks");
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(f.observed.events.lock().unwrap().len(), 3);
    assert_eq!(f.observed.trace.lock().unwrap().len(), 6);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn deferred_commit_failure_does_not_refresh_or_publish_until_retry() {
    let f = Fixture::with_pool_size(1).await;
    let (status, created) = f.create(f.owner, valid("Deferred failure")).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION pg_temp.reject_profile_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit fault'; END $$; CREATE CONSTRAINT TRIGGER profile_commit_fault AFTER UPDATE ON runtime_profile DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_profile_commit()").await.unwrap();
    drop(db);
    assert_eq!(
        call(
            &f.app,
            "PATCH",
            &path,
            &jwt(f.owner, 3600),
            json!({"display_name":"Changed"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        f.stored(id).await,
        Some(("Deferred failure".into(), "omp".into()))
    );
    assert_eq!(f.observed.events.lock().unwrap().len(), 1);
    assert_eq!(f.observed.trace.lock().unwrap().len(), 2);
    let db = f.pool.get().await.unwrap();
    db.batch_execute("DROP TRIGGER profile_commit_fault ON runtime_profile")
        .await
        .unwrap();
    drop(db);
    assert_eq!(
        call(
            &f.app,
            "PATCH",
            &path,
            &jwt(f.owner, 3600),
            json!({"display_name":"Changed"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(f.stored(id).await, Some(("Changed".into(), "omp".into())));
    assert_eq!(f.observed.events.lock().unwrap().len(), 2);
    assert_eq!(f.observed.trace.lock().unwrap().len(), 4);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn concurrent_profile_delete_only_refreshes_and_publishes_once() {
    let f = Fixture::new().await;
    let (status, created) = f.create(f.owner, valid("Delete once")).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    let token = jwt(f.owner, 3600);
    let (a, b) = tokio::join!(
        call(&f.app, "DELETE", &path, &token, Value::Null),
        call(&f.app, "DELETE", &path, &token, Value::Null),
    );
    assert!([a.0, b.0].contains(&StatusCode::NO_CONTENT), "{a:?} {b:?}");
    assert!([a.0, b.0].contains(&StatusCode::NOT_FOUND), "{a:?} {b:?}");
    assert_eq!(f.observed.events.lock().unwrap().len(), 2);
    assert_eq!(f.observed.trace.lock().unwrap().len(), 4);
}

#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn request_decoder_matches_go_without_content_type_and_preserves_rows() {
    let f = Fixture::new().await;
    let send = |method: &str, path: String, raw: String| {
        let app = f.app.clone();
        let token = jwt(f.owner, 3600);
        let method = method.to_owned();
        async move {
            let response = app
                .oneshot(
                    Request::builder()
                        .method(method.as_str())
                        .uri(path)
                        .header("authorization", format!("Bearer {token}"))
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
    let (status, created) = send("POST", f.path(),
        r#"{"DISPLAY_NAME":" Raw ","display_name":null,"protocol_family":"pi","runtime_type":"omp","command_name":"omp","fixed_args":["-old"],"FIXED_ARGS":null,"enabled":false,"ENABLED":null} trailing"#.to_owned()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["display_name"], "Raw");
    assert_eq!(created["fixed_args"], json!([]));
    assert_eq!(created["enabled"], true);
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    let path = format!("{}/{}", f.path(), id);
    let (status, patched) = send("PATCH", path.clone(),
        r#"{"DISPLAY_NAME":"First","display_name":null,"fixed_args":["-new"],"FIXED_ARGS":null,"enabled":false,"ENABLED":null,"runtime_type":"omp","RUNTIME_TYPE":null} extra"#.to_owned()).await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["display_name"], "Raw");
    assert_eq!(patched["fixed_args"], json!([]));
    assert_eq!(patched["enabled"], true);
    assert_eq!(f.stored(id).await, Some(("Raw".into(), "omp".into())));
    for raw in [
        r#"{"display_name":9}"#,
        r#"{"display_name":"Invalid","protocol_family":"pi","command_name":"omp","fixed_args":[null]}"#,
    ] {
        assert_eq!(
            send("POST", f.path(), raw.to_owned()).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        send("PUT", path.clone(), r#"{"fixed_args":[null]}"#.to_owned())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let huge = format!(
        r#"{{"display_name":" Large ","protocol_family":"pi","command_name":"omp","ignored":"{}"}} extra"#,
        "a".repeat(2_200_000)
    );
    let (status, large) = send("POST", f.path(), huge).await;
    assert_eq!(status, StatusCode::CREATED, "{large}");
    assert_eq!(large["display_name"], "Large");
    assert_eq!(f.observed.events.lock().unwrap().len(), 3);
}
