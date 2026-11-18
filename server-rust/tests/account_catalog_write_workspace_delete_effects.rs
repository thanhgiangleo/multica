//! Workspace deletion evicts cache before teardown, then publishes after commit.
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
    time::Duration,
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi65-workspace-delete-effects-synthetic";
type Trace = Arc<Mutex<Vec<(String, Value)>>>;
struct Recorder {
    pool: Arc<Pool>,
    trace: Trace,
    workspace: Uuid,
    demote: Option<Uuid>,
    barrier: Option<(Uuid, Arc<tokio::sync::Barrier>)>,
}
impl CatalogPublisher for Recorder {
    fn notify_runtime_profiles_changed<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("non-profile mutation must not refresh runtime profiles") })
    }

    fn disconnect_member<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("workspace delete must not emit member removals") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        ws: Uuid,
        user: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(ws, self.workspace);
            let db = self.pool.get().await.unwrap();
            let member: bool = db
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2)",
                    &[&ws, &user],
                )
                .await
                .unwrap()
                .get(0);
            assert!(member, "eviction must precede deletion");
            self.trace
                .lock()
                .unwrap()
                .push(("evict".into(), json!(user)));
            if self.demote == Some(user) {
                db.execute(
                    "UPDATE member SET role='admin' WHERE workspace_id=$1 AND user_id=$2",
                    &[&ws, &user],
                )
                .await
                .unwrap();
            }
            drop(db);
            if let Some((last_user, barrier)) = &self.barrier
                && user == *last_user
            {
                barrier.wait().await;
            }
        })
    }
    fn publish<'a>(
        &'a self,
        publication: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(publication.workspace_id, self.workspace);
            let db = self.pool.get().await.unwrap();
            let exists: bool = db
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM workspace WHERE id=$1)",
                    &[&publication.workspace_id],
                )
                .await
                .unwrap()
                .get(0);
            assert!(!exists, "publication must observe committed deletion");
            assert_eq!(publication.change.event_type(), "workspace:deleted");
            let CatalogChange::WorkspaceDeleted { workspace_id } = publication.change else {
                panic!("wrong publication")
            };
            assert_eq!(workspace_id, self.workspace);
            self.trace.lock().unwrap().push((
                "workspace:deleted".into(),
                json!({"workspace_id":workspace_id,"actor_user_id":publication.actor_user_id}),
            ));
        })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let db = self.pool.get().await.unwrap();
            let exists: bool = db
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM workspace WHERE id=$1)",
                    &[&self.workspace],
                )
                .await
                .unwrap()
                .get(0);
            assert!(!exists, "daemon refresh must observe committed deletion");
            self.trace
                .lock()
                .unwrap()
                .push(("daemon".into(), json!(users)));
        })
    }
}
struct Fixture {
    pool: Arc<Pool>,
    ws: Uuid,
    neighbour: Uuid,
    owner: Uuid,
    other: Uuid,
    outsider: Uuid,
    trace: Trace,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_pool_size(1).await
    }
    async fn with_pool_size(size: usize) -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|host| matches!(host, tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(size)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let mut users = Vec::new();
        for name in ["Owner", "Other", "Outsider"] {
            users.push(
                db.query_one(
                    "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                    &[
                        &name,
                        &format!("effects-{}@invalid.example", Uuid::new_v4()),
                    ],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [owner, other, outsider] = <[Uuid; 3]>::try_from(users).unwrap();
        let mut workspaces = Vec::new();
        for name in ["Delete", "Neighbour"] {
            workspaces.push(
                db.query_one(
                    "INSERT INTO workspace(name,slug) VALUES($1,$2) RETURNING id",
                    &[&name, &format!("effects-{}", Uuid::new_v4())],
                )
                .await
                .unwrap()
                .get::<_, Uuid>(0),
            );
        }
        let [ws, neighbour] = <[Uuid; 2]>::try_from(workspaces).unwrap();
        db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'admin'),($4,$2,'owner')", &[&ws,&owner,&other,&neighbour]).await.unwrap();
        drop(db);
        Self {
            pool,
            ws,
            neighbour,
            owner,
            other,
            outsider,
            trace: Arc::default(),
        }
    }
    fn app(&self, demote: Option<Uuid>, adapter: bool) -> axum::Router {
        self.app_with(demote, adapter, None)
    }
    fn app_with(
        &self,
        demote: Option<Uuid>,
        adapter: bool,
        barrier: Option<(Uuid, Arc<tokio::sync::Barrier>)>,
    ) -> axum::Router {
        let mut state = ApiState::new(self.pool.clone(), SECRET.to_vec());
        if adapter {
            state = state.with_catalog_publisher(Arc::new(Recorder {
                pool: self.pool.clone(),
                trace: self.trace.clone(),
                workspace: self.ws,
                demote,
                barrier,
            }));
        }
        router_with_api(
            Arc::new(Health::new(
                Some(self.pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        )
    }
    fn path(&self) -> String {
        format!("/api/workspaces/{}", self.ws)
    }
    fn events(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.trace.lock().unwrap())
    }
    async fn exists(&self) -> bool {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM workspace WHERE id=$1)",
                &[&self.ws],
            )
            .await
            .unwrap()
            .get(0)
    }
}
fn jwt(user: Uuid, expiry: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+expiry}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
async fn call(app: &axum::Router, path: &str, token: Option<&str>, cookie: bool) -> StatusCode {
    let mut req = Request::builder().method("DELETE").uri(path);
    if let Some(token) = token {
        req = if cookie {
            req.header("cookie", format!("multica_auth={token}"))
        } else {
            req.header("authorization", format!("Bearer {token}"))
        };
    }
    let response = tokio::time::timeout(
        Duration::from_secs(12),
        app.clone().oneshot(req.body(Body::empty()).unwrap()),
    )
    .await
    .expect("callback retained DB connection")
    .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    if status == StatusCode::NO_CONTENT {
        assert!(body.is_empty());
    }
    status
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn deletion_eviction_publication_refresh_order_and_scope() {
    let f = Fixture::new().await;
    let app = f.app(None, true);
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::NO_CONTENT
    );
    assert!(!f.exists().await);
    let mut users = vec![f.owner, f.other];
    users.sort();
    assert_eq!(
        f.events(),
        vec![
            ("evict".into(), json!(users[0])),
            ("evict".into(), json!(users[1])),
            (
                "workspace:deleted".into(),
                json!({"workspace_id":f.ws,"actor_user_id":f.owner})
            ),
            ("daemon".into(), json!(users)),
        ]
    );
    let db = f.pool.get().await.unwrap();
    let exists: bool = db
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE id=$1)",
            &[&f.neighbour],
        )
        .await
        .unwrap()
        .get(0);
    assert!(exists, "cross-workspace delete");
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn rejection_does_not_evict_or_publish() {
    let f = Fixture::new().await;
    let app = f.app(None, true);
    for (token, cookie, expected) in [
        (None, false, StatusCode::UNAUTHORIZED),
        (Some(jwt(f.owner, -300)), false, StatusCode::UNAUTHORIZED),
        (Some(jwt(f.outsider, 3600)), false, StatusCode::NOT_FOUND),
        (Some(jwt(f.other, 3600)), false, StatusCode::FORBIDDEN),
        (Some(jwt(f.owner, 3600)), true, StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            call(&app, &f.path(), token.as_deref(), cookie).await,
            expected
        );
        assert!(f.events().is_empty());
    }
    assert!(f.exists().await);
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn rollback_keeps_workspace_without_postcommit_effects() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION pg_temp.reject_workspace_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic workspace fault'; END $$; CREATE TRIGGER effects_delete_fault BEFORE DELETE ON workspace FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_delete()").await.unwrap();
    drop(db);
    let app = f.app(None, true);
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.exists().await);
    assert!(
        f.events().iter().all(|(kind, _)| kind == "evict"),
        "rollback must not publish or notify daemons"
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("DROP TRIGGER effects_delete_fault ON workspace")
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn commit_failure_emits_no_postcommit_effects_then_retry_emits_once() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION pg_temp.reject_workspace_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit fault'; END $$; CREATE CONSTRAINT TRIGGER effects_commit_fault AFTER DELETE ON workspace DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_commit()").await.unwrap();
    drop(db);
    let app = f.app(None, true);
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(f.exists().await);
    assert_eq!(
        f.events()
            .iter()
            .filter(|(kind, _)| kind == "evict")
            .count(),
        2
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("DROP TRIGGER effects_commit_fault ON workspace")
        .await
        .unwrap();
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        f.events()
            .iter()
            .filter(|(kind, _)| kind == "workspace:deleted")
            .count(),
        1
    );
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn concurrent_deletes_publish_once() {
    let f = Fixture::with_pool_size(4).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let last = std::cmp::max(f.owner, f.other);
    let app = f.app_with(None, true, Some((last, barrier)));
    let token = jwt(f.owner, 3600);
    let path = f.path();
    let (a, b) = tokio::join!(
        call(&app, &path, Some(&token), false),
        call(&app, &path, Some(&token), false)
    );
    assert!(
        matches!(
            (a, b),
            (StatusCode::NO_CONTENT, StatusCode::NOT_FOUND)
                | (StatusCode::NOT_FOUND, StatusCode::NO_CONTENT)
        ),
        "concurrent delete status {a} {b}"
    );
    assert!(!f.exists().await);
    let events = f.events();
    assert_eq!(
        events
            .iter()
            .filter(|(kind, _)| kind == "workspace:deleted")
            .count(),
        1
    );
    assert_eq!(
        events.iter().filter(|(kind, _)| kind == "daemon").count(),
        1
    );
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn demotion_during_predelete_eviction_blocks_teardown() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    db.execute(
        "UPDATE member SET role='owner' WHERE workspace_id=$1 AND user_id=$2",
        &[&f.ws, &f.other],
    )
    .await
    .unwrap();
    drop(db);
    let app = f.app(Some(f.owner), true);
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::FORBIDDEN
    );
    assert!(f.exists().await);
    assert!(f.events().iter().all(|(kind, _)| kind == "evict"));
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn personal_access_token_expiry_revocation_and_active_actor() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let token = format!("mul_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix,revoked) VALUES($1,'synthetic',$2,'mul_synthetic',true)", &[&f.owner,&hash]).await.unwrap();
    drop(db);
    let app = f.app(None, true);
    assert_eq!(
        call(&app, &f.path(), Some(&token), false).await,
        StatusCode::UNAUTHORIZED
    );
    assert!(f.events().is_empty());
    f.pool.get().await.unwrap().execute("UPDATE personal_access_token SET revoked=false,expires_at=now()-interval '1 hour' WHERE token_hash=$1", &[&hash]).await.unwrap();
    assert_eq!(
        call(&app, &f.path(), Some(&token), false).await,
        StatusCode::UNAUTHORIZED
    );
    assert!(f.events().is_empty());
    f.pool.get().await.unwrap().execute("UPDATE personal_access_token SET expires_at=now()+interval '1 hour' WHERE token_hash=$1", &[&hash]).await.unwrap();
    assert_eq!(
        call(&app, &f.path(), Some(&token), false).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        f.events()
            .iter()
            .filter(|(kind, _)| kind == "workspace:deleted")
            .count(),
        1
    );
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn task_actor_never_eviction_or_delete() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic agent','cloud',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic runtime','cloud','synthetic','online') RETURNING id", &[&f.ws]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES($1,'Synthetic issue','member',$2) RETURNING id", &[&f.ws,&f.owner]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,runtime_id,status) VALUES($1,$2,$3,'running') RETURNING id", &[&agent,&issue,&runtime]).await.unwrap().get(0);
    let token = format!("mat_delete_effects_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&f.ws,&f.owner]).await.unwrap();
    drop(db);
    let app = f.app(None, true);
    assert_eq!(
        call(&app, &f.path(), Some(&token), false).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            &format!("/api/workspaces/{}", f.neighbour),
            Some(&token),
            false
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert!(f.events().is_empty());
    assert!(f.exists().await);
}
#[tokio::test]
#[ignore = "requires disposable loopback multica_zbsi65 database"]
async fn missing_adapter_keeps_selfhost_delete_available_but_is_partial() {
    let f = Fixture::new().await;
    let app = f.app(None, false);
    assert_eq!(
        call(&app, &f.path(), Some(&jwt(f.owner, 3600)), false).await,
        StatusCode::NO_CONTENT
    );
    assert!(!f.exists().await);
}
