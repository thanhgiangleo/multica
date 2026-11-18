//! Invitation metrics and asynchronous mail use committed data and fake adapters only.
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
        account_catalog_write_invitation_effects::{
            InvitationMail, InvitationMailer, InvitationMetric, InvitationMetrics,
        },
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi65-invitation-effects-synthetic-secret";
type Metrics = Arc<Mutex<Vec<(Uuid, Uuid, InvitationMetric)>>>;
struct MetricRecorder {
    pool: Arc<Pool>,
    events: Metrics,
}
impl InvitationMetrics for MetricRecorder {
    fn record<'a>(
        &'a self,
        user: Uuid,
        ws: Uuid,
        event: InvitationMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Tests hold one of two slots. A retained handler connection deadlocks here.
            let db = self.pool.get().await.unwrap();
            let committed: bool = match &event {
                InvitationMetric::Sent { .. } => db.query_one("SELECT EXISTS(SELECT 1 FROM workspace_invitation WHERE workspace_id=$1 AND inviter_id=$2 AND status='pending')", &[&ws,&user]).await.unwrap().get(0),
                InvitationMetric::Accepted { .. } => db.query_one("SELECT EXISTS(SELECT 1 FROM member m JOIN \"user\" u ON u.id=m.user_id WHERE m.workspace_id=$1 AND m.user_id=$2 AND u.onboarded_at IS NOT NULL)", &[&ws,&user]).await.unwrap().get(0),
            };
            assert!(committed, "metric preceded commit");
            self.events.lock().unwrap().push((user, ws, event));
        })
    }
}
struct MailRecorder {
    pool: Arc<Pool>,
    started: mpsc::UnboundedSender<InvitationMail>,
    finished: mpsc::UnboundedSender<()>,
    release: Arc<Semaphore>,
}
impl InvitationMailer for MailRecorder {
    fn send<'a>(
        &'a self,
        mail: InvitationMail,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + 'a>> {
        Box::pin(async move {
            {
                let db = self.pool.get().await.unwrap();
                let row = db
                    .query_one(
                        "SELECT invitee_email,status FROM workspace_invitation WHERE id=$1",
                        &[&mail.invitation_id],
                    )
                    .await
                    .unwrap();
                assert_eq!(mail.recipient, row.get::<_, String>(0));
                assert_eq!(row.get::<_, String>(1), "pending");
            }
            self.started.send(mail).unwrap();
            self.release.acquire().await.unwrap().forget();
            self.finished.send(()).unwrap();
            // Provider failure must not roll back committed invitation or turn HTTP into 500.
            Err(())
        })
    }
}
struct Fixture {
    pool: Arc<Pool>,
    state: ApiState,
    app: axum::Router,
    events: Metrics,
    ws: Uuid,
    other: Uuid,
    owner: Uuid,
    member: Uuid,
    invitee: Uuid,
    outsider: Uuid,
}
impl Fixture {
    async fn new() -> Self {
        let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
        assert!(
            cfg.get_hosts()
                .iter()
                .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
        );
        let pool = Arc::new(
            Pool::builder(Manager::new(cfg, NoTls))
                .max_size(2)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let (ws, other, owner, member, invitee, outsider) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        for id in [owner, member, invitee, outsider] {
            db.execute(
                "INSERT INTO \"user\"(id,name,email) VALUES($1,'Inviter <name>',$2)",
                &[&id, &format!("{id}@invalid.example")],
            )
            .await
            .unwrap();
        }
        for id in [ws, other] {
            db.execute(
                "INSERT INTO workspace(id,name,slug) VALUES($1,'Workspace <name>',$2)",
                &[&id, &format!("effects-{id}")],
            )
            .await
            .unwrap();
        }
        db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($3,$2,'owner'),($1,$4,'member')", &[&ws,&owner,&other,&member]).await.unwrap();
        drop(db);
        let events: Metrics = Arc::default();
        let recorder = Arc::new(MetricRecorder {
            pool: pool.clone(),
            events: events.clone(),
        });
        let state = ApiState::new(pool.clone(), SECRET.to_vec()).with_invitation_metrics(recorder);
        let app = app(&pool, state.clone());
        Self {
            pool,
            state,
            app,
            events,
            ws,
            other,
            owner,
            member,
            invitee,
            outsider,
        }
    }
    fn take(&self) -> Vec<(Uuid, Uuid, InvitationMetric)> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
    async fn seed(&self, ws: Uuid, user: Uuid) -> Uuid {
        self.pool.get().await.unwrap().query_one("INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,created_at,expires_at) VALUES($1,$2,$3,$4,'member',now()-interval '2 days 12 hours',now()+interval '1 day') RETURNING id", &[&ws,&self.owner,&format!("{user}@invalid.example"),&user]).await.unwrap().get(0)
    }
}
fn app(pool: &Arc<Pool>, state: ApiState) -> axum::Router {
    router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        state,
    )
}
fn token(user: Uuid, ttl: i64) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+ttl}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    user: Uuid,
    body: Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {}", token(user, 3600)))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = tokio::time::timeout(Duration::from_secs(10), app.clone().oneshot(request))
        .await
        .expect("handler held DB connection or awaited mail delivery")
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn mail_is_postcommit_nonblocking_and_failure_is_nonfatal() {
    let f = Fixture::new().await;
    let (started, mut received) = mpsc::unbounded_channel();
    let (finished, mut completed) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let mailer = Arc::new(MailRecorder {
        pool: f.pool.clone(),
        started,
        finished,
        release: release.clone(),
    });
    let app = app(&f.pool, f.state.clone().with_invitation_mailer(mailer));
    let db = f.pool.get().await.unwrap();
    let path = format!("/api/workspaces/{}/members", f.ws);
    let email = format!("{}@invalid.example", f.invitee);
    let (status, inv) = call(
        &app,
        "POST",
        &path,
        f.owner,
        json!({"email":format!("  {}  ",email.to_uppercase())}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{inv}");
    let mail = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mail,
        InvitationMail {
            recipient: email.clone(),
            inviter_name: "Inviter <name>".into(),
            workspace_name: "Workspace <name>".into(),
            invitation_id: serde_json::from_value(inv["id"].clone()).unwrap()
        }
    );
    assert!(
        completed.try_recv().is_err(),
        "mail delivery should still be blocked"
    );
    assert_eq!(
        f.take(),
        vec![(
            f.owner,
            f.ws,
            InvitationMetric::Sent {
                invited_email_domain: "invalid.example".into()
            }
        )]
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), completed.recv())
        .await
        .unwrap()
        .unwrap();
    let (status, _) = call(&app, "POST", &path, f.owner, json!({"email":email})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(received.try_recv().is_err());
    assert!(f.take().is_empty());
    assert_eq!(
        db.query_one(
            "SELECT status FROM workspace_invitation WHERE id=$1",
            &[&mail.invitation_id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    // No service configured is valid self-hosted behavior and still records metrics.
    assert_eq!(
        call(
            &f.app,
            "POST",
            &path,
            f.owner,
            json!({"email":"no-mail@invalid.example"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert_eq!(f.take().len(), 1);
    let body = json!({"email":"concurrent-mail@invalid.example"});
    let (one, two) = tokio::join!(
        call(&app, "POST", &path, f.owner, body.clone()),
        call(&app, "POST", &path, f.owner, body)
    );
    assert!(matches!(
        (one.0, two.0),
        (StatusCode::CREATED, StatusCode::CONFLICT) | (StatusCode::CONFLICT, StatusCode::CREATED)
    ));
    let mail = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mail.recipient, "concurrent-mail@invalid.example");
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), completed.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(received.try_recv().is_err());
    assert_eq!(f.take().len(), 1);
    // Go skips email when workspace-name enrichment is empty.
    db.execute("UPDATE workspace SET name='' WHERE id=$1", &[&f.ws])
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            f.owner,
            json!({"email":"empty-workspace@invalid.example"})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    assert!(received.try_recv().is_err());
    assert_eq!(f.take().len(), 1);
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn metrics_respect_scope_idempotency_and_whole_days() {
    let f = Fixture::new().await;
    let id = f.seed(f.ws, f.invitee).await;
    let path = format!("/api/invitations/{id}/accept");
    let create = format!("/api/workspaces/{}/members", f.ws);
    for (user, status) in [
        (f.member, StatusCode::FORBIDDEN),
        (f.outsider, StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            call(
                &f.app,
                "POST",
                &create,
                user,
                json!({"email":"denied@invalid.example"})
            )
            .await
            .0,
            status
        );
    }
    assert_eq!(
        call(&f.app, "POST", &path, f.outsider, Value::Null).await.0,
        StatusCode::FORBIDDEN
    );
    let cloud = app(
        &f.pool,
        f.state
            .clone()
            .with_cloud_url("https://example.invalid")
            .unwrap(),
    );
    assert_eq!(
        call(&cloud, "POST", &path, f.invitee, Value::Null).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            "/api/invitations/not-a-uuid/accept",
            f.invitee,
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let csrf = Request::builder()
        .method("POST")
        .uri(&create)
        .header("cookie", format!("multica_auth={}", token(f.owner, 3600)))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"email":"csrf@invalid.example"}).to_string(),
        ))
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(csrf).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert!(f.take().is_empty());
    let db = f.pool.get().await.unwrap();
    let (status, member) = call(&f.app, "POST", &path, f.invitee, Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        f.take(),
        vec![(
            f.invitee,
            f.ws,
            InvitationMetric::Accepted {
                days_since_invite: 2
            }
        )]
    );
    assert_eq!(
        call(&f.app, "POST", &path, f.invitee, Value::Null).await.1,
        member
    );
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{id}/decline"),
            f.invitee,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(f.take().is_empty());
    drop(db);
    let id = f.seed(f.other, f.invitee).await;
    f.pool
        .get()
        .await
        .unwrap()
        .execute(
            "UPDATE workspace_invitation SET created_at=now()-interval '23 hours' WHERE id=$1",
            &[&id],
        )
        .await
        .unwrap();
    let path = format!("/api/invitations/{id}/accept");
    let (one, two) = tokio::join!(
        call(&f.app, "POST", &path, f.invitee, Value::Null),
        call(&f.app, "POST", &path, f.invitee, Value::Null)
    );
    assert_eq!(one.0, StatusCode::OK);
    assert_eq!(two.0, StatusCode::OK);
    assert_eq!(
        f.take(),
        vec![(
            f.invitee,
            f.other,
            InvitationMetric::Accepted {
                days_since_invite: 0
            }
        )]
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn failed_commit_never_sends_mail_or_metrics() {
    let f = Fixture::new().await;
    let id = f.seed(f.ws, f.invitee).await;
    let (started, mut received) = mpsc::unbounded_channel();
    let (finished, mut completed) = mpsc::unbounded_channel();
    let mailer = Arc::new(MailRecorder {
        pool: f.pool.clone(),
        started,
        finished,
        release: Arc::new(Semaphore::new(10)),
    });
    let app = app(&f.pool, f.state.clone().with_invitation_mailer(mailer));
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION pg_temp.reject_invitation_effects() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit fault'; END $$; CREATE CONSTRAINT TRIGGER account_catalog_invitation_effects_fault AFTER INSERT OR UPDATE ON workspace_invitation DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_invitation_effects()").await.unwrap();
    for (path, user, body) in [
        (
            format!("/api/workspaces/{}/members", f.ws),
            f.owner,
            json!({"email":"rollback-mail@invalid.example"}),
        ),
        (
            format!("/api/invitations/{id}/accept"),
            f.invitee,
            Value::Null,
        ),
    ] {
        assert_eq!(
            call(&app, "POST", &path, user, body).await.0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(f.take().is_empty());
    }
    assert_eq!(
        db.query_one(
            "SELECT status FROM workspace_invitation WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    assert!(!db.query_one("SELECT EXISTS(SELECT 1 FROM workspace_invitation WHERE workspace_id=$1 AND invitee_email='rollback-mail@invalid.example')",&[&f.ws]).await.unwrap().get::<_,bool>(0));
    db.batch_execute(
        "DROP TRIGGER account_catalog_invitation_effects_fault ON workspace_invitation",
    )
    .await
    .unwrap();
    assert!(received.try_recv().is_err());
    assert!(completed.try_recv().is_err());
}
