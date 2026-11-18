//! Invitation publications use committed snapshots and the shared catalog adapter.
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
        account_catalog_write_catalog_publish::{CatalogPublication, CatalogPublisher},
        account_catalog_write_onboarding::{CompletionPath, OnboardingMetric, OnboardingMetrics},
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
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi65-invitation-publication-synthetic-secret";
type Events = Arc<Mutex<Vec<(String, Value)>>>;
struct Recorder {
    pool: Arc<Pool>,
    events: Events,
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
        Box::pin(async { panic!("invitations must not disconnect members") })
    }
    fn invalidate_membership<'a>(
        &'a self,
        _: Uuid,
        _: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("frozen invitation path does not invalidate membership cache") })
    }
    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.events
                .lock()
                .unwrap()
                .push(("daemon".into(), json!(users)));
        })
    }
    fn publish<'a>(
        &'a self,
        event: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let kind = event.change.event_type();
            let payload = serde_json::to_value(&event.change).unwrap();
            // This connection cannot be checked out if the handler still owns
            // its transaction's connection: the test holds the other pool slot.
            let db = self.pool.get().await.unwrap();
            if let Some(invitation) = payload.get("invitation") {
                let id: Uuid = serde_json::from_value(invitation["id"].clone()).unwrap();
                let row = db.query_one("SELECT workspace_id,invitee_email,status FROM workspace_invitation WHERE id=$1", &[&id]).await.unwrap();
                assert_eq!(row.get::<_, Uuid>(0), event.workspace_id);
                assert_eq!(invitation["invitee_email"], row.get::<_, String>(1));
                assert_eq!(invitation["status"], row.get::<_, String>(2));
            }
            if let Some(member) = payload.get("member") {
                let id: Uuid = serde_json::from_value(member["id"].clone()).unwrap();
                let row = db
                    .query_one(
                        "SELECT workspace_id,user_id,role FROM member WHERE id=$1",
                        &[&id],
                    )
                    .await
                    .unwrap();
                assert_eq!(row.get::<_, Uuid>(0), event.workspace_id);
                assert_eq!(member["user_id"], row.get::<_, Uuid>(1).to_string());
                assert_eq!(member["role"], row.get::<_, String>(2));
            }
            if let Some(id) = payload.get("invitation_id") {
                let id: Uuid = serde_json::from_value(id.clone()).unwrap();
                let row = db
                    .query_opt(
                        "SELECT status FROM workspace_invitation WHERE id=$1",
                        &[&id],
                    )
                    .await
                    .unwrap();
                match kind {
                    "invitation:revoked" => assert!(row.is_none()),
                    "invitation:accepted" => {
                        assert_eq!(row.unwrap().get::<_, String>(0), "accepted")
                    }
                    "invitation:declined" => {
                        assert_eq!(row.unwrap().get::<_, String>(0), "declined")
                    }
                    _ => panic!("unexpected invitation event"),
                }
            }
            self.events.lock().unwrap().push((kind.into(), json!({
                "workspace_id":event.workspace_id,"actor_user_id":event.actor_user_id,"payload":payload
            })));
        })
    }
}
impl OnboardingMetrics for Recorder {
    fn record<'a>(
        &'a self,
        event: OnboardingMetric,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(
                event,
                OnboardingMetric::Completed {
                    path: CompletionPath::InviteAccept
                }
            );
            let user: Uuid = {
                let events = self.events.lock().unwrap();
                let accepted = events
                    .iter()
                    .rev()
                    .find(|(name, _)| name == "invitation:accepted")
                    .unwrap();
                serde_json::from_value(accepted.1["payload"]["member"]["user_id"].clone()).unwrap()
            };
            let db = self.pool.get().await.unwrap();
            let timestamp: Option<chrono::DateTime<chrono::Utc>> = db
                .query_one("SELECT onboarded_at FROM \"user\" WHERE id=$1", &[&user])
                .await
                .unwrap()
                .get(0);
            assert!(
                timestamp.is_some(),
                "counter sees committed onboarding timestamp"
            );
            self.events.lock().unwrap().push((
                "onboarding".into(),
                json!({"counter": event.counter_name(), "labels": event.labels()}),
            ));
        })
    }
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
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app.clone().oneshot(request),
    )
    .await
    .expect("post-commit callback held DB connection")
    .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}
struct Fixture {
    app: axum::Router,
    cloud: axum::Router,
    pool: Arc<Pool>,
    events: Events,
    ws: Uuid,
    other: Uuid,
    owner: Uuid,
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
        let (ws, other, owner, invitee, outsider) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        for id in [owner, invitee, outsider] {
            db.execute(
                "INSERT INTO \"user\"(id,name,email) VALUES($1,'Synthetic',$2)",
                &[&id, &format!("{id}@invalid.example")],
            )
            .await
            .unwrap();
        }
        for id in [ws, other] {
            db.execute(
                "INSERT INTO workspace(id,name,slug) VALUES($1,'Invitations',$2)",
                &[&id, &format!("invitations-{id}")],
            )
            .await
            .unwrap();
        }
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($3,$2,'owner')",
            &[&ws, &owner, &other],
        )
        .await
        .unwrap();
        let events: Events = Arc::default();
        let recorder = Arc::new(Recorder {
            pool: pool.clone(),
            events: events.clone(),
        });
        let state = ApiState::new(pool.clone(), SECRET.to_vec())
            .with_catalog_publisher(recorder.clone())
            .with_onboarding_metrics(recorder);
        let health = Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        ));
        Self {
            app: router_with_api(health.clone(), state.clone()),
            cloud: router_with_api(
                health,
                state.with_cloud_url("https://example.invalid").unwrap(),
            ),
            pool,
            events,
            ws,
            other,
            owner,
            invitee,
            outsider,
        }
    }
    fn take(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
    async fn invitation(&self, ws: Uuid, user: Uuid) -> Uuid {
        self.pool.get().await.unwrap().query_one(
            "INSERT INTO workspace_invitation(workspace_id,inviter_id,invitee_email,invitee_user_id,role,expires_at) VALUES($1,$2,$3,$4,'member',now()+interval '1 day') RETURNING id",
            &[&ws,&self.owner,&format!("{user}@invalid.example"),&user]).await.unwrap().get(0)
    }
}
fn names(events: &[(String, Value)]) -> Vec<&str> {
    events.iter().map(|(kind, _)| kind.as_str()).collect()
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn invitation_publications_are_scoped_committed_and_idempotent() {
    let f = Fixture::new().await;
    let db = f.pool.get().await.unwrap();
    let create = format!("/api/workspaces/{}/members", f.ws);
    let email = format!("{}@invalid.example", f.invitee);
    assert_eq!(
        call(&f.app, "POST", &create, f.outsider, json!({"email":email}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&f.cloud, "POST", &create, f.owner, json!({"email":email}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let cookie = Request::builder()
        .method("POST")
        .uri(&create)
        .header("cookie", format!("multica_auth={}", token(f.owner, 3600)))
        .header("content-type", "application/json")
        .body(Body::from(json!({"email":email}).to_string()))
        .unwrap();
    assert_eq!(
        f.app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert!(f.take().is_empty());
    let (status, inv) = call(
        &f.app,
        "POST",
        &create,
        f.owner,
        json!({"email":email.to_uppercase(),"role":"admin"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{inv}");
    let events = f.take();
    assert_eq!(names(&events), ["invitation:created"]);
    assert_eq!(
        events[0].1,
        json!({"workspace_id":f.ws,"actor_user_id":f.owner,"payload":{"invitation":inv,"workspace_name":"Invitations"}})
    );
    assert_eq!(
        call(&f.app, "POST", &create, f.owner, json!({"email":email}))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert!(f.take().is_empty());
    let id: Uuid = serde_json::from_value(inv["id"].clone()).unwrap();
    let accept = format!("/api/invitations/{id}/accept");
    assert_eq!(
        call(&f.app, "POST", &accept, f.outsider, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&f.cloud, "POST", &accept, f.invitee, Value::Null)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.take().is_empty());
    db.execute(
        "UPDATE \"user\" SET cloud_waitlist_email=$2 WHERE id=$1",
        &[&f.invitee, &email],
    )
    .await
    .unwrap();
    let (status, member) = call(&f.app, "POST", &accept, f.invitee, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{member}");
    let events = f.take();
    assert_eq!(
        names(&events),
        [
            "member:added",
            "invitation:accepted",
            "daemon",
            "onboarding"
        ]
    );
    assert_eq!(
        events[0].1,
        json!({"workspace_id":f.ws,"actor_user_id":f.invitee,"payload":{"member":member,"workspace_name":"Invitations"}})
    );
    assert_eq!(
        events[1].1,
        json!({"workspace_id":f.ws,"actor_user_id":f.invitee,"payload":{"invitation_id":id,"member":member}})
    );
    assert_eq!(events[2].1, json!([f.invitee]));
    assert_eq!(
        events[3].1,
        json!({
            "counter":"multica_onboarding_completed_total",
            "labels":[["completion_path","invite_accept"]]
        })
    );
    let row = db
        .query_one(
            "SELECT cloud_waitlist_email,onboarded_at FROM \"user\" WHERE id=$1",
            &[&f.invitee],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), email);
    assert!(
        row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
            .is_some()
    );
    assert_eq!(
        call(&f.app, "POST", &accept, f.invitee, Value::Null)
            .await
            .1,
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
    let second = f.invitation(f.other, f.invitee).await;
    assert_eq!(
        call(
            &f.app,
            "POST",
            &format!("/api/invitations/{second}/accept"),
            f.invitee,
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        names(&f.take()),
        ["member:added", "invitation:accepted", "daemon"]
    );
    let declined = f.invitation(f.ws, f.outsider).await;
    let path = format!("/api/invitations/{declined}/decline");
    let (a, b) = tokio::join!(
        call(&f.app, "POST", &path, f.outsider, Value::Null),
        call(&f.app, "POST", &path, f.outsider, Value::Null)
    );
    assert_eq!(a.0, StatusCode::NO_CONTENT);
    assert_eq!(b.0, StatusCode::NO_CONTENT);
    let events = f.take();
    assert_eq!(names(&events), ["invitation:declined"]);
    assert_eq!(
        events[0].1,
        json!({"workspace_id":f.ws,"actor_user_id":f.outsider,"payload":{"invitation_id":declined,"invitee_email":format!("{}@invalid.example",f.outsider)}})
    );
    let revoked = f.invitation(f.ws, f.outsider).await;
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!("/api/workspaces/{}/invitations/{revoked}", f.other),
            f.owner,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(f.take().is_empty());
    let path = format!("/api/workspaces/{}/invitations/{revoked}", f.ws);
    let (a, b) = tokio::join!(
        call(&f.app, "DELETE", &path, f.owner, Value::Null),
        call(&f.app, "DELETE", &path, f.owner, Value::Null)
    );
    assert!(matches!(
        (a.0, b.0),
        (StatusCode::NO_CONTENT, StatusCode::NOT_FOUND)
            | (StatusCode::NOT_FOUND, StatusCode::NO_CONTENT)
    ));
    let events = f.take();
    assert_eq!(names(&events), ["invitation:revoked"]);
    assert_eq!(
        events[0].1,
        json!({"workspace_id":f.ws,"actor_user_id":f.owner,"payload":{"invitation_id":revoked,"invitee_email":format!("{}@invalid.example",f.outsider),"invitee_user_id":f.outsider}})
    );
    // Unknown recipient: JSON null must remain present on revoke.
    let (status, unknown) = call(
        &f.app,
        "POST",
        &create,
        f.owner,
        json!({"email":"unknown@invalid.example"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    f.take();
    assert_eq!(
        call(
            &f.app,
            "DELETE",
            &format!(
                "/api/workspaces/{}/invitations/{}",
                f.ws,
                unknown["id"].as_str().unwrap()
            ),
            f.owner,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let events = f.take();
    assert!(
        events[0].1["payload"]
            .get("invitee_user_id")
            .unwrap()
            .is_null()
    );
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn failed_commit_emits_nothing_and_concurrent_accept_completes_once() {
    let f = Fixture::new().await;
    let id = f.invitation(f.ws, f.invitee).await;
    let db = f.pool.get().await.unwrap();
    db.batch_execute("CREATE FUNCTION pg_temp.reject_invitation_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit fault'; END $$; CREATE CONSTRAINT TRIGGER account_catalog_invitation_publication_fault AFTER INSERT OR UPDATE OR DELETE ON workspace_invitation DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_invitation_publication()").await.unwrap();
    for (method, path, user, body) in [
        (
            "POST",
            format!("/api/workspaces/{}/members", f.ws),
            f.owner,
            json!({"email":"rollback@invalid.example"}),
        ),
        (
            "POST",
            format!("/api/invitations/{id}/accept"),
            f.invitee,
            Value::Null,
        ),
        (
            "POST",
            format!("/api/invitations/{id}/decline"),
            f.invitee,
            Value::Null,
        ),
        (
            "DELETE",
            format!("/api/workspaces/{}/invitations/{id}", f.ws),
            f.owner,
            Value::Null,
        ),
    ] {
        assert_eq!(
            call(&f.app, method, &path, user, body).await.0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(f.take().is_empty(), "failed commit published callbacks");
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
        assert!(
            !db.query_one(
                "SELECT EXISTS(SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2)",
                &[&f.ws, &f.invitee]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
        assert!(
            db.query_one(
                "SELECT onboarded_at FROM \"user\" WHERE id=$1",
                &[&f.invitee]
            )
            .await
            .unwrap()
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
            .is_none()
        );
    }
    db.batch_execute(
        "DROP TRIGGER account_catalog_invitation_publication_fault ON workspace_invitation",
    )
    .await
    .unwrap();
    drop(db);
    let second = f.invitation(f.other, f.invitee).await;
    let a = format!("/api/invitations/{id}/accept");
    let b = format!("/api/invitations/{second}/accept");
    let (one, two) = tokio::join!(
        call(&f.app, "POST", &a, f.invitee, Value::Null),
        call(&f.app, "POST", &b, f.invitee, Value::Null)
    );
    assert_eq!(one.0, StatusCode::OK, "{}", one.1);
    assert_eq!(two.0, StatusCode::OK, "{}", two.1);
    let events = f.take();
    for (kind, count) in [
        ("member:added", 2),
        ("invitation:accepted", 2),
        ("daemon", 2),
        ("onboarding", 1),
    ] {
        assert_eq!(
            events.iter().filter(|(name, _)| name == kind).count(),
            count
        );
    }
    assert_eq!(
        call(&f.app, "POST", &a, f.invitee, Value::Null).await.1,
        one.1
    );
    assert!(f.take().is_empty());
}
