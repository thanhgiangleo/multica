//! Workspace and membership publication against a disposable migrated database.
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

const SECRET: &[u8] = b"zbsi65-workspace-publication-synthetic-secret";
type Events = Arc<Mutex<Vec<(String, Value)>>>;
struct Recorder {
    pool: Arc<Pool>,
    events: Events,
    actor: Uuid,
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
        ws: Uuid,
        user: Uuid,
        member: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let db = self.pool.get().await.unwrap();
            assert!(
                db.query_opt(
                    "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&ws, &user],
                )
                .await
                .unwrap()
                .is_none(),
                "disconnect before commit",
            );
            self.events.lock().unwrap().push((
                "disconnect".into(),
                json!({"workspace_id":ws,"user_id":user,"member_id":member}),
            ));
        })
    }

    fn invalidate_membership<'a>(
        &'a self,
        ws: Uuid,
        user: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let db = self.pool.get().await.unwrap();
            let role = db
                .query_opt(
                    "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&ws, &user],
                )
                .await
                .unwrap()
                .map(|row| row.get::<_, String>(0));
            self.events.lock().unwrap().push((
                "invalidate".into(),
                json!({"workspace_id":ws,"user_id":user,"role":role}),
            ));
        })
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
            let db = self.pool.get().await.unwrap();
            if let Some(snapshot) = payload.get("workspace") {
                let name: String = db
                    .query_one(
                        "SELECT name FROM workspace WHERE id=$1",
                        &[&event.workspace_id],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(snapshot["name"], name);
            }
            if let Some(snapshot) = payload.get("member") {
                let id: Uuid = serde_json::from_value(snapshot["id"].clone()).unwrap();
                let role: String = db
                    .query_one("SELECT role FROM member WHERE id=$1", &[&id])
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(snapshot["role"], role);
            }
            assert_eq!(event.actor_user_id, self.actor);
            self.events.lock().unwrap().push((kind.into(), payload));
        })
    }
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
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app.clone().oneshot(request),
    )
    .await
    .expect("handler or post-commit callback held the only available DB connection")
    .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}
#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn workspace_and_role_publish_committed_scoped_effects() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(2)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let ws = Uuid::new_v4();
    let other = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let member = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    for id in [owner, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Synthetic',$2)",
            &[&id, &format!("{id}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for id in [ws, other] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Original',$2)",
            &[&id, &format!("workspace-{id}")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member')",
        &[&ws, &owner, &member],
    )
    .await
    .unwrap();
    let member_id: Uuid = db
        .query_one(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &member],
        )
        .await
        .unwrap()
        .get(0);
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_catalog_publisher(Arc::new(Recorder {
            pool,
            events: events.clone(),
            actor: owner,
        })),
    );
    let jwt = |id: Uuid, ttl| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":chrono::Utc::now().timestamp()+ttl}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    };
    let owner_jwt = jwt(owner, 3600);
    let outsider_jwt = jwt(outsider, 3600);
    let member_jwt = jwt(member, 3600);
    let path = format!("/api/workspaces/{ws}");
    let role_path = format!("{path}/members/{member_id}");
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &jwt(owner, -3600),
            json!({"name":"Denied"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &outsider_jwt,
            json!({"name":"Denied"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "PATCH", &path, &member_jwt, json!({"name":"Denied"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/workspaces/{other}"),
            &owner_jwt,
            json!({"name":"Denied"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &role_path,
            &member_jwt,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("{path}/members/invalid"),
            &owner_jwt,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let cookie = Request::builder()
        .method("PATCH")
        .uri(&path)
        .header("cookie", format!("multica_auth={owner_jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({"name":"Denied"}).to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(cookie).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert!(events.lock().unwrap().is_empty());
    db.batch_execute("CREATE FUNCTION pg_temp.reject_workspace_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit fault'; END $$; CREATE CONSTRAINT TRIGGER account_catalog_workspace_publication_fault AFTER UPDATE ON workspace DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_publication()").await.unwrap();
    assert_eq!(
        call(&app, "PATCH", &path, &owner_jwt, json!({"name":"Rollback"}))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("DROP TRIGGER account_catalog_workspace_publication_fault ON workspace; CREATE CONSTRAINT TRIGGER account_catalog_member_publication_fault AFTER UPDATE ON member DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_publication()").await.unwrap();
    assert_eq!(
        call(
            &app,
            "PATCH",
            &role_path,
            &owner_jwt,
            json!({"role":"admin"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    db.batch_execute("DROP TRIGGER account_catalog_member_publication_fault ON member")
        .await
        .unwrap();
    assert!(events.lock().unwrap().is_empty());
    let original: String = db
        .query_one("SELECT name FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap()
        .get(0);
    assert_eq!(original, "Original");
    let original: String = db
        .query_one("SELECT role FROM member WHERE id=$1", &[&member_id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(original, "member");
    let (status, role) = call(
        &app,
        "PATCH",
        &role_path,
        &owner_jwt,
        json!({"role":"admin"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{role}");
    let snapshot = events.lock().unwrap().clone();
    assert_eq!(
        snapshot
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["invalidate", "member:updated"]
    );
    assert_eq!(snapshot[0].1["user_id"], member.to_string());
    assert_eq!(snapshot[0].1["role"], "admin");
    assert_eq!(snapshot[1].1["member"], role);
    events.lock().unwrap().clear();
    let (status, updated) = call(
        &app,
        "PATCH",
        &path,
        &owner_jwt,
        json!({"name":"Renamed","settings":{"pr_merge_status":"none"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    let snapshot = events.lock().unwrap().clone();
    assert_eq!(
        snapshot
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["workspace:updated", "daemon"]
    );
    assert_eq!(snapshot[0].1["workspace"], updated);
    let mut notified: Vec<Uuid> = serde_json::from_value(snapshot[1].1.clone()).unwrap();
    notified.sort();
    let mut expected = vec![owner, member];
    expected.sort();
    assert_eq!(notified, expected);
    events.lock().unwrap().clear();
    assert_eq!(
        call(
            &app,
            "PATCH",
            &path,
            &owner_jwt,
            json!({"description":"Description"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        ["workspace:updated"]
    );
    events.lock().unwrap().clear();
    assert_eq!(
        call(&app, "PATCH", &path, &owner_jwt, json!({"name":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert!(events.lock().unwrap().is_empty());
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&other, &member],
    )
    .await
    .unwrap();
    let (status, _) = call(&app, "DELETE", &role_path, &outsider_jwt, Value::Null).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        &app,
        "DELETE",
        &format!("{path}/members/invalid"),
        &owner_jwt,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(events.lock().unwrap().is_empty());
    db.batch_execute("CREATE CONSTRAINT TRIGGER account_catalog_member_departure_fault AFTER DELETE ON member DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_workspace_publication()")
        .await
        .unwrap();
    let (status, _) = call(&app, "DELETE", &role_path, &owner_jwt, Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(events.lock().unwrap().is_empty());
    assert!(
        db.query_opt("SELECT id FROM member WHERE id=$1", &[&member_id])
            .await
            .unwrap()
            .is_some()
    );
    db.batch_execute("DROP TRIGGER account_catalog_member_departure_fault ON member")
        .await
        .unwrap();
    let (status, _) = call(&app, "DELETE", &role_path, &owner_jwt, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let snapshot = events.lock().unwrap().clone();
    assert_eq!(
        snapshot
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["disconnect", "invalidate", "member:removed", "daemon"]
    );
    let expected = json!({"member_id":member_id,"workspace_id":ws,"user_id":member});
    assert_eq!(snapshot[0].1, expected);
    assert_eq!(
        snapshot[1].1,
        json!({"workspace_id":ws,"user_id":member,"role":null})
    );
    assert_eq!(snapshot[2].1, expected);
    assert_eq!(snapshot[3].1, json!([member]));
    assert!(
        db.query_opt("SELECT id FROM member WHERE id=$1", &[&member_id])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.query_opt(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&other, &member]
        )
        .await
        .unwrap()
        .is_some()
    );
    events.lock().unwrap().clear();
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("{path}/leave"),
            &owner_jwt,
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST,
        "last owner cannot leave"
    );
    assert!(events.lock().unwrap().is_empty());
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
    let owner_member: Uuid = db
        .query_one(
            "SELECT id FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &owner],
        )
        .await
        .unwrap()
        .get(0);
    let (status, _) = call(
        &app,
        "POST",
        &format!("{path}/leave"),
        &owner_jwt,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let snapshot = events.lock().unwrap().clone();
    assert_eq!(
        snapshot
            .iter()
            .map(|(kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["disconnect", "invalidate", "member:removed", "daemon"]
    );
    let expected = json!({"member_id":owner_member,"workspace_id":ws,"user_id":owner});
    assert_eq!(snapshot[0].1, expected);
    assert_eq!(
        snapshot[1].1,
        json!({"workspace_id":ws,"user_id":owner,"role":null})
    );
    assert_eq!(snapshot[2].1, expected);
    assert_eq!(snapshot[3].1, json!([owner]));
    assert!(
        db.query_opt("SELECT id FROM member WHERE id=$1", &[&owner_member])
            .await
            .unwrap()
            .is_none()
    );
}
