//! Disposable PostgreSQL checks for scoped personal pin mutations.
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
        queries_sprint_pins::{PinEvent, PinEventKind, PinEventSink},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Default)]
struct Events(Mutex<Vec<PinEvent>>);
impl PinEventSink for Events {
    fn publish(&self, event: PinEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"queries-sprint-pin-test")).unwrap()
}

fn app(pool: Arc<Pool>, events: Option<Arc<Events>>) -> axum::Router {
    let state = ApiState::new(pool.clone(), b"queries-sprint-pin-test".to_vec());
    let state = if let Some(events) = events {
        state.with_pin_event_sink(events)
    } else {
        state
    };
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "queries-sprint-pins".into(),
        )),
        state,
    )
}

async fn request(
    app: &axum::Router,
    ws: Uuid,
    user: Option<Uuid>,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(format!("/api/pins/{path}?workspace_id={ws}"))
        .header("content-type", "application/json");
    if let Some(user) = user {
        req = req.header("authorization", format!("Bearer {}", jwt(user)));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn pin_writes_validate_workspace_privacy_order_concurrency_and_rollback() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(ip) if ip=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = Vec::new();
    for name in ["owner", "peer", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("queries-pin-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Pin Workspace',$1) RETURNING id",
            &[&format!("pin-ws-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign Workspace',$1) RETURNING id",
            &[&format!("pin-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Pin issue','member',$2,'todo','medium',101) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let foreign:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Other pin issue','member',$2,'todo','medium',101) RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES ($1,'Pin project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let private_view:Uuid=db.query_one("INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,visibility,query) VALUES ($1,$2,'Private','workspace','private','{}'::jsonb) RETURNING id", &[&ws,&users[1]]).await.unwrap().get(0);
    let shared_view:Uuid=db.query_one("INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,visibility,query) VALUES ($1,$2,'Shared','workspace','workspace','{}'::jsonb) RETURNING id", &[&ws,&users[1]]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let router = app(pool.clone(), Some(events.clone()));
    let create = |kind: &str, id: Uuid| json!({"item_type":kind,"item_id":id});
    assert_eq!(
        request(&router, ws, None, "POST", "", create("issue", issue))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[2]),
            "POST",
            "",
            create("issue", issue)
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for (kind, id) in [("issue", foreign), ("view", private_view)] {
        assert_eq!(
            request(&router, ws, Some(users[0]), "POST", "", create(kind, id))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "POST",
            "",
            json!({"item_type":"file","item_id":issue})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "POST",
            "",
            json!({"item_type":"issue","item_id":"bad"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let without_sink = app(pool.clone(), None);
    assert_eq!(
        request(
            &without_sink,
            ws,
            Some(users[0]),
            "POST",
            "",
            create("issue", issue)
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM pinned_item WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );

    let (a, b) = tokio::join!(
        request(
            &router,
            ws,
            Some(users[0]),
            "POST",
            "",
            create("issue", issue)
        ),
        request(
            &router,
            ws,
            Some(users[0]),
            "POST",
            "",
            create("project", project)
        )
    );
    assert_eq!((a.0, b.0), (StatusCode::CREATED, StatusCode::CREATED));
    assert_ne!(a.1["position"], b.1["position"]);
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "POST",
            "",
            create("issue", issue)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let (status, view) = request(
        &router,
        ws,
        Some(users[0]),
        "POST",
        "",
        create("view", shared_view),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(view["position"], 3.0);
    assert_eq!(view["workspace_id"], ws.to_string());
    assert!(view["created_at"].as_str().unwrap().contains('T'));
    let pin_a = Uuid::parse_str(a.1["id"].as_str().unwrap()).unwrap();
    let pin_b = Uuid::parse_str(b.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(events.0.lock().unwrap().len(), 3);
    assert_eq!(events.0.lock().unwrap()[0].kind.wire_name(), "pin:created");
    assert_eq!(events.0.lock().unwrap()[0].user_id, users[0]);

    let reorder = json!({"items":[{"id":pin_a,"position":4.0},{"id":pin_b,"position":5.0}]});
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "PUT",
            "reorder",
            reorder.clone()
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let before = events.0.lock().unwrap().len();
    db.batch_execute("CREATE FUNCTION queries_sprint_pin_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.position=99 THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER queries_sprint_pin_rollback BEFORE UPDATE ON pinned_item FOR EACH ROW EXECUTE FUNCTION queries_sprint_pin_fail();").await.unwrap();
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "PUT",
            "reorder",
            json!({"items":[{"id":pin_a,"position":50.0},{"id":pin_b,"position":99.0}]})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.0.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one("SELECT position FROM pinned_item WHERE id=$1", &[&pin_a])
            .await
            .unwrap()
            .get::<_, f64>(0),
        4.0
    );
    db.batch_execute("DROP TRIGGER queries_sprint_pin_rollback ON pinned_item; DROP FUNCTION queries_sprint_pin_fail();").await.unwrap();
    let peer_pin = request(
        &router,
        ws,
        Some(users[1]),
        "POST",
        "",
        create("issue", issue),
    )
    .await
    .1;
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "PUT",
            "reorder",
            json!({"items":[{"id":peer_pin["id"],"position":33.0}]})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT position FROM pinned_item WHERE id=$1",
            &[&Uuid::parse_str(peer_pin["id"].as_str().unwrap()).unwrap()]
        )
        .await
        .unwrap()
        .get::<_, f64>(0),
        1.0
    );
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "DELETE",
            &format!("issue/{issue}"),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(
            &router,
            ws,
            Some(users[0]),
            "DELETE",
            &format!("issue/{issue}"),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM pinned_item WHERE user_id=$1 AND item_id=$2",
            &[&users[1], &issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let messages = events.0.lock().unwrap();
    assert!(
        messages
            .iter()
            .any(|event| event.kind == PinEventKind::Reordered && event.payload == reorder)
    );
    assert_eq!(messages.last().unwrap().kind, PinEventKind::Deleted);
}
