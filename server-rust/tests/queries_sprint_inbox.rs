//! Disposable PostgreSQL checks for recipient-scoped inbox writes and event contracts.
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
        queries_sprint_inbox::{InboxEvent, InboxEventKind, InboxEventSink},
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
struct Events(Mutex<Vec<InboxEvent>>);
impl InboxEventSink for Events {
    fn publish(&self, event: InboxEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"queries-sprint-inbox-test")).unwrap()
}

async fn post(
    app: &axum::Router,
    ws: Uuid,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/api/inbox/{path}?workspace_id={ws}"));
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    (
        status,
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap(),
    )
}

fn app(pool: Arc<Pool>, events: Option<Arc<Events>>) -> axum::Router {
    let state = ApiState::new(pool.clone(), b"queries-sprint-inbox-test".to_vec());
    let state = if let Some(events) = events {
        state.with_inbox_event_sink(events)
    } else {
        state
    };
    router_with_api(
        Arc::new(Health::new(
            Some(pool),
            MigrationCatalog::embedded().unwrap(),
            "queries-sprint-test".into(),
        )),
        state,
    )
}

#[tokio::test]
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn inbox_mutations_scope_group_counts_events_and_rollback() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(6)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = vec![];
    for name in ["owner", "peer", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("queries-sprint-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Queries Inbox',$1) RETURNING id",
            &[&format!("queries-inbox-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Other Queries Inbox',$1) RETURNING id",
            &[&format!("queries-inbox-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')",&[&ws,&users[0],&users[1],&other]).await.unwrap();
    let mut issues = vec![];
    for (n, status) in [
        (1i32, "in_progress"),
        (2, "done"),
        (3, "shipped"),
        (4, "abandoned"),
    ] {
        issues.push(db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,priority,number) VALUES ($1,'Synthetic inbox issue','member',$2,$3,'high',$4) RETURNING id",&[&ws,&users[0],&status,&n]).await.unwrap().get::<_,Uuid>(0));
    }
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,archived_at) VALUES ($1,'shipped','Shipped','done','#123456',now()),($1,'abandoned','Abandoned','closed','#123456',NULL)",&[&ws]).await.unwrap();
    let insert = "INSERT INTO inbox_item(workspace_id,recipient_type,recipient_id,type,issue_id,title,body,read,archived,created_at,details) VALUES ($1,$2,$3,'new_comment',$4,$5,$6,$7,$8,now()+$9::text::interval,$10) RETURNING id";
    let body = "Việt 🧪".repeat(100);
    let old: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &"member",
                &users[0],
                &Some(issues[0]),
                &"old",
                &body,
                &false,
                &false,
                &"-1 hour",
                &json!({"comment_id":"synthetic"}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let new: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &"member",
                &users[0],
                &Some(issues[0]),
                &"new",
                &body,
                &false,
                &false,
                &"0 seconds",
                &json!({}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let peer: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &"member",
                &users[1],
                &Some(issues[0]),
                &"peer",
                &"private peer",
                &false,
                &false,
                &"0 seconds",
                &json!({}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let agent: Uuid = db
        .query_one(
            insert,
            &[
                &ws,
                &"agent",
                &users[0],
                &Some(issues[0]),
                &"agent",
                &"private agent",
                &false,
                &false,
                &"0 seconds",
                &json!({}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            insert,
            &[
                &other,
                &"member",
                &users[0],
                &Some(issues[0]),
                &"foreign",
                &"private workspace",
                &false,
                &false,
                &"0 seconds",
                &json!({}),
            ],
        )
        .await
        .unwrap()
        .get(0);
    let events = Arc::new(Events::default());
    let app = app(pool.clone(), Some(events.clone()));
    let token = jwt(users[0]);
    let endpoint = format!("{new}/read");
    assert_eq!(
        post(&app, ws, &endpoint, None).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, ws, &endpoint, Some("bad")).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, ws, &endpoint, Some(&jwt(users[2]))).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(&app, ws, "not-a-uuid/read", Some(&token)).await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid inbox item id"})
        )
    );
    for id in [peer, agent, foreign, Uuid::new_v4()] {
        assert_eq!(
            post(&app, ws, &format!("{id}/read"), Some(&token)).await,
            (
                StatusCode::NOT_FOUND,
                json!({"error":"inbox item not found"})
            )
        );
    }
    let (status, response) = post(&app, ws, &endpoint, Some(&token)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["body"], body);
    assert_eq!(response["issue_status"], "in_progress");
    assert_eq!(response["issue_priority"], "high");
    assert_eq!(response["read"], true);
    assert_eq!(response["archived"], false);
    assert!(chrono::DateTime::parse_from_rfc3339(response["created_at"].as_str().unwrap()).is_ok());
    assert!(
        !db.query_one("SELECT read FROM inbox_item WHERE id=$1", &[&old])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        events.0.lock().unwrap()[0],
        InboxEvent {
            kind: InboxEventKind::Read,
            workspace_id: ws,
            recipient_id: users[0],
            payload: json!({"item_id":new,"recipient_id":users[0]})
        }
    );
    assert_eq!(
        post(&app, ws, &format!("{new}/unread"), Some(&token))
            .await
            .1["read"],
        false
    );
    assert_eq!(
        events.0.lock().unwrap().last().unwrap().kind.wire_name(),
        "inbox:unread"
    );
    let (a, b) = tokio::join!(
        post(&app, ws, &endpoint, Some(&token)),
        post(&app, ws, &endpoint, Some(&token))
    );
    assert_eq!((a.0, b.0), (StatusCode::OK, StatusCode::OK));
    let before = events.0.lock().unwrap().len();
    db.batch_execute("CREATE FUNCTION queries_sprint_fail_sibling() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.title='old' AND NEW.archived THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER queries_sprint_fail_sibling BEFORE UPDATE ON inbox_item FOR EACH ROW EXECUTE FUNCTION queries_sprint_fail_sibling();").await.unwrap();
    assert_eq!(
        post(&app, ws, &format!("{new}/archive"), Some(&token))
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.0.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE id=ANY($1) AND archived",
            &[&vec![old, new]]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.batch_execute("DROP TRIGGER queries_sprint_fail_sibling ON inbox_item; DROP FUNCTION queries_sprint_fail_sibling();").await.unwrap();
    assert_eq!(
        post(&app, ws, &format!("{new}/archive"), Some(&token))
            .await
            .1["archived"],
        true
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM inbox_item WHERE id=ANY($1) AND archived",
            &[&vec![old, new]]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    for id in [peer, agent, foreign] {
        assert!(
            !db.query_one("SELECT archived FROM inbox_item WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
    }
    assert_eq!(
        post(&app, ws, &format!("{new}/unarchive"), Some(&token))
            .await
            .1["archived"],
        false
    );
    assert!(
        !db.query_one("SELECT read FROM inbox_item WHERE id=$1", &[&old])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert_eq!(
        post(&app, ws, "archive-all-read", Some(&token)).await,
        (StatusCode::OK, json!({"count":2}))
    );
    // Newest unread protects all older siblings, even when an older item is read.
    db.execute(
        "UPDATE inbox_item SET archived=false,read=(id=$1) WHERE id=ANY($2)",
        &[&old, &vec![old, new]],
    )
    .await
    .unwrap();
    assert_eq!(
        post(&app, ws, "archive-all-read", Some(&token)).await.1,
        json!({"count":0})
    );
    assert_eq!(
        post(&app, ws, "mark-all-read", Some(&token)).await.1,
        json!({"count":1})
    );
    assert_eq!(
        post(&app, ws, "mark-all-read", Some(&token)).await.1,
        json!({"count":0})
    );
    assert_eq!(
        post(&app, ws, "archive-all", Some(&token)).await.1,
        json!({"count":2})
    );
    for issue in &issues[1..] {
        db.query_one(
            insert,
            &[
                &ws,
                &"member",
                &users[0],
                &Some(*issue),
                &"terminal",
                &"terminal body",
                &false,
                &false,
                &"0 seconds",
                &json!({}),
            ],
        )
        .await
        .unwrap();
    }
    assert_eq!(
        post(&app, ws, "archive-completed", Some(&token)).await.1,
        json!({"count":3})
    );
    assert_eq!(
        post(&app, ws, "archive-completed", Some(&token)).await.1,
        json!({"count":0})
    );
    // Production without recipient event delivery fails closed before touching rows.
    db.execute(
        "UPDATE inbox_item SET read=false,archived=false WHERE id=$1",
        &[&new],
    )
    .await
    .unwrap();
    let no_sink = crate::app(pool.clone(), None);
    assert_eq!(
        post(&no_sink, ws, &endpoint, Some(&token)).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"inbox event delivery is unavailable"})
        )
    );
    assert!(
        !db.query_one("SELECT read FROM inbox_item WHERE id=$1", &[&new])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.workspace_id == ws && e.recipient_id == users[0])
    );
    assert_eq!(
        events.0.lock().unwrap().last().unwrap().kind,
        InboxEventKind::BatchArchived
    );
    for id in [peer, agent, foreign] {
        assert!(
            !db.query_one(
                "SELECT archived OR read FROM inbox_item WHERE id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
    }
}
