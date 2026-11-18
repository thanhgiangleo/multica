//! Synthetic, owned PostgreSQL only; never production or installed agents.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Timelike;
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{
        ApiState,
        issue_write::{IssueDeletedEvent, IssueEventSink, IssueMetadataEvent},
    },
    health::{Health, MigrationCatalog},
    migrate::{self, Direction, Stream},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "issue_write_labels/cases.rs"]
mod label_cases;

#[derive(Default)]
struct Sink(
    Mutex<Vec<IssueMetadataEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueReactionEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueAssignmentEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueCommentResolutionEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueWakeupActivityEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueTaskCancelledEvent>>,
    Mutex<Vec<multica_server::api::issue_write::IssueWakeupDispatchRequest>>,
);
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected event")
    }
    fn publish_issue_created(&self, _: multica_server::api::issue_write::IssueCreatedEvent) {
        panic!("unexpected issue create");
    }
    fn publish_comment_created(
        &self,
        _event: multica_server::api::issue_write::IssueCommentCreatedEvent,
    ) {
        panic!("comment creation must use the comment-specific test sink");
    }
    fn publish_comment_changed(
        &self,
        _: multica_server::api::issue_write::IssueCommentChangedEvent,
    ) {
        panic!("comment changes must use the comment-specific test sink");
    }
    fn publish_metadata_changed(&self, event: IssueMetadataEvent) {
        self.0.lock().unwrap().push(event);
    }
    fn publish_reaction_changed(
        &self,
        event: multica_server::api::issue_write::IssueReactionEvent,
    ) {
        self.1.lock().unwrap().push(event);
    }
    fn publish_comment_resolution(
        &self,
        event: multica_server::api::issue_write::IssueCommentResolutionEvent,
    ) {
        self.3.lock().unwrap().push(event);
    }
    fn publish_wakeup_activity(
        &self,
        event: multica_server::api::issue_write::IssueWakeupActivityEvent,
    ) {
        self.4.lock().unwrap().push(event);
    }
    fn publish_task_cancelled(
        &self,
        event: multica_server::api::issue_write::IssueTaskCancelledEvent,
    ) {
        self.5.lock().unwrap().push(event);
    }
    fn request_wakeup_dispatch(
        &self,
        request: multica_server::api::issue_write::IssueWakeupDispatchRequest,
    ) {
        self.6.lock().unwrap().push(request);
    }
    fn publish_assignment_changed(
        &self,
        event: multica_server::api::issue_write::IssueAssignmentEvent,
    ) {
        self.2.lock().unwrap().push(event);
    }
    fn publish_subscriber_changed(
        &self,
        _: multica_server::api::issue_write::IssueSubscriberEvent,
    ) {
        panic!("unexpected subscription event");
    }
    fn publish_issue_updated(&self, _: multica_server::api::issue_write::IssueUpdatedEvent) {
        panic!("unexpected issue update");
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    raw_call(app, method, path, token, body.to_string().as_bytes()).await
}

async fn raw_call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &[u8],
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
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
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn metadata_http_transactions_events_isolation_and_races() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let hooks: Value =
        serde_json::from_str(include_str!("../baseline/concurrent-index-hooks.json")).unwrap();
    let up: HashMap<String, String> = serde_json::from_value(hooks["upstream_up"].clone()).unwrap();
    let zbs: HashMap<String, String> = serde_json::from_value(hooks["zbs_up"].clone()).unwrap();
    migrate::run_both(
        &pool,
        &Stream {
            directory: &root.join("migrations"),
            table: "schema_migrations",
            lock_key: 6601,
            hooks: &up,
            production_gates: true,
        },
        &Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 6602,
            hooks: &zbs,
            production_gates: true,
        },
        Direction::Up,
    )
    .await
    .unwrap();
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let user: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('member',$1) RETURNING id",
            &[&format!("metadata-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Metadata',$1,'META') RETURNING id", &[&format!("metadata-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Foreign',$1,'OTHER') RETURNING id", &[&format!("other-{suffix}")]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Metadata','member',$2,42) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Foreign','member',$2,42) RETURNING id", &[&other,&user]).await.unwrap().get(0);
    let sink = Arc::new(Sink::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), b"metadata-test-secret".to_vec())
            .with_issue_event_sink(sink.clone()),
    );
    let auth = |sub: Uuid, exp: u64| {
        encode(
            &Header::default(),
            &json!({"sub":sub,"exp":exp}),
            &EncodingKey::from_secret(b"metadata-test-secret"),
        )
        .unwrap()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = auth(user, now + 3600);
    let path = |id: &str, key: &str| format!("/api/issues/{id}/metadata/{key}?workspace_id={ws}");
    let p = path("meta-042", "count");
    let first = call(&app, "PUT", &p, &token, json!({"value":1})).await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    let rev = first.1["issue_revision"].as_i64().unwrap();
    assert_eq!(first.1["metadata"], json!({"count":1}));
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    let event = sink.0.lock().unwrap()[0].clone();
    assert_eq!(
        (
            event.actor_type,
            event.actor_id,
            event.workspace_id,
            event.issue_id,
            event.issue_revision
        ),
        ("member", user, ws, issue, rev)
    );
    assert_eq!(
        call(&app, "PUT", &p, &token, json!({"value":1})).await,
        first
    );
    // PostgreSQL considers 1 and 1.0 equal; no revision/event bump.
    assert_eq!(
        call(&app, "PUT", &p, &token, json!({"value":1.0})).await,
        first
    );
    assert_eq!(sink.0.lock().unwrap().len(), 1);
    for (body, message) in [
        (
            json!({"value":null}),
            "value cannot be null (use DELETE to remove a key)",
        ),
        (json!({}), "value is required"),
        (
            json!({"value":[]}),
            "value must be a primitive: string, number, or bool",
        ),
    ] {
        assert_eq!(
            call(&app, "PUT", &p, &token, body).await,
            (StatusCode::BAD_REQUEST, json!({"error":message}))
        );
    }
    for bad in ["bad-id", &foreign.to_string(), "OTHER-42"] {
        assert_eq!(
            call(&app, "PUT", &path(bad, "x"), &token, json!({"value":true}))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            "PUT",
            &path("META-42", "1bad"),
            &token,
            json!({"value":true})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, "PUT", &p, &auth(user, 1), json!({"value":2}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let pat = format!("mul_issue_write_{suffix}");
    let hash = hex::encode(Sha256::digest(pat.as_bytes()));
    db.execute("INSERT INTO personal_access_token(user_id,name,token_hash,token_prefix) VALUES ($1,'synthetic',$2,'mul_issue')", &[&user,&hash]).await.unwrap();
    assert_eq!(call(&app, "PUT", &p, &pat, json!({"value":1})).await, first);
    db.execute(
        "UPDATE personal_access_token SET revoked=true WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "PUT", &p, &pat, json!({"value":2})).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "PUT", &p, "mdt_not_a_task", json!({"value":2}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let rejected = call(&app, "PUT", &p, &token, json!({"value":"x".repeat(9000)})).await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert_eq!(
        call(&app, "PUT", &p, &token, json!({"value":1})).await,
        first
    );
    // Every independent key survives concurrent writes; revisions are unique.
    let mut tasks = Vec::new();
    for i in 0..20 {
        let app = app.clone();
        let token = token.clone();
        let p = path("META-42", &format!("k{i}"));
        tasks.push(tokio::spawn(async move {
            call(&app, "PUT", &p, &token, json!({"value":i})).await
        }));
    }
    let mut revisions = Vec::new();
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
        revisions.push(result.1["issue_revision"].as_i64().unwrap());
    }
    revisions.sort();
    revisions.dedup();
    assert_eq!(revisions.len(), 20);
    let stored: Value = db
        .query_one("SELECT metadata FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored.as_object().unwrap().len(), 21);
    for i in 20..49 {
        assert_eq!(
            call(
                &app,
                "PUT",
                &path("META-42", &format!("k{i}")),
                &token,
                json!({"value":i})
            )
            .await
            .0,
            StatusCode::OK
        );
    }
    assert_eq!(
        call(
            &app,
            "PUT",
            &path("META-42", "overflow"),
            &token,
            json!({"value":1})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let deleted = call(&app, "DELETE", &p, &token, json!(null)).await;
    assert_eq!(deleted.0, StatusCode::OK);
    assert!(deleted.1["metadata"].get("count").is_none());
    assert_eq!(call(&app, "DELETE", &p, &token, json!(null)).await, deleted);
    let req = Request::builder()
        .method("PUT")
        .uri(&p)
        .header("cookie", format!("multica_auth={token}"))
        .body(Body::from(r#"{"value":2}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    // Without a real event bus there is no advertised mutation endpoint.
    let unwired = router_with_api(
        health,
        ApiState::new(pool.clone(), b"metadata-test-secret".to_vec()),
    );
    let response = unwired
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&p)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'visible') RETURNING id", &[&issue,&ws,&user]).await.unwrap().get(0);
    let rx_path = format!("/api/comments/{comment}/reactions?workspace_id={ws}");
    let rx = call(&app, "POST", &rx_path, &token, json!({"emoji":"+1"})).await;
    assert_eq!(rx.0, StatusCode::CREATED, "{rx:?}");
    let rx_rev = rx.1["comment_revision"].as_i64().unwrap();
    assert_eq!(sink.1.lock().unwrap().len(), 1);
    assert_eq!(sink.1.lock().unwrap()[0].event_type, "reaction:added");
    assert_eq!(sink.1.lock().unwrap()[0].payload["reaction"], rx.1);
    let duplicate = call(&app, "POST", &rx_path, &token, json!({"emoji":"+1"})).await;
    assert_eq!(duplicate.0, StatusCode::CREATED);
    assert!(duplicate.1.get("comment_revision").is_none());
    assert_eq!(sink.1.lock().unwrap().len(), 1);
    assert_eq!(
        call(&app, "POST", &rx_path, &token, json!({"emoji":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/comments/not-a-uuid/reactions?workspace_id={ws}"),
            &token,
            json!({"emoji":"+1"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/comments/{comment}/reactions?workspace_id={other}"),
            &token,
            json!({"emoji":"+1"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let removed = call(&app, "DELETE", &rx_path, &token, json!({"emoji":"+1"})).await;
    assert_eq!(removed.0, StatusCode::NO_CONTENT);
    assert_eq!(sink.1.lock().unwrap().len(), 2);
    assert_eq!(sink.1.lock().unwrap()[1].event_type, "reaction:removed");
    assert_eq!(
        sink.1.lock().unwrap()[1].payload["comment_revision"],
        json!(rx_rev + 1)
    );
    assert_eq!(
        call(&app, "DELETE", &rx_path, &token, json!({"emoji":"+1"}))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sink.1.lock().unwrap().len(), 2);
    let rx_count: i64 = db
        .query_one(
            "SELECT count(*) FROM comment_reaction WHERE comment_id=$1",
            &[&comment],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rx_count, 0);
    db.execute(
        "UPDATE comment SET deleted_at=now() WHERE id=$1",
        &[&comment],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "POST", &rx_path, &token, json!({"emoji":"+1"}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // Transaction-local identity must not leak into a reused pool connection.
    let setting: Option<String> = db
        .query_one(
            "SELECT nullif(current_setting('multica.actor_id',true),'')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(setting.is_none());

    let label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,resource_type,name,color) VALUES ($1,'issue','Visible','#ffffff') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,resource_type,name,color) VALUES ($1,'issue','Foreign','#ffffff') RETURNING id", &[&other]).await.unwrap().get(0);
    let agent_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,resource_type,name,color) VALUES ($1,'agent','Agent','#ffffff') RETURNING id", &[&ws]).await.unwrap().get(0);
    let label_path = format!("/api/issues/META-42/labels?workspace_id={ws}");
    for (id, status, message) in [
        (foreign_label, StatusCode::NOT_FOUND, "label not found"),
        (agent_label, StatusCode::NOT_FOUND, "issue label not found"),
    ] {
        assert_eq!(
            call(&app, "POST", &label_path, &token, json!({"label_id":id})).await,
            (status, json!({"error":message}))
        );
    }
    assert_eq!(
        call(&app, "POST", &label_path, &token, json!({"label_id":"bad"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &label_path,
            &auth(user, 1),
            json!({"label_id":label})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{foreign}/labels?workspace_id={ws}"),
            &token,
            json!({"label_id":label})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let attached = call(&app, "POST", &label_path, &token, json!({"label_id":label})).await;
    assert_eq!(attached.0, StatusCode::OK, "{attached:?}");
    assert_eq!(attached.1["labels"][0]["resource_type"], "issue");
    assert_eq!(attached.1["labels"][0]["usage_count"], 0);
    assert_eq!(
        sink.2.lock().unwrap().last().unwrap().event_type,
        "issue_labels:changed"
    );
    assert_eq!(
        sink.2.lock().unwrap().last().unwrap().payload["labels"],
        attached.1["labels"]
    );
    let after_first = sink.2.lock().unwrap().len();
    let duplicate = call(&app, "POST", &label_path, &token, json!({"label_id":label})).await;
    assert_eq!(duplicate.1, json!({"labels":attached.1["labels"]}));
    assert_eq!(sink.2.lock().unwrap().len(), after_first);
    let detach = format!("/api/issues/META-42/labels/{label}?workspace_id={ws}");
    let detached = call(&app, "DELETE", &detach, &token, Value::Null).await;
    assert_eq!(detached.0, StatusCode::OK, "{detached:?}");
    assert_eq!(detached.1["labels"], json!([]));
    assert_eq!(
        detached.1["issue_revision"],
        json!(attached.1["issue_revision"].as_i64().unwrap() + 1)
    );
    assert_eq!(
        call(&app, "DELETE", &detach, &token, Value::Null).await.1,
        json!({"labels":[]})
    );
    assert_eq!(sink.2.lock().unwrap().len(), after_first + 1);
    let labels_count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_to_label WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(labels_count, 0);
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let app = app.clone();
        let token = token.clone();
        let path = label_path.clone();
        tasks.push(tokio::spawn(async move {
            call(&app, "POST", &path, &token, json!({"label_id":label})).await
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap().0, StatusCode::OK);
    }
    assert_eq!(sink.2.lock().unwrap().len(), after_first + 2);
    let labels_count: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_to_label WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(labels_count, 1);
    label_cases::verify(&app, &token, &db, ws, issue, label, &sink).await;

    let text_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Notes','text') RETURNING id", &[&ws]).await.unwrap().get(0);
    let actor_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Reviewer','actor') RETURNING id", &[&ws]).await.unwrap().get(0);
    let url_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Reference','url') RETURNING id", &[&ws]).await.unwrap().get(0);
    let multi_url_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'References','multi_url') RETURNING id", &[&ws]).await.unwrap().get(0);
    let multi_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type,config) VALUES ($1,'Flags','multi_select',$2) RETURNING id", &[&ws,&json!({"options":[{"id":Uuid::new_v4().to_string(),"name":"first"},{"id":Uuid::new_v4().to_string(),"name":"second"}]})]).await.unwrap().get(0);
    let foreign_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Foreign','text') RETURNING id", &[&other]).await.unwrap().get(0);
    let ppath = |id: Uuid| format!("/api/issues/META-42/properties/{id}?workspace_id={ws}");
    let text_path = ppath(text_property);
    let large_port_url = "HTTP://example.com:99999/path?key=value";
    let url_result = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":large_port_url}),
    )
    .await;
    assert_eq!(url_result.0, StatusCode::OK, "{url_result:?}");
    assert_eq!(
        url_result.1["properties"][url_property.to_string()],
        large_port_url
    );
    let list_result = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":[large_port_url,"https://example.com:999999999999999999999999",large_port_url]}),
    )
    .await;
    assert_eq!(list_result.0, StatusCode::OK, "{list_result:?}");
    assert_eq!(
        list_result.1["properties"][multi_url_property.to_string()],
        json!([
            large_port_url,
            "https://example.com:999999999999999999999999"
        ])
    );
    let invalid_port = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":"http://example.com:-1"}),
    )
    .await;
    assert_eq!(
        invalid_port,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"value must be an http(s) URL"})
        )
    );
    let stored_urls: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_urls[url_property.to_string()], large_port_url);
    let hostless_url = "http://user@:99999/notes";
    let hostless = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":hostless_url}),
    )
    .await;
    assert_eq!(hostless.0, StatusCode::OK, "{hostless:?}");
    assert_eq!(
        hostless.1["properties"][url_property.to_string()],
        hostless_url
    );
    let hostless_multi = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":["http://:80/path", "https://:"]}),
    )
    .await;
    assert_eq!(hostless_multi.0, StatusCode::OK, "{hostless_multi:?}");
    let invalid_hostless = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":"http://:letters"}),
    )
    .await;
    assert_eq!(invalid_hostless.0, StatusCode::BAD_REQUEST);
    for bad_host in ["http://%41.example", r"http://foo\bar"] {
        assert_eq!(
            call(
                &app,
                "PUT",
                &ppath(url_property),
                &token,
                json!({"value":bad_host})
            )
            .await
            .0,
            StatusCode::BAD_REQUEST,
            "{bad_host}",
        );
    }
    let raw_query = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":"http://example.com/?q=%ZZ"}),
    )
    .await;
    assert_eq!(raw_query.0, StatusCode::OK, "{raw_query:?}");
    for invalid in [
        "http://example.com/path%ZZ",
        "http://example.com/#%ZZ",
        "http://user%ZZ@example.com/path",
        "http://user name@example.com",
        "http://ü@example.com",
    ] {
        let rejected = call(
            &app,
            "PUT",
            &ppath(url_property),
            &token,
            json!({"value":invalid}),
        )
        .await;
        assert_eq!(
            rejected.0,
            StatusCode::BAD_REQUEST,
            "{invalid}: {rejected:?}"
        );
    }
    let rejected_multi = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":["http://example.com/ok", "http://example.com/#%"]}),
    )
    .await;
    assert_eq!(
        rejected_multi.0,
        StatusCode::BAD_REQUEST,
        "{rejected_multi:?}"
    );
    let rejected_userinfo_list = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":["https://example.com/ok", "https://user%ZZ@example.com/"]}),
    )
    .await;
    assert_eq!(rejected_userinfo_list.0, StatusCode::BAD_REQUEST);
    let stored_urls: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        stored_urls[url_property.to_string()],
        "http://example.com/?q=%ZZ"
    );
    assert_eq!(
        stored_urls[multi_url_property.to_string()],
        json!(["http://:80/path", "https://:"])
    );
    let scoped_url = "http://[fe80::1%25eth0]:99999/notes";
    let scoped = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":scoped_url}),
    )
    .await;
    assert_eq!(scoped.0, StatusCode::OK, "{scoped:?}");
    assert_eq!(scoped.1["properties"][url_property.to_string()], scoped_url);
    let scoped_multi = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":[scoped_url,"http://[fe80::1%25eth0%20wifi]/",scoped_url]}),
    )
    .await;
    assert_eq!(scoped_multi.0, StatusCode::OK, "{scoped_multi:?}");
    assert_eq!(
        scoped_multi.1["properties"][multi_url_property.to_string()],
        json!([scoped_url, "http://[fe80::1%25eth0%20wifi]/"])
    );
    for invalid_zone in ["http://[fe80::1%25eth0%ZZ]/", "http://[fe80::1%eth0]/"] {
        let rejected = call(
            &app,
            "PUT",
            &ppath(url_property),
            &token,
            json!({"value":invalid_zone}),
        )
        .await;
        assert_eq!(
            rejected.0,
            StatusCode::BAD_REQUEST,
            "{invalid_zone}: {rejected:?}"
        );
    }
    let stored_scoped: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_scoped[url_property.to_string()], scoped_url);
    assert_eq!(
        stored_scoped[multi_url_property.to_string()],
        json!([scoped_url, "http://[fe80::1%25eth0%20wifi]/"])
    );
    let encoded_host = "https://user:pass@example%25.com:99999/notes";
    let accepted = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":encoded_host}),
    )
    .await;
    assert_eq!(accepted.0, StatusCode::OK, "{accepted:?}");
    assert_eq!(
        accepted.1["properties"][url_property.to_string()],
        encoded_host
    );
    let accepted_list = call(
        &app,
        "PUT",
        &ppath(multi_url_property),
        &token,
        json!({"value":["https://%25/path", encoded_host, encoded_host]}),
    )
    .await;
    assert_eq!(accepted_list.0, StatusCode::OK, "{accepted_list:?}");
    assert_eq!(
        accepted_list.1["properties"][multi_url_property.to_string()],
        json!(["https://%25/path", encoded_host])
    );
    let rejected_host = call(
        &app,
        "PUT",
        &ppath(url_property),
        &token,
        json!({"value":"https://example%2F.com/"}),
    )
    .await;
    assert_eq!(rejected_host.0, StatusCode::BAD_REQUEST);
    let stored_encoded: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored_encoded[url_property.to_string()], encoded_host);
    assert_eq!(
        stored_encoded[multi_url_property.to_string()],
        json!(["https://%25/path", encoded_host])
    );
    let invalid = call(&app, "PUT", &text_path, &token, json!({"value":null})).await;
    assert_eq!(
        invalid,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"value cannot be null (use DELETE to unset a property)"})
        )
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &ppath(foreign_property),
            &token,
            json!({"value":"a"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &text_path,
            &auth(user, 1),
            json!({"value":"a"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    let text_result = call(
        &app,
        "PUT",
        &text_path,
        &token,
        json!({"value":"  persisted  "}),
    )
    .await;
    assert_eq!(text_result.0, StatusCode::OK, "{text_result:?}");
    assert_eq!(
        text_result.1["properties"][text_property.to_string()],
        json!("  persisted  ")
    );
    let prop_rev = text_result.1["issue_revision"].as_i64().unwrap();
    assert_eq!(
        sink.2.lock().unwrap().last().unwrap().event_type,
        "issue_properties:changed"
    );
    assert_eq!(
        sink.2.lock().unwrap().last().unwrap().payload,
        text_result
            .1
            .as_object()
            .map(|obj| {
                let mut copy = obj.clone();
                copy.insert("issue_id".into(), json!(issue));
                Value::Object(copy)
            })
            .unwrap()
    );
    let no_op = call(
        &app,
        "PUT",
        &text_path,
        &token,
        json!({"value":"  persisted  "}),
    )
    .await;
    assert_eq!(no_op, text_result);
    assert_eq!(
        sink.2.lock().unwrap().last().unwrap().payload["issue_revision"],
        json!(prop_rev)
    );
    let duplicate = raw_call(
        &app,
        "PUT",
        &text_path,
        &token,
        br#"{"value":"first","VALUE":"last"} trailing"#,
    )
    .await;
    assert_eq!(duplicate.0, StatusCode::OK, "{duplicate:?}");
    assert_eq!(duplicate.1["properties"][text_property.to_string()], "last");
    assert_eq!(duplicate.1["issue_revision"], prop_rev + 1);
    let missing = raw_call(&app, "PUT", &text_path, &token, br#"{}"#).await;
    assert_eq!(
        missing,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"value is required"})
        )
    );
    let unknown_overflow = raw_call(
        &app,
        "PUT",
        &text_path,
        &token,
        br#"{"ignored":1e400,"value":"last"}"#,
    )
    .await;
    assert_eq!(unknown_overflow.0, StatusCode::OK, "{unknown_overflow:?}");
    assert_eq!(unknown_overflow.1["issue_revision"], prop_rev + 1);
    let old_count = sink.2.lock().unwrap().len();
    assert_eq!(
        call(&app, "PUT", &text_path, &token, json!({"value":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(sink.2.lock().unwrap().len(), old_count);
    for (body, message) in [
        (
            br#"{"value":"ok","VALUE":null}"#.as_slice(),
            "value cannot be null (use DELETE to unset a property)",
        ),
        (
            br#"{"value":1e400}"#,
            "value must be valid JSON: json: cannot unmarshal number 1e400 into Go value of type float64",
        ),
        (
            br#"{"value":{"n":1e400}}"#,
            "value must be valid JSON: json: cannot unmarshal number 1e400 into Go value of type float64",
        ),
        (br#"{"value":"ok","VALUE":}"#, "invalid request body"),
    ] {
        assert_eq!(
            raw_call(&app, "PUT", &text_path, &token, body).await,
            (StatusCode::BAD_REQUEST, json!({"error":message}))
        );
        assert_eq!(sink.2.lock().unwrap().len(), old_count);
        assert_eq!(
            db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
                .await
                .unwrap()
                .get::<_, i64>(0),
            prop_rev + 1
        );
    }
    let invalid_utf8 = raw_call(&app, "PUT", &text_path, &token, b"{\"value\":\"\xff\"}").await;
    assert_eq!(invalid_utf8.0, StatusCode::OK, "{invalid_utf8:?}");
    assert_eq!(
        invalid_utf8.1["properties"][text_property.to_string()],
        "\u{fffd}"
    );
    assert_eq!(invalid_utf8.1["issue_revision"], prop_rev + 2);
    assert_eq!(sink.2.lock().unwrap().len(), old_count + 1);
    assert_eq!(
        call(
            &app,
            "PUT",
            &ppath(actor_property),
            &token,
            json!({"value":format!("member:{foreign}")})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &ppath(actor_property),
            &token,
            json!({"value":format!("member:{user}")})
        )
        .await
        .0,
        StatusCode::OK
    );
    let definition_config: Value = db
        .query_one(
            "SELECT config FROM issue_property WHERE id=$1",
            &[&multi_property],
        )
        .await
        .unwrap()
        .get(0);
    let opts = definition_config["options"].as_array().unwrap();
    let a = opts[0]["id"].as_str().unwrap();
    let b = opts[1]["id"].as_str().unwrap();
    let selected = call(
        &app,
        "PUT",
        &ppath(multi_property),
        &token,
        json!({"value":[b,a,b]}),
    )
    .await;
    assert_eq!(selected.0, StatusCode::OK, "{selected:?}");
    assert_eq!(
        selected.1["properties"][multi_property.to_string()],
        json!([a, b])
    );
    db.execute(
        "UPDATE issue_property SET archived_at=now() WHERE id=$1",
        &[&text_property],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "PUT", &text_path, &token, json!({"value":"blocked"}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let unset = call(&app, "DELETE", &text_path, &token, Value::Null).await;
    assert_eq!(unset.0, StatusCode::OK, "{unset:?}");
    assert!(
        unset.1["properties"]
            .get(text_property.to_string())
            .is_none()
    );
    assert_eq!(
        call(&app, "DELETE", &text_path, &token, Value::Null).await,
        unset
    );
    let request = Request::builder()
        .method("PUT")
        .uri(ppath(actor_property))
        .header("cookie", format!("multica_auth={token}"))
        .body(Body::from(format!("{{\"value\":\"member:{user}\"}}")))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let direct: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(direct, unset.1["properties"]);

    // Independent property keys must not overwrite one another. The same
    // concurrent update also exercises the transaction-local wakeup actor.
    let mut paths = Vec::new();
    for i in 0..8 {
        let prop: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,$2,'text') RETURNING id", &[&ws,&format!("Concurrent {i}")]).await.unwrap().get(0);
        paths.push((prop, ppath(prop)));
    }
    let mut tasks = Vec::new();
    for (i, (_, path)) in paths.iter().enumerate() {
        let app = app.clone();
        let token = token.clone();
        let path = path.clone();
        tasks.push(tokio::spawn(async move {
            call(
                &app,
                "PUT",
                &path,
                &token,
                json!({"value":format!("value{i}")}),
            )
            .await
        }));
    }
    let mut property_revisions = Vec::new();
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
        property_revisions.push(result.1["issue_revision"].as_i64().unwrap());
    }
    property_revisions.sort();
    property_revisions.dedup();
    assert_eq!(property_revisions.len(), paths.len());
    let direct: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    for (i, (prop, _)) in paths.iter().enumerate() {
        assert_eq!(direct[prop.to_string()], format!("value{i}"));
    }

    let rollback: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES ($1,'Rollback','text') RETURNING id", &[&ws]).await.unwrap().get(0);
    for (prop, _) in &paths {
        let result = call(
            &app,
            "PUT",
            &ppath(*prop),
            &token,
            json!({"value":"x".repeat(1900)}),
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{result:?}");
    }
    let previous: Value = db
        .query_one("SELECT properties FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let event_count = sink.2.lock().unwrap().len();
    let rejected = call(
        &app,
        "PUT",
        &ppath(rollback),
        &token,
        json!({"value":"x".repeat(1900)}),
    )
    .await;
    assert_eq!(
        rejected,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"issue properties exceed the 16KB size limit"})
        )
    );
    let current = db
        .query_one(
            "SELECT properties,revision FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(current.get::<_, Value>(0), previous);
    assert_eq!(current.get::<_, i64>(1), revision);
    assert_eq!(sink.2.lock().unwrap().len(), event_count);
    let setting: Option<String> = db
        .query_one(
            "SELECT nullif(current_setting('multica.actor_type',true),'')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(setting.is_none());

    let issue_reactions = format!("/api/issues/{issue}/reactions?workspace_id={ws}");
    let before: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let added = call(
        &app,
        "POST",
        &issue_reactions,
        &token,
        json!({"emoji":"✨"}),
    )
    .await;
    assert_eq!(added.0, StatusCode::CREATED, "{added:?}");
    assert_eq!(added.1["issue_revision"], before + 1);
    assert_eq!(added.1["actor_type"], "member");
    assert_eq!(
        call(
            &app,
            "POST",
            &issue_reactions,
            &token,
            json!({"emoji":"✨"})
        )
        .await
        .1["id"],
        added.1["id"]
    );
    assert_eq!(
        sink.1.lock().unwrap().last().unwrap().event_type,
        "issue_reaction:added"
    );
    assert_eq!(
        sink.1.lock().unwrap().last().unwrap().payload["creator_id"],
        json!(user)
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &issue_reactions,
            &token,
            json!({"emoji":"✨"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let event_count = sink.1.lock().unwrap().len();
    assert_eq!(
        sink.1.lock().unwrap().last().unwrap().event_type,
        "issue_reaction:removed"
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &issue_reactions,
            &token,
            json!({"emoji":"✨"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sink.1.lock().unwrap().len(), event_count);
    assert_eq!(
        call(&app, "POST", &issue_reactions, &token, json!({"emoji":""}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{foreign}/reactions?workspace_id={ws}"),
            &token,
            json!({"emoji":"✨"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let after: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(after, before + 2);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_reaction WHERE issue_id=$1",
            &[&issue]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );

    // Raw request bodies exercise Go's struct decoder, not serde_json::Value's
    // map canonicalization. Keep their row/event assertions isolated.
    let scalar_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Scalar decoding','member',$2,43) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let scalar_path = path(&scalar_issue.to_string(), "number");
    let metadata_events = sink.0.lock().unwrap().len();
    let set = raw_call(
        &app,
        "PUT",
        &scalar_path,
        &token,
        br#"{"value":0,"VALUE":18446744073709551616} trailing"#,
    )
    .await;
    assert_eq!(set.0, StatusCode::OK, "{set:?}");
    let revision = set.1["issue_revision"].as_i64().unwrap();
    assert_eq!(sink.0.lock().unwrap().len(), metadata_events + 1);
    assert_eq!(
        db.query_one(
            "SELECT metadata->>'number' FROM issue WHERE id=$1",
            &[&scalar_issue]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "18446744073709551616"
    );
    let next = raw_call(
        &app,
        "PUT",
        &scalar_path,
        &token,
        br#"{"VaLuE":18446744073709551617}"#,
    )
    .await;
    assert_eq!(next.0, StatusCode::OK, "{next:?}");
    assert_eq!(next.1["issue_revision"], revision + 1);
    assert_eq!(
        db.query_one(
            "SELECT metadata->>'number' FROM issue WHERE id=$1",
            &[&scalar_issue]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "18446744073709551617"
    );
    let noop = raw_call(
        &app,
        "PUT",
        &scalar_path,
        &token,
        br#"{"value":18446744073709551617.0}"#,
    )
    .await;
    assert_eq!(noop, next);
    for body in [
        br#"{"\ud800":0,"VALUE":18446744073709551617}"#.as_slice(),
        b"{\"\xff\":0,\"VALUE\":18446744073709551617}",
    ] {
        assert_eq!(
            raw_call(&app, "PUT", &scalar_path, &token, body).await,
            next
        );
    }
    assert_eq!(sink.0.lock().unwrap().len(), metadata_events + 2);
    for (body, status, message) in [
        (
            br#"{"value":true,"VALUE":null}"#.as_slice(),
            StatusCode::BAD_REQUEST,
            "value cannot be null (use DELETE to remove a key)",
        ),
        (
            br#"null trailing"#,
            StatusCode::BAD_REQUEST,
            "value is required",
        ),
        (
            br#"{"value":1e400}"#,
            StatusCode::BAD_REQUEST,
            "value must be valid JSON: json: cannot unmarshal number 1e400 into Go value of type float64",
        ),
        (
            br#"{"value":{"n":1e400}}"#,
            StatusCode::BAD_REQUEST,
            "value must be valid JSON: json: cannot unmarshal number 1e400 into Go value of type float64",
        ),
        (
            br#"{"value":"\ud800"}"#,
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to set metadata key",
        ),
        (
            b"{\"value\":\"\xff\"}",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to set metadata key",
        ),
        (
            b"{\"\xff\":0,\"VALUE\":\"\xfe\"}",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to set metadata key",
        ),
        (
            br#"{"value":[1]}"#,
            StatusCode::BAD_REQUEST,
            "value must be a primitive: string, number, or bool",
        ),
        (
            br#"{"value":4,"VALUE":[]}"#,
            StatusCode::BAD_REQUEST,
            "value must be a primitive: string, number, or bool",
        ),
        (
            br#"{"value":4,"VALUE":}"#,
            StatusCode::BAD_REQUEST,
            "invalid request body",
        ),
    ] {
        assert_eq!(
            raw_call(&app, "PUT", &scalar_path, &token, body).await,
            (status, json!({"error":message}))
        );
        assert_eq!(
            db.query_one("SELECT revision FROM issue WHERE id=$1", &[&scalar_issue])
                .await
                .unwrap()
                .get::<_, i64>(0),
            revision + 1
        );
        assert_eq!(sink.0.lock().unwrap().len(), metadata_events + 2);
    }
    // Go decodes metadata JSONB as map[string]any for responses and events:
    // large integer storage remains exact, but the public projection is float64.
    let precision_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,metadata) VALUES ($1,'Metadata precision','member',$2,44,'{\"nested\":9007199254740993}'::jsonb) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let precision_path = path(&precision_issue.to_string(), "number");
    let events_before = sink.0.lock().unwrap().len();
    let rounded = json!(9007199254740992_u64);
    let changed = raw_call(
        &app,
        "PUT",
        &precision_path,
        &token,
        br#"{"value":9007199254740993}"#,
    )
    .await;
    assert_eq!(changed.0, StatusCode::OK, "{changed:?}");
    assert_eq!(
        changed.1["metadata"],
        json!({"nested":rounded,"number":rounded})
    );
    assert_eq!(sink.0.lock().unwrap().len(), events_before + 1);
    assert_eq!(
        sink.0.lock().unwrap().last().unwrap().metadata,
        changed.1["metadata"]
    );
    let stored: String = db
        .query_one(
            "SELECT metadata->>'number' FROM issue WHERE id=$1",
            &[&precision_issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "9007199254740993");
    let no_change = raw_call(
        &app,
        "PUT",
        &precision_path,
        &token,
        br#"{"value":9007199254740993}"#,
    )
    .await;
    assert_eq!(no_change, changed);
    let missing = format!("/api/issues/{precision_issue}/metadata/absent?workspace_id={ws}");
    let no_delete = raw_call(&app, "DELETE", &missing, &token, b"").await;
    assert_eq!(no_delete.0, StatusCode::OK);
    assert_eq!(no_delete.1["metadata"], changed.1["metadata"]);
    assert_eq!(sink.0.lock().unwrap().len(), events_before + 1);

    let scalar_comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'Scalar reactions') RETURNING id", &[&scalar_issue,&ws,&user]).await.unwrap().get(0);
    for reaction_path in [
        format!("/api/issues/{scalar_issue}/reactions?workspace_id={ws}"),
        format!("/api/comments/{scalar_comment}/reactions?workspace_id={ws}"),
    ] {
        let events = sink.1.lock().unwrap().len();
        let body = br#"{"emoji":"first","EMOJI":"raw","emoji":null} trailing"#;
        let added = raw_call(&app, "POST", &reaction_path, &token, body).await;
        assert_eq!(added.0, StatusCode::CREATED, "{added:?}");
        assert_eq!(added.1["emoji"], "raw");
        assert_eq!(added.1["actor_id"], json!(user));
        assert_eq!(sink.1.lock().unwrap().len(), events + 1);
        assert_eq!(
            sink.1.lock().unwrap().last().unwrap().payload["reaction"],
            added.1
        );
        let duplicate = raw_call(&app, "POST", &reaction_path, &token, br#"{"EMOJI":"raw"}"#).await;
        assert_eq!(duplicate.0, StatusCode::CREATED);
        assert_eq!(duplicate.1["id"], added.1["id"]);
        assert_eq!(sink.1.lock().unwrap().len(), events + 1);
        for (body, message) in [
            (
                br#"{"emoji":4,"EMOJI":"raw"}"#.as_slice(),
                "invalid request body",
            ),
            (br#"{"emoji":false}"#, "invalid request body"),
            (br#"{"emoji":[]}"#, "invalid request body"),
            (br#"{"emoji":null}"#, "emoji is required"),
            (br#"null"#, "emoji is required"),
        ] {
            for method in ["POST", "DELETE"] {
                assert_eq!(
                    raw_call(&app, method, &reaction_path, &token, body).await,
                    (StatusCode::BAD_REQUEST, json!({"error":message}))
                );
            }
            assert_eq!(sink.1.lock().unwrap().len(), events + 1);
        }
        assert_eq!(
            raw_call(&app, "DELETE", &reaction_path, &token, body)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(sink.1.lock().unwrap().len(), events + 2);
        assert_eq!(
            sink.1.lock().unwrap().last().unwrap().payload["emoji"],
            "raw"
        );
        assert_eq!(
            raw_call(&app, "DELETE", &reaction_path, &token, body)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(sink.1.lock().unwrap().len(), events + 2);
    }

    // A resolution is unique within one thread, not the whole issue.
    let root: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'Root') RETURNING id", &[&issue,&ws,&user]).await.unwrap().get(0);
    let child: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,parent_id,content) VALUES ($1,$2,'member',$3,$4,'Child') RETURNING id", &[&issue,&ws,&user,&root]).await.unwrap().get(0);
    let sibling: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,parent_id,content) VALUES ($1,$2,'member',$3,$4,'Sibling') RETURNING id", &[&issue,&ws,&user,&root]).await.unwrap().get(0);
    let independent: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'Independent') RETURNING id", &[&issue,&ws,&user]).await.unwrap().get(0);
    let foreign_comment: Uuid = db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'Foreign') RETURNING id", &[&foreign,&other,&user]).await.unwrap().get(0);
    let path = |id: Uuid| format!("/api/comments/{id}/resolve?workspace_id={ws}");
    let first = call(&app, "POST", &path(child), &token, json!({})).await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(first.1["id"], json!(child));
    assert_eq!(first.1["resolved_by_type"], "member");
    assert_eq!(first.1["resolved_by_id"], json!(user));
    assert_eq!(first.1["revision"], 2);
    assert_eq!(first.1["reactions"], json!([]));
    assert_eq!(first.1["attachments"], json!([]));
    let first_time = first.1["resolved_at"].clone();
    assert_eq!(
        call(&app, "POST", &path(child), &token, json!({})).await,
        first
    );
    assert_eq!(sink.3.lock().unwrap().len(), 1);
    assert_eq!(
        call(&app, "POST", &path(independent), &token, json!({}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "POST", &path(sibling), &token, json!({}))
            .await
            .0,
        StatusCode::OK
    );
    let rows = db
        .query(
            "SELECT id,resolved_at,revision FROM comment WHERE id=ANY($1)",
            &[&vec![root, child, sibling, independent]],
        )
        .await
        .unwrap();
    let states: HashMap<Uuid, (bool, i64)> = rows
        .iter()
        .map(|r| {
            (
                r.get(0),
                (
                    r.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
                        .is_some(),
                    r.get(2),
                ),
            )
        })
        .collect();
    assert_eq!(states[&root], (false, 1));
    assert_eq!(states[&child], (false, 3));
    assert_eq!(states[&sibling], (true, 2));
    assert_eq!(states[&independent], (true, 2));
    let events = sink.3.lock().unwrap().clone();
    assert_eq!(
        events.iter().map(|e| e.event_type).collect::<Vec<_>>(),
        vec![
            "comment:resolved",
            "comment:resolved",
            "comment:unresolved",
            "comment:resolved"
        ]
    );
    assert_eq!(events[2].payload["comment"]["id"], json!(child));
    assert_eq!(events[2].payload["comment"]["revision"], 3);
    assert_eq!(events[2].workspace_id, ws);
    assert_eq!(events[2].actor_id, user);
    assert!(first_time.is_string());
    let cleared = call(&app, "DELETE", &path(sibling), &token, json!({})).await;
    assert_eq!(cleared.0, StatusCode::OK, "{cleared:?}");
    assert!(cleared.1["resolved_at"].is_null());
    assert_eq!(cleared.1["revision"], 3);
    assert_eq!(
        call(&app, "DELETE", &path(sibling), &token, json!({})).await,
        cleared
    );
    assert_eq!(sink.3.lock().unwrap().len(), 5);
    for invalid in ["not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
        let status = call(
            &app,
            "POST",
            &format!("/api/comments/{invalid}/resolve?workspace_id={ws}"),
            &token,
            json!({}),
        )
        .await
        .0;
        assert_eq!(
            status,
            if invalid == "not-a-uuid" {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::NOT_FOUND
            }
        );
    }
    assert_eq!(
        call(&app, "POST", &path(foreign_comment), &token, json!({}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    db.execute("UPDATE comment SET deleted_at=now() WHERE id=$1", &[&child])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "POST", &path(child), &token, json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(sink.3.lock().unwrap().len(), 5);
    let calls: Vec<_> = (0..8)
        .map(|i| {
            let app = app.clone();
            let path = path(if i % 2 == 0 { root } else { sibling });
            let token = token.clone();
            tokio::spawn(async move { call(&app, "POST", &path, &token, json!({})).await })
        })
        .collect();
    for task in calls {
        let response = task.await.unwrap();
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
    }
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM comment WHERE id=ANY($1) AND resolved_at IS NOT NULL",
            &[&vec![root, sibling]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        count, 1,
        "concurrent resolutions must not leave two thread conclusions"
    );

    // A check-in is a task-scoped write, never a human-authored shortcut.
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Checkin runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Checkin agent','cloud',$2,$3) RETURNING id", &[&ws,&user,&runtime]).await.unwrap().get(0);
    let wakeup: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds) VALUES (gen_random_uuid(),$1,$2,$3,$4,'Check','every','continuous',3600) RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    let run: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context) VALUES ($1,$2,$3,'running',$4) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":wakeup})]).await.unwrap().get(0);
    let task_token = format!("mat_checkin_{suffix}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&run,&agent,&ws,&user]).await.unwrap();
    let wakeup_path = format!("/api/issues/{issue}/wakeups/{wakeup}/checkin?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            "POST",
            &wakeup_path,
            &token,
            json!({"note":"not a run"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let other_rule: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds) VALUES (gen_random_uuid(),$1,$2,$3,$4,'Other','every','continuous',3600) RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{issue}/wakeups/{other_rule}/checkin?workspace_id={ws}"),
            &task_token,
            json!({"note":"wrong rule"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{foreign}/wakeups/{wakeup}/checkin?workspace_id={other}"),
            &task_token,
            json!({"note":"wrong workspace"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &wakeup_path,
            &task_token,
            json!({"note":"  "})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &wakeup_path,
            &task_token,
            json!({"note":"nothing new"})
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let context: Value = db
        .query_one("SELECT context FROM agent_task_queue WHERE id=$1", &[&run])
        .await
        .unwrap()
        .get(0);
    assert_eq!(context["wakeup_checkin"]["note"], "nothing new");
    let log: Value = db
        .query_one(
            "SELECT details FROM activity_log WHERE issue_id=$1 AND action='wakeup_checkin'",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(log["task_id"], json!(run));
    assert_eq!(log["wakeup"]["kind"], "every");
    let event = sink.4.lock().unwrap()[0].clone();
    assert_eq!(event.workspace_id, ws);
    assert_eq!(event.actor_id, agent);
    assert_eq!(event.payload["entry"]["details"], log);
    db.execute(
        "UPDATE agent_task_queue SET status='completed' WHERE id=$1",
        &[&run],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &wakeup_path,
            &task_token,
            json!({"note":"late"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(sink.4.lock().unwrap().len(), 1);

    // Instruction edits fence both the revision and the previous text, without
    // invalidating queued receipts or rearming the rule.
    let instruction_path =
        format!("/api/issues/{issue}/wakeups/{wakeup}/instruction?workspace_id={ws}");
    let edit = json!({"instruction":"  New check  ","expected_instruction":"Check","revision":1});
    assert_eq!(
        call(&app, "PATCH", &instruction_path, &token, edit.clone())
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let state: (String, i64) = {
        let r = db
            .query_one(
                "SELECT instruction,revision FROM issue_wakeup WHERE id=$1",
                &[&wakeup],
            )
            .await
            .unwrap();
        (r.get(0), r.get(1))
    };
    assert_eq!(state, ("New check".into(), 1));
    assert_eq!(
        call(&app, "PATCH", &instruction_path, &token, edit).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &instruction_path,
            &token,
            json!({"instruction":"wrong","expected_instruction":"New check","revision":2})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &instruction_path,
            &token,
            json!({"instruction":" ","expected_instruction":"New check","revision":1})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(call(&app, "PATCH", &instruction_path, &token, json!({"instruction":"oops","expected_instruction":"New check","revision":1,"extra":true})).await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/issues/{foreign}/wakeups/{wakeup}/instruction?workspace_id={other}"),
            &token,
            json!({"instruction":"foreign","expected_instruction":"New check","revision":1})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let stranger: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Stranger',$1) RETURNING id",
            &[&format!("stranger-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &stranger],
    )
    .await
    .unwrap();
    let stranger_token = auth(stranger, now + 3600);
    assert_eq!(
        call(
            &app,
            "PATCH",
            &instruction_path,
            &stranger_token,
            json!({"instruction":"stolen","expected_instruction":"New check","revision":1})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let system: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds,system_rule) VALUES (gen_random_uuid(),$1,$2,$3,$4,'System','every','continuous',3600,'child_done') RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            "PATCH",
            &format!("/api/issues/{issue}/wakeups/{system}/instruction?workspace_id={ws}"),
            &token,
            json!({"instruction":"stolen","expected_instruction":"System","revision":1})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    let trigger_path = format!("/api/issues/{issue}/wakeups/{wakeup}/trigger?workspace_id={ws}");
    for bad_path in [
        format!("/api/issues/{foreign}/wakeups/{wakeup}/trigger?workspace_id={other}"),
        format!("/api/issues/{issue}/wakeups/bad-id/trigger?workspace_id={ws}"),
    ] {
        assert_ne!(
            call(&app, "POST", &bad_path, &token, Value::Null).await.0,
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        call(&app, "POST", &trigger_path, &stranger_token, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{issue}/wakeups/{system}/trigger?workspace_id={ws}"),
            &token,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let manual = call(&app, "POST", &trigger_path, &token, Value::Null).await;
    assert_eq!(manual.0, StatusCode::NO_CONTENT, "{manual:?}");
    let receipt = db.query_one(
        "SELECT revision,event_key,event_type,payload,processed_at FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='wakeup.manual'",
        &[&wakeup],
    ).await.unwrap();
    assert_eq!(receipt.get::<_, i64>(0), 1);
    assert!(receipt.get::<_, String>(1).starts_with("manual:"));
    assert_eq!(receipt.get::<_, String>(2), "wakeup.manual");
    let payload: Value = receipt.get(3);
    assert_eq!(payload["actor_type"], "member");
    assert_eq!(payload["actor_id"], json!(user));
    assert_eq!(payload["event_id"], json!(receipt.get::<_, String>(1)));
    assert!(
        receipt
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(4)
            .is_none()
    );
    assert_eq!(sink.6.lock().unwrap().len(), 1);
    assert_eq!(sink.6.lock().unwrap()[0].wakeup_id, wakeup);
    assert_eq!(sink.6.lock().unwrap()[0].workspace_id, ws);
    assert_eq!(
        call(&app, "POST", &trigger_path, &token, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(sink.6.lock().unwrap().len(), 2);
    let manual_count: i64 = db.query_one(
        "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='wakeup.manual'",
        &[&wakeup],
    ).await.unwrap().get(0);
    assert_eq!(
        manual_count, 2,
        "manual triggers must not collapse distinct requests"
    );
    assert_eq!(manual_count, 2);
    db.execute("UPDATE issue SET status='done' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "POST", &trigger_path, &token, Value::Null)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute("UPDATE issue SET status='todo' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    db.execute(
        "UPDATE issue_wakeup SET disabled_at=now() WHERE id=$1",
        &[&wakeup],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "POST", &trigger_path, &token, Value::Null)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "UPDATE issue_wakeup SET disabled_at=NULL WHERE id=$1",
        &[&wakeup],
    )
    .await
    .unwrap();
    assert_eq!(
        sink.6.lock().unwrap().len(),
        2,
        "rejected triggers cannot reach dispatcher"
    );

    let queued: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context) VALUES ($1,$2,$3,'queued',$4) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":wakeup})]).await.unwrap().get(0);
    let running: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context,started_at) VALUES ($1,$2,$3,'running',$4,now()) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":wakeup})]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES (gen_random_uuid(),$1,1,'pending','task.completed','{}'::jsonb)", &[&wakeup]).await.unwrap();
    // Go keeps the revision stable for instruction-only edits: two writers
    // with identical expected text must serialize on the rule row.
    let edits = ["race A", "race B"].into_iter().map(|text| {
        let app = app.clone();
        let path = instruction_path.clone();
        let token = token.clone();
        tokio::spawn(async move {
            call(
                &app,
                "PATCH",
                &path,
                &token,
                json!({"instruction":text,"expected_instruction":"New check","revision":1}),
            )
            .await
            .0
        })
    });
    let mut tasks: Vec<_> = edits.collect();
    let first = tasks.remove(0);
    let results = [first.await.unwrap(), tasks.remove(0).await.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|s| **s == StatusCode::NO_CONTENT)
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    let revision: i64 = db
        .query_one("SELECT revision FROM issue_wakeup WHERE id=$1", &[&wakeup])
        .await
        .unwrap()
        .get(0);
    assert_eq!(revision, 1);
    // Revoked invocation denies further edits even for the rule creator.
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "PATCH",
            &instruction_path,
            &token,
            json!({"instruction":"not allowed","expected_instruction":"race A","revision":1})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    db.execute("UPDATE agent SET archived_at=NULL WHERE id=$1", &[&agent])
        .await
        .unwrap();
    let disable_path = format!("/api/issues/{issue}/wakeups/{wakeup}/disable?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &disable_path, &stranger_token, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let disabled = call(&app, "POST", &disable_path, &token, Value::Null).await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    assert_eq!(disabled.1["enabled"], false);
    assert!(!disabled.1["disabled_at"].is_null());
    let task_status: String = db
        .query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&queued],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(task_status, "cancelled");
    let running_status: String = db
        .query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&running],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(running_status, "running");
    let pending: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL",
            &[&wakeup],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pending, 0);
    let cancellations = sink.5.lock().unwrap().clone();
    assert_eq!(cancellations.len(), 1);
    assert_eq!(
        (
            cancellations[0].workspace_id,
            cancellations[0].task_id,
            cancellations[0].agent_id,
            cancellations[0].issue_id
        ),
        (ws, queued, agent, Some(issue))
    );
    assert_eq!(
        call(&app, "POST", &disable_path, &token, Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        sink.5.lock().unwrap().len(),
        1,
        "idempotent disable cannot rebroadcast cancellation"
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{issue}/wakeups/{system}?workspace_id={ws}"),
            &token,
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
            &format!("/api/issues/{issue}/wakeups/{wakeup}?workspace_id={ws}"),
            &stranger_token,
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
            &format!("/api/issues/{issue}/wakeups/{wakeup}?workspace_id={ws}"),
            &token,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let remaining: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&wakeup],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(remaining, 0);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{issue}/wakeups/{wakeup}?workspace_id={ws}"),
            &token,
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(sink.5.lock().unwrap().len(), 1);
    let delete_rule: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds) VALUES (gen_random_uuid(),$1,$2,$3,$4,'Delete queued','every','continuous',3600) RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    let delete_task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context) VALUES ($1,$2,$3,'deferred',$4) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":delete_rule})]).await.unwrap().get(0);
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES (gen_random_uuid(),$1,1,'delete-pending','task.completed','{}'::jsonb)", &[&delete_rule]).await.unwrap();
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{issue}/wakeups/{delete_rule}?workspace_id={ws}"),
            &token,
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let deleted_status: String = db
        .query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&delete_task],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(deleted_status, "cancelled");
    assert_eq!(sink.5.lock().unwrap()[1].task_id, delete_task);
    let deleted_receipts: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&delete_rule],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(deleted_receipts, 0);
    // Enabling re-arms supported persisted schedules without losing filters.
    let enable_rule: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds,disabled_at,enabled,revision) VALUES (gen_random_uuid(),$1,$2,$3,$4,'Enable','every','continuous',3600,now(),false,3) RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    let enable_path = format!("/api/issues/{issue}/wakeups/{enable_rule}/enable?workspace_id={ws}");
    db.execute("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES (gen_random_uuid(),$1,3,'old-event','comment.created','{}'::jsonb)", &[&enable_rule]).await.unwrap();
    let queued_enable: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context) VALUES ($1,$2,$3,'queued',$4) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":enable_rule})]).await.unwrap().get(0);
    let old_events = sink.5.lock().unwrap().len();
    for (path, body, expected) in [
        (&enable_path, json!({"revision":2}), StatusCode::CONFLICT),
        (
            &enable_path,
            json!({"revision":3,"at":"2027-01-01T00:00:00Z"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            &enable_path,
            json!({"revision":3,"unexpected":1}),
            StatusCode::BAD_REQUEST,
        ),
        (
            &format!("/api/issues/{foreign}/wakeups/{enable_rule}/enable?workspace_id={other}"),
            json!({"revision":3}),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let result = call(&app, "POST", path, &token, body).await;
        assert_eq!(result.0, expected, "{result:?}");
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &enable_path,
            &stranger_token,
            json!({"revision":3})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL",
            &[&enable_rule]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute("UPDATE agent SET archived_at=now() WHERE id=$1", &[&agent])
        .await
        .unwrap();
    assert_eq!(
        call(&app, "POST", &enable_path, &token, json!({"revision":3}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute("UPDATE agent SET archived_at=NULL WHERE id=$1", &[&agent])
        .await
        .unwrap();
    let enabled = call(&app, "POST", &enable_path, &token, json!({"revision":3})).await;
    assert_eq!(enabled.0, StatusCode::OK, "{enabled:?}");
    assert_eq!(enabled.1["revision"], 4);
    assert_eq!(enabled.1["enabled"], true);
    assert!(enabled.1["next_fire_at"].as_str().is_some());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL",
            &[&enable_rule]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&queued_enable]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cancelled"
    );
    assert_eq!(sink.5.lock().unwrap().len(), old_events + 1);
    let already = call(&app, "POST", &enable_path, &token, json!({"revision":4})).await;
    assert_eq!(already.0, StatusCode::OK);
    assert_eq!(already.1["revision"], 4);
    assert_eq!(
        call(
            &app,
            "POST",
            &enable_path,
            &token,
            json!({"revision":4,"rearm":true})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(sink.5.lock().unwrap().len(), old_events + 1);
    let at_rule: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,disabled_at,enabled,next_fire_at) VALUES (gen_random_uuid(),$1,$2,$3,$4,'At','at','once',now(),false,now() - interval '1 minute') RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    let at_path = format!("/api/issues/{issue}/wakeups/{at_rule}/enable?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &at_path, &token, json!({"revision":1}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let at = call(
        &app,
        "POST",
        &at_path,
        &token,
        json!({"revision":1,"at":"2027-01-01T00:00:00Z"}),
    )
    .await;
    assert_eq!(at.0, StatusCode::OK, "{at:?}");
    assert_eq!(at.1["revision"], 2);
    let race_rule: Uuid = db.query_one("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,interval_seconds,disabled_at,enabled,revision) VALUES (gen_random_uuid(),$1,$2,$3,$4,'Race','every','continuous',120,now(),false,5) RETURNING id", &[&ws,&issue,&agent,&user]).await.unwrap().get(0);
    let race_path = format!("/api/issues/{issue}/wakeups/{race_rule}/enable?workspace_id={ws}");
    let (first, second) = tokio::join!(
        call(&app, "POST", &race_path, &token, json!({"revision":5})),
        call(&app, "POST", &race_path, &token, json!({"revision":5})),
    );
    assert_eq!(
        [first.0, second.0]
            .iter()
            .filter(|status| **status == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [first.0, second.0]
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT revision FROM issue_wakeup WHERE id=$1",
            &[&race_rule]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        6
    );
    // Creating scheduled rules commits their timeline entry with the rule and
    // emits exactly one post-commit workspace event. Unsupported kinds cannot
    // create an inert, falsely successful subscription.
    db.execute(
        "UPDATE issue SET status='in_progress' WHERE id=$1",
        &[&issue],
    )
    .await
    .unwrap();
    let create_path = format!("/api/issues/{issue}/wakeups?workspace_id={ws}");
    let scheduled = json!({"agent_id":agent,"instruction":"  Report progress  ","kind":"every","interval_seconds":3600});
    let events_before = sink.4.lock().unwrap().len();
    let (status, rule) = call(&app, "POST", &create_path, &token, scheduled.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{rule:?}");
    assert_eq!(rule["instruction"], "Report progress");
    assert_eq!(rule["mode"], "continuous");
    let created_id: Uuid = rule["id"].as_str().unwrap().parse().unwrap();
    let stored: String = db
        .query_one(
            "SELECT instruction FROM issue_wakeup WHERE id=$1 AND workspace_id=$2",
            &[&created_id, &ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "Report progress");
    let detail: Value = db.query_one("SELECT details FROM activity_log WHERE issue_id=$1 AND action='wakeup_created' AND details->'wakeup'->>'id'=$2", &[&issue,&created_id.to_string()]).await.unwrap().get(0);
    assert_eq!(detail["wakeup"]["kind"], "every");
    let event = sink.4.lock().unwrap()[events_before].clone();
    assert_eq!(event.actor_id, user);
    assert_eq!(event.payload["entry"]["details"], detail);
    assert_eq!(event.payload["entry"]["action"], "wakeup_created");
    let deadline = json!({"agent_id":agent,"instruction":"Timed report","kind":"every","interval_seconds":3600,"expires_in_seconds":7200});
    let (deadline_status, deadline_rule) = call(&app, "POST", &create_path, &token, deadline).await;
    assert_eq!(deadline_status, StatusCode::CREATED, "{deadline_rule:?}");
    let deadline_id: Uuid = deadline_rule["id"].as_str().unwrap().parse().unwrap();
    let stored_deadline = db.query_one(
        "SELECT expiry_seconds,on_timeout,expires_at,next_fire_at FROM issue_wakeup WHERE id=$1",
        &[&deadline_id],
    ).await.unwrap();
    assert_eq!(stored_deadline.get::<_, Option<i64>>(0), Some(7200));
    assert_eq!(
        stored_deadline.get::<_, Option<String>>(1).as_deref(),
        Some("end")
    );
    assert!(
        stored_deadline.get::<_, chrono::DateTime<chrono::Utc>>(2)
            > stored_deadline.get::<_, chrono::DateTime<chrono::Utc>>(3)
    );
    let zoned = json!({"agent_id":agent,"instruction":"Zoned report","kind":"every","interval_seconds":3600,"timezone":"Asia/Ho_Chi_Minh","max_fires":5});
    let (zoned_status, zoned_rule) = call(&app, "POST", &create_path, &token, zoned).await;
    assert_eq!(zoned_status, StatusCode::CREATED, "{zoned_rule:?}");
    assert_eq!(zoned_rule["timezone"], "Asia/Ho_Chi_Minh");
    assert_eq!(zoned_rule["max_fires"], 5);
    let zoned_id: Uuid = zoned_rule["id"].as_str().unwrap().parse().unwrap();
    let persisted_zone: String = db
        .query_one(
            "SELECT timezone FROM issue_wakeup WHERE id=$1",
            &[&zoned_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(persisted_zone, "Asia/Ho_Chi_Minh");
    // UTC cron uses DB time and persists the original five-field expression.
    let cron_body = json!({"agent_id":agent,"instruction":"Daily check","kind":"cron","cron_expression":"0 9 * * *"});
    let (cron_status, cron_rule) =
        call(&app, "POST", &create_path, &token, cron_body.clone()).await;
    assert_eq!(cron_status, StatusCode::CREATED, "{cron_rule:?}");
    assert_eq!(cron_rule["mode"], "continuous");
    assert_eq!(cron_rule["cron_expression"], "0 9 * * *");
    let cron_id: Uuid = cron_rule["id"].as_str().unwrap().parse().unwrap();
    let cron_row = db
        .query_one(
            "SELECT cron_expression,next_fire_at FROM issue_wakeup WHERE id=$1",
            &[&cron_id],
        )
        .await
        .unwrap();
    assert_eq!(cron_row.get::<_, String>(0), "0 9 * * *");
    let first: chrono::DateTime<chrono::Utc> = cron_row.get(1);
    assert_eq!(first.hour(), 9);
    assert_eq!(first.minute(), 0);
    let cron_disable = format!("/api/issues/{issue}/wakeups/{cron_id}/disable?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &cron_disable, &token, Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    let cron_enable = format!("/api/issues/{issue}/wakeups/{cron_id}/enable?workspace_id={ws}");
    let (enabled_status, enabled) =
        call(&app, "POST", &cron_enable, &token, json!({"revision":1})).await;
    assert_eq!(enabled_status, StatusCode::OK, "{enabled:?}");
    assert_eq!(enabled["cron_expression"], "0 9 * * *");
    let next: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT next_fire_at FROM issue_wakeup WHERE id=$1",
            &[&cron_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(next.hour(), 9);
    assert_eq!(next.minute(), 0);
    assert!(next >= first);
    // Named timezone fires at local 09:00; disable/rearm must use the same zone.
    let mut zoned_cron = cron_body.clone();
    zoned_cron["timezone"] = json!("Asia/Ho_Chi_Minh");
    let (zone_status, zone_rule) = call(&app, "POST", &create_path, &token, zoned_cron).await;
    assert_eq!(zone_status, StatusCode::CREATED, "{zone_rule:?}");
    let zone_id: Uuid = zone_rule["id"].as_str().unwrap().parse().unwrap();
    let zone_next: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT next_fire_at FROM issue_wakeup WHERE id=$1",
            &[&zone_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((zone_next.hour(), zone_next.minute()), (2, 0));
    let zone_disable = format!("/api/issues/{issue}/wakeups/{zone_id}/disable?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &zone_disable, &token, Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    let zone_enable = format!("/api/issues/{issue}/wakeups/{zone_id}/enable?workspace_id={ws}");
    let (zone_rearm, _) = call(&app, "POST", &zone_enable, &token, json!({"revision":1})).await;
    assert_eq!(zone_rearm, StatusCode::OK);
    let zone_next: chrono::DateTime<chrono::Utc> = db
        .query_one(
            "SELECT next_fire_at FROM issue_wakeup WHERE id=$1",
            &[&zone_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((zone_next.hour(), zone_next.minute()), (2, 0));
    let before_unsupported: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    let mut half_hour_dst = cron_body.clone();
    half_hour_dst["timezone"] = json!("Australia/Lord_Howe");
    assert_eq!(
        call(&app, "POST", &create_path, &token, half_hour_dst)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let mut invalid_cron = cron_body.clone();
    invalid_cron["timezone"] = json!("Invalid/Zone");
    assert_eq!(
        call(&app, "POST", &create_path, &token, invalid_cron)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let after_unsupported: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        before_unsupported, after_unsupported,
        "rejected cron must not persist"
    );
    // The database trigger must capture the subscribed event in the same
    // transaction as the source write, without waiting for an in-memory bus.
    let subscription = json!({"agent_id":agent,"instruction":"Watch comments","kind":"event","mode":"continuous","event_types":["comment.created"]});
    let (subscription_status, subscription_rule) =
        call(&app, "POST", &create_path, &token, subscription).await;
    assert_eq!(
        subscription_status,
        StatusCode::CREATED,
        "{subscription_rule:?}"
    );
    assert_eq!(subscription_rule["max_fires"], 20);
    assert_eq!(subscription_rule["next_fire_at"], Value::Null);
    let subscription_id: Uuid = subscription_rule["id"].as_str().unwrap().parse().unwrap();
    let fresh_comment: Uuid = db.query_one(
        "INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'wake event') RETURNING id",
        &[&issue,&ws,&user],
    ).await.unwrap().get(0);
    let receipt = db.query_one(
        "SELECT event_type,payload FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL",
        &[&subscription_id],
    ).await.unwrap();
    assert_eq!(receipt.get::<_, String>(0), "comment.created");
    assert_eq!(
        receipt.get::<_, Value>(1)["comment_id"],
        json!(fresh_comment)
    );
    let actor_filter = json!({"agent_id":agent,"instruction":"Watch member","kind":"event","event_types":["comment.created"],"filter_actor_type":"member","filter_actor_id":user});
    let (actor_status, actor_rule) = call(&app, "POST", &create_path, &token, actor_filter).await;
    assert_eq!(actor_status, StatusCode::CREATED, "{actor_rule:?}");
    let actor_rule_id: Uuid = actor_rule["id"].as_str().unwrap().parse().unwrap();
    db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'filtered event') RETURNING id", &[&issue,&ws,&user]).await.unwrap();
    let captured: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&actor_rule_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(captured, 1);
    let agent_filter = json!({"agent_id":agent,"instruction":"Watch agent","kind":"event","event_types":["comment.created"],"filter_agent_id":agent});
    let (filter_status, filter_rule) = call(&app, "POST", &create_path, &token, agent_filter).await;
    assert_eq!(filter_status, StatusCode::CREATED, "{filter_rule:?}");
    assert_eq!(filter_rule["filter_agent_id"], Value::Null);
    assert_eq!(filter_rule["filter_actor_type"], "agent");
    assert_eq!(filter_rule["filter_actor_id"], json!(agent));
    let agent_rule_id: Uuid = filter_rule["id"].as_str().unwrap().parse().unwrap();
    db.query_one("INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content) VALUES ($1,$2,'member',$3,'not an agent') RETURNING id", &[&issue,&ws,&user]).await.unwrap();
    let not_captured: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&agent_rule_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(not_captured, 0);
    for foreign_filter in [
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":["comment.created"],"filter_agent_id":Uuid::new_v4()}),
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":["comment.created"],"filter_actor_type":"member","filter_actor_id":Uuid::new_v4()}),
    ] {
        assert_eq!(
            call(&app, "POST", &create_path, &token, foreign_filter)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    // A source task is locked until subscription and terminal snapshot commit.
    // Task events arriving afterwards are captured by the database trigger.
    let terminal: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,completed_at) VALUES ($1,$2,$3,'completed',now()) RETURNING id",
        &[&agent,&runtime,&issue],
    ).await.unwrap().get(0);
    let completed = json!({"agent_id":agent,"instruction":"Terminal receipt","kind":"event","event_types":["task.completed"],"filter_task_id":terminal});
    let (completed_status, completed_rule) =
        call(&app, "POST", &create_path, &token, completed.clone()).await;
    assert_eq!(completed_status, StatusCode::CREATED, "{completed_rule:?}");
    assert_eq!(completed_rule["filter_task_id"], json!(terminal));
    assert_eq!(completed_rule["enabled"], false);
    let completed_id: Uuid = completed_rule["id"].as_str().unwrap().parse().unwrap();
    let snapshot: Value = db.query_one(
        "SELECT payload FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='task.completed'",
        &[&completed_id],
    ).await.unwrap().get(0);
    assert_eq!(snapshot["registration_snapshot"], true);
    assert_eq!(snapshot["source_task_id"], json!(terminal));
    assert_eq!(snapshot["event_id"], format!("{terminal}:completed"));
    let terminal_enable =
        format!("/api/issues/{issue}/wakeups/{completed_id}/enable?workspace_id={ws}");
    let stale = call(
        &app,
        "POST",
        &terminal_enable,
        &token,
        json!({"revision":1}),
    )
    .await;
    assert_eq!(stale.0, StatusCode::BAD_REQUEST, "{stale:?}");
    let rearmed = call(
        &app,
        "POST",
        &terminal_enable,
        &token,
        json!({"revision":1,"rearm":true}),
    )
    .await;
    assert_eq!(rearmed.0, StatusCode::OK, "{rearmed:?}");
    assert_eq!(rearmed.1["revision"], 2);
    assert_eq!(rearmed.1["enabled"], false);
    let snapshots = db.query("SELECT revision,processed_at,payload FROM issue_wakeup_receipt WHERE wakeup_id=$1 ORDER BY revision", &[&completed_id]).await.unwrap();
    assert_eq!(snapshots.len(), 2);
    assert!(
        snapshots[0]
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
            .is_some()
    );
    assert_eq!(snapshots[1].get::<_, i64>(0), 2);
    assert!(
        snapshots[1]
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
            .is_none()
    );
    assert_eq!(
        snapshots[1].get::<_, Value>(2)["registration_snapshot"],
        true
    );
    let retry = call(
        &app,
        "POST",
        &terminal_enable,
        &token,
        json!({"revision":1,"rearm":true}),
    )
    .await;
    assert_eq!(retry.0, StatusCode::CONFLICT, "{retry:?}");
    let pending: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running') RETURNING id",
        &[&agent,&runtime,&issue],
    ).await.unwrap().get(0);
    let pending_body = json!({"agent_id":agent,"instruction":"Wait for completion","kind":"event","event_types":["task.completed"],"filter_task_id":pending,"filter_agent_id":agent});
    let (pending_status, pending_rule) =
        call(&app, "POST", &create_path, &token, pending_body.clone()).await;
    assert_eq!(pending_status, StatusCode::CREATED, "{pending_rule:?}");
    assert_eq!(pending_rule["enabled"], true);
    let pending_id: Uuid = pending_rule["id"].as_str().unwrap().parse().unwrap();
    let before: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&pending_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 0);
    db.execute(
        "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1",
        &[&pending],
    )
    .await
    .unwrap();
    let after: i64 = db.query_one("SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='task.completed'", &[&pending_id]).await.unwrap().get(0);
    assert_eq!(after, 1);
    let pending_enable =
        format!("/api/issues/{issue}/wakeups/{pending_id}/enable?workspace_id={ws}");
    let disabled = call(
        &app,
        "POST",
        &format!("/api/issues/{issue}/wakeups/{pending_id}/disable?workspace_id={ws}"),
        &token,
        Value::Null,
    )
    .await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    let revision = disabled.1["revision"].as_i64().unwrap();
    let resumed = call(
        &app,
        "POST",
        &pending_enable,
        &token,
        json!({"revision":revision,"rearm":true}),
    )
    .await;
    assert_eq!(resumed.0, StatusCode::OK, "{resumed:?}");
    assert_eq!(resumed.1["enabled"], false);
    assert_eq!(resumed.1["revision"], revision + 1);
    let new_snapshot: Value = db.query_one("SELECT payload FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND revision=$2 AND processed_at IS NULL", &[&pending_id,&(revision+1)]).await.unwrap().get(0);
    assert_eq!(new_snapshot["source_task_id"], json!(pending));
    let mut wrong_agent = pending_body.clone();
    wrong_agent["filter_agent_id"] = json!(Uuid::new_v4());
    assert_eq!(
        call(&app, "POST", &create_path, &token, wrong_agent)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut missing_task = pending_body.clone();
    missing_task["filter_task_id"] = json!(Uuid::new_v4());
    assert_eq!(
        call(&app, "POST", &create_path, &token, missing_task)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let foreign_task: Uuid = db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running') RETURNING id",
        &[&agent,&runtime,&foreign],
    ).await.unwrap().get(0);
    let mut body = pending_body.clone();
    body["filter_task_id"] = json!(foreign_task);
    assert_eq!(
        call(&app, "POST", &create_path, &token, body).await.0,
        StatusCode::BAD_REQUEST
    );
    let mut body = pending_body.clone();
    body["event_types"] = json!(["task.completed", "comment.created"]);
    assert_eq!(
        call(&app, "POST", &create_path, &token, body).await.0,
        StatusCode::BAD_REQUEST
    );
    let invalid_path = format!("/api/issues/{foreign}/wakeups?workspace_id={other}");
    assert_eq!(
        call(&app, "POST", &invalid_path, &token, scheduled.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    for body in [
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"condition":{"type":"status"}}),
    ] {
        assert_eq!(
            call(&app, "POST", &create_path, &token, body).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    for body in [
        json!({"agent_id":agent,"instruction":" ","kind":"every","interval_seconds":3600}),
        json!({"agent_id":agent,"instruction":"x","kind":"at","after_seconds":0}),
        json!({"agent_id":agent,"instruction":"x","kind":"cron","cron_expression":"60 9 * * *"}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":59}),
        json!({"agent_id":agent,"instruction":"x","kind":"at","at":"invalid"}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"unexpected":true}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"expires_in_seconds":59}),
        json!({"agent_id":agent,"instruction":"x","kind":"at","after_seconds":3600,"expires_in_seconds":3600}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"expires_in_seconds":3600,"on_timeout":"wake"}),
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":["issue.created"]}),
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":[]}),
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":["comment.created"],"mode":"once","max_fires":1}),
        json!({"agent_id":agent,"instruction":"x","kind":"event","event_types":["comment.created"],"mode":"continuous","max_fires":1001}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"timezone":"../../etc/passwd"}),
        json!({"agent_id":agent,"instruction":"x","kind":"every","interval_seconds":3600,"timezone":"Mars/Phobos"}),
    ] {
        assert_eq!(
            call(&app, "POST", &create_path, &token, body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(sink.4.lock().unwrap().len(), events_before + 10);
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM activity_log WHERE issue_id=$1 AND action='wakeup_created'",
            &[&issue],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 10);
}
