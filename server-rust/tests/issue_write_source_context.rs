//! Synthetic HTTP/DB capture test; ignored unless run against owned disposable PostgreSQL.
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::{ApiState, issue_write::*},
    health::{Health, MigrationCatalog},
    migrate::{self, Direction, Stream},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;
#[derive(Default)]
struct Sink {
    events: Mutex<Vec<IssueCreatedEvent>>,
}
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected event")
    }
    fn publish_issue_created(&self, event: IssueCreatedEvent) {
        self.events.lock().unwrap().push(event);
    }
    fn publish_issue_updated(&self, _: IssueUpdatedEvent) {
        panic!("unexpected event")
    }
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {
        panic!("unexpected event")
    }
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {
        panic!("unexpected event")
    }
    fn publish_comment_created(&self, _: IssueCommentCreatedEvent) {
        panic!("unexpected event")
    }
    fn publish_comment_changed(&self, _: IssueCommentChangedEvent) {
        panic!("unexpected event")
    }
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {
        panic!("unexpected event")
    }
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {
        panic!("unexpected event")
    }
    fn publish_comment_resolution(&self, _: IssueCommentResolutionEvent) {
        panic!("unexpected event")
    }
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {
        panic!("unexpected event")
    }
    fn publish_task_cancelled(&self, _: IssueTaskCancelledEvent) {
        panic!("unexpected event")
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected dispatch")
    }
}
struct Runtime;
#[async_trait]
impl IssueCreateRuntime for Runtime {
    async fn count_policy(&self, _: Uuid) -> Result<IssueCountPolicy, ()> {
        Ok(IssueCountPolicy::default())
    }
    fn created(&self, _: &IssueCreatedEvent) {}
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}
#[tokio::test]
#[ignore = "requires owned disposable loopback PostgreSQL multica_zbsi66_source"]
async fn preview_capture_staleness_isolation_and_no_partial_writes() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi66_source"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(h) if h=="127.0.0.1"))
    );
    assert!(cfg.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
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
            "INSERT INTO \"user\"(name,email) VALUES('Owner',$1) RETURNING id",
            &[&format!("source-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("source-other-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Source',$1,'SRC') RETURNING id",
            &[&format!("source-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Other',$1,'OTH') RETURNING id",
            &[&format!("source-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&other, &outsider],
    )
    .await
    .unwrap();
    let source:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,description,creator_type,creator_id,number) VALUES($1,'Source issue','Original','member',$2,1) RETURNING id",&[&ws,&user]).await.unwrap().get(0);
    db.execute("UPDATE workspace SET issue_counter=1 WHERE id=$1", &[&ws])
        .await
        .unwrap();
    let root:Uuid=db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content) VALUES($1,$2,'member',$3,'Root') RETURNING id",&[&ws,&source,&user]).await.unwrap().get(0);
    let anchor:Uuid=db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,parent_id,content) VALUES($1,$2,'member',$3,$4,'Anchor') RETURNING id",&[&ws,&source,&user,&root]).await.unwrap().get(0);
    let sink = Arc::new(Sink::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"source-test-secret".to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_create_runtime(Arc::new(Runtime)),
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let auth = |id: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":now+3600}),
            &EncodingKey::from_secret(b"source-test-secret"),
        )
        .unwrap()
    };
    let path = format!("/api/comments/{anchor}/sub-issue-preview?workspace_id={ws}");
    let expired = encode(
        &Header::default(),
        &json!({"sub":user,"exp":now-3600}),
        &EncodingKey::from_secret(b"source-test-secret"),
    )
    .unwrap();
    assert_eq!(
        call(&app, "GET", &path, &expired, Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
    let preview = call(&app, "GET", &path, &auth(user), Value::Null).await;
    assert_eq!(preview.0, StatusCode::OK, "{preview:?}");
    assert_eq!(preview.1["source_issue"]["id"], source.to_string());
    assert_eq!(preview.1["comment_thread"].as_array().unwrap().len(), 2);
    assert_eq!(preview.1["limits"]["attachment_count"], 0);
    assert_eq!(
        call(&app, "GET", &path, &auth(outsider), Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let create_path = format!("/api/comments/{anchor}/sub-issues?workspace_id={ws}");
    let token = preview.1["capture_token"].clone();
    let req = json!({"mode":"manual","capture_token":token,"issue":{"title":"  Follow-up  ","parent_issue_id":"invalid-parent"}});
    let agent = call(
        &app,
        "POST",
        &create_path,
        &auth(user),
        json!({"mode":"agent","capture_token":token}),
    )
    .await;
    assert_eq!(agent.0, StatusCode::SERVICE_UNAVAILABLE, "{agent:?}");
    assert_eq!(call(&app,"POST",&create_path,&auth(user),json!({"mode":"manual","capture_token":token,"issue":{"title":"Attachment","attachment_ids":[Uuid::new_v4()]}})).await.0,StatusCode::SERVICE_UNAVAILABLE);
    let stale = json!({"mode":"manual","capture_token":"sha256:invalid","issue":{"title":"No"}});
    assert_eq!(
        call(&app, "POST", &create_path, &auth(user), stale).await.0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE comment SET content='Changed',revision=revision+1 WHERE id=$1",
        &[&root],
    )
    .await
    .unwrap();
    let mismatch = call(&app, "POST", &create_path, &auth(user), req.clone()).await;
    assert_eq!(mismatch.0, StatusCode::CONFLICT, "{mismatch:?}");
    assert_eq!(mismatch.1["code"], "source_context_changed");
    assert_eq!(
        db.query_one("SELECT count(*) FROM issue WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_source_context WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let fresh = call(&app, "GET", &path, &auth(user), Value::Null).await;
    assert_eq!(fresh.0, StatusCode::OK, "{fresh:?}");
    let created=call(&app,"POST",&create_path,&auth(user),json!({"mode":"manual","capture_token":fresh.1["capture_token"],"issue":{"title":"  Follow-up  ","parent_issue_id":"invalid-parent","origin_type":"quick_create","origin_id":Uuid::new_v4()}})).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    assert_eq!(created.1["parent_issue_id"], source.to_string());
    assert_eq!(created.1["title"], "Follow-up");
    let child = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let origin = db
        .query_one(
            "SELECT origin_type,origin_id FROM issue WHERE id=$1",
            &[&child],
        )
        .await
        .unwrap();
    assert_eq!(origin.get::<_, Option<String>>("origin_type"), None);
    assert_eq!(origin.get::<_, Option<Uuid>>("origin_id"), None);
    let captured=db.query_one("SELECT snapshot,snapshot_version,capture_digest,source_issue_id,anchor_comment_id,captured_by_user_id,state FROM issue_source_context WHERE issue_id=$1",&[&child]).await.unwrap();
    let snapshot: Value = captured.get("snapshot");
    assert_eq!(snapshot["comment_thread"][0]["content"], "Changed");
    assert_eq!(snapshot["version"], 1);
    assert_eq!(snapshot["captured_by_user_id"], user.to_string());
    assert_eq!(captured.get::<_, i16>("snapshot_version"), 1);
    assert_eq!(captured.get::<_, Uuid>("source_issue_id"), source);
    assert_eq!(captured.get::<_, Uuid>("anchor_comment_id"), anchor);
    assert_eq!(captured.get::<_, Uuid>("captured_by_user_id"), user);
    assert_eq!(captured.get::<_, String>("state"), "attached");
    assert_eq!(
        format!(
            "sha256:{}:{}",
            captured.get::<_, String>("capture_digest"),
            source
        ),
        fresh.1["capture_token"]
    );
    assert_eq!(sink.events.lock().unwrap().len(), 1);
    let duplicate=call(&app,"POST",&create_path,&auth(user),json!({"mode":"manual","capture_token":fresh.1["capture_token"],"issue":{"title":"Follow-up"}})).await;
    assert_eq!(duplicate.0, StatusCode::CONFLICT, "{duplicate:?}");
    assert_eq!(duplicate.1["code"], "active_duplicate_issue");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_source_context WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let file:Uuid=db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'member',$3,'file.txt','https://example.invalid/file','text/plain',1) RETURNING id",&[&ws,&source,&user]).await.unwrap().get(0);
    let no_file = call(&app, "GET", &path, &auth(user), Value::Null).await;
    assert_eq!(no_file.0, StatusCode::SERVICE_UNAVAILABLE, "{no_file:?}");
    assert_eq!(no_file.1["code"], "source_context_attachments_unsupported");
    let blocked=call(&app,"POST",&create_path,&auth(user),json!({"mode":"manual","capture_token":fresh.1["capture_token"],"issue":{"title":"Other follow-up"}})).await;
    assert_eq!(blocked.0, StatusCode::SERVICE_UNAVAILABLE, "{blocked:?}");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_source_context WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.execute("DELETE FROM attachment WHERE id=$1", &[&file])
        .await
        .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap();
    db.execute("DELETE FROM workspace WHERE id=$1", &[&other])
        .await
        .unwrap();
    db.execute(
        "DELETE FROM \"user\" WHERE id=$1 OR id=$2",
        &[&user, &outsider],
    )
    .await
    .unwrap();
}
