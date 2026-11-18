//! Disposable loopback PostgreSQL. Runtime and events are recording test doubles.
use async_trait::async_trait;
use axum::{
    Router,
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

#[path = "issue_write_comment_legacy/cases.rs"]
mod legacy_cases;
#[path = "issue_write_comment_lineage/cases.rs"]
mod lineage_cases;
#[path = "issue_write_comment_projection/cases.rs"]
mod projection_cases;
#[path = "issue_write_comment_reopen/cases.rs"]
mod reopen_cases;

#[derive(Default)]
struct Sink {
    created: Mutex<Vec<IssueCommentCreatedEvent>>,
    changed: Mutex<Vec<IssueCommentChangedEvent>>,
    resolved: Mutex<Vec<IssueCommentResolutionEvent>>,
}
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected event")
    }
    fn publish_issue_created(&self, _: IssueCreatedEvent) {
        panic!("unexpected issue create");
    }
    fn publish_comment_created(&self, e: IssueCommentCreatedEvent) {
        self.created.lock().unwrap().push(e);
    }
    fn publish_comment_changed(&self, e: IssueCommentChangedEvent) {
        self.changed.lock().unwrap().push(e);
    }
    fn publish_comment_resolution(&self, e: IssueCommentResolutionEvent) {
        self.resolved.lock().unwrap().push(e);
    }
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {
        panic!("unexpected event");
    }
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {
        panic!("unexpected event");
    }
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {
        panic!("unexpected event");
    }
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {
        panic!("unexpected subscription event");
    }
    fn publish_issue_updated(&self, _: IssueUpdatedEvent) {
        panic!("unexpected issue update");
    }
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {
        panic!("unexpected event");
    }
    fn publish_task_cancelled(&self, _: IssueTaskCancelledEvent) {
        panic!("unexpected event");
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected dispatch");
    }
}
#[derive(Default)]
struct Runtime(
    Mutex<Vec<CommentCreatedAction>>,
    Mutex<Vec<CommentChangedAction>>,
);
#[async_trait]
impl IssueCommentRuntime for Runtime {
    async fn created(&self, a: CommentCreatedAction) -> CommentFeedback {
        self.0.lock().unwrap().push(a);
        CommentFeedback::default()
    }
    async fn changed(&self, a: CommentChangedAction) -> CommentFeedback {
        self.1.lock().unwrap().push(a);
        CommentFeedback::default()
    }
}
async fn call(
    app: &Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let body = body.to_string();
    call_raw(app, method, path, token, body.as_bytes()).await
}
async fn call_raw(
    app: &Router,
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
async fn create_comment_http_db_events_and_runtime_fences() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi66"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(h) if h=="127.0.0.1"))
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
    let member: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Comment owner',$1) RETURNING id",
            &[&format!("comment-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES ('Outside',$1) RETURNING id",
            &[&format!("outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws:Uuid=db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Comment',$1,'CMNT') RETURNING id", &[&format!("comment-{suffix}")]).await.unwrap().get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES ('Other',$1,'OTHR') RETURNING id",
            &[&format!("other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &member],
    )
    .await
    .unwrap();
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Comment issue','member',$2,777) RETURNING id", &[&ws,&member]).await.unwrap().get(0);
    let foreign:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES ($1,'Other issue','member',$2,777) RETURNING id", &[&other,&member]).await.unwrap().get(0);
    let foreign_comment: Uuid=db.query_one("INSERT INTO comment(workspace_id,issue_id,author_type,author_id,content,type) VALUES ($1,$2,'member',$3,'foreign','comment') RETURNING id",&[&other,&foreign,&member]).await.unwrap().get(0);
    let attachment:Uuid=db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,'member',$3,'test.txt','synthetic://comment','text/plain',2) RETURNING id", &[&ws,&issue,&member]).await.unwrap().get(0);
    let sink = Arc::new(Sink::default());
    let runtime = Arc::new(Runtime::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let secret = b"comment-synthetic-secret";
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), secret.to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_comment_runtime(runtime.clone()),
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let auth = |user: Uuid, exp: u64| {
        encode(
            &Header::default(),
            &json!({"sub":user,"exp":exp}),
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    };
    let token = auth(member, now + 3600);
    let path = |id: &str| format!("/api/issues/{id}/comments?workspace_id={ws}");
    let p = path("CMNT-777");
    for body in [
        json!({}),
        json!({"content":"\u{0}"}),
        json!({"content":"x","type":"system"}),
        json!({"content":"x","parent_id":"bad"}),
    ] {
        assert_eq!(
            call(&app, "POST", &p, &token, body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &p,
            &auth(outsider, now + 3600),
            json!({"content":"x"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &p, &auth(member, 1), json!({"content":"x"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for id in [foreign.to_string(), "OTHR-777".into(), "invalid".into()] {
        assert_eq!(
            call(&app, "POST", &path(&id), &token, json!({"content":"x"}))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            "POST",
            &p,
            &token,
            json!({"content":"x","parent_id":foreign})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let wrong_attachment = Uuid::new_v4();
    let failed = call(
        &app,
        "POST",
        &p,
        &token,
        json!({"content":"rollback","attachment_ids":[attachment,wrong_attachment]}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::CONFLICT, "{failed:?}");
    let after_fail: i64 = db
        .query_one("SELECT count(*) FROM comment WHERE issue_id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert_eq!(after_fail, 0);
    let unbound: Option<Uuid> = db
        .query_one(
            "SELECT comment_id FROM attachment WHERE id=$1",
            &[&attachment],
        )
        .await
        .unwrap()
        .get(0);
    assert!(unbound.is_none());
    assert!(sink.created.lock().unwrap().is_empty());
    assert!(runtime.0.lock().unwrap().is_empty());
    let bad_element = call_raw(
        &app,
        "POST",
        &p,
        &token,
        br#"{"content":"x","attachment_ids":[null]}"#,
    )
    .await;
    assert_eq!(bad_element.0, StatusCode::BAD_REQUEST);
    assert_eq!(bad_element.1["error"], "invalid attachment_ids");
    let first_body = format!(
        r#"{{"content":"bad","CONTENT":"root \u0000 **markdown**","content":null,"attachment_ids":["bad"],"ATTACHMENT_IDS":["{attachment}"],"type":"progress_update","TYPE":null}}"#
    );
    let first = call_raw(&app, "POST", &p, &token, first_body.as_bytes()).await;
    assert_eq!(first.0, StatusCode::CREATED, "{first:?}");
    let root_id = Uuid::parse_str(first.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(root_id.get_version_num(), 7);
    assert_eq!(first.1["content"], "root  **markdown**");
    assert_eq!(first.1["attachments"][0]["id"], json!(attachment));
    assert_eq!(first.1["reactions"], json!([]));
    let revision = first.1["issue_revision"].as_i64().unwrap();
    let issue_row = db
        .query_one(
            "SELECT revision,updated_at,last_activity_at FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(issue_row.get::<_, i64>("revision"), revision);
    assert!(
        issue_row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_activity_at")
            .is_some()
    );
    assert_eq!(
        sink.created.lock().unwrap()[0].payload["comment"]["id"],
        first.1["id"]
    );
    assert!(
        sink.created.lock().unwrap()[0].payload["comment"]
            .get("trigger_outcomes")
            .is_none()
    );
    assert_eq!(runtime.0.lock().unwrap()[0].member_originator, Some(member));
    db.execute("UPDATE comment SET resolved_at=now(),resolved_by_type='member',resolved_by_id=$1 WHERE id=$2", &[&member,&root_id]).await.unwrap();
    let reply = call(
        &app,
        "POST",
        &path(&issue.to_string()),
        &token,
        json!({"content":"reply","parent_id":root_id}),
    )
    .await;
    assert_eq!(reply.0, StatusCode::CREATED, "{reply:?}");
    assert_eq!(reply.1["parent_id"], json!(root_id));
    assert_eq!(sink.resolved.lock().unwrap().len(), 1);
    assert_eq!(
        sink.resolved.lock().unwrap()[0].event_type,
        "comment:unresolved"
    );
    let reopened: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one("SELECT resolved_at FROM comment WHERE id=$1", &[&root_id])
        .await
        .unwrap()
        .get(0);
    assert!(reopened.is_none());
    let req = Request::builder()
        .method("POST")
        .uri(&p)
        .header("cookie", format!("multica_auth={token}"))
        .body(Body::from(r#"{"content":"forbidden"}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let missing_runtime = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), secret.to_vec()).with_issue_event_sink(sink.clone()),
    );
    assert_eq!(
        call(
            &missing_runtime,
            "POST",
            &p,
            &token,
            json!({"content":"not persisted"})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let missing_bus = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), secret.to_vec()).with_issue_comment_runtime(runtime.clone()),
    );
    assert_eq!(
        missing_bus
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&p)
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM comment WHERE issue_id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );

    let edit = |id: Uuid| format!("/api/comments/{id}?workspace_id={ws}");
    let root_path = edit(root_id);
    let reply_id = Uuid::parse_str(reply.1["id"].as_str().unwrap()).unwrap();
    let root_rev: i64 = db
        .query_one("SELECT revision FROM comment WHERE id=$1", &[&root_id])
        .await
        .unwrap()
        .get(0);
    let no_runtime_edit = call(
        &missing_runtime,
        "PUT",
        &root_path,
        &token,
        json!({"content":"not persisted"}),
    )
    .await;
    assert_eq!(no_runtime_edit.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        call(&missing_runtime, "DELETE", &root_path, &token, json!({}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &edit(foreign_comment),
            &token,
            json!({"content":"no"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &edit(Uuid::new_v4()), &token, json!({}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            "/api/comments/not-a-uuid",
            &token,
            json!({"content":"no"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &root_path,
            &auth(member, 1),
            json!({"content":"no"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &root_path,
            &auth(outsider, now + 3600),
            json!({"content":"no"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
        &[&ws, &outsider],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &root_path,
            &auth(outsider, now + 3600),
            json!({"content":"no"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &root_path,
            &auth(outsider, now + 3600),
            json!({})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for body in [
        json!({}),
        json!({"content":"x","expected_revision":0}),
        json!({"content":"x","attachment_ids":["bad"]}),
    ] {
        assert_eq!(
            call(&app, "PUT", &root_path, &token, body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let noop_body = format!(
        r#"{{"content":"wrong","CONTENT":"root  **markdown**","content":null,"content_base":"wrong","CONTENT_BASE":null,"attachment_ids":[],"ATTACHMENT_IDS":null,"expected_revision":0,"EXPECTED_REVISION":{root_rev}}} trailing"#
    );
    let noop = call_raw(&app, "PUT", &root_path, &token, noop_body.as_bytes()).await;
    assert_eq!(noop.0, StatusCode::OK, "{noop:?}");
    assert_eq!(noop.1["revision"], json!(root_rev));
    assert_eq!(noop.1["issue_revision"], Value::Null);
    assert!(runtime.1.lock().unwrap().is_empty());
    let issue_rev_before: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let attachment_only = call(
        &app,
        "PUT",
        &root_path,
        &token,
        json!({"content":"root  **markdown**","attachment_ids":[],"expected_revision":root_rev}),
    )
    .await;
    assert_eq!(attachment_only.0, StatusCode::OK, "{attachment_only:?}");
    assert_eq!(attachment_only.1["revision"], json!(root_rev + 1));
    assert_eq!(attachment_only.1["attachments"], json!([]));
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        issue_rev_before
    );
    let stale = call(
        &app,
        "PUT",
        &root_path,
        &token,
        json!({"content":"stale","expected_revision":root_rev,"attachment_ids":[attachment]}),
    )
    .await;
    assert_eq!(stale.0, StatusCode::CONFLICT, "{stale:?}");
    assert_eq!(stale.1["actual_revision"], json!(root_rev + 1));
    let field_conflict = call(
        &app,
        "PUT",
        &root_path,
        &token,
        json!({"content":"other","content_base":"wrong"}),
    )
    .await;
    assert_eq!(field_conflict.0, StatusCode::CONFLICT, "{field_conflict:?}");
    assert_eq!(field_conflict.1["code"], "revision_conflict");
    assert!(runtime.1.lock().unwrap().is_empty());

    let runtime_id: Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Comment runtime','cloud','synthetic','online') RETURNING id",&[&ws]).await.unwrap().get(0);
    let agent: Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Comment agent','cloud',$2,$3) RETURNING id",&[&ws,&member,&runtime_id]).await.unwrap().get(0);
    let pending: Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,trigger_comment_id,status) VALUES ($1,$2,$3,$4,'queued') RETURNING id",&[&agent,&runtime_id,&issue,&root_id]).await.unwrap().get(0);
    let coalesced: Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,trigger_comment_id,coalesced_comment_ids,status) VALUES ($1,$2,$3,$4,$5,'running') RETURNING id",&[&agent,&runtime_id,&issue,&reply_id,&vec![root_id]]).await.unwrap().get(0);
    let receipt_comment = Uuid::new_v4();
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$6,'pending')",&[&pending,&ws,&issue,&receipt_comment,&member,&Uuid::new_v4()]).await.unwrap();
    let legacy = call(
        &app,
        "PUT",
        &root_path,
        &token,
        json!({"content":"legacy edit"}),
    )
    .await;
    assert_eq!(legacy.0, StatusCode::SERVICE_UNAVAILABLE, "{legacy:?}");
    assert_eq!(
        call(&app, "DELETE", &root_path, &token, json!({})).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        db.query_one(
            "SELECT revision,content FROM comment WHERE id=$1",
            &[&root_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        root_rev + 1
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&pending]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "queued"
    );
    assert!(runtime.1.lock().unwrap().is_empty());
    let edited=call(&app,"PUT",&root_path,&token,json!({"content":"edited","attachment_ids":[attachment],"expected_revision":root_rev+1,"suppress_agent_ids":[agent]})).await;
    assert_eq!(edited.0, StatusCode::OK, "{edited:?}");
    assert_eq!(edited.1["revision"], json!(root_rev + 2));
    assert_eq!(edited.1["attachments"][0]["id"], json!(attachment));
    assert_eq!(edited.1["issue_revision"], json!(issue_rev_before + 1));
    assert_eq!(runtime.1.lock().unwrap().len(), 1);
    let changed = runtime.1.lock().unwrap()[0].clone();
    assert!(!changed.deleted);
    assert_eq!(changed.comment.suppressed_agent_ids, vec![agent]);
    assert_eq!(changed.cancelled_task_ids.len(), 2);
    assert!(changed.cancelled_task_ids.contains(&pending));
    assert!(changed.cancelled_task_ids.contains(&coalesced));
    for task in [pending, coalesced] {
        let row = db
            .query_one(
                "SELECT status,cancelled_by_type,context FROM agent_task_queue WHERE id=$1",
                &[&task],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>("status"), "cancelled");
        assert_eq!(
            row.get::<_, Option<String>>("cancelled_by_type").as_deref(),
            Some("system")
        );
        assert_eq!(
            row.get::<_, Value>("context")["comment_change_cancelled_task_id"],
            json!(task)
        );
    }
    assert_eq!(
        db.query_one(
            "SELECT status FROM task_supplement WHERE task_id=$1",
            &[&pending]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "failed"
    );
    assert_eq!(
        sink.changed.lock().unwrap().last().unwrap().event_type,
        "comment:updated"
    );
    assert_eq!(
        sink.changed.lock().unwrap().last().unwrap().payload["comment"]["content"],
        "edited"
    );
    assert!(
        sink.changed.lock().unwrap().last().unwrap().payload["comment"]
            .get("trigger_outcomes")
            .is_none()
    );
    let conflict_after_edit = call(
        &app,
        "PUT",
        &root_path,
        &token,
        json!({"content":"stale again","expected_revision":root_rev+1}),
    )
    .await;
    assert_eq!(conflict_after_edit.0, StatusCode::CONFLICT);
    assert_eq!(
        db.query_one("SELECT content FROM comment WHERE id=$1", &[&root_id])
            .await
            .unwrap()
            .get::<_, String>(0),
        "edited"
    );
    assert_eq!(runtime.1.lock().unwrap().len(), 1);

    // CAS must be decided under the comment lock; a loser cannot publish or cancel.
    let reply_path = edit(reply_id);
    let reply_rev: i64 = db
        .query_one("SELECT revision FROM comment WHERE id=$1", &[&reply_id])
        .await
        .unwrap()
        .get(0);
    let (left, right) = tokio::join!(
        call(
            &app,
            "PUT",
            &reply_path,
            &token,
            json!({"content":"race left","expected_revision":reply_rev})
        ),
        call(
            &app,
            "PUT",
            &reply_path,
            &token,
            json!({"content":"race right","expected_revision":reply_rev})
        ),
    );
    assert_eq!(
        [left.0, right.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [left.0, right.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(runtime.1.lock().unwrap().len(), 2);

    // A live delivery receipt fences deletion; neither storage nor runtime changes.
    db.execute("INSERT INTO task_supplement(task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status) VALUES ($1,$2,$3,$4,$5,$6,'delivering')",&[&Uuid::new_v4(),&ws,&issue,&reply_id,&member,&Uuid::new_v4()]).await.unwrap();
    let events_before = sink.changed.lock().unwrap().len();
    let rejected = call(&app, "DELETE", &reply_path, &token, json!({})).await;
    assert_eq!(rejected.0, StatusCode::CONFLICT, "{rejected:?}");
    assert_eq!(
        db.query_one("SELECT count(*) FROM comment WHERE id=$1", &[&reply_id])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert_eq!(sink.changed.lock().unwrap().len(), events_before);
    assert_eq!(runtime.1.lock().unwrap().len(), 2);
    db.execute(
        "UPDATE task_supplement SET status='failed' WHERE comment_id=$1",
        &[&reply_id],
    )
    .await
    .unwrap();

    // A deleted parent remains as an empty tombstone until its last reply goes.
    let before_delete = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(
        call(&app, "DELETE", &root_path, &token, json!({})).await.0,
        StatusCode::NO_CONTENT
    );
    let root = db
        .query_one(
            "SELECT content,deleted_at FROM comment WHERE id=$1",
            &[&root_id],
        )
        .await
        .unwrap();
    assert_eq!(root.get::<_, String>("content"), "");
    assert!(
        root.get::<_, Option<chrono::DateTime<chrono::Utc>>>("deleted_at")
            .is_some()
    );
    assert_eq!(
        db.query_one("SELECT parent_id FROM comment WHERE id=$1", &[&reply_id])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(root_id)
    );
    {
        let events = sink.changed.lock().unwrap();
        assert_eq!(events.last().unwrap().event_type, "comment:updated");
        assert!(events.last().unwrap().payload["comment"]["deleted_at"].is_string());
    }
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        before_delete + 1
    );
    assert_eq!(runtime.1.lock().unwrap().len(), 3);
    let deleted = runtime.1.lock().unwrap()[2].clone();
    assert!(deleted.deleted);
    assert_eq!(deleted.deleted_attachment_urls, vec!["synthetic://comment"]);
    assert_eq!(
        call(
            &app,
            "PUT",
            &root_path,
            &token,
            json!({"content":"resurrect"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &root_path, &token, json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &edit(reply_id), &token, json!({}))
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM comment WHERE id=ANY($1::uuid[])",
            &[&vec![root_id, reply_id]]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        sink.changed
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(2)
            .filter(|e| e.event_type == "comment:deleted")
            .count(),
        2
    );
    legacy_cases::run(legacy_cases::Fixture {
        pool: pool.clone(),
        health,
        secret,
        token: &token,
        ws,
        member,
        outsider,
        issue,
        agent,
        runtime_id,
        foreign_comment,
    })
    .await;
    reopen_cases::run(&db, &app, &token, &sink, &runtime, (ws, member)).await;
    lineage_cases::run(&db, &app, &token, &sink, &runtime, (ws, member, outsider)).await;
    projection_cases::run(&db, &app, &token, &sink, &runtime, (ws, member)).await;
}
