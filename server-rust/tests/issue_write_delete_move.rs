//! Isolated deletion, ordering, tenant fences and post-commit delivery.
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
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "issue_write_pull_request/cases.rs"]
mod pull_request_cases;

#[derive(Default)]
struct Sink {
    deleted: Mutex<Vec<IssueDeletedEvent>>,
    updated: Mutex<Vec<IssueUpdatedEvent>>,
}
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, e: IssueDeletedEvent) {
        self.deleted.lock().unwrap().push(e);
    }
    fn publish_issue_updated(&self, e: IssueUpdatedEvent) {
        self.updated.lock().unwrap().push(e);
    }
    fn publish_issue_created(&self, _: IssueCreatedEvent) {
        panic!("unexpected create")
    }
    fn publish_subscriber_changed(&self, _: IssueSubscriberEvent) {
        panic!("unexpected subscriber")
    }
    fn publish_assignment_changed(&self, _: IssueAssignmentEvent) {
        panic!("unexpected assignment")
    }
    fn publish_comment_created(&self, _: IssueCommentCreatedEvent) {
        panic!("unexpected comment")
    }
    fn publish_comment_changed(&self, _: IssueCommentChangedEvent) {
        panic!("unexpected comment")
    }
    fn publish_metadata_changed(&self, _: IssueMetadataEvent) {
        panic!("unexpected metadata")
    }
    fn publish_reaction_changed(&self, _: IssueReactionEvent) {
        panic!("unexpected reaction")
    }
    fn publish_comment_resolution(&self, _: IssueCommentResolutionEvent) {
        panic!("unexpected resolution")
    }
    fn publish_wakeup_activity(&self, _: IssueWakeupActivityEvent) {
        panic!("unexpected wakeup")
    }
    fn publish_task_cancelled(&self, _: IssueTaskCancelledEvent) {
        panic!("unexpected cancellation")
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected dispatch")
    }
}
#[derive(Default)]
struct Runtime {
    prepared: Mutex<Vec<IssueDeletePreparation>>,
    urls: Mutex<Vec<String>>,
    deny: AtomicBool,
}
#[async_trait]
impl IssueDeleteRuntime for Runtime {
    async fn prepare_delete(&self, a: &IssueDeletePreparation) -> Result<(), ()> {
        if self.deny.load(Ordering::SeqCst) {
            return Err(());
        }
        self.prepared.lock().unwrap().push(a.clone());
        Ok(())
    }
    fn storage_key(&self, url: &str) -> Option<String> {
        url.strip_prefix("synthetic://").map(str::to_string)
    }
    fn delete_objects(&self, urls: &[String]) {
        self.urls.lock().unwrap().extend_from_slice(urls);
    }
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    call_raw(app, method, path, token, body.to_string()).await
}

async fn call_raw(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: String,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let code = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        code,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        },
    )
}
async fn issue(
    db: &tokio_postgres::Client,
    ws: Uuid,
    user: Uuid,
    number: i32,
    title: &str,
    parent: Option<Uuid>,
    duplicate: Option<Uuid>,
) -> Uuid {
    db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,parent_issue_id,stage,status,duplicate_of_issue_id) VALUES($1,$2,'member',$3,$4,$5,$6,$7,$8) RETURNING id", &[&ws,&title,&user,&number,&parent,&parent.map(|_|2_i32),&if duplicate.is_some() {"cancelled"} else {"todo"},&duplicate]).await.unwrap().get(0)
}
async fn count(db: &tokio_postgres::Client, ws: Uuid) -> i64 {
    db.query_one("SELECT count(*) FROM issue WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap()
        .get(0)
}
#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn delete_and_move_http_db_concurrency_isolation() {
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
            .max_size(12)
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
            lock_key: 6611,
            hooks: &up,
            production_gates: true,
        },
        &Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 6612,
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
            &[&format!("delete-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("delete-outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Owned',$1,'DEL') RETURNING id",
            &[&format!("delete-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Foreign',$1,'FOR') RETURNING id",
            &[&format!("delete-foreign-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let parent = issue(&db, ws, user, 11, "Parent", None, None).await;
    let child = issue(&db, ws, user, 12, "Child", Some(parent), None).await;
    let duplicate = issue(&db, ws, user, 13, "Duplicate", None, Some(parent)).await;
    let moved = issue(&db, ws, user, 14, "Moved", None, None).await;
    let before = issue(&db, ws, user, 15, "Before", None, None).await;
    let after = issue(&db, ws, user, 16, "After", None, None).await;
    let foreign_id = issue(&db, foreign, user, 11, "Secret", None, None).await;
    db.execute("UPDATE issue SET position=1 WHERE id=$1", &[&before])
        .await
        .unwrap();
    db.execute("UPDATE issue SET position=3 WHERE id=$1", &[&after])
        .await
        .unwrap();
    let sink = Arc::new(Sink::default());
    let runtime = Arc::new(Runtime::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), b"delete-test-secret".to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_delete_runtime(runtime.clone()),
    );
    let auth = |sub: Uuid, exp: u64| {
        encode(
            &Header::default(),
            &json!({"sub":sub,"exp":exp}),
            &EncodingKey::from_secret(b"delete-test-secret"),
        )
        .unwrap()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = auth(user, now + 3600);
    let move_path = format!("/api/issues/{moved}/move?workspace_id={ws}");
    for (body, expected) in [
        (json!({"before_id":before}), StatusCode::BAD_REQUEST),
        (
            json!({"before_id":moved,"after_id":null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"before_id":"bad","after_id":null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"before_id":before,"after_id":before}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"before_id":after,"after_id":before}),
            StatusCode::CONFLICT,
        ),
        (
            json!({"before_id":foreign_id,"after_id":null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"before_id":null,"after_id":null,"unsupported":1}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"before_id":null,"after_id":null,"status":"done"}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let out = call(&app, "POST", &move_path, &token, body).await;
        assert_eq!(out.0, expected, "{out:?}");
    }
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&moved])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    // Go's json.Marshal rejects a nonfinite no-anchor position before any
    // delegated mutation; serde_json::json! would silently encode it as null.
    db.execute(
        "UPDATE issue SET position='NaN'::float8 WHERE id=$1",
        &[&moved],
    )
    .await
    .unwrap();
    let invalid_position = call(
        &app,
        "POST",
        &move_path,
        &token,
        json!({"before_id":null,"after_id":null}),
    )
    .await;
    assert_eq!(invalid_position.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(invalid_position.1["error"], "failed to encode move");
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&moved])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert!(sink.updated.lock().unwrap().is_empty());
    db.execute("UPDATE issue SET position=0 WHERE id=$1", &[&moved])
        .await
        .unwrap();
    let moved_response = call(
        &app,
        "POST",
        &move_path,
        &token,
        json!({"before_id":before,"after_id":after,"expected_revision":1}),
    )
    .await;
    assert_eq!(moved_response.0, StatusCode::OK, "{moved_response:?}");
    assert_eq!(moved_response.1["position"], 2.0);
    assert_eq!(moved_response.1["revision"], 2);
    let cas = call(
        &app,
        "POST",
        &move_path,
        &token,
        json!({"before_id":before,"after_id":after,"expected_revision":1}),
    )
    .await;
    assert_eq!(cas.0, StatusCode::CONFLICT, "{cas:?}");
    let contenders = (0..4)
        .map(|_| {
            let app = app.clone();
            let path = move_path.clone();
            let token = token.clone();
            tokio::spawn(async move {
                call(
                    &app,
                    "POST",
                    &path,
                    &token,
                    json!({"before_id":null,"after_id":before,"expected_revision":2}),
                )
                .await
                .0
            })
        })
        .collect::<Vec<_>>();
    let mut winners = 0;
    for contender in contenders {
        match contender.await.unwrap() {
            StatusCode::OK => winners += 1,
            StatusCode::CONFLICT => {}
            code => panic!("unexpected move race response: {code}"),
        }
    }
    assert_eq!(winners, 1);
    let no_runtime = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), b"delete-test-secret".to_vec())
            .with_issue_event_sink(sink.clone()),
    );
    let parent_path = format!("/api/issues/DEL-11?workspace_id={ws}");
    assert_eq!(
        call(&no_runtime, "DELETE", &parent_path, &token, json!({}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &parent_path,
            &auth(outsider, now + 3600),
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "DELETE", &parent_path, &auth(user, 1), json!({}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/issues/{foreign_id}?workspace_id={ws}"),
            &token,
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&parent_path)
                .header("cookie", format!("multica_auth={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    runtime.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        call(&app, "DELETE", &parent_path, &token, json!({}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    runtime.deny.store(false, Ordering::SeqCst);
    assert!(runtime.prepared.lock().unwrap().is_empty());
    let attach:Uuid=db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'member',$3,'artifact','synthetic://parent','text/plain',10) RETURNING id",&[&ws,&parent,&user]).await.unwrap().get(0);
    let source:Uuid=db.query_one("INSERT INTO issue_source_context(id,workspace_id,issue_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state,attached_at) VALUES($1,$2,$3,$4,$5,$6,1,'{}','synthetic','attached',now()) RETURNING id",&[&Uuid::new_v4(),&ws,&parent,&parent,&Uuid::new_v4(),&user]).await.unwrap().get(0);
    let clone:Uuid=db.query_one("INSERT INTO attachment(workspace_id,source_context_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'member',$3,'clone','synthetic://clone','text/plain',10) RETURNING id",&[&ws,&source,&user]).await.unwrap().get(0);
    let function = format!("delete_fail_{}", suffix.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback'; END $$; CREATE TRIGGER {function} BEFORE DELETE ON issue FOR EACH ROW WHEN (OLD.id='{parent}'::uuid) EXECUTE FUNCTION {function}();")).await.unwrap();
    let fail = call(&app, "DELETE", &parent_path, &token, json!({})).await;
    assert_eq!(fail.0, StatusCode::INTERNAL_SERVER_ERROR, "{fail:?}");
    assert_eq!(count(&db, ws).await, 6);
    assert!(sink.deleted.lock().unwrap().is_empty());
    assert!(runtime.urls.lock().unwrap().is_empty());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_source_context_object_intent WHERE source_context_id=$1",
            &[&source]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    assert_eq!(
        db.query_one("SELECT parent_issue_id FROM issue WHERE id=$1", &[&child])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(parent)
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON issue; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    let result = call(&app, "DELETE", &parent_path, &token, json!({})).await;
    assert_eq!(result.0, StatusCode::NO_CONTENT, "{result:?}");
    assert_eq!(count(&db, ws).await, 5);
    assert_eq!(
        db.query_one(
            "SELECT parent_issue_id,stage FROM issue WHERE id=$1",
            &[&child]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        None
    );
    assert_eq!(
        db.query_one(
            "SELECT duplicate_of_issue_id FROM issue WHERE id=$1",
            &[&duplicate]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        None
    );
    assert!(
        db.query_opt("SELECT 1 FROM attachment WHERE id=$1", &[&attach])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.query_opt("SELECT 1 FROM attachment WHERE id=$1", &[&clone])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.query_opt("SELECT 1 FROM issue_source_context WHERE id=$1", &[&source])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(db.query_one("SELECT storage_key,object_url FROM issue_source_context_object_intent WHERE source_context_id=$1",&[&source]).await.unwrap().get::<_,String>(0),"clone");
    assert!(
        runtime
            .urls
            .lock()
            .unwrap()
            .contains(&"synthetic://clone".to_string())
    );
    assert!(
        runtime
            .urls
            .lock()
            .unwrap()
            .contains(&"synthetic://parent".to_string())
    );
    assert_eq!(
        sink.deleted.lock().unwrap()[0],
        IssueDeletedEvent {
            workspace_id: ws,
            actor_type: "member",
            actor_id: user,
            issue_id: parent
        }
    );
    {
        let updates = sink.updated.lock().unwrap();
        assert!(
            updates
                .iter()
                .any(|e| e.payload["issue"]["id"] == child.to_string()
                    && e.payload["issue"]["parent_issue_id"].is_null())
        );
        assert!(
            updates
                .iter()
                .any(|e| e.payload["issue"]["id"] == duplicate.to_string()
                    && e.payload["prev_duplicate_of_issue_id"] == parent.to_string()
                    && e.payload["prev_duplicate_of_identifier"] == "DEL-11")
        );
    }
    let batch_path = format!("/api/issues/batch-delete?workspace_id={ws}");
    let batch_failure = format!("delete_batch_fail_{}", suffix.simple());
    db.batch_execute(&format!("CREATE FUNCTION {batch_failure}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic batch rollback'; END $$; CREATE TRIGGER {batch_failure} BEFORE DELETE ON issue FOR EACH ROW WHEN (OLD.id='{moved}'::uuid) EXECUTE FUNCTION {batch_failure}();")).await.unwrap();
    let failed_batch = call(
        &app,
        "POST",
        &batch_path,
        &token,
        json!({"issue_ids":[child,moved]}),
    )
    .await;
    assert_eq!(
        failed_batch.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{failed_batch:?}"
    );
    assert_eq!(count(&db, ws).await, 5);
    assert_eq!(sink.deleted.lock().unwrap().len(), 1);
    db.batch_execute(&format!(
        "DROP TRIGGER {batch_failure} ON issue; DROP FUNCTION {batch_failure}();"
    ))
    .await
    .unwrap();
    let batch=call(&app,"POST",&batch_path,&token,json!({"issue_ids":[child.to_string(),"bad",foreign_id.to_string(),child.to_string(),moved.to_string()]})).await;
    assert_eq!(batch, (StatusCode::OK, json!({"deleted":2})));
    assert_eq!(count(&db, ws).await, 3);
    assert_eq!(count(&db, foreign).await, 1);
    assert_eq!(sink.deleted.lock().unwrap().len(), 3);
    assert_eq!(
        call(
            &app,
            "POST",
            &batch_path,
            &token,
            json!({"issue_ids":["bad",foreign_id]})
        )
        .await,
        (StatusCode::OK, json!({"deleted":0}))
    );
    assert_eq!(
        call(&app, "POST", &batch_path, &token, json!({"issue_ids":[]}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert!(
        runtime
            .prepared
            .lock()
            .unwrap()
            .iter()
            .all(|a| a.workspace_id == ws && a.actor_id == user && a.actor_type == "member")
    );
    let mut ordered = [
        issue(&db, ws, user, 21, "Batch first", None, None).await,
        issue(&db, ws, user, 22, "Batch second", None, None).await,
    ];
    ordered.sort_unstable();
    ordered.reverse();
    let prepared_before = runtime.prepared.lock().unwrap().len();
    let deleted_before = sink.deleted.lock().unwrap().len();
    let malformed = format!(
        r#"{{"ISSUE_IDS":["{}","{}"],"issue_ids":["{}",9]}}"#,
        ordered[0], ordered[1], ordered[0]
    );
    assert_eq!(
        call_raw(&app, "POST", &batch_path, &token, malformed)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(runtime.prepared.lock().unwrap().len(), prepared_before);
    assert_eq!(sink.deleted.lock().unwrap().len(), deleted_before);
    let duplicate = format!(
        r#"{{"ISSUE_IDS":["{}","{}"],"issue_ids":[null,"{}","{}"],"unknown":1e10000}} trailing"#,
        ordered[0], ordered[1], ordered[0], ordered[1]
    );
    assert_eq!(
        call_raw(&app, "POST", &batch_path, &token, duplicate).await,
        (StatusCode::OK, json!({"deleted":2}))
    );
    assert_eq!(
        runtime.prepared.lock().unwrap()[prepared_before..]
            .iter()
            .map(|a| a.issue_id)
            .collect::<Vec<_>>(),
        ordered
    );
    assert_eq!(
        sink.deleted.lock().unwrap()[deleted_before..]
            .iter()
            .map(|e| e.issue_id)
            .collect::<Vec<_>>(),
        ordered
    );
    for id in ordered {
        assert!(
            db.query_opt(
                "SELECT id FROM issue WHERE workspace_id=$1 AND id=$2",
                &[&ws, &id]
            )
            .await
            .unwrap()
            .is_none()
        );
    }
    let raced = issue(&db, ws, user, 17, "Raced", None, None).await;
    let raced_path = format!("/api/issues/{raced}?workspace_id={ws}");
    let (left, right) = tokio::join!(
        call(&app, "DELETE", &raced_path, &token, json!({})),
        call(&app, "DELETE", &raced_path, &token, json!({})),
    );
    assert_eq!(
        [left.0, right.0]
            .iter()
            .filter(|s| **s == StatusCode::NO_CONTENT)
            .count(),
        1
    );
    assert!(
        [left.0, right.0].iter().all(|s| matches!(
            *s,
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND | StatusCode::INTERNAL_SERVER_ERROR
        )),
        "{left:?} {right:?}"
    );
    assert_eq!(
        sink.deleted
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.issue_id == raced)
            .count(),
        1
    );
    let agent_issue = issue(&db, ws, user, 18, "Agent deletion", None, None).await;
    let runtime_id:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic delete runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Synthetic delete agent','cloud',$2,$3) RETURNING id", &[&ws,&user,&runtime_id]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,'running') RETURNING id", &[&agent,&runtime_id,&agent_issue]).await.unwrap().get(0);
    let task_token = format!("mat_delete_{suffix}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&user]).await.unwrap();
    let actor_delete = call(
        &app,
        "DELETE",
        &format!("/api/issues/{agent_issue}?workspace_id={ws}"),
        &task_token,
        json!({}),
    )
    .await;
    assert_eq!(actor_delete.0, StatusCode::NO_CONTENT, "{actor_delete:?}");
    assert_eq!(
        runtime.prepared.lock().unwrap().last().unwrap(),
        &IssueDeletePreparation {
            workspace_id: ws,
            issue_id: agent_issue,
            actor_type: "agent",
            actor_id: agent,
            source_task_id: Some(task)
        }
    );
    assert_eq!(
        sink.deleted.lock().unwrap().last().unwrap(),
        &IssueDeletedEvent {
            workspace_id: ws,
            issue_id: agent_issue,
            actor_type: "agent",
            actor_id: agent
        }
    );
    pull_request_cases::run(
        &db,
        pool.clone(),
        health.clone(),
        ws,
        foreign,
        user,
        outsider,
        &token,
    )
    .await;
}
