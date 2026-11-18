//! Synthetic HTTP/DB/event tests. Run only against owned disposable loopback PostgreSQL.
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
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "issue_write_issue_decode/cases.rs"]
mod decoder_cases;
#[path = "issue_write_status/cases.rs"]
mod status_cases;
#[path = "issue_write_structure/cases.rs"]
mod structure_cases;
#[path = "issue_write_subscriber/cases.rs"]
mod subscriber_cases;

#[derive(Default)]
struct Sink {
    updates: Mutex<Vec<IssueUpdatedEvent>>,
    subscriptions: Mutex<Vec<IssueSubscriberEvent>>,
    attachments: Mutex<Vec<IssueAssignmentEvent>>,
    order: Mutex<Vec<&'static str>>,
    cancellations: Mutex<Vec<IssueTaskCancelledEvent>>,
}
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected event")
    }
    fn publish_issue_created(&self, _: IssueCreatedEvent) {
        panic!("unexpected issue create");
    }
    fn publish_issue_updated(&self, e: IssueUpdatedEvent) {
        self.order.lock().unwrap().push("issue:updated");
        self.updates.lock().unwrap().push(e)
    }
    fn publish_subscriber_changed(&self, e: IssueSubscriberEvent) {
        self.subscriptions.lock().unwrap().push(e)
    }
    fn publish_assignment_changed(&self, e: IssueAssignmentEvent) {
        self.order.lock().unwrap().push(e.event_type);
        self.attachments.lock().unwrap().push(e)
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
    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent) {
        self.order.lock().unwrap().push("task:cancelled");
        self.cancellations.lock().unwrap().push(event);
    }
    fn request_wakeup_dispatch(&self, _: IssueWakeupDispatchRequest) {
        panic!("unexpected dispatch")
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
async fn update_and_subscribe_http_db_events_races_and_isolation() {
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
            &[&format!("update-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("outsider-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Owned',$1,'OWN') RETURNING id",
            &[&format!("owned-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Foreign',$1,'FOR') RETURNING id",
            &[&format!("foreign-{suffix}")],
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
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,description,creator_type,creator_id,number) VALUES($1,'Before','old','member',$2,17) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let child:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,parent_issue_id,creator_type,creator_id,number) VALUES($1,'Child',$2,'member',$3,18) RETURNING id", &[&ws,&issue,&user]).await.unwrap().get(0);
    let foreign:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Foreign','member',$2,17) RETURNING id", &[&foreign_ws,&user]).await.unwrap().get(0);
    let attach:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'new.txt','synthetic://file','text/plain',2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let sink = Arc::new(Sink::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health,
        ApiState::new(pool.clone(), b"update-test-secret".to_vec())
            .with_issue_event_sink(sink.clone()),
    );
    let auth = |sub: Uuid, exp: u64| {
        encode(
            &Header::default(),
            &json!({"sub":sub,"exp":exp}),
            &EncodingKey::from_secret(b"update-test-secret"),
        )
        .unwrap()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = auth(user, now + 3600);
    let path = format!("/api/issues/OWN-17?workspace_id={ws}");
    let first=call(&app,"PUT",&path,&token,json!({"title":"New","title_base":"Before","priority":"high","start_date":"2026-11-02","attachment_ids":[attach]})).await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(first.1["identifier"], "OWN-17");
    assert_eq!(first.1["title"], "New");
    assert_eq!(first.1["revision"], 2);
    assert_eq!(first.1["start_date"], "2026-11-02");
    assert_eq!(sink.updates.lock().unwrap().len(), 1);
    let event = sink.updates.lock().unwrap()[0].clone();
    assert_eq!(
        (event.workspace_id, event.actor_id, event.actor_type),
        (ws, user, "member")
    );
    assert_eq!(event.payload["prev_title"], "Before");
    assert_eq!(event.payload["title_changed"], true);
    assert_eq!(
        sink.attachments.lock().unwrap()[0].payload["issue_revision"],
        2
    );
    assert_eq!(
        db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&attach])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(issue)
    );
    assert_eq!(
        *sink.order.lock().unwrap(),
        vec!["issue:updated", "issue_attachments:changed"]
    );
    let before = db
        .query_one(
            "SELECT updated_at,last_activity_at FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    let no_op = call(
        &app,
        "PUT",
        &path,
        &token,
        json!({"title":"New","start_date":"2026-11-02"}),
    )
    .await;
    assert_eq!(no_op.0, StatusCode::OK, "{no_op:?}");
    assert_eq!(no_op.1["revision"], 2);
    let after = db
        .query_one(
            "SELECT updated_at,last_activity_at FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(
        before.get::<_, chrono::DateTime<chrono::Utc>>(0),
        after.get::<_, chrono::DateTime<chrono::Utc>>(0)
    );
    assert_eq!(
        before.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1),
        after.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
    );
    for (body, status) in [
        (
            json!({"expected_revision":1,"title":"Lost"}),
            StatusCode::CONFLICT,
        ),
        (
            json!({"title":"Lost","title_base":"Before"}),
            StatusCode::CONFLICT,
        ),
        (json!({"status":"done"}), StatusCode::SERVICE_UNAVAILABLE),
        (
            json!({"parent_issue_id":null}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (json!({"priority":"bad"}), StatusCode::BAD_REQUEST),
        (json!({"start_date":"2026-02-30"}), StatusCode::BAD_REQUEST),
    ] {
        let out = call(&app, "PUT", &path, &token, body).await;
        assert_eq!(out.0, status, "{out:?}");
    }
    let update = call(
        &app,
        "PUT",
        &path,
        &token,
        json!({"description":"edit","due_date":"2026-11-03"}),
    )
    .await;
    assert_eq!(update.0, StatusCode::OK, "{update:?}");
    assert_eq!(update.1["due_date"], "2026-11-03");
    let mut tasks = vec![];
    for i in 0..8 {
        let a = app.clone();
        let p = path.clone();
        let t = token.clone();
        tasks.push(tokio::spawn(async move {
            call(
                &a,
                "PUT",
                &p,
                &t,
                json!({"expected_revision":3,"title":format!("winner-{i}")}),
            )
            .await
        }));
    }
    let mut won = 0;
    for task in tasks {
        let out = task.await.unwrap();
        if out.0 == StatusCode::OK {
            won += 1
        } else {
            assert_eq!(out.0, StatusCode::CONFLICT, "{out:?}");
        }
    }
    assert_eq!(won, 1);
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        4
    );
    let activity: Option<chrono::DateTime<chrono::Utc>> = db
        .query_one("SELECT last_activity_at FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let position = call(&app, "PUT", &path, &token, json!({"position":42.5})).await;
    assert_eq!(position.0, StatusCode::OK, "{position:?}");
    assert_eq!(position.1["revision"], 5);
    assert_eq!(
        db.query_one("SELECT last_activity_at FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0),
        activity
    );
    let cleared = call(
        &app,
        "PUT",
        &path,
        &token,
        json!({"start_date":null,"due_date":""}),
    )
    .await;
    assert_eq!(cleared.0, StatusCode::OK, "{cleared:?}");
    assert!(cleared.1["start_date"].is_null() && cleared.1["due_date"].is_null());
    assert_eq!(cleared.1["revision"], 6);
    // Failure after the owner update must roll back both the owner and linking.
    let failing:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'rollback.txt','synthetic://rollback','text/plain',2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let function = format!("fail_link_{}", suffix.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{failing}'::uuid THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} AFTER UPDATE ON attachment FOR EACH ROW EXECUTE FUNCTION {function}();")).await.unwrap();
    let failed = call(
        &app,
        "PUT",
        &path,
        &token,
        json!({"title":"must roll back","attachment_ids":[failing]}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        6
    );
    assert_eq!(
        db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&failing])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        None
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON attachment; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    for route in [
        path.clone(),
        format!("/api/issues/{issue}/subscribe?workspace_id={ws}"),
    ] {
        let method = if route == path { "PUT" } else { "POST" };
        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(route)
                    .header("cookie", format!("multica_auth={token}"))
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    }
    for id in [foreign.to_string(), "FOR-17".into(), "broken".into()] {
        assert_eq!(
            call(
                &app,
                "PUT",
                &format!("/api/issues/{id}?workspace_id={ws}"),
                &token,
                json!({"title":"leak"})
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            &auth(user, 1),
            json!({"title":"invalid"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            &auth(outsider, now + 3600),
            json!({"title":"invalid"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let p = format!("/api/issues/{issue}/subscribe?workspace_id={ws}");
    let leave = format!("/api/issues/{issue}/unsubscribe?workspace_id={ws}");
    let tree = format!("/api/issues/{issue}/unsubscribe/subtree?workspace_id={ws}");
    let foreign_target = call(&app, "POST", &p, &token, json!({"user_id":outsider})).await;
    assert_eq!(
        foreign_target.0,
        StatusCode::FORBIDDEN,
        "{foreign_target:?}"
    );
    assert_eq!(
        call(&app, "POST", &p, &token, json!({"user_id":"malformed"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "POST", &p, &token, json!({})).await,
        (StatusCode::OK, json!({"subscribed":true}))
    );
    assert_eq!(
        call(&app, "POST", &leave, &token, json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT opt_out_scope FROM issue_subscriber WHERE issue_id=$1 AND user_id=$2",
            &[&issue, &user]
        )
        .await
        .unwrap()
        .get::<_, Option<String>>(0)
        .as_deref(),
        Some("issue")
    );
    assert_eq!(
        call(&app, "POST", &p, &token, json!({})).await.0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one(
            "SELECT unsubscribed_at FROM issue_subscriber WHERE issue_id=$1 AND user_id=$2",
            &[&issue, &user]
        )
        .await
        .unwrap()
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>(0),
        None
    );
    db.execute("INSERT INTO issue_subscriber(issue_id,user_type,user_id,reason) VALUES($1,'member',$2,'manual')", &[&child,&user]).await.unwrap();
    assert_eq!(
        call(&app, "POST", &tree, &token, json!({})).await.0,
        StatusCode::OK
    );
    let tombstones=db.query("SELECT issue_id,opt_out_scope FROM issue_subscriber WHERE user_id=$1 AND issue_id IN($2,$3) ORDER BY issue_id", &[&user,&issue,&child]).await.unwrap();
    assert_eq!(tombstones.len(), 2);
    for row in tombstones {
        assert_eq!(row.get::<_, Option<String>>(1).as_deref(), Some("subtree"));
    }
    {
        let events = sink.subscriptions.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.event_type == "subscriber:removed")
                .count(),
            3
        );
        assert!(
            events
                .iter()
                .any(|e| e.payload["issue_id"] == child.to_string())
        );
        assert!(
            events
                .iter()
                .all(|e| e.workspace_id == ws && e.actor_id == user)
        );
    }
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Synthetic agent','cloud',$2,$3) RETURNING id", &[&ws,&user,&runtime]).await.unwrap().get(0);
    let run:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&issue]).await.unwrap().get(0);
    let task_token = format!("mat_update_{suffix}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&run,&agent,&ws,&user]).await.unwrap();
    let agent_update = call(
        &app,
        "PUT",
        &path,
        &task_token,
        json!({"title":"Agent edit"}),
    )
    .await;
    assert_eq!(agent_update.0, StatusCode::OK, "{agent_update:?}");
    let agent_event = sink.updates.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        (agent_event.actor_type, agent_event.actor_id),
        ("agent", agent)
    );
    assert_eq!(
        call(&app, "POST", &p, &task_token, json!({})).await.0,
        StatusCode::OK
    );
    let agent_subscription = sink.subscriptions.lock().unwrap().last().unwrap().clone();
    assert_eq!(agent_subscription.payload["user_id"], agent.to_string());
    assert_eq!(agent_subscription.payload["user_type"], "agent");
    assert_eq!(agent_subscription.actor_id, agent);
    assert_eq!(db.query_one("SELECT count(*) FROM issue_subscriber WHERE issue_id=$1 AND user_type='agent' AND user_id=$2 AND unsubscribed_at IS NULL", &[&issue,&agent]).await.unwrap().get::<_,i64>(0),1);
    let no_sink = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"update-test-secret".to_vec()),
    );
    assert_eq!(
        call(
            &no_sink,
            "PUT",
            &path,
            &token,
            json!({"title":"must not write"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&no_sink, "POST", &p, &token, json!({})).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/issues/{foreign}/subscribe?workspace_id={ws}"),
            &token,
            json!({})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &p, &auth(outsider, now + 3600), json!({}))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &p, &auth(user, 1), json!({})).await.0,
        StatusCode::UNAUTHORIZED
    );
    batch_checks(
        &app,
        &db,
        &sink,
        ws,
        issue,
        child,
        foreign,
        attach,
        &token,
        &task_token,
        run,
        agent,
    )
    .await;
    let batch_path = format!("/api/issues/batch?workspace_id={ws}");
    for (auth_token, expected) in [
        (auth(user, 1), StatusCode::UNAUTHORIZED),
        (auth(outsider, now + 3600), StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            call(
                &app,
                "PATCH",
                &batch_path,
                &auth_token,
                json!({"issue_ids":[issue],"updates":{"priority":"urgent"}})
            )
            .await
            .0,
            expected
        );
    }
    let csrf = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&batch_path)
                .header("cookie", format!("multica_auth={token}"))
                .body(Body::from(
                    json!({"issue_ids":[issue],"updates":{"title":"CSRF"}}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(csrf.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        call(
            &no_sink,
            "PATCH",
            &batch_path,
            &token,
            json!({"issue_ids":[issue],"updates":{"title":"disabled"}})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    structure_cases::run(
        &db,
        pool.clone(),
        sink.clone(),
        ws,
        foreign_ws,
        user,
        outsider,
        &token,
        &task_token,
        run,
        agent,
    )
    .await;
    status_cases::run(
        &db,
        pool.clone(),
        sink.clone(),
        ws,
        foreign_ws,
        user,
        foreign,
        &token,
        &task_token,
        run,
        agent,
    )
    .await;
    decoder_cases::run(&db, pool.clone(), sink.clone(), ws, user, &token).await;
    subscriber_cases::run(&db, &app, &sink, ws, user, agent, &task_token).await;
}

#[allow(clippy::too_many_arguments)]
async fn batch_checks(
    app: &axum::Router,
    db: &tokio_postgres::Client,
    sink: &Sink,
    ws: Uuid,
    issue: Uuid,
    child: Uuid,
    foreign: Uuid,
    attachment: Uuid,
    token: &str,
    task_token: &str,
    run: Uuid,
    agent: Uuid,
) {
    let path = format!("/api/issues/batch?workspace_id={ws}");
    let before = sink.updates.lock().unwrap().len();
    for body in [
        json!({"issue_ids":[issue],"status":"done"}),
        json!({"issue_ids":[issue]}),
        json!({"issue_ids":[issue],"updates":null}),
        json!({"issue_ids":[issue],"updates":{"title":null,"status":null,"attachment_ids":["invalid"],"expected_revision":-2,"enter_workflow_step":true}}),
    ] {
        assert_eq!(
            call(app, "PATCH", &path, token, body).await,
            (StatusCode::OK, json!({"updated":0}))
        );
    }
    for (updates, expected) in [
        (json!({"priority":"invalid"}), StatusCode::BAD_REQUEST),
        (
            json!({"duplicate_of_issue_id":null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"stage":null,"title":"fail"}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (json!({"status":"done"}), StatusCode::SERVICE_UNAVAILABLE),
        (json!([]), StatusCode::BAD_REQUEST),
    ] {
        assert_eq!(
            call(
                app,
                "PATCH",
                &path,
                token,
                json!({"issue_ids":[issue],"updates":updates})
            )
            .await
            .0,
            expected
        );
    }
    assert_eq!(
        call(
            app,
            "PATCH",
            &path,
            token,
            json!({"updates":{"title":"bad"}})
        )
        .await,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"issue_ids is required"})
        )
    );
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    for date in ["2026-02-29", "2026-2-01"] {
        assert_eq!(call(app,"PATCH",&path,token,json!({"issue_ids":[issue,child],"updates":{"title":"must not write","due_date":date}})).await,(StatusCode::OK,json!({"updated":0})));
    }
    let ids = json!([
        null,
        "invalid",
        "OWN-17",
        foreign,
        Uuid::new_v4(),
        issue,
        child,
        issue
    ]);
    assert_eq!(call(app,"PATCH",&path,token,json!({"issue_ids":ids,"updates":{"priority":"medium","expected_revision":-1,"title_base":"wrong","attachment_ids":["invalid"],"start_date":null,"due_date":""}})).await,(StatusCode::OK,json!({"updated":3})));
    let events = sink.updates.lock().unwrap()[before..].to_vec();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].payload["priority_changed"], true);
    assert_eq!(events[2].payload["priority_changed"], false);
    assert_eq!(
        events[0].payload["issue"]["revision"],
        events[2].payload["issue"]["revision"]
    );
    for e in &events {
        assert_eq!(e.payload.as_object().unwrap().len(), 8);
        assert_eq!(
            (e.actor_type, e.actor_id),
            (
                "member",
                Uuid::parse_str(e.payload["issue"]["creator_id"].as_str().unwrap()).unwrap()
            )
        );
        assert!(e.payload["issue"]["start_date"].is_null());
        assert!(e.payload["issue"]["due_date"].is_null());
    }
    assert_eq!(
        db.query_one("SELECT priority FROM issue WHERE id=$1", &[&foreign])
            .await
            .unwrap()
            .get::<_, String>(0),
        "none"
    );

    // Rollback is per item. Failure on item 2 must retain committed item 1 and
    // continue to item 3, without an event or audit row for the failed item.
    db.batch_execute(&format!("CREATE SCHEMA issue_write_batch_test; CREATE TABLE issue_write_batch_test.audit(issue_id uuid,source text,actor text); CREATE FUNCTION issue_write_batch_test.guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{child}'::uuid AND NEW.priority='urgent' THEN RAISE EXCEPTION 'synthetic per-item failure'; END IF; INSERT INTO issue_write_batch_test.audit VALUES(NEW.id,current_setting('multica.source_task_id',true),current_setting('multica.actor_type',true)); RETURN NEW; END $$; CREATE TRIGGER issue_write_batch_test_guard BEFORE UPDATE ON issue FOR EACH ROW EXECUTE FUNCTION issue_write_batch_test.guard();")).await.unwrap();
    let before = sink.updates.lock().unwrap().len();
    assert_eq!(
        call(
            app,
            "PATCH",
            &path,
            task_token,
            json!({"issue_ids":[issue,child,issue],"updates":{"priority":"urgent"}})
        )
        .await,
        (StatusCode::OK, json!({"updated":2}))
    );
    assert_eq!(
        db.query_one("SELECT priority FROM issue WHERE id=$1", &[&child])
            .await
            .unwrap()
            .get::<_, String>(0),
        "medium"
    );
    let audit = db
        .query(
            "SELECT issue_id,source,actor FROM issue_write_batch_test.audit",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(audit.len(), 2);
    for row in audit {
        assert_eq!(row.get::<_, Uuid>(0), issue);
        assert_eq!(row.get::<_, String>(1), run.to_string());
        assert_eq!(row.get::<_, String>(2), "agent");
    }
    assert_eq!(sink.updates.lock().unwrap().len(), before + 2);
    assert_eq!(sink.updates.lock().unwrap().last().unwrap().actor_id, agent);
    db.batch_execute("DROP TRIGGER issue_write_batch_test_guard ON issue; DROP SCHEMA issue_write_batch_test CASCADE;").await.unwrap();

    let marker = format!("<!-- multica:channel-media:{attachment} -->");
    db.execute(
        "UPDATE issue SET description=$2 WHERE id=$1",
        &[&issue, &marker],
    )
    .await
    .unwrap();
    assert_eq!(call(app,"PATCH",&path,token,json!({"issue_ids":[issue],"updates":{"description":"Batch prose","description_base":marker}})).await,(StatusCode::OK,json!({"updated":1})));
    let description: String = db
        .query_one("SELECT description FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    assert!(
        description.contains("Batch prose")
            && description.contains(&marker)
            && description.contains(&format!("/api/attachments/{attachment}/download"))
    );

    let before_revision: i64 = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let before_events = sink.updates.lock().unwrap().len();
    let mut writers = Vec::new();
    for i in 0..8 {
        let app = app.clone();
        let path = path.clone();
        let token = token.to_owned();
        writers.push(tokio::spawn(async move { call(&app,"PATCH",&path,&token,json!({"issue_ids":[issue],"updates":{"title":format!("Batch concurrent {i}"),"expected_revision":1}})).await }));
    }
    for writer in writers {
        assert_eq!(
            writer.await.unwrap(),
            (StatusCode::OK, json!({"updated":1}))
        );
    }
    assert_eq!(sink.updates.lock().unwrap().len(), before_events + 8);
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, i64>(0),
        before_revision + 8
    );
    let before = db
        .query_one(
            "SELECT revision,last_activity_at FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(
        call(
            app,
            "PATCH",
            &path,
            token,
            json!({"issue_ids":[issue],"updates":{"position":-73.0}})
        )
        .await,
        (StatusCode::OK, json!({"updated":1}))
    );
    let after = db
        .query_one(
            "SELECT revision,last_activity_at FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(after.get::<_, i64>(0), before.get::<_, i64>(0) + 1);
    assert_eq!(
        after.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1),
        before.get::<_, Option<chrono::DateTime<chrono::Utc>>>(1)
    );
    // Frozen Go's public route is POST /batch-update, not the older Rust alias.
    let post_path = format!("/api/issues/batch-update?workspace_id={ws}");
    let events_before = sink.updates.lock().unwrap().len();
    assert_eq!(
        call(
            app,
            "POST",
            &post_path,
            token,
            json!({"issue_ids":[issue],"updates":{"title":"Go batch route"}})
        )
        .await,
        (StatusCode::OK, json!({"updated":1}))
    );
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Go batch route"
    );
    assert_eq!(sink.updates.lock().unwrap().len(), events_before + 1);
}
