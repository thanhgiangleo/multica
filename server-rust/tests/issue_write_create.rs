//! Synthetic create admission, HTTP/DB/event, rollback and concurrency checks.
//! Run only against the owned disposable loopback database multica_zbsi66.
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

#[path = "issue_write_quick_create/cases.rs"]
mod quick_create;

#[derive(Default)]
struct Sink {
    events: Mutex<Vec<IssueCreatedEvent>>,
    order: Arc<Mutex<Vec<&'static str>>>,
}
impl IssueEventSink for Sink {
    fn publish_issue_deleted(&self, _: IssueDeletedEvent) {
        panic!("unexpected event")
    }
    fn publish_issue_created(&self, event: IssueCreatedEvent) {
        self.order.lock().unwrap().push("event");
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
struct Runtime {
    policy: Mutex<IssueCountPolicy>,
    order: Arc<Mutex<Vec<&'static str>>>,
    analytics: Mutex<Vec<IssueCreatedEvent>>,
    media_ready: AtomicBool,
}
struct MediaPresenter;
static MEDIA_PRESENTER: MediaPresenter = MediaPresenter;
impl IssueCreateAttachmentPresenter for MediaPresenter {
    fn urls(&self, attachment: &IssueCreateAttachment, stable: bool) -> IssueCreateAttachmentUrls {
        let id = attachment.id;
        IssueCreateAttachmentUrls {
            download_url: if stable {
                format!("/api/attachments/{id}/download")
            } else {
                format!("https://signed.invalid/{id}?expires=synthetic")
            },
            markdown_url: format!("https://public.invalid/api/attachments/{id}/download"),
        }
    }
}
#[async_trait]
impl IssueCreateRuntime for Runtime {
    async fn count_policy(&self, _: Uuid) -> Result<IssueCountPolicy, ()> {
        Ok(*self.policy.lock().unwrap())
    }
    fn created(&self, event: &IssueCreatedEvent) {
        self.order.lock().unwrap().push("analytics");
        self.analytics.lock().unwrap().push(event.clone());
    }
    fn attachment_presenter(&self) -> Option<&dyn IssueCreateAttachmentPresenter> {
        self.media_ready
            .load(Ordering::Relaxed)
            .then_some(&MEDIA_PRESENTER)
    }
}
async fn call(app: &axum::Router, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
    call_with_capabilities(app, path, token, body, "").await
}
async fn call_with_capabilities(
    app: &axum::Router,
    path: &str,
    token: &str,
    body: Value,
    capabilities: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .header("x-client-capabilities", capabilities)
                // A JWT caller cannot forge task/agent provenance.
                .header("x-agent-id", Uuid::nil().to_string())
                .header("x-task-id", Uuid::nil().to_string())
                .body(Body::from(body.to_string()))
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
async fn call_raw(app: &axum::Router, path: &str, token: &str, body: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn origin_task(
    db: &tokio_postgres::Client,
    agent: Uuid,
    runtime: Uuid,
    requester: Option<Uuid>,
    context: Option<Value>,
) -> Uuid {
    db.query_one(
        "INSERT INTO agent_task_queue(agent_id,runtime_id,status,context,originator_user_id,accountable_user_id) VALUES($1,$2,'queued',$3,$4,$4) RETURNING id",
        &[&agent, &runtime, &context, &requester],
    ).await.unwrap().get(0)
}
async fn pending_source(
    db: &tokio_postgres::Client,
    ws: Uuid,
    task: Uuid,
    requester: Uuid,
    source_issue: Uuid,
    source: Uuid,
) {
    db.execute(
        "INSERT INTO issue_source_context(id,workspace_id,origin_task_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state) VALUES($1,$2,$3,$4,$5,$6,1,$7,'synthetic-digest','pending')",
        &[&source,&ws,&task,&source_issue,&Uuid::new_v4(),&requester,&json!({"messages":[{"body":"Immutable source"}]})],
    ).await.unwrap();
}
async fn count(db: &tokio_postgres::Client, ws: Uuid) -> i64 {
    db.query_one("SELECT count(*) FROM issue WHERE workspace_id=$1", &[&ws])
        .await
        .unwrap()
        .get(0)
}
async fn counter(db: &tokio_postgres::Client, ws: Uuid) -> i32 {
    db.query_one("SELECT issue_counter FROM workspace WHERE id=$1", &[&ws])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "requires owned loopback PostgreSQL database multica_zbsi66"]
async fn create_http_db_events_quota_races_rollback_and_isolation() {
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
            "INSERT INTO \"user\"(name,email) VALUES('Create owner',$1) RETURNING id",
            &[&format!("create-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outsider',$1) RETURNING id",
            &[&format!("create-outside-{suffix}@invalid.example")],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Create owned',$1,'CRT') RETURNING id", &[&format!("create-{suffix}")]).await.unwrap().get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug,issue_prefix) VALUES('Foreign',$1,'FOR') RETURNING id",
            &[&format!("create-foreign-{suffix}")],
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
    let label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color) VALUES($1,'Synthetic','#123456') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label: Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color) VALUES($1,'Foreign','#123456') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let text_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES($1,'Notes','text') RETURNING id", &[&ws]).await.unwrap().get(0);
    let actor_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES($1,'Reviewer','actor') RETURNING id", &[&ws]).await.unwrap().get(0);
    let select_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type,config) VALUES($1,'Size','multi_select',$2) RETURNING id", &[&ws,&json!({"options":[{"id":"small","name":"Small"},{"id":"large","name":"Large"}]})]).await.unwrap().get(0);
    let foreign_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES($1,'Other','text') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let archived_property: Uuid = db.query_one("INSERT INTO issue_property(workspace_id,name,type,archived_at) VALUES($1,'Old','text',now()) RETURNING id", &[&ws]).await.unwrap().get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Synthetic project') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_parent: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Foreign parent','member',$2,1) RETURNING id", &[&foreign,&user]).await.unwrap().get(0);
    let sink = Arc::new(Sink::default());
    let runtime = Arc::new(Runtime {
        policy: Mutex::new(IssueCountPolicy::default()),
        order: sink.order.clone(),
        analytics: Mutex::new(Vec::new()),
        media_ready: AtomicBool::new(false),
    });
    let make_app = |with_runtime: bool| {
        let state = ApiState::new(pool.clone(), b"create-test-secret".to_vec())
            .with_issue_event_sink(sink.clone());
        let state = if with_runtime {
            state.with_issue_create_runtime(runtime.clone())
        } else {
            state
        };
        router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            state,
        )
    };
    let app = make_app(true);
    let auth = |sub: Uuid, exp: u64| {
        encode(
            &Header::default(),
            &json!({"sub":sub,"exp":exp}),
            &EncodingKey::from_secret(b"create-test-secret"),
        )
        .unwrap()
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token = auth(user, now + 3600);
    let path = format!("/api/issues?workspace_id={ws}");
    let first = call(&app,&path,&token,json!({"title":"First issue","description":"Synthetic","priority":"high","status":"  TODO ","label_ids":[label,label],"assignee_type":"member","assignee_id":user,"project_id":project,"stage":2,"due_date":"2026-11-02"})).await;
    assert_eq!(first.0, StatusCode::CREATED, "{first:?}");
    assert_eq!(first.1["identifier"], "CRT-1");
    assert_eq!(first.1["revision"], 1);
    assert_eq!(first.1["position"], -1.0);
    assert_eq!(first.1["creator_id"], user.to_string());
    assert_eq!(first.1["creator_type"], "member");
    assert_eq!(first.1["status_category"], "todo");
    assert_eq!(first.1["due_date"], "2026-11-02");
    assert_eq!(first.1["labels"].as_array().unwrap().len(), 1);
    assert_eq!(first.1["labels"][0]["id"], label.to_string());
    assert!(first.1["origin_id"].is_null());
    let first_id = Uuid::parse_str(first.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(first_id.get_version_num(), 7);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_to_label WHERE issue_id=$1",
            &[&first_id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(sink.events.lock().unwrap()[0].payload["issue"], first.1);
    assert_eq!(*sink.order.lock().unwrap(), vec!["event", "analytics"]);
    assert_eq!(
        *sink.events.lock().unwrap(),
        *runtime.analytics.lock().unwrap()
    );

    // Parent project inheritance, empty authoritative labels, explicit null defaults.
    let second = call(&app,&path,&token,json!({"title":"Second issue","parent_issue_id":first_id,"status":null,"priority":null,"attachment_ids":null,"label_ids":null,"allow_duplicate":null})).await;
    assert_eq!(second.0, StatusCode::CREATED, "{second:?}");
    assert_eq!(second.1["project_id"], project.to_string());
    assert_eq!(second.1["parent_issue_id"], first_id.to_string());
    assert_eq!(second.1["labels"], json!([]));
    assert_eq!(second.1["position"], -2.0);
    let before = counter(&db, ws).await;
    for (body, status, message) in [
        (
            json!({"title":"Rejected","parent_issue_id":foreign_parent}),
            StatusCode::BAD_REQUEST,
            "parent issue not found in this workspace",
        ),
        (
            json!({"title":"Rejected","project_id":Uuid::new_v4()}),
            StatusCode::BAD_REQUEST,
            "project not found in this workspace",
        ),
        (
            json!({"title":"Rejected","label_ids":[foreign_label]}),
            StatusCode::BAD_REQUEST,
            "one or more labels not found in this workspace",
        ),
        (
            json!({"title":"Rejected","assignee_type":"member","assignee_id":outsider}),
            StatusCode::BAD_REQUEST,
            "assignee_id does not refer to a member of this workspace",
        ),
        (
            json!({"title":"Rejected","assignee_type":"member"}),
            StatusCode::BAD_REQUEST,
            "assignee_type and assignee_id must be provided together",
        ),
        (
            json!({"title":"Rejected","attachment_ids":["bad"]}),
            StatusCode::BAD_REQUEST,
            "invalid attachment_ids",
        ),
        (
            json!({"title":"Rejected","attachment_ids":[Uuid::new_v4()]}),
            StatusCode::SERVICE_UNAVAILABLE,
            "issue create runtime unavailable",
        ),
        (
            json!({"title":"Rejected","properties":[]}),
            StatusCode::BAD_REQUEST,
            "invalid request body",
        ),
        (
            json!({"title":"Rejected","assignee_type":"agent","assignee_id":Uuid::new_v4()}),
            StatusCode::SERVICE_UNAVAILABLE,
            "issue create runtime unavailable",
        ),
    ] {
        let result = call(&app, &path, &token, body).await;
        assert_eq!(result, (status, json!({"error":message})));
    }
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), 2);
    assert_eq!(
        call(
            &make_app(false),
            &path,
            &token,
            json!({"title":"No runtime"})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Raw create JSON follows Go's first-value streaming decoder. Duplicate
    // exact keys (including nested/unknown keys) reject before any DB write;
    // folded keys merge typed properties and reuse attachment/label slices.
    let raw_events = sink.events.lock().unwrap().len();
    for raw in [
        r#"{"title":"Rejected","title":"Other"}"#,
        r#"{"title":"Rejected","unknown":{"x":1,"x":2}}"#,
        r#"{"title":"Rejected","properties":{"x":1,"x":2}}"#,
        r#"{"title":"Rejected","stage":2147483648}"#,
        r#"{"title":"Rejected","properties":[]}"#,
        r#"{"title":"Rejected"#,
    ] {
        let result = call_raw(&app, &path, &token, raw).await;
        assert_eq!(
            result,
            (
                StatusCode::BAD_REQUEST,
                json!({"error":"invalid request body"})
            ),
            "{raw}"
        );
    }
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), raw_events);
    let raw = format!(
        "{{\"title\":\"Old\",\"TITLE\":\"Folded create\",\"label_ids\":[\"{label}\",\"other\"],\"LABEL_IDS\":[null],\"properties\":{{\"{text_property}\":\"folded\"}},\"PROPERTIES\":{{\"{select_property}\":[\"large\"]}}}} trailing"
    );
    let folded = call_raw(&app, &path, &token, &raw).await;
    assert_eq!(folded.0, StatusCode::CREATED, "{folded:?}");
    assert_eq!(folded.1["title"], "Folded create");
    assert_eq!(folded.1["labels"][0]["id"], label.to_string());
    assert_eq!(folded.1["labels"].as_array().unwrap().len(), 1);
    assert_eq!(
        folded.1["properties"],
        json!({text_property.to_string():"folded",select_property.to_string():["large"]})
    );
    let folded_id = Uuid::parse_str(folded.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one(
            "SELECT properties FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&folded_id, &ws]
        )
        .await
        .unwrap()
        .get::<_, Value>(0),
        folded.1["properties"]
    );
    assert_eq!(sink.events.lock().unwrap().len(), raw_events + 1);
    assert_eq!(
        sink.events.lock().unwrap().last().unwrap().payload["issue"],
        folded.1
    );
    let before = counter(&db, ws).await;

    // Named workflows admit listed statuses and reject a missing or handed-off
    // status before consuming a workspace issue number.
    let workflow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Synthetic workflow','todo',$2) RETURNING id", &[&ws,&json!([{"status_key":"todo"},{"status_key":"in_progress","handler":{"type":"agent","id":Uuid::new_v4()}}])]).await.unwrap().get(0);
    db.execute(
        "UPDATE project SET workflow_id=$1 WHERE id=$2",
        &[&workflow, &project],
    )
    .await
    .unwrap();
    let allowed = call(
        &app,
        &path,
        &token,
        json!({"title":"Workflow allowed","project_id":project}),
    )
    .await;
    assert_eq!(allowed.0, StatusCode::CREATED, "{allowed:?}");
    assert_eq!(counter(&db, ws).await, before + 1);
    let rejected = call(
        &app,
        &path,
        &token,
        json!({"title":"Workflow rejected","project_id":project,"status":"in_review"}),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert_eq!(rejected.1["code"], "status_not_in_workflow");
    assert_eq!(
        rejected.1["allowed_statuses"],
        json!(["todo", "in_progress"])
    );
    assert_eq!(counter(&db, ws).await, before + 1);
    let handed = call(
        &app,
        &path,
        &token,
        json!({"title":"Workflow handoff","project_id":project,"status":"in_progress"}),
    )
    .await;
    assert_eq!(handed.0, StatusCode::SERVICE_UNAVAILABLE, "{handed:?}");
    assert_eq!(counter(&db, ws).await, before + 1);
    let chosen = call(&app, &path, &token, json!({"title":"Workflow chosen member","project_id":project,"status":"in_progress","assignee_type":"member","assignee_id":user})).await;
    assert_eq!(chosen.0, StatusCode::CREATED, "{chosen:?}");
    assert_eq!(chosen.1["assignee_id"], json!(user));
    // Concrete and relative member handlers use the same workspace gate as
    // explicit assignment; no handler must consume a number on rejection.
    for (index, (handler, lead, expected)) in [
        (json!({"type":"member","id":user}), None, Some(user)),
        (json!({"type":"creator"}), None, Some(user)),
        (json!({"type":"project_lead"}), Some(user), Some(user)),
        (json!({"type":"project_lead"}), None, None),
        (json!({"type":"agent","id":"invalid"}), None, None),
    ]
    .into_iter()
    .enumerate()
    {
        db.execute(
            "UPDATE issue_workflow SET steps=$1 WHERE id=$2",
            &[&json!([{"status_key":"todo","handler":handler}]), &workflow],
        )
        .await
        .unwrap();
        db.execute("UPDATE project SET lead_type=CASE WHEN $1::uuid IS NULL THEN NULL ELSE 'member' END,lead_id=$1 WHERE id=$2", &[&lead,&project]).await.unwrap();
        let result = call(
            &app,
            &path,
            &token,
            json!({"title":format!("Member handoff {index}"),"parent_issue_id":first_id}),
        )
        .await;
        assert_eq!(result.0, StatusCode::CREATED, "{result:?}");
        assert_eq!(
            result
                .1
                .get("assignee_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            expected.map(|id| id.to_string())
        );
        assert_eq!(result.1["project_id"], json!(project));
        assert_eq!(
            sink.events.lock().unwrap().last().unwrap().payload["issue"],
            result.1
        );
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&foreign, &outsider],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_workflow SET steps=$1 WHERE id=$2",
        &[
            &json!([{"status_key":"todo","handler":{"type":"member","id":outsider}}]),
            &workflow,
        ],
    )
    .await
    .unwrap();
    let before_member = counter(&db, ws).await;
    let before_events = sink.events.lock().unwrap().len();
    let rejected = call(
        &app,
        &path,
        &token,
        json!({"title":"Foreign workflow member","project_id":project}),
    )
    .await;
    assert_eq!(
        rejected,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"assignee_id does not refer to a member of this workspace"})
        )
    );
    assert_eq!(counter(&db, ws).await, before_member);
    assert_eq!(sink.events.lock().unwrap().len(), before_events);
    let override_member = call(&app,&path,&token,json!({"title":"Explicit member beats workflow","project_id":project,"assignee_type":"member","assignee_id":user})).await;
    assert_eq!(
        override_member.0,
        StatusCode::CREATED,
        "{override_member:?}"
    );
    assert_eq!(override_member.1["assignee_id"], json!(user));
    db.execute(
        "UPDATE project SET workflow_id=NULL WHERE id=$1",
        &[&project],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_workflow SET is_default=true WHERE id=$1",
        &[&workflow],
    )
    .await
    .unwrap();
    let default = call(
        &app,
        &path,
        &token,
        json!({"title":"Default outside list","parent_issue_id":first_id,"status":"done"}),
    )
    .await;
    assert_eq!(default.0, StatusCode::CREATED, "{default:?}");
    db.execute("DELETE FROM issue_workflow WHERE id=$1", &[&workflow])
        .await
        .unwrap();

    // Custom status responses and the shared archive fence match the Go contract.
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,'approval','Human Approval','started','#123456')", &[&ws]).await.unwrap();
    let custom = call(
        &app,
        &path,
        &token,
        json!({"title":"Custom status","status":"  APPROVAL "}),
    )
    .await;
    assert_eq!(custom.0, StatusCode::CREATED, "{custom:?}");
    assert_eq!(custom.1["status_category"], "in_progress");
    assert_eq!(custom.1["status_name"], "Human Approval");
    let bad = call(
        &app,
        &path,
        &token,
        json!({"title":"Invalid status","status":"unknown"}),
    )
    .await;
    assert_eq!(
        bad,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid status \"unknown\"; valid values: approval (Human Approval), backlog, todo, in_progress, in_review, blocked, done, cancelled"})
        )
    );
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,'retiring','Retiring','started','#123456')", &[&ws]).await.unwrap();
    let mut locker = pool.get().await.unwrap();
    let archive = locker.transaction().await.unwrap();
    archive
        .execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("{ws}:issue_status")],
        )
        .await
        .unwrap();
    let pending_app = app.clone();
    let pending_path = path.clone();
    let pending_token = token.clone();
    let pending = tokio::spawn(async move {
        call(
            &pending_app,
            &pending_path,
            &pending_token,
            json!({"title":"Archive race","status":"retiring"}),
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%pg_advisory_xact_lock_shared%')", &[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        waiting,
        "create must take the Go-compatible shared catalog lock"
    );
    archive
        .execute(
            "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key='retiring'",
            &[&ws],
        )
        .await
        .unwrap();
    archive.commit().await.unwrap();
    assert_eq!(
        pending.await.unwrap(),
        (
            StatusCode::CONFLICT,
            json!({"error":"the target status was archived while this request was in flight; reload the status list and retry"})
        )
    );
    let before = counter(&db, ws).await;

    // Duplicate guard is serialized before numbering, including title normalization.
    let dup = call(
        &app,
        &path,
        &token,
        json!({"title":" first  ISSUE ","project_id":project}),
    )
    .await;
    assert_eq!(dup.0, StatusCode::CONFLICT, "{dup:?}");
    assert_eq!(dup.1["code"], "active_duplicate_issue");
    assert_eq!(dup.1["issue"]["id"], first_id.to_string());
    assert!(dup.1["issue"].get("labels").is_none());
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(
        call(
            &app,
            &path,
            &token,
            json!({"title":"First issue","project_id":project,"allow_duplicate":true})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let a = call(&app, &path, &token, json!({"title":"Concurrent duplicate"}));
    let b = call(&app, &path, &token, json!({"title":"Concurrent duplicate"}));
    let (a, b) = tokio::join!(a, b);
    let mut statuses = vec![a.0.as_u16(), b.0.as_u16()];
    statuses.sort();
    assert_eq!(statuses, vec![201, 409], "{a:?} {b:?}");

    // Terminal and Triage rows are not active duplicates.
    for (title, status, triage) in [
        ("Terminal", "done", None),
        ("Closed", "cancelled", None),
        ("Triage", "todo", Some("pending")),
    ] {
        db.execute("INSERT INTO issue(workspace_id,title,status,triage_state,creator_type,creator_id,number) VALUES($1,$2,$3,$4,'member',$5,1000+(SELECT count(*)::int FROM issue WHERE workspace_id=$1))", &[&ws,&title,&status,&triage,&user]).await.unwrap();
        assert_eq!(
            call(&app, &path, &token, json!({"title":title})).await.0,
            StatusCode::CREATED
        );
    }

    // Typed create values use the same definition locks and canonical values
    // as individual property writes; invalid bags leave no row, counter or event.
    let properties = json!({text_property.to_string():"note\u{0000} text",actor_property.to_string():format!("member:{}",user.to_string().to_uppercase()),select_property.to_string():["large","small","large"]});
    let canonical = json!({text_property.to_string():"note text",actor_property.to_string():format!("member:{user}"),select_property.to_string():["small","large"]});
    let created = call(
        &app,
        &path,
        &token,
        json!({"title":"Typed properties","properties":properties}),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    assert_eq!(created.1["properties"], canonical);
    let created_id = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one("SELECT properties FROM issue WHERE id=$1", &[&created_id])
            .await
            .unwrap()
            .get::<_, Value>(0),
        canonical
    );
    assert_eq!(
        sink.events.lock().unwrap().last().unwrap().payload["issue"]["properties"],
        canonical
    );
    let before_count = count(&db, ws).await;
    let before_counter = counter(&db, ws).await;
    let before_events = sink.events.lock().unwrap().len();
    for (key, value, message) in [
        (
            "bad".to_owned(),
            json!("x"),
            "property id must be a UUID".to_owned(),
        ),
        (
            foreign_property.to_string(),
            json!("x"),
            "property not found in this workspace".to_owned(),
        ),
        (
            Uuid::new_v4().to_string(),
            json!("x"),
            "property not found in this workspace".to_owned(),
        ),
        (
            archived_property.to_string(),
            json!("x"),
            "property \"Old\" is archived and cannot receive new values".to_owned(),
        ),
        (
            text_property.to_string(),
            json!(null),
            "value cannot be null (use DELETE to unset a property)".to_owned(),
        ),
        (
            text_property.to_string(),
            json!(5),
            "value must be a string".to_owned(),
        ),
        (
            text_property.to_string(),
            json!("x".repeat(2001)),
            "value must be 2000 characters or fewer".to_owned(),
        ),
        (
            actor_property.to_string(),
            json!(format!("member:{outsider}")),
            format!("\"member:{outsider}\" does not refer to a member of this workspace"),
        ),
        (
            actor_property.to_string(),
            json!(format!("agent:{user}")),
            "unknown actor kind \"agent\"; valid kinds: member".to_owned(),
        ),
    ] {
        let result = call(
            &app,
            &path,
            &token,
            json!({"title":"Invalid property","properties":{key.clone():value}}),
        )
        .await;
        assert_eq!(
            result,
            (
                StatusCode::BAD_REQUEST,
                json!({"code":"invalid_issue_property","property_id":key,"error":message})
            )
        );
    }
    assert_eq!(count(&db, ws).await, before_count);
    assert_eq!(counter(&db, ws).await, before_counter);
    assert_eq!(sink.events.lock().unwrap().len(), before_events);
    let large_property:Uuid=db.query_one("INSERT INTO issue_property(workspace_id,name,type) VALUES($1,'Many notes','multi_text') RETURNING id",&[&ws]).await.unwrap().get(0);
    let large: Vec<_> = (0..20)
        .map(|_| {
            (0..63)
                .map(|_| Uuid::new_v4().simple().to_string())
                .collect::<String>()[..2000]
                .to_string()
        })
        .collect();
    let too_large = call(
        &app,
        &path,
        &token,
        json!({"title":"Large bag","properties":{large_property.to_string():large}}),
    )
    .await;
    assert_eq!(
        too_large,
        (
            StatusCode::BAD_REQUEST,
            json!({"code":"issue_properties_too_large","error":"issue properties exceed the 16KB size limit"})
        )
    );
    assert_eq!(count(&db, ws).await, before_count);
    assert_eq!(counter(&db, ws).await, before_counter);
    assert_eq!(sink.events.lock().unwrap().len(), before_events);

    // A catalog archive holding the shared advisory key wins before create
    // can validate. Observe the actual PostgreSQL waiter, not a timed guess.
    let mut catalog = pool.get().await.unwrap();
    let archive = catalog.transaction().await.unwrap();
    archive
        .execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("prop:{text_property}")],
        )
        .await
        .unwrap();
    let pending_app = app.clone();
    let pending_path = path.clone();
    let pending_token = token.clone();
    let pending = tokio::spawn(async move {
        call(
            &pending_app,
            &pending_path,
            &pending_token,
            json!({"title":"Archive race","properties":{text_property.to_string():"x"}}),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let waiting:bool=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))",&[]).await.unwrap().get(0);
            if waiting { break; }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }).await.expect("create must wait for catalog lock");
    archive
        .execute(
            "UPDATE issue_property SET archived_at=now() WHERE id=$1",
            &[&text_property],
        )
        .await
        .unwrap();
    archive.commit().await.unwrap();
    assert_eq!(
        pending.await.unwrap(),
        (
            StatusCode::BAD_REQUEST,
            json!({"code":"invalid_issue_property","property_id":text_property,"error":"property \"Notes\" is archived and cannot receive new values"})
        )
    );
    assert_eq!(counter(&db, ws).await, before_counter);
    assert_eq!(sink.events.lock().unwrap().len(), before_events);

    // Only one concurrent request may claim the final quota slot.
    let limit = count(&db, ws).await + 1;
    *runtime.policy.lock().unwrap() = IssueCountPolicy {
        limit: Some(limit),
        revision: 42,
    };
    let before = counter(&db, ws).await;
    let (a, b) = tokio::join!(
        call(&app, &path, &token, json!({"title":"Quota A"})),
        call(&app, &path, &token, json!({"title":"Quota B"}))
    );
    let (ok, denied) = if a.0 == StatusCode::CREATED {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(ok.0, StatusCode::CREATED, "{ok:?}");
    assert_eq!(
        denied,
        (
            StatusCode::PAYMENT_REQUIRED,
            json!({"code":"issue_limit_reached","error":"workspace has reached its issue limit","limit":limit,"policy_revision":42})
        )
    );
    assert_eq!(count(&db, ws).await, limit);
    assert_eq!(counter(&db, ws).await, before + 1);
    *runtime.policy.lock().unwrap() = IssueCountPolicy::default();

    // Inject insert failure after counter allocation; all writes and publications roll back.
    db.batch_execute("CREATE FUNCTION zbsi66_create_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.title='__zbsi66_create_fail__' THEN RAISE EXCEPTION 'synthetic create failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi66_create_fail BEFORE INSERT ON issue FOR EACH ROW EXECUTE FUNCTION zbsi66_create_fail();").await.unwrap();
    let before = counter(&db, ws).await;
    let events = sink.events.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            &path,
            &token,
            json!({"title":"__zbsi66_create_fail__","label_ids":[label]})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);
    db.batch_execute(
        "DROP TRIGGER zbsi66_create_fail ON issue; DROP FUNCTION zbsi66_create_fail();",
    )
    .await
    .unwrap();

    // Verified task attribution, never the forged headers from call().
    let runtime_id:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic create runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Synthetic create agent','cloud',$2,$3) RETURNING id", &[&ws,&user,&runtime_id]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,'running') RETURNING id", &[&agent,&runtime_id,&first_id]).await.unwrap().get(0);
    let task_token = format!("mat_create_{suffix}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&user]).await.unwrap();
    quick_create::exercise(
        &pool,
        ws,
        foreign,
        user,
        outsider,
        (agent, first_id, project),
        &task_token,
    )
    .await;
    let created = call(&app, &path, &task_token, json!({"title":"Agent created"})).await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    assert_eq!(created.1["creator_type"], "agent");
    assert_eq!(created.1["creator_id"], agent.to_string());
    // Origin is stored provenance, deliberately absent from Go's IssueResponse.
    assert!(created.1.get("origin_type").is_none());
    assert!(created.1.get("origin_id").is_none());
    let created_id = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let origin = db
        .query_one(
            "SELECT origin_type,origin_id FROM issue WHERE id=$1",
            &[&created_id],
        )
        .await
        .unwrap();
    assert_eq!(
        origin.get::<_, Option<String>>(0).as_deref(),
        Some("agent_create")
    );
    assert_eq!(origin.get::<_, Option<Uuid>>(1), Some(task));
    assert_eq!(sink.events.lock().unwrap().last().unwrap().actor_id, agent);

    // Explicit origin pair validation occurs before quota/counter allocation.
    let before = counter(&db, ws).await;
    for (body, message) in [
        (
            json!({"title":"Invalid origin","origin_type":"quick_create"}),
            "origin_type and origin_id must be provided together",
        ),
        (
            json!({"title":"Invalid origin","origin_id":Uuid::new_v4()}),
            "origin_type and origin_id must be provided together",
        ),
        (
            json!({"title":"Invalid origin","origin_type":"agent_create","origin_id":task}),
            "unsupported origin_type",
        ),
        (
            json!({"title":"Invalid origin","origin_type":"quick_create","origin_id":"bad"}),
            "invalid origin_id",
        ),
    ] {
        assert_eq!(
            call(&app, &path, &task_token, body).await,
            (StatusCode::BAD_REQUEST, json!({"error":message}))
        );
    }
    assert_eq!(counter(&db, ws).await, before);

    let source = Uuid::new_v4();
    let quick = origin_task(
        &db,
        agent,
        runtime_id,
        Some(user),
        Some(json!({
            "type":"quick_create","workspace_id":ws,"requester_id":user,
            "source_context_id":source,"prompt":"Create from source","attachment_ids":null
        })),
    )
    .await;
    pending_source(&db, ws, quick, user, first_id, source).await;
    let quick_body = json!({"title":"Quick-created issue","origin_type":"quick_create","origin_id":quick,"label_ids":[label]});
    let quick_created = call(&app, &path, &task_token, quick_body.clone()).await;
    assert_eq!(quick_created.0, StatusCode::CREATED, "{quick_created:?}");
    let quick_id = Uuid::parse_str(quick_created.1["id"].as_str().unwrap()).unwrap();
    assert!(quick_created.1.get("origin_id").is_none());
    assert_eq!(quick_created.1["labels"][0]["id"], label.to_string());
    assert_eq!(quick_created.1["properties"], json!({}));
    let row = db
        .query_one(
            "SELECT origin_type,origin_id FROM issue WHERE id=$1",
            &[&quick_id],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Option<String>>(0).as_deref(),
        Some("quick_create")
    );
    assert_eq!(row.get::<_, Option<Uuid>>(1), Some(quick));
    let captured = db
        .query_one(
            "SELECT state,issue_id,snapshot,attached_at FROM issue_source_context WHERE id=$1",
            &[&source],
        )
        .await
        .unwrap();
    assert_eq!(captured.get::<_, String>(0), "attached");
    assert_eq!(captured.get::<_, Option<Uuid>>(1), Some(quick_id));
    assert_eq!(
        captured.get::<_, Value>(2)["messages"][0]["body"],
        "Immutable source"
    );
    assert!(
        captured
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>(3)
            .is_some()
    );
    assert_eq!(
        sink.events.lock().unwrap().last().unwrap().payload["issue"],
        quick_created.1
    );
    let before = counter(&db, ws).await;
    let events = sink.events.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            &path,
            &task_token,
            json!({"title":"Already attached","origin_type":"quick_create","origin_id":quick})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);

    // A human, an unrelated agent and a task owned by another workspace cannot
    // claim a quick-create origin, even when they know its task UUID.
    let other_agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Other create agent','cloud',$2,$3) RETURNING id", &[&ws,&user,&runtime_id]).await.unwrap().get(0);
    let other_task = origin_task(
        &db,
        other_agent,
        runtime_id,
        Some(user),
        Some(json!({"type":"quick_create","workspace_id":ws})),
    )
    .await;
    let foreign_runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Foreign create runtime','cloud','synthetic','online') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let foreign_agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES($1,'Foreign create agent','cloud',$2,$3) RETURNING id", &[&foreign,&user,&foreign_runtime]).await.unwrap().get(0);
    let foreign_task = origin_task(
        &db,
        foreign_agent,
        foreign_runtime,
        Some(user),
        Some(json!({"type":"quick_create","workspace_id":foreign})),
    )
    .await;
    let missing_task = Uuid::new_v4();
    for (caller, claimed) in [
        (token.as_str(), quick),
        (task_token.as_str(), other_task),
        (task_token.as_str(), foreign_task),
        (task_token.as_str(), missing_task),
    ] {
        let rejected=call(&app,&path,caller,json!({"title":format!("Claim {claimed}"),"origin_type":"quick_create","origin_id":claimed})).await;
        assert_eq!(
            rejected,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error":"failed to create issue"})
            )
        );
    }
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);

    // Decode and bind the task's stored quick-create context before source
    // attach. No invalid context leaves an issue, label, or event behind.
    for context in [
        None,
        Some(Value::Null),
        Some(json!({"type":"wrong","workspace_id":ws})),
        Some(json!({"type":"quick_create","workspace_id":foreign})),
        Some(json!({"type":"quick_create","workspace_id":"bad"})),
        Some(json!({"type":"quick_create","workspace_id":ws,"prompt":12})),
        Some(json!({"type":"quick_create","workspace_id":ws,"attachment_ids":[12]})),
        Some(
            json!({"type":"quick_create","workspace_id":ws,"source_context_id":"bad","requester_id":user}),
        ),
        Some(
            json!({"type":"quick_create","workspace_id":ws,"source_context_id":Uuid::new_v4(),"requester_id":"bad"}),
        ),
        Some(
            json!({"type":"quick_create","workspace_id":ws,"source_context_id":Uuid::new_v4(),"requester_id":user}),
        ),
    ] {
        let invalid = origin_task(&db, agent, runtime_id, Some(user), context).await;
        let rejected=call(&app,&path,&task_token,json!({"title":format!("Invalid context {invalid}"),"origin_type":"quick_create","origin_id":invalid,"label_ids":[label]})).await;
        assert_eq!(
            rejected.0,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{rejected:?}"
        );
    }
    let requester_mismatch = origin_task(&db,agent,runtime_id,Some(outsider),Some(json!({"type":"quick_create","workspace_id":ws,"requester_id":user,"source_context_id":Uuid::new_v4()}))).await;
    assert_eq!(call(&app,&path,&task_token,json!({"title":"Requester mismatch","origin_type":"quick_create","origin_id":requester_mismatch})).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);

    // The pending row is locked and checked against both context and human
    // originator. Failed attachment leaves source row pending and counter intact.
    let bad_source = Uuid::new_v4();
    let pending_task=origin_task(&db,agent,runtime_id,Some(user),Some(json!({"type":"quick_create","workspace_id":ws,"requester_id":user,"source_context_id":Uuid::new_v4()}))).await;
    pending_source(&db, ws, pending_task, user, first_id, bad_source).await;
    assert_eq!(call(&app,&path,&task_token,json!({"title":"Source ID mismatch","origin_type":"quick_create","origin_id":pending_task})).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    let mismatch_task=origin_task(&db,agent,runtime_id,Some(user),Some(json!({"type":"quick_create","workspace_id":ws,"requester_id":user,"source_context_id":Uuid::new_v4()}))).await;
    let mismatch_source = Uuid::new_v4();
    db.execute("UPDATE agent_task_queue SET context=jsonb_set(context,'{source_context_id}',to_jsonb($2::text)) WHERE id=$1", &[&mismatch_task,&mismatch_source.to_string()]).await.unwrap();
    pending_source(&db, ws, mismatch_task, outsider, first_id, mismatch_source).await;
    assert_eq!(call(&app,&path,&task_token,json!({"title":"Source owner mismatch","origin_type":"quick_create","origin_id":mismatch_task})).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    for id in [bad_source, mismatch_source] {
        assert_eq!(
            db.query_one(
                "SELECT state,issue_id FROM issue_source_context WHERE id=$1",
                &[&id]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "pending"
        );
    }
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);

    // Failure after source attachment rolls back the source, issue, labels and
    // counter; retry can attach once without copying a second source snapshot.
    let retry_source = Uuid::new_v4();
    let retry_task=origin_task(&db,agent,runtime_id,Some(user),Some(json!({"type":"quick_create","workspace_id":ws,"requester_id":user,"source_context_id":retry_source}))).await;
    pending_source(&db, ws, retry_task, user, first_id, retry_source).await;
    db.batch_execute("CREATE FUNCTION zbsi66_create_label_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM issue WHERE id=NEW.issue_id AND title='__zbsi66_create_label_fail__') THEN RAISE EXCEPTION 'synthetic label failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi66_create_label_fail BEFORE INSERT ON issue_to_label FOR EACH ROW EXECUTE FUNCTION zbsi66_create_label_fail();").await.unwrap();
    assert_eq!(call(&app,&path,&task_token,json!({"title":"__zbsi66_create_label_fail__","origin_type":"quick_create","origin_id":retry_task,"label_ids":[label]})).await.0,StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(counter(&db, ws).await, before);
    assert_eq!(sink.events.lock().unwrap().len(), events);
    assert_eq!(
        db.query_one(
            "SELECT state,issue_id FROM issue_source_context WHERE id=$1",
            &[&retry_source]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "pending"
    );
    db.batch_execute("DROP TRIGGER zbsi66_create_label_fail ON issue_to_label; DROP FUNCTION zbsi66_create_label_fail();").await.unwrap();
    let retried=call(&app,&path,&task_token,json!({"title":"Origin retry succeeded","origin_type":"quick_create","origin_id":retry_task,"label_ids":[label]})).await;
    assert_eq!(retried.0, StatusCode::CREATED, "{retried:?}");
    let retried_id = Uuid::parse_str(retried.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one(
            "SELECT issue_id FROM issue_source_context WHERE id=$1",
            &[&retry_source]
        )
        .await
        .unwrap()
        .get::<_, Uuid>(0),
        retried_id
    );

    // Two different titles race for the same source; only one commit can
    // attach it and publish its event.
    let race_source = Uuid::new_v4();
    let race_task=origin_task(&db,agent,runtime_id,Some(user),Some(json!({"Type":"quick_create","WORKSPACE_ID":ws,"requester_id":user,"source_context_id":race_source,"attachment_ids":[null]}))).await;
    pending_source(&db, ws, race_task, user, first_id, race_source).await;
    let before = counter(&db, ws).await;
    let events = sink.events.lock().unwrap().len();
    let (a, b) = tokio::join!(
        call(
            &app,
            &path,
            &task_token,
            json!({"title":"Origin race A","origin_type":"quick_create","origin_id":race_task})
        ),
        call(
            &app,
            &path,
            &task_token,
            json!({"title":"Origin race B","origin_type":"quick_create","origin_id":race_task})
        )
    );
    let (won, lost) = if a.0 == StatusCode::CREATED {
        (a, b)
    } else {
        (b, a)
    };
    assert_eq!(won.0, StatusCode::CREATED, "{won:?}");
    assert_eq!(
        lost,
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"failed to create issue"})
        )
    );
    assert_eq!(counter(&db, ws).await, before + 1);
    assert_eq!(sink.events.lock().unwrap().len(), events + 1);
    assert_eq!(
        db.query_one(
            "SELECT issue_id FROM issue_source_context WHERE id=$1",
            &[&race_source]
        )
        .await
        .unwrap()
        .get::<_, Uuid>(0),
        Uuid::parse_str(won.1["id"].as_str().unwrap()).unwrap()
    );

    // A quick-create task without captured source still stamps origin after
    // task/agent/context validation; no source row is required in this case.
    let plain_task = origin_task(
        &db,
        agent,
        runtime_id,
        None,
        Some(json!({"type":"quick_create","workspace_id":ws})),
    )
    .await;
    let plain=call(&app,&path,&task_token,json!({"title":"Quick-create without source","origin_type":"quick_create","origin_id":plain_task})).await;
    assert_eq!(plain.0, StatusCode::CREATED, "{plain:?}");
    let plain_id = Uuid::parse_str(plain.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        db.query_one(
            "SELECT origin_type,origin_id FROM issue WHERE id=$1",
            &[&plain_id]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(1),
        Some(plain_task)
    );

    // Go links only unbound, non-source attachments after commit. A real URL
    // policy is mandatory before the write; the recording presenter below
    // exercises signed default and explicitly advertised stable mode.
    runtime.media_ready.store(true, Ordering::Relaxed);
    let file:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'Photo.png','https://storage.invalid/photo','image/png',123) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let foreign_file:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'Foreign.png','https://storage.invalid/foreign','image/png',1) RETURNING id", &[&foreign,&user]).await.unwrap().get(0);
    let already_linked:Uuid=db.query_one("INSERT INTO attachment(workspace_id,issue_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'member',$3,'Linked.png','https://storage.invalid/linked','image/png',1) RETURNING id", &[&ws,&first_id,&user]).await.unwrap().get(0);
    let source_clone:Uuid=db.query_one("INSERT INTO attachment(workspace_id,source_context_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,$2,'member',$3,'Source.png','https://storage.invalid/source','image/png',1) RETURNING id", &[&ws,&source,&user]).await.unwrap().get(0);
    let media=call(&app,&path,&token,json!({"title":"Created with media","attachment_ids":[file,file,foreign_file,already_linked,source_clone,Uuid::new_v4()]})).await;
    assert_eq!(media.0, StatusCode::CREATED, "{media:?}");
    let media_id = Uuid::parse_str(media.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(media.1["revision"], 1);
    assert_eq!(media.1["attachments"].as_array().unwrap().len(), 1);
    let returned = &media.1["attachments"][0];
    assert_eq!(returned["id"], file.to_string());
    assert_eq!(returned["issue_id"], media_id.to_string());
    assert_eq!(returned["comment_id"], Value::Null);
    assert_eq!(returned["chat_session_id"], Value::Null);
    assert_eq!(returned["chat_message_id"], Value::Null);
    assert_eq!(returned["filename"], "Photo.png");
    assert_eq!(returned["url"], "https://storage.invalid/photo");
    assert_eq!(returned["size_bytes"], 123);
    assert_eq!(returned["content_type"], "image/png");
    assert_eq!(returned["uploader_type"], "member");
    assert_eq!(
        returned["download_url"],
        format!("https://signed.invalid/{file}?expires=synthetic")
    );
    assert_eq!(
        returned["markdown_url"],
        format!("https://public.invalid/api/attachments/{file}/download")
    );
    assert!(returned.get("attachment_download_url").is_none());
    assert_eq!(returned["created_at"].as_str().unwrap().len(), 20);
    assert_eq!(
        sink.events.lock().unwrap().last().unwrap().payload["issue"],
        media.1
    );
    for (id, owner) in [
        (file, Some(media_id)),
        (foreign_file, None),
        (already_linked, Some(first_id)),
        (source_clone, None),
    ] {
        assert_eq!(
            db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&id])
                .await
                .unwrap()
                .get::<_, Option<Uuid>>(0),
            owner
        );
    }
    let stable_file:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'agent',$2,'Agent.png','https://storage.invalid/agent','image/png',10) RETURNING id", &[&ws,&agent]).await.unwrap().get(0);
    let stable = call_with_capabilities(
        &app,
        &path,
        &task_token,
        json!({"title":"Media with stable URLs","attachment_ids":[stable_file]}),
        "unknown, stable_attachment_urls ",
    )
    .await;
    assert_eq!(stable.0, StatusCode::CREATED, "{stable:?}");
    assert_eq!(
        stable.1["attachments"][0]["download_url"],
        format!("/api/attachments/{stable_file}/download")
    );
    assert_eq!(stable.1["attachments"][0]["uploader_type"], "agent");
    assert_eq!(stable.1["attachments"][0]["uploader_id"], agent.to_string());

    // Even after the issue commits, a storage linkage failure remains a 201,
    // without linked rows or attachments in either response or event.
    let broken:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'link-fail','https://storage.invalid/fail','image/png',1) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    db.batch_execute("CREATE FUNCTION zbsi66_media_link_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.filename='link-fail' THEN RAISE EXCEPTION 'synthetic media link failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi66_media_link_fail BEFORE UPDATE ON attachment FOR EACH ROW EXECUTE FUNCTION zbsi66_media_link_fail();").await.unwrap();
    let degraded = call(
        &app,
        &path,
        &token,
        json!({"title":"Committed despite media failure","attachment_ids":[broken]}),
    )
    .await;
    assert_eq!(degraded.0, StatusCode::CREATED, "{degraded:?}");
    assert!(degraded.1.get("attachments").is_none());
    assert_eq!(degraded.1["revision"], 1);
    assert_eq!(
        sink.events.lock().unwrap().last().unwrap().payload["issue"],
        degraded.1
    );
    assert!(
        db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&broken])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0)
            .is_none()
    );
    db.batch_execute("DROP TRIGGER zbsi66_media_link_fail ON attachment; DROP FUNCTION zbsi66_media_link_fail();").await.unwrap();

    // Source attachment is atomic with issue creation; ordinary uploaded media
    // remains a separate best-effort operation after that commit.
    let both_source = Uuid::new_v4();
    let both_task=origin_task(&db,agent,runtime_id,Some(user),Some(json!({"type":"quick_create","workspace_id":ws,"requester_id":user,"source_context_id":both_source}))).await;
    pending_source(&db, ws, both_task, user, first_id, both_source).await;
    let both_file:Uuid=db.query_one("INSERT INTO attachment(workspace_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES($1,'member',$2,'Both.png','https://storage.invalid/both','image/png',1) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let both=call(&app,&path,&task_token,json!({"title":"Origin and media","origin_type":"quick_create","origin_id":both_task,"attachment_ids":[both_file]})).await;
    assert_eq!(both.0, StatusCode::CREATED, "{both:?}");
    let both_id = Uuid::parse_str(both.1["id"].as_str().unwrap()).unwrap();
    assert_eq!(both.1["attachments"][0]["id"], both_file.to_string());
    assert_eq!(
        db.query_one(
            "SELECT issue_id FROM issue_source_context WHERE id=$1",
            &[&both_source]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        Some(both_id)
    );
    assert_eq!(
        db.query_one("SELECT issue_id FROM attachment WHERE id=$1", &[&both_file])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        Some(both_id)
    );

    db.execute("DELETE FROM task_token WHERE task_id=$1", &[&task])
        .await
        .unwrap();
    assert_eq!(
        call(&app, &path, &task_token, json!({"title":"Revoked"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            &path,
            &auth(outsider, now + 3600),
            json!({"title":"Outsider"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, &path, &auth(user, 1), json!({"title":"Expired"}))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(
            &app,
            &format!("/api/issues?workspace_id={foreign}"),
            &token,
            json!({"title":"Foreign"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("cookie", format!("token={token}"))
                .header("origin", "https://evil.invalid")
                .body(Body::from(r#"{"title":"CSRF"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        response.status(),
        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
    ));
    // The legacy date client may send a timestamp exactly on a UTC day
    // boundary; all mutation paths must persist the calendar date, not the
    // timestamp's local date or its text representation.
    let legacy = call(
        &app,
        &path,
        &token,
        json!({
            "title":"Legacy calendar date",
            "assignee_type":"member", "assignee_id":user,
            "start_date":"2026-03-01T07:00:00+07:00",
            "due_date":"2026-02-28T19:00:00-05:00"
        }),
    )
    .await;
    assert_eq!(legacy.0, StatusCode::CREATED, "{legacy:?}");
    assert_eq!(legacy.1["start_date"], "2026-03-01");
    assert_eq!(legacy.1["due_date"], "2026-03-01");
    let legacy_id = Uuid::parse_str(legacy.1["id"].as_str().unwrap()).unwrap();
    let stored = db
        .query_one(
            "SELECT start_date::text,due_date::text FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&legacy_id, &ws],
        )
        .await
        .unwrap();
    assert_eq!(stored.get::<_, String>(0), "2026-03-01");
    assert_eq!(stored.get::<_, String>(1), "2026-03-01");
    let go_legacy = call(
        &app,
        &path,
        &token,
        json!({
            "title":"Go RFC3339 offset boundaries",
            "start_date":"2026-03-02T00:00:00+24:00",
            "due_date":"2026-03-02T01:00:00,000+00:60"
        }),
    )
    .await;
    assert_eq!(go_legacy.0, StatusCode::CREATED, "{go_legacy:?}");
    assert_eq!(go_legacy.1["start_date"], "2026-03-01");
    assert_eq!(go_legacy.1["due_date"], "2026-03-02");
    let id = Uuid::parse_str(go_legacy.1["id"].as_str().unwrap()).unwrap();
    let stored = db
        .query_one(
            "SELECT start_date::text,due_date::text FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .unwrap();
    assert_eq!(stored.get::<_, String>(0), "2026-03-01");
    assert_eq!(stored.get::<_, String>(1), "2026-03-02");
    for date in [
        "2026-03-01T00:00:01Z",
        "2026-03-01T00:00:00+07:00",
        "2026-03-01t00:00:00Z",
        "2026-03-01T00:00:00z",
    ] {
        let invalid = call(
            &app,
            &path,
            &token,
            json!({"title":"Invalid date","due_date":date}),
        )
        .await;
        assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{invalid:?}");
        assert_eq!(
            invalid.1["error"],
            "invalid due_date format, expected YYYY-MM-DD"
        );
    }

    assert_eq!(
        *sink.events.lock().unwrap(),
        *runtime.analytics.lock().unwrap()
    );
}
