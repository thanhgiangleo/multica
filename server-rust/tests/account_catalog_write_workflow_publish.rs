//! HTTP remap with a recording shared-publisher adapter, never a runtime event bus.
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
        account_catalog_write_workflow_publish::{
            WorkflowPublication, WorkflowPublicationContext, WorkflowPublisher,
        },
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
use uuid::Uuid;

#[path = "workflow_support/sink.rs"]
mod sink;

const SECRET: &[u8] = b"zbsi65-workflow-publish-synthetic-secret";

#[derive(Debug, PartialEq)]
struct Recorded {
    kind: &'static str,
    workspace: Uuid,
    actor: Uuid,
    id: Option<Uuid>,
}
struct RecordingPublisher {
    pool: Arc<Pool>,
    events: Mutex<Vec<Recorded>>,
}
impl WorkflowPublisher for RecordingPublisher {
    fn publish<'a>(
        &'a self,
        context: &'a WorkflowPublicationContext,
        event: WorkflowPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // A separate connection must see the committed row before any publication.
            let db = self.pool.get().await.unwrap();
            let (kind, id) = match event {
                WorkflowPublication::IssueSnapshot(row) => {
                    let id: Uuid = row.get("id");
                    let committed = db
                        .query_one(
                            "SELECT status,revision,assignee_id FROM issue WHERE id=$1",
                            &[&id],
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        committed.get::<_, String>(0),
                        row.get::<_, String>("status")
                    );
                    assert_eq!(committed.get::<_, i64>(1), row.get::<_, i64>("revision"));
                    assert_eq!(
                        committed.get::<_, Option<Uuid>>(2),
                        row.get::<_, Option<Uuid>>("assignee_id")
                    );
                    ("issue:updated", Some(id))
                }
                WorkflowPublication::CancelledWakeup(row) => {
                    let id: Uuid = row.get("id");
                    assert_eq!(row.get::<_, String>("status"), "cancelled");
                    assert_eq!(
                        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&id])
                            .await
                            .unwrap()
                            .get::<_, String>(0),
                        "cancelled"
                    );
                    ("task:cancelled", Some(id))
                }
                WorkflowPublication::ProjectUpdated(project) => {
                    let id = Uuid::parse_str(project["id"].as_str().unwrap()).unwrap();
                    let workflow = db
                        .query_one("SELECT workflow_id FROM project WHERE id=$1", &[&id])
                        .await
                        .unwrap()
                        .get::<_, Option<Uuid>>(0);
                    assert_eq!(json!(workflow), project["workflow_id"]);
                    ("project:updated", Some(id))
                }
                WorkflowPublication::StatusCatalogCreated => {
                    assert!(db.query_one("SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND key='shipped'", &[&context.workspace_id]).await.unwrap().get::<_, i64>(0) > 0);
                    ("issue_status:changed", None)
                }
                WorkflowPublication::WorkflowUpdated => ("issue_workflow:changed", None),
            };
            self.events.lock().unwrap().push(Recorded {
                kind,
                workspace: context.workspace_id,
                actor: context.actor_user_id,
                id,
            });
        })
    }
}
struct Fixture {
    pool: Arc<Pool>,
    publisher: Arc<RecordingPublisher>,
    app: axum::Router,
    ws: Uuid,
    user: Uuid,
    project: Uuid,
    workflow: Uuid,
    narrow: Uuid,
    issue: Uuid,
    wakeup: Uuid,
    task: Uuid,
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
                .max_size(6)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let user = Uuid::new_v4();
        let ws = Uuid::new_v4();
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Publish',$2)",
            &[&user, &format!("publish-{user}@invalid.example")],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Publish',$2)",
            &[&ws, &format!("publish-{ws}")],
        )
        .await
        .unwrap();
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
            &[&ws, &user],
        )
        .await
        .unwrap();
        let wide = json!([{"status_key":"todo"},{"status_key":"in_progress"},{"status_key":"done"},{"status_key":"cancelled"}]);
        let narrow_steps =
            json!([{"status_key":"todo"},{"status_key":"done"},{"status_key":"cancelled"}]);
        let workflow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps,owner_id) VALUES($1,'Wide','todo',$2,$3) RETURNING id", &[&ws,&wide,&user]).await.unwrap().get(0);
        let narrow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps,owner_id) VALUES($1,'Narrow','todo',$2,$3) RETURNING id", &[&ws,&narrow_steps,&user]).await.unwrap().get(0);
        // Ensure the workflow edit's catalog validation has all fixed statuses.
        db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) SELECT $1,key,key,category,'#334455' FROM (VALUES ('todo','unstarted'),('in_progress','started'),('done','done'),('cancelled','closed')) s(key,category)", &[&ws]).await.unwrap();
        let project: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Publish',$2) RETURNING id", &[&ws,&workflow]).await.unwrap().get(0);
        let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number,revision,assignee_type,assignee_id) VALUES($1,'Publish','member',$2,$3,'in_progress',1,4,'member',$2) RETURNING id", &[&ws,&user,&project]).await.unwrap().get(0);
        let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Synthetic','cloud',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
        let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
        let wakeup = Uuid::new_v4();
        db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode) VALUES($1,$2,$3,$4,$5,'Synthetic','event','continuous')", &[&wakeup,&ws,&issue,&agent,&user]).await.unwrap();
        let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context) VALUES($1,$2,$3,'queued',$4) RETURNING id", &[&agent,&runtime,&issue,&json!({"wakeup_id":wakeup})]).await.unwrap().get(0);
        let publisher = Arc::new(RecordingPublisher {
            pool: pool.clone(),
            events: Mutex::new(Vec::new()),
        });
        let app = router_with_api(
            Arc::new(Health::new(
                Some(pool.clone()),
                MigrationCatalog::embedded().unwrap(),
                "test".into(),
            )),
            ApiState::new(pool.clone(), SECRET.to_vec())
                .with_issue_event_sink(Arc::new(sink::StrictSink))
                .with_workflow_publisher(publisher.clone()),
        );
        Self {
            pool,
            publisher,
            app,
            ws,
            user,
            project,
            workflow,
            narrow,
            issue,
            wakeup,
            task,
        }
    }
    fn token(&self) -> String {
        encode(
            &Header::default(),
            &json!({"sub":self.user,"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    }
    async fn call(&self, method: &str, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::builder()
            .method(method)
            .uri(format!("{path}?workspace_id={}", self.ws))
            .header("authorization", format!("Bearer {}", self.token()))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = tower::ServiceExt::oneshot(self.app.clone(), req)
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }
    fn switch_body(&self) -> Value {
        json!({"workflow_id":self.narrow,"status_mapping":{"in_progress":"done"}})
    }
    fn edit_body(&self) -> Value {
        json!({"name":"Edited","steps":[{"status_key":"todo"},{"new_status":{"name":"Shipped","category":"done","color":"#112233"}},{"status_key":"done"},{"status_key":"cancelled"}],"status_mapping":{"in_progress":"shipped"}})
    }
    fn kinds(&self) -> Vec<&'static str> {
        self.publisher
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.kind)
            .collect()
    }
    async fn assert_moved(&self, status: &str) {
        let db = self.pool.get().await.unwrap();
        let row = db
            .query_one(
                "SELECT status,revision,assignee_type,assignee_id FROM issue WHERE id=$1",
                &[&self.issue],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), status);
        assert_eq!(row.get::<_, i64>(1), 5);
        assert_eq!(row.get::<_, Option<String>>(2).as_deref(), Some("member"));
        assert_eq!(row.get::<_, Option<Uuid>>(3), Some(self.user));
        assert!(
            !db.query_one(
                "SELECT enabled FROM issue_wakeup WHERE id=$1",
                &[&self.wakeup]
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        );
        assert_eq!(
            db.query_one(
                "SELECT status FROM agent_task_queue WHERE id=$1",
                &[&self.task]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
            "cancelled"
        );
        for e in self.publisher.events.lock().unwrap().iter() {
            assert_eq!(e.workspace, self.ws);
            assert_eq!(e.actor, self.user);
        }
    }
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn project_remap_publishes_committed_snapshots_then_tasks_then_project() {
    let f = Fixture::new().await;
    let path = format!("/api/projects/{}/workflow", f.project);
    let mut preview = f.switch_body();
    preview["dry_run"] = json!(true);
    assert_eq!(f.call("POST", &path, preview).await.0, StatusCode::OK);
    assert!(f.kinds().is_empty());
    let (status, result) = f.call("POST", &path, f.switch_body()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["updated_issues"], 1);
    assert_eq!(result["project"]["workflow_id"], f.narrow.to_string());
    assert_eq!(
        f.kinds(),
        ["issue:updated", "task:cancelled", "project:updated"]
    );
    f.assert_moved("done").await;
    f.publisher.events.lock().unwrap().clear();
    let (status, result) = f.call("POST", &path, f.switch_body()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["updated_issues"], 0);
    assert_eq!(f.kinds(), ["project:updated"]);
    f.assert_moved("done").await;
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn workflow_remap_publishes_after_commit_and_rolls_back_dry_run_statuses() {
    let f = Fixture::new().await;
    let path = format!("/api/issue-workflows/{}", f.workflow);
    let mut preview = f.edit_body();
    preview["dry_run"] = json!(true);
    assert_eq!(f.call("PATCH", &path, preview).await.0, StatusCode::OK);
    assert!(f.kinds().is_empty());
    let db = f.pool.get().await.unwrap();
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND key='shipped'",
            &[&f.ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let (status, result) = f.call("PATCH", &path, f.edit_body()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["name"], "Edited");
    assert_eq!(
        f.kinds(),
        [
            "issue:updated",
            "task:cancelled",
            "issue_status:changed",
            "issue_workflow:changed"
        ]
    );
    f.assert_moved("shipped").await;
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn remap_http_errors_and_deferred_commit_failure_never_publish() {
    for edit in [false, true] {
        for at_commit in [false, true] {
            let f = Fixture::new().await;
            let db = f.pool.get().await.unwrap();
            let suffix = Uuid::new_v4().simple().to_string();
            let function = format!("zbsi65_fault_{suffix}");
            let trigger = format!("zbsi65_trigger_{suffix}");
            let (table, id) = if at_commit {
                ("issue", f.issue)
            } else {
                ("issue_wakeup", f.wakeup)
            };
            let trigger_ddl = if at_commit {
                format!(
                    "CREATE CONSTRAINT TRIGGER {trigger} AFTER UPDATE ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {function}()"
                )
            } else {
                format!(
                    "CREATE TRIGGER {trigger} BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {function}()"
                )
            };
            db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{id}'::uuid THEN RAISE EXCEPTION 'synthetic remap failure'; END IF; RETURN NEW; END $$; {trigger_ddl};")).await.unwrap();
            let (method, path, body) = if edit {
                (
                    "PATCH",
                    format!("/api/issue-workflows/{}", f.workflow),
                    f.edit_body(),
                )
            } else {
                (
                    "POST",
                    format!("/api/projects/{}/workflow", f.project),
                    f.switch_body(),
                )
            };
            let response = f.call(method, &path, body.clone()).await;
            // Remove only this fixture's injected fault before assertions can panic.
            db.batch_execute(&format!(
                "DROP TRIGGER {trigger} ON {table}; DROP FUNCTION {function}();"
            ))
            .await
            .unwrap();
            assert_eq!(
                response.0,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{response:?}"
            );
            assert_eq!(
                response.1["error"],
                if at_commit {
                    if edit {
                        "failed to save workflow"
                    } else {
                        "failed to change workflow"
                    }
                } else {
                    "failed to move issues to the new statuses"
                }
            );
            assert!(f.kinds().is_empty());
            let issue = db
                .query_one("SELECT status,revision FROM issue WHERE id=$1", &[&f.issue])
                .await
                .unwrap();
            assert_eq!(issue.get::<_, String>(0), "in_progress");
            assert_eq!(issue.get::<_, i64>(1), 4);
            assert!(
                db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&f.wakeup])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
            assert_eq!(
                db.query_one(
                    "SELECT status FROM agent_task_queue WHERE id=$1",
                    &[&f.task]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                "queued"
            );
            assert_eq!(
                db.query_one("SELECT workflow_id FROM project WHERE id=$1", &[&f.project])
                    .await
                    .unwrap()
                    .get::<_, Option<Uuid>>(0),
                Some(f.workflow)
            );
            assert_eq!(
                db.query_one(
                    "SELECT name FROM issue_workflow WHERE id=$1",
                    &[&f.workflow]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
                "Wide"
            );
            assert_eq!(
                db.query_one(
                    "SELECT count(*) FROM issue_status WHERE workspace_id=$1 AND key='shipped'",
                    &[&f.ws]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
                0
            );
            let response = f.call(method, &path, body).await;
            assert_eq!(response.0, StatusCode::OK, "{response:?}");
            f.assert_moved(if edit { "shipped" } else { "done" }).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn concurrent_identical_switches_remap_only_once() {
    let f = Fixture::new().await;
    let path = format!("/api/projects/{}/workflow", f.project);
    let (first, second) = tokio::join!(
        f.call("POST", &path, f.switch_body()),
        f.call("POST", &path, f.switch_body())
    );
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(second.0, StatusCode::OK, "{second:?}");
    assert_eq!(
        first.1["updated_issues"].as_u64().unwrap() + second.1["updated_issues"].as_u64().unwrap(),
        1
    );
    let kinds = f.kinds();
    assert_eq!(kinds.iter().filter(|&&k| k == "issue:updated").count(), 1);
    assert_eq!(kinds.iter().filter(|&&k| k == "task:cancelled").count(), 1);
    assert_eq!(kinds.iter().filter(|&&k| k == "project:updated").count(), 2);
    f.assert_moved("done").await;
}
