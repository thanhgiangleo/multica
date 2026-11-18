//! Post-commit catalog events on a disposable, loopback-only PostgreSQL database.
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
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

#[path = "account_catalog_write_project_events/mod.rs"]
mod account_catalog_write_project_events;

const SECRET: &[u8] = b"zbsi65-catalog-publish-synthetic-secret";
type Events = Arc<Mutex<Vec<(&'static str, Value, Uuid, Uuid)>>>;

struct Recorder {
    pool: Arc<Pool>,
    events: Events,
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
        _workspace: Uuid,
        _user: Uuid,
        _member: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("catalog test must not disconnect members") })
    }

    fn invalidate_membership<'a>(
        &'a self,
        _workspace: Uuid,
        _user: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("catalog test must not change membership") })
    }

    fn notify_daemon_workspaces_changed<'a>(
        &'a self,
        _users: Vec<Uuid>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async { panic!("catalog test must not notify daemons") })
    }

    fn publish<'a>(
        &'a self,
        event: CatalogPublication,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let kind = event.change.event_type();
            let payload = serde_json::to_value(&event.change).unwrap();
            let db = self.pool.get().await.unwrap();
            if let Some(raw) = payload
                .get("label")
                .or_else(|| payload.get("property"))
                .or_else(|| payload.get("resource"))
            {
                let id: Uuid = serde_json::from_value(raw["id"].clone()).unwrap();
                let table = if kind.starts_with("label:") {
                    "issue_label"
                } else if kind.starts_with("property:") {
                    "issue_property"
                } else {
                    "project_resource"
                };
                let row = db
                    .query_one(
                        &format!("SELECT workspace_id FROM {table} WHERE id=$1"),
                        &[&id],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row.get::<_, Uuid>(0),
                    event.workspace_id,
                    "event published before commit"
                );
            } else if let Some(project) = payload.get("project") {
                let id: Uuid = serde_json::from_value(project["id"].clone()).unwrap();
                let row = db
                    .query_one("SELECT workspace_id,title FROM project WHERE id=$1", &[&id])
                    .await
                    .unwrap();
                assert_eq!(
                    row.get::<_, Uuid>(0),
                    event.workspace_id,
                    "project event before commit"
                );
                assert_eq!(project["title"], row.get::<_, String>(1));
            } else if kind == "project:deleted" {
                let id: Uuid = serde_json::from_value(payload["project_id"].clone()).unwrap();
                assert_eq!(
                    db.query_one("SELECT count(*) FROM project WHERE id=$1", &[&id])
                        .await
                        .unwrap()
                        .get::<_, i64>(0),
                    0
                );
            } else if let Some(raw) = payload
                .get("label_id")
                .or_else(|| payload.get("resource_id"))
            {
                let id: Uuid = serde_json::from_value(raw.clone()).unwrap();
                let table = if kind.starts_with("label:") {
                    "issue_label"
                } else {
                    "project_resource"
                };
                assert_eq!(
                    db.query_one(&format!("SELECT count(*) FROM {table} WHERE id=$1"), &[&id])
                        .await
                        .unwrap()
                        .get::<_, i64>(0),
                    0
                );
            }
            self.events.lock().unwrap().push((
                kind,
                payload,
                event.workspace_id,
                event.actor_user_id,
            ));
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
    let response = tower::ServiceExt::oneshot(app.clone(), request)
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn catalog_events_are_scoped_committed_and_exactly_once() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    // Fixture connection + one handler connection + publisher callback. A handler
    // that holds its connection across publish deadlocks on this small pool.
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(2)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let ws = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let user = Uuid::new_v4();
    let outsider = Uuid::new_v4();
    for (id, name) in [(user, "Owner"), (outsider, "Stranger")] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,$2,$3)",
            &[&id, &name, &format!("{id}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for (id, name) in [(ws, "Catalog"), (foreign, "Other")] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,$2,$3)",
            &[&id, &name, &format!("catalog-{id}")],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &user],
    )
    .await
    .unwrap();
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Catalog') RETURNING id",
            &[&ws],
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
        })),
    );
    let jwt = |id: Uuid, age: i64| {
        encode(
            &Header::default(),
            &json!({"sub":id,"exp":chrono::Utc::now().timestamp()+age}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    };
    let owner = jwt(user, 3600);
    let stranger = jwt(outsider, 3600);
    let expired = jwt(user, -3600);
    let labels = format!("/api/labels/?workspace_id={ws}");
    let label_body = json!({"name":"Events","color":"112233"});
    assert_eq!(
        call(&app, "POST", &labels, &expired, label_body.clone())
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "POST", &labels, &stranger, label_body.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/labels/?workspace_id={foreign}"),
            &owner,
            label_body.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &labels,
            &owner,
            json!({"name":"Invalid","color":"red"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert!(events.lock().unwrap().is_empty());
    let (status, label) = call(&app, "POST", &labels, &owner, label_body.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{label:?}");
    let label_id = label["id"].as_str().unwrap();
    assert_eq!(events.lock().unwrap()[0].1, json!({"label":label}));
    let label_path = format!("/api/labels/{label_id}/?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &labels, &owner, label_body).await.0,
        StatusCode::CONFLICT
    );
    let (status, label) = call(&app, "PUT", &label_path, &owner, json!({"color":"445566"})).await;
    assert_eq!(status, StatusCode::OK, "{label:?}");
    assert_eq!(events.lock().unwrap()[1].1, json!({"label":label}));
    let label_uuid = Uuid::parse_str(label_id).unwrap();
    let synthetic_agent = Uuid::new_v4();
    db.execute(
        "INSERT INTO agent_to_label(agent_id,label_id) VALUES($1,$2)",
        &[&synthetic_agent, &label_uuid],
    )
    .await
    .unwrap();
    let function = format!("account_catalog_write_fail_{}", ws.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic catalog rollback'; END $$; CREATE TRIGGER {function} BEFORE DELETE ON issue_label FOR EACH ROW WHEN (OLD.id='{label_uuid}'::uuid) EXECUTE FUNCTION {function}();")).await.unwrap();
    assert_eq!(
        call(&app, "DELETE", &label_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        events.lock().unwrap().len(),
        2,
        "rolled-back delete published an event"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_to_label WHERE label_id=$1",
            &[&label_uuid]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM issue_label WHERE id=$1",
            &[&label_uuid]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON issue_label; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &label_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(events.lock().unwrap()[2].1, json!({"label_id":label_id}));
    assert_eq!(
        call(&app, "DELETE", &label_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let props = format!("/api/properties/?workspace_id={ws}");
    let (status, prop) = call(
        &app,
        "POST",
        &props,
        &owner,
        json!({"name":"Acceptance","type":"text"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{prop:?}");
    assert_eq!(events.lock().unwrap()[3].1, json!({"property":prop}));
    let prop_path = format!(
        "/api/properties/{}/?workspace_id={ws}",
        prop["id"].as_str().unwrap()
    );
    let (status, prop) = call(
        &app,
        "PATCH",
        &prop_path,
        &owner,
        json!({"description":"Check"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{prop:?}");
    assert_eq!(events.lock().unwrap()[4].1, json!({"property":prop}));
    assert_eq!(
        call(
            &app,
            "PATCH",
            &prop_path,
            &stranger,
            json!({"description":"No"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    let statuses = format!("/api/issue-statuses/?workspace_id={ws}");
    let (status, custom) = call(
        &app,
        "POST",
        &statuses,
        &owner,
        json!({"name":"Review","category":"started","color":"123abc"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{custom:?}");
    let status_path = format!(
        "/api/issue-statuses/{}?workspace_id={ws}",
        custom["id"].as_str().unwrap()
    );
    assert_eq!(
        call(
            &app,
            "PATCH",
            &status_path,
            &owner,
            json!({"name":"QA Review"})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, other_status) = call(
        &app,
        "POST",
        &statuses,
        &owner,
        json!({"name":"Ready","category":"started","color":"123abc"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{other_status:?}");
    let reorder = format!("/api/issue-statuses/reorder?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            "PATCH",
            &reorder,
            &owner,
            json!({"category":"started","ids":[other_status["id"],custom["id"]]})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "DELETE", &status_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, "DELETE", &status_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::OK
    );

    let resources = format!("/api/projects/{project}/resources?workspace_id={ws}");
    let (status,resource)=call(&app,"POST",&resources,&owner,json!({"resource_type":"github_repo","resource_ref":{"url":"https://github.com/example/repo","ref":"main"}})).await;
    assert_eq!(status, StatusCode::CREATED, "{resource:?}");
    let resource_path = format!(
        "/api/projects/{project}/resources/{}?workspace_id={ws}",
        resource["id"].as_str().unwrap()
    );
    let (status, resource) =
        call(&app, "PUT", &resource_path, &owner, json!({"label":"Repo"})).await;
    assert_eq!(status, StatusCode::OK, "{resource:?}");
    assert_eq!(
        call(&app, "DELETE", &resource_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        call(&app, "DELETE", &resource_path, &owner, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let recorded = events.lock().unwrap().clone();
    assert_eq!(
        recorded.iter().map(|r| r.0).collect::<Vec<_>>(),
        [
            "label:created",
            "label:updated",
            "label:deleted",
            "property:created",
            "property:updated",
            "issue_status:changed",
            "issue_status:changed",
            "issue_status:changed",
            "issue_status:changed",
            "issue_status:changed",
            "issue_workflow:changed",
            "project_resource:created",
            "project_resource:updated",
            "project_resource:deleted"
        ]
    );
    assert_eq!(recorded[5].1, json!({"action":"created"}));
    assert_eq!(recorded[6].1, json!({"action":"updated"}));
    assert_eq!(recorded[7].1, json!({"action":"created"}));
    assert_eq!(recorded[8].1, json!({"action":"reordered"}));
    assert_eq!(recorded[9].1, json!({"action":"archived"}));
    assert_eq!(recorded[10].1, json!({"action":"updated"}));
    assert_eq!(
        recorded[12].1,
        json!({"resource":resource,"project_id":project})
    );
    assert!(recorded.iter().all(|r| r.2 == ws && r.3 == user));

    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Catalog synthetic agent','cloud',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Catalog synthetic runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_catalog_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&user]).await.unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &props,
            &task_token,
            json!({"name":"Not admin","type":"text"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &statuses,
            &task_token,
            json!({"name":"Not admin","category":"started","color":"123abc"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(events.lock().unwrap().len(), 14);
    let (status, label) = call(
        &app,
        "POST",
        &labels,
        &task_token,
        json!({"name":"Task-authored","color":"123456"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{label:?}");
    let event = events.lock().unwrap()[14].clone();
    assert_eq!(event, ("label:created", json!({"label":label}), ws, user));
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &labels,
            &task_token,
            json!({"name":"Expired task","color":"123456"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(events.lock().unwrap().len(), 15);
}
