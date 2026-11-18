//! Project/workflow publication reuses the same catalog adapter and disposable DB.
use super::{Events, Recorder, SECRET, call};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi65 database"]
async fn project_workflow_events_follow_commit_and_frozen_go_order() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(s) if s=="127.0.0.1"))
    );
    // One fixture connection, one handler/callback connection: retaining a
    // handler connection across awaited publication deadlocks this test.
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(2)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let ws = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let owner = Uuid::new_v4();
    let member = Uuid::new_v4();
    for user in [owner, member] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Project event user',$2)",
            &[&user, &format!("{user}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Project events',$2)",
            &[&workspace, &format!("events-{workspace}")],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner'),($1,$3,'member'),($4,$2,'member')", &[&ws,&owner,&member,&foreign]).await.unwrap();
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_catalog_publisher(Arc::new(Recorder {
            pool: pool.clone(),
            events: events.clone(),
        })),
    );
    let jwt = |user: Uuid| {
        encode(
            &Header::default(),
            &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    };
    let token = jwt(owner);
    let member_token = jwt(member);
    let projects = format!("/api/projects/?workspace_id={ws}");
    let bundled = json!({"title":"Bundle", "resources":[
        {"resource_type":"github_repo","resource_ref":{"url":"https://github.com/example/first"},"label":"First"},
        {"resource_type":"github_repo","resource_ref":{"url":"https://github.com/example/second"},"label":"Second"}
    ]});
    let cookie = Request::builder()
        .method("POST")
        .uri(&projects)
        .header("cookie", format!("multica_auth={token}"))
        .header("content-type", "application/json")
        .body(Body::from(bundled.to_string()))
        .unwrap();
    assert_eq!(
        tower::ServiceExt::oneshot(app.clone(), cookie)
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let duplicate = json!({"title":"Rollback bundle","resources":[bundled["resources"][0],bundled["resources"][0]]});
    assert_eq!(
        call(&app, "POST", &projects, &token, duplicate).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM project WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(events.lock().unwrap().is_empty());
    let (status, mut created) = call(&app, "POST", &projects, &token, bundled).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let resources = created
        .as_object_mut()
        .unwrap()
        .remove("resources")
        .unwrap();
    let id: Uuid = serde_json::from_value(created["id"].clone()).unwrap();
    let detail = format!("/api/projects/{id}/?workspace_id={ws}");
    {
        let recorded = events.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert_eq!(
            recorded[0],
            ("project:created", json!({"project":created}), ws, owner)
        );
        assert_eq!(
            recorded[1].1,
            json!({"project_id":id,"resource":resources[0]})
        );
        assert_eq!(
            recorded[2].1,
            json!({"project_id":id,"resource":resources[1]})
        );
        assert_eq!(recorded[1].0, "project_resource:created");
        assert_eq!(recorded[2].0, "project_resource:created");
    }
    for (path, body, expected) in [
        (
            detail.clone(),
            json!({"status":"invalid"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/projects/not-a-uuid/?workspace_id={ws}"),
            json!({}),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/api/projects/{id}/?workspace_id={foreign}"),
            json!({"title":"No"}),
            StatusCode::NOT_FOUND,
        ),
    ] {
        assert_eq!(call(&app, "PUT", &path, &token, body).await.0, expected);
    }
    assert_eq!(events.lock().unwrap().len(), 3);
    let (status, updated) = call(
        &app,
        "PUT",
        &detail,
        &member_token,
        json!({"title":"Updated","description":"Checked"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["resource_count"], 2);
    assert_eq!(
        events.lock().unwrap()[3],
        ("project:updated", json!({"project":updated}), ws, member)
    );
    let function = format!("account_catalog_write_project_fail_{}", ws.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic project rollback'; END $$; CREATE TRIGGER {function} BEFORE DELETE ON project FOR EACH ROW WHEN (OLD.id='{id}'::uuid) EXECUTE FUNCTION {function}();")).await.unwrap();
    let view: Uuid = db.query_one("INSERT INTO issue_view(workspace_id,owner_id,name,scope_type,scope_id,visibility,query) VALUES($1,$2,'Project events','project',$3,'private','{}') RETURNING id", &[&ws,&owner,&id]).await.unwrap().get(0);
    assert_eq!(
        call(&app, "DELETE", &detail, &token, Value::Null).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.lock().unwrap().len(), 4);
    assert_eq!(
        db.query_one("SELECT count(*) FROM issue_view WHERE id=$1", &[&view])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON project; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &detail, &member_token, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (a, b) = tokio::join!(
        call(&app, "DELETE", &detail, &token, Value::Null),
        call(&app, "DELETE", &detail, &token, Value::Null)
    );
    assert!(
        (a.0 == StatusCode::NO_CONTENT && b.0 == StatusCode::NOT_FOUND)
            || (b.0 == StatusCode::NO_CONTENT && a.0 == StatusCode::NOT_FOUND),
        "{a:?} {b:?}"
    );
    assert_eq!(events.lock().unwrap().len(), 5);
    assert_eq!(
        events.lock().unwrap()[4],
        ("project:deleted", json!({"project_id":id}), ws, owner)
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM issue_view WHERE id=$1", &[&view])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );

    events.lock().unwrap().clear();
    let workflows = format!("/api/issue-workflows/?workspace_id={ws}");
    assert_eq!(call(&app,"POST",&workflows,&token,json!({"name":"Invalid flow","steps":[{"new_status":{"name":"Roll back this status"}},{"status_key":"missing"}]})).await.0,StatusCode::BAD_REQUEST);
    assert!(events.lock().unwrap().is_empty());
    let new_flow = json!({"name":"Event flow","steps":[{"new_status":{"name":"Event review","category":"started","color":"123456"}}]});
    let (status, flow) = call(&app, "POST", &workflows, &token, new_flow.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{flow}");
    assert_eq!(
        events.lock().unwrap()[0],
        (
            "issue_status:changed",
            json!({"action":"created"}),
            ws,
            owner
        )
    );
    assert_eq!(
        events.lock().unwrap()[1],
        (
            "issue_workflow:changed",
            json!({"action":"created"}),
            ws,
            owner
        )
    );
    let workflow: Uuid = serde_json::from_value(flow["id"].clone()).unwrap();
    assert_eq!(
        call(&app, "POST", &workflows, &token, new_flow).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(events.lock().unwrap().len(), 2);
    let (status, reused) = call(
        &app,
        "POST",
        &workflows,
        &member_token,
        json!({"name":"Reuse event status","steps":[{"new_status":{"name":"EVENT REVIEW"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{reused}");
    assert_eq!(
        events.lock().unwrap().len(),
        3,
        "reused status must not emit created"
    );
    assert_eq!(
        events.lock().unwrap()[2],
        (
            "issue_workflow:changed",
            json!({"action":"created"}),
            ws,
            member
        )
    );
    let (status, project) = call(
        &app,
        "POST",
        &projects,
        &token,
        json!({"title":"Copy target","workflow_id":workflow}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{project}");
    let project: Uuid = serde_json::from_value(project["id"].clone()).unwrap();
    events.lock().unwrap().clear();
    let handoff = format!("/api/issue-workflows/{workflow}/handoffs/todo?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            "PUT",
            &handoff,
            &member_token,
            json!({"handler":{"type":"none"}})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &handoff,
            &token,
            json!({"handler":{"type":"none"}})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        events.lock().unwrap()[0],
        (
            "issue_workflow:changed",
            json!({"action":"updated"}),
            ws,
            owner
        )
    );
    let copy = json!({"copy_for_project_id":project,"handler":{"type":"project_lead"},"instructions":"Check"});
    // Failure after workflow insertion but before project switch must publish nothing.
    let function = format!("account_catalog_write_copy_fail_{}", ws.simple());
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic copy rollback'; END $$; CREATE TRIGGER {function} BEFORE UPDATE ON project FOR EACH ROW WHEN (OLD.id='{project}'::uuid) EXECUTE FUNCTION {function}();")).await.unwrap();
    assert_eq!(
        call(&app, "PUT", &handoff, &token, copy.clone()).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.lock().unwrap().len(), 1);
    assert_eq!(db.query_one("SELECT count(*) FROM issue_workflow WHERE workspace_id=$1 AND name LIKE 'Event flow (%'", &[&ws]).await.unwrap().get::<_,i64>(0),0);
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON project; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    let (status, copied) = call(&app, "PUT", &handoff, &token, copy).await;
    assert_eq!(status, StatusCode::CREATED, "{copied}");
    let recorded = events.lock().unwrap().clone();
    assert_eq!(recorded.len(), 3);
    assert_eq!(
        recorded[1],
        (
            "issue_workflow:changed",
            json!({"action":"created"}),
            ws,
            owner
        )
    );
    assert_eq!(recorded[2].0, "project:updated");
    assert_eq!(recorded[2].1["project"]["workflow_id"], copied["id"]);
    assert_eq!(recorded[2].1["project"]["id"], project.to_string());
    assert!(recorded.iter().all(|e| e.2 == ws && e.3 == owner));
    let delete = format!("/api/issue-workflows/{workflow}?workspace_id={ws}");
    let (a, b) = tokio::join!(
        call(&app, "DELETE", &delete, &token, Value::Null),
        call(&app, "DELETE", &delete, &token, Value::Null)
    );
    assert!(
        (a.0 == StatusCode::NO_CONTENT && b.0 == StatusCode::NOT_FOUND)
            || (b.0 == StatusCode::NO_CONTENT && a.0 == StatusCode::NOT_FOUND),
        "{a:?} {b:?}"
    );
    assert_eq!(events.lock().unwrap().len(), 4);
    assert_eq!(
        events.lock().unwrap()[3],
        (
            "issue_workflow:changed",
            json!({"action":"deleted"}),
            ws,
            owner
        )
    );

    events.lock().unwrap().clear();
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'Project event synthetic','cloud',$2) RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Project event synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let task_token = format!("mat_project_events_{}", Uuid::new_v4().simple());
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&owner]).await.unwrap();
    let (status, project) = call(
        &app,
        "POST",
        &projects,
        &task_token,
        json!({"title":"Task project"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{project}");
    let task_project = format!(
        "/api/projects/{}/?workspace_id={ws}",
        project["id"].as_str().unwrap()
    );
    assert_eq!(
        events.lock().unwrap()[0],
        ("project:created", json!({"project":project}), ws, owner)
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &task_project,
            &task_token,
            json!({"title":"Task updated"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(events.lock().unwrap()[1].3, owner);
    assert_eq!(
        call(&app, "DELETE", &task_project, &task_token, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    db.execute(
        "UPDATE task_token SET expires_at=now()-interval '1 second' WHERE token_hash=$1",
        &[&hash],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &task_project,
            &task_token,
            json!({"title":"Expired"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(events.lock().unwrap().len(), 2);
}
