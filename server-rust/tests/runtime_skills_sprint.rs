//! Global skill mutations against a disposable loopback PostgreSQL instance.
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
        runtime_skills_sprint::{SkillEvent, SkillEventSink},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls, config::Host};
use tower::ServiceExt;
use uuid::Uuid;

const SECRET: &[u8] = b"zbsi71-skill-test-synthetic-secret";

#[derive(Default)]
struct Events(Mutex<Vec<SkillEvent>>);
impl SkillEventSink for Events {
    fn publish(&self, event: SkillEvent) {
        self.0.lock().unwrap().push(event);
    }
}

async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn token(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires migrated disposable multica_zbsi71 PostgreSQL on random loopback port"]
async fn skill_crud_files_labels_rollback_auth_and_events() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi71"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h,Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|p| *p != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["creator", "member", "admin", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES($1,$2) RETURNING id",
                &[&name, &format!("{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Skills',$1) RETURNING id",
            &[&format!("skills-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other',$1) RETURNING id",
            &[&format!("skills-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [
        (users[0], "member"),
        (users[1], "member"),
        (users[2], "admin"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO skill(workspace_id,name) VALUES($1,'private') RETURNING id",
            &[&other],
        )
        .await
        .unwrap()
        .get(0);
    let label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES($1,'ops','#ffffff','skill') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES($1,'private','#000000','skill') RETURNING id", &[&other]).await.unwrap().get(0);
    let wrong_type:Uuid=db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES($1,'agent','#111111','agent') RETURNING id", &[&ws]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_skill_event_sink(events.clone()),
    );
    let no_sink = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let jwt: Vec<String> = users.iter().map(|user| token(*user)).collect();
    let collection = format!("/api/skills/?workspace_id={ws}");
    let base = json!({"name":"alpha","content":"# Alpha","files":[{"path":"references/guide.md","content":"hello"}]});
    assert_eq!(
        call(&no_sink, "POST", &collection, Some(&jwt[0]), base.clone())
            .await
            .0,
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        call(&app, "POST", &collection, None, base.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, "POST", &collection, Some(&jwt[3]), base.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/skills/?workspace_id={other}"),
            Some(&jwt[0]),
            base.clone()
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &collection,
            Some(&jwt[0]),
            json!({"name":"unsafe","files":[{"path":"../SKILL.md","content":"x"}]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, invalid_path) = call(
        &app,
        "POST",
        &collection,
        Some(&jwt[0]),
        json!({"name":"unsafe-specific","files":[{"path":"../guide.md","content":"x"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_path["error"], "invalid file path: ../guide.md");
    let (status, created) = call(&app, "POST", &collection, Some(&jwt[0]), base.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{created:?}");
    assert_eq!(created["created_by"], users[0].to_string());
    assert_eq!(created["files"][0]["path"], "references/guide.md");
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    let skill_path = format!("/api/skills/{id}/?workspace_id={ws}");
    assert_eq!(
        call(&app, "POST", &collection, Some(&jwt[0]), base.clone())
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &skill_path,
            Some(&jwt[1]),
            json!({"name":"takeover"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    // Authorize the parent before decoding or validating a mutation body.
    assert_eq!(
        call(&app, "PUT", &skill_path, Some(&jwt[1]), json!({"files":42}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/skills/{foreign}/?workspace_id={ws}"),
            Some(&jwt[2]),
            json!({"name":"takeover"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(call(&app,"PUT",&skill_path,Some(&jwt[0]),json!({"files":[{"path":"SKILL.md","content":"replaced"},{"path":"new.md","content":"new"}]})).await.0,StatusCode::OK);
    let current:Value=db.query_one("SELECT jsonb_build_object('content',content,'created_by',created_by) FROM skill WHERE id=$1", &[&id]).await.unwrap().get(0);
    assert_eq!(current["content"], "# Alpha");
    assert_eq!(current["created_by"], users[0].to_string());
    assert_eq!(
        db.query("SELECT path FROM skill_file WHERE skill_id=$1", &[&id])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["new.md"]
    );
    let file_path = format!("/api/skills/{id}/files?workspace_id={ws}");
    for path in [
        "/etc/passwd",
        "C:/tmp",
        "a//b",
        "a/./b",
        "a/../b",
        "a\\b",
        "SKILL.md",
    ] {
        assert_eq!(
            call(
                &app,
                "PUT",
                &file_path,
                Some(&jwt[0]),
                json!({"path":path,"content":"bad"})
            )
            .await
            .0,
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
    let (status, added) = call(
        &app,
        "PUT",
        &file_path,
        Some(&jwt[2]),
        json!({"path":"new.md","content":"newer"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(added["content"], "newer");
    assert_eq!(
        call(
            &app,
            "PUT",
            &file_path,
            Some(&jwt[1]),
            json!({"path":"no.md","content":"denied"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let file_id: Uuid = added["id"].as_str().unwrap().parse().unwrap();
    let other_skill: Uuid = db
        .query_one(
            "INSERT INTO skill(workspace_id,name,created_by) VALUES($1,'other',$2) RETURNING id",
            &[&ws, &users[2]],
        )
        .await
        .unwrap()
        .get(0);
    let other_file:Uuid=db.query_one("INSERT INTO skill_file(skill_id,path,content) VALUES($1,'other.md','private') RETURNING id", &[&other_skill]).await.unwrap().get(0);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/skills/{id}/files/{other_file}?workspace_id={ws}"),
            Some(&jwt[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/skills/{id}/files/{file_id}?workspace_id={ws}"),
            Some(&jwt[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    let label_path = format!("/api/skills/{id}/labels?workspace_id={ws}");
    let (missing_label_status, missing_label) =
        call(&app, "POST", &label_path, Some(&jwt[0]), json!({})).await;
    assert_eq!(missing_label_status, StatusCode::BAD_REQUEST);
    assert_eq!(missing_label["error"], "label_id is required");
    assert_eq!(
        call(
            &app,
            "POST",
            &label_path,
            Some(&jwt[0]),
            json!({"label_id":foreign_label})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &label_path,
            Some(&jwt[0]),
            json!({"label_id":wrong_type})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &label_path,
            Some(&jwt[1]),
            json!({"label_id":label})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let (status, labels) = call(
        &app,
        "POST",
        &label_path,
        Some(&jwt[0]),
        json!({"label_id":label}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{labels:?}");
    assert_eq!(labels["labels"][0]["id"], label.to_string());
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/skills/{id}/labels/{foreign_label}?workspace_id={ws}"),
            Some(&jwt[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, empty) = call(
        &app,
        "DELETE",
        &format!("/api/skills/{id}/labels/{label}?workspace_id={ws}"),
        Some(&jwt[0]),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(empty["labels"], json!([]));
    // SQL trigger fails only this synthetic content; skill UPDATE + file replace must roll back.
    let trigger_name = format!("zbsi71_fail_{suffix}");
    db.batch_execute(&format!("CREATE FUNCTION {trigger_name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.content='rollback-{suffix}' THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {trigger_name} BEFORE INSERT ON skill_file FOR EACH ROW EXECUTE FUNCTION {trigger_name}()"))
        .await.unwrap();
    let events_before = events.0.lock().unwrap().len();
    let failure=call(&app,"PUT",&skill_path,Some(&jwt[0]),json!({"content":"uncommitted","files":[{"path":"fail.md","content":format!("rollback-{suffix}")}]})).await;
    assert_eq!(failure.0, StatusCode::INTERNAL_SERVER_ERROR);
    let body: String = db
        .query_one("SELECT content FROM skill WHERE id=$1", &[&id])
        .await
        .unwrap()
        .get(0);
    assert_eq!(body, "# Alpha");
    assert_eq!(events.0.lock().unwrap().len(), events_before);
    let create_failure=call(&app,"POST",&collection,Some(&jwt[0]),json!({"name":"broken","files":[{"path":"fail.md","content":format!("rollback-{suffix}")}]})).await;
    assert_eq!(create_failure.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        db.query_opt(
            "SELECT id FROM skill WHERE workspace_id=$1 AND name='broken'",
            &[&ws]
        )
        .await
        .unwrap()
        .is_none()
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {trigger_name} ON skill_file; DROP FUNCTION {trigger_name}()"
    ))
    .await
    .unwrap();
    // A concurrent same-name creation must produce one row and one event.
    let race = json!({"name": "race"});
    let (left, right) = tokio::join!(
        call(&app, "POST", &collection, Some(&jwt[0]), race.clone()),
        call(&app, "POST", &collection, Some(&jwt[0]), race),
    );
    let statuses = [left.0, right.0];
    assert!(statuses.contains(&StatusCode::CREATED));
    assert!(statuses.contains(&StatusCode::CONFLICT));
    let count: i64 = db
        .query_one(
            "SELECT count(*) FROM skill WHERE workspace_id=$1 AND name='race'",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    // Deleting a skill must clean attached labels and agent links even on schemas without FKs.
    assert_eq!(
        call(
            &app,
            "POST",
            &label_path,
            Some(&jwt[0]),
            json!({"label_id":label})
        )
        .await
        .0,
        StatusCode::OK
    );
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,'synthetic','cloud',$2) RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES($1,$2)",
        &[&agent, &id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "DELETE", &skill_path, Some(&jwt[1]), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "DELETE", &skill_path, Some(&jwt[2]), Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        db.query_opt("SELECT id FROM skill WHERE id=$1", &[&id])
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.query("SELECT * FROM skill_to_label WHERE skill_id=$1", &[&id])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        db.query("SELECT * FROM agent_skill WHERE skill_id=$1", &[&id])
            .await
            .unwrap()
            .is_empty()
    );
    let events = events.0.lock().unwrap();
    assert_eq!(
        events.iter().map(|e| e.event_type).collect::<Vec<_>>(),
        [
            "skill:created",
            "skill:updated",
            "label:updated",
            "label:updated",
            "label:updated",
            "skill:created",
            "label:updated",
            "skill:deleted"
        ]
    );
    assert!(
        events
            .iter()
            .all(|e| e.actor_type == "member" && e.workspace_id == ws)
    );
    assert_eq!(events[0].payload["skill"]["id"], id.to_string());
    assert_eq!(events.last().unwrap().payload["skill_id"], id.to_string());
}
