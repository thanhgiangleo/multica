//! Real PostgreSQL checks for quick-action definition management, never execution.
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use deadpool_postgres::{Manager, Pool};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header, encode};
use multica_server::{
    api::ApiState,
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

fn jwt(user: Uuid) -> String {
    encode(&Header::default(), &json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}), &EncodingKey::from_secret(b"queries-sprint-action-test")).unwrap()
}

async fn request(
    app: &axum::Router,
    ws: Uuid,
    user: Option<Uuid>,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(format!("/api/quick-actions/{path}?workspace_id={ws}"))
        .header("content-type", "application/json");
    if let Some(user) = user {
        req = req.header("authorization", format!("Bearer {}", jwt(user)));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
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
#[ignore = "requires owned migrated PostgreSQL 16 on random loopback port"]
async fn quick_action_definitions_enforce_privacy_roles_bindings_and_atomic_cap() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi69"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host,tokio_postgres::config::Host::Tcp(ip) if ip=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4();
    let mut users = Vec::new();
    for name in ["owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[
                    &name,
                    &format!("queries-action-{name}-{suffix}@invalid.example"),
                ],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Action Workspace',$1) RETURNING id",
            &[&format!("action-ws-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign Action Workspace',$1) RETURNING id",
            &[&format!("action-other-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    db.execute("INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner'),($1,$3,'member'),($4,$2,'owner')", &[&ws,&users[0],&users[1],&other]).await.unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let public: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode) VALUES ($1,'Public Agent','cloud',$2,$3,'public_to') RETURNING id", &[&ws,&runtime,&users[0]]).await.unwrap().get(0);
    let private: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode) VALUES ($1,'Restricted Agent','cloud',$2,$3,'private') RETURNING id", &[&ws,&runtime,&users[0]]).await.unwrap().get(0);
    let foreign: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,runtime_id,owner_id,permission_mode) VALUES ($1,'Foreign Agent','cloud',$2,$3,'public_to') RETURNING id", &[&other,&runtime,&users[0]]).await.unwrap().get(0);
    db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2)", &[&public,&ws]).await.unwrap();
    let squad: Uuid = db.query_one("INSERT INTO squad(workspace_id,name,leader_id,creator_id) VALUES ($1,'Action Squad',$2,$3) RETURNING id", &[&ws,&public,&users[0]]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "queries-sprint-action".into(),
        )),
        ApiState::new(pool.clone(), b"queries-sprint-action-test".to_vec()),
    );
    let basic = json!({"name":" Triage ","description":" Notes ","prompt":" Review this issue ","visibility":"private"});
    assert_eq!(
        request(&app, ws, None, "POST", "", basic.clone()).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, ws, Some(users[2]), "POST", "", basic.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[1]),
            "POST",
            "",
            json!({"name":"Public","prompt":"Go"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, ws, Some(users[1]), "POST", "", json!({}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (status, personal) = request(&app, ws, Some(users[1]), "POST", "", basic.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(personal["name"], "Triage");
    assert_eq!(personal["description"], "Notes");
    assert_eq!(personal["prompt"], "Review this issue");
    assert_eq!(personal["assignee_id"], "");
    assert_eq!(personal["target_public"], true);
    assert_eq!(personal["target_missing"], false);
    assert!(personal.get("target_name").is_none());
    assert!(personal["last_used_at"].is_null());
    assert_eq!(personal["use_count"], 0);
    assert_eq!(personal["created_by_id"], users[1].to_string());
    let personal_id = personal["id"].as_str().unwrap();
    for method in ["PATCH", "DELETE"] {
        assert_eq!(
            request(
                &app,
                ws,
                Some(users[0]),
                method,
                personal_id,
                json!({"prompt":"Steal"})
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        request(&app, ws, Some(users[0]), "PATCH", personal_id, json!([]))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &app,
            other,
            Some(users[0]),
            "PATCH",
            personal_id,
            json!({"prompt":"Cross workspace"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[1]),
            "PATCH",
            personal_id,
            json!({"visibility":"public"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for bad in [
        json!({"name":"","prompt":"Review"}),
        json!({"name":"A","prompt":"{{issue.title}}"}),
        json!({"name":"A","prompt":"[@x](mention://member/x)"}),
        json!({"name":"A","prompt":"mention://all/workspace"}),
        json!({"name":"A","prompt":"mention://squad/x"}),
        json!({"name":"🌱".repeat(33),"prompt":"Review"}),
        json!({"name":"A","prompt":"Review","description":"é".repeat(201)}),
        json!({"name":"A","prompt":"Review","assignee_type":"member","assignee_id":users[0]}),
        json!({"name":"A","prompt":"Review","assignee_type":"agent","assignee_id":"bad"}),
        json!({"name":"A","prompt":"Review","assignee_type":"agent","assignee_id":foreign}),
        json!({"name":"A","prompt":"Review","assignee_type":"agent","assignee_id":private}),
    ] {
        assert_eq!(
            request(&app, ws, Some(users[0]), "POST", "", bad.clone())
                .await
                .0,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }
    let (status, shared) = request(&app,ws,Some(users[0]),"POST","",json!({"name":"Squad action","prompt":"Review mention://issue/example","assignee_type":"squad","assignee_id":squad})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(shared["target_name"], "Action Squad");
    assert_eq!(shared["target_public"], true);
    let shared_id = shared["id"].as_str().unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[1]),
            "PATCH",
            shared_id,
            json!({"prompt":"Rewrite"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, ws, Some(users[1]), "DELETE", shared_id, Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    // A target's current permission is rechecked even when the patch only changes its name.
    db.execute(
        "UPDATE agent SET permission_mode='private' WHERE id=$1",
        &[&public],
    )
    .await
    .unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[0]),
            "PATCH",
            shared_id,
            json!({"name":"Changed"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (status, changed) = request(
        &app,
        ws,
        Some(users[0]),
        "PATCH",
        shared_id,
        json!({"visibility":"private","name":"Changed"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(changed["target_public"], false);
    let (status, cleared) = request(
        &app,
        ws,
        Some(users[0]),
        "PATCH",
        shared_id,
        json!({"assignee_type":"","assignee_id":"","name":null}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleared["assignee_type"], "");
    assert_eq!(cleared["assignee_id"], "");
    assert_eq!(cleared["name"], "Changed");
    // Trigger failure proves that a failed patch does not persist sibling fields.
    db.batch_execute("CREATE FUNCTION queries_sprint_action_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='Fail update' THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER queries_sprint_action_rollback BEFORE UPDATE ON quick_action FOR EACH ROW EXECUTE FUNCTION queries_sprint_action_fail();").await.unwrap();
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[0]),
            "PATCH",
            shared_id,
            json!({"name":"Fail update","prompt":"Must not persist"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let stored: String = db
        .query_one(
            "SELECT prompt FROM quick_action WHERE id=$1",
            &[&Uuid::parse_str(shared_id).unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, "Review mention://issue/example");
    db.batch_execute("DROP TRIGGER queries_sprint_action_rollback ON quick_action; DROP FUNCTION queries_sprint_action_fail();").await.unwrap();
    assert_eq!(
        request(&app, ws, Some(users[0]), "DELETE", shared_id, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(&app, ws, Some(users[0]), "DELETE", shared_id, Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    let (status, archived) = request(
        &app,
        ws,
        Some(users[1]),
        "PATCH",
        personal_id,
        json!({"status":"archived"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(archived["status"], "archived");
    // Two different members compete for slot 30. Workspace lock, not per-user
    // locking, keeps the active cap true across simultaneous creates.
    db.execute("INSERT INTO quick_action(workspace_id,name,prompt,assignee_type,visibility,created_by_type,created_by_id) SELECT $1,'Filler '||g,'Review','','private','member',$2 FROM generate_series(1,29) g", &[&ws,&users[0]]).await.unwrap();
    let (a, b) = tokio::join!(
        request(&app, ws, Some(users[0]), "POST", "", basic.clone()),
        request(&app, ws, Some(users[1]), "POST", "", basic.clone()),
    );
    assert!(matches!(
        (a.0, b.0),
        (StatusCode::CREATED, StatusCode::BAD_REQUEST)
            | (StatusCode::BAD_REQUEST, StatusCode::CREATED)
    ));
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM quick_action WHERE workspace_id=$1 AND status='active'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        30
    );
    assert_eq!(
        request(
            &app,
            ws,
            Some(users[1]),
            "PATCH",
            personal_id,
            json!({"status":"active"})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let listed = request(&app, ws, Some(users[1]), "GET", "", Value::Null).await;
    assert_eq!(listed.0, StatusCode::OK);
    assert!(
        listed.1["quick_actions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|q| q["created_by_id"] == users[1].to_string())
    );
    assert_eq!(
        request(&app, ws, Some(users[1]), "DELETE", personal_id, Value::Null)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
}
