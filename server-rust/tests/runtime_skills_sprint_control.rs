//! Synthetic update RPC via disposable PostgreSQL and fake heartbeat transport.
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
        runtime_skills_sprint_control::{RuntimeRpcTransport, claim_pending_update},
    },
    health::{Health, MigrationCatalog},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio_postgres::{Config, NoTls, config::Host};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "runtime_skills_cases/local.rs"]
mod local;
#[path = "runtime_skills_cases/local_import.rs"]
mod local_import;
#[path = "runtime_skills_cases/models.rs"]
mod models;

const SECRET: &[u8] = b"zbsi71-update-test-secret";
struct FakeTransport(Mutex<Vec<Uuid>>);
impl RuntimeRpcTransport for FakeTransport {
    fn notify_update_pending(&self, id: Uuid) {
        self.0.lock().unwrap().push(id);
    }
    fn notify_model_list_pending(&self, id: Uuid) {
        self.0.lock().unwrap().push(id);
    }
    fn notify_local_skill_list_pending(&self, id: Uuid) {
        self.0.lock().unwrap().push(id);
    }
    fn notify_local_skill_import_pending(&self, id: Uuid) {
        self.0.lock().unwrap().push(id);
    }
}
fn jwt(user: Uuid) -> String {
    encode(
        &Header::default(),
        &json!({"sub":user,"exp":chrono::Utc::now().timestamp()+3600}),
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}
async fn call(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    ws: Option<Uuid>,
    body: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    if let Some(ws) = ws {
        req = req.header("x-workspace-id", ws.to_string());
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
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
#[ignore = "requires child-owned migrated multica_zbsi71 PostgreSQL on random loopback port"]
async fn update_lifecycle_authorization_concurrency_timeout_and_redaction() {
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
    for schema in [
        include_str!("../runtime_skills_sprint_migrations/001_update_table.sql"),
        include_str!("../runtime_skills_sprint_migrations/002_update_id_index.sql"),
        include_str!("../runtime_skills_sprint_migrations/003_update_active_index.sql"),
    ] {
        db.batch_execute(schema).await.unwrap();
    }
    let [
        owner,
        admin,
        member,
        outsider,
        ws,
        foreign,
        runtime,
        private_runtime,
    ]: [Uuid; 8] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, admin, member, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Control',$2)",
            &[&user, &format!("control-{user}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Control',$2)",
            &[&workspace, &format!("control-{workspace}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [(owner, "member"), (admin, "admin"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id,visibility) VALUES($1,$2,'control-machine','Control','local','synthetic','online',$3,'public')", &[&runtime,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id,visibility) VALUES($1,$2,'private-machine','Private','local','synthetic','online',$3,'private')", &[&private_runtime,&ws,&owner]).await.unwrap();
    let machine = format!("mdt_control_{ws}");
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES($1,$2,'control-machine',$3,now()+interval '1 hour')",
        &[&hex::encode(Sha256::digest(machine.as_bytes())),&ws,&owner]).await.unwrap();
    let wrong = format!("mdt_wrong_{ws}");
    db.execute("INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,owner_id,expires_at) VALUES($1,$2,'private-machine',$3,now()+interval '1 hour')",
        &[&hex::encode(Sha256::digest(wrong.as_bytes())),&ws,&owner]).await.unwrap();
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let disconnected =
        router_with_api(health.clone(), ApiState::new(pool.clone(), SECRET.to_vec()));
    let path = format!("/api/runtimes/{runtime}/update");
    assert_eq!(
        call(
            &disconnected,
            "POST",
            &path,
            Some(&jwt(owner)),
            Some(ws),
            r#"{"target_version":"1.2.3"}"#
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let transport = Arc::new(FakeTransport(Mutex::new(Vec::new())));
    let app = router_with_api(
        health,
        ApiState::new(pool.clone(), SECRET.to_vec()).with_runtime_rpc_transport(transport.clone()),
    );
    for token in [None, Some("bad_token")] {
        assert_eq!(
            call(&app, "POST", &path, token, Some(ws), "{}").await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        call(&app, "POST", &path, Some(&jwt(outsider)), Some(ws), "{}")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_ne!(
        call(&app, "POST", &path, Some(&jwt(owner)), Some(foreign), "{}")
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/runtimes/{private_runtime}/update"),
            Some(&jwt(admin)),
            Some(ws),
            "{}"
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &path, Some(&jwt(member)), Some(ws), "{}")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "POST", &path, Some(&machine), Some(ws), "{}")
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    for invalid in ["bad", "{}", r#"{"target_version":42}"#] {
        assert_eq!(
            call(&app, "POST", &path, Some(&jwt(owner)), Some(ws), invalid)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let payload = r#"{"TARGET_VERSION":"1.2.3","target_version":null,"ignored":[1]} trailing"#;
    let (status, update) = call(&app, "POST", &path, Some(&jwt(owner)), Some(ws), payload).await;
    assert_eq!(status, StatusCode::OK, "{update:?}");
    let id = update["id"].as_str().unwrap().parse::<Uuid>().unwrap();
    assert_eq!(update["target_version"], "1.2.3");
    assert_eq!(transport.0.lock().unwrap().as_slice(), &[runtime]);
    assert_eq!(
        call(
            &app,
            "POST",
            &path,
            Some(&jwt(admin)),
            Some(ws),
            r#"{"target_version":"1.2.4"}"#
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let poll = format!("{path}/{id}");
    assert_eq!(
        call(&app, "GET", &poll, Some(&jwt(member)), Some(ws), "")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, "GET", &poll, Some(&jwt(admin)), Some(ws), "")
            .await
            .0,
        StatusCode::OK
    );
    let report = format!("/api/daemon/runtimes/{runtime}/update/{id}/result");
    assert_eq!(
        call(&app, "POST", &report, Some(&wrong), None, "{}")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &report, Some(&jwt(admin)), None, "{}")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &report, None, None, "{}").await.0,
        StatusCode::UNAUTHORIZED
    );
    let (a, b) = tokio::join!(claim_pending_update(&**db, runtime), async {
        let other = pool.get().await.unwrap();
        claim_pending_update(&**other, runtime).await
    });
    assert_eq!(
        [a.unwrap(), b.unwrap()]
            .iter()
            .filter(|r| r.is_some())
            .count(),
        1
    );
    assert_eq!(
        call(&app, "POST", &report, Some(&machine), None, "bad JSON")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &report,
            Some(&machine),
            None,
            r#"{"status":"unknown"}"#
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            &report,
            Some(&machine),
            None,
            r#"{"STATUS":"running"}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    let synthetic = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
    let result = json!({"status":"completed","output":format!("release {synthetic}")}).to_string();
    assert_eq!(
        call(&app, "POST", &report, Some(&machine), None, &result)
            .await
            .0,
        StatusCode::OK
    );
    let (_, completed) = call(&app, "GET", &poll, Some(&jwt(owner)), Some(ws), "").await;
    assert_eq!(completed["status"], "completed");
    assert_eq!(completed["output"], "release [REDACTED GITHUB TOKEN]");
    assert!(!completed.to_string().contains(synthetic));
    assert_eq!(
        call(&app, "POST", &report, Some(&machine), None, "bad JSON")
            .await
            .0,
        StatusCode::OK,
        "terminal report ignores stale body"
    );
    assert_eq!(
        call(&app, "GET", &poll, Some(&jwt(owner)), Some(ws), "")
            .await
            .1["status"],
        "completed"
    );
    let (status, second) = call(
        &app,
        "POST",
        &path,
        Some(&jwt(admin)),
        Some(ws),
        r#"{"target_version":"1.2.4"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second:?}");
    let second_id = second["id"].as_str().unwrap();
    db.execute(
        "UPDATE runtime_rpc_update SET created_at=now()-interval '121 seconds' WHERE id=$1",
        &[&Uuid::parse_str(second_id).unwrap()],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{second_id}"),
            Some(&jwt(admin)),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "timeout"
    );
    let (status, third) = call(
        &app,
        "POST",
        &path,
        Some(&jwt(owner)),
        Some(ws),
        r#"{"target_version":"1.2.5"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{third:?}");
    let third_id = Uuid::parse_str(third["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        claim_pending_update(&**db, runtime)
            .await
            .unwrap()
            .unwrap()
            .0,
        third_id
    );
    db.execute(
        "UPDATE runtime_rpc_update SET started_at=now()-interval '151 seconds' WHERE id=$1",
        &[&third_id],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            &format!("/api/daemon/runtimes/{runtime}/update/{third_id}/result"),
            Some(&machine),
            None,
            r#"{"status":"completed"}"#
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{third_id}"),
            Some(&jwt(owner)),
            Some(ws),
            ""
        )
        .await
        .1["status"],
        "timeout"
    );
    // Concurrent creates reserve exactly one live update, even on separate API requests.
    let owner_token = jwt(owner);
    let admin_token = jwt(admin);
    let (left, right) = tokio::join!(
        call(
            &app,
            "POST",
            &path,
            Some(&owner_token),
            Some(ws),
            r#"{"target_version":"rollback-test"}"#
        ),
        call(
            &app,
            "POST",
            &path,
            Some(&admin_token),
            Some(ws),
            r#"{"target_version":"rollback-test"}"#
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
    let winner = if left.0 == StatusCode::OK {
        left.1
    } else {
        right.1
    };
    let winner_id = Uuid::parse_str(winner["id"].as_str().unwrap()).unwrap();
    let winner_report = format!("/api/daemon/runtimes/{runtime}/update/{winner_id}/result");
    db.batch_execute("CREATE FUNCTION runtime_rpc_update_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'DB_PASSWORD=must-not-leak'; END $$; CREATE TRIGGER runtime_rpc_update_test_fail BEFORE UPDATE ON runtime_rpc_update FOR EACH ROW WHEN (NEW.status='completed' AND NEW.target_version='rollback-test') EXECUTE FUNCTION runtime_rpc_update_test_fail();").await.unwrap();
    let failure = call(
        &app,
        "POST",
        &winner_report,
        Some(&machine),
        None,
        r#"{"status":"completed","output":"must-roll-back"}"#,
    )
    .await;
    assert_eq!(failure.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!failure.1.to_string().contains("must-not-leak"));
    let row = db
        .query_one(
            "SELECT status,output FROM runtime_rpc_update WHERE id=$1",
            &[&winner_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "pending");
    assert_eq!(row.get::<_, String>(1), "");
    db.batch_execute("DROP TRIGGER runtime_rpc_update_test_fail ON runtime_rpc_update; DROP FUNCTION runtime_rpc_update_test_fail();").await.unwrap();
    let (left, right) = tokio::join!(
        call(
            &app,
            "POST",
            &winner_report,
            Some(&machine),
            None,
            r#"{"status":"completed","output":"first"}"#
        ),
        call(
            &app,
            "POST",
            &winner_report,
            Some(&machine),
            None,
            r#"{"status":"failed","error":"second"}"#
        ),
    );
    assert_eq!(left.0, StatusCode::OK);
    assert_eq!(right.0, StatusCode::OK);
    let row = db
        .query_one(
            "SELECT status,output,error FROM runtime_rpc_update WHERE id=$1",
            &[&winner_id],
        )
        .await
        .unwrap();
    match row.get::<_, String>(0).as_str() {
        "completed" => {
            assert_eq!(row.get::<_, String>(1), "first");
            assert_eq!(row.get::<_, String>(2), "");
        }
        "failed" => {
            assert_eq!(row.get::<_, String>(1), "");
            assert_eq!(row.get::<_, String>(2), "second");
        }
        other => panic!("unexpected terminal {other}"),
    }
    // A demoted initiator can keep polling only while still allowed to read this runtime.
    let initiated = call(
        &app,
        "POST",
        &path,
        Some(&admin_token),
        Some(ws),
        r#"{"target_version":"1.2.6"}"#,
    )
    .await;
    assert_eq!(initiated.0, StatusCode::OK);
    let initiated_id = initiated.1["id"].as_str().unwrap();
    db.execute(
        "UPDATE member SET role='member' WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &admin],
    )
    .await
    .unwrap();
    let view = call(
        &app,
        "GET",
        &format!("{path}/{initiated_id}"),
        Some(&admin_token),
        Some(ws),
        "",
    )
    .await;
    assert_eq!(view.0, StatusCode::OK);
    assert!(view.1.get("initiator_user_id").is_none());
    assert!(view.1.get("started_at").is_none());
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("{path}/{initiated_id}"),
            Some(&jwt(member)),
            Some(ws),
            ""
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    models::check(
        &app,
        &pool,
        runtime,
        private_runtime,
        ws,
        owner,
        admin,
        member,
        &machine,
        &wrong,
    )
    .await;
    local::check(
        &app,
        &pool,
        runtime,
        private_runtime,
        ws,
        owner,
        admin,
        member,
        outsider,
        foreign,
        &machine,
        &wrong,
        &transport,
    )
    .await;
    local_import::check(
        &pool, &transport, runtime, ws, owner, member, &machine, &wrong,
    )
    .await;
    db.execute(
        "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&app, "GET", &poll, Some(&jwt(owner)), Some(ws), "")
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "POST", &report, Some(&machine), None, "{}")
            .await
            .0,
        StatusCode::OK,
        "bound machine keeps valid ownership on member removal"
    );
}
