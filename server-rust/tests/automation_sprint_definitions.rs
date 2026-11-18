//! Disposable PostgreSQL + real HTTP/auth contracts. No provider or runtime invocation.
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
    migrate::{self, Direction, Stream},
    router_with_api,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "automation_sprint/automation_sprint_failure_monitor_test.rs"]
mod automation_sprint_failure_monitor_test;
#[path = "automation_sprint/automation_sprint_ingress_test.rs"]
mod automation_sprint_ingress_test;
#[path = "automation_sprint/automation_sprint_lifecycle_test.rs"]
mod automation_sprint_lifecycle_test;
#[path = "automation_sprint/automation_sprint_manual_test.rs"]
mod automation_sprint_manual_test;
#[path = "automation_sprint/automation_sprint_quota_test.rs"]
mod automation_sprint_quota_test;
#[path = "automation_sprint/automation_sprint_schedule_test.rs"]
mod automation_sprint_schedule_test;
#[path = "automation_sprint/automation_sprint_worker_test.rs"]
mod automation_sprint_worker_test;

const SECRET: &[u8] = b"synthetic-automation-sprint-test-only";
fn token(user: Uuid) -> String {
    encode(&Header::default(),&json!({"sub":user,"exp":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+3600}),&EncodingKey::from_secret(SECRET)).unwrap()
}
async fn request(
    app: &axum::Router,
    method: &str,
    path: &str,
    ws: Uuid,
    token: Option<&str>,
    input: Value,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("X-Workspace-ID", ws.to_string())
        .header("content-type", "application/json");
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(req.body(Body::from(input.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("HTTP JSON")
        },
    )
}
async fn replay_request(
    app: &axum::Router,
    path: &str,
    ws: Uuid,
    bearer: &str,
    key: &str,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("X-Workspace-ID", ws.to_string())
                .header("Authorization", format!("Bearer {bearer}"))
                .header("Idempotency-Key", key)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let data = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&data).unwrap())
}
async fn versions(db: &tokio_postgres::Client, id: Uuid) -> i64 {
    db.query_one(
        "SELECT count(*) FROM autopilot_rule_version WHERE autopilot_id=$1",
        &[&id],
    )
    .await
    .unwrap()
    .get(0)
}

#[tokio::test]
#[ignore = "requires owned disposable loopback multica_zbsi75 PostgreSQL"]
async fn definitions_scope_permissions_transactions_and_credentials() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi75"));
    assert!(!config.get_hosts().is_empty());
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
            lock_key: 7501,
            hooks: &up,
            production_gates: true,
        },
        &Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 7502,
            hooks: &zbs,
            production_gates: true,
        },
        Direction::Up,
    )
    .await
    .unwrap();
    let db = pool.get().await.unwrap();
    let nonce = Uuid::new_v4();
    let mut users = Vec::new();
    for name in ["admin", "creator", "collaborator", "reader", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("automation-{name}-{nonce}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Automation',$1) RETURNING id",
            &[&format!("automation-{nonce}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign',$1) RETURNING id",
            &[&format!("automation-other-{nonce}")],
        )
        .await
        .unwrap()
        .get(0);
    for (i, role) in ["owner", "member", "member", "member"].iter().enumerate() {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &users[i], &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&other, &users[0]],
    )
    .await
    .unwrap();
    let runtime: Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Automation synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Synthetic automation agent','cloud',$2,$3) RETURNING id", &[&ws,&users[0],&runtime]).await.unwrap().get(0);
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "automation-test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let tokens: Vec<_> = users.iter().map(|u| token(*u)).collect();
    let input = json!({"title":"Daily audit","description":"synthetic","assignee_id":agent,"execution_mode":"run_only","issue_title_template":"Review {{ date }}","subscribers":[{"user_type":"member","user_id":users[3]}]});
    for (token, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(tokens[4].as_str()), StatusCode::NOT_FOUND),
    ] {
        let result = request(&app, "POST", "/api/autopilots/", ws, token, input.clone()).await;
        assert_eq!(result.0, status, "{result:?}");
    }
    let mut invalid = input.clone();
    invalid["subscribers"] = json!([{"user_type":"member","user_id":users[4]}]);
    let result = request(
        &app,
        "POST",
        "/api/autopilots/",
        ws,
        Some(&tokens[1]),
        invalid,
    )
    .await;
    assert_eq!(result.0, StatusCode::BAD_REQUEST, "{result:?}");
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM autopilot WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let created = request(
        &app,
        "POST",
        "/api/autopilots/",
        ws,
        Some(&tokens[1]),
        input,
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED, "{created:?}");
    let id = Uuid::parse_str(created.1["id"].as_str().unwrap()).unwrap();
    let path = format!("/api/autopilots/{id}/");
    assert_eq!(created.1["created_by_id"], users[1].to_string());
    assert_eq!(created.1["subscribers"].as_array().unwrap().len(), 1);
    assert_eq!(versions(&db, id).await, 1);
    let foreign = request(&app, "GET", &path, other, Some(&tokens[0]), Value::Null).await;
    assert_eq!(foreign.0, StatusCode::NOT_FOUND);
    let denied = request(
        &app,
        "PATCH",
        &path,
        ws,
        Some(&tokens[3]),
        json!({"title":"unauthorized"}),
    )
    .await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN);
    let grant = request(
        &app,
        "POST",
        &format!("/api/autopilots/{id}/collaborators"),
        ws,
        Some(&tokens[1]),
        json!({"user_id":users[2]}),
    )
    .await;
    assert_eq!(grant.0, StatusCode::CREATED, "{grant:?}");
    let regrant = request(
        &app,
        "POST",
        &format!("/api/autopilots/{id}/collaborators"),
        ws,
        Some(&tokens[2]),
        json!({"user_id":users[3]}),
    )
    .await;
    assert_eq!(regrant.0, StatusCode::FORBIDDEN);
    let renamed = request(
        &app,
        "PATCH",
        &path,
        ws,
        Some(&tokens[2]),
        json!({"title":"Renamed"}),
    )
    .await;
    assert_eq!(renamed.0, StatusCode::OK, "{renamed:?}");
    assert_eq!(versions(&db, id).await, 1);
    let webhook=request(&app,"POST",&format!("/api/autopilots/{id}/triggers"),ws,Some(&tokens[2]),json!({"kind":"webhook","provider":"github","event_filters":[{"event":"pull_request","actions":["opened"]}]})).await;
    assert_eq!(webhook.0, StatusCode::CREATED, "{webhook:?}");
    let trigger = Uuid::parse_str(webhook.1["id"].as_str().unwrap()).unwrap();
    let tp = format!("/api/autopilots/{id}/triggers/{trigger}");
    let credential = webhook.1["webhook_token"].as_str().unwrap().to_owned();
    assert!(credential.starts_with("awt_"));
    assert_eq!(credential.len(), 47);
    let reader = request(&app, "GET", &path, ws, Some(&tokens[3]), Value::Null).await;
    assert_eq!(reader.0, StatusCode::OK);
    assert_eq!(reader.1["autopilot"]["can_write"], false);
    for field in ["webhook_token", "webhook_path", "webhook_url"] {
        assert!(reader.1["triggers"][0][field].is_null(), "{field}");
    }
    assert!(!reader.1.to_string().contains(&credential));
    let signing = request(
        &app,
        "PUT",
        &format!("{tp}/signing-secret"),
        ws,
        Some(&tokens[1]),
        json!({"signing_secret":"synthetic-signing-credential"}),
    )
    .await;
    assert_eq!(signing.0, StatusCode::OK, "{signing:?}");
    assert_eq!(signing.1["has_signing_secret"], true);
    assert!(signing.1.get("signing_secret").is_none());
    let rotated = request(
        &app,
        "POST",
        &format!("{tp}/rotate-webhook-token"),
        ws,
        Some(&tokens[1]),
        json!({}),
    )
    .await;
    assert_eq!(rotated.0, StatusCode::OK, "{rotated:?}");
    assert_ne!(rotated.1["webhook_token"], credential);
    let row = db
        .query_one(
            "SELECT created_by_id,published_by_id FROM autopilot_trigger WHERE id=$1",
            &[&trigger],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Uuid>(0), users[2]);
    assert_eq!(row.get::<_, Uuid>(1), users[2]);
    let before = versions(&db, id).await;
    let labeled = request(
        &app,
        "PATCH",
        &format!("{tp}/"),
        ws,
        Some(&tokens[1]),
        json!({"label":"Display only"}),
    )
    .await;
    assert_eq!(labeled.0, StatusCode::OK, "{labeled:?}");
    assert_eq!(versions(&db, id).await, before);
    let cleared = request(
        &app,
        "PATCH",
        &format!("{tp}/"),
        ws,
        Some(&tokens[1]),
        json!({"event_filters":[]}),
    )
    .await;
    assert_eq!(cleared.0, StatusCode::OK, "{cleared:?}");
    assert!(cleared.1.get("event_filters").is_none());
    assert_eq!(versions(&db, id).await, before + 1);
    automation_sprint_ingress_test::verify(
        &app,
        &db,
        &pool,
        id,
        trigger,
        &credential,
        rotated.1["webhook_token"].as_str().unwrap(),
        ws,
        &tokens[1],
    )
    .await;
    let schedule = request(
        &app,
        "POST",
        &format!("/api/autopilots/{id}/triggers"),
        ws,
        Some(&tokens[1]),
        json!({"kind":"schedule","cron_expression":"*/5 * * * *","timezone":"Asia/Ho_Chi_Minh"}),
    )
    .await;
    assert_eq!(schedule.0, StatusCode::CREATED, "{schedule:?}");
    assert!(schedule.1["next_run_at"].is_string());
    assert!(schedule.1["webhook_token"].is_null());
    let invalid = request(
        &app,
        "POST",
        &format!("/api/autopilots/{id}/triggers"),
        ws,
        Some(&tokens[1]),
        json!({"kind":"schedule","cron_expression":"bad","timezone":"UTC"}),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{invalid:?}");
    let preview = request(
        &app,
        "GET",
        "/api/autopilots/cron-preview?expr=0+9+*+*+*&tz=UTC",
        ws,
        Some(&tokens[1]),
        Value::Null,
    )
    .await;
    assert_eq!(preview.0, StatusCode::OK, "{preview:?}");
    assert_eq!(preview.1["next_runs"].as_array().unwrap().len(), 3);
    let never = request(
        &app,
        "GET",
        "/api/autopilots/cron-preview?expr=0+0+30+2+*",
        ws,
        Some(&tokens[1]),
        Value::Null,
    )
    .await;
    assert_eq!(never.0, StatusCode::OK, "{never:?}");
    assert_eq!(never.1["next_runs"], json!([]));
    let never_trigger = request(
        &app,
        "POST",
        &format!("/api/autopilots/{id}/triggers"),
        ws,
        Some(&tokens[1]),
        json!({"kind":"schedule","cron_expression":"0 0 30 2 *","timezone":"UTC"}),
    )
    .await;
    assert_eq!(never_trigger.0, StatusCode::CREATED, "{never_trigger:?}");
    assert!(
        never_trigger.1["next_run_at"]
            .as_str()
            .unwrap()
            .starts_with("0001-01-01")
    );
    // Bound runtime owner must not replace recorded human originator.
    let task: Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,'running',$3,$3) RETURNING id", &[&agent,&runtime,&users[3]]).await.unwrap().get(0);
    let task_token = format!("mat_automation_{nonce}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&task,&agent,&ws,&users[0]]).await.unwrap();
    let denied = request(
        &app,
        "PATCH",
        &path,
        ws,
        Some(&task_token),
        json!({"title":"runtime-owner-escalation"}),
    )
    .await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN, "{denied:?}");
    let authorized_task: Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status,originator_user_id,accountable_user_id) VALUES ($1,$2,'running',$3,$3) RETURNING id", &[&agent,&runtime,&users[1]]).await.unwrap().get(0);
    let task_token = format!("mat_automation_authorized_{nonce}");
    let hash = hex::encode(Sha256::digest(task_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&authorized_task,&agent,&ws,&users[0]]).await.unwrap();
    let allowed = request(
        &app,
        "PATCH",
        &path,
        ws,
        Some(&task_token),
        json!({"title":"Originator authorized"}),
    )
    .await;
    assert_eq!(allowed.0, StatusCode::OK, "{allowed:?}");
    // Fault-injected version publication must roll back trigger definition changes.
    db.batch_execute("CREATE FUNCTION automation_sprint_reject_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic rollback'; END $$; CREATE TRIGGER automation_sprint_reject_version BEFORE INSERT ON autopilot_rule_version FOR EACH ROW EXECUTE FUNCTION automation_sprint_reject_version();").await.unwrap();
    let old: bool = db
        .query_one(
            "SELECT enabled FROM autopilot_trigger WHERE id=$1",
            &[&trigger],
        )
        .await
        .unwrap()
        .get(0);
    let failed = request(
        &app,
        "PATCH",
        &format!("{tp}/"),
        ws,
        Some(&tokens[1]),
        json!({"enabled":!old}),
    )
    .await;
    assert_eq!(failed.0, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
    assert_eq!(
        db.query_one(
            "SELECT enabled FROM autopilot_trigger WHERE id=$1",
            &[&trigger]
        )
        .await
        .unwrap()
        .get::<_, bool>(0),
        old
    );
    db.batch_execute("DROP TRIGGER automation_sprint_reject_version ON autopilot_rule_version; DROP FUNCTION automation_sprint_reject_version();").await.unwrap();
    let original: Uuid = db.query_one("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,event,status,selected_headers,content_type,raw_body) VALUES ($1,$2,$3,'github','github.pull_request.opened','dispatched',$4,'application/json',$5) RETURNING id", &[&ws,&id,&trigger,&json!({"x-github-event":"pull_request"}),&br#"{"action":"opened"}"#.as_slice()]).await.unwrap().get(0);
    let replay_path = format!("/api/autopilots/{id}/deliveries/{original}/replay");
    let foreign = replay_request(&app, &replay_path, other, &tokens[0], "foreign").await;
    assert_eq!(foreign.0, StatusCode::NOT_FOUND, "{foreign:?}");
    let denied = replay_request(&app, &replay_path, ws, &tokens[3], "no-writer").await;
    assert_eq!(denied.0, StatusCode::FORBIDDEN, "{denied:?}");
    let key = format!("replay-{nonce}");
    let (first, second) = tokio::join!(
        replay_request(&app, &replay_path, ws, &tokens[1], &key),
        replay_request(&app, &replay_path, ws, &tokens[1], &key)
    );
    assert_eq!(first.0, StatusCode::ACCEPTED, "{first:?}");
    assert_eq!(second.0, StatusCode::ACCEPTED, "{second:?}");
    assert_eq!(first.1["id"], second.1["id"]);
    assert_eq!(first.1["event"], "github.pull_request.opened");
    assert_eq!(first.1["status"], "queued");
    assert!(first.1["dedupe_key"].is_null());
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM webhook_delivery WHERE replayed_from_delivery_id=$1",
            &[&original]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let rejected: Uuid=db.query_one("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,event,status,signature_status,raw_body) VALUES ($1,$2,$3,'github','bad','rejected','invalid',$4) RETURNING id",&[&ws,&id,&trigger,&br#"{}"#.as_slice()]).await.unwrap().get(0);
    let rejected_path = format!("/api/autopilots/{id}/deliveries/{rejected}/replay");
    assert_eq!(
        replay_request(&app, &rejected_path, ws, &tokens[1], "rejected")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let malformed: Uuid=db.query_one("INSERT INTO webhook_delivery(workspace_id,autopilot_id,trigger_id,provider,event,status,raw_body) VALUES ($1,$2,$3,'github','bad','failed',$4) RETURNING id",&[&ws,&id,&trigger,&b"7".to_vec()]).await.unwrap().get(0);
    let malformed_path = format!("/api/autopilots/{id}/deliveries/{malformed}/replay");
    assert_eq!(
        replay_request(&app, &malformed_path, ws, &tokens[1], "bad-shape")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let disabled = request(
        &app,
        "PATCH",
        &format!("{tp}/"),
        ws,
        Some(&tokens[1]),
        json!({"enabled":false}),
    )
    .await;
    assert_eq!(disabled.0, StatusCode::OK, "{disabled:?}");
    assert_eq!(
        replay_request(&app, &replay_path, ws, &tokens[1], "disabled")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let enabled = request(
        &app,
        "PATCH",
        &format!("{tp}/"),
        ws,
        Some(&tokens[1]),
        json!({"enabled":true}),
    )
    .await;
    assert_eq!(enabled.0, StatusCode::OK, "{enabled:?}");
    let pause = request(
        &app,
        "PATCH",
        &path,
        ws,
        Some(&tokens[1]),
        json!({"status":"paused"}),
    )
    .await;
    assert_eq!(pause.0, StatusCode::OK, "{pause:?}");
    assert_eq!(
        replay_request(&app, &replay_path, ws, &tokens[1], "while-paused")
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let run: Uuid=db.query_one("INSERT INTO autopilot_run(autopilot_id,source,status) VALUES ($1,'manual','completed') RETURNING id", &[&id]).await.unwrap().get(0);
    let archived = request(&app, "DELETE", &path, ws, Some(&tokens[1]), Value::Null).await;
    assert_eq!(archived.0, StatusCode::NO_CONTENT, "{archived:?}");
    assert_eq!(
        db.query_one("SELECT status FROM autopilot WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, String>(0),
        "archived"
    );
    assert_eq!(
        db.query_one("SELECT count(*) FROM autopilot_run WHERE id=$1", &[&run])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    automation_sprint_manual_test::verify(&db, &pool, ws, agent, runtime, &users).await;
    automation_sprint_quota_test::verify(&db, &pool, ws, other, agent, runtime, &users).await;
    automation_sprint_schedule_test::verify(&db, &pool, ws, agent, users[1]).await;
    automation_sprint_lifecycle_test::verify(&db, &pool, ws, other, agent, runtime, users[1]).await;
    automation_sprint_failure_monitor_test::verify(&db, &pool, ws, other, agent, users[0]).await;
    println!(
        "automation_sprint: HTTP auth, scope, grants, originator, cron, secrets, publisher, rollback and archive assertions passed"
    );
}
