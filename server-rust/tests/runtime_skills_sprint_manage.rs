//! Synthetic runtime metadata changes through HTTP and disposable PostgreSQL.
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
        runtime_skills_sprint_manage::{RuntimeMetadataEvent, RuntimeMetadataEventSink},
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

const SECRET: &[u8] = b"zbsi71-metadata-synthetic-secret";
#[derive(Default)]
struct Events(Mutex<Vec<RuntimeMetadataEvent>>);
impl RuntimeMetadataEventSink for Events {
    fn publish(&self, event: RuntimeMetadataEvent) {
        self.0.lock().unwrap().push(event);
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
    path: &str,
    token: Option<&str>,
    ws: Uuid,
    body: &str,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("PATCH")
        .uri(path)
        .header("x-workspace-id", ws.to_string())
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let resp = app
        .clone()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let code = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (code, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
#[ignore = "requires child-owned disposable migrated PostgreSQL on random loopback port"]
async fn metadata_authorization_sharing_dependencies_machine_rename_and_rollback() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi71"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|host| matches!(host,Host::Tcp(s) if s=="127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let [
        owner,
        admin,
        member,
        other,
        outsider,
        ws,
        foreign,
        rt,
        second,
        private,
        qoder,
    ]: [Uuid; 11] = std::array::from_fn(|_| Uuid::new_v4());
    for user in [owner, admin, member, other, outsider] {
        db.execute(
            "INSERT INTO \"user\"(id,name,email) VALUES($1,'Runtime User',$2)",
            &[&user, &format!("runtime-{user}@invalid.example")],
        )
        .await
        .unwrap();
    }
    for workspace in [ws, foreign] {
        db.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,'Runtime',$2)",
            &[&workspace, &format!("runtime-{workspace}")],
        )
        .await
        .unwrap();
    }
    for (user, role) in [
        (owner, "member"),
        (admin, "admin"),
        (member, "member"),
        (other, "member"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    for (id, daemon, runtime_owner, provider, visibility) in [
        (rt, "machine", owner, "synthetic", "public"),
        (second, "machine", other, "synthetic_other", "private"),
        (private, "isolated", owner, "synthetic", "private"),
        (qoder, "managed", owner, "qoder_cloud", "public"),
    ] {
        db.execute("INSERT INTO agent_runtime(id,workspace_id,daemon_id,name,runtime_mode,provider,status,owner_id,visibility) VALUES($1,$2,$3,'Synthetic','local',$4,'online',$5,$6)",&[&id,&ws,&daemon,&provider,&runtime_owner,&visibility]).await.unwrap();
    }
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let closed = router_with_api(health.clone(), ApiState::new(pool.clone(), SECRET.to_vec()));
    let path = format!("/api/runtimes/{rt}/");
    assert_eq!(
        call(&closed, &path, Some(&jwt(owner)), ws, "{}").await.0,
        StatusCode::NOT_FOUND
    );
    let events = Arc::new(Events::default());
    let app = router_with_api(
        health,
        ApiState::new(pool.clone(), SECRET.to_vec())
            .with_runtime_metadata_event_sink(events.clone()),
    );
    for token in [None, Some("invalid"), Some("mdt_fake_machine_token")] {
        assert_eq!(
            call(&app, &path, token, ws, "{}").await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        call(&app, &path, Some(&jwt(outsider)), ws, "{}").await.0,
        StatusCode::NOT_FOUND
    );
    assert_ne!(
        call(&app, &path, Some(&jwt(owner)), foreign, "{}").await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, &path, Some(&jwt(member)), ws, "{}").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            &format!("/api/runtimes/{private}/"),
            Some(&jwt(member)),
            ws,
            "{}"
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for bad in [
        "{",
        r#"{"custom_name":3}"#,
        r#"{"visibility":"workspace"}"#,
        r#"{"shared_with":["bad"]}"#,
        r#"{"custom_name":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}"#,
    ] {
        assert_eq!(
            call(&app, &path, Some(&jwt(owner)), ws, bad).await.0,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }
    assert!(events.0.lock().unwrap().is_empty());
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(admin)),
        ws,
        r#"{"visibility":"private"}"#,
    )
    .await;
    assert_eq!(code, StatusCode::FORBIDDEN, "{result:?}");
    db.execute("UPDATE agent_runtime SET metadata=$1 WHERE id=$2", &[&json!({"auth_token":"synthetic-secret","nested":{"app_version":"test", "db_password":"synthetic-secret"}}),&rt]).await.unwrap();
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(owner)),
        ws,
        &format!(r#"{{"visibility":"private","shared_with":["{member}","{member}","{owner}"]}}"#),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{result:?}");
    assert_eq!(result["shared_with"], json!([member.to_string()]));
    assert_eq!(result["visibility"], "private");
    assert_eq!(result["metadata"], json!({"nested":{"app_version":"test"}}));
    assert_eq!(events.0.lock().unwrap().len(), 1);
    assert_eq!(events.0.lock().unwrap()[0].actor_id, owner);
    assert_eq!(
        call(
            &app,
            &path,
            Some(&jwt(owner)),
            ws,
            &format!(r#"{{"shared_with":["{outsider}"]}}"#)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            &path,
            Some(&jwt(owner)),
            ws,
            r#"{"visibility":"public","shared_with":["00000000-0000-0000-0000-000000000001"]}"#
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(events.0.lock().unwrap().len(), 1);
    // A member may rename their own provider on a shared machine, not somebody else's.
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(owner)),
        ws,
        r#"{"custom_name":"  My Machine  ","apply_to_machine":true}"#,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{result:?}");
    assert_eq!(result["custom_name"], "My Machine");
    let other_name: Option<String> = db
        .query_one(
            "SELECT custom_name FROM agent_runtime WHERE id=$1",
            &[&second],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(other_name, None);
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(admin)),
        ws,
        r#"{"custom_name":"  Shared Admin  ","apply_to_machine":true}"#,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{result:?}");
    let other_name: Option<String> = db
        .query_one(
            "SELECT custom_name FROM agent_runtime WHERE id=$1",
            &[&second],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(other_name.as_deref(), Some("Shared Admin"));
    let second_path = format!("/api/runtimes/{second}/");
    let admin_token = jwt(admin);
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        tokio::join!(
            call(
                &app,
                &path,
                Some(&admin_token),
                ws,
                r#"{"custom_name":"A","apply_to_machine":true}"#
            ),
            call(
                &app,
                &second_path,
                Some(&admin_token),
                ws,
                r#"{"custom_name":"B","apply_to_machine":true}"#
            )
        )
    })
    .await
    .expect("ordered runtime locks cannot deadlock");
    assert_eq!(a.0, StatusCode::OK, "{:?}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{:?}", b.1);
    let names: Vec<String> = db
        .query(
            "SELECT custom_name FROM agent_runtime WHERE id=ANY($1::uuid[]) ORDER BY id",
            &[&vec![rt, second]],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names.len(), 2);
    assert_eq!(names[0], names[1]);
    assert!(names[0] == "A" || names[0] == "B");
    assert_eq!(
        call(&app, &path, Some(&jwt(owner)), ws, r#"{"custom_name":" "}"#)
            .await
            .0,
        StatusCode::OK
    );
    let name: Option<String> = db
        .query_one("SELECT custom_name FROM agent_runtime WHERE id=$1", &[&rt])
        .await
        .unwrap()
        .get(0);
    assert!(name.is_none());
    // A bound teammate cannot silently lose machine access. No rename may commit either.
    let agent = Uuid::new_v4();
    db.execute("INSERT INTO agent(id,workspace_id,name,owner_id,runtime_id,runtime_mode,status) VALUES($1,$2,'Dependent',$3,$4,'local','working')",&[&agent,&ws,&member,&rt]).await.unwrap();
    let before = events.0.lock().unwrap().len();
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(owner)),
        ws,
        r#"{"visibility":"private","shared_with":[],"custom_name":"should roll back"}"#,
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{result:?}");
    assert_eq!(result["code"], "runtime_has_nonowner_dependents");
    assert_eq!(result["nonowner_agents"][0]["id"], agent.to_string());
    assert_eq!(events.0.lock().unwrap().len(), before);
    let current = db
        .query_one(
            "SELECT visibility,shared_with,custom_name FROM agent_runtime WHERE id=$1",
            &[&rt],
        )
        .await
        .unwrap();
    assert_eq!(current.get::<_, String>(0), "private");
    assert_eq!(current.get::<_, Vec<Option<Uuid>>>(1), vec![Some(member)]);
    assert_eq!(current.get::<_, Option<String>>(2), None);
    db.execute("UPDATE agent SET runtime_id=NULL WHERE id=$1", &[&agent])
        .await
        .unwrap();
    let issue = Uuid::new_v4();
    let task = Uuid::new_v4();
    db.execute("INSERT INTO issue(id,workspace_id,title,creator_type,creator_id) VALUES($1,$2,'Synthetic','member',$3)",&[&issue,&ws,&owner]).await.unwrap();
    db.execute("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status) VALUES($1,$2,$3,$4,'running')",&[&task,&agent,&rt,&issue]).await.unwrap();
    let (code, result) = call(&app, &path, Some(&jwt(owner)), ws, r#"{"shared_with":[]}"#).await;
    assert_eq!(code, StatusCode::CONFLICT, "{result:?}");
    assert_eq!(result["nonowner_agents"], json!([]));
    assert_eq!(result["active_task_ids"], json!([task.to_string()]));
    assert_eq!(events.0.lock().unwrap().len(), before);
    // A second change with a DB trigger failure must leave access unchanged.
    db.batch_execute("CREATE FUNCTION zbsi71_reject_runtime_name() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.custom_name = 'reject' THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi71_reject_runtime_name BEFORE UPDATE ON agent_runtime FOR EACH ROW EXECUTE FUNCTION zbsi71_reject_runtime_name()").await.unwrap();
    let (code, result) = call(
        &app,
        &path,
        Some(&jwt(owner)),
        ws,
        r#"{"visibility":"public","custom_name":"reject"}"#,
    )
    .await;
    assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{result:?}");
    assert_eq!(events.0.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one("SELECT visibility FROM agent_runtime WHERE id=$1", &[&rt])
            .await
            .unwrap()
            .get::<_, String>(0),
        "private"
    );
    db.batch_execute("DROP TRIGGER zbsi71_reject_runtime_name ON agent_runtime; DROP FUNCTION zbsi71_reject_runtime_name()").await.unwrap();
    db.execute("INSERT INTO cloud_agent_connection(workspace_id,provider,owner_id,token_id,config_encrypted) VALUES($1,'qoder_cloud',$2,$3,'synthetic')",&[&ws,&owner,&Uuid::new_v4()]).await.unwrap();
    let (code, result) = call(
        &app,
        &format!("/api/runtimes/{qoder}/"),
        Some(&jwt(owner)),
        ws,
        r#"{"visibility":"private"}"#,
    )
    .await;
    assert_eq!(code, StatusCode::CONFLICT, "{result:?}");
    assert_eq!(result["code"], "runtime_managed_by_connection");
    db.execute(
        "UPDATE agent_runtime SET owner_id=$1 WHERE id=$2",
        &[&other, &private],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            &format!("/api/runtimes/{private}/"),
            Some(&jwt(owner)),
            ws,
            r#"{"custom_name":"stolen"}"#
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}
