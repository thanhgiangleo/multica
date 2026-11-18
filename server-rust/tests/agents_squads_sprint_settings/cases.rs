use super::*;

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn settings_authorize_audit_rollback_redact_and_scope_runtime_overrides() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi70"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for name in ["owner", "admin", "agent-owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&name, &format!("settings-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Settings',$1) RETURNING id",
            &[&format!("settings-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign settings',$1) RETURNING id",
            &[&format!("foreign-settings-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [
        (users[0], "owner"),
        (users[1], "admin"),
        (users[2], "member"),
        (users[3], "member"),
    ] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,$3)",
            &[&ws, &user, &role],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
        &[&foreign, &users[0]],
    )
    .await
    .unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Fake only','local','claude','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,custom_env,mcp_config,runtime_config) VALUES ($1,'Settings target','local',$2,$3,$4,$5,$6) RETURNING id", &[&ws,&users[2],&runtime,&json!({"KEEP":"env-secret-marker","REMOVE":"old"}),&json!({"env":{"KEY":"mcp-secret-marker"}}),&json!({"gateway":{"token":"gateway-secret-marker"}})]).await.unwrap().get(0);
    let skill: Uuid = db.query_one("INSERT INTO skill(workspace_id,name,content) VALUES ($1,'Keep assigned','fake') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&agent, &skill],
    )
    .await
    .unwrap();
    let events = Arc::new(Events::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    // Production registers only the P0 agent-event bus; env writes must work
    // without the squad CRUD sink and still fan out to workspace members.
    let p0_only = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_local_p0_agent_events(),
    );
    let no_bus = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = format!("/api/agents/{agent}/env?workspace_id={ws}");
    let toggle = format!("/api/agents/{agent}/runtime-skills/enabled?workspace_id={ws}");
    for (user, status) in [
        (users[3], StatusCode::FORBIDDEN),
        (users[4], StatusCode::NOT_FOUND),
    ] {
        for method in [Method::GET, Method::PUT] {
            assert_eq!(
                call(&app, method, &path, &jwt(user), json!({})).await.0,
                status
            );
        }
    }
    assert_eq!(
        call(
            &app,
            Method::GET,
            &format!("/api/agents/{agent}/env?workspace_id={foreign}"),
            &jwt(users[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for user in [users[0], users[1], users[2]] {
        let (status, body) = call(&no_bus, Method::GET, &path, &jwt(user), Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["custom_env"]["KEEP"], "env-secret-marker");
    }
    assert_eq!(
        call(&no_bus, Method::PUT, &path, &jwt(users[2]), json!({}))
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let (status, body) = call(
        &app,
        Method::PUT,
        &path,
        &jwt(users[2]),
        json!({"custom_env":{"KEEP":"****","NEW_MASK":"****","ADDED":"new-secret-marker"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["custom_env"],
        json!({"KEEP":"env-secret-marker","ADDED":"new-secret-marker"})
    );
    let audit: Value = db.query_one("SELECT details FROM activity_log WHERE workspace_id=$1 AND action='agent_env_updated' ORDER BY created_at DESC LIMIT 1", &[&ws]).await.unwrap().get(0);
    assert_eq!(audit["preserved_keys"], json!(["KEEP"]));
    assert_eq!(audit["added_keys"], json!(["ADDED"]));
    assert_eq!(audit["removed_keys"], json!(["REMOVE"]));
    assert!(!audit.to_string().contains("secret-marker"));
    let payload = events.0.lock().unwrap().last().unwrap().payload.clone();
    assert!(!payload.to_string().contains("secret-marker"));
    assert_eq!(payload["agent"]["custom_env_key_count"], 2);
    assert_eq!(payload["agent"]["skills"][0]["id"], skill.to_string());
    assert_eq!(
        payload["agent"]["runtime_config"]["gateway"]["token"],
        "***"
    );
    assert!(payload["agent"].get("custom_env").is_none());
    // A transaction-scoped trigger faults only this synthetic workspace's audit
    // rows. Prove that neither plaintext nor a partial mutation escapes.
    let function = format!("settings_audit_{suffix}");
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.workspace_id = '{ws}'::uuid THEN RAISE EXCEPTION 'synthetic audit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} BEFORE INSERT ON activity_log FOR EACH ROW EXECUTE FUNCTION {function}();")).await.unwrap();
    let count = events.0.lock().unwrap().len();
    for (method, body) in [
        (Method::GET, Value::Null),
        (Method::PUT, json!({"custom_env":{"REPLACED":"no-write"}})),
    ] {
        let (status, body) = call(&app, method, &path, &jwt(users[2]), body).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert!(!body.to_string().contains("secret-marker"));
    }
    assert_eq!(events.0.lock().unwrap().len(), count);
    assert_eq!(
        db.query_one("SELECT custom_env FROM agent WHERE id=$1", &[&agent])
            .await
            .unwrap()
            .get::<_, Value>(0),
        json!({"KEEP":"env-secret-marker","ADDED":"new-secret-marker"})
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON activity_log; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();

    // The production P0 wiring supplies only the agent-event bus; the same
    // masked write must succeed there without the squad CRUD sink.
    let (status, body) = call(
        &p0_only,
        Method::PUT,
        &path,
        &jwt(users[2]),
        json!({"custom_env":{"KEEP":"****"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["custom_env"], json!({"KEEP":"env-secret-marker"}));

    let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_settings_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&task,&agent,&ws,&users[0]]).await.unwrap();
    for method in [Method::GET, Method::PUT] {
        assert_eq!(
            call(&app, method, &path, &token, json!({})).await.0,
            StatusCode::FORBIDDEN
        );
    }
    let request = json!({"runtime_id":runtime,"root":"provider","key":"a/../b//skill","name":" First ","enabled":false});
    assert_eq!(
        call(&app, Method::PUT, &toggle, &token, request.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[3]), request.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[2]), request.clone())
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let mut repeated = request.clone();
    repeated["name"] = json!("Changed name");
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[1]), repeated)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    let overrides: Value = db
        .query_one(
            "SELECT disabled_runtime_skills FROM agent WHERE id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(overrides.as_array().unwrap().len(), 1);
    assert_eq!(overrides[0]["key"], "b/skill");
    assert_eq!(overrides[0]["name"], "Changed name");
    let mut enable = request.clone();
    enable["enabled"] = json!(true);
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[0]), enable)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        db.query_one(
            "SELECT disabled_runtime_skills FROM agent WHERE id=$1",
            &[&agent]
        )
        .await
        .unwrap()
        .get::<_, Value>(0),
        json!([])
    );
    let mut first = request.clone();
    first["key"] = json!("concurrent-a");
    let mut second = request.clone();
    second["key"] = json!("concurrent-b");
    let owner_token = jwt(users[2]);
    let admin_token = jwt(users[1]);
    let (a, b) = tokio::join!(
        call(&app, Method::PUT, &toggle, &owner_token, first),
        call(&app, Method::PUT, &toggle, &admin_token, second),
    );
    assert_eq!((a.0, b.0), (StatusCode::NO_CONTENT, StatusCode::NO_CONTENT));
    let concurrent: Value = db
        .query_one(
            "SELECT disabled_runtime_skills FROM agent WHERE id=$1",
            &[&agent],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(concurrent.as_array().unwrap().len(), 2);
    let keys: Vec<_> = concurrent
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["key"].as_str().unwrap())
        .collect();
    assert!(keys.contains(&"concurrent-a") && keys.contains(&"concurrent-b"));
    for key in ["../escape", "/absolute", "."] {
        let mut bad = request.clone();
        bad["key"] = json!(key);
        assert_eq!(
            call(&app, Method::PUT, &toggle, &jwt(users[2]), bad)
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let mut stale = request.clone();
    stale["runtime_id"] = json!(Uuid::new_v4());
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[2]), stale)
            .await
            .0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE agent_runtime SET provider='codex' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    let mut plugin = request.clone();
    plugin["root"] = json!("plugin");
    plugin["plugin"] = json!("fake-plugin");
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[2]), plugin)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    db.execute(
        "UPDATE agent_runtime SET provider='qoder_cloud' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"custom_env":{"KEY":"forbidden"}})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(&app, Method::PUT, &toggle, &jwt(users[2]), request)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"custom_env":null})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        db.query_one("SELECT custom_env FROM agent WHERE id=$1", &[&agent])
            .await
            .unwrap()
            .get::<_, Value>(0),
        json!({})
    );
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .all(|event| !event.payload.to_string().contains("secret-marker"))
    );
}
