use super::*;

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn agents_create_update_restore_enforce_machine_consent_and_owner_access() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi70"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(10)
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
                &[&name, &format!("agents-{name}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Agents',$1) RETURNING id",
            &[&format!("agents-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Foreign agents',$1) RETURNING id",
            &[&format!("foreign-agents-{suffix}")],
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
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status,owner_id,visibility) VALUES ($1,'Fake local only','local','claude','online',$2,'private') RETURNING id", &[&ws,&users[2]]).await.unwrap().get(0);
    let foreign_rt:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id) VALUES ($1,'Foreign fake','local','claude',$2) RETURNING id", &[&foreign,&users[0]]).await.unwrap().get(0);
    let skill:Uuid=db.query_one("INSERT INTO skill(workspace_id,name,content) VALUES ($1,'Agent skill','fake only') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_skill:Uuid=db.query_one("INSERT INTO skill(workspace_id,name,content) VALUES ($1,'Foreign skill','fake only') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    let no_bus = router_with_api(health, ApiState::new(pool.clone(), SECRET.to_vec()));
    let path = format!("/api/agents/?workspace_id={ws}");
    // Go authorizes and validates before any event bus exists. A missing agent
    // must therefore stay 404 on the fail-closed router, and malformed input
    // must stay 400 on an existing agent, instead of leaking a bus 503.
    let missing = Uuid::new_v4();
    for (method, url, body) in [
        (
            Method::PUT,
            format!("/api/agents/{missing}?workspace_id={ws}"),
            json!({"name":"Ghost"}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{missing}/?workspace_id={ws}"),
            json!({"max_concurrent_tasks":0}),
        ),
        (
            Method::POST,
            format!("/api/agents/{missing}/restore?workspace_id={ws}"),
            Value::Null,
        ),
        (
            Method::POST,
            format!("/api/agents/{missing}/labels?workspace_id={ws}"),
            json!({"label_id":"not-a-uuid"}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{missing}/skills?workspace_id={ws}"),
            json!({"skill_ids":"not-a-list"}),
        ),
        (
            Method::PUT,
            format!(
                "/api/agents/{missing}/skills/{}/enabled?workspace_id={ws}",
                Uuid::new_v4()
            ),
            json!({"enabled":"not-a-bool"}),
        ),
        (
            Method::DELETE,
            format!(
                "/api/agents/{missing}/skills/{}?workspace_id={ws}",
                Uuid::new_v4()
            ),
            Value::Null,
        ),
        (
            Method::PUT,
            format!(
                "/api/agents/{missing}/mcp-servers/{}/enabled?workspace_id={ws}",
                Uuid::new_v4()
            ),
            json!({"enabled":"not-a-bool"}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{missing}/runtime-skills/enabled?workspace_id={ws}"),
            json!({"runtime_id":runtime,"root":"provider","key":"ghost","enabled":true}),
        ),
    ] {
        let (actual, result) = call(&no_bus, method, &url, &jwt(users[2]), body).await;
        assert_eq!(actual, StatusCode::NOT_FOUND, "{url}: {result}");
    }
    assert_eq!(
        call(&no_bus, Method::POST, &path, &jwt(users[2]), json!({}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let basic = json!({"name":"Created agent","runtime_id":runtime});
    assert_eq!(
        call(&no_bus, Method::POST, &path, &jwt(users[2]), basic.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&app, Method::POST, &path, &jwt(users[4]), basic.clone())
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    // Workspace admin/owner cannot lend another person's private machine.
    for user in [users[0], users[1], users[3]] {
        assert_eq!(
            call(&app, Method::POST, &path, &jwt(user), basic.clone())
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    for (body, status) in [
        (
            json!({"name":"Foreign rt","runtime_id":foreign_rt}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"Foreign skill","runtime_id":runtime,"skill_ids":[foreign_skill]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"Bad cap","runtime_id":runtime,"max_concurrent_tasks":0}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"Provider settings","runtime_id":runtime,"custom_args":["--unsupported"]}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (
            json!({"name":"Malformed args","runtime_id":runtime,"custom_args":""}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"Owner override","runtime_id":runtime,"owner_id":users[0]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"name":"Foreign avatar","runtime_id":runtime,"avatar_url":"/uploads/workspaces/foreign.png"}),
            StatusCode::FORBIDDEN,
        ),
        (
            json!({"name":"Malformed targets","runtime_id":runtime,"invocation_targets":{}}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let (actual, result) = call(&app, Method::POST, &path, &jwt(users[2]), body.clone()).await;
        assert_eq!(actual, status, "{body}: {result}");
    }
    assert_eq!(
        db.query_one("SELECT count(*) FROM agent WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(events.0.lock().unwrap().is_empty());
    let create_fault = format!("agents_create_fault_{suffix}");
    db.batch_execute(&format!("CREATE FUNCTION {create_fault}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.created_by='{}'::uuid THEN RAISE EXCEPTION 'synthetic access write failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {create_fault} BEFORE INSERT ON agent_invocation_target FOR EACH ROW EXECUTE FUNCTION {create_fault}();",users[2])).await.unwrap();
    let (status, out) = call(
        &app,
        Method::POST,
        &path,
        &jwt(users[2]),
        json!({"name":"Rollback create","runtime_id":runtime,"permission_mode":"public_to"}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{out}");
    assert_eq!(
        db.query_one("SELECT count(*) FROM agent WHERE workspace_id=$1", &[&ws])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert!(events.0.lock().unwrap().is_empty());
    db.batch_execute(&format!(
        "DROP TRIGGER {create_fault} ON agent_invocation_target; DROP FUNCTION {create_fault}();"
    ))
    .await
    .unwrap();
    let body = json!({"name":"Created agent","runtime_id":runtime,"description":"đ".repeat(255),"skill_ids":[skill,skill],"custom_env":{"TOKEN":"created-env-secret-marker"},"runtime_config":{"gateway":{"token":"created-gateway-secret-marker"}},"mcp_config":{"env":{"TOKEN":"created-mcp-secret-marker"}},"permission_mode":"public_to","invocation_targets":[{"target_type":"member","target_id":users[3]},{"target_type":"member","target_id":users[3]}],"conversation_starters":[{"label":" Ask ","prompt":" Test "}]});
    let (status, created) = call(&app, Method::POST, &path, &jwt(users[2]), body.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = Uuid::parse_str(created["id"].as_str().unwrap()).unwrap();
    assert_eq!(created["owner_id"], users[2].to_string());
    assert_eq!(created["max_concurrent_tasks"], 6);
    assert_eq!(created["status"], "idle");
    assert_eq!(created["visibility"], "private");
    assert_eq!(created["permission_mode"], "public_to");
    assert_eq!(created["invocation_targets"].as_array().unwrap().len(), 1);
    assert_eq!(created["skills"].as_array().unwrap().len(), 1);
    assert_eq!(
        created["conversation_starters"],
        json!([{"label":"Ask","prompt":"Test"}])
    );
    assert!(!created.to_string().contains("secret-marker"));
    assert_eq!(created["runtime_config"]["gateway"]["token"], "***");
    assert_eq!(created["custom_env_key_count"], 1);
    assert_eq!(
        call(&app, Method::POST, &path, &jwt(users[2]), body)
            .await
            .0,
        StatusCode::CONFLICT
    );
    // Existing agent on the fail-closed router: body validation still reports
    // 400, and only a fully valid mutation reaches the missing-bus 503.
    for (method, url, invalid) in [
        (
            Method::PUT,
            format!("/api/agents/{id}/?workspace_id={ws}"),
            json!({"max_concurrent_tasks":0}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{id}/skills?workspace_id={ws}"),
            json!({"skill_ids":"not-a-list"}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{id}/runtime-skills/enabled?workspace_id={ws}"),
            json!({"runtime_id":runtime,"root":"provider","key":"x"}),
        ),
    ] {
        assert_eq!(
            call(&no_bus, method, &url, &jwt(users[2]), invalid).await.0,
            StatusCode::BAD_REQUEST,
            "{url}"
        );
    }
    for (method, url, valid) in [
        (
            Method::PUT,
            format!("/api/agents/{id}/?workspace_id={ws}"),
            json!({"name":"Bus required"}),
        ),
        (
            Method::PUT,
            format!("/api/agents/{id}/skills?workspace_id={ws}"),
            json!({"skill_ids":[]}),
        ),
    ] {
        assert_eq!(
            call(&no_bus, method, &url, &jwt(users[2]), valid).await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{url}"
        );
    }
    let update = format!("/api/agents/{id}/?workspace_id={ws}");
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &update,
            &jwt(users[3]),
            json!({"name":"Hijack"})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &format!("/api/agents/{id}/?workspace_id={foreign}"),
            &jwt(users[0]),
            json!({"name":"Hijack"})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for user in [users[0], users[1]] {
        assert_eq!(
            call(
                &app,
                Method::PUT,
                &update,
                &jwt(user),
                json!({"permission_mode":"public_to"})
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        let (status, out) = call(
            &app,
            Method::PUT,
            &update,
            &jwt(user),
            json!({"description":"Admin metadata","visibility":"private"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{out}");
        assert_eq!(out["permission_mode"], "public_to");
        assert_eq!(
            out["invocation_targets"][0]["target_id"],
            users[3].to_string()
        );
    }
    let (status,out)=call(&app,Method::PUT,&update,&jwt(users[1]),json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"member","target_id":users[3]}]})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status,out)=call(&app,Method::PUT,&update,&jwt(users[2]),json!({"runtime_config":{"gateway":{"token":"***"}},"name":"Renamed","max_concurrent_tasks":50})).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["name"], "Renamed");
    assert_eq!(out["max_concurrent_tasks"], 50);
    assert_eq!(
        db.query_one(
            "SELECT runtime_config #>> '{gateway,token}' FROM agent WHERE id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "created-gateway-secret-marker"
    );
    for (request, status) in [
        (json!({"custom_env":null}), StatusCode::BAD_REQUEST),
        (
            json!({"runtime_id":foreign_rt}),
            StatusCode::NOT_IMPLEMENTED,
        ),
        (json!({"thinking_level":""}), StatusCode::NOT_IMPLEMENTED),
        (json!({"service_tier":""}), StatusCode::NOT_IMPLEMENTED),
        (
            json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"member","target_id":users[4]}]}),
            StatusCode::BAD_REQUEST,
        ),
        (json!({"permission_mode":"bad"}), StatusCode::BAD_REQUEST),
        (json!({"permission_mode":true}), StatusCode::BAD_REQUEST),
        (
            json!({"invocation_targets":[{"target_type":"member","target_id":users[3]}]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"description":"đ".repeat(256)}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let before = events.0.lock().unwrap().len();
        let (actual, result) =
            call(&app, Method::PUT, &update, &jwt(users[2]), request.clone()).await;
        assert_eq!(actual, status, "{request}: {result}");
        assert_eq!(events.0.lock().unwrap().len(), before);
    }
    // Fault after permission replacement: data and fanout must roll back together.
    let function = format!("agents_fault_{suffix}");
    db.batch_execute(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.workspace_id='{ws}'::uuid AND NEW.name='Fail metadata' THEN RAISE EXCEPTION 'synthetic write failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER {function} BEFORE UPDATE ON agent FOR EACH ROW EXECUTE FUNCTION {function}();")).await.unwrap();
    let before = events.0.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &update,
            &jwt(users[2]),
            json!({"name":"Fail metadata","permission_mode":"private"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.0.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one("SELECT permission_mode FROM agent WHERE id=$1", &[&id])
            .await
            .unwrap()
            .get::<_, String>(0),
        "public_to"
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_invocation_target WHERE agent_id=$1",
            &[&id]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {function} ON agent; DROP FUNCTION {function}();"
    ))
    .await
    .unwrap();
    // Concurrent replacements serialize on the agent row; no mixed target sets.
    let auth = jwt(users[2]);
    let req1 = json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"member","target_id":users[0]},{"target_type":"member","target_id":users[1]}]});
    let req2 = json!({"permission_mode":"public_to","invocation_targets":[{"target_type":"workspace","target_id":foreign}]});
    let (a, b) = tokio::join!(
        call(&app, Method::PUT, &update, &auth, req1),
        call(&app, Method::PUT, &update, &auth, req2)
    );
    assert_eq!(a.0, StatusCode::OK, "{}", a.1);
    assert_eq!(b.0, StatusCode::OK, "{}", b.1);
    let persisted=db.query("SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY target_type,target_id", &[&id]).await.unwrap();
    assert!(
        persisted.len() == 2
            || (persisted.len() == 1 && persisted[0].get::<_, Uuid>("target_id") == ws)
    );
    // Restore is config-only and must not resurrect terminal tasks.
    let restore = format!("/api/agents/{id}/restore?workspace_id={ws}");
    assert_eq!(
        call(&app, Method::POST, &restore, &auth, Value::Null)
            .await
            .0,
        StatusCode::CONFLICT
    );
    db.execute(
        "UPDATE agent SET archived_at=now(),archived_by=$2 WHERE id=$1",
        &[&id, &users[2]],
    )
    .await
    .unwrap();
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'cancelled') RETURNING id", &[&id,&runtime]).await.unwrap().get(0);
    let token = format!("mat_agents_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&task,&id,&ws,&users[2]]).await.unwrap();
    // Use a separate active task so auth accepts the token before the human gate.
    db.execute(
        "UPDATE agent_task_queue SET status='running' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    for (method, url, body) in [
        (Method::POST, &path, basic),
        (Method::PUT, &update, json!({"name":"No task privilege"})),
        (Method::POST, &restore, Value::Null),
    ] {
        assert_eq!(
            call(&app, method, url, &token, body).await.0,
            StatusCode::FORBIDDEN
        );
    }
    db.execute(
        "UPDATE agent_task_queue SET status='cancelled' WHERE id=$1",
        &[&task],
    )
    .await
    .unwrap();
    assert_eq!(
        call(&no_bus, Method::POST, &restore, &auth, Value::Null)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(&app, Method::POST, &restore, &jwt(users[3]), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let (status, out) = call(&app, Method::POST, &restore, &jwt(users[1]), Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["archived_at"].is_null());
    assert!(out["archived_by"].is_null());
    assert_eq!(
        db.query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
            .await
            .unwrap()
            .get::<_, String>(0),
        "cancelled"
    );
    assert!(
        !events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.payload.to_string().contains("secret-marker"))
    );
    assert_eq!(
        events.0.lock().unwrap().last().unwrap().event_type,
        "agent:restored"
    );
    // Selected-member consent works; public consent never repairs ownerless runtime.
    db.execute(
        "UPDATE agent_runtime SET shared_with=$2 WHERE id=$1",
        &[&runtime, &vec![users[3]]],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            Method::POST,
            &path,
            &jwt(users[3]),
            json!({"name":"Shared machine","runtime_id":runtime})
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let mut blocker = pool.get().await.unwrap();
    let revoke = blocker.transaction().await.unwrap();
    revoke
        .execute(
            "UPDATE agent_runtime SET shared_with='{}' WHERE id=$1",
            &[&runtime],
        )
        .await
        .unwrap();
    let auth = jwt(users[3]);
    let raced = call(
        &app,
        Method::POST,
        &path,
        &auth,
        json!({"name":"Revoked during create","runtime_id":runtime}),
    );
    let release = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        revoke.commit().await.unwrap();
    };
    let (result, ()) = tokio::join!(raced, release);
    assert_eq!(result.0, StatusCode::FORBIDDEN, "{}", result.1);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent WHERE workspace_id=$1 AND name='Revoked during create'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    db.execute(
        "UPDATE agent_runtime SET owner_id=NULL,visibility='public' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    assert_eq!(
        call(
            &app,
            Method::POST,
            &path,
            &jwt(users[0]),
            json!({"name":"Ownerless machine","runtime_id":runtime})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn mika_provisions_once_and_reuses_each_members_onboarding_session() {
    let config = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi70"));
    assert!(
        config.get_hosts().iter().all(
            |host| matches!(host,tokio_postgres::config::Host::Tcp(host) if host=="127.0.0.1")
        )
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(Manager::new(config, NoTls))
            .max_size(10)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for label in ["first", "second", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("mika-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Mika test',$1) RETURNING id",
            &[&format!("mika-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for user in &users[..2] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'member')",
            &[&ws, user],
        )
        .await
        .unwrap();
    }
    let rt:Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,visibility,status) VALUES ($1,'Fake machine','local','claude',$2,'public','offline') RETURNING id", &[&ws,&users[0]]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    let no_bus = router_with_api(health, ApiState::new(pool.clone(), SECRET.to_vec()));
    let path = format!("/api/agents/mika?workspace_id={ws}");
    let body = json!({"runtime_id":rt,"language":"en","session_title":"First kickoff"});
    let (status, invalid) = call(
        &app,
        Method::POST,
        &path,
        &jwt(users[0]),
        json!({"runtime_id":rt,"language":"fr"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"], "language must be en, zh, ko, or ja");
    assert_eq!(
        call(&no_bus, Method::POST, &path, &jwt(users[0]), body.clone())
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent WHERE workspace_id=$1 AND system_key='mika'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );
    let first_auth = jwt(users[0]);
    let second_auth = jwt(users[1]);
    let (first, second) = tokio::join!(
        call(&app, Method::POST, &path, &first_auth, body.clone()),
        call(&app, Method::POST, &path, &second_auth, body.clone())
    );
    for (status, response) in [&first, &second] {
        assert!(
            matches!(*status, StatusCode::CREATED | StatusCode::OK),
            "{response}"
        );
        assert_eq!(response["system_key"], "mika");
        assert_eq!(response["name"], "Mika");
        assert_eq!(response["visibility"], "workspace");
        assert_eq!(response["permission_mode"], "public_to");
        assert_eq!(response["max_concurrent_tasks"], 3);
        assert_eq!(response["onboarding_session"]["agent_id"], response["id"]);
    }
    assert_ne!(first.0, second.0);
    assert_eq!(first.1["id"], second.1["id"]);
    assert_ne!(
        first.1["onboarding_session"]["id"],
        second.1["onboarding_session"]["id"]
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent WHERE workspace_id=$1 AND system_key='mika'",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    assert_eq!(db.query_one("SELECT count(*) FROM agent_invocation_target WHERE agent_id=$1 AND target_type='workspace' AND target_id=$2", &[&Uuid::parse_str(first.1["id"].as_str().unwrap()).unwrap(),&ws]).await.unwrap().get::<_,i64>(0),1);
    for (user, response) in [(users[0], &first.1), (users[1], &second.1)] {
        let (status, again) = call(
            &no_bus,
            Method::POST,
            &path,
            &jwt(user),
            json!({"runtime_id":rt,"language":"ja","session_title":"Changed title"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{again}");
        assert_eq!(
            again["onboarding_session"]["id"],
            response["onboarding_session"]["id"]
        );
        assert_eq!(again["onboarding_session"]["title"], "First kickoff");
    }
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM chat_session WHERE workspace_id=$1",
            &[&ws]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}
