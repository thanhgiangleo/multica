use super::*;

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn bindings_enforce_tenants_roles_atomicity_toggles_and_redaction() {
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
    for label in ["owner", "admin", "agent-owner", "member", "outsider"] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("binding-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let mut workspaces = Vec::new();
    for label in ["bindings", "foreign-bindings"] {
        workspaces.push(
            db.query_one(
                "INSERT INTO workspace(name,slug) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("{label}-{suffix}")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws = workspaces[0];
    let foreign = workspaces[1];
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
    let runtime:Uuid=db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'Bindings synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent:Uuid=db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'Binding target','cloud',$2,$3) RETURNING id", &[&ws,&users[2],&runtime]).await.unwrap().get(0);
    let mut skills = Vec::new();
    for (workspace, name) in [(ws, "A"), (ws, "B"), (foreign, "Foreign")] {
        skills.push(db.query_one("INSERT INTO skill(workspace_id,name,content) VALUES ($1,$2,'not returned in summary') RETURNING id", &[&workspace,&name]).await.unwrap().get::<_,Uuid>(0));
    }
    let config = json!({"command":"fake-only","env":{"SECRET":"synthetic-hidden-marker"},"headers":{"Authorization":"synthetic-hidden-marker"}});
    let server:Uuid=db.query_one("INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'MCP',$2) RETURNING id", &[&ws,&config]).await.unwrap().get(0);
    let foreign_server:Uuid=db.query_one("INSERT INTO workspace_mcp_server(workspace_id,name,config) VALUES ($1,'Foreign MCP',$2) RETURNING id", &[&foreign,&config]).await.unwrap().get(0);
    let events = Arc::new(Events::default());
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()).with_squad_event_sink(events.clone()),
    );
    let no_bus = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let path = format!("/api/agents/{agent}/skills?workspace_id={ws}");
    let add = format!("/api/agents/{agent}/skills/add?workspace_id={ws}");
    let skill_enabled = format!(
        "/api/agents/{agent}/skills/{}/enabled?workspace_id={ws}",
        skills[0]
    );
    let mcp = format!("/api/agents/{agent}/mcp-servers?workspace_id={ws}");
    let mcp_enabled = format!("/api/agents/{agent}/mcp-servers/{server}/enabled?workspace_id={ws}");
    let mcp_remove = format!("/api/agents/{agent}/mcp-servers/{server}?workspace_id={ws}");

    for (user, expected) in [
        (users[3], StatusCode::FORBIDDEN),
        (users[4], StatusCode::NOT_FOUND),
    ] {
        assert_eq!(
            call(
                &app,
                Method::PUT,
                &path,
                &jwt(user),
                json!({"skill_ids":[skills[0]]})
            )
            .await
            .0,
            expected
        );
        assert_eq!(
            call(
                &app,
                Method::POST,
                &mcp,
                &jwt(user),
                json!({"server_id":server})
            )
            .await
            .0,
            expected
        );
    }
    assert_eq!(
        call(
            &no_bus,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":[skills[0]]})
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &format!("/api/agents/{agent}/skills?workspace_id={foreign}"),
            &jwt(users[0]),
            json!({"skill_ids":[skills[2]]})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":["not-uuid"]})
        )
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
            json!({"skill_ids":[skills[2]]})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &mcp,
            &jwt(users[2]),
            json!({"server_id":foreign_server})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, Method::PUT, &skill_enabled, &jwt(users[2]), json!({}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &skill_enabled,
            &jwt(users[2]),
            json!({"enabled":true})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &mcp_enabled,
            &jwt(users[2]),
            json!({"enabled":false})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &mcp_remove,
            &jwt(users[2]),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(events.0.lock().unwrap().is_empty());

    let (status, result) = call(
        &app,
        Method::PUT,
        &path,
        &jwt(users[2]),
        json!({"skill_ids":[skills[0],skills[0]]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result.as_array().unwrap().len(), 1);
    assert_eq!(result[0]["enabled"], true);
    assert!(result[0].get("content").is_none());
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &skill_enabled,
            &jwt(users[0]),
            json!({"enabled":false})
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, result) = call(
        &app,
        Method::POST,
        &add,
        &jwt(users[1]),
        json!({"skill_ids":[skills[0],skills[1]]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result.as_array().unwrap().len(), 2);
    assert_eq!(
        result[0]["enabled"], false,
        "add preserves disabled duplicate"
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":[skills[2]]})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_skill WHERE agent_id=$1",
            &[&agent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2,
        "invalid replacement must not erase prior bindings"
    );

    // Inject a real DB failure after DELETE and before INSERT. Transaction must
    // restore the old set and emit no status event for the failed replacement.
    db.batch_execute("CREATE FUNCTION zbsi70_bindings_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('application_name') <> 'never-used' THEN RAISE EXCEPTION 'synthetic insert failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER zbsi70_bindings_reject BEFORE INSERT ON agent_skill FOR EACH ROW EXECUTE FUNCTION zbsi70_bindings_reject()").await.unwrap();
    let before = events.0.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":[skills[1]]})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events.0.lock().unwrap().len(), before);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_skill WHERE agent_id=$1",
            &[&agent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        2
    );
    db.batch_execute("DROP TRIGGER zbsi70_bindings_reject ON agent_skill; DROP FUNCTION zbsi70_bindings_reject()").await.unwrap();

    // Competing full replacements serialize on the agent, never union the sets.
    let owner_auth = jwt(users[2]);
    let admin_auth = jwt(users[1]);
    let (a, b) = tokio::join!(
        call(
            &app,
            Method::PUT,
            &path,
            &owner_auth,
            json!({"skill_ids":[skills[0]]})
        ),
        call(
            &app,
            Method::PUT,
            &path,
            &admin_auth,
            json!({"skill_ids":[skills[1]]})
        )
    );
    assert_eq!(a.0, StatusCode::OK);
    assert_eq!(b.0, StatusCode::OK);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_skill WHERE agent_id=$1",
            &[&agent]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        1
    );
    let remove_skill = format!("/api/agents/{agent}/skills/{}?workspace_id={ws}", skills[0]);
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &remove_skill,
            &jwt(users[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":[]})
        )
        .await
        .1,
        json!([])
    );

    // MCP config remains write-only. Duplicate adds retain disabled toggles.
    let (status, result) = call(
        &app,
        Method::POST,
        &mcp,
        &jwt(users[2]),
        json!({"server_id":server}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result[0]["transport"], "stdio");
    assert!(result[0].get("config").is_none());
    assert!(!result.to_string().contains("synthetic-hidden-marker"));
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &mcp_enabled,
            &jwt(users[1]),
            json!({"enabled":false})
        )
        .await
        .1[0]["enabled"],
        false
    );
    assert_eq!(
        call(
            &app,
            Method::POST,
            &mcp,
            &jwt(users[0]),
            json!({"server_id":server})
        )
        .await
        .1[0]["enabled"],
        false
    );
    assert_eq!(
        call(
            &app,
            Method::DELETE,
            &mcp_remove,
            &jwt(users[2]),
            Value::Null
        )
        .await
        .1,
        json!([])
    );

    // Simulate a catalog deleter following the lock-before-sweep contract.
    // An attachment waiting on that row must recheck existence, not orphan it.
    let mut deleting = pool.get().await.unwrap();
    let tx = deleting.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM workspace_mcp_server WHERE id=$1 FOR UPDATE",
        &[&server],
    )
    .await
    .unwrap();
    let race_app = app.clone();
    let race_path = mcp.clone();
    let race_auth = jwt(users[2]);
    let mut pending = tokio::spawn(async move {
        call(
            &race_app,
            Method::POST,
            &race_path,
            &race_auth,
            json!({"server_id":server}),
        )
        .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    tx.execute(
        "DELETE FROM agent_mcp_server WHERE server_id=$1",
        &[&server],
    )
    .await
    .unwrap();
    tx.execute("DELETE FROM workspace_mcp_server WHERE id=$1", &[&server])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(pending.await.unwrap().0, StatusCode::NOT_FOUND);
    assert_eq!(
        db.query_one(
            "SELECT count(*) FROM agent_mcp_server WHERE server_id=$1",
            &[&server]
        )
        .await
        .unwrap()
        .get::<_, i64>(0),
        0
    );

    let label:Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Agent label','#123456','agent') RETURNING id", &[&ws]).await.unwrap().get(0);
    let wrong_label:Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Issue label','#123456','issue') RETURNING id", &[&ws]).await.unwrap().get(0);
    let foreign_label:Uuid = db.query_one("INSERT INTO issue_label(workspace_id,name,color,resource_type) VALUES ($1,'Foreign label','#123456','agent') RETURNING id", &[&foreign]).await.unwrap().get(0);
    let label_path = format!("/api/agents/{agent}/labels?workspace_id={ws}");
    let label_remove = format!("/api/agents/{agent}/labels/{label}?workspace_id={ws}");
    for target in [wrong_label, foreign_label] {
        assert_eq!(
            call(
                &app,
                Method::POST,
                &label_path,
                &jwt(users[2]),
                json!({"label_id":target})
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            Method::POST,
            &label_path,
            &jwt(users[3]),
            json!({"label_id":label})
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    for user in [users[2], users[1]] {
        let (status, result) = call(
            &app,
            Method::POST,
            &label_path,
            &jwt(user),
            json!({"label_id":label}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{result}");
        assert_eq!(result["labels"].as_array().unwrap().len(), 1);
        assert_eq!(result["labels"][0]["id"], label.to_string());
        assert_eq!(result["labels"][0]["usage_count"], 0);
    }
    for _ in 0..2 {
        assert_eq!(
            call(
                &app,
                Method::DELETE,
                &label_remove,
                &jwt(users[0]),
                Value::Null
            )
            .await
            .1,
            json!({"labels":[]})
        );
    }
    // Null and absent slices follow Go's nil-slice decoding, clearing the set.
    assert_eq!(
        call(
            &app,
            Method::PUT,
            &path,
            &jwt(users[2]),
            json!({"skill_ids":null})
        )
        .await
        .1,
        json!([])
    );
    assert_eq!(
        call(&app, Method::PUT, &path, &jwt(users[2]), json!({}))
            .await
            .1,
        json!([])
    );

    // The human owner on a task token must not lend management authority to an
    // agent principal, even when that owner is a workspace administrator.
    let task:Uuid=db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&agent,&runtime]).await.unwrap().get(0);
    let token = format!("mat_bindings_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&task,&agent,&ws,&users[0]]).await.unwrap();
    for (method, url, body) in [
        (Method::PUT, path.clone(), json!({"skill_ids":[skills[0]]})),
        (Method::POST, add.clone(), json!({"skill_ids":[skills[0]]})),
        (Method::PUT, skill_enabled.clone(), json!({"enabled":true})),
        (Method::DELETE, remove_skill.clone(), Value::Null),
        (Method::POST, mcp.clone(), json!({"server_id":server})),
        (Method::PUT, mcp_enabled.clone(), json!({"enabled":true})),
        (Method::DELETE, mcp_remove.clone(), Value::Null),
        (Method::POST, label_path.clone(), json!({"label_id":label})),
        (Method::DELETE, label_remove.clone(), Value::Null),
    ] {
        assert_eq!(
            call(&app, method, &url, &token, body).await.0,
            StatusCode::FORBIDDEN,
            "{url}"
        );
    }
    let recorded = events.0.lock().unwrap();
    assert!(
        recorded
            .iter()
            .any(|e| e.event_type == "label:updated"
                && e.payload["label"]["id"] == label.to_string())
    );
    assert!(recorded.iter().all(|e| e.workspace_id == ws
        && match e.event_type {
            "agent:status" => e.payload["agent_id"] == agent.to_string(),
            "label:updated" =>
                e.payload["label"]["id"] == label.to_string()
                    || e.payload["label_id"] == label.to_string(),
            _ => false,
        }
        && !e.payload.to_string().contains("synthetic-hidden-marker")));
}
