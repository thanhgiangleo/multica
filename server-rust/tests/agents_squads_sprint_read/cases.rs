use super::*;

#[tokio::test]
#[ignore = "requires owned disposable multica_zbsi70 PostgreSQL database"]
async fn agent_inventory_keeps_view_permissions_secrets_and_runtime_projection_separate() {
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
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let mut users = Vec::new();
    for label in [
        "owner",
        "admin",
        "agent-owner",
        "invited",
        "uninvited",
        "outsider",
    ] {
        users.push(
            db.query_one(
                "INSERT INTO \"user\"(name,email) VALUES ($1,$2) RETURNING id",
                &[&label, &format!("read-{label}-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get::<_, Uuid>(0),
        );
    }
    let ws: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Read agents',$1) RETURNING id",
            &[&format!("read-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let other: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES ('Elsewhere',$1) RETURNING id",
            &[&format!("read-elsewhere-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (user, role) in [
        (users[0], "owner"),
        (users[1], "admin"),
        (users[2], "member"),
        (users[3], "member"),
        (users[4], "member"),
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
        &[&other, &users[0]],
    )
    .await
    .unwrap();
    let runtime:Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,visibility,status,last_seen_at) VALUES ($1,'private fake','local','claude',$2,'private','offline',now()) RETURNING id", &[&ws,&users[2]]).await.unwrap().get(0);
    let private:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,custom_env,mcp_config,runtime_config,composio_toolkit_allowlist,permission_mode) VALUES ($1,'Secret agent','local',$2,$3,$4,$5,$6,$7,'private') RETURNING id", &[&ws,&users[2],&runtime,&json!({"API_KEY":"never disclose"}),&json!({"servers":{"secret":"synthetic-mcp-token"}}),&json!({"gateway":{"token":"synthetic-gateway-token"}}),&vec!["synthetic-personal-app"]]).await.unwrap().get(0);
    let shared:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,permission_mode) VALUES ($1,'Selected member','local',$2,$3,'public_to') RETURNING id", &[&ws,&users[2],&runtime]).await.unwrap().get(0);
    let team:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode) VALUES ($1,'Inert team','local',$2,'public_to') RETURNING id", &[&ws,&users[2]]).await.unwrap().get(0);
    let archived:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,permission_mode,visibility,archived_at) VALUES ($1,'Archived public','local',$2,$3,'public_to','workspace',now()) RETURNING id", &[&ws,&users[2],&runtime]).await.unwrap().get(0);
    let foreign:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES ($1,'Other workspace','local',$2) RETURNING id", &[&other,&users[0]]).await.unwrap().get(0);
    let system:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,kind) VALUES ($1,'Hidden carrier','local',$2,'system') RETURNING id", &[&ws,&users[2]]).await.unwrap().get(0);
    for (agent, kind, target) in [
        (shared, "member", Some(users[3])),
        (team, "team", Some(users[3])),
        (archived, "workspace", Some(ws)),
    ] {
        db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id,created_by) VALUES ($1,$2,$3,$4)", &[&agent,&kind,&target,&users[2]]).await.unwrap();
    }
    let skill:Uuid = db.query_one("INSERT INTO skill(workspace_id,name,description,content) VALUES ($1,'Bound skill','small','private SKILL.md text') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&shared, &skill],
    )
    .await
    .unwrap();
    let foreign_skill:Uuid = db.query_one("INSERT INTO skill(workspace_id,name,description,content) VALUES ($1,'Foreign skill','foreign metadata','foreign private text') RETURNING id", &[&other]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO agent_skill(agent_id,skill_id) VALUES ($1,$2)",
        &[&shared, &foreign_skill],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE agent SET mcp_config=$2 WHERE id=$1",
        &[&shared, &json!({"secret":"synthetic-shared-mcp"})],
    )
    .await
    .unwrap();
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), SECRET.to_vec()),
    );
    let list = format!("/api/agents/?workspace_id={ws}");
    let detail = |id| format!("/api/agents/{id}/?workspace_id={ws}");
    for user in [users[3], users[4]] {
        let (status, items) = call(&app, Method::GET, &list, &jwt(user), Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{items}");
        assert_eq!(
            items.as_array().unwrap().len(),
            if user == users[3] { 1 } else { 0 }
        );
    }
    let (status, item) = call(
        &app,
        Method::GET,
        &detail(shared),
        &jwt(users[3]),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{item}");
    assert_eq!(item["invocation_targets"][0]["target_id"], json!(users[3]));
    assert_eq!(item["visibility"], "private");
    assert_eq!(item["skills"].as_array().unwrap().len(), 1);
    assert_eq!(item["skills"][0]["id"], json!(skill));
    assert!(item["mcp_config"].is_null());
    assert_eq!(item["mcp_config_redacted"], true);
    assert!(item["skills"][0].get("content").is_none());
    assert_eq!(item["runtime_availability"], "unstable");
    for id in [private, team] {
        assert_eq!(
            call(&app, Method::GET, &detail(id), &jwt(users[3]), Value::Null)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    for id in [foreign, system] {
        assert_eq!(
            call(&app, Method::GET, &detail(id), &jwt(users[0]), Value::Null)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        call(
            &app,
            Method::GET,
            &detail(private),
            &jwt(users[5]),
            Value::Null
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &app,
            Method::GET,
            &format!("/api/agents/not-a-uuid/?workspace_id={ws}"),
            &jwt(users[0]),
            Value::Null
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let (_, admin) = call(
        &app,
        Method::GET,
        &detail(private),
        &jwt(users[1]),
        Value::Null,
    )
    .await;
    assert_eq!(
        admin["mcp_config"]["servers"]["secret"],
        "synthetic-mcp-token"
    );
    assert_eq!(admin["mcp_config_redacted"], false);
    assert_eq!(admin["runtime_config"]["gateway"]["token"], "***");
    assert_eq!(admin["has_custom_env"], true);
    assert_eq!(admin["custom_env_key_count"], 1);
    assert!(admin.get("custom_env").is_none());
    assert!(admin.get("runtime_availability").is_none());
    assert!(admin.get("composio_toolkit_allowlist").is_none());
    let (_, owned) = call(
        &app,
        Method::GET,
        &detail(private),
        &jwt(users[2]),
        Value::Null,
    )
    .await;
    assert_eq!(owned["runtime_bound"], true);
    assert_eq!(
        owned["mcp_config"]["servers"]["secret"],
        "synthetic-mcp-token"
    );
    assert_eq!(owned["runtime_config"]["gateway"]["token"], "***");
    let (_, all) = call(
        &app,
        Method::GET,
        &format!("{list}&include_archived=true"),
        &jwt(users[0]),
        Value::Null,
    )
    .await;
    assert_eq!(all.as_array().unwrap().len(), 4);
    assert!(
        all.as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == json!(archived))
    );
    let archived_item = all
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == json!(archived))
        .unwrap();
    assert!(archived_item.get("runtime_availability").is_none());
    db.execute(
        "UPDATE workspace SET settings=$2 WHERE id=$1",
        &[&ws, &json!({"always_redact_env":true})],
    )
    .await
    .unwrap();
    let (_, redacted) = call(
        &app,
        Method::GET,
        &detail(private),
        &jwt(users[2]),
        Value::Null,
    )
    .await;
    assert!(redacted["mcp_config"].is_null());
    assert_eq!(redacted["mcp_config_redacted"], true);
    assert!(redacted.get("custom_env").is_none());
    db.execute("UPDATE workspace SET settings='{}' WHERE id=$1", &[&ws])
        .await
        .unwrap();
    let task:Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id", &[&shared,&runtime]).await.unwrap().get(0);
    let token = format!("mat_read_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(token.as_bytes())),&task,&shared,&ws,&users[2]]).await.unwrap();
    let (status, agent_actor) =
        call(&app, Method::GET, &detail(private), &token, Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{agent_actor}");
    assert!(agent_actor["mcp_config"].is_null());
    assert_eq!(agent_actor["mcp_config_redacted"], true);
    assert_eq!(agent_actor["runtime_config"]["gateway"]["token"], "***");
    let serialized = serde_json::to_string(&agent_actor).unwrap();
    for secret in [
        "never disclose",
        "synthetic-mcp-token",
        "synthetic-gateway-token",
        "synthetic-personal-app",
    ] {
        assert!(!serialized.contains(secret), "leak: {secret}");
    }
    db.execute(
        "UPDATE agent_runtime SET last_seen_at=now()-interval '10 minutes' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    let (_, offline) = call(
        &app,
        Method::GET,
        &detail(shared),
        &jwt(users[3]),
        Value::Null,
    )
    .await;
    assert_eq!(offline["runtime_availability"], "offline");
    db.execute(
        "UPDATE agent_runtime SET visibility='public' WHERE id=$1",
        &[&runtime],
    )
    .await
    .unwrap();
    let (_, public_runtime) = call(
        &app,
        Method::GET,
        &detail(shared),
        &jwt(users[3]),
        Value::Null,
    )
    .await;
    assert!(public_runtime.get("runtime_availability").is_none());
    db.execute(
        "UPDATE agent_runtime SET visibility='private',shared_with=$2 WHERE id=$1",
        &[&runtime, &vec![users[3]]],
    )
    .await
    .unwrap();
    let (_, shared_runtime) = call(
        &app,
        Method::GET,
        &detail(shared),
        &jwt(users[3]),
        Value::Null,
    )
    .await;
    assert!(shared_runtime.get("runtime_availability").is_none());
    let (status, archived_view) = call(
        &app,
        Method::GET,
        &detail(archived),
        &jwt(users[3]),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(archived_view.get("runtime_availability").is_none());
    assert_eq!(archived_view["visibility"], "workspace");

    // A2A inspection bypasses view restrictions, never secret restrictions.
    let caller_runtime:Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,owner_id,visibility,status) VALUES ($1,'fake A2A runtime','local','claude',$2,'private','online') RETURNING id", &[&ws,&users[4]]).await.unwrap().get(0);
    let caller:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id) VALUES ($1,'A2A reader','local',$2,$3) RETURNING id", &[&ws,&users[4],&caller_runtime]).await.unwrap().get(0);
    let caller_task: Uuid = db
        .query_one(
            "INSERT INTO agent_task_queue(agent_id,runtime_id,status) VALUES ($1,$2,'running') RETURNING id",
            &[&caller,&caller_runtime],
        )
        .await
        .unwrap()
        .get(0);
    let caller_token = format!("mat_other_read_{suffix}");
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hex::encode(Sha256::digest(caller_token.as_bytes())),&caller_task,&caller,&ws,&users[4]]).await.unwrap();
    let (status, a2a) = call(
        &app,
        Method::GET,
        &detail(private),
        &caller_token,
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{a2a}");
    assert!(a2a["mcp_config"].is_null());
    assert_eq!(a2a["mcp_config_redacted"], true);
    assert_eq!(
        call(
            &app,
            Method::GET,
            &format!("/api/agents/?workspace_id={other}"),
            &caller_token,
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    // Replacing mode + targets atomically must not produce a mixed snapshot.
    let writer_pool = pool.clone();
    let writer = tokio::spawn(async move {
        let mut writer_db = writer_pool.get().await.unwrap();
        for n in 0..24 {
            let tx = writer_db.transaction().await.unwrap();
            tx.execute(
                "UPDATE agent SET permission_mode=$2 WHERE id=$1",
                &[&shared, &if n % 2 == 0 { "private" } else { "public_to" }],
            )
            .await
            .unwrap();
            tx.execute(
                "DELETE FROM agent_invocation_target WHERE agent_id=$1",
                &[&shared],
            )
            .await
            .unwrap();
            if n % 2 == 1 {
                tx.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES ($1,'workspace',$2)", &[&shared,&ws]).await.unwrap();
            }
            tx.commit().await.unwrap();
        }
    });
    for _ in 0..24 {
        let (status, current) = call(
            &app,
            Method::GET,
            &detail(shared),
            &jwt(users[3]),
            Value::Null,
        )
        .await;
        if status == StatusCode::OK {
            assert_eq!(current["permission_mode"], "public_to");
            assert_eq!(current["invocation_targets"].as_array().unwrap().len(), 1);
            let target = &current["invocation_targets"][0];
            assert!(target["target_id"] == json!(ws) || target["target_id"] == json!(users[3]));
        } else {
            assert_eq!(status, StatusCode::FORBIDDEN, "{current}");
        }
    }
    writer.await.unwrap();
    let product_agent:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,system_key) VALUES ($1,'Product agent','local',$2,'mika') RETURNING id", &[&ws,&users[2]]).await.unwrap().get(0);
    let (status, product) = call(
        &app,
        Method::GET,
        &detail(product_agent),
        &jwt(users[2]),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{product}");
    assert_eq!(product["system_key"], "mika");
    assert!(
        product["system_instructions"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
}
