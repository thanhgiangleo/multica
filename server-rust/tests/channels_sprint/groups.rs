use super::*;

#[tokio::test]
#[ignore = "requires migrated disposable loopback multica_zbsi73 database"]
async fn dingtalk_group_inventory_privacy_pagination_and_forget() {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi73"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h,tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1"))
    );
    let pool = Arc::new(
        Pool::builder(Manager::new(cfg, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let db = pool.get().await.unwrap();
    let suffix = Uuid::new_v4().simple().to_string();
    let owner = insert_user(&db, "group-owner", &suffix).await;
    let member = insert_user(&db, "group-member", &suffix).await;
    let outsider = insert_user(&db, "group-outsider", &suffix).await;
    let workspace: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Groups',$1) RETURNING id",
            &[&format!("groups-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    let foreign: Uuid = db
        .query_one(
            "INSERT INTO workspace(name,slug) VALUES('Other groups',$1) RETURNING id",
            &[&format!("other-groups-{suffix}")],
        )
        .await
        .unwrap()
        .get(0);
    for (u, role) in [(owner, "owner"), (member, "member")] {
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,$3)",
            &[&workspace, &u, &role],
        )
        .await
        .unwrap();
    }
    let mut agents = Vec::new();
    let mut installations = Vec::new();
    for i in 0..5 {
        let ws = if i == 4 { foreign } else { workspace };
        let mode = if i == 1 { "private" } else { "public_to" };
        let agent:Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,permission_mode,visibility) VALUES($1,$2,'local',$3,$4,'workspace') RETURNING id", &[&ws,&format!("Bot {i}"),&owner,&mode]).await.unwrap().get(0);
        if i != 1 {
            db.execute("INSERT INTO agent_invocation_target(agent_id,target_type,target_id) VALUES($1,'workspace',$2)", &[&agent,&ws]).await.unwrap();
        }
        let inst:Uuid = db.query_one("INSERT INTO channel_installation(workspace_id,agent_id,channel_type,installer_user_id,config,status) VALUES($1,$2,'dingtalk',$3,$4,$5) RETURNING id", &[&ws,&agent,&owner,&json!({"client_secret":"never-project","ws_lease_token":"never-project"}),&(if i==3 {"revoked"} else {"active"})]).await.unwrap().get(0);
        db.execute("INSERT INTO dingtalk_bot_identity(workspace_id,installation_id,bot_name) VALUES($1,$2,$3)", &[&ws,&inst,&format!("Robot {i}")]).await.unwrap();
        db.execute("INSERT INTO dingtalk_group_presence(workspace_id,installation_id,conversation_id,conversation_title,last_active_at,mention_count) VALUES($1,$2,'shared','Alpha',now(),7)", &[&ws,&inst]).await.unwrap();
        for idx in 0..4 {
            db.execute("INSERT INTO dingtalk_group_presence(workspace_id,installation_id,conversation_id,conversation_title,last_active_at) VALUES($1,$2,$3,'Historical',now()-interval '91 days')", &[&ws,&inst,&format!("history-{idx}")]).await.unwrap();
        }
        agents.push(agent);
        installations.push(inst);
    }
    let fake = Arc::new(FakeRuntime::default());
    let health = Arc::new(Health::new(
        Some(pool.clone()),
        MigrationCatalog::embedded().unwrap(),
        "test".into(),
    ));
    let app = router_with_api(
        health.clone(),
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()).with_channel_runtime(fake),
    );
    let disabled = router_with_api(
        health,
        ApiState::new(pool.clone(), SECRET.as_bytes().to_vec()),
    );
    let root = format!("/api/workspaces/{workspace}/dingtalk/groups");
    let get = |path: String, user: Uuid| {
        let app = app.clone();
        async move { call(&app, "GET", &path, &token(user), Value::Null).await }
    };
    let normal = get(root.clone(), member).await;
    assert_eq!(normal.0, StatusCode::OK, "{normal:?}");
    assert_eq!(normal.1["groups"].as_array().unwrap().len(), 1);
    let bots = normal.1["groups"][0]["bots"].as_array().unwrap();
    assert_eq!(
        bots.len(),
        2,
        "private/revoked/foreign bot must not be visible"
    );
    assert!(
        bots.iter()
            .all(|b| b["installation_id"] != json!(installations[1]))
    );
    assert_eq!(bots[0]["mention_count"], 7);
    assert!(
        normal.1["inactive_group_counts"]
            .get(installations[1].to_string())
            .is_none()
    );
    assert!(
        normal.1["bot_identities"]
            .get(installations[1].to_string())
            .is_none()
    );
    assert!(!normal.1.to_string().contains("never-project"));
    let admin_view = get(root.clone(), owner).await;
    assert_eq!(
        admin_view.1["groups"][0]["bots"].as_array().unwrap().len(),
        3
    );
    assert_eq!(get(root.clone(), outsider).await.0, StatusCode::FORBIDDEN);
    assert_eq!(
        call(&app, "GET", &root, "invalid-jwt", Value::Null).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        get(format!("/api/agents/{}/dingtalk/groups", agents[1]), member)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let agent_view = get(format!("/api/agents/{}/dingtalk/groups", agents[0]), member).await;
    assert_eq!(agent_view.0, StatusCode::OK, "{agent_view:?}");
    assert_eq!(
        agent_view.1["groups"][0]["bots"].as_array().unwrap().len(),
        1
    );
    let page = get(
        format!(
            "{root}?activity=inactive&installation_id={}&limit=2",
            installations[0]
        ),
        member,
    )
    .await;
    assert_eq!(page.0, StatusCode::OK, "{page:?}");
    assert_eq!(page.1["groups"].as_array().unwrap().len(), 2);
    assert_eq!(page.1["next_offset"], 2);
    let tail = get(
        format!(
            "{root}?activity=inactive&installation_id={}&limit=2&offset=2",
            installations[0]
        ),
        member,
    )
    .await;
    assert_eq!(tail.1["groups"].as_array().unwrap().len(), 2);
    assert!(tail.1.get("next_offset").is_none());
    for hidden in [
        installations[1],
        installations[3],
        installations[4],
        Uuid::new_v4(),
    ] {
        let response = get(
            format!("{root}?activity=inactive&installation_id={hidden}&limit=1"),
            member,
        )
        .await;
        assert_eq!(response.0, StatusCode::OK, "{response:?}");
        assert_eq!(response.1["groups"], json!([]));
        assert!(
            response.1.get("next_offset").is_none(),
            "cursor must not leak hidden installation size"
        );
    }
    for query in [
        "activity=active",
        "activity=inactive",
        "activity=inactive&installation_id=bad",
        "activity=inactive&limit=101",
        "activity=inactive&offset=-1",
    ] {
        let query = if query.contains("installation_id") || !query.contains('&') {
            query.to_string()
        } else {
            format!("{query}&installation_id={}", installations[0])
        };
        assert_eq!(
            get(format!("{root}?{query}"), member).await.0,
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    let empty = call(&disabled, "GET", &root, &token(member), Value::Null).await;
    assert_eq!(
        empty.1,
        json!({"groups":[],"group_discovery_supported":true,"inactive_group_counts":{},"bot_identities":{}})
    );
    let forget = format!(
        "/api/workspaces/{workspace}/dingtalk/installations/{}/groups/shared",
        installations[0]
    );
    assert_eq!(
        call(&app, "DELETE", &forget, &token(member), Value::Null)
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!(
                "/api/workspaces/{foreign}/dingtalk/installations/{}/groups/shared",
                installations[0]
            ),
            &token(owner),
            Value::Null
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let owner_jwt = token(owner);
    let (first, second) = tokio::join!(
        call(&app, "DELETE", &forget, &owner_jwt, Value::Null),
        call(&app, "DELETE", &forget, &owner_jwt, Value::Null)
    );
    let mut statuses = [first.0.as_u16(), second.0.as_u16()];
    statuses.sort();
    assert_eq!(statuses, [204, 404]);
    let identity_count: i64 = db
        .query_one(
            "SELECT count(*) FROM dingtalk_bot_identity WHERE installation_id=$1",
            &[&installations[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(identity_count, 1, "forget retains bot identity");
    let active: bool = db
        .query_one(
            "SELECT status='active' FROM channel_installation WHERE id=$1",
            &[&installations[0]],
        )
        .await
        .unwrap()
        .get(0);
    assert!(active, "forget retains installation");
    assert_eq!(
        get(root.clone(), member).await.1["groups"][0]["bots"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    // A later authenticated ingress can observe the same group again; forget is not a tombstone.
    db.execute("INSERT INTO dingtalk_group_presence(workspace_id,installation_id,conversation_id,last_active_at) VALUES($1,$2,'shared',now())", &[&workspace,&installations[0]]).await.unwrap();
    assert_eq!(
        get(root.clone(), member).await.1["groups"][0]["bots"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    db.batch_execute("CREATE FUNCTION channels_sprint_reject_forget() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.conversation_id='rollback-case' THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN OLD; END $$; CREATE TRIGGER channels_sprint_reject_forget BEFORE DELETE ON dingtalk_group_presence FOR EACH ROW EXECUTE FUNCTION channels_sprint_reject_forget()").await.unwrap();
    db.execute("INSERT INTO dingtalk_group_presence(workspace_id,installation_id,conversation_id) VALUES($1,$2,'rollback-case')", &[&workspace,&installations[0]]).await.unwrap();
    let rollback = forget.replace("/groups/shared", "/groups/rollback-case");
    assert_eq!(
        call(&app, "DELETE", &rollback, &token(owner), Value::Null)
            .await
            .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    let still_exists:bool = db.query_one("SELECT EXISTS(SELECT 1 FROM dingtalk_group_presence WHERE installation_id=$1 AND conversation_id='rollback-case')", &[&installations[0]]).await.unwrap().get(0);
    assert!(still_exists, "database error rolls back forget");
    db.batch_execute("DROP TRIGGER channels_sprint_reject_forget ON dingtalk_group_presence; DROP FUNCTION channels_sprint_reject_forget()").await.unwrap();
}
