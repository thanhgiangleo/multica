use super::*;

pub(super) async fn run(
    db: &tokio_postgres::Client,
    pool: Arc<Pool>,
    ws: Uuid,
    foreign_ws: Uuid,
    user: Uuid,
    token: &str,
    agent: Uuid,
) {
    let sink = Arc::new(Sink::default());
    let children = Arc::new(Children {
        calls: Mutex::new(Vec::new()),
        sink: sink.clone(),
    });
    let app = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"update-test-secret".to_vec())
            .with_issue_event_sink(sink.clone())
            .with_issue_child_runtime(children.clone()),
    );
    let steps = json!([
        {"status_key":"todo","handler":{"type":"creator"}},
        {"status_key":"in_progress","handler":{"type":"member","id":user}},
        {"status_key":"done","handler":{"type":"agent","id":agent}}
    ]);
    let workflow: Uuid = db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Opt-in entry','todo',$2) RETURNING id", &[&ws,&steps]).await.unwrap().get(0);
    let project: Uuid = db.query_one("INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Opt-in entry',$2) RETURNING id", &[&ws,&workflow]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,'Opt-in issue','member',$2,11900,'todo') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let path = format!("/api/issues/{issue}?workspace_id={ws}");
    let moved = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"enter_workflow_step":true}),
    )
    .await;
    assert_eq!(moved.0, StatusCode::OK, "{moved:?}");
    assert_eq!(moved.1["status"], "todo");
    assert_eq!(moved.1["assignee_type"], "member");
    assert_eq!(moved.1["assignee_id"], json!(user));
    let event = sink.updates.lock().unwrap().last().unwrap().clone();
    assert_eq!(
        event.payload["workflow_handoff"],
        json!({"workflow":"Opt-in entry","to_type":"member","to_id":user,"run":"false"})
    );
    assert_eq!(event.payload["status_changed"], false);
    assert_eq!(event.payload["project_changed"], true);
    assert_eq!(event.payload["issue"], moved.1);
    let stored = db
        .query_one(
            "SELECT project_id,status,assignee_type,assignee_id,revision FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(stored.get::<_, Option<Uuid>>("project_id"), Some(project));
    assert_eq!(stored.get::<_, String>("status"), "todo");
    assert_eq!(
        stored.get::<_, Option<String>>("assignee_type").as_deref(),
        Some("member")
    );
    assert_eq!(stored.get::<_, Option<Uuid>>("assignee_id"), Some(user));
    assert_eq!(stored.get::<_, i64>("revision"), moved.1["revision"]);

    // Re-entering without a project move is not a handoff. An explicit
    // assignee on a project move wins over the configured step handler.
    let unchanged = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"enter_workflow_step":true,"status":"todo"}),
    )
    .await;
    assert_eq!(unchanged.0, StatusCode::OK, "{unchanged:?}");
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );
    let detached = call(&app, "PUT", &path, token, json!({"project_id":null})).await;
    assert_eq!(detached.0, StatusCode::OK, "{detached:?}");
    let explicit = call(&app, "PUT", &path, token, json!({"project_id":project,"enter_workflow_step":true,"assignee_type":"member","assignee_id":user})).await;
    assert_eq!(explicit.0, StatusCode::OK, "{explicit:?}");
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );

    // An opt-in move with explicit status enters that status, not the old one.
    let detached = call(&app, "PUT", &path, token, json!({"project_id":null})).await;
    assert_eq!(detached.0, StatusCode::OK, "{detached:?}");
    let entered = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"status":"in_progress","enter_workflow_step":true}),
    )
    .await;
    assert_eq!(entered.0, StatusCode::OK, "{entered:?}");
    assert_eq!(entered.1["status"], "in_progress");
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["workflow_handoff"]["to_id"],
        json!(user)
    );

    // An unavailable task dispatch, missing member, or foreign project cannot
    // commit title, project, assignment, revision, or issue events.
    let agent_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,'Agent entry','member',$2,11901,'done') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let agent_path = format!("/api/issues/{agent_issue}?workspace_id={ws}");
    let before = sink.updates.lock().unwrap().len();
    let gated = call(
        &app,
        "PUT",
        &agent_path,
        token,
        json!({"project_id":project,"enter_workflow_step":true,"title":"must roll back"}),
    )
    .await;
    assert_eq!(gated.0, StatusCode::SERVICE_UNAVAILABLE, "{gated:?}");
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    let not_changed = db
        .query_one(
            "SELECT title,project_id,revision FROM issue WHERE id=$1",
            &[&agent_issue],
        )
        .await
        .unwrap();
    assert_eq!(not_changed.get::<_, String>("title"), "Agent entry");
    assert_eq!(not_changed.get::<_, Option<Uuid>>("project_id"), None);
    assert_eq!(not_changed.get::<_, i64>("revision"), 1);

    let outsider: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Outside opt-in',$1) RETURNING id",
            &[&format!("opt-in-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'member')",
        &[&foreign_ws, &outsider],
    )
    .await
    .unwrap();
    let outsider_issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,'Outside creator','member',$2,11902,'todo') RETURNING id", &[&ws,&outsider]).await.unwrap().get(0);
    let outsider_path = format!("/api/issues/{outsider_issue}?workspace_id={ws}");
    let denied = call(
        &app,
        "PUT",
        &outsider_path,
        token,
        json!({"project_id":project,"enter_workflow_step":true,"title":"must roll back"}),
    )
    .await;
    assert_eq!(denied.0, StatusCode::BAD_REQUEST, "{denied:?}");
    let not_changed = db
        .query_one(
            "SELECT title,project_id,revision FROM issue WHERE id=$1",
            &[&outsider_issue],
        )
        .await
        .unwrap();
    assert_eq!(not_changed.get::<_, String>("title"), "Outside creator");
    assert_eq!(not_changed.get::<_, Option<Uuid>>("project_id"), None);
    assert_eq!(not_changed.get::<_, i64>("revision"), 1);
    assert_eq!(sink.updates.lock().unwrap().len(), before);

    let foreign: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign opt-in') RETURNING id",
            &[&foreign_ws],
        )
        .await
        .unwrap()
        .get(0);
    let denied = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":foreign,"enter_workflow_step":true}),
    )
    .await;
    assert_eq!(denied.0, StatusCode::BAD_REQUEST, "{denied:?}");
    let stop_noop = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"stop_previous_assignee_runs":true,"title":"No workflow handoff"}),
    )
    .await;
    assert_eq!(stop_noop.0, StatusCode::OK, "{stop_noop:?}");
    assert_eq!(stop_noop.1["title"], "No workflow handoff");
    assert_eq!(sink.updates.lock().unwrap().len(), before + 1);
    assert!(children.calls.lock().unwrap().is_empty());
    assert!(sink.cancellations.lock().unwrap().is_empty());

    // Stop has no effect when there is no workflow handoff, even for an
    // agent-owned issue. A real stop on an agent-to-member handoff stays 503.
    let old_agent: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,assignee_type,assignee_id) VALUES($1,'Old agent','member',$2,11903,'todo','agent',$3) RETURNING id", &[&ws,&user,&agent]).await.unwrap().get(0);
    let old_path = format!("/api/issues/{old_agent}?workspace_id={ws}");
    let queued: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,runtime_id) VALUES($1,$2,'queued',(SELECT runtime_id FROM agent WHERE id=$1)) RETURNING id", &[&agent,&old_agent]).await.unwrap().get(0);
    let noop = call(
        &app,
        "PUT",
        &old_path,
        token,
        json!({"stop_previous_assignee_runs":true,"title":"Unrelated edit"}),
    )
    .await;
    assert_eq!(noop.0, StatusCode::OK, "{noop:?}");
    let before = sink.updates.lock().unwrap().len();
    let stop = call(&app, "PUT", &old_path, token, json!({"project_id":project,"enter_workflow_step":true,"stop_previous_assignee_runs":true,"title":"must roll back"})).await;
    assert_eq!(stop.0, StatusCode::SERVICE_UNAVAILABLE, "{stop:?}");
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    let row = db
        .query_one(
            "SELECT project_id,assignee_id,title,revision FROM issue WHERE id=$1",
            &[&old_agent],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>("project_id"), None);
    assert_eq!(row.get::<_, Option<Uuid>>("assignee_id"), Some(agent));
    assert_eq!(row.get::<_, String>("title"), "Unrelated edit");
    assert_eq!(row.get::<_, i64>("revision"), noop.1["revision"]);
    let kept = call(
        &app,
        "PUT",
        &old_path,
        token,
        json!({"project_id":project,"enter_workflow_step":true}),
    )
    .await;
    assert_eq!(kept.0, StatusCode::OK, "{kept:?}");
    assert_eq!(kept.1["assignee_id"], json!(user));
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["workflow_handoff"]["run"],
        "false"
    );
    assert_eq!(
        db.query_one(
            "SELECT status FROM agent_task_queue WHERE id=$1",
            &[&queued]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "queued"
    );
    assert!(sink.cancellations.lock().unwrap().is_empty());

    // A Triage proposal never hands off, even with explicit entry requested.
    let triage: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,triage_state) VALUES($1,'Triage entry','member',$2,11904,'todo','pending') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let triage_path = format!("/api/issues/{triage}?workspace_id={ws}");
    let proposed = call(
        &app,
        "PUT",
        &triage_path,
        token,
        json!({"project_id":project,"enter_workflow_step":true,"stop_previous_assignee_runs":true}),
    )
    .await;
    assert_eq!(proposed.0, StatusCode::OK, "{proposed:?}");
    assert_eq!(proposed.1["assignee_id"], Value::Null);
    assert!(
        sink.updates
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .payload
            .get("workflow_handoff")
            .is_none()
    );

    // Implicit category mapping enters the mapped step and triggers child
    // progress; a folded null cannot erase Go's scalar boolean true.
    let remapped: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,parent_issue_id) VALUES($1,'Remapped entry','member',$2,11905,'in_review',$3) RETURNING id", &[&ws,&user,&issue]).await.unwrap().get(0);
    let remap_path = format!("/api/issues/{remapped}?workspace_id={ws}");
    let mapped = call_raw(&app, "PUT", &remap_path, token, format!(r#"{{"project_id":"{project}","enter_workflow_step":true,"ENTER_WORKFLOW_STEP":null,"stop_previous_assignee_runs":true}}"#)).await;
    assert_eq!(mapped.0, StatusCode::OK, "{mapped:?}");
    assert_eq!(mapped.1["status"], "in_progress");
    assert_eq!(mapped.1["assignee_id"], json!(user));
    let event = sink.updates.lock().unwrap().last().unwrap().clone();
    assert_eq!(event.payload["workflow_handoff"]["to_id"], json!(user));
    assert_eq!(event.payload["status_changed"], true);
    assert_eq!(
        children.calls.lock().unwrap().last().unwrap().1,
        vec![issue]
    );
    assert!(sink.cancellations.lock().unwrap().is_empty());
}
