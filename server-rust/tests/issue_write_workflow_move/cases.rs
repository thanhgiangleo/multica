use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    db: &tokio_postgres::Client,
    pool: Arc<Pool>,
    ws: Uuid,
    foreign_ws: Uuid,
    user: Uuid,
    token: &str,
    task_token: &str,
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
    let working = format!("wf_working_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let old = format!("wf_old_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let finished = format!("wf_finished_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let foreign_only = format!("wf_foreign_{}", &Uuid::new_v4().simple().to_string()[..8]);
    for (key, category) in [
        (&working, "started"),
        (&old, "started"),
        (&finished, "done"),
    ] {
        db.execute(
            "INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,$2,$2,$3,'#112233')",
            &[&ws, key, &category],
        )
        .await
        .unwrap();
    }
    db.execute(
        "INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,$2,$2,'started','#112233')",
        &[&foreign_ws, &foreign_only],
    )
    .await
    .unwrap();
    // Archived source statuses still carry lifecycle category for migration.
    db.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key=$2",
        &[&ws, &old],
    )
    .await
    .unwrap();
    let steps = json!([
        {"status_key":"todo"},
        {"status_key":working},
        {"status_key":"in_progress"},
        {"status_key":"done"},
        {"status_key":"cancelled"}
    ]);
    let workflow: Uuid = db
        .query_one(
            "INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Move target','todo',$2) RETURNING id",
            &[&ws, &steps],
        )
        .await
        .unwrap()
        .get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Move target',$2) RETURNING id",
            &[&ws, &workflow],
        )
        .await
        .unwrap()
        .get(0);
    let parent: Uuid = db
        .query_one(
            "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Move parent','member',$2,11600) RETURNING id",
            &[&ws, &user],
        )
        .await
        .unwrap()
        .get(0);
    let fixtures = [
        ("in_review", working.as_str(), true),
        (old.as_str(), working.as_str(), true),
        ("backlog", "todo", true),
        (foreign_only.as_str(), "todo", true),
        ("in_progress", "in_progress", false),
        (finished.as_str(), "done", true),
    ];
    for (index, (previous, target, changed)) in fixtures.into_iter().enumerate() {
        let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,assignee_type,assignee_id,parent_issue_id) VALUES($1,'Move child','member',$2,$3,$4,'member',$2,$5) RETURNING id", &[&ws,&user,&(11601+index as i32),&previous,&parent]).await.unwrap().get(0);
        let path = format!("/api/issues/{issue}?workspace_id={ws}");
        let pending = if previous == finished {
            let rule = Uuid::new_v4();
            db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode) VALUES($1,$2,$3,$4,$5,'Synthetic','event','continuous')", &[&rule,&ws,&issue,&agent,&user]).await.unwrap();
            let task: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,context,runtime_id) VALUES($1,$2,'queued',$3,(SELECT runtime_id FROM agent WHERE id=$1)) RETURNING id", &[&agent,&issue,&json!({"wakeup_id":rule})]).await.unwrap().get(0);
            Some((rule, task))
        } else {
            None
        };
        let before_events = sink.updates.lock().unwrap().len();
        let before_cancelled = sink.cancellations.lock().unwrap().len();
        let before_child = children.calls.lock().unwrap().len();
        let result = call(&app, "PUT", &path, token, json!({"project_id":project})).await;
        assert_eq!(result.0, StatusCode::OK, "{previous}: {result:?}");
        assert_eq!(result.1["status"], target);
        assert_eq!(result.1["project_id"], json!(project));
        assert_eq!(result.1["assignee_id"], json!(user));
        assert_eq!(result.1["revision"], 2);
        let event = sink.updates.lock().unwrap()[before_events].clone();
        assert_eq!(event.payload["prev_status"], previous);
        assert_eq!(event.payload["status_changed"], changed);
        assert_eq!(event.payload["project_changed"], true);
        assert_eq!(sink.updates.lock().unwrap().len(), before_events + 1);
        assert_eq!(
            children.calls.lock().unwrap().len(),
            before_child + usize::from(changed)
        );
        if let Some((rule, task)) = pending {
            assert_eq!(
                sink.cancellations.lock().unwrap().len(),
                before_cancelled + 1
            );
            assert_eq!(
                sink.cancellations.lock().unwrap().last().unwrap().task_id,
                task
            );
            let enabled: bool = db
                .query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
                .await
                .unwrap()
                .get(0);
            assert!(!enabled);
            let status: String = db
                .query_one("SELECT status FROM agent_task_queue WHERE id=$1", &[&task])
                .await
                .unwrap()
                .get(0);
            assert_eq!(status, "cancelled");
        } else {
            assert_eq!(sink.cancellations.lock().unwrap().len(), before_cancelled);
        }
        if changed {
            assert_eq!(
                children.calls.lock().unwrap().last().unwrap().1,
                vec![parent]
            );
            let kinds: Vec<String> = db.query("SELECT kind FROM issue_child_event WHERE workspace_id=$1 AND child_id=$2 ORDER BY created_at,id", &[&ws,&issue]).await.unwrap().iter().map(|r| r.get(0)).collect();
            assert_eq!(kinds, ["attached"], "{previous}");
        }
    }

    // A project move that does not enter its new column cannot start the
    // configured agent handoff, even when its mapped status has a handler.
    db.execute(
        "UPDATE issue_workflow SET steps=$1 WHERE id=$2",
        &[
            &json!([
                {"status_key":"todo"}, {"status_key":working,"handler":{"type":"agent","id":agent}},
                {"status_key":"done"}, {"status_key":"cancelled"}
            ]),
            &workflow,
        ],
    )
    .await
    .unwrap();
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,assignee_type,assignee_id) VALUES($1,'Handoff not entered','member',$2,11610,'in_review','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let path = format!("/api/issues/{issue}?workspace_id={ws}");
    let administrative = call(&app, "PUT", &path, token, json!({"project_id":project})).await;
    assert_eq!(administrative.0, StatusCode::OK, "{administrative:?}");
    assert_eq!(administrative.1["status"], working);
    assert_eq!(administrative.1["assignee_id"], json!(user));
    let agent_move = call(
        &app,
        "PUT",
        &path,
        task_token,
        json!({"project_id":null,"title":"agent move"}),
    )
    .await;
    assert_eq!(agent_move.0, StatusCode::OK, "{agent_move:?}");

    // Explicit status wins over category mapping; an unlisted explicit status
    // is rejected without a title, revision, activity or event leak.
    let events = sink.updates.lock().unwrap().len();
    let rejected = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"status":"blocked","title":"uncommitted"}),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::BAD_REQUEST, "{rejected:?}");
    assert_eq!(rejected.1["code"], "status_not_in_workflow");
    assert_eq!(sink.updates.lock().unwrap().len(), events);
    let row = db
        .query_one(
            "SELECT status,title,revision,project_id FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), working);
    assert_eq!(row.get::<_, String>("title"), "agent move");
    assert_eq!(row.get::<_, Option<Uuid>>("project_id"), None);

    // A task bound to another issue does not own this issue's workflow step.
    db.execute("UPDATE issue SET status='in_review' WHERE id=$1", &[&issue])
        .await
        .unwrap();
    let agent_move = call(
        &app,
        "PUT",
        &path,
        task_token,
        json!({"project_id":project,"title":"agent move into project"}),
    )
    .await;
    assert_eq!(agent_move.0, StatusCode::OK, "{agent_move:?}");
    assert_eq!(agent_move.1["status"], working);

    // The bound run tracks the column it is actually working on. A move by
    // another writer cannot be undone by its next status change.
    let bound: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,runtime_id,workflow_step) VALUES($1,$2,'running',(SELECT runtime_id FROM agent WHERE id=$1),$3) RETURNING id", &[&agent,&issue,&working]).await.unwrap().get(0);
    let bound_token = format!("mat_wf_step_{}", Uuid::new_v4());
    let hash = hex::encode(Sha256::digest(bound_token.as_bytes()));
    db.execute("INSERT INTO task_token(token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES($1,$2,$3,$4,$5,now()+interval '1 hour')", &[&hash,&bound,&agent,&ws,&user]).await.unwrap();
    let next = call(&app, "PUT", &path, &bound_token, json!({"status":"done"})).await;
    assert_eq!(next.0, StatusCode::OK, "{next:?}");
    let step: Option<String> = db
        .query_one(
            "SELECT workflow_step FROM agent_task_queue WHERE id=$1",
            &[&bound],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(step.as_deref(), Some("done"));
    db.execute(
        "UPDATE issue SET status=$1 WHERE id=$2",
        &[&working, &issue],
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE issue_status SET name='Working on it' WHERE workspace_id=$1 AND key=$2",
        &[&ws, &working],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO activity_log(workspace_id,issue_id,action,actor_type,actor_id,created_at) VALUES($1,$2,'status_changed','member',$3,now()+interval '1 minute')", &[&ws,&issue,&user]).await.unwrap();
    let workspace = db
        .query_one(
            "SELECT name,issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .unwrap();
    let prefix: String = workspace.get("issue_prefix");
    let prefix = if prefix.is_empty() {
        workspace
            .get::<_, String>("name")
            .chars()
            .filter(char::is_ascii_alphabetic)
            .take(3)
            .collect::<String>()
            .to_ascii_uppercase()
    } else {
        prefix
    };
    let number: i32 = db
        .query_one("SELECT number FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let mover: String = db
        .query_one("SELECT name FROM \"user\" WHERE id=$1", &[&user])
        .await
        .unwrap()
        .get(0);
    let before = db
        .query_one("SELECT revision,title FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    let blocked = call(
        &app,
        "PUT",
        &path,
        &bound_token,
        json!({"status":"todo","title":"uncommitted"}),
    )
    .await;
    assert_eq!(blocked.0, StatusCode::CONFLICT, "{blocked:?}");
    assert_eq!(blocked.1["code"], "workflow_step_moved");
    assert_eq!(blocked.1["step"], "done");
    assert_eq!(blocked.1["current_status"], working);
    assert_eq!(
        blocked.1["error"],
        format!(
            "{prefix}-{number} is no longer at \"done\": {mover} moved it to \"Working on it\" ({working}) while this run was working, so this status change was not applied. Do not change the status again in this run; leave a comment if something still needs attention."
        )
    );
    let after = db
        .query_one("SELECT revision,title FROM issue WHERE id=$1", &[&issue])
        .await
        .unwrap();
    assert_eq!(
        after.get::<_, i64>("revision"),
        before.get::<_, i64>("revision")
    );
    assert_eq!(
        after.get::<_, String>("title"),
        before.get::<_, String>("title")
    );
    db.execute("UPDATE issue SET status='in_review' WHERE id=$1", &[&issue])
        .await
        .unwrap();

    // Default never remaps. Without the child adapter an administrative move
    // that preserves status is safe, but an implicit status change is not.
    let no_child = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"update-test-secret".to_vec())
            .with_issue_event_sink(sink.clone()),
    );
    db.execute(
        "UPDATE issue_workflow SET is_default=true WHERE id=$1",
        &[&workflow],
    )
    .await
    .unwrap();
    let default_move = call(
        &no_child,
        "PUT",
        &path,
        token,
        json!({"project_id":project}),
    )
    .await;
    assert_eq!(default_move.0, StatusCode::OK, "{default_move:?}");
    assert_eq!(default_move.1["status"], "in_review");
    let detached = call(&no_child, "PUT", &path, token, json!({"project_id":null})).await;
    assert_eq!(detached.0, StatusCode::OK, "{detached:?}");
    db.execute(
        "UPDATE issue_workflow SET is_default=false WHERE id=$1",
        &[&workflow],
    )
    .await
    .unwrap();
    let no_adapter = call(
        &no_child,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"title":"no adapter"}),
    )
    .await;
    assert_eq!(
        no_adapter.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{no_adapter:?}"
    );

    // Namespace reservation happens before locking owner rows. If catalog
    // archival already holds the exclusive lock, remapping waits, then refuses
    // its archived target without leaking project/title/revision/events.
    let mut lock_db = pool.get().await.unwrap();
    let tx = lock_db.transaction().await.unwrap();
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .unwrap();
    let backend: i32 = tx
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let prior = db
        .query_one(
            "SELECT revision,status,title,project_id FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    let event_count = sink.updates.lock().unwrap().len();
    let pending_app = app.clone();
    let pending_path = path.clone();
    let pending_token = token.to_owned();
    let pending = tokio::spawn(async move {
        call(
            &pending_app,
            "PUT",
            &pending_path,
            &pending_token,
            json!({"project_id":project,"status":null,"title":"archive race"}),
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'SELECT pg_advisory_xact_lock_shared%')", &[&backend]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waiting, "project move must wait for status catalog writer");
    tx.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key=$2",
        &[&ws, &working],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let raced = pending.await.unwrap();
    assert_eq!(raced.0, StatusCode::CONFLICT, "{raced:?}");
    let after = db
        .query_one(
            "SELECT revision,status,title,project_id FROM issue WHERE id=$1",
            &[&issue],
        )
        .await
        .unwrap();
    assert_eq!(
        after.get::<_, i64>("revision"),
        prior.get::<_, i64>("revision")
    );
    assert_eq!(
        after.get::<_, Option<Uuid>>("project_id"),
        prior.get::<_, Option<Uuid>>("project_id")
    );
    for key in ["status", "title"] {
        assert_eq!(after.get::<_, String>(key), prior.get::<_, String>(key));
    }
    assert_eq!(sink.updates.lock().unwrap().len(), event_count);
}
