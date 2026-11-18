use super::*;

#[path = "../issue_write_workflow_batch/cases.rs"]
mod workflow_batch;
#[path = "../issue_write_workflow_enter/cases.rs"]
mod workflow_enter;
#[path = "../issue_write_workflow_move/cases.rs"]
mod workflow_move;

#[derive(Default)]
struct Children {
    calls: Mutex<Vec<(Uuid, Vec<Uuid>)>>,
    sink: Arc<Sink>,
}
impl IssueChildRuntime for Children {
    fn process_child_events(&self, ws: Uuid, parents: Vec<Uuid>) {
        self.sink.order.lock().unwrap().push("child:process");
        self.calls.lock().unwrap().push((ws, parents));
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    db: &tokio_postgres::Client,
    pool: Arc<Pool>,
    sink: Arc<Sink>,
    ws: Uuid,
    foreign_ws: Uuid,
    user: Uuid,
    outsider: Uuid,
    token: &str,
    task_token: &str,
    run_id: Uuid,
    agent: Uuid,
) {
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
    let parent_a: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Parent A','member',$2,101) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let parent_b: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Parent B','member',$2,102) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let child_a: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Child A','member',$2,103) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let child_b: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Child B','member',$2,104) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let triage: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,triage_state) VALUES($1,'Triage','member',$2,105,'pending') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Local') RETURNING id",
            &[&ws],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign') RETURNING id",
            &[&foreign_ws],
        )
        .await
        .unwrap()
        .get(0);
    let path = format!("/api/issues/{child_a}?workspace_id={ws}");
    let path_b = format!("/api/issues/{child_b}?workspace_id={ws}");
    let batch = format!("/api/issues/batch?workspace_id={ws}");
    let events = |id: Uuid| async move {
        db.query("SELECT kind,parent_id,source_task_id FROM issue_child_event WHERE workspace_id=$1 AND child_id=$2 ORDER BY created_at,id", &[&ws,&id]).await.unwrap()
    };

    let first = call(&app,"PUT",&path,token,json!({"parent_issue_id":parent_a,"stage":2,"project_id":project,"assignee_type":"member","assignee_id":user,"expected_revision":1})).await;
    assert_eq!(first.0, StatusCode::OK, "{first:?}");
    assert_eq!(first.1["revision"], 2);
    assert_eq!(first.1["parent_issue_id"], json!(parent_a));
    assert_eq!(first.1["stage"], 2);
    assert_eq!(first.1["project_id"], json!(project));
    assert_eq!(first.1["assignee_id"], json!(user));
    assert_eq!(events(child_a).await.len(), 1);
    assert_eq!(
        events(child_a).await[0].get::<_, String>("kind"),
        "attached"
    );
    assert_eq!(
        children.calls.lock().unwrap().as_slice(),
        &[(ws, vec![parent_a])]
    );
    assert_eq!(
        sink.order.lock().unwrap().last().copied(),
        Some("child:process")
    );
    let event = sink.updates.lock().unwrap().last().unwrap().clone();
    assert_eq!(event.payload["assignee_changed"], true);
    assert_eq!(event.payload["project_changed"], true);
    assert_eq!(event.payload["issue"]["parent_issue_id"], json!(parent_a));

    let unchanged = call(&app,"PUT",&path,token,json!({"parent_issue_id":parent_a,"stage":2,"project_id":project,"assignee_type":"member","assignee_id":user})).await;
    assert_eq!(unchanged.0, StatusCode::OK, "{unchanged:?}");
    assert_eq!(unchanged.1["revision"], 2);
    assert_eq!(events(child_a).await.len(), 1);
    assert_eq!(children.calls.lock().unwrap().len(), 1);
    let cleared = call(&app,"PUT",&path,token,json!({"parent_issue_id":null,"stage":null,"project_id":null,"assignee_type":null,"assignee_id":null})).await;
    assert_eq!(cleared.0, StatusCode::OK, "{cleared:?}");
    assert_eq!(cleared.1["revision"], 3);
    for key in [
        "parent_issue_id",
        "stage",
        "project_id",
        "assignee_type",
        "assignee_id",
    ] {
        assert!(cleared.1[key].is_null(), "{key}");
    }
    assert_eq!(events(child_a).await.len(), 2);
    assert_eq!(
        events(child_a).await[1].get::<_, String>("kind"),
        "detached"
    );
    assert_eq!(children.calls.lock().unwrap().len(), 2);

    for (body, code) in [
        (
            json!({"parent_issue_id":child_a,"title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"parent_issue_id":Uuid::new_v4(),"title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"parent_issue_id":"bad","title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"parent_issue_id":parent_b,"stage":0,"title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"project_id":foreign_project,"title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"assignee_type":"member","assignee_id":outsider,"title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"assignee_type":"member","title":"not committed"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"assignee_type":"agent","assignee_id":agent,"title":"not committed"}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        let result = call(&app, "PUT", &path, token, body).await;
        assert_eq!(result.0, code, "{result:?}");
    }
    let row = db
        .query_one("SELECT title,revision FROM issue WHERE id=$1", &[&child_a])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("title"), "Child A");
    assert_eq!(row.get::<_, i64>("revision"), 3);
    assert_eq!(events(child_a).await.len(), 2);

    let cycle_setup = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"parent_issue_id":parent_a}),
    )
    .await;
    assert_eq!(cycle_setup.0, StatusCode::OK, "{cycle_setup:?}");
    let cycle = call(
        &app,
        "PUT",
        &format!("/api/issues/{parent_a}?workspace_id={ws}"),
        token,
        json!({"parent_issue_id":child_a}),
    )
    .await;
    assert_eq!(cycle.0, StatusCode::BAD_REQUEST, "{cycle:?}");
    assert_eq!(cycle.1["error"], "circular parent relationship detected");
    let foreign = db
        .query_one(
            "SELECT id FROM issue WHERE workspace_id=$1 LIMIT 1",
            &[&foreign_ws],
        )
        .await
        .unwrap()
        .get::<_, Uuid>(0);
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            token,
            json!({"parent_issue_id":foreign})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    let triage_result = call(
        &app,
        "PUT",
        &format!("/api/issues/{triage}?workspace_id={ws}"),
        token,
        json!({"parent_issue_id":null}),
    )
    .await;
    assert_eq!(triage_result.0, StatusCode::BAD_REQUEST);
    assert_eq!(triage_result.1["code"], "issue_in_triage");
    let preflight=call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,triage],"updates":{"parent_issue_id":parent_b,"title":"not committed"}})).await;
    assert_eq!(preflight.0, StatusCode::BAD_REQUEST, "{preflight:?}");
    assert_eq!(preflight.1["code"], "issue_in_triage");
    assert_eq!(
        db.query_one(
            "SELECT title,parent_issue_id FROM issue WHERE id=$1",
            &[&child_a]
        )
        .await
        .unwrap()
        .get::<_, String>("title"),
        "Child A"
    );
    let invalid_project=call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a],"updates":{"project_id":foreign_project,"title":"not committed"}})).await;
    assert_eq!(
        invalid_project.0,
        StatusCode::BAD_REQUEST,
        "{invalid_project:?}"
    );

    let before_assign = children.calls.lock().unwrap().len();
    let assigned=call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,child_b],"updates":{"project_id":project,"assignee_type":"member","assignee_id":user}})).await;
    assert_eq!(
        assigned,
        (StatusCode::OK, json!({"updated":2})),
        "{assigned:?}"
    );
    assert_eq!(children.calls.lock().unwrap().len(), before_assign);
    let event = sink.updates.lock().unwrap().last().unwrap().clone();
    assert_eq!(event.payload["assignee_changed"], true);
    assert_eq!(event.payload["project_changed"], true);
    let gated_agent = call(
        &app,
        "PATCH",
        &batch,
        token,
        json!({"issue_ids":[child_a],"updates":{"assignee_type":"agent","assignee_id":agent}}),
    )
    .await;
    assert_eq!(
        gated_agent.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{gated_agent:?}"
    );

    let before = children.calls.lock().unwrap().len();
    let updates = sink.updates.lock().unwrap().len();
    let batch_result=call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,child_b,parent_b,"bad",foreign,child_a],"updates":{"parent_issue_id":parent_b,"stage":3}})).await;
    assert_eq!(
        batch_result,
        (StatusCode::OK, json!({"updated":3})),
        "{batch_result:?}"
    );
    assert_eq!(children.calls.lock().unwrap().len(), before + 1);
    let mut expected_parents = vec![parent_a, parent_b];
    expected_parents.sort_unstable();
    assert_eq!(
        children.calls.lock().unwrap()[before],
        (ws, expected_parents)
    );
    assert_eq!(sink.updates.lock().unwrap().len(), updates + 3);
    let b = db
        .query_one(
            "SELECT parent_issue_id,stage FROM issue WHERE id=$1",
            &[&child_b],
        )
        .await
        .unwrap();
    assert_eq!(b.get::<_, Option<Uuid>>("parent_issue_id"), Some(parent_b));
    assert_eq!(b.get::<_, Option<i32>>("stage"), Some(3));
    assert_eq!(call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,child_b],"updates":{"parent_issue_id":foreign,"title":"not committed"}})).await,(StatusCode::OK,json!({"updated":0})));
    assert_eq!(
        call(
            &app,
            "PATCH",
            &batch,
            token,
            json!({"issue_ids":[child_a],"updates":{"stage":0,"title":"not committed"}})
        )
        .await,
        (StatusCode::OK, json!({"updated":0}))
    );

    let source = call(&app, "PUT", &path_b, task_token, json!({"stage":4})).await;
    assert_eq!(source.0, StatusCode::OK, "{source:?}");
    let source_events = events(child_b).await;
    assert_eq!(
        source_events.last().unwrap().get::<_, String>("kind"),
        "restaged"
    );
    assert_eq!(
        source_events
            .last()
            .unwrap()
            .get::<_, Option<Uuid>>("source_task_id"),
        Some(run_id)
    );
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().actor_type,
        "agent"
    );

    let failing = format!("deny_structure_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {failing}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{child_a}'::uuid AND NEW.title='fail-rollback' THEN RAISE EXCEPTION 'synthetic rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {failing} AFTER UPDATE ON issue FOR EACH ROW EXECUTE FUNCTION {failing}();")).await.unwrap();
    let count = events(child_a).await.len();
    let calls = children.calls.lock().unwrap().len();
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            token,
            json!({"parent_issue_id":parent_a,"stage":5,"title":"fail-rollback"})
        )
        .await
        .0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(events(child_a).await.len(), count);
    assert_eq!(children.calls.lock().unwrap().len(), calls);
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&child_a])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Child A"
    );
    let partial = call(
        &app,
        "PATCH",
        &batch,
        token,
        json!({"issue_ids":[child_a,child_b],"updates":{"title":"fail-rollback"}}),
    )
    .await;
    assert_eq!(
        partial,
        (StatusCode::OK, json!({"updated":1})),
        "{partial:?}"
    );
    db.batch_execute(&format!(
        "DROP TRIGGER {failing} ON issue; DROP FUNCTION {failing}();"
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&child_b])
            .await
            .unwrap()
            .get::<_, String>(0),
        "fail-rollback"
    );
    let unstage = call(
        &app,
        "PATCH",
        &batch,
        token,
        json!({"issue_ids":[child_a,child_b],"updates":{"parent_issue_id":null,"stage":null,"project_id":null}}),
    )
    .await;
    assert_eq!(unstage, (StatusCode::OK, json!({"updated":2})));
    let row = db
        .query_one(
            "SELECT parent_issue_id,stage FROM issue WHERE id=$1",
            &[&child_a],
        )
        .await
        .unwrap();
    assert!(row.get::<_, Option<Uuid>>(0).is_none() && row.get::<_, Option<i32>>(1).is_none());

    // A workflow activated by the first committed item rejects the second
    // item, but must not swallow the first item's committed child notification.
    let workflow_trigger = format!("activate_workflow_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {workflow_trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE flow uuid; BEGIN IF NEW.id='{child_a}'::uuid AND NEW.project_id='{project}'::uuid THEN INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES('{ws}'::uuid,'Activated during batch','todo','[]'::jsonb) RETURNING id INTO flow; UPDATE project SET workflow_id=flow WHERE id='{project}'::uuid; END IF; RETURN NEW; END $$; CREATE TRIGGER {workflow_trigger} AFTER UPDATE ON issue FOR EACH ROW EXECUTE FUNCTION {workflow_trigger}();")).await.unwrap();
    let before = children.calls.lock().unwrap().len();
    let interrupted = call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,child_b],"updates":{"parent_issue_id":parent_a,"project_id":project}})).await;
    assert_eq!(
        interrupted,
        (StatusCode::OK, json!({"updated":1})),
        "{interrupted:?}"
    );
    assert_eq!(children.calls.lock().unwrap().len(), before + 1);
    assert_eq!(children.calls.lock().unwrap()[before], (ws, vec![parent_a]));
    assert_eq!(
        db.query_one("SELECT project_id FROM issue WHERE id=$1", &[&child_b])
            .await
            .unwrap()
            .get::<_, Option<Uuid>>(0),
        None
    );
    db.batch_execute(&format!("DROP TRIGGER {workflow_trigger} ON issue; DROP FUNCTION {workflow_trigger}(); UPDATE project SET workflow_id=NULL WHERE id='{project}'::uuid; DELETE FROM issue_workflow WHERE workspace_id='{ws}'::uuid AND name='Activated during batch';")).await.unwrap();
    assert_eq!(
        call(
            &app,
            "PUT",
            &path,
            token,
            json!({"parent_issue_id":null,"project_id":null})
        )
        .await
        .0,
        StatusCode::OK
    );

    // Opposite concurrent moves cannot both pass the ancestor check.
    let left = format!("/api/issues/{parent_a}?workspace_id={ws}");
    let right = format!("/api/issues/{parent_b}?workspace_id={ws}");
    let (a, b) = tokio::join!(
        call(
            &app,
            "PUT",
            &left,
            token,
            json!({"parent_issue_id":parent_b})
        ),
        call(
            &app,
            "PUT",
            &right,
            token,
            json!({"parent_issue_id":parent_a})
        ),
    );
    assert!(
        matches!(
            (a.0, b.0),
            (StatusCode::OK, StatusCode::BAD_REQUEST) | (StatusCode::BAD_REQUEST, StatusCode::OK)
        ),
        "{a:?} {b:?}"
    );
    let workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key) VALUES($1,'Synthetic named','todo') RETURNING id", &[&ws]).await.unwrap().get(0);
    db.execute(
        "UPDATE project SET workflow_id=$1 WHERE id=$2",
        &[&workflow, &project],
    )
    .await
    .unwrap();
    let gated = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"title":"not committed"}),
    )
    .await;
    assert_eq!(gated.0, StatusCode::BAD_REQUEST, "{gated:?}");
    assert_eq!(gated.1["code"], "status_not_in_workflow");
    let gated_batch=call(&app,"PATCH",&batch,token,json!({"issue_ids":[child_a,child_b],"updates":{"project_id":project,"title":"not committed"}})).await;
    assert_eq!(
        gated_batch,
        (StatusCode::OK, json!({"updated":0})),
        "{gated_batch:?}"
    );
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&child_a])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Child A"
    );

    let steps = json!([
        {"status_key":"todo"},
        {"status_key":"in_progress"},
        {"status_key":"in_review","HANDLER":{"TYPE":"member","ID":user}},
        {"status_key":"done"},
        {"status_key":"cancelled"}
    ]);
    db.execute(
        "UPDATE issue_workflow SET steps=$1 WHERE id=$2",
        &[&steps, &workflow],
    )
    .await
    .unwrap();
    let accepted = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"project_id":project,"status":"todo","title":"Workflow member"}),
    )
    .await;
    assert_eq!(accepted.0, StatusCode::OK, "{accepted:?}");
    assert_eq!(accepted.1["project_id"], json!(project));
    let ignored = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"status":"blocked","title":"Not changed"}),
    )
    .await;
    assert_eq!(ignored.0, StatusCode::BAD_REQUEST, "{ignored:?}");
    assert_eq!(ignored.1["code"], "status_not_in_workflow");
    assert_eq!(ignored.1["workflow_name"], "Synthetic named");
    assert_eq!(
        ignored.1["allowed_statuses"],
        json!(["todo", "in_progress", "in_review", "done", "cancelled"])
    );
    let handoff = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"status":"in_review","title":"Not changed"}),
    )
    .await;
    assert_eq!(handoff.0, StatusCode::OK, "{handoff:?}");
    assert_eq!(handoff.1["assignee_id"], json!(user));
    let assigned = db
        .query_one(
            "SELECT title,status,revision FROM issue WHERE id=$1",
            &[&child_a],
        )
        .await
        .unwrap();
    assert_eq!(assigned.get::<_, String>("title"), "Not changed");
    assert_eq!(assigned.get::<_, String>("status"), "in_review");
    assert_eq!(
        assigned.get::<_, i64>("revision"),
        accepted.1["revision"].as_i64().unwrap() + 1
    );
    let reset = call(&app, "PUT", &path, token, json!({"status":"todo"})).await;
    assert_eq!(reset.0, StatusCode::OK, "{reset:?}");
    let explicit = call(
        &app,
        "PUT",
        &path,
        token,
        json!({"status":"in_review","assignee_type":"member","assignee_id":user}),
    )
    .await;
    assert_eq!(explicit.0, StatusCode::OK, "{explicit:?}");
    assert_eq!(explicit.1["assignee_id"], json!(user));
    assert_eq!(explicit.1["status"], "in_review");
    let default_steps = json!([{"status_key":"todo"},{"status_key":"in_review","handler":{"type":"member","id":user}}]);
    db.execute(
        "UPDATE issue_workflow SET steps=$1,is_default=true WHERE id=$2",
        &[&default_steps, &workflow],
    )
    .await
    .unwrap();
    let outside = call(&app, "PUT", &path_b, token, json!({"status":"blocked"})).await;
    assert_eq!(outside.0, StatusCode::OK, "{outside:?}");
    let default = call(&app, "PUT", &path, token, json!({"status":"blocked"})).await;
    assert_eq!(default.0, StatusCode::OK, "{default:?}");
    assert_eq!(default.1["status"], "blocked");

    // A cross-workspace or deleted named workflow reference uses this
    // workspace's Default, never the foreign workflow's private step list.
    let foreign_workflow:Uuid=db.query_one("INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Foreign secret','todo',$2) RETURNING id", &[&foreign_ws,&json!([{"status_key":"secret"}])]).await.unwrap().get(0);
    db.execute(
        "UPDATE project SET workflow_id=$1 WHERE id=$2",
        &[&foreign_workflow, &project],
    )
    .await
    .unwrap();
    let scoped = call(&app, "PUT", &path, token, json!({"status":"done"})).await;
    assert_eq!(scoped.0, StatusCode::OK, "{scoped:?}");
    db.execute(
        "UPDATE project SET workflow_id=$1 WHERE id=$2",
        &[&workflow, &project],
    )
    .await
    .unwrap();

    // Definition edits and issue writes serialize at the workflow row. A
    // request already in flight must see the committed new allowed statuses,
    // then reject atomically without a revision, event or task side effect.
    db.execute(
        "UPDATE issue_workflow SET is_default=false,steps=$1 WHERE id=$2",
        &[&steps, &workflow],
    )
    .await
    .unwrap();
    let mut lock_db = pool.get().await.unwrap();
    let tx = lock_db.transaction().await.unwrap();
    tx.query_one(
        "SELECT id FROM issue_workflow WHERE id=$1 FOR UPDATE",
        &[&workflow],
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
            "SELECT title,status,revision FROM issue WHERE id=$1",
            &[&child_a],
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
            json!({"status":"in_progress","title":"Must roll back"}),
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting=db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'SELECT name,is_default,steps FROM issue_workflow%')", &[&backend]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        waiting,
        "workflow writer must wait for definition transaction"
    );
    tx.execute(
        "UPDATE issue_workflow SET steps=$1 WHERE id=$2",
        &[
            &json!([{"status_key":"todo"},{"status_key":"done"},{"status_key":"cancelled"}]),
            &workflow,
        ],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let race = pending.await.unwrap();
    assert_eq!(race.0, StatusCode::BAD_REQUEST, "{race:?}");
    assert_eq!(race.1["code"], "status_not_in_workflow");
    let after = db
        .query_one(
            "SELECT title,status,revision FROM issue WHERE id=$1",
            &[&child_a],
        )
        .await
        .unwrap();
    for key in ["title", "status"] {
        assert_eq!(after.get::<_, String>(key), prior.get::<_, String>(key));
    }
    assert_eq!(
        after.get::<_, i64>("revision"),
        prior.get::<_, i64>("revision")
    );
    assert_eq!(sink.updates.lock().unwrap().len(), event_count);

    workflow_enter::run(db, pool.clone(), ws, foreign_ws, user, token, agent).await;
    workflow_move::run(
        db,
        pool.clone(),
        ws,
        foreign_ws,
        user,
        token,
        task_token,
        agent,
    )
    .await;
    workflow_batch::run(db, pool, ws, foreign_ws, user, token, task_token, agent).await;
}
