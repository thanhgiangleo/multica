use super::*;

#[path = "../issue_write_workflow_member/cases.rs"]
mod member;

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
    let custom = format!("wf_batch_{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.execute(
        "INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,$2,$2,'started','#112233')",
        &[&ws, &custom],
    )
    .await
    .unwrap();
    let steps = json!([
        {"status_key":"todo"},
        {"status_key":custom},
        {"status_key":"done"},
        {"status_key":"blocked","handler":{"type":"agent","id":agent}}
    ]);
    let workflow: Uuid = db
        .query_one(
            "INSERT INTO issue_workflow(workspace_id,name,initial_status_key,steps) VALUES($1,'Batch target','todo',$2) RETURNING id",
            &[&ws, &steps],
        )
        .await
        .unwrap()
        .get(0);
    let project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title,workflow_id) VALUES($1,'Batch target',$2) RETURNING id",
            &[&ws, &workflow],
        )
        .await
        .unwrap()
        .get(0);
    let foreign_project: Uuid = db
        .query_one(
            "INSERT INTO project(workspace_id,title) VALUES($1,'Foreign batch') RETURNING id",
            &[&foreign_ws],
        )
        .await
        .unwrap()
        .get(0);
    let parent: Uuid = db
        .query_one(
            "INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Batch parent','member',$2,11700) RETURNING id",
            &[&ws, &user],
        )
        .await
        .unwrap()
        .get(0);
    let first: Uuid = db
        .query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,parent_issue_id) VALUES($1,'First batch','member',$2,11701,'in_review',$3) RETURNING id", &[&ws, &user, &parent])
        .await
        .unwrap()
        .get(0);
    let second: Uuid = db
        .query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,parent_issue_id) VALUES($1,'Second batch','member',$2,11702,'backlog',$3) RETURNING id", &[&ws, &user, &parent])
        .await
        .unwrap()
        .get(0);
    let batch = format!("/api/issues/batch-update?workspace_id={ws}");
    let before = sink.updates.lock().unwrap().len();
    let moved = call(
        &app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[first,second],"updates":{"project_id":project}}),
    )
    .await;
    assert_eq!(moved, (StatusCode::OK, json!({"updated":2})), "{moved:?}");
    let rows = db.query("SELECT id,status,project_id,revision FROM issue WHERE id=ANY($1::uuid[]) ORDER BY number", &[&vec![first,second]]).await.unwrap();
    assert_eq!(rows[0].get::<_, String>("status"), custom);
    assert_eq!(rows[1].get::<_, String>("status"), "todo");
    assert!(
        rows.iter()
            .all(|r| r.get::<_, Option<Uuid>>("project_id") == Some(project)
                && r.get::<_, i64>("revision") == 2)
    );
    {
        let events = sink.updates.lock().unwrap();
        assert_eq!(events.len(), before + 2);
        assert!(
            events[before..].iter().all(
                |e| e.payload["status_changed"] == true && e.payload["project_changed"] == true
            )
        );
    }
    assert_eq!(
        children.calls.lock().unwrap().last().unwrap().1,
        vec![parent]
    );
    assert_eq!(db.query("SELECT child_id FROM issue_child_event WHERE workspace_id=$1 AND child_id=ANY($2::uuid[])", &[&ws,&vec![first,second]]).await.unwrap().len(),2);

    let before = sink.updates.lock().unwrap().len();
    let rejected = call(&app,"POST",&batch,token,json!({"issue_ids":[first,second],"updates":{"status":"in_progress","title":"not applied"}})).await;
    assert_eq!(
        rejected,
        (StatusCode::OK, json!({"updated":0})),
        "{rejected:?}"
    );
    assert_eq!(sink.updates.lock().unwrap().len(), before);
    let first_title: String = db
        .query_one("SELECT title FROM issue WHERE id=$1", &[&first])
        .await
        .unwrap()
        .get(0);
    assert_eq!(first_title, "First batch");

    let handoff = call(
        &app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[first,second],"updates":{"status":"blocked","title":"no handoff"}}),
    )
    .await;
    assert_eq!(
        handoff,
        (StatusCode::OK, json!({"updated":0})),
        "{handoff:?}"
    );
    let override_handoff=call(&app,"POST",&batch,token,json!({"issue_ids":[first],"updates":{"status":"blocked","assignee_type":"member","assignee_id":user}})).await;
    assert_eq!(
        override_handoff,
        (StatusCode::OK, json!({"updated":1})),
        "{override_handoff:?}"
    );
    assert_eq!(
        db.query_one(
            "SELECT status,assignee_type,assignee_id FROM issue WHERE id=$1",
            &[&first]
        )
        .await
        .unwrap()
        .get::<_, String>("status"),
        "blocked"
    );

    let foreign = call(
        &app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[first],"updates":{"project_id":foreign_project,"title":"not applied"}}),
    )
    .await;
    assert_eq!(foreign.0, StatusCode::BAD_REQUEST, "{foreign:?}");

    // A task bound to another issue must not gate this item's status write.
    let agent_step = call(
        &app,
        "POST",
        &batch,
        task_token,
        json!({"issue_ids":[second],"updates":{"status":"done","title":"agent batch applied"}}),
    )
    .await;
    assert_eq!(
        agent_step,
        (StatusCode::OK, json!({"updated":1})),
        "{agent_step:?}"
    );

    let no_child = router_with_api(
        Arc::new(Health::new(
            Some(pool.clone()),
            MigrationCatalog::embedded().unwrap(),
            "test".into(),
        )),
        ApiState::new(pool.clone(), b"update-test-secret".to_vec())
            .with_issue_event_sink(sink.clone()),
    );
    let third:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,'No child adapter','member',$2,11703,'in_review') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let no_adapter = call(
        &no_child,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[third],"updates":{"project_id":project}}),
    )
    .await;
    assert_eq!(
        no_adapter,
        (StatusCode::OK, json!({"updated":0})),
        "{no_adapter:?}"
    );
    let row = db
        .query_one(
            "SELECT project_id,status,revision FROM issue WHERE id=$1",
            &[&third],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>("project_id"), None);
    assert_eq!(row.get::<_, String>("status"), "in_review");
    assert_eq!(row.get::<_, i64>("revision"), 1);

    member::run(db, &app, &sink, ws, foreign_ws, user, token, agent).await;

    // An archived shared target is a request-level 409, not a per-item skip.
    // The first item has not committed; the second must not commit either.
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
    let before = sink.updates.lock().unwrap().len();
    let prior = db
        .query("SELECT id,title,revision,project_id FROM issue WHERE id=ANY($1::uuid[]) ORDER BY number", &[&vec![second,third]])
        .await
        .unwrap();
    let pending_app = app.clone();
    let pending_batch = batch.clone();
    let pending_token = token.to_owned();
    let pending = tokio::spawn(async move {
        call(&pending_app,"POST",&pending_batch,&pending_token,json!({"issue_ids":[third,second],"updates":{"project_id":project,"title":"archive race"}})).await
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'SELECT pg_advisory_xact_lock_shared%')", &[&backend]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waiting, "batch move must wait for status catalog writer");
    tx.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key=$2",
        &[&ws, &custom],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let raced = pending.await.unwrap();
    assert_eq!(raced.0, StatusCode::CONFLICT, "{raced:?}");
    let after = db
        .query("SELECT id,title,revision,project_id FROM issue WHERE id=ANY($1::uuid[]) ORDER BY number", &[&vec![second,third]])
        .await
        .unwrap();
    for (before_row, after_row) in prior.iter().zip(after.iter()) {
        assert_eq!(
            after_row.get::<_, String>("title"),
            before_row.get::<_, String>("title")
        );
        assert_eq!(
            after_row.get::<_, i64>("revision"),
            before_row.get::<_, i64>("revision")
        );
        assert_eq!(
            after_row.get::<_, Option<Uuid>>("project_id"),
            before_row.get::<_, Option<Uuid>>("project_id")
        );
    }
    assert_eq!(sink.updates.lock().unwrap().len(), before);
}
