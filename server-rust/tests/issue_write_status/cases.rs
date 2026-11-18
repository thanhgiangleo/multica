use super::*;

#[derive(Default)]
struct Children {
    calls: Mutex<Vec<(Uuid, Vec<Uuid>)>>,
    sink: Arc<Sink>,
}
impl IssueChildRuntime for Children {
    fn process_child_events(&self, workspace: Uuid, parents: Vec<Uuid>) {
        self.sink.order.lock().unwrap().push("child:process");
        self.calls.lock().unwrap().push((workspace, parents));
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
    foreign: Uuid,
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
    let source: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,position) VALUES($1,'Status source','member',$2,250,42) RETURNING id", &[&ws, &user]).await.unwrap().get(0);
    let target: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status,position) VALUES($1,'Status target','member',$2,251,'done',-8) RETURNING id", &[&ws, &user]).await.unwrap().get(0);
    let child: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,parent_issue_id) VALUES($1,'Status child','member',$2,252,$3) RETURNING id", &[&ws, &user, &source]).await.unwrap().get(0);
    let source_path = format!("/api/issues/{source}?workspace_id={ws}");
    let child_path = format!("/api/issues/{child}?workspace_id={ws}");
    let batch = format!("/api/issues/batch-update?workspace_id={ws}");
    let custom = format!("finished_{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,position) VALUES($1,$2,'Finished','done','#112233',10)", &[&ws, &custom]).await.unwrap();

    let invalid = call(
        &app,
        "PUT",
        &source_path,
        token,
        json!({"status":"not_a_status","title":"not committed"}),
    )
    .await;
    assert_eq!(invalid.0, StatusCode::BAD_REQUEST, "{invalid:?}");
    let moved = call(&app, "PUT", &source_path, token, json!({"status":"done"})).await;
    assert_eq!(moved.0, StatusCode::OK, "{moved:?}");
    assert_eq!(moved.1["position"], -9.0);
    assert_eq!(moved.1["status_category"], "done");
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["status_changed"],
        true
    );
    assert!(children.calls.lock().unwrap().is_empty());
    let no_op = call(&app, "PUT", &source_path, token, json!({"status":"done"})).await;
    assert_eq!(no_op.0, StatusCode::OK, "{no_op:?}");
    assert_eq!(no_op.1["revision"], moved.1["revision"]);
    assert_eq!(no_op.1["position"], -9.0);
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["status_changed"],
        false
    );
    let custom_result = call(
        &app,
        "PUT",
        &source_path,
        task_token,
        json!({"status":custom,"position":-30.0}),
    )
    .await;
    assert_eq!(custom_result.0, StatusCode::OK, "{custom_result:?}");
    assert_eq!(custom_result.1["status"], custom);
    assert_eq!(custom_result.1["status_category"], "done");
    assert_eq!(custom_result.1["status_name"], "Finished");
    assert_eq!(custom_result.1["position"], -30.0);
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().actor_type,
        "agent"
    );
    assert_eq!(sink.updates.lock().unwrap().last().unwrap().actor_id, agent);
    let changed: Vec<_> = db.query("SELECT kind,source_task_id FROM issue_child_event WHERE child_id=$1 AND workspace_id=$2 ORDER BY created_at,id", &[&source,&ws]).await.unwrap();
    assert!(changed.is_empty());
    let child_status = call(
        &app,
        "PUT",
        &child_path,
        task_token,
        json!({"status":"done"}),
    )
    .await;
    assert_eq!(child_status.0, StatusCode::OK, "{child_status:?}");
    assert!(
        !db.query(
            "SELECT id FROM issue_child_event WHERE child_id=$1 AND source_task_id=$2",
            &[&child, &run_id]
        )
        .await
        .unwrap()
        .is_empty()
    );

    for (body, code) in [
        (
            json!({"duplicate_of_issue_id":null}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"duplicate_of_issue_id":source}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"duplicate_of_issue_id":foreign}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"duplicate_of_issue_id":target,"status":"todo"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"duplicate_of_issue_id":target,"description":"edit"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let result = call(&app, "PUT", &source_path, token, body).await;
        assert_eq!(result.0, code, "{result:?}");
    }
    let duplicate: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number) VALUES($1,'Duplicate','member',$2,253) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
    let duplicate_path = format!("/api/issues/{duplicate}?workspace_id={ws}");
    let marked = call(
        &app,
        "PUT",
        &duplicate_path,
        token,
        json!({"duplicate_of_issue_id":source}),
    )
    .await;
    assert_eq!(marked.0, StatusCode::OK, "{marked:?}");
    assert_eq!(marked.1["status"], "cancelled");
    assert_eq!(marked.1["duplicate_of"]["id"], json!(source));
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["duplicate_of_issue_id"],
        json!(source)
    );
    let before = db
        .query_one("SELECT revision FROM issue WHERE id=$1", &[&source])
        .await
        .unwrap()
        .get::<_, i64>(0);
    let rejected = call(
        &app,
        "PUT",
        &source_path,
        token,
        json!({"duplicate_of_issue_id":duplicate,"title":"not committed"}),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::CONFLICT, "{rejected:?}");
    assert_eq!(rejected.1["code"], "duplicate_target_is_duplicate");
    assert_eq!(
        db.query_one("SELECT revision FROM issue WHERE id=$1", &[&source])
            .await
            .unwrap()
            .get::<_, i64>(0),
        before
    );
    let rejected = call(
        &app,
        "PUT",
        &source_path,
        token,
        json!({"duplicate_of_issue_id":target}),
    )
    .await;
    assert_eq!(rejected.0, StatusCode::CONFLICT, "{rejected:?}");
    assert_eq!(rejected.1["code"], "issue_has_duplicates");
    let unmarked = call(
        &app,
        "PUT",
        &duplicate_path,
        token,
        json!({"status":"todo"}),
    )
    .await;
    assert_eq!(unmarked.0, StatusCode::OK, "{unmarked:?}");
    assert!(unmarked.1["duplicate_of"].is_null());
    assert_eq!(
        sink.updates.lock().unwrap().last().unwrap().payload["prev_duplicate_of_issue_id"],
        json!(source)
    );
    assert_eq!(
        db.query_one(
            "SELECT duplicate_of_issue_id FROM issue WHERE id=$1",
            &[&duplicate]
        )
        .await
        .unwrap()
        .get::<_, Option<Uuid>>(0),
        None
    );
    let foreign_mark = call(
        &app,
        "PUT",
        &duplicate_path,
        token,
        json!({"duplicate_of_issue_id":foreign}),
    )
    .await;
    assert_eq!(foreign_mark.0, StatusCode::BAD_REQUEST, "{foreign_mark:?}");
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&duplicate])
            .await
            .unwrap()
            .get::<_, String>(0),
        "todo"
    );

    let archived = format!("archived_{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color,archived_at) VALUES($1,$2,'Archived','started','#112233',now())", &[&ws,&archived]).await.unwrap();
    assert_eq!(
        call(&app, "PUT", &source_path, token, json!({"status":archived}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );

    let batch_status = call(
        &app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[duplicate,foreign,"bad",target],"updates":{"status":"in_review"}}),
    )
    .await;
    assert_eq!(
        batch_status,
        (StatusCode::OK, json!({"updated":2})),
        "{batch_status:?}"
    );
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&duplicate])
            .await
            .unwrap()
            .get::<_, String>(0),
        "in_review"
    );
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&target])
            .await
            .unwrap()
            .get::<_, String>(0),
        "in_review"
    );
    let stale = call(
        &app,
        "POST",
        &batch,
        token,
        json!({"issue_ids":[source],"updates":{"status":archived}}),
    )
    .await;
    assert_eq!(stale.0, StatusCode::BAD_REQUEST, "{stale:?}");
    let foreign_status = db
        .query_one(
            "SELECT status FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&foreign, &foreign_ws],
        )
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(foreign_status, "backlog");

    let dispatched: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,assignee_type,assignee_id) VALUES($1,'Dispatched','member',$2,254,'agent',$3) RETURNING id", &[&ws,&user,&agent]).await.unwrap().get(0);
    let fail_closed = call(
        &app,
        "PUT",
        &format!("/api/issues/{dispatched}?workspace_id={ws}"),
        token,
        json!({"status":"done","title":"must not commit"}),
    )
    .await;
    assert_eq!(
        fail_closed.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{fail_closed:?}"
    );
    let batch_gate = call(&app,"POST",&batch,token,json!({"issue_ids":[source,dispatched],"updates":{"status":"done","title":"must not commit"}})).await;
    assert_eq!(
        batch_gate.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "{batch_gate:?}"
    );
    assert_eq!(
        db.query_one("SELECT title FROM issue WHERE id=$1", &[&source])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Status source"
    );
    assert!(sink.cancellations.lock().unwrap().is_empty());

    // Closing owns only the cleanup transaction, never wakeup definition APIs.
    let rule = Uuid::new_v4();
    let system = Uuid::new_v4();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode) VALUES($1,$2,$3,$4,$5,'Synthetic','event','continuous')", &[&rule,&ws,&duplicate,&agent,&user]).await.unwrap();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,system_rule) VALUES($1,$2,$3,$4,$5,'Synthetic system','event','continuous','child_done')", &[&system,&ws,&duplicate,&agent,&user]).await.unwrap();
    let mut tasks = Vec::new();
    for (status, started, wakeup) in [
        ("queued", false, true),
        ("deferred", false, true),
        ("queued", true, true),
        ("running", true, true),
        ("dispatched", false, true),
        ("queued", false, false),
    ] {
        let context = if wakeup {
            json!({"wakeup_id":Uuid::new_v4()})
        } else {
            json!({})
        };
        let id: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,issue_id,status,context,started_at,runtime_id) VALUES($1,$2,$3,$4,CASE WHEN $5 THEN now() ELSE NULL END,(SELECT runtime_id FROM agent WHERE id=$1)) RETURNING id", &[&agent,&duplicate,&status,&context,&started]).await.unwrap().get(0);
        tasks.push((id, status));
    }
    let fail = format!("fail_status_cancel_{}", Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE FUNCTION {fail}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id='{}'::uuid AND NEW.status='cancelled' THEN RAISE EXCEPTION 'synthetic close rollback'; END IF; RETURN NEW; END $$; CREATE TRIGGER {fail} BEFORE UPDATE ON agent_task_queue FOR EACH ROW EXECUTE FUNCTION {fail}();",tasks[0].0)).await.unwrap();
    let updates = sink.updates.lock().unwrap().len();
    let rejected = call(
        &app,
        "PUT",
        &duplicate_path,
        token,
        json!({"status":"done","title":"rollback close"}),
    )
    .await;
    assert_eq!(
        rejected.0,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{rejected:?}"
    );
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&duplicate])
            .await
            .unwrap()
            .get::<_, String>(0),
        "in_review"
    );
    assert!(
        db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(sink.cancellations.lock().unwrap().is_empty());
    assert_eq!(sink.updates.lock().unwrap().len(), updates);
    db.batch_execute(&format!(
        "DROP TRIGGER {fail} ON agent_task_queue; DROP FUNCTION {fail}();"
    ))
    .await
    .unwrap();
    let order = sink.order.lock().unwrap().len();
    let closed = call(
        &app,
        "PUT",
        &duplicate_path,
        token,
        json!({"status":custom}),
    )
    .await;
    assert_eq!(closed.0, StatusCode::OK, "{closed:?}");
    assert!(
        !db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&system])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    for (index, (id, original)) in tasks.iter().enumerate() {
        let row = db
            .query_one(
                "SELECT status,error FROM agent_task_queue WHERE id=$1",
                &[id],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, String>(0),
            if index < 2 { "cancelled" } else { original }
        );
        if index < 2 {
            assert_eq!(
                row.get::<_, Option<String>>(1).as_deref(),
                Some("Issue closed; wakeup disabled")
            );
        }
    }
    assert_eq!(sink.cancellations.lock().unwrap().len(), 2);
    assert!(
        sink.cancellations
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.workspace_id == ws && e.issue_id == Some(duplicate) && e.agent_id == agent)
    );
    assert_eq!(
        &sink.order.lock().unwrap()[order..],
        &["task:cancelled", "task:cancelled", "issue:updated"]
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &duplicate_path,
            token,
            json!({"status":"todo"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(
        !db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    // Sorted pair locks prohibit inverse duplicate marks, without deadlock.
    let a = call(
        &app,
        "PUT",
        &source_path,
        token,
        json!({"duplicate_of_issue_id":target}),
    );
    let target_path = format!("/api/issues/{target}?workspace_id={ws}");
    let b = call(
        &app,
        "PUT",
        &target_path,
        token,
        json!({"duplicate_of_issue_id":source}),
    );
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(a, b)
    })
    .await
    .unwrap();
    let mut codes = vec![a.0, b.0];
    codes.sort();
    assert_eq!(
        codes,
        vec![StatusCode::OK, StatusCode::CONFLICT],
        "{a:?} {b:?}"
    );

    // Archive commits after preflight but before the shared catalog lock.
    let race = format!("race_{}", &Uuid::new_v4().simple().to_string()[..8]);
    db.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,$2,'Race','started','#112233')", &[&ws,&race]).await.unwrap();
    let mut locker = pool.get().await.unwrap();
    let tx = locker.transaction().await.unwrap();
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .unwrap();
    let app_race = app.clone();
    let path_race = duplicate_path.clone();
    let token_race = token.to_owned();
    let key_race = race.clone();
    let request = tokio::spawn(async move {
        call(
            &app_race,
            "PUT",
            &path_race,
            &token_race,
            json!({"status":key_race}),
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..200 {
        waiting = db.query_one("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock_shared%')", &[]).await.unwrap().get(0);
        if waiting {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(waiting, "writer must reach the catalog guard");
    tx.execute(
        "UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key=$2",
        &[&ws, &race],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let raced = tokio::time::timeout(std::time::Duration::from_secs(10), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raced.0, StatusCode::CONFLICT, "{raced:?}");
    assert_eq!(
        db.query_one("SELECT status FROM issue WHERE id=$1", &[&duplicate])
            .await
            .unwrap()
            .get::<_, String>(0),
        "todo"
    );
}
