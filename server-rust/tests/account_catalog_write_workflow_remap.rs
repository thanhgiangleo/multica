//! Transactional remap boundary; HTTP mapped writes stay closed until event integration.
use deadpool_postgres::{Manager, Pool};
use multica_server::api::account_catalog_write_workflow_remap::remap_issues;
use serde_json::{Value, json};
use std::str::FromStr;
use tokio_postgres::{Config, NoTls};
use uuid::Uuid;

async fn pool() -> Pool {
    let cfg = Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
    assert_eq!(cfg.get_dbname(), Some("multica_zbsi65"));
    assert!(
        cfg.get_hosts()
            .iter()
            .all(|h| matches!(h, tokio_postgres::config::Host::Tcp(s) if s == "127.0.0.1"))
    );
    Pool::builder(Manager::new(cfg, NoTls))
        .max_size(4)
        .build()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn remap_cleans_only_closed_issue_wakeups_and_preserves_assignment() {
    let pool = pool().await;
    let mut db = pool.get().await.unwrap();
    let user = Uuid::new_v4();
    let ws = Uuid::new_v4();
    let foreign = Uuid::new_v4();
    let project = Uuid::new_v4();
    let other_project = Uuid::new_v4();
    let agent = Uuid::new_v4();
    let tx = db.transaction().await.unwrap();
    tx.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES($1,'Remap',$2)",
        &[&user, &format!("remap-{user}@invalid.example")],
    )
    .await
    .unwrap();
    for (workspace, name) in [(ws, "Remap"), (foreign, "Foreign remap")] {
        tx.execute(
            "INSERT INTO workspace(id,name,slug) VALUES($1,$2,$3)",
            &[&workspace, &name, &format!("remap-{workspace}")],
        )
        .await
        .unwrap();
    }
    for id in [project, other_project] {
        tx.execute(
            "INSERT INTO project(id,workspace_id,title) VALUES($1,$2,'Synthetic')",
            &[&id, &ws],
        )
        .await
        .unwrap();
    }
    tx.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES($1,$2,'Synthetic','cloud',$3)", &[&agent,&ws,&user]).await.unwrap();
    for (workspace, key, category) in [
        (ws, "finished", "done"),
        (ws, "rejected", "closed"),
        (ws, "working", "started"),
    ] {
        tx.execute("INSERT INTO issue_status(workspace_id,key,name,category,color) VALUES($1,$2,$2,$3,'#334455')", &[&workspace,&key,&category]).await.unwrap();
    }
    let mut issues = Vec::new();
    for (number, workspace, project_id, status) in [
        (1i32, ws, project, "in_progress"),
        (2, ws, project, "in_review"),
        (3, ws, project, "todo"),
        (4, ws, other_project, "in_progress"),
        (5, foreign, project, "in_progress"),
    ] {
        let id:Uuid=tx.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number,assignee_type,assignee_id,position,revision,duplicate_of_issue_id) VALUES($1,'Remap','member',$2,$3,$4,$5,'agent',$6,8,7,$7) RETURNING id", &[&workspace,&user,&project_id,&status,&number,&agent,&Uuid::new_v4()]).await.unwrap().get(0);
        issues.push(id);
    }
    // Existing destination column determines rank; no unrelated issue is changed.
    tx.execute("INSERT INTO issue(workspace_id,title,creator_type,creator_id,status,number,position) VALUES($1,'Rank','member',$2,'finished',6,-10)", &[&ws,&user]).await.unwrap();
    let mut wakeups = Vec::new();
    for (issue, system) in [
        (issues[0], false),
        (issues[0], true),
        (issues[1], false),
        (issues[2], false),
        (issues[3], false),
        (issues[4], false),
    ] {
        let id = Uuid::new_v4();
        let system_rule: Option<&str> = system.then_some("child_done");
        tx.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,system_rule) VALUES($1,$2,$3,$4,$5,'Synthetic','event','continuous',$6)", &[&id,&if issue==issues[4] {foreign} else {ws},&issue,&agent,&user,&system_rule]).await.unwrap();
        wakeups.push(id);
    }
    let runtime: Uuid = tx.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let mut tasks = Vec::new();
    for (issue, status, started, context) in [
        (issues[0], "queued", false, json!({"wakeup_id":wakeups[0]})),
        (
            issues[0],
            "deferred",
            false,
            json!({"wakeup_id":wakeups[1]}),
        ),
        (issues[0], "running", true, json!({"wakeup_id":wakeups[0]})),
        (issues[0], "queued", true, json!({"wakeup_id":wakeups[0]})),
        (issues[0], "queued", false, json!({"wakeup_id":null})),
        (issues[0], "queued", false, json!({})),
        (issues[1], "queued", false, json!({"wakeup_id":wakeups[2]})),
        (issues[2], "queued", false, json!({"wakeup_id":wakeups[3]})),
        (issues[3], "queued", false, json!({"wakeup_id":wakeups[4]})),
    ] {
        // Distinct agents keep every pending run valid under the per-thread unique index.
        let task_agent: Uuid = tx.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id) VALUES($1,$2,'cloud',$3) RETURNING id", &[&ws,&format!("Synthetic run {}", tasks.len()),&user]).await.unwrap().get(0);
        let id:Uuid = tx.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,context,started_at) VALUES($1,$2,$3,$4,$5,CASE WHEN $6 THEN now() ELSE NULL END) RETURNING id", &[&task_agent,&runtime,&issue,&status,&context,&started]).await.unwrap().get(0);
        tasks.push(id);
    }
    let mapping = vec![
        ("in_progress".into(), "finished".into()),
        ("in_review".into(), "rejected".into()),
        ("todo".into(), "working".into()),
    ];
    let effects = remap_issues(&tx, ws, &[project], &mapping).await.unwrap();
    assert_eq!(effects.changed.len(), 3);
    let mut cancelled: Vec<Uuid> = effects
        .cancelled_wakeups
        .iter()
        .map(|r| r.get("id"))
        .collect();
    cancelled.sort();
    let mut expected = vec![tasks[0], tasks[1], tasks[6]];
    expected.sort();
    assert_eq!(cancelled, expected);
    for row in &effects.changed {
        assert_eq!(row.get::<_, i64>("revision"), 8);
        assert_eq!(row.get::<_, Option<Uuid>>("duplicate_of_issue_id"), None);
        assert_eq!(row.get::<_, Option<Uuid>>("assignee_id"), Some(agent));
        assert_eq!(
            row.get::<_, Option<String>>("assignee_type").as_deref(),
            Some("agent")
        );
    }
    assert_eq!(effects.changed[0].get::<_, f64>("position"), -11.0);
    for (i, id) in wakeups.iter().enumerate() {
        let row = tx
            .query_one(
                "SELECT enabled,disabled_at IS NOT NULL FROM issue_wakeup WHERE id=$1",
                &[id],
            )
            .await
            .unwrap();
        let disabled = matches!(i, 0 | 2);
        assert_eq!(row.get::<_, bool>(0), !disabled, "wakeup {i}");
        assert_eq!(row.get::<_, bool>(1), disabled, "wakeup {i}");
    }
    for (i, id) in tasks.iter().enumerate() {
        let row = tx
            .query_one(
                "SELECT status,error,completed_at IS NOT NULL FROM agent_task_queue WHERE id=$1",
                &[id],
            )
            .await
            .unwrap();
        let stopped = matches!(i, 0 | 1 | 6);
        assert_eq!(
            row.get::<_, String>(0),
            if stopped {
                "cancelled"
            } else if i == 2 {
                "running"
            } else {
                "queued"
            },
            "task {i}"
        );
        assert_eq!(row.get::<_, bool>(2), stopped);
        if stopped {
            assert_eq!(
                row.get::<_, Option<String>>(1).as_deref(),
                Some("Issue closed; wakeup disabled")
            );
        }
    }
    for id in &issues[3..] {
        let row = tx
            .query_one("SELECT status,revision FROM issue WHERE id=$1", &[id])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "in_progress");
        assert_eq!(row.get::<_, i64>(1), 7);
    }
    // Replaying the census mapping finds nothing; same-status mappings never increment revision.
    let replay = remap_issues(&tx, ws, &[project], &mapping).await.unwrap();
    assert!(replay.changed.is_empty());
    assert!(replay.cancelled_wakeups.is_empty());
    assert!(
        remap_issues(&tx, ws, &[project], &[("working".into(), "working".into())])
            .await
            .unwrap()
            .changed
            .is_empty()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires migrated, owned loopback multica_zbsi65 database"]
async fn remap_cleanup_failure_rolls_back_caller_project_and_every_issue() {
    let pool = pool().await;
    let mut db = pool.get().await.unwrap();
    let ws = Uuid::new_v4();
    let project = Uuid::new_v4();
    let user = Uuid::new_v4();
    let agent = Uuid::new_v4();
    db.execute(
        "INSERT INTO \"user\"(id,name,email) VALUES($1,'Rollback',$2)",
        &[&user, &format!("rollback-{user}@invalid.example")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO workspace(id,name,slug) VALUES($1,'Rollback',$2)",
        &[&ws, &format!("rollback-{ws}")],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO project(id,workspace_id,title) VALUES($1,$2,'Before')",
        &[&project, &ws],
    )
    .await
    .unwrap();
    db.execute("INSERT INTO agent(id,workspace_id,name,runtime_mode,owner_id) VALUES($1,$2,'Synthetic','cloud',$3)", &[&agent,&ws,&user]).await.unwrap();
    let issue:Uuid=db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,project_id,status,number) VALUES($1,'Rollback','member',$2,$3,'in_progress',1) RETURNING id", &[&ws,&user,&project]).await.unwrap().get(0);
    let rule = Uuid::new_v4();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode) VALUES($1,$2,$3,$4,$5,'Synthetic','event','continuous')", &[&rule,&ws,&issue,&agent,&user]).await.unwrap();
    let before: Value = db
        .query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
        .await
        .unwrap()
        .get(0);
    let tx = db.transaction().await.unwrap();
    // Fault lives only in this transaction, including its trigger and function.
    tx.batch_execute("CREATE FUNCTION pg_temp.zbsi65_fail_remap_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic wakeup cleanup failure'; END $$; CREATE TRIGGER zbsi65_fail_remap_cleanup BEFORE UPDATE ON issue_wakeup FOR EACH ROW EXECUTE FUNCTION pg_temp.zbsi65_fail_remap_cleanup();").await.unwrap();
    tx.execute("UPDATE project SET title='After' WHERE id=$1", &[&project])
        .await
        .unwrap();
    let error = remap_issues(
        &tx,
        ws,
        &[project],
        &[("in_progress".into(), "done".into())],
    )
    .await
    .err()
    .expect("cleanup error must propagate");
    assert!(
        error
            .as_db_error()
            .unwrap()
            .message()
            .contains("synthetic wakeup cleanup failure")
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        db.query_one("SELECT to_jsonb(i) FROM issue i WHERE id=$1", &[&issue])
            .await
            .unwrap()
            .get::<_, Value>(0),
        before
    );
    assert_eq!(
        db.query_one("SELECT title FROM project WHERE id=$1", &[&project])
            .await
            .unwrap()
            .get::<_, String>(0),
        "Before"
    );
    assert!(
        db.query_one("SELECT enabled FROM issue_wakeup WHERE id=$1", &[&rule])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    // A failed transaction did not leave the synthetic trigger installed.
    let tx = db.transaction().await.unwrap();
    let effects = remap_issues(
        &tx,
        ws,
        &[project],
        &[("in_progress".into(), "done".into())],
    )
    .await
    .unwrap();
    assert_eq!(effects.changed.len(), 1);
    tx.rollback().await.unwrap();
}
