//! Durable wakeup scheduler contract on an owned, disposable PostgreSQL 17 database.
use chrono::Utc;
use deadpool_postgres::{Manager as PgManager, Pool};
use multica_server::{
    migrate::{self, Direction, Stream},
    scheduler::{self, Lease, Manager},
};
use serde_json::{Value, json};
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};
use tokio_postgres::{Config, NoTls, config::Host};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires run-owned PostgreSQL 17 multica_wakeup_scheduler_contract; run tests/issue_wakeup_scheduler_verify.sh"]
async fn due_wakeup_dispatches_once_across_replayed_plans() {
    let url = std::env::var("RUST_API_TEST_DATABASE_URL").expect("run-owned database URL");
    let config = Config::from_str(&url).unwrap();
    assert_eq!(
        config.get_dbname(),
        Some("multica_wakeup_scheduler_contract")
    );
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, Host::Tcp(host) if host == "127.0.0.1"))
    );
    assert!(config.get_ports().iter().all(|port| *port != 5432));
    let pool = Arc::new(
        Pool::builder(PgManager::new(config, NoTls))
            .max_size(8)
            .build()
            .unwrap(),
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let hooks: Value =
        serde_json::from_str(include_str!("../baseline/concurrent-index-hooks.json")).unwrap();
    let up: HashMap<String, String> = serde_json::from_value(hooks["upstream_up"].clone()).unwrap();
    let zbs: HashMap<String, String> = serde_json::from_value(hooks["zbs_up"].clone()).unwrap();
    migrate::run_both(
        &pool,
        &Stream {
            directory: &root.join("migrations"),
            table: "schema_migrations",
            lock_key: 7601,
            hooks: &up,
            production_gates: true,
        },
        &Stream {
            directory: &root.join("migrations_zbs"),
            table: "schema_migrations_zbs",
            lock_key: 7602,
            hooks: &zbs,
            production_gates: true,
        },
        Direction::Up,
    )
    .await
    .unwrap();
    let db = pool.get().await.unwrap();
    let owner: Uuid = db
        .query_one(
            "INSERT INTO \"user\"(name,email) VALUES('Wakeup owner',$1) RETURNING id",
            &[&format!("wake-{}@invalid.example", Uuid::new_v4())],
        )
        .await
        .unwrap()
        .get(0);
    let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES('Wakeup isolated',$1,'WAK') RETURNING id", &[&format!("wake-{}",Uuid::new_v4())]).await.unwrap().get(0);
    db.execute(
        "INSERT INTO member(workspace_id,user_id,role) VALUES($1,$2,'owner')",
        &[&ws, &owner],
    )
    .await
    .unwrap();
    let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES($1,'Synthetic wakeup','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
    let agent: Uuid = db.query_one("INSERT INTO agent(workspace_id,name,runtime_mode,owner_id,runtime_id,permission_mode) VALUES($1,'Synthetic agent','cloud',$2,$3,'private') RETURNING id", &[&ws,&owner,&runtime]).await.unwrap().get(0);
    let issue: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id,number,status) VALUES($1,'Due issue','member',$2,1,'todo') RETURNING id", &[&ws,&owner]).await.unwrap().get(0);
    let wakeup = Uuid::now_v7();
    db.execute("INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,next_fire_at) VALUES($1,$2,$3,$4,$5,'Check this issue','at','once',now()-interval '1 minute')", &[&wakeup,&ws,&issue,&agent,&owner]).await.unwrap();
    drop(db);

    let mut first = Manager::new(pool.clone());
    first.register(scheduler::issue_wakeup_job()).unwrap();
    first.run_once().await.unwrap();
    let db = pool.get().await.unwrap();
    let task = match db
        .query_opt(
            "SELECT id,status,context,handoff_note,originator_user_id,workflow_step FROM agent_task_queue WHERE context->>'wakeup_id'=$1",
            &[&wakeup.to_string()],
        )
        .await
        .unwrap()
    {
        Some(task) => task,
        None => {
            let error: Option<String> = db
                .query_one("SELECT last_error FROM issue_wakeup WHERE id=$1", &[&wakeup])
                .await
                .map(|row| row.get(0))
                .unwrap_or_default();
            panic!("wakeup dispatch produced no task; last_error={error:?}")
        }
    };
    let task_id: Uuid = task.get("id");
    assert_eq!(task.get::<_, String>("status"), "queued");
    assert_eq!(task.get::<_, Uuid>("originator_user_id"), owner);
    assert_eq!(task.get::<_, String>("workflow_step"), "todo");
    assert!(
        task.get::<_, String>("handoff_note")
            .contains("Check this issue")
    );
    let context: Value = task.get("context");
    assert_eq!(context["wakeup_revision"], json!(1));
    let receipt = db.query_one("SELECT task_id,processed_at FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='time.due'", &[&wakeup]).await.unwrap();
    assert_eq!(receipt.get::<_, Option<Uuid>>("task_id"), Some(task_id));
    assert!(
        receipt
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("processed_at")
            .is_some()
    );
    let rule = db
        .query_one(
            "SELECT enabled,next_fire_at,last_task_id,fire_count FROM issue_wakeup WHERE id=$1",
            &[&wakeup],
        )
        .await
        .unwrap();
    assert!(!rule.get::<_, bool>("enabled"));
    assert!(
        rule.get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_fire_at")
            .is_none()
    );
    assert_eq!(rule.get::<_, Option<Uuid>>("last_task_id"), Some(task_id));
    assert_eq!(rule.get::<_, i32>("fire_count"), 1);
    drop(db);

    // Replay the dispatcher itself as well as the scheduler plan: neither may enqueue again.
    let job = scheduler::issue_wakeup_job();
    job.handler
        .handle(
            &pool,
            Utc::now(),
            Lease {
                id: Uuid::new_v4(),
                token: Uuid::new_v4(),
                attempt: 1,
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let mut replay = Manager::new(pool.clone());
    replay.register(scheduler::issue_wakeup_job()).unwrap();
    replay.run_once().await.unwrap();
    let db = pool.get().await.unwrap();
    let tasks: i64 = db
        .query_one(
            "SELECT count(*) FROM agent_task_queue WHERE context->>'wakeup_id'=$1",
            &[&wakeup.to_string()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(tasks, 1);
    let receipts: i64 = db
        .query_one(
            "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&wakeup],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(receipts, 1);
}
