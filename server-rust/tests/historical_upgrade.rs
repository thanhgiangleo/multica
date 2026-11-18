//! Real staged-schema upgrade from 102, 197 and 430 with synthetic history.
//! Run only against a database created for this test; this test leaves its
//! database in place for independent inspection and explicit owned cleanup.
use deadpool_postgres::{Manager, Pool};
use multica_server::migrate::{self, Direction, Stream};
use std::{collections::HashMap, path::Path, str::FromStr};
use tokio_postgres::{Config, NoTls, config::Host};

fn stage(root: &Path, limit: u32) -> std::path::PathBuf {
    let dir = root.join(format!("stage_{limit}"));
    std::fs::create_dir_all(&dir).unwrap();
    for file in std::fs::read_dir(root.join("migrations")).unwrap() {
        let path = file.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if name.ends_with(".up.sql")
            && name.split('_').next().unwrap().parse::<u32>().unwrap() <= limit
        {
            std::fs::copy(&path, dir.join(name)).unwrap();
        }
    }
    dir
}

#[tokio::test]
#[ignore = "requires empty, run-owned multica_zbsi62_upgrade disposable database"]
async fn actual_historical_upgrade_with_data() {
    let url = std::env::var("RUST_UPGRADE_DATABASE_URL").expect("explicit owned DB URL");
    let config = Config::from_str(&url).unwrap();
    assert_eq!(config.get_dbname(), Some("multica_zbsi62_upgrade"));
    assert!(
        config
            .get_hosts()
            .iter()
            .all(|h| matches!(h, Host::Tcp(host) if host == "127.0.0.1"))
    );
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(8)
        .build()
        .unwrap();
    let conn = pool.get().await.unwrap();
    assert!(
        conn.query_one("SELECT to_regclass('schema_migrations') IS NULL", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    drop(conn);
    let hooks = HashMap::new();
    for limit in [102, 190, 197, 430] {
        let staged = stage(&root, limit);
        let stream = Stream {
            directory: &staged,
            table: "schema_migrations",
            lock_key: 0,
            hooks: &hooks,
            production_gates: true,
        };
        let report = migrate::run_stream(&pool, &stream, Direction::Up)
            .await
            .unwrap();
        assert!(report.applied > 0);
        std::fs::remove_dir_all(staged).unwrap();
        let conn = pool.get().await.unwrap();
        if limit == 102 {
            conn.batch_execute("INSERT INTO \"user\"(id,name,email) VALUES ('00000000-0000-4000-8000-000000000001','upgrade-user','upgrade@example.invalid'); INSERT INTO workspace(name,slug) VALUES ('rust-upgrade','rust-upgrade'); INSERT INTO agent_runtime(workspace_id,daemon_id,name,runtime_mode,provider,status,device_info,metadata,last_seen_at) SELECT id,NULL,'upgrade-runtime','cloud','p','online','{}','{}',now() FROM workspace WHERE slug='rust-upgrade'; INSERT INTO agent(workspace_id,name,description,runtime_mode,runtime_config,runtime_id,visibility,max_concurrent_tasks) SELECT workspace.id,'upgrade-agent','','cloud','{}',agent_runtime.id,'workspace',1 FROM workspace JOIN agent_runtime ON agent_runtime.workspace_id=workspace.id WHERE workspace.slug='rust-upgrade'; INSERT INTO agent_task_queue(agent_id,runtime_id,status) SELECT agent.id,agent_runtime.id,'queued' FROM agent JOIN agent_runtime ON agent.runtime_id=agent_runtime.id WHERE agent.name='upgrade-agent'; INSERT INTO task_usage(task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,created_at) SELECT id,'openai','gpt-test',10,20,0,0,now()-INTERVAL '2 hours' FROM agent_task_queue").await.unwrap();
            let guard = std::fs::read_to_string(
                root.join("migrations/103_drop_legacy_daily_rollups.up.sql"),
            )
            .unwrap();
            assert!(
                conn.batch_execute(&guard).await.is_err(),
                "103 must fail before historical backfill"
            );
        } else if limit == 190 {
            conn.batch_execute("UPDATE agent_task_queue SET originator_user_id='00000000-0000-4000-8000-000000000001'::uuid, accountable_user_id=NULL, originator_source=NULL").await.unwrap();
        } else if limit == 197 {
            let guard =
                std::fs::read_to_string(root.join(
                    "migrations/198_agent_task_attribution_strict_constraint_validate.up.sql",
                ))
                .unwrap();
            assert!(
                conn.batch_execute(&guard).await.is_err(),
                "198 must fail before historical reconciliation"
            );
        } else if limit == 430 {
            conn.batch_execute("INSERT INTO chat_session (workspace_id, creator_id, agent_id) SELECT workspace_id, '00000000-0000-4000-8000-000000000001'::uuid, id FROM agent WHERE name='upgrade-agent'").await.unwrap();
            let rows: i64 = conn
                .query_one(
                    "SELECT count(*) FROM chat_session WHERE explicitly_created_at IS NULL",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert!(rows > 0);
        }
    }
    let upstream = Stream {
        directory: &root.join("migrations"),
        table: "schema_migrations",
        lock_key: 0,
        hooks: &hooks,
        production_gates: true,
    };
    let zbs = Stream {
        directory: &root.join("migrations_zbs"),
        table: "schema_migrations_zbs",
        lock_key: 0,
        hooks: &hooks,
        production_gates: true,
    };
    let (a, b) = migrate::run_both(&pool, &upstream, &zbs, Direction::Up)
        .await
        .unwrap();
    assert!(a.applied > 0);
    assert_eq!(b.applied, 44);
    let conn = pool.get().await.unwrap();
    let hourly: i64 = conn
        .query_one("SELECT count(*) FROM task_usage_hourly", &[])
        .await
        .unwrap()
        .get(0);
    assert!(hourly > 0);
    let accountable: bool = conn.query_one("SELECT bool_and(accountable_user_id=originator_user_id) FROM agent_task_queue WHERE originator_user_id IS NOT NULL", &[]).await.unwrap().get(0);
    assert!(accountable);
    let explicit: i64 = conn
        .query_one(
            "SELECT count(*) FROM chat_session WHERE explicitly_created_at=created_at",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(explicit > 0);
    let (a, b) = migrate::run_both(&pool, &upstream, &zbs, Direction::Up)
        .await
        .unwrap();
    assert_eq!(
        (a.applied, b.applied, a.skipped, b.skipped),
        (0, 0, 619, 44)
    );
}
