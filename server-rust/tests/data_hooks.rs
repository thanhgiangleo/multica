use deadpool_postgres::{Manager, Pool};
use multica_server::{
    data_hooks,
    migrate::{self, Direction, Stream},
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio_postgres::{Config, NoTls};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pool: Pool,
    admin: Pool,
    schema: String,
}
impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("RUST_TEST_DATABASE_URL").expect("isolated test DB URL");
        let mut config = Config::from_str(&url).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi62"));
        assert!(
            config.get_hosts().iter().all(
                |h| matches!(h, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
            )
        );
        let admin = Pool::builder(Manager::new(config.clone(), NoTls))
            .max_size(4)
            .build()
            .unwrap();
        let schema = format!(
            "rust_hooks_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        config.options(format!("-c search_path={schema}"));
        let pool = Pool::builder(Manager::new(config, NoTls))
            .max_size(4)
            .build()
            .unwrap();
        Self {
            pool,
            admin,
            schema,
        }
    }
    async fn run(&self, sql: &str) {
        self.pool
            .get()
            .await
            .unwrap()
            .batch_execute(sql)
            .await
            .unwrap();
    }
    async fn count(&self, sql: &str) -> i64 {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(sql, &[])
            .await
            .unwrap()
            .get(0)
    }
    async fn clean(self) {
        drop(self.pool);
        self.admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn attribution_pages_resume_and_never_record_failed_migration() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE agent_task_queue (id int PRIMARY KEY, originator_user_id int, accountable_user_id int, originator_source text); INSERT INTO agent_task_queue SELECT n, n, NULL, NULL FROM generate_series(1, 5001) n; UPDATE agent_task_queue SET accountable_user_id=-1, originator_source='direct_human' WHERE id=1; INSERT INTO agent_task_queue VALUES (5002, NULL, 4, NULL); CREATE TABLE schema_migrations (version text PRIMARY KEY)").await;
    let root = std::env::temp_dir().join(format!("rust-hook-{}", f.schema));
    std::fs::create_dir_all(&root).unwrap();
    let version = "198_agent_task_attribution_strict_constraint_validate";
    std::fs::write(root.join(format!("{version}.up.sql")), "SELECT 1").unwrap();
    let hooks = HashMap::new();
    let stream = Stream {
        directory: &root,
        table: "schema_migrations",
        lock_key: 887712,
        hooks: &hooks,
        production_gates: true,
    };
    f.run("CREATE FUNCTION fail_second_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id=5001 THEN RAISE EXCEPTION 'injected second batch failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_second_batch BEFORE UPDATE ON agent_task_queue FOR EACH ROW EXECUTE FUNCTION fail_second_batch()").await;
    assert!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .is_err()
    );
    assert_eq!(f.count("SELECT count(*) FROM schema_migrations").await, 0);
    assert_eq!(
        f.count(
            "SELECT count(*) FROM agent_task_queue WHERE accountable_user_id = originator_user_id"
        )
        .await,
        5000
    );
    f.run("DROP TRIGGER fail_second_batch ON agent_task_queue")
        .await;
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM agent_task_queue WHERE accountable_user_id = originator_user_id"
        )
        .await,
        5001
    );
    assert_eq!(
        f.count(
            "SELECT count(*) FROM agent_task_queue WHERE id=1 AND originator_source='direct_human'"
        )
        .await,
        1
    );
    assert_eq!(
        f.count("SELECT count(*) FROM agent_task_queue WHERE id=5002 AND accountable_user_id=4")
            .await,
        1
    );
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .unwrap()
            .skipped,
        1
    );
    std::fs::remove_dir_all(root).unwrap();
    f.clean().await;
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn chat_origin_pages_skip_channel_history_and_resume_after_failure() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE chat_session (id uuid PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), explicitly_created_at timestamptz); CREATE TABLE channel_chat_session_binding (chat_session_id uuid); CREATE TABLE chat_message (chat_session_id uuid, channel_ingested boolean); INSERT INTO chat_session(id) SELECT lpad(to_hex(n),32,'0')::uuid FROM generate_series(1,1001) n; INSERT INTO channel_chat_session_binding VALUES (lpad(to_hex(3),32,'0')::uuid); INSERT INTO chat_message VALUES (lpad(to_hex(4),32,'0')::uuid,true); INSERT INTO chat_message VALUES (lpad(to_hex(5),32,'0')::uuid,false)").await;
    f.run("CREATE TABLE schema_migrations(version text PRIMARY KEY)")
        .await;
    let root = std::env::temp_dir().join(format!("rust-chat-{}", f.schema));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("431_chat_explicit_origin_backfill.up.sql"),
        "SELECT 1",
    )
    .unwrap();
    let hooks = HashMap::new();
    let stream = Stream {
        directory: &root,
        table: "schema_migrations",
        lock_key: 772213,
        hooks: &hooks,
        production_gates: true,
    };
    f.run("CREATE FUNCTION fail_last_page() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id=lpad(to_hex(1001),32,'0')::uuid THEN RAISE EXCEPTION 'injected final page failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_last_page BEFORE UPDATE ON chat_session FOR EACH ROW EXECUTE FUNCTION fail_last_page()").await;
    assert!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .is_err()
    );
    assert_eq!(f.count("SELECT count(*) FROM schema_migrations").await, 0);
    assert_eq!(
        f.count("SELECT count(*) FROM chat_session WHERE explicitly_created_at IS NOT NULL")
            .await,
        998
    );
    f.run("DROP TRIGGER fail_last_page ON chat_session").await;
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(
        f.count("SELECT count(*) FROM chat_session WHERE explicitly_created_at = created_at")
            .await,
        999
    );
    data_hooks::chat_origin(&f.pool).await.unwrap();
    assert_eq!(
        f.count("SELECT count(*) FROM chat_session WHERE explicitly_created_at IS NULL")
            .await,
        2
    );
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Up)
            .await
            .unwrap()
            .skipped,
        1
    );
    std::fs::remove_dir_all(root).unwrap();
    f.clean().await;
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn rollup_months_retry_failed_slice_and_stamp_only_after_completion() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE task_usage (created_at timestamptz, updated_at timestamptz); CREATE TABLE task_usage_hourly_rollup_state (id int PRIMARY KEY, watermark_at timestamptz NOT NULL); INSERT INTO task_usage_hourly_rollup_state VALUES(1,'1970-01-01'); INSERT INTO task_usage VALUES ('2024-01-25',NULL),('2024-02-10',NULL); CREATE TABLE rollup_calls(month int PRIMARY KEY); CREATE FUNCTION rollup_task_usage_hourly_window(a timestamptz,b timestamptz) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN IF extract(month FROM a)=2 AND NOT EXISTS(SELECT 1 FROM rollup_calls WHERE month=2) THEN RAISE EXCEPTION 'injected February failure'; END IF; INSERT INTO rollup_calls VALUES(extract(month FROM a)::int) ON CONFLICT DO NOTHING; RETURN 1; END $$").await;
    assert!(data_hooks::task_usage_hourly(&f.pool).await.is_err());
    assert_eq!(f.count("SELECT count(*) FROM rollup_calls").await, 1);
    assert_eq!(
        f.count(
            "SELECT count(*) FROM task_usage_hourly_rollup_state WHERE watermark_at='1970-01-01'"
        )
        .await,
        1
    );
    f.run("CREATE OR REPLACE FUNCTION rollup_task_usage_hourly_window(a timestamptz,b timestamptz) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN INSERT INTO rollup_calls VALUES(extract(month FROM a)::int) ON CONFLICT DO NOTHING; RETURN 1; END $$").await;
    data_hooks::task_usage_hourly(&f.pool).await.unwrap();
    assert_eq!(f.count("SELECT count(*) FROM rollup_calls").await, 2);
    assert_eq!(
        f.count(
            "SELECT count(*) FROM task_usage_hourly_rollup_state WHERE watermark_at > '2024-02-10'"
        )
        .await,
        1
    );
    data_hooks::task_usage_hourly(&f.pool).await.unwrap();
    assert_eq!(f.count("SELECT count(*) FROM rollup_calls").await, 2);
    f.clean().await;
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn rollback_guards_preserve_ledger_until_data_is_removed() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE issue_source_context(id int); CREATE TABLE attachment(source_context_id int); CREATE TABLE issue_source_context_object_intent(id int); INSERT INTO issue_source_context VALUES(1); CREATE TABLE schema_migrations(version text PRIMARY KEY); INSERT INTO schema_migrations VALUES('407_issue_source_context')").await;
    let root = std::env::temp_dir().join(format!("rust-guard-{}", f.schema));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("407_issue_source_context.down.sql"),
        "DROP TABLE issue_source_context",
    )
    .unwrap();
    let hooks = HashMap::new();
    let stream = Stream {
        directory: &root,
        table: "schema_migrations",
        lock_key: 772211,
        hooks: &hooks,
        production_gates: true,
    };
    assert!(
        migrate::run_stream(&f.pool, &stream, Direction::Down)
            .await
            .is_err()
    );
    assert_eq!(f.count("SELECT count(*) FROM schema_migrations").await, 1);
    assert_eq!(
        f.count("SELECT count(*) FROM issue_source_context").await,
        1
    );
    f.run("DELETE FROM issue_source_context").await;
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Down)
            .await
            .unwrap()
            .applied,
        1
    );
    assert_eq!(f.count("SELECT count(*) FROM schema_migrations").await, 0);
    std::fs::remove_dir_all(root).unwrap();
    f.clean().await;
}

#[tokio::test]
#[ignore = "owned disposable PostgreSQL only"]
async fn channel_route_retirement_blocks_rollback() {
    let f = Fixture::new().await;
    f.run("CREATE TABLE chat_session(id int PRIMARY KEY, explicitly_created_at timestamptz); CREATE TABLE channel_chat_session_binding(chat_session_id int,retired_at timestamptz); INSERT INTO chat_session VALUES (1,now()); INSERT INTO channel_chat_session_binding VALUES(1,NULL); CREATE TABLE schema_migrations(version text PRIMARY KEY); INSERT INTO schema_migrations VALUES ('430_channel_outbound_message_binding_index')").await;
    let root = std::env::temp_dir().join(format!("rust-channel-{}", f.schema));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("430_channel_outbound_message_binding_index.down.sql"),
        "SELECT 1",
    )
    .unwrap();
    let hooks = HashMap::new();
    let stream = Stream {
        directory: &root,
        table: "schema_migrations",
        lock_key: 772212,
        hooks: &hooks,
        production_gates: true,
    };
    assert!(
        migrate::run_stream(&f.pool, &stream, Direction::Down)
            .await
            .is_err()
    );
    assert_eq!(f.count("SELECT count(*) FROM schema_migrations").await, 1);
    f.run("DELETE FROM channel_chat_session_binding").await;
    assert_eq!(
        migrate::run_stream(&f.pool, &stream, Direction::Down)
            .await
            .unwrap()
            .applied,
        1
    );
    std::fs::remove_dir_all(root).unwrap();
    f.clean().await;
}
