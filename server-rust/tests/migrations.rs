use deadpool_postgres::{Manager, Pool};
use multica_server::migrate::{self, Direction, Stream};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio_postgres::{Config, NoTls};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    pool: Pool,
    schema: String,
    root: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let url =
            std::env::var("RUST_TEST_DATABASE_URL").expect("test-only disposable DB URL required");
        let mut config = Config::from_str(&url).unwrap();
        assert_eq!(
            config.get_dbname(),
            Some("multica_zbsi62"),
            "refuse unexpected DB"
        );
        assert!(
            config.get_hosts().iter().all(
                |host| matches!(host, tokio_postgres::config::Host::Tcp(h) if h == "127.0.0.1")
            ),
            "refuse non-loopback DB"
        );
        let schema = format!(
            "rust_migration_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let admin = Pool::builder(Manager::new(config.clone(), NoTls))
            .max_size(4)
            .build()
            .unwrap();
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
        let root = std::env::temp_dir().join(format!("rust-migration-{}", schema));
        std::fs::create_dir_all(root.join("migrations")).unwrap();
        std::fs::create_dir_all(root.join("migrations_zbs")).unwrap();
        Self { pool, schema, root }
    }
    fn file(&self, stream: &str, name: &str, sql: &str) {
        std::fs::write(self.root.join(stream).join(name), sql).unwrap();
    }
    async fn sql_bool(&self, sql: &str) -> bool {
        self.pool
            .get()
            .await
            .unwrap()
            .query_one(sql, &[])
            .await
            .unwrap()
            .get(0)
    }
    async fn sql_i64(&self, sql: &str) -> i64 {
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
        let url = std::env::var("RUST_TEST_DATABASE_URL").unwrap();
        let config = Config::from_str(&url).unwrap();
        let admin = Pool::builder(Manager::new(config, NoTls))
            .max_size(2)
            .build()
            .unwrap();
        admin
            .get()
            .await
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn fresh_both_streams_idempotent_then_down() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "001_base.up.sql",
        "CREATE TABLE item (id int primary key, note text)",
    );
    f.file(
        "migrations",
        "002_idx.up.sql",
        "CREATE INDEX CONCURRENTLY item_note_idx ON item(note)",
    );
    f.file("migrations", "001_base.down.sql", "DROP TABLE item");
    f.file(
        "migrations",
        "002_idx.down.sql",
        "DROP INDEX CONCURRENTLY item_note_idx",
    );
    f.file(
        "migrations_zbs",
        "001_ext.up.sql",
        "ALTER TABLE item ADD COLUMN extra text",
    );
    f.file(
        "migrations_zbs",
        "001_ext.down.sql",
        "ALTER TABLE item DROP COLUMN extra",
    );
    let hooks = HashMap::new();
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47001,
        hooks: &hooks,
        production_gates: false,
    };
    let zbs = Stream {
        directory: &f.root.join("migrations_zbs"),
        table: "schema_migrations_zbs",
        lock_key: 47001,
        hooks: &hooks,
        production_gates: false,
    };
    let (a, b) = migrate::run_both(&f.pool, &up, &zbs, Direction::Up)
        .await
        .unwrap();
    assert_eq!((a.applied, b.applied), (2, 1));
    assert!(f.sql_bool("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='item' AND column_name='extra')").await);
    let (a, b) = migrate::run_both(&f.pool, &up, &zbs, Direction::Up)
        .await
        .unwrap();
    assert_eq!((a.skipped, b.skipped), (2, 1));
    assert_eq!((a.applied, b.applied), (0, 0));
    let (a, b) = migrate::run_both(&f.pool, &up, &zbs, Direction::Down)
        .await
        .unwrap();
    assert_eq!((a.applied, b.applied), (2, 1));
    assert!(!f.sql_bool("SELECT to_regclass('item') IS NOT NULL").await);
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 0);
    assert_eq!(
        f.sql_i64("SELECT count(*) FROM schema_migrations_zbs")
            .await,
        0
    );
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn failed_statement_rolls_back_sql_and_ledger_then_upgrade() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "001_initial.up.sql",
        "CREATE TABLE item (id int)",
    );
    f.file(
        "migrations",
        "002_fail.up.sql",
        "CREATE TABLE should_rollback (id int); SELECT 1/0",
    );
    let hooks = HashMap::new();
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47002,
        hooks: &hooks,
        production_gates: false,
    };
    assert!(
        migrate::run_stream(&f.pool, &up, Direction::Up)
            .await
            .is_err()
    );
    assert!(f.sql_bool("SELECT to_regclass('item') IS NOT NULL").await);
    assert!(
        !f.sql_bool("SELECT to_regclass('should_rollback') IS NOT NULL")
            .await
    );
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 1);
    f.file(
        "migrations",
        "002_fail.up.sql",
        "ALTER TABLE item ADD COLUMN upgraded text",
    );
    let report = migrate::run_stream(&f.pool, &up, Direction::Up)
        .await
        .unwrap();
    assert_eq!((report.applied, report.skipped), (1, 1));
    assert!(f.sql_bool("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='item' AND column_name='upgraded')").await);
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn failed_concurrent_index_is_cleaned_and_retried() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "001_initial.up.sql",
        "CREATE TABLE item (id int); INSERT INTO item VALUES (1),(1)",
    );
    f.file(
        "migrations",
        "002_unique.up.sql",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS item_id_uidx ON item(id)",
    );
    let hooks = HashMap::from([("002_unique".to_owned(), "item_id_uidx".to_owned())]);
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47003,
        hooks: &hooks,
        production_gates: false,
    };
    assert!(
        migrate::run_stream(&f.pool, &up, Direction::Up)
            .await
            .is_err()
    );
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 1);
    assert!(
        !f.sql_bool("SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('item_id_uidx')")
            .await
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("DELETE FROM item WHERE ctid IN (SELECT ctid FROM item LIMIT 1)")
        .await
        .unwrap();
    let report = migrate::run_stream(&f.pool, &up, Direction::Up)
        .await
        .unwrap();
    assert_eq!(report.applied, 1);
    assert!(
        f.sql_bool("SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass('item_id_uidx')")
            .await
    );
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn unconverted_fork_and_broken_hook_fail_closed() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "103_drop_legacy_daily_rollups.up.sql",
        "CREATE TABLE forbidden (id int)",
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE task_usage(id int); INSERT INTO task_usage VALUES (1); CREATE TABLE task_usage_hourly_rollup_state(id int, watermark_at timestamptz); INSERT INTO task_usage_hourly_rollup_state VALUES(1, now())")
        .await
        .unwrap();
    f.file(
        "migrations_zbs",
        "001_ext.up.sql",
        "CREATE TABLE fork (id int)",
    );
    let hooks = HashMap::new();
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47004,
        hooks: &hooks,
        production_gates: true,
    };
    let zbs = Stream {
        directory: &f.root.join("migrations_zbs"),
        table: "schema_migrations_zbs",
        lock_key: 47004,
        hooks: &hooks,
        production_gates: true,
    };
    let error = migrate::run_both(&f.pool, &up, &zbs, Direction::Up)
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("created_at"), "{error:?}");
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 0);
    assert!(
        !f.sql_bool("SELECT to_regclass('forbidden') IS NOT NULL")
            .await
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("INSERT INTO schema_migrations(version) VALUES ('564_ext')")
        .await
        .unwrap();
    let error = migrate::run_both(&f.pool, &up, &zbs, Direction::Up)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unconverted fork versions"));
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn concurrent_runners_share_advisory_lock() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "001_create.up.sql",
        "CREATE TABLE item (id int)",
    );
    let hooks = HashMap::new();
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47005,
        hooks: &hooks,
        production_gates: false,
    };
    let (a, b) = tokio::join!(
        migrate::run_stream(&f.pool, &up, Direction::Up),
        migrate::run_stream(&f.pool, &up, Direction::Up)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!((a.applied + b.applied, a.skipped + b.skipped), (1, 1));
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 1);
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn cleanup_refuses_non_index_relation() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "001_unique.up.sql",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS item_id_uidx ON item(id)",
    );
    f.pool
        .get()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE item_id_uidx (id int); CREATE TABLE item(id int)")
        .await
        .unwrap();
    let hooks = HashMap::from([("001_unique".to_owned(), "item_id_uidx".to_owned())]);
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47006,
        hooks: &hooks,
        production_gates: false,
    };
    let err = migrate::run_stream(&f.pool, &up, Direction::Up)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not an index"));
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 0);
    f.clean().await;
}

#[tokio::test]
#[ignore = "requires owned disposable PostgreSQL RUST_TEST_DATABASE_URL"]
async fn conditional_skip_is_recorded_and_idempotent() {
    let f = Fixture::new().await;
    f.file(
        "migrations",
        "139_issue_description_trgm_index.up.sql",
        "CREATE TABLE must_not_exist(id int)",
    );
    let hooks = HashMap::new();
    let up = Stream {
        directory: &f.root.join("migrations"),
        table: "schema_migrations",
        lock_key: 47007,
        hooks: &hooks,
        production_gates: true,
    };
    let report = migrate::run_stream(&f.pool, &up, Direction::Up)
        .await
        .unwrap();
    assert_eq!((report.applied, report.conditional), (1, 1));
    assert!(
        !f.sql_bool("SELECT to_regclass('must_not_exist') IS NOT NULL")
            .await
    );
    assert_eq!(f.sql_i64("SELECT count(*) FROM schema_migrations").await, 1);
    assert_eq!(
        migrate::run_stream(&f.pool, &up, Direction::Up)
            .await
            .unwrap()
            .skipped,
        1
    );
    f.clean().await;
}
