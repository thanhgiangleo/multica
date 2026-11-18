//! PostgreSQL migration executor with paged historical data hooks.
use deadpool_postgres::Pool;
use std::{
    collections::HashMap,
    error::Error,
    path::{Path, PathBuf},
};

pub type MigrationResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
pub const ADVISORY_LOCK_KEY: i64 = 7244554146635925501;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

impl Direction {
    fn suffix(self) -> &'static str {
        match self {
            Self::Up => ".up.sql",
            Self::Down => ".down.sql",
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct RunReport {
    pub applied: usize,
    pub skipped: usize,
    pub conditional: usize,
}

pub struct Stream<'a> {
    pub directory: &'a Path,
    pub table: &'a str,
    pub lock_key: i64,
    pub hooks: &'a HashMap<String, String>,
    pub production_gates: bool,
}

pub fn files(directory: &Path, direction: Direction) -> MigrationResult<Vec<PathBuf>> {
    let suffix = direction.suffix();
    let mut files = std::fs::read_dir(directory)?
        .map(|item| item.map(|item| item.path()))
        .collect::<Result<Vec<_>, _>>()?;
    files.retain(|path| {
        path.is_file()
            && path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(suffix))
    });
    files.sort();
    if direction == Direction::Down {
        files.reverse();
    }
    Ok(files)
}

fn quote_table(name: &str) -> MigrationResult<String> {
    let parts = name.split('.').collect::<Vec<_>>();
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(format!("invalid migration table: {name}").into());
    }
    Ok(parts
        .iter()
        .map(|p| format!("\"{p}\""))
        .collect::<Vec<_>>()
        .join("."))
}

fn version(path: &Path, direction: Direction) -> MigrationResult<String> {
    let name = path
        .file_name()
        .ok_or("missing migration filename")?
        .to_str()
        .ok_or("non-UTF8 migration filename")?;
    Ok(name
        .strip_suffix(direction.suffix())
        .ok_or("unexpected migration suffix")?
        .to_owned())
}

/// Call while holding no pool connection: hooks may need a separate session.
pub async fn run_stream(
    pool: &Pool,
    stream: &Stream<'_>,
    direction: Direction,
) -> MigrationResult<RunReport> {
    let table = quote_table(stream.table)?;
    let files = files(stream.directory, direction)?;
    if files.is_empty() {
        return Err(format!(
            "no {} migration files in {}",
            direction.suffix(),
            stream.directory.display()
        )
        .into());
    }
    let key = if stream.lock_key == 0 {
        ADVISORY_LOCK_KEY
    } else {
        stream.lock_key
    };
    let mut conn = pool.get().await?;
    conn.query_one("SELECT pg_advisory_lock($1)", &[&key])
        .await?;
    let result = run_locked(pool, &mut conn, stream, &table, direction, &files).await;
    // If the session is disconnected, PostgreSQL releases the session lock itself.
    let unlock = conn
        .query_one("SELECT pg_advisory_unlock($1)", &[&key])
        .await;
    match result {
        Err(err) => Err(err),
        Ok(report) => {
            if !unlock?.get::<_, bool>(0) {
                return Err("migration advisory lock was lost".into());
            }
            Ok(report)
        }
    }
}

async fn run_locked(
    pool: &Pool,
    conn: &mut deadpool_postgres::Object,
    stream: &Stream<'_>,
    table: &str,
    direction: Direction,
    files: &[PathBuf],
) -> MigrationResult<RunReport> {
    conn.batch_execute(&format!("CREATE TABLE IF NOT EXISTS {table} (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")).await?;
    let mut report = RunReport::default();
    for file in files {
        let version = version(file, direction)?;
        let present: bool = conn
            .query_one(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE version = $1)"),
                &[&version],
            )
            .await?
            .get(0);
        if present != (direction == Direction::Down) {
            report.skipped += 1;
            continue;
        }
        let sql = std::fs::read_to_string(file)?;
        if let Some(index) = stream.hooks.get(&version) {
            cleanup_invalid_index(pool, index)
                .await
                .map_err(|err| format!("hook {version}: {err}"))?;
        }
        if stream.production_gates {
            if direction == Direction::Up {
                run_data_hook(pool, &version).await?;
            } else {
                check_rollback_guard(pool, &version).await?;
            }
        }
        let apply = if stream.production_gates {
            check_condition(conn, &version, direction).await?
        } else {
            true
        };
        // CONCURRENTLY cannot run in an explicit transaction. A failed build may
        // leave an INVALID index; the version is never recorded in that case.
        if sql.to_ascii_uppercase().contains("CONCURRENTLY") {
            if apply {
                conn.batch_execute(&sql)
                    .await
                    .map_err(|err| format!("{}: {err}", file.display()))?;
            }
            record(conn, table, &version, direction).await?;
        } else {
            let tx = conn.transaction().await?;
            if apply {
                tx.batch_execute(&sql)
                    .await
                    .map_err(|err| format!("{}: {err}", file.display()))?;
            }
            record(&tx, table, &version, direction).await?;
            tx.commit().await?;
        }
        report.applied += 1;
        if !apply {
            report.conditional += 1;
        }
    }
    Ok(report)
}

async fn record(
    conn: &impl deadpool_postgres::GenericClient,
    table: &str,
    version: &str,
    direction: Direction,
) -> MigrationResult<()> {
    let sql = match direction {
        Direction::Up => format!("INSERT INTO {table} (version) VALUES ($1)"),
        Direction::Down => format!("DELETE FROM {table} WHERE version = $1"),
    };
    conn.execute(&sql, &[&version]).await?;
    Ok(())
}

async fn run_data_hook(pool: &Pool, version: &str) -> MigrationResult<()> {
    match version {
        "103_drop_legacy_daily_rollups" => crate::data_hooks::task_usage_hourly(pool).await,
        "198_agent_task_attribution_strict_constraint_validate" => {
            crate::data_hooks::attribution(pool).await
        }
        "431_chat_explicit_origin_backfill" => crate::data_hooks::chat_origin(pool).await,
        _ => Ok(()),
    }
}

async fn check_rollback_guard(pool: &Pool, version: &str) -> MigrationResult<()> {
    if matches!(
        version,
        "407_issue_source_context"
            | "408_issue_source_context_id_index"
            | "409_issue_source_context_issue_index"
            | "410_issue_source_context_origin_task_index"
            | "411_attachment_source_context_index"
            | "412_issue_source_context_object_intent_key_index"
            | "413_issue_source_context_object_intent_due_index"
            | "414_issue_source_context_object_intent_context_index"
    ) {
        let conn = pool.get().await?;
        let present: bool = conn.query_one("SELECT EXISTS(SELECT 1 FROM issue_source_context) OR EXISTS(SELECT 1 FROM attachment WHERE source_context_id IS NOT NULL) OR EXISTS(SELECT 1 FROM issue_source_context_object_intent)", &[]).await?.get(0);
        if present {
            return Err(format!("{version}: captured source-context data blocks rollback").into());
        }
    }
    if version == "430_channel_outbound_message_binding_index" {
        let conn = pool.get().await?;
        let present: bool = conn.query_one("SELECT EXISTS(SELECT 1 FROM channel_chat_session_binding binding LEFT JOIN chat_session session ON session.id=binding.chat_session_id WHERE binding.retired_at IS NOT NULL OR session.explicitly_created_at IS NOT NULL)", &[]).await?.get(0);
        if present {
            return Err("channel chat route state blocks rollback".into());
        }
    }
    Ok(())
}

async fn cleanup_invalid_index(pool: &Pool, index: &str) -> MigrationResult<()> {
    if !index.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err("invalid hook index name".into());
    }
    let conn = pool.get().await?;
    let row = conn.query_opt("SELECT n.nspname, c.relname, c.relkind = 'i', COALESCE(i.indisvalid,FALSE) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_index i ON i.indexrelid=c.oid WHERE c.oid=to_regclass($1)", &[&index]).await?;
    if let Some(row) = row {
        let is_index: bool = row.get(2);
        let valid: bool = row.get(3);
        if !is_index {
            return Err(format!("{index} exists but is not an index").into());
        }
        if !valid {
            let schema: String = row.get(0);
            let relation: String = row.get(1);
            let quoted = format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                relation.replace('"', "\"\"")
            );
            conn.batch_execute(&format!("DROP INDEX CONCURRENTLY IF EXISTS {quoted}"))
                .await?;
        }
    }
    Ok(())
}

async fn check_condition(
    conn: &deadpool_postgres::Object,
    version: &str,
    direction: Direction,
) -> MigrationResult<bool> {
    if direction == Direction::Up {
        if matches!(
            version,
            "469_issue_status_lifecycle_categories"
                | "139_issue_description_trgm_index"
                | "140_comment_content_trgm_index"
        ) {
            return Ok(false);
        }
        if version == "371_comment_content_search_index_strategy" {
            return index_usable(conn).await;
        }
        if version == "446_issue_properties_bigm_index" {
            return bigm_available(conn).await;
        }
    } else if matches!(
        version,
        "454_drop_comment_content_bigm_index" | "463_drop_issue_description_bigm_index"
    ) {
        return bigm_available(conn).await;
    } else if version == "455_drop_comment_content_trgm_index" {
        return Ok(!bigm_available(conn).await?);
    }
    Ok(true)
}

async fn bigm_available(conn: &deadpool_postgres::Object) -> MigrationResult<bool> {
    Ok(conn.query_one("SELECT EXISTS(SELECT 1 FROM pg_opclass opc JOIN pg_am am ON am.oid=opc.opcmethod JOIN pg_depend dep ON dep.classid='pg_opclass'::regclass AND dep.objid=opc.oid AND dep.refclassid='pg_extension'::regclass AND dep.deptype='e' JOIN pg_extension ext ON ext.oid=dep.refobjid WHERE opc.opcname='gin_bigm_ops' AND am.amname='gin' AND ext.extname='pg_bigm' AND pg_opclass_is_visible(opc.oid))", &[]).await?.get(0))
}

async fn index_usable(conn: &deadpool_postgres::Object) -> MigrationResult<bool> {
    Ok(conn.query_opt("SELECT COALESCE(idx.relkind='i' AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique AND i.indpred IS NULL AND i.indexprs IS NOT NULL AND i.indrelid=to_regclass('public.comment') AND i.indnkeyatts=1 AND i.indnatts=1 AND am.amname='gin' AND opc.opcname='gin_bigm_ops' AND pg_get_indexdef(i.indexrelid,1,FALSE)='lower(content)' AND EXISTS(SELECT 1 FROM pg_depend dep JOIN pg_extension ext ON ext.oid=dep.refobjid WHERE dep.classid='pg_opclass'::regclass AND dep.objid=opc.oid AND dep.refclassid='pg_extension'::regclass AND dep.deptype='e' AND ext.extname='pg_bigm'), FALSE) FROM pg_class idx LEFT JOIN pg_index i ON i.indexrelid=idx.oid LEFT JOIN pg_am am ON am.oid=idx.relam LEFT JOIN pg_opclass opc ON opc.oid=i.indclass[0] WHERE idx.oid=to_regclass('public.idx_comment_content_bigm')", &[]).await?.map(|row| row.get(0)).unwrap_or(false))
}

pub async fn refuse_legacy_fork(
    pool: &Pool,
    upstream: &str,
    zbs_directory: &Path,
) -> MigrationResult<()> {
    let table = quote_table(upstream)?;
    let legacy = files(zbs_directory, Direction::Up)?
        .iter()
        .filter_map(|path| {
            let version = version(path, Direction::Up).ok()?;
            let (number, suffix) = version.split_once('_')?;
            Some(format!("{}_{}", number.parse::<i32>().ok()? + 563, suffix))
        })
        .collect::<Vec<_>>();
    let conn = pool.get().await?;
    let exists: bool = conn
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&upstream])
        .await?
        .get(0);
    if exists {
        let count: i64 = conn
            .query_one(
                &format!("SELECT count(*) FROM {table} WHERE version=ANY($1)"),
                &[&legacy],
            )
            .await?
            .get(0);
        if count > 0 {
            return Err(format!("found {count} unconverted fork versions in {upstream}; run the reviewed legacy conversion procedure first").into());
        }
    }
    Ok(())
}

pub async fn run_both(
    pool: &Pool,
    upstream: &Stream<'_>,
    zbs: &Stream<'_>,
    direction: Direction,
) -> MigrationResult<(RunReport, RunReport)> {
    refuse_legacy_fork(pool, upstream.table, zbs.directory).await?;
    if direction == Direction::Up {
        let first = run_stream(pool, upstream, direction).await?;
        Ok((first, run_stream(pool, zbs, direction).await?))
    } else {
        let second = run_stream(pool, zbs, direction).await?;
        Ok((run_stream(pool, upstream, direction).await?, second))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filenames_sorted_and_down_reversed() {
        let dir = std::env::temp_dir().join(format!("rust-migration-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for name in [
            "002_b.up.sql",
            "001_a.up.sql",
            "002_b.down.sql",
            "001_a.down.sql",
        ] {
            std::fs::write(dir.join(name), "SELECT 1").unwrap();
        }
        assert_eq!(
            files(&dir, Direction::Up)
                .unwrap()
                .iter()
                .map(|p| version(p, Direction::Up).unwrap())
                .collect::<Vec<_>>(),
            ["001_a", "002_b"]
        );
        assert_eq!(
            files(&dir, Direction::Down)
                .unwrap()
                .iter()
                .map(|p| version(p, Direction::Down).unwrap())
                .collect::<Vec<_>>(),
            ["002_b", "001_a"]
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn rejects_unsafe_identifiers() {
        for name in ["", "a..b", "a.b.c", "a;DROP TABLE x", "schema.\"evil"] {
            assert!(quote_table(name).is_err());
        }
    }
}
