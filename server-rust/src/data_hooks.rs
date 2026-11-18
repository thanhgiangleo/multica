//! Idempotent, bounded pre-migration data conversions from the staged Go runner.
use crate::migrate::MigrationResult;
use chrono::{DateTime, Datelike, Months, TimeZone, Utc};
use deadpool_postgres::Pool;

const ATTRIBUTION_BATCH: i64 = 5000;
const CHAT_PAGE: i64 = 1000;
const ROLLUP_LOCK: i64 = 4246;

pub async fn attribution(pool: &Pool) -> MigrationResult<()> {
    let conn = pool.get().await?;
    let mismatches: i64 = conn.query_one(
        "SELECT count(*) FROM agent_task_queue WHERE originator_user_id IS NOT NULL AND accountable_user_id IS NOT NULL AND accountable_user_id <> originator_user_id", &[]
    ).await?.get(0);
    if mismatches > 0 {
        eprintln!(
            "attribution pre-198: normalizing {mismatches} conflicting accountable users to their originators"
        );
    }
    loop {
        // The row lock and repeated predicate protect a concurrent writer which
        // clears originator_user_id while the migration waits on its row lock.
        let touched = conn.execute("WITH batch AS (SELECT id FROM agent_task_queue WHERE originator_user_id IS NOT NULL AND accountable_user_id IS DISTINCT FROM originator_user_id ORDER BY id LIMIT $1 FOR UPDATE) UPDATE agent_task_queue q SET accountable_user_id = q.originator_user_id, originator_source = COALESCE(q.originator_source, 'backfill') FROM batch WHERE q.id = batch.id AND q.originator_user_id IS NOT NULL AND q.accountable_user_id IS DISTINCT FROM q.originator_user_id", &[&ATTRIBUTION_BATCH]).await?;
        if touched == 0 {
            break;
        }
    }
    Ok(())
}

pub async fn chat_origin(pool: &Pool) -> MigrationResult<()> {
    let conn = pool.get().await?;
    let mut cursor: Option<String> = None;
    loop {
        // Walk ALL primary keys, not just rows to update: channel-originated
        // sessions must advance the cursor too. Each page autocommits.
        let row = conn.query_one("WITH page AS MATERIALIZED (SELECT id FROM chat_session WHERE ($1::text IS NULL OR id > $1::text::uuid) ORDER BY id LIMIT $2), updated AS (UPDATE chat_session AS session SET explicitly_created_at = session.created_at FROM page WHERE session.id = page.id AND session.explicitly_created_at IS NULL AND NOT EXISTS (SELECT 1 FROM channel_chat_session_binding AS binding WHERE binding.chat_session_id = session.id) AND NOT EXISTS (SELECT 1 FROM chat_message AS message WHERE message.chat_session_id = session.id AND message.channel_ingested) RETURNING session.id) SELECT COALESCE((SELECT id::text FROM page ORDER BY id DESC LIMIT 1), ''), (SELECT count(*) FROM page), (SELECT count(*) FROM updated)", &[&cursor, &CHAT_PAGE]).await?;
        let visited: i64 = row.get(1);
        if visited == 0 {
            break;
        }
        cursor = Some(row.get(0));
    }
    Ok(())
}

fn month_floor(t: DateTime<Utc>) -> MigrationResult<DateTime<Utc>> {
    Ok(Utc
        .with_ymd_and_hms(t.year(), t.month(), 1, 0, 0, 0)
        .single()
        .ok_or("invalid usage month")?)
}

async fn stamp_watermark(conn: &deadpool_postgres::Object) -> MigrationResult<()> {
    let updated = conn.execute("UPDATE task_usage_hourly_rollup_state SET watermark_at=now()-INTERVAL '5 minutes' WHERE id=1", &[]).await?;
    if updated != 1 {
        return Err("task_usage_hourly_rollup_state watermark row is missing".into());
    }
    Ok(())
}

pub async fn task_usage_hourly(pool: &Pool) -> MigrationResult<()> {
    let conn = pool.get().await?;
    let state_exists: bool = conn.query_one("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='task_usage_hourly_rollup_state')", &[]).await?.get(0);
    if !state_exists {
        return Ok(());
    }
    let range = conn
        .query_one(
            "SELECT MIN(created_at), MAX(COALESCE(updated_at,created_at)) FROM task_usage",
            &[],
        )
        .await?;
    let first: Option<DateTime<Utc>> = range.get(0);
    let last: Option<DateTime<Utc>> = range.get(1);
    if first.is_none() {
        return stamp_watermark(&conn).await;
    }
    let first = first.ok_or("task_usage min timestamp missing")?;
    let last = last.ok_or("task_usage max timestamp missing")?;
    let watermark: Option<DateTime<Utc>> = conn
        .query_opt(
            "SELECT watermark_at FROM task_usage_hourly_rollup_state WHERE id=1",
            &[],
        )
        .await?
        .and_then(|row| row.get(0));
    let watermark =
        watermark.ok_or("task_usage_hourly_rollup_state watermark row is missing or NULL")?;
    if last.signed_duration_since(watermark) <= chrono::Duration::hours(1) {
        return stamp_watermark(&conn).await;
    }
    drop(conn);

    // Separate session: this key also guards pg_cron, the Go backfill command
    // and the scheduler. Every monthly function call commits independently.
    let conn = pool.get().await?;
    conn.query_one("SELECT pg_advisory_lock($1)", &[&ROLLUP_LOCK])
        .await?;
    let result = async {
        let mut cursor = month_floor(first)?;
        let end = month_floor(last)?
            .checked_add_months(Months::new(1))
            .ok_or("usage month overflow")?;
        while cursor < end {
            let next = cursor
                .checked_add_months(Months::new(1))
                .ok_or("usage month overflow")?;
            conn.query_one(
                "SELECT rollup_task_usage_hourly_window($1::timestamptz,$2::timestamptz)",
                &[&cursor, &next],
            )
            .await?;
            cursor = next;
        }
        stamp_watermark(&conn).await
    }
    .await;
    let unlock = conn
        .query_one("SELECT pg_advisory_unlock($1)", &[&ROLLUP_LOCK])
        .await;
    result?;
    if !unlock?.get::<_, bool>(0) {
        return Err("task usage rollup advisory lock was lost".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn month_walk_handles_leap_year() {
        let feb = month_floor(Utc.with_ymd_and_hms(2024, 2, 29, 1, 0, 0).unwrap()).unwrap();
        assert_eq!(
            feb.checked_add_months(Months::new(1)).unwrap(),
            Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap()
        );
    }
}
