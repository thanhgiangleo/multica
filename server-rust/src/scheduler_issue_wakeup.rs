//! Durable wakeup dispatch. The scheduler lease chooses a pass; the rule row lock
//! and receipt/task writes in one transaction make each pass replayable.
use crate::scheduler::{CatchUp, HandlerResult, JobHandler, JobSpec, Lease, Scope};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio_postgres::Transaction;
use uuid::Uuid;

use crate::api::issue_write_create_wakeup::cron;

pub const ISSUE_WAKEUP_JOB: &str = "issue_wakeup_dispatch";
const PASS_BUDGET: Duration = Duration::from_secs(15);
const MAX_RULES: usize = 1500;

pub fn issue_wakeup_job() -> JobSpec {
    JobSpec {
        name: ISSUE_WAKEUP_JOB,
        cadence: Duration::from_secs(30),
        schedule_delay: Duration::ZERO,
        catch_up: CatchUp::LatestOnly,
        catch_up_window: Duration::from_secs(3600),
        max_plans_per_tick: 0,
        run_timeout: Duration::from_secs(45),
        stale_timeout: Duration::from_secs(60),
        heartbeat_interval: Duration::from_secs(10),
        allow_stale_reentry: true,
        max_attempts: 1,
        retry_backoff: vec![],
        scopes: vec![Scope::global()],
        handler: Arc::new(IssueWakeup),
    }
}

struct IssueWakeup;

fn pool_error(error: deadpool_postgres::PoolError) -> String {
    format!("pool: {error}")
}

fn db_error(error: tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(detail) => format!("database {}: {}", detail.code().code(), detail.message()),
        None => format!("database: {error}"),
    }
}

#[async_trait]
impl JobHandler for IssueWakeup {
    async fn handle(
        &self,
        pool: &Pool,
        _: DateTime<Utc>,
        _: Lease,
        _: Duration,
    ) -> Result<HandlerResult, String> {
        let started = tokio::time::Instant::now();
        let db = pool.get().await.map_err(pool_error)?;
        db.execute("WITH expired AS MATERIALIZED (SELECT id FROM issue_wakeup_receipt WHERE processed_at < now()-interval '7 days' ORDER BY processed_at,id LIMIT 1000 FOR UPDATE SKIP LOCKED) DELETE FROM issue_wakeup_receipt r USING expired WHERE r.id=expired.id", &[]).await.map_err(db_error)?;
        let mut seen = HashSet::new();
        let mut dispatched = 0;
        let mut failures = Vec::new();
        let mut input_turn = true;
        while seen.len() < MAX_RULES && started.elapsed() < PASS_BUDGET {
            let ids: Vec<Uuid> = seen.iter().copied().collect();
            let input = db.query_opt("SELECT w.id FROM issue_wakeup w WHERE EXISTS (SELECT 1 FROM issue_wakeup_receipt r WHERE r.wakeup_id=w.id AND r.processed_at IS NULL) AND NOT (w.id=ANY($1::uuid[])) ORDER BY w.updated_at,w.id LIMIT 1", &[&ids]).await.map_err(db_error)?;
            let due = db.query_opt("SELECT w.id FROM issue_wakeup w WHERE w.enabled AND (w.next_fire_at<=now() OR w.expires_at<=now()) AND NOT (w.id=ANY($1::uuid[])) ORDER BY w.updated_at,w.id LIMIT 1", &[&ids]).await.map_err(db_error)?;
            let id = match (input_turn, input, due) {
                (_, None, None) => break,
                (true, Some(row), _) | (_, Some(row), None) => row.get::<_, Uuid>(0),
                (_, _, Some(row)) => row.get::<_, Uuid>(0),
            };
            seen.insert(id);
            input_turn = !input_turn;
            let outcome = tokio::time::timeout(Duration::from_secs(2), dispatch(pool, id)).await;
            match outcome {
                Ok(Ok(created)) => dispatched += i64::from(created),
                other => {
                    let message = match other {
                        Ok(Err(e)) => e,
                        Err(_) => "wakeup dispatch timed out".to_owned(),
                        Ok(Ok(_)) => unreachable!(),
                    };
                    failures.push(format!("{id}: {message}"));
                    let _ = db.execute("UPDATE issue_wakeup SET last_error=$2,updated_at=clock_timestamp() WHERE id=$1", &[&id, &message.chars().take(500).collect::<String>()]).await;
                }
            }
            // Rotate candidates without changing their actual due time or receipt state.
            let _ = db
                .execute(
                    "UPDATE issue_wakeup SET updated_at=clock_timestamp() WHERE id=$1",
                    &[&id],
                )
                .await;
        }
        if !failures.is_empty() {
            return Err(failures.join("; "));
        }
        Ok(HandlerResult {
            rows_affected: dispatched,
            result: json!({"visited":seen.len(), "dispatched":dispatched}),
        })
    }
}

fn priority(value: &str) -> i32 {
    match value {
        "urgent" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

/// One immediate attempt at a rule whose receipt the caller just committed.
/// Go does the same from the manual-trigger handler ("dispatch now rather than
/// on the next tick") under the same 2s budget as the tick, and records the
/// failure on the rule so the next tick retries with the reason visible.
pub(crate) async fn dispatch_now(pool: &Pool, id: Uuid) -> Result<bool, String> {
    let message = match tokio::time::timeout(Duration::from_secs(2), dispatch(pool, id)).await {
        Ok(Ok(created)) => return Ok(created),
        Ok(Err(error)) => error,
        Err(_) => "wakeup dispatch timed out".to_owned(),
    };
    if let Ok(db) = pool.get().await {
        let _ = db
            .execute(
                "UPDATE issue_wakeup SET last_error=$2,updated_at=clock_timestamp() WHERE id=$1",
                &[&id, &message.chars().take(500).collect::<String>()],
            )
            .await;
    }
    Err(message)
}

async fn dispatch(pool: &Pool, id: Uuid) -> Result<bool, String> {
    let mut db = pool.get().await.map_err(pool_error)?;
    let tx = db.transaction().await.map_err(db_error)?;
    tx.batch_execute("SET LOCAL lock_timeout='50ms'")
        .await
        .map_err(db_error)?;
    // Take owner locks before issue/rule locks, as the Go writer does.
    let Some(candidate) = tx.query_opt("SELECT w.agent_id,w.issue_id,a.runtime_id FROM issue_wakeup w LEFT JOIN agent a ON a.id=w.agent_id AND a.workspace_id=w.workspace_id WHERE w.id=$1", &[&id]).await.map_err(db_error)? else { return Ok(false) };
    let agent_id: Uuid = candidate.get(0);
    let issue_id: Uuid = candidate.get(1);
    let runtime: Option<Uuid> = candidate.get(2);
    if runtime.is_some() {
        let locked: bool = tx
            .query_one(
                "SELECT lock_task_owner_rows($1,$2,$3)",
                &[&agent_id, &issue_id, &runtime],
            )
            .await
            .map_err(db_error)?
            .get(0);
        if !locked {
            return Err("task owner unavailable".into());
        }
    } else {
        // A missing agent cannot enqueue; the issue/rule lock still protects disabling it.
        tx.query_one("SELECT id FROM workspace WHERE id=(SELECT workspace_id FROM issue_wakeup WHERE id=$1) FOR UPDATE", &[&id]).await.map_err(db_error)?;
    }
    let issue = tx.query_opt("SELECT id,workspace_id,status,priority,assignee_type,assignee_id FROM issue WHERE id=$1 FOR UPDATE", &[&issue_id]).await.map_err(db_error)?;
    let Some(rule) = tx
        .query_opt("SELECT * FROM issue_wakeup WHERE id=$1 FOR UPDATE", &[&id])
        .await
        .map_err(db_error)?
    else {
        return Ok(false);
    };
    let ws: Uuid = rule.get("workspace_id");
    let Some(issue) = issue.filter(|row| row.get::<_, Uuid>("workspace_id") == ws) else {
        tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id]).await.map_err(db_error)?;
        tx.execute(
            "DELETE FROM issue_wakeup_receipt WHERE wakeup_id=$1",
            &[&id],
        )
        .await
        .map_err(db_error)?;
        tx.execute("DELETE FROM issue_wakeup WHERE id=$1", &[&id])
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        return Ok(false);
    };
    let revision: i64 = rule.get("revision");
    let kind: String = rule.get("kind");
    if rule.get::<_, Option<Value>>("condition").is_some()
        || rule.get::<_, Option<String>>("system_rule").is_some()
    {
        return Err("condition and system wakeups require the Go dispatcher".into());
    }
    let creator: Uuid = rule.get("created_by");
    let allowed: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM agent a JOIN member m ON m.workspace_id=a.workspace_id AND m.user_id=$3 WHERE a.id=$1 AND a.workspace_id=$2 AND a.archived_at IS NULL AND a.runtime_id IS NOT NULL AND (a.owner_id=$3 OR (a.permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (t.target_type='workspace' AND t.target_id=$2 OR t.target_type='member' AND t.target_id=$3)))))", &[&agent_id,&ws,&creator]).await.map_err(db_error)?.get(0);
    let status: String = issue.get("status");
    let closed: bool = tx.query_one("SELECT $2::text IN ('done','cancelled') OR EXISTS(SELECT 1 FROM issue_status WHERE workspace_id=$1 AND key=$2 AND category IN ('done','closed'))", &[&ws,&status]).await.map_err(db_error)?.get(0);
    if rule
        .get::<_, Option<DateTime<Utc>>>("disabled_at")
        .is_some()
        || closed
        || !allowed
    {
        tx.execute("UPDATE issue_wakeup SET enabled=false,disabled_at=COALESCE(disabled_at,now()),last_error='Wakeup disabled: issue closed or permission unavailable' WHERE id=$1", &[&id]).await.map_err(db_error)?;
        tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id]).await.map_err(db_error)?;
        tx.execute("UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error='Issue closed; wakeup disabled' WHERE context->>'wakeup_id'=$1 AND status IN ('queued','deferred') AND started_at IS NULL", &[&id.to_string()]).await.map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND revision<>$2 AND processed_at IS NULL", &[&id,&revision]).await.map_err(db_error)?;
    let now: DateTime<Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(db_error)?
        .get(0);
    let mode: String = rule.get("mode");
    if mode == "once" && rule.get::<_, Option<Uuid>>("last_task_id").is_some() {
        tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id]).await.map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    let mut next: Option<DateTime<Utc>> = rule.get("next_fire_at");
    let mut enabled: bool = rule.get("enabled");
    let expired = enabled
        && rule
            .get::<_, Option<DateTime<Utc>>>("expires_at")
            .is_some_and(|at| at <= now);
    if expired {
        // Do not silently drop an event rule configured to wake on timeout.
        if kind == "event" && rule.get::<_, Option<String>>("on_timeout").as_deref() == Some("wake")
        {
            return Err("event wake-on-timeout requires the Go dispatcher".into());
        }
        enabled = false;
        next = None;
    }
    if !expired && enabled && kind != "event" && next.is_some_and(|at| at <= now) {
        let mut planned = next.expect("due next_fire_at");
        next = match kind.as_str() {
            "at" => {
                enabled = false;
                None
            }
            "every" => {
                let seconds: Option<i64> = rule.get("interval_seconds");
                let seconds = seconds
                    .filter(|v| *v > 0)
                    .ok_or("invalid wakeup interval")?;
                let steps = (now - planned).num_seconds().div_euclid(seconds);
                planned += ChronoDuration::seconds(steps * seconds);
                Some(planned + ChronoDuration::seconds(seconds))
            }
            "cron" => {
                let expression: Option<String> = rule.get("cron_expression");
                let zone: String = rule.get("timezone");
                Some(
                    cron::next_occurrence(expression.as_deref().unwrap_or(""), &zone, now)
                        .map_err(str::to_owned)?,
                )
            }
            _ => return Err("invalid wakeup kind".into()),
        };
        let payload =
            json!({"planned_at": planned.to_rfc3339_opts(SecondsFormat::Secs, true), "kind":kind});
        let key = planned.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let receipt: Uuid = tx.query_one("INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES($1,$2,$3,$4,'time.due',$5) ON CONFLICT(wakeup_id,revision,event_key) DO UPDATE SET event_key=EXCLUDED.event_key RETURNING id", &[&Uuid::now_v7(),&id,&revision,&key,&payload]).await.map_err(db_error)?.get(0);
        tx.execute("DELETE FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='time.due' AND processed_at IS NULL AND id<>$2", &[&id,&receipt]).await.map_err(db_error)?;
    }
    let own = tx.query_opt("SELECT id,status FROM agent_task_queue WHERE context->>'wakeup_id'=$1 AND status IN ('queued','dispatched') ORDER BY created_at LIMIT 1 FOR UPDATE", &[&id.to_string()]).await.map_err(db_error)?;
    if own
        .as_ref()
        .is_some_and(|task| task.get::<_, String>("status") == "dispatched")
    {
        advance(&tx, id, enabled, next, None).await?;
        if expired {
            tx.execute(
                "UPDATE issue_wakeup SET timed_out_at=now() WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    let rows = tx.query("SELECT id,event_type,payload,task_id FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND revision=$2 AND processed_at IS NULL ORDER BY created_at,id LIMIT 100 FOR UPDATE", &[&id,&revision]).await.map_err(db_error)?;
    let mut ids = Vec::new();
    let mut facts = Vec::new();
    for row in rows {
        if row.get::<_, Option<Uuid>>("task_id").is_some() {
            continue;
        } // reserved for an unstarted claim
        ids.push(row.get::<_, Uuid>("id"));
        facts.push(json!({"event_type":row.get::<_,String>("event_type"),"payload":row.get::<_,Value>("payload")}));
    }
    if ids.is_empty() {
        advance(&tx, id, enabled, next, None).await?;
        if expired {
            tx.execute(
                "UPDATE issue_wakeup SET timed_out_at=now() WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)?;
        return Ok(false);
    }
    if mode == "once" {
        enabled = false;
        next = None;
    }
    let instruction: String = rule.get("instruction");
    let mut note = format!("Wakeup {id} triggered. Instruction:\n{instruction}\n");
    if kind == "every" || kind == "cron" {
        note.push_str(&format!("This is a scheduled check. If it finds nothing that needs a reply, end with `multica issue wakeup checkin {issue_id} {id} --note \"<what you checked and found>\"` instead of posting a comment; the note is shown on the rule. Post a comment when something changed, needs attention, or the check is done.\n"));
    }
    note.push_str("Trigger facts (read current state before deciding what to do):\n");
    for fact in &facts {
        note.push_str(&format!(
            "{} {}\n",
            fact["event_type"].as_str().unwrap_or(""),
            fact["payload"]
        ));
    }
    let evidence = json!({"version":1,"instruction":instruction,"facts":facts});
    let created = own.is_none();
    let task_id = if let Some(task) = own {
        let task_id: Uuid = task.get("id");
        tx.execute("UPDATE agent_task_queue SET handoff_note=$2,context=COALESCE(context,'{}'::jsonb)||jsonb_build_object('wakeup_evidence',$3::jsonb) WHERE id=$1 AND status='queued'", &[&task_id,&note,&evidence]).await.map_err(db_error)?;
        task_id
    } else {
        let runtime = runtime.ok_or("agent runtime unavailable")?;
        let context = json!({"wakeup_id":id,"wakeup_revision":revision,"wakeup_evidence":evidence,"wakeup_chain":[]});
        let summary: String = instruction.chars().take(160).collect();
        let summary = format!("Wakeup: {summary}");
        let prio: String = issue.get("priority");
        let source: Option<Uuid> = rule.get("source_task_id");
        let comment: Option<Uuid> = rule.get("parent_comment_id");
        let squad: Option<Uuid> = if issue.get::<_, Option<String>>("assignee_type").as_deref()
            == Some("squad")
        {
            let assignee: Option<Uuid> = issue.get("assignee_id");
            if let Some(assignee) = assignee {
                tx.query_opt("SELECT id FROM squad WHERE id=$1 AND workspace_id=$2 AND leader_id=$3 AND archived_at IS NULL", &[&assignee,&ws,&agent_id]).await.map_err(db_error)?.map(|row| row.get(0))
            } else {
                None
            }
        } else {
            None
        };
        let new_id = Uuid::now_v7();
        let row = tx.query_opt("INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,priority,trigger_comment_id,trigger_summary,handoff_note,is_leader_task,squad_id,context,originator_user_id,accountable_user_id,originator_source,delegated_from_task_id,trigger_evidence_kind,trigger_evidence_ref_id,workflow_step) SELECT $1,$2,$3,$4,'queued',$5,$6,$7,$8,$9,$10,$11,$12,$12,'trigger_owner',$13,'issue_wakeup',$14,COALESCE((SELECT workflow_step FROM agent_task_queue WHERE id=$13 AND issue_id=$4),i.status) FROM issue i WHERE i.id=$4 AND lock_task_owner_rows($2,$4,$3) RETURNING id", &[&new_id,&agent_id,&runtime,&issue_id,&priority(&prio),&comment,&summary,&note,&squad.is_some(),&squad,&context,&creator,&source,&id]).await.map_err(db_error)?;
        row.ok_or("task owner unavailable")?.get(0)
    };
    tx.execute(
        "UPDATE issue_wakeup_receipt SET task_id=$2,processed_at=now() WHERE id=ANY($1::uuid[])",
        &[&ids, &task_id],
    )
    .await
    .map_err(db_error)?;
    advance(&tx, id, enabled, next, Some(task_id)).await?;
    if expired {
        tx.execute(
            "UPDATE issue_wakeup SET timed_out_at=now() WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(db_error)?;
    }
    if created {
        tx.execute(
            "UPDATE issue_wakeup SET fire_count=fire_count+1 WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(db_error)?;
        let count: i32 = rule.get("fire_count");
        let max: Option<i32> = rule.get("max_fires");
        if enabled && max.is_some_and(|limit| count + 1 >= limit) {
            tx.execute("UPDATE issue_wakeup SET enabled=false,paused_reason='max_fires',next_fire_at=NULL WHERE id=$1", &[&id]).await.map_err(db_error)?;
        }
    }
    tx.commit().await.map_err(db_error)?;
    Ok(created)
}

async fn advance(
    tx: &Transaction<'_>,
    id: Uuid,
    enabled: bool,
    next: Option<DateTime<Utc>>,
    task: Option<Uuid>,
) -> Result<(), String> {
    tx.execute("UPDATE issue_wakeup SET enabled=$2,next_fire_at=$3,last_task_id=COALESCE($4,last_task_id),last_error=NULL,updated_at=clock_timestamp() WHERE id=$1", &[&id,&enabled,&next,&task]).await.map_err(db_error)?;
    Ok(())
}
