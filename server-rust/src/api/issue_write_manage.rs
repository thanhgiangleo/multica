//! Manage existing issue wakeups with Go's workspace → issue → rule lock order.
use super::issue_write_create_wakeup::{TerminalSnapshot, record_terminal_task_snapshot};
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueTaskCancelledEvent, IssueWakeupDispatchRequest},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{delete, patch, post},
};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::Transaction;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/issues/{id}/wakeups/{wakeupID}/instruction",
            patch(edit_instruction),
        )
        .route(
            "/api/issues/{id}/wakeups/{wakeupID}/disable",
            axum::routing::post(disable),
        )
        .route("/api/issues/{id}/wakeups/{wakeupID}/trigger", post(trigger))
        .route("/api/issues/{id}/wakeups/{wakeupID}/enable", post(enable))
        .route("/api/issues/{id}/wakeups/{wakeupID}", delete(delete_wakeup))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstructionInput {
    instruction: String,
    expected_instruction: String,
    revision: i64,
}

fn parse_instruction(body: &[u8]) -> Result<InstructionInput, ApiError> {
    if body.len() > 160_000 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid instruction body"));
    }
    let mut input: InstructionInput = serde_json::from_slice(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid instruction body"))?;
    input.instruction = input.instruction.trim().to_owned();
    if input.instruction.is_empty() || input.instruction.len() > 12_000 || input.revision < 1 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: instruction must be 1–12000 bytes and revision is required",
        ));
    }
    Ok(input)
}

struct ManagedRule {
    id: Uuid,
    revision: i64,
    instruction: String,
    agent_id: Uuid,
}

fn failure() -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup")
}

async fn lock_rule(
    tx: &Transaction<'_>,
    issue: Uuid,
    ws: Uuid,
    id: Uuid,
    member: Uuid,
) -> Result<ManagedRule, ApiError> {
    // The workspace lock fences teardown; all management mutations take these
    // locks in the same order as Save, Disable and the Go scheduler.
    tx.query_opt("SELECT w.id FROM workspace w JOIN issue i ON i.workspace_id=w.id WHERE i.id=$1 AND w.id=$2 FOR KEY SHARE OF w", &[&issue, &ws])
        .await.map_err(|_| failure())?.ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    tx.query_opt(
        "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
        &[&issue, &ws],
    )
    .await
    .map_err(|_| failure())?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    let role = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &member],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(|| error(StatusCode::FORBIDDEN, "wakeup permission denied"))?;
    let row = tx.query_opt("SELECT id,issue_id,workspace_id,created_by,system_rule,agent_id,revision,instruction FROM issue_wakeup WHERE id=$1 FOR UPDATE", &[&id])
        .await.map_err(|_| failure())?.ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    if row.get::<_, Uuid>("issue_id") != issue || row.get::<_, Uuid>("workspace_id") != ws {
        return Err(error(StatusCode::NOT_FOUND, "wakeup not found"));
    }
    let role: String = role.get("role");
    if row.get::<_, Option<String>>("system_rule").is_some()
        || (row.get::<_, Uuid>("created_by") != member && role != "owner" && role != "admin")
    {
        return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
    }
    Ok(ManagedRule {
        id,
        revision: row.get("revision"),
        instruction: row.get("instruction"),
        agent_id: row.get("agent_id"),
    })
}

async fn may_invoke(
    tx: &Transaction<'_>,
    ws: Uuid,
    member: Uuid,
    agent_id: Uuid,
) -> Result<(), ApiError> {
    let allowed: bool = tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent a WHERE a.id=$1 AND a.workspace_id=$2 AND a.archived_at IS NULL AND a.runtime_id IS NOT NULL AND (a.owner_id=$3 OR (a.permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (t.target_type='workspace' AND t.target_id=$2 OR t.target_type='member' AND t.target_id=$3)))))",
        &[&agent_id, &ws, &member],
    ).await.map_err(|_| failure())?.get(0);
    if !allowed {
        return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
    }
    Ok(())
}

async fn begin(
    state: &ApiState,
    ws: Uuid,
    raw_issue: &str,
    raw_wakeup: &str,
    actor: Actor,
    user: Uuid,
) -> Result<(deadpool_postgres::Client, Uuid, Uuid, Uuid), ApiError> {
    let db = state.pool.get().await.map_err(|_| failure())?;
    let issue: Uuid = load_issue(&db, ws, raw_issue).await?.get("id");
    let id = Uuid::parse_str(raw_wakeup)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup id"))?;
    // A verified task carries its human originator; client-provided actor headers are ignored.
    let member = match actor {
        Actor::Human | Actor::Cloud => user,
        Actor::Task(task) => task.user,
        Actor::Daemon => {
            return Err(error(
                StatusCode::FORBIDDEN,
                "a human originator is required",
            ));
        }
    };
    Ok((db, issue, id, member))
}

async fn edit_instruction(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let (mut db, issue, id, member) =
        begin(&state, ws, &raw_issue, &raw_wakeup, actor, user).await?;
    let input = parse_instruction(&body)?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    let rule = lock_rule(&tx, issue, ws, id, member).await?;
    may_invoke(&tx, ws, member, rule.agent_id).await?;
    if rule.revision != input.revision || rule.instruction != input.expected_instruction {
        return Err(error(
            StatusCode::CONFLICT,
            "wakeup changed; refresh and retry",
        ));
    }
    tx.execute(
        "UPDATE issue_wakeup SET instruction=$2,updated_at=now() WHERE id=$1 AND workspace_id=$3",
        &[&rule.id, &input.instruction, &ws],
    )
    .await
    .map_err(|_| failure())?;
    tx.commit().await.map_err(|_| failure())?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnableInput {
    revision: i64,
    at: Option<String>,
    #[serde(default)]
    rearm: bool,
}

// The conditional/cron validators and terminal-task subscription snapshot
// belong to the scheduler integration. Reject those cases rather than
// accepting an enabled rule that cannot be correctly armed.
async fn enable(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    if state.issue_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        ));
    }
    if body.len() > 1024 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid enable body"));
    }
    let input: EnableInput = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid enable body"))?;
    let source = match &actor {
        Actor::Task(task) => Some(task.task),
        _ => None,
    };
    let (mut db, issue, id, member) =
        begin(&state, ws, &raw_issue, &raw_wakeup, actor, user).await?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    let rule = lock_rule(&tx, issue, ws, id, member).await?;
    let status: String = tx
        .query_one("SELECT status FROM issue WHERE id=$1", &[&issue])
        .await
        .map_err(|_| failure())?
        .get(0);
    let category: String = match status.as_str() {
        "backlog" | "todo" | "in_progress" | "in_review" | "done" | "blocked" | "cancelled" => {
            status
        }
        _ => tx
            .query_opt(
                "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
                &[&ws, &status],
            )
            .await
            .map_err(|_| failure())?
            .ok_or_else(failure)?
            .get(0),
    };
    if matches!(category.as_str(), "done" | "closed" | "cancelled") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: issue is closed",
        ));
    }
    if input.revision < 1 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: revision is required",
        ));
    }
    if rule.revision != input.revision {
        return Err(error(
            StatusCode::CONFLICT,
            "wakeup changed; refresh and retry",
        ));
    }
    let row = tx.query_one("SELECT enabled,disabled_at,kind,mode,interval_seconds,next_fire_at,expires_at,expiry_seconds,on_timeout,condition,last_task_id,filter_task_id,event_types,filter_agent_id,cron_expression,timezone FROM issue_wakeup WHERE id=$1", &[&id])
        .await.map_err(|_| failure())?;
    let kind: String = row.get("kind");
    let mode: String = row.get("mode");
    if input.at.is_some() && kind != "at" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: only single-time wakeups accept a new time",
        ));
    }
    may_invoke(&tx, ws, member, rule.agent_id).await?;
    let enabled: bool = row.get("enabled");
    if enabled {
        if input.rearm || input.at.is_some() {
            return Err(error(
                StatusCode::CONFLICT,
                "wakeup changed; refresh and retry",
            ));
        }
        let value: Value = tx
            .query_one("SELECT to_jsonb(w) FROM issue_wakeup w WHERE id=$1", &[&id])
            .await
            .map_err(|_| failure())?
            .get(0);
        tx.commit().await.map_err(|_| failure())?;
        return Ok(GoJson(value));
    }
    if row
        .get::<_, Option<Value>>("condition")
        .is_some_and(|condition| condition != serde_json::json!({}))
    {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "wakeup scheduler integration required",
        ));
    }
    if mode == "once"
        && (row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("disabled_at")
            .is_none()
            || row.get::<_, Option<Uuid>>("last_task_id").is_some())
    {
        if !input.rearm {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: consumed one-shot requires explicit rearm",
            ));
        }
        let active: bool = tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE issue_id=$1 AND context->>'wakeup_id'=$2 AND status IN ('queued','deferred','dispatched','running','waiting_local_directory'))",
            &[&issue, &id.to_string()],
        ).await.map_err(|_| failure())?.get(0);
        if active {
            return Err(error(
                StatusCode::CONFLICT,
                "wakeup changed; previous run is still active",
            ));
        }
    }
    let now: chrono::DateTime<chrono::Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(|_| failure())?
        .get(0);
    let next: Option<chrono::DateTime<chrono::Utc>> = match kind.as_str() {
        "event" => None,
        "cron" => {
            if mode != "continuous" {
                return Err(failure());
            }
            let expression: Option<String> = row.get("cron_expression");
            let timezone: String = row.get("timezone");
            let next = super::issue_write_create_wakeup::cron::next_occurrence(
                expression.as_deref().unwrap_or(""),
                &timezone,
                now,
            )
            .map_err(|message| {
                error(
                    if message == "wakeup scheduler integration required" {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        StatusCode::BAD_REQUEST
                    },
                    message,
                )
            })?;
            Some(next)
        }
        "every" => {
            let seconds: Option<i64> = row.get("interval_seconds");
            let seconds = seconds.ok_or_else(failure)?;
            if !(60..=31_536_000).contains(&seconds) || mode != "continuous" {
                return Err(failure());
            }
            Some(now + chrono::Duration::seconds(seconds))
        }
        "at" => {
            let time = if let Some(at) = input.at {
                chrono::DateTime::parse_from_rfc3339(&at)
                    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid enable body"))?
                    .with_timezone(&chrono::Utc)
            } else {
                row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("next_fire_at")
                    .ok_or_else(|| {
                        error(
                            StatusCode::BAD_REQUEST,
                            "invalid wakeup: choose a future time",
                        )
                    })?
            };
            if time <= now {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "invalid wakeup: choose a future time",
                ));
            }
            Some(time)
        }
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: unsupported kind",
            ));
        }
    };
    let expires: Option<chrono::DateTime<chrono::Utc>> =
        if let Some(seconds) = row.get::<_, Option<i64>>("expiry_seconds") {
            Some(now + chrono::Duration::seconds(seconds))
        } else {
            row.get("expires_at")
        };
    if expires
        .is_some_and(|deadline| deadline <= now || deadline > now + chrono::Duration::days(365))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: expires_at must be in the future and within one year",
        ));
    }
    if kind == "at" && (expires.is_some() || row.get::<_, Option<String>>("on_timeout").is_some()) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: a single-time wakeup takes no deadline",
        ));
    }
    // No writes occur until all validation is complete. A failed request
    // leaves the rule, receipts and tasks unchanged.
    tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id])
        .await.map_err(|_| failure())?;
    let tasks = cancel_unstarted(&tx, id).await?;
    tx.execute("UPDATE issue_wakeup SET created_by=$2,source_task_id=$3,next_fire_at=$4,expires_at=$5,enabled=true,disabled_at=NULL,revision=revision+1,fire_count=0,condition_state='',paused_reason=NULL,timed_out_at=NULL,last_task_id=NULL,last_error=NULL,updated_at=now() WHERE id=$1", &[&id, &member, &source, &next, &expires])
        .await.map_err(|_| failure())?;
    if let Some(task_id) = row.get::<_, Option<Uuid>>("filter_task_id") {
        // Match Go Save: withdraw old work, lock the concrete source, then
        // register the terminal snapshot under the new revision before commit.
        let task = tx.query_opt(
            "SELECT id,agent_id,status,completed_at,retry_of_task_id,rerun_of_task_id FROM agent_task_queue WHERE id=$1 AND issue_id=$2 FOR UPDATE",
            &[&task_id, &issue],
        ).await.map_err(|_| failure())?
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid wakeup: run does not belong to this issue"))?;
        if row
            .get::<_, Option<Uuid>>("filter_agent_id")
            .is_some_and(|filter| filter != task.get::<_, Uuid>("agent_id"))
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: run and agent filters disagree",
            ));
        }
        record_terminal_task_snapshot(
            &tx,
            TerminalSnapshot {
                wakeup_id: id,
                revision: rule.revision + 1,
                issue,
                workspace: ws,
                mode: &mode,
                event_types: &row.get::<_, Vec<String>>("event_types"),
                now,
                task: &task,
            },
        )
        .await?;
    }
    let value: Value = tx
        .query_one("SELECT to_jsonb(w) FROM issue_wakeup w WHERE id=$1", &[&id])
        .await
        .map_err(|_| failure())?
        .get(0);
    tx.commit().await.map_err(|_| failure())?;
    publish_cancelled(&state, ws, tasks);
    Ok(GoJson(value))
}

// Both Disable and Delete must withdraw queued/deferred tasks and broadcast
// task:cancelled *after commit*. A missing sink must never silently drop it.
async fn cancel_unstarted(
    tx: &Transaction<'_>,
    id: Uuid,
) -> Result<Vec<(Uuid, Uuid, Option<Uuid>)>, ApiError> {
    let rows = tx.query(
        "UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error='Wakeup disabled or updated' WHERE context->>'wakeup_id'=$1 AND status IN ('queued','deferred') AND started_at IS NULL RETURNING id,agent_id,issue_id",
        &[&id.to_string()],
    ).await.map_err(|_| failure())?;
    Ok(rows
        .iter()
        .map(|r| (r.get("id"), r.get("agent_id"), r.get("issue_id")))
        .collect())
}

fn publish_cancelled(state: &ApiState, ws: Uuid, tasks: Vec<(Uuid, Uuid, Option<Uuid>)>) {
    if let Some(sink) = &state.issue_events {
        for (task_id, agent_id, issue_id) in tasks {
            sink.publish_task_cancelled(IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id,
                agent_id,
                issue_id,
            });
        }
    }
}

async fn disable(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    if state.issue_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        ));
    }
    let (mut db, issue, id, member) =
        begin(&state, ws, &raw_issue, &raw_wakeup, actor, user).await?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    lock_rule(&tx, issue, ws, id, member).await?;
    tx.execute("UPDATE issue_wakeup SET enabled=false,disabled_at=COALESCE(disabled_at,now()),updated_at=now() WHERE id=$1", &[&id]).await.map_err(|_| failure())?;
    tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id]).await.map_err(|_| failure())?;
    let tasks = cancel_unstarted(&tx, id).await?;
    let rule: Value = tx
        .query_one("SELECT to_jsonb(w) FROM issue_wakeup w WHERE id=$1", &[&id])
        .await
        .map_err(|_| failure())?
        .get(0);
    tx.commit().await.map_err(|_| failure())?;
    publish_cancelled(&state, ws, tasks);
    Ok(GoJson(rule))
}

// Manual trigger records a unique receipt while holding the rule lock. Dispatch
// is requested only after commit; the scheduler can recover a missed nudge by
// scanning pending receipts. Never acknowledge a trigger without that receipt.
async fn trigger(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "wakeup dispatcher unavailable",
        )
    })?;
    let (mut db, issue, id, member) =
        begin(&state, ws, &raw_issue, &raw_wakeup, actor, user).await?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    let rule = lock_rule(&tx, issue, ws, id, member).await?;
    let status: String = tx
        .query_one(
            "SELECT status FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&issue, &ws],
        )
        .await
        .map_err(|_| failure())?
        .get(0);
    let category = match status.as_str() {
        "backlog" | "todo" | "in_progress" | "in_review" | "done" | "blocked" | "cancelled" => {
            status
        }
        _ => tx
            .query_opt(
                "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
                &[&ws, &status],
            )
            .await
            .map_err(|_| failure())?
            .ok_or_else(failure)?
            .get(0),
    };
    if category == "done" || category == "closed" || category == "cancelled" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: issue is closed",
        ));
    }
    let disabled: bool = tx
        .query_one(
            "SELECT disabled_at IS NOT NULL FROM issue_wakeup WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| failure())?
        .get(0);
    if disabled {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: turn the wakeup on first",
        ));
    }
    may_invoke(&tx, ws, member, rule.agent_id).await?;
    let now: chrono::DateTime<chrono::Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(|_| failure())?
        .get(0);
    let key = format!("manual:{}", Uuid::new_v4());
    let payload = serde_json::json!({
        "event_id": key,
        "requested_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "actor_type": "member",
        "actor_id": member,
    });
    tx.execute(
        "INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) \
         VALUES ($1,$2,$3,$4,'wakeup.manual',$5) \
         ON CONFLICT(wakeup_id,revision,event_key) DO UPDATE SET event_key=EXCLUDED.event_key",
        &[&Uuid::new_v4(), &id, &rule.revision, &key, &payload],
    )
    .await
    .map_err(|_| failure())?;
    tx.commit().await.map_err(|_| failure())?;
    sink.request_wakeup_dispatch(IssueWakeupDispatchRequest {
        workspace_id: ws,
        issue_id: issue,
        wakeup_id: id,
    });
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_wakeup(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    if state.issue_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        ));
    }
    let (mut db, issue, id, member) =
        begin(&state, ws, &raw_issue, &raw_wakeup, actor, user).await?;
    let tx = db.transaction().await.map_err(|_| failure())?;
    lock_rule(&tx, issue, ws, id, member).await?;
    let tasks = cancel_unstarted(&tx, id).await?;
    tx.execute(
        "DELETE FROM issue_wakeup_receipt WHERE wakeup_id=$1",
        &[&id],
    )
    .await
    .map_err(|_| failure())?;
    tx.execute(
        "DELETE FROM issue_wakeup WHERE id=$1 AND issue_id=$2",
        &[&id, &issue],
    )
    .await
    .map_err(|_| failure())?;
    tx.commit().await.map_err(|_| failure())?;
    publish_cancelled(&state, ws, tasks);
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn instruction_validation() {
        assert_eq!(
            parse_instruction(
                br#"{"instruction":" ok ","expected_instruction":"old","revision":1}"#
            )
            .unwrap()
            .instruction,
            "ok"
        );
        assert!(
            parse_instruction(br#"{"instruction":" ","expected_instruction":"old","revision":1}"#)
                .is_err()
        );
        assert!(
            parse_instruction(br#"{"instruction":"ok","expected_instruction":"old","revision":0}"#)
                .is_err()
        );
        assert!(
            parse_instruction(
                br#"{"instruction":"ok","expected_instruction":"old","revision":1,"unknown":0}"#
            )
            .is_err()
        );
        assert!(
            parse_instruction(
                format!(
                    "{{\"instruction\":\"{}\",\"expected_instruction\":\"old\",\"revision\":1}}",
                    "á".repeat(6001)
                )
                .as_bytes()
            )
            .is_err()
        );
    }
}
