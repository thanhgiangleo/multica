//! Full Save(existingID) replacement: one lock-ordered transaction fences stale
//! receipts and queued runs before the new revision becomes visible.
use super::{
    ApiError, ApiState, Expiry, GoJson, Identity, TerminalSnapshot, WakeupInput, condition, error,
    expiry, invalid, record_terminal_task_snapshot, schedule, validated_timezone,
};
use crate::api::{
    actor::Actor, catalog::WorkspaceId, issue_read::load_issue,
    issue_write::IssueTaskCancelledEvent,
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_postgres::Transaction;
use uuid::Uuid;

fn fail() -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup")
}
fn forbidden() -> ApiError {
    error(StatusCode::FORBIDDEN, "wakeup permission denied")
}
fn missing() -> ApiError {
    error(StatusCode::NOT_FOUND, "wakeup not found")
}
fn busy_or_fail(err: tokio_postgres::Error) -> ApiError {
    if err
        .as_db_error()
        .is_some_and(|db| db.code().code() == "55P03")
    {
        error(
            StatusCode::CONFLICT,
            "source run is changing; retry registration",
        )
    } else {
        fail()
    }
}

async fn member_role(tx: &Transaction<'_>, ws: Uuid, member: Uuid) -> Result<String, ApiError> {
    tx.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &member],
    )
    .await
    .map_err(|_| fail())?
    .map(|r| r.get(0))
    .ok_or_else(forbidden)
}

async fn agent_access(
    tx: &Transaction<'_>,
    ws: Uuid,
    member: Uuid,
    agent: Uuid,
) -> Result<(), ApiError> {
    // Same membership + CanMemberInvokeAgent contract as POST and Go Save.
    let allowed: bool = tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent a WHERE a.id=$1 AND a.workspace_id=$2 AND a.archived_at IS NULL AND a.runtime_id IS NOT NULL AND (a.owner_id=$3 OR (a.permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (t.target_type='workspace' AND t.target_id=$2 OR t.target_type='member' AND t.target_id=$3)))))",
        &[&agent, &ws, &member],
    ).await.map_err(|_| fail())?.get(0);
    if !allowed {
        return Err(forbidden());
    }
    Ok(())
}

pub(super) async fn replace(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_wakeup)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let issue: Uuid = load_issue(&db, ws, &raw_issue).await?.get("id");
    if body.len() > 32_768 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup body"));
    }
    let mut input: WakeupInput = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup body"))?;
    let id = Uuid::parse_str(&raw_wakeup)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup id"))?;
    let (member, source) = match actor {
        Actor::Human | Actor::Cloud => (user, None),
        Actor::Task(task) => {
            if input.agent_id.is_none() {
                input.agent_id = Some(task.agent);
            }
            (task.user, Some(task.task))
        }
        Actor::Daemon => {
            return Err(error(
                StatusCode::FORBIDDEN,
                "a human originator is required",
            ));
        }
    };
    let instruction = input.instruction.trim().to_owned();
    if instruction.is_empty() || instruction.len() > 12_000 {
        return Err(invalid("instruction must contain 1–12000 bytes"));
    }
    let tx = db.transaction().await.map_err(|_| fail())?;
    // Save's lock order: workspace, issue, then wakeup. Locking the issue also
    // serializes closing it with changing a subscription.
    tx.query_opt("SELECT w.id FROM workspace w JOIN issue i ON i.workspace_id=w.id WHERE i.id=$1 AND w.id=$2 FOR KEY SHARE OF w", &[&issue, &ws])
        .await.map_err(|_| fail())?.ok_or_else(missing)?;
    let issue_row = tx
        .query_opt(
            "SELECT status FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
            &[&issue, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(missing)?;
    let status: String = issue_row.get(0);
    let category: String = match status.as_str() {
        "backlog" | "todo" => "unstarted".into(),
        "in_progress" | "in_review" | "blocked" => "started".into(),
        "done" => "done".into(),
        "cancelled" => "closed".into(),
        _ => tx
            .query_opt(
                "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
                &[&ws, &status],
            )
            .await
            .map_err(|_| fail())?
            .ok_or_else(fail)?
            .get(0),
    };
    if matches!(category.as_str(), "done" | "closed") {
        return Err(invalid("issue is closed"));
    }
    let now: DateTime<Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(|_| fail())?
        .get(0);

    // Go Save validates and authorizes the replacement before withdrawing any
    // old work. All later writes still roll back together on any error.
    let mut condition_value = None;
    let (mode, next) = if let Some(raw) = input.condition.as_ref().filter(|v| !v.is_null()) {
        if input.kind != "event" {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: conditions use kind event",
            ));
        }
        if !input.event_types.is_empty()
            || input.filter_agent_id.is_some()
            || input.filter_task_id.is_some()
            || input
                .filter_actor_type
                .as_deref()
                .is_some_and(|v| !v.is_empty())
            || input.filter_actor_id.is_some()
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: a condition cannot be combined with events or filters",
            ));
        }
        if input.after_seconds != 0
            || input.at.is_some()
            || input.interval_seconds != 0
            || input
                .cron_expression
                .as_deref()
                .is_some_and(|s| !s.is_empty())
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: event wakeups cannot contain a schedule",
            ));
        }
        let (normalized, hints) = condition::normalize(&tx, issue, ws, raw).await?;
        condition_value = Some(normalized);
        input.event_types = hints;
        let mode = if input.mode.is_empty() {
            "once"
        } else {
            input.mode.as_str()
        };
        if !matches!(mode, "once" | "continuous") {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: mode must be once or continuous",
            ));
        }
        if input.max_fires != 0 && (mode != "continuous" || !(1..=1000).contains(&input.max_fires))
        {
            return Err(invalid("max_fires must be 1–1000 on a repeating rule"));
        }
        (mode.to_owned(), Some(now))
    } else {
        // Go treats an explicit JSON null condition as no condition.
        input.condition = None;
        schedule(&input, now)?
    };
    let timezone = validated_timezone(input.timezone.as_deref())?;
    let expiry = if input.on_timeout.as_deref() == Some("wake")
        && input.kind == "event"
        && (input.expires_at.is_some() || input.expires_in_seconds != 0)
    {
        // POST currently fails closed on timeout-wake; Save(existingID) must
        // accept the complete Go event-rule contract, including the deadline.
        if input.expires_at.is_some() && input.expires_in_seconds != 0 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: provide expires_at or expires_in_seconds, not both",
            ));
        }
        if input.expires_in_seconds != 0 {
            if !(60..=31_536_000).contains(&input.expires_in_seconds) {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "invalid wakeup: expires_in_seconds must be between 60 and 31536000",
                ));
            }
            Expiry {
                at: Some(now + chrono::Duration::seconds(input.expires_in_seconds)),
                seconds: Some(input.expires_in_seconds),
                on_timeout: Some("wake"),
            }
        } else {
            let deadline = DateTime::parse_from_rfc3339(input.expires_at.as_deref().unwrap_or(""))
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup body"))?
                .to_utc();
            if deadline <= now || deadline - now > chrono::Duration::seconds(31_536_000) {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "invalid wakeup: expires_at must be in the future and within one year",
                ));
            }
            Expiry {
                at: Some(deadline),
                seconds: None,
                on_timeout: Some("wake"),
            }
        }
    } else {
        expiry(&input, now)?
    };
    let agent_id = input
        .agent_id
        .ok_or_else(|| invalid("agent_id is required"))?;
    let role = member_role(&tx, ws, member).await?;
    agent_access(&tx, ws, member, agent_id).await?;
    if let Some(filter) = input.filter_agent_id {
        let exists: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2)",
                &[&filter, &ws],
            )
            .await
            .map_err(|_| fail())?
            .get(0);
        if !exists {
            return Err(forbidden());
        }
    }
    if let Some(filter) = input.filter_actor_id {
        let exists: bool = match input.filter_actor_type.as_deref() {
            Some("member") => tx.query_one("SELECT EXISTS(SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2)", &[&filter, &ws]).await,
            Some("agent") => tx.query_one("SELECT EXISTS(SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2)", &[&filter, &ws]).await,
            _ => return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup: actor filter requires an event, member or agent type, and actor ID")),
        }.map_err(|_| fail())?.get(0);
        if !exists {
            return Err(forbidden());
        }
    }
    if let Some(parent) = input.parent_comment_id {
        let exists: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM comment WHERE id=$1 AND issue_id=$2 AND deleted_at IS NULL)", &[&parent, &issue])
            .await.map_err(|_| fail())?.get(0);
        if !exists {
            return Err(invalid("comment not found"));
        }
    }
    let old = tx.query_opt("SELECT issue_id,workspace_id,created_by,system_rule FROM issue_wakeup WHERE id=$1 FOR UPDATE", &[&id])
        .await.map_err(|_| fail())?.ok_or_else(missing)?;
    if old.get::<_, Uuid>("issue_id") != issue || old.get::<_, Uuid>("workspace_id") != ws {
        return Err(missing());
    }
    if old.get::<_, Option<String>>("system_rule").is_some()
        || (old.get::<_, Uuid>("created_by") != member
            && !matches!(role.as_str(), "owner" | "admin"))
    {
        return Err(forbidden());
    }
    // The old revision's unprocessed receipts and unstarted jobs become inert.
    tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id])
        .await.map_err(|_| fail())?;
    let cancelled = tx.query(
        "UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error='Wakeup disabled or updated' WHERE context->>'wakeup_id'=$1 AND status IN ('queued','deferred') AND started_at IS NULL RETURNING id,agent_id,issue_id",
        &[&id.to_string()],
    ).await.map_err(|_| fail())?;
    // Under the same row lock, a source task that was already terminal becomes
    // a receipt for the new revision; a future transition is captured by DB.
    let task = if let Some(filter) = input.filter_task_id {
        let row = tx.query_opt("SELECT id,agent_id,status,completed_at,retry_of_task_id,rerun_of_task_id FROM agent_task_queue WHERE id=$1 AND issue_id=$2 FOR UPDATE NOWAIT", &[&filter, &issue])
            .await.map_err(busy_or_fail)?.ok_or_else(|| invalid("run does not belong to this issue"))?;
        if input
            .filter_agent_id
            .is_some_and(|a| a != row.get::<_, Uuid>("agent_id"))
        {
            return Err(invalid("run and agent filters disagree"));
        }
        Some(row)
    } else {
        None
    };
    let (filter_agent, filter_actor_type, filter_actor_id) = if input.kind == "event" {
        if let Some(agent) = input
            .filter_agent_id
            .filter(|_| input.event_types.iter().all(|e| !e.starts_with("task.")))
        {
            (None, Some("agent"), Some(agent))
        } else {
            (
                input.filter_agent_id,
                input.filter_actor_type.as_deref().filter(|s| !s.is_empty()),
                input.filter_actor_id,
            )
        }
    } else {
        (None, None, None)
    };
    let interval = (input.interval_seconds > 0).then_some(input.interval_seconds);
    let cron = input.cron_expression.as_deref().filter(|s| !s.is_empty());
    let on_timeout = expiry.on_timeout;
    let max_fires = if input.max_fires > 0 {
        Some(input.max_fires)
    } else if input.kind == "event" && mode == "continuous" {
        Some(20)
    } else {
        None
    };
    let rule: Value = tx.query_one(
        "UPDATE issue_wakeup SET agent_id=$2,created_by=$3,source_task_id=$4,parent_comment_id=$5,instruction=$6,kind=$7,mode=$8,event_types=$9,filter_agent_id=$10,filter_task_id=$11,interval_seconds=$12,cron_expression=$13,timezone=$14,next_fire_at=$15,filter_actor_type=$16,filter_actor_id=$17,expires_at=$18,expiry_seconds=$19,on_timeout=$20,timed_out_at=NULL,condition=$21,max_fires=$22,condition_state='',fire_count=0,paused_reason=NULL,enabled=true,disabled_at=NULL,revision=revision+1,last_task_id=NULL,last_error=NULL,updated_at=now() WHERE id=$1 RETURNING to_jsonb(issue_wakeup)",
        &[&id,&agent_id,&member,&source,&input.parent_comment_id,&instruction,&input.kind,&mode,&input.event_types,&filter_agent,&input.filter_task_id,&interval,&cron,&timezone,&next,&filter_actor_type,&filter_actor_id,&expiry.at,&expiry.seconds,&on_timeout,&condition_value,&max_fires],
    ).await.map_err(|e| {
        if e.as_db_error().is_some_and(|db| db.constraint() == Some("issue_wakeup_active_limit")) {
            error(StatusCode::BAD_REQUEST, "wakeup capacity exceeded")
        } else { fail() }
    })?.get(0);
    if let Some(condition) = condition_value.as_ref()
        && condition["type"] == "pull_request"
        && condition["event"] == "checks_finished"
    {
        condition::baseline(
            &tx,
            id,
            issue,
            ws,
            condition,
            now,
            state
                .pull_request_refresh
                .as_ref()
                .is_some_and(|refresh| refresh.enabled()),
        )
        .await?;
    }
    if let Some(task) = task.as_ref() {
        record_terminal_task_snapshot(
            &tx,
            TerminalSnapshot {
                wakeup_id: id,
                revision: rule["revision"].as_i64().ok_or_else(fail)?,
                issue,
                workspace: ws,
                mode: &mode,
                event_types: &input.event_types,
                now,
                task,
            },
        )
        .await?;
    }
    let output: Value = tx
        .query_one("SELECT to_jsonb(w) FROM issue_wakeup w WHERE id=$1", &[&id])
        .await
        .map_err(|_| fail())?
        .get(0);
    tx.commit().await.map_err(|_| fail())?;
    for row in cancelled {
        sink.publish_task_cancelled(IssueTaskCancelledEvent {
            workspace_id: ws,
            task_id: row.get("id"),
            agent_id: row.get("agent_id"),
            issue_id: row.get("issue_id"),
        });
    }
    Ok(GoJson(output))
}
