//! Create scheduled wakeups and event subscriptions. Unsupported scheduler contracts fail closed.
#[path = "issue_write_wakeup_condition.rs"]
mod condition;
#[path = "issue_write_cron.rs"]
pub(crate) mod cron;
#[path = "issue_write_replace_wakeup.rs"]
mod replace;
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_read::load_issue, issue_write::IssueWakeupActivityEvent,
    issue_write_wakeup::wakeup_preview,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{post, put},
};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Component, Path as FsPath},
};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/wakeups", post(create))
        .route("/api/issues/{id}/wakeups/{wakeupID}", put(replace::replace))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WakeupInput {
    agent_id: Option<Uuid>,
    instruction: String,
    kind: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    event_types: Vec<String>,
    filter_agent_id: Option<Uuid>,
    filter_actor_type: Option<String>,
    filter_actor_id: Option<Uuid>,
    filter_task_id: Option<Uuid>,
    parent_comment_id: Option<Uuid>,
    #[serde(default)]
    after_seconds: i64,
    at: Option<String>,
    #[serde(default)]
    interval_seconds: i64,
    cron_expression: Option<String>,
    timezone: Option<String>,
    expires_at: Option<String>,
    #[serde(default)]
    expires_in_seconds: i64,
    on_timeout: Option<String>,
    condition: Option<Value>,
    #[serde(default)]
    max_fires: i32,
}

const EVENT_TYPES: &[&str] = &[
    "task.queued",
    "task.dispatched",
    "task.started",
    "task.deferred",
    "task.waiting_local_directory",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "issue.updated",
    "issue.status_changed",
    "issue.assignee_changed",
    "issue.parent_changed",
    "issue.project_changed",
    "issue.labels_changed",
    "issue.properties_changed",
    "issue.metadata_changed",
    "comment.created",
    "comment.updated",
    "comment.deleted",
    "comment.resolved",
    "comment.unresolved",
    "reaction.added",
    "reaction.removed",
    "attachment.attached",
    "attachment.detached",
];

fn invalid(message: &str) -> ApiError {
    let message = match message {
        "instruction must contain 1–12000 bytes" => {
            "invalid wakeup: instruction must contain 1–12000 bytes"
        }
        "kind must be event, at, every or cron" => {
            "invalid wakeup: kind must be event, at, every or cron"
        }
        "time wakeup cannot contain event filters" => {
            "invalid wakeup: time wakeup cannot contain event filters"
        }
        "max_fires must be 1–1000 on a repeating rule" => {
            "invalid wakeup: max_fires must be 1–1000 on a repeating rule"
        }
        "provide at or after_seconds (1–31536000)" => {
            "invalid wakeup: provide at or after_seconds (1–31536000)"
        }
        "every requires continuous mode and interval_seconds between 60 and 31536000" => {
            "invalid wakeup: every requires continuous mode and interval_seconds between 60 and 31536000"
        }
        "cron must have a future occurrence" => {
            "invalid wakeup: cron must have a future occurrence"
        }
        "cron requires continuous mode without other schedule fields" => {
            "invalid wakeup: cron requires continuous mode without other schedule fields"
        }
        "agent_id is required" => "invalid wakeup: agent_id is required",
        "comment not found" => "invalid wakeup: comment not found",
        "issue is closed" => "invalid wakeup: issue is closed",
        _ => "invalid wakeup",
    };
    error(StatusCode::BAD_REQUEST, message)
}

fn unsupported() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "wakeup scheduler integration unavailable for this rule",
    )
}

// Go validates named timezones with time.LoadLocation. Time-based rules here
// compute their next occurrence from an absolute instant, so the zone affects
// the stored rule and later scheduler behavior, not this initial calculation.
fn validated_timezone(value: Option<&str>) -> Result<&str, ApiError> {
    let zone = value.filter(|zone| !zone.is_empty()).unwrap_or("UTC");
    if zone == "UTC" {
        return Ok(zone);
    }
    let path = FsPath::new(zone);
    if path
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
        || zone.starts_with('.')
        || !["/usr/share/zoneinfo", "/usr/share/lib/zoneinfo"]
            .iter()
            .any(|root| {
                std::fs::File::open(FsPath::new(root).join(path))
                    .and_then(|mut file| {
                        let mut magic = [0; 4];
                        file.read_exact(&mut magic)?;
                        Ok(magic == *b"TZif")
                    })
                    .unwrap_or(false)
            })
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: invalid timezone",
        ));
    }
    Ok(zone)
}

// Normalize only cases whose scheduler and registration-side behavior are known.
// Never acknowledge a rule whose event capture/condition/terminal snapshot is not ported.
fn schedule(
    input: &WakeupInput,
    now: DateTime<Utc>,
) -> Result<(String, Option<DateTime<Utc>>), ApiError> {
    let mode = if input.mode.is_empty() {
        match input.kind.as_str() {
            "at" | "event" => "once",
            "every" | "cron" => "continuous",
            _ => "",
        }
    } else {
        &input.mode
    };
    if input.condition.is_some() {
        return Err(unsupported());
    }
    if !matches!(input.kind.as_str(), "event" | "at" | "every" | "cron") {
        return Err(invalid("kind must be event, at, every or cron"));
    }
    if !matches!(mode, "once" | "continuous") {
        return Err(invalid("mode must be once or continuous"));
    }
    if input.max_fires != 0 && (mode != "continuous" || !(1..=1000).contains(&input.max_fires)) {
        return Err(invalid("max_fires must be 1–1000 on a repeating rule"));
    }
    validated_timezone(input.timezone.as_deref())?;
    if input.kind != "event"
        && (input.filter_task_id.is_some()
            || input.filter_agent_id.is_some()
            || input.filter_actor_id.is_some()
            || input.filter_actor_type.is_some())
    {
        return Err(invalid("time wakeup cannot contain event filters"));
    }
    if input.kind != "event" && !input.event_types.is_empty() {
        return Err(invalid("time wakeup cannot contain event filters"));
    }
    if input.kind == "event" {
        if (input.filter_actor_type.is_some() || input.filter_actor_id.is_some())
            && (!matches!(input.filter_actor_type.as_deref(), Some("member" | "agent"))
                || input.filter_actor_id.is_none()
                || input.filter_agent_id.is_some()
                || input.filter_task_id.is_some()
                || input
                    .event_types
                    .iter()
                    .any(|event| event.starts_with("task.")))
        {
            return Err(invalid("invalid actor filter"));
        }
        if input.filter_task_id.is_some()
            && input
                .event_types
                .iter()
                .any(|event| !event.starts_with("task."))
        {
            return Err(invalid("task filter requires task events"));
        }
        if !matches!(mode, "once" | "continuous")
            || input.event_types.is_empty()
            || input.event_types.len() > EVENT_TYPES.len()
            || input
                .event_types
                .iter()
                .any(|event| !EVENT_TYPES.contains(&event.as_str()))
            || input.after_seconds != 0
            || input.at.is_some()
            || input.interval_seconds != 0
            || input
                .cron_expression
                .as_deref()
                .is_some_and(|v| !v.is_empty())
        {
            return Err(invalid("invalid event subscription"));
        }
        return Ok((mode.to_owned(), None));
    }
    if input.kind != "cron"
        && input
            .cron_expression
            .as_deref()
            .is_some_and(|v| !v.is_empty())
    {
        return Err(invalid("time wakeup cannot contain event filters"));
    }
    let next = match input.kind.as_str() {
        "at" => {
            if mode != "once"
                || input.interval_seconds != 0
                || (input.at.is_some() == (input.after_seconds != 0))
                || !(0..=31_536_000).contains(&input.after_seconds)
            {
                return Err(invalid("provide at or after_seconds (1–31536000)"));
            }
            match &input.at {
                Some(at) => DateTime::parse_from_rfc3339(at)
                    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup body"))?
                    .to_utc(),
                None => now + Duration::seconds(input.after_seconds),
            }
        }
        "every" => {
            if mode != "continuous"
                || !(60..=31_536_000).contains(&input.interval_seconds)
                || input.at.is_some()
                || input.after_seconds != 0
            {
                return Err(invalid(
                    "every requires continuous mode and interval_seconds between 60 and 31536000",
                ));
            }
            now + Duration::seconds(input.interval_seconds)
        }
        "cron" => {
            if mode != "continuous"
                || input.at.is_some()
                || input.after_seconds != 0
                || input.interval_seconds != 0
            {
                return Err(invalid(
                    "cron requires continuous mode without other schedule fields",
                ));
            }
            cron::next_occurrence(
                input.cron_expression.as_deref().unwrap_or(""),
                validated_timezone(input.timezone.as_deref())?,
                now,
            )
            .map_err(|message| {
                if message == "wakeup scheduler integration required" {
                    unsupported()
                } else {
                    invalid(message)
                }
            })?
        }
        _ => unreachable!(),
    };
    Ok((mode.to_owned(), Some(next)))
}

/// Committed deadline fields. A relative deadline keeps its duration so that
/// re-enabling the rule restarts the wait from "now".
#[derive(Debug)]
struct Expiry {
    at: Option<DateTime<Utc>>,
    seconds: Option<i64>,
    on_timeout: Option<&'static str>,
}

fn expiry(input: &WakeupInput, now: DateTime<Utc>) -> Result<Expiry, ApiError> {
    let has_deadline = input.expires_at.is_some() || input.expires_in_seconds != 0;
    let timeout = input.on_timeout.as_deref().unwrap_or("");
    if !matches!(timeout, "" | "end" | "wake") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: on_timeout must be end or wake",
        ));
    }
    if input.kind == "at" && (has_deadline || !timeout.is_empty()) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: a single-time wakeup ends when it fires; it takes no deadline",
        ));
    }
    if input.expires_at.is_some() && input.expires_in_seconds != 0 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: provide expires_at or expires_in_seconds, not both",
        ));
    }
    if !has_deadline {
        if !timeout.is_empty() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: on_timeout requires a deadline",
            ));
        }
        return Ok(Expiry {
            at: None,
            seconds: None,
            on_timeout: None,
        });
    }
    // Wake-on-timeout needs an event subscription and its scheduler. Never
    // persist one as an inert time rule.
    if timeout == "wake" {
        if input.kind != "event" {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: only event wakeups can wake the target on timeout",
            ));
        }
        return Err(unsupported());
    }
    if input.expires_in_seconds != 0 {
        let seconds = input.expires_in_seconds;
        if !(60..=31_536_000).contains(&seconds) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid wakeup: expires_in_seconds must be between 60 and 31536000",
            ));
        }
        return Ok(Expiry {
            at: Some(now + Duration::seconds(seconds)),
            seconds: Some(seconds),
            on_timeout: Some("end"),
        });
    }
    let end = DateTime::parse_from_rfc3339(input.expires_at.as_deref().unwrap_or(""))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup body"))?
        .to_utc();
    if end <= now || end - now > Duration::seconds(31_536_000) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: expires_at must be in the future and within one year",
        ));
    }
    Ok(Expiry {
        at: Some(end),
        seconds: None,
        on_timeout: Some("end"),
    })
}

// The source row stays locked until the rule and receipt commit. A terminal
// transition that wins before this lock is materialized here; one that wins
// afterwards is captured by the database event trigger.
pub(super) struct TerminalSnapshot<'a> {
    pub(super) wakeup_id: Uuid,
    pub(super) revision: i64,
    pub(super) issue: Uuid,
    pub(super) workspace: Uuid,
    pub(super) mode: &'a str,
    pub(super) event_types: &'a [String],
    pub(super) now: DateTime<Utc>,
    pub(super) task: &'a Row,
}

pub(super) async fn record_terminal_task_snapshot(
    tx: &Transaction<'_>,
    snapshot: TerminalSnapshot<'_>,
) -> Result<(), ApiError> {
    let TerminalSnapshot {
        wakeup_id,
        revision,
        issue,
        workspace: ws,
        mode,
        event_types,
        now,
        task,
    } = snapshot;
    let status: String = task.get("status");
    let event_type = format!("task.{status}");
    if !matches!(status.as_str(), "completed" | "failed" | "cancelled")
        || !event_types.contains(&event_type)
    {
        return Ok(());
    }
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    let task_id: Uuid = task.get("id");
    let task_agent: Uuid = task.get("agent_id");
    let key = format!("{task_id}:{status}");
    let payload = json!({
        "event_id":key,"event_type":event_type,"version":1,
        "occurred_at":task.get::<_, Option<DateTime<Utc>>>("completed_at")
            .map(|at| at.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
        "observed_at":now.to_rfc3339_opts(SecondsFormat::AutoSi, true),"registration_snapshot":true,
        "workspace_id":ws,"issue_id":issue,"task_id":task_id,"source_task_id":task_id,
        "status":status,"agent_id":task_agent,"actor_type":"agent","actor_id":task_agent,
        "retry_of_task_id":task.get::<_, Option<Uuid>>("retry_of_task_id"),
        "rerun_of_task_id":task.get::<_, Option<Uuid>>("rerun_of_task_id"),
    });
    tx.execute(
        "INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(wakeup_id,revision,event_key) DO UPDATE SET event_key=EXCLUDED.event_key",
        &[&Uuid::new_v4(),&wakeup_id,&revision,&key,&event_type,&payload],
    ).await.map_err(|_| fail())?;
    if mode == "once" {
        tx.execute("UPDATE issue_wakeup SET enabled=false,next_fire_at=NULL,updated_at=clock_timestamp() WHERE id=$1", &[&wakeup_id])
            .await.map_err(|_| fail())?;
    }
    Ok(())
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_issue): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    if body.len() > 32_768 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup body"));
    }
    let input: WakeupInput = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid wakeup body"))?;
    let instruction = input.instruction.trim();
    if instruction.is_empty() || instruction.len() > 12_000 {
        return Err(invalid("instruction must contain 1–12000 bytes"));
    }
    let (member, agent_id, source, actor_type, actor_id) = match actor {
        Actor::Human | Actor::Cloud => (
            user,
            input
                .agent_id
                .ok_or_else(|| invalid("agent_id is required"))?,
            None,
            "member",
            user,
        ),
        Actor::Task(task) => (
            task.user,
            input.agent_id.unwrap_or(task.agent),
            Some(task.task),
            "agent",
            task.agent,
        ),
        Actor::Daemon => {
            return Err(error(
                StatusCode::FORBIDDEN,
                "a human originator is required",
            ));
        }
    };
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let issue: Uuid = load_issue(&db, ws, &raw_issue).await?.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup"))?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    // Workspace lock precedes issue lock as in Go Save; this fences teardown.
    tx.query_opt("SELECT w.id FROM workspace w JOIN issue i ON i.workspace_id=w.id WHERE i.id=$1 AND w.id=$2 FOR KEY SHARE OF w", &[&issue, &ws])
        .await.map_err(|_| fail())?.ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    let issue_row = tx
        .query_opt(
            "SELECT status FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
            &[&issue, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "wakeup not found"))?;
    let status: String = issue_row.get("status");
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
            .map_err(|_| fail())?
            .ok_or_else(fail)?
            .get(0),
    };
    if matches!(category.as_str(), "done" | "closed" | "cancelled") {
        return Err(invalid("issue is closed"));
    }
    let now: DateTime<Utc> = tx
        .query_one("SELECT now()", &[])
        .await
        .map_err(|_| fail())?
        .get(0);
    let (mode, next) = schedule(&input, now)?;
    let timezone = validated_timezone(input.timezone.as_deref())?;
    let Expiry {
        at: expires_at,
        seconds: expiry_seconds,
        on_timeout,
    } = expiry(&input, now)?;
    let allowed: bool = tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent a WHERE a.id=$1 AND a.workspace_id=$2 AND a.archived_at IS NULL AND a.runtime_id IS NOT NULL AND (a.owner_id=$3 OR (a.permission_mode='public_to' AND EXISTS(SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (t.target_type='workspace' AND t.target_id=$2 OR t.target_type='member' AND t.target_id=$3)))))",
        &[&agent_id,&ws,&member],
    ).await.map_err(|_| fail())?.get(0);
    if !allowed {
        return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
    }
    if let Some(parent) = input.parent_comment_id {
        let present: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM comment WHERE id=$1 AND issue_id=$2 AND deleted_at IS NULL)", &[&parent,&issue])
            .await.map_err(|_| fail())?.get(0);
        if !present {
            return Err(invalid("comment not found"));
        }
    }
    let (filter_agent, filter_actor_type, filter_actor_id) =
        if input.kind == "event" {
            if let Some(actor_id) = input.filter_actor_id {
                let exists: bool = match input.filter_actor_type.as_deref() {
                Some("member") => tx.query_one(
                    "SELECT EXISTS(SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2)",
                    &[&actor_id,&ws],
                ).await,
                Some("agent") => tx.query_one(
                    "SELECT EXISTS(SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2)",
                    &[&actor_id,&ws],
                ).await,
                _ => unreachable!(),
            }.map_err(|_| fail())?.get(0);
                if !exists {
                    return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
                }
            }
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
                    return Err(error(StatusCode::FORBIDDEN, "wakeup permission denied"));
                }
            }
            // Frozen Go aliases mutation-only agent filters to actor filters.
            if let Some(filter) = input.filter_agent_id.filter(|_| {
                input
                    .event_types
                    .iter()
                    .all(|event| !event.starts_with("task."))
            }) {
                (None, Some("agent"), Some(filter))
            } else {
                (
                    input.filter_agent_id,
                    input.filter_actor_type.as_deref(),
                    input.filter_actor_id,
                )
            }
        } else {
            (None, None, None)
        };
    // Hold the source row through subscription registration. A concurrent
    // terminal status change either fires the trigger after this insert, or
    // precedes the lock and is materialized as a registration receipt below.
    let source_task = if let Some(task_id) = input.filter_task_id {
        let task = tx.query_opt(
            "SELECT id,agent_id,status,completed_at,retry_of_task_id,rerun_of_task_id FROM agent_task_queue WHERE id=$1 AND issue_id=$2 FOR UPDATE NOWAIT",
            &[&task_id, &issue],
        ).await.map_err(|_| fail())?.ok_or_else(|| invalid("run does not belong to this issue"))?;
        if input
            .filter_agent_id
            .is_some_and(|filter| filter != task.get::<_, Uuid>("agent_id"))
        {
            return Err(invalid("run and agent filters disagree"));
        }
        Some(task)
    } else {
        None
    };
    let id = Uuid::new_v4();
    tx.query_one(
        "INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,source_task_id,parent_comment_id,instruction,kind,mode,event_types,interval_seconds,timezone,next_fire_at,max_fires,expires_at,expiry_seconds,on_timeout,filter_agent_id,filter_actor_type,filter_actor_id,filter_task_id,cron_expression) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$21,$13,$14,$15,$16,$17,$18,$19,$20,$22,$23) RETURNING to_jsonb(issue_wakeup)",
        &[&id,&ws,&issue,&agent_id,&member,&source,&input.parent_comment_id,&instruction,&input.kind,&mode,&input.event_types,&(if input.kind == "every" { Some(input.interval_seconds) } else { None }),&next,&(if input.max_fires == 0 && input.kind == "event" && mode == "continuous" { Some(20) } else if input.max_fires == 0 { None } else { Some(input.max_fires) }),&expires_at,&expiry_seconds,&on_timeout,&filter_agent,&filter_actor_type,&filter_actor_id,&timezone,&input.filter_task_id,&(if input.kind == "cron" { input.cron_expression.as_deref() } else { None })],
    ).await.map_err(|e| {
        if e.as_db_error().is_some_and(|e| e.constraint() == Some("issue_wakeup_active_limit")) {
            error(StatusCode::BAD_REQUEST, "wakeup capacity exceeded")
        } else { fail() }
    })?;
    if let Some(task) = source_task {
        record_terminal_task_snapshot(
            &tx,
            TerminalSnapshot {
                wakeup_id: id,
                revision: 1,
                issue,
                workspace: ws,
                mode: &mode,
                event_types: &input.event_types,
                now,
                task: &task,
            },
        )
        .await?;
    }
    let rule: Value = tx
        .query_one("SELECT to_jsonb(w) FROM issue_wakeup w WHERE id=$1", &[&id])
        .await
        .map_err(|_| fail())?
        .get(0);
    let mut preview = wakeup_preview(&rule);
    if source.is_some() {
        preview["created_by_agent_id"] = json!(actor_id);
    }
    let details = json!({"wakeup":preview});
    let activity = tx
        .query_one(
            "INSERT INTO activity_log(id,workspace_id,issue_id,actor_type,actor_id,action,details) \
         VALUES($1,$2,$3,$4,$5,'wakeup_created',$6) RETURNING id,created_at",
            &[
                &Uuid::new_v4(),
                &ws,
                &issue,
                &actor_type,
                &actor_id,
                &details,
            ],
        )
        .await
        .map_err(|_| fail())?;
    let activity_id: Uuid = activity.get("id");
    let created: DateTime<Utc> = activity.get("created_at");
    tx.commit().await.map_err(|_| fail())?;
    sink.publish_wakeup_activity(IssueWakeupActivityEvent {
        workspace_id: ws,
        actor_id,
        payload: json!({"issue_id":issue,"entry":{
            "type":"activity","id":activity_id,"actor_type":actor_type,"actor_id":actor_id,
            "action":"wakeup_created","details":details,
            "created_at":created.to_rfc3339_opts(SecondsFormat::AutoSi,true),
        }}),
    });
    Ok((StatusCode::CREATED, GoJson(rule)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deadline_matches_go_expiry_contract() {
        let now = Utc::now();
        let base = json!({"instruction":"x","kind":"every","interval_seconds":60});
        let with = |extra: Value| {
            let mut input = base.clone();
            input
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            serde_json::from_value::<WakeupInput>(input).unwrap()
        };
        let relative = with(json!({"expires_in_seconds":3600}));
        let relative = expiry(&relative, now).unwrap();
        assert_eq!(relative.at, Some(now + Duration::hours(1)));
        assert_eq!(
            (relative.seconds, relative.on_timeout),
            (Some(3600), Some("end"))
        );
        let absolute = with(json!({"expires_at":(now + Duration::hours(2)).to_rfc3339()}));
        let absolute = expiry(&absolute, now).unwrap();
        assert_eq!((absolute.seconds, absolute.on_timeout), (None, Some("end")));
        for bad in [
            json!({"expires_in_seconds":59}),
            json!({"expires_in_seconds":31_536_001}),
            json!({"expires_at":(now - Duration::seconds(1)).to_rfc3339()}),
            json!({"expires_at":(now + Duration::days(366)).to_rfc3339()}),
            json!({"expires_in_seconds":3600,"expires_at":(now + Duration::hours(1)).to_rfc3339()}),
            json!({"on_timeout":"end"}),
            json!({"expires_in_seconds":3600,"on_timeout":"wake"}),
            json!({"expires_in_seconds":3600,"on_timeout":"unknown"}),
            json!({"kind":"at","after_seconds":120,"expires_in_seconds":3600}),
        ] {
            assert_eq!(
                expiry(&with(bad), now).unwrap_err().0,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[test]
    fn event_subscription_matches_go_whitelist_and_default_mode() {
        let now = Utc::now();
        for event in EVENT_TYPES {
            let value = json!({"instruction":"watch","kind":"event","event_types":[event]});
            let input: WakeupInput = serde_json::from_value(value).unwrap();
            assert_eq!(schedule(&input, now).unwrap(), ("once".into(), None));
        }
        for bad in [
            "issue.created",
            "issue.deleted",
            "task.running",
            "comment:created",
        ] {
            let value = json!({"instruction":"watch","kind":"event","event_types":[bad]});
            let input: WakeupInput = serde_json::from_value(value).unwrap();
            assert_eq!(
                schedule(&input, now).unwrap_err().0,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[test]
    fn event_fire_limit_and_zone_validation() {
        let now = Utc::now();
        let base = json!({"instruction":"watch","kind":"event","event_types":["comment.created"]});
        let input = |extra: Value| {
            let mut value = base.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            serde_json::from_value::<WakeupInput>(value).unwrap()
        };
        assert_eq!(
            schedule(
                &input(json!({"mode":"continuous","max_fires":1000,"timezone":"Asia/Ho_Chi_Minh"})),
                now
            )
            .unwrap()
            .0,
            "continuous"
        );
        for bad in [
            json!({"mode":"once","max_fires":1}),
            json!({"mode":"continuous","max_fires":1001}),
            json!({"mode":"continuous","max_fires":-1}),
            json!({"timezone":"../etc/passwd"}),
            json!({"timezone":"Unreal/Nowhere"}),
            json!({"timezone":"zone.tab"}),
        ] {
            assert_eq!(
                schedule(&input(bad), now).unwrap_err().0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(validated_timezone(None).unwrap(), "UTC");
        assert_eq!(validated_timezone(Some("UTC")).unwrap(), "UTC");
    }

    #[test]
    fn task_filters_require_task_events() {
        let now = Utc::now();
        let input: WakeupInput = serde_json::from_value(json!({
            "instruction":"watch","kind":"event","event_types":["task.completed"],
            "filter_task_id":Uuid::new_v4()
        }))
        .unwrap();
        assert_eq!(schedule(&input, now).unwrap(), ("once".into(), None));
        let mixed: WakeupInput = serde_json::from_value(json!({
            "instruction":"watch","kind":"event","event_types":["task.completed","comment.created"],
            "filter_task_id":Uuid::new_v4()
        }))
        .unwrap();
        assert_eq!(
            schedule(&mixed, now).unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_unimplemented_scheduler_contracts() {
        let base =
            json!({"agent_id":Uuid::new_v4(),"instruction":" Go ","kind":"at","after_seconds":60});
        let input: WakeupInput = serde_json::from_value(base.clone()).unwrap();
        assert_eq!(schedule(&input, Utc::now()).unwrap().0, "once");
        for change in [
            json!({"condition":{"type":"issue_status"}}),
            json!({"condition":{"type":"issue_status","status":"done"}}),
        ] {
            let mut value = base.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(change.as_object().unwrap().clone());
            let input: WakeupInput = serde_json::from_value(value).unwrap();
            assert_eq!(
                schedule(&input, Utc::now()).unwrap_err().0,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
    }
}
