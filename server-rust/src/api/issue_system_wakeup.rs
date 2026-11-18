//! Issue-level child-done system rule read from the frozen Go handler contract.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error,
    issue_read::load_issue, issue_write::IssueTaskCancelledEvent, squad::legacy_prefix,
    system_wakeup,
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

#[derive(Serialize)]
struct Target {
    #[serde(rename = "type")]
    kind: &'static str,
    id: Uuid,
    name: String,
}

#[derive(Serialize)]
struct Rule {
    id: String,
    revision: i64,
    rule: &'static str,
    enabled: bool,
    instruction: String,
    default_instruction: String,
    customized: bool,
    paused_reason: Option<String>,
    staged: bool,
    stage: Option<i32>,
    total: usize,
    remaining: usize,
    waiting: Vec<String>,
    target: Option<Target>,
    blocked: &'static str,
    workspace_default: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SystemWakeupInput {
    pub enabled: Option<bool>,
    pub instruction: Option<String>,
}

pub(super) fn parse_input(body: &[u8]) -> Result<SystemWakeupInput, ApiError> {
    if body.len() > 16_384 {
        return Err(error(StatusCode::BAD_REQUEST, "invalid system wakeup body"));
    }
    // Go's Decoder.Decode reads the first value, without requiring EOF after it.
    Option::<SystemWakeupInput>::deserialize(&mut serde_json::Deserializer::from_slice(body))
        .map(Option::unwrap_or_default)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid system wakeup body"))
}

pub(super) fn save_failure() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "could not save system wakeup",
    )
}

pub(super) async fn human_role(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<String, ApiError> {
    if !matches!(actor, Actor::Human) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only members can change system wakeups",
        ));
    }
    db.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .map_err(|_| error(StatusCode::FORBIDDEN, "wakeup permission denied"))?
    .map(|row| row.get(0))
    .ok_or_else(|| error(StatusCode::FORBIDDEN, "wakeup permission denied"))
}

// A newly enabled rule must not fire for a stage already closed while it was off.
// For initial creation only, pending closed-child events are treated as open:
// the writer recording that closure may not have dispatched it yet.
pub(super) async fn baseline(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    rule: Uuid,
    pending_as_open: bool,
) -> Result<(), ApiError> {
    let pending: HashSet<Uuid> = if pending_as_open {
        tx.query("SELECT DISTINCT child_id FROM issue_child_event WHERE parent_id=$1 AND kind='closed' AND processed_at IS NULL", &[&issue])
            .await.map_err(|_| save_failure())?.iter().map(|r| r.get(0)).collect()
    } else {
        HashSet::new()
    };
    let children = tx.query("SELECT id,status,stage FROM issue WHERE parent_issue_id=$1 AND workspace_id=$2 ORDER BY id", &[&issue, &ws])
        .await.map_err(|_| save_failure())?;
    let mut stages: BTreeMap<i32, (usize, usize)> = BTreeMap::new();
    let mut closed = 0usize;
    for child in &children {
        let id: Uuid = child.get("id");
        let status: String = child.get("status");
        let category = category(tx, ws, &status)
            .await
            .map_err(|_| save_failure())?;
        let done = !pending.contains(&id) && matches!(category.as_str(), "done" | "closed");
        if done {
            closed += 1;
        }
        if let Some(stage) = child.get::<_, Option<i32>>("stage") {
            let count = stages.entry(stage).or_default();
            count.0 += 1;
            if done {
                count.1 += 1;
            }
        }
    }
    let mut fingerprint = String::new();
    if !children.is_empty() && closed == children.len() {
        fingerprint = "all".into();
    } else {
        let mut frontier = None;
        for (stage, (total, done)) in stages {
            if done < total {
                if let Some(closed_stage) = frontier {
                    fingerprint = format!("stage:{closed_stage}");
                }
                break;
            }
            frontier = Some(stage);
        }
    }
    tx.execute("UPDATE issue_wakeup SET condition_state=$2,next_fire_at=NULL,updated_at=clock_timestamp() WHERE id=$1", &[&rule,&fingerprint])
        .await.map_err(|_| save_failure())?;
    Ok(())
}

pub(super) async fn ensure_rule(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
) -> Result<(Uuid, bool, String), ApiError> {
    if let Some(row) = tx.query_opt("SELECT id,enabled,instruction FROM issue_wakeup WHERE issue_id=$1 AND system_rule='child_done'", &[&issue])
        .await.map_err(|_| save_failure())? {
        return Ok((row.get("id"), row.get("enabled"), row.get("instruction")));
    }
    let settings: Value = tx
        .query_one("SELECT settings FROM workspace WHERE id=$1", &[&ws])
        .await
        .map_err(|_| save_failure())?
        .get(0);
    let (enabled, _) = system_wakeup::default(&settings);
    let id = Uuid::new_v4();
    let row = tx.query_opt("INSERT INTO issue_wakeup(id,workspace_id,issue_id,instruction,kind,mode,event_types,timezone,condition,condition_state,enabled,system_rule) VALUES($1,$2,$3,'','event','continuous',ARRAY['issue.status_changed'],'UTC','{\"type\":\"children_done\",\"each_stage\":true}'::jsonb,'',$4,'child_done') ON CONFLICT (issue_id,system_rule) WHERE system_rule IS NOT NULL DO NOTHING RETURNING id", &[&id,&ws,&issue,&enabled])
        .await.map_err(|_| save_failure())?;
    if row.is_some() {
        baseline(tx, ws, issue, id, true).await?;
        Ok((id, enabled, String::new()))
    } else {
        let row = tx.query_one("SELECT id,enabled,instruction FROM issue_wakeup WHERE issue_id=$1 AND system_rule='child_done'", &[&issue])
            .await.map_err(|_| save_failure())?;
        Ok((row.get("id"), row.get("enabled"), row.get("instruction")))
    }
}

pub(super) async fn put(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, rule)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let mut db = state.pool.get().await.map_err(|_| save_failure())?;
    let issue: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    if rule != "child_done" {
        return Err(error(StatusCode::NOT_FOUND, "system wakeup not found"));
    }
    let input = parse_input(&body)?;
    human_role(&**db, ws, user, &actor).await?;
    let tx = db.transaction().await.map_err(|_| save_failure())?;
    tx.query_one(
        "SELECT id FROM issue WHERE id=$1 FOR NO KEY UPDATE",
        &[&issue],
    )
    .await
    .map_err(|_| save_failure())?;
    let (id, was_enabled, prior_instruction) = ensure_rule(&tx, ws, issue).await?;
    tx.query_one("SELECT id FROM issue_wakeup WHERE id=$1 FOR UPDATE", &[&id])
        .await
        .map_err(|_| save_failure())?;
    let enabled = input.enabled.unwrap_or(was_enabled);
    let instruction = input
        .instruction
        .map(|s| s.trim().to_owned())
        .unwrap_or(prior_instruction);
    if instruction.len() > 4000 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: instruction must be at most 4000 bytes",
        ));
    }
    tx.execute("UPDATE issue_wakeup SET enabled=$2,instruction=$3,customized_at=clock_timestamp(),paused_reason=CASE WHEN $2::bool THEN NULL ELSE paused_reason END,disabled_at=CASE WHEN $2::bool THEN NULL ELSE disabled_at END,updated_at=clock_timestamp() WHERE id=$1", &[&id,&enabled,&instruction])
        .await.map_err(|_| save_failure())?;
    if enabled && !was_enabled {
        baseline(&tx, ws, issue, id, false).await?;
    }
    let mut cancelled = Vec::new();
    if !enabled {
        tx.execute("UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL", &[&id])
            .await.map_err(|_| save_failure())?;
        cancelled = tx.query("UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error='Wakeup disabled or updated' WHERE context->>'wakeup_id'=$1 AND status IN ('queued','deferred') AND started_at IS NULL RETURNING id,agent_id,issue_id", &[&id.to_string()])
            .await.map_err(|_| save_failure())?
            .into_iter().map(|r| (r.get::<_, Uuid>(0),r.get::<_, Uuid>(1),r.get::<_, Option<Uuid>>(2))).collect();
    }
    tx.commit().await.map_err(|_| save_failure())?;
    if let Some(sink) = &state.issue_events {
        for (task_id, agent_id, issue_id) in cancelled {
            sink.publish_task_cancelled(IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id,
                agent_id,
                issue_id,
            });
        }
    }
    // Go re-reads the public projection after the commit, including its
    // special empty-array response when the parent or all children are closed.
    list(State(state), Extension(WorkspaceId(ws)), Path(raw)).await
}

fn failure() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "could not load system wakeups",
    )
}

// Built-in behaviors resolve without a catalog query. Custom keys must be
// present in the catalog; an unresolved status cannot be treated as open.
pub(super) async fn category(
    db: &impl GenericClient,
    ws: Uuid,
    status: &str,
) -> Result<String, ApiError> {
    let builtin = match status {
        "backlog" | "todo" => Some("unstarted"),
        "in_progress" | "in_review" | "blocked" => Some("started"),
        "done" => Some("done"),
        "cancelled" => Some("closed"),
        _ => None,
    };
    if let Some(category) = builtin {
        return Ok(category.into());
    }
    db.query_opt(
        "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
        &[&ws, &status],
    )
    .await
    .map_err(|_| failure())?
    .map(|row| row.get(0))
    .ok_or_else(failure)
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state.pool.get().await.map_err(|_| failure())?;
    // The shared lookup accepts either UUID or the selected workspace's prefix-number.
    let id: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let parent = db
        .query_one(
            "SELECT status, assignee_type, assignee_id FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failure())?;
    let status: String = parent.get("status");
    if matches!(
        category(&**db, ws, &status).await?.as_str(),
        "done" | "closed"
    ) {
        return Ok(GoJson(json!([])));
    }
    let children = db.query(
        "SELECT status, stage, number FROM issue WHERE parent_issue_id=$1 AND workspace_id=$2 ORDER BY number ASC",
        &[&id, &ws],
    ).await.map_err(|_| failure())?;
    if children.is_empty() {
        return Ok(GoJson(json!([])));
    }
    let mut open = Vec::new();
    let mut lowest_stage: Option<i32> = None;
    for child in &children {
        let status: String = child.get("status");
        if !matches!(
            category(&**db, ws, &status).await?.as_str(),
            "done" | "closed"
        ) {
            let stage: Option<i32> = child.get("stage");
            if let Some(stage) = stage {
                lowest_stage = Some(lowest_stage.map_or(stage, |current| current.min(stage)));
            }
            open.push(child);
        }
    }
    if open.is_empty() {
        return Ok(GoJson(json!([])));
    }
    let total = children
        .iter()
        .filter(|child| {
            lowest_stage.is_none_or(|stage| child.get::<_, Option<i32>>("stage") == Some(stage))
        })
        .count();
    let in_scope: Vec<_> = open
        .into_iter()
        .filter(|child| {
            lowest_stage.is_none_or(|stage| child.get::<_, Option<i32>>("stage") == Some(stage))
        })
        .collect();
    let ws_row = db
        .query_one(
            "SELECT name, issue_prefix, settings FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .map_err(|_| failure())?;
    let prefix: String = ws_row.get("issue_prefix");
    let prefix = if prefix.is_empty() {
        legacy_prefix(ws_row.get("name"))
    } else {
        prefix
    };
    let waiting = in_scope
        .iter()
        .take(5)
        .map(|child| format!("{prefix}-{}", child.get::<_, i32>("number")))
        .collect();
    let settings = ws_row.get("settings");
    let (workspace_default, workspace_instruction) = system_wakeup::default(&settings);
    let default_instruction = if workspace_instruction.is_empty() {
        system_wakeup::CHILD_DONE_INSTRUCTION.to_owned()
    } else {
        workspace_instruction
    };
    let rule = db.query_opt(
        "SELECT id, revision, enabled, instruction, customized_at IS NOT NULL AS customized, paused_reason FROM issue_wakeup WHERE issue_id=$1 AND system_rule='child_done'",
        &[&id],
    ).await.map_err(|_| failure())?;
    let assignee_type: Option<String> = parent.get("assignee_type");
    let assignee_id: Option<Uuid> = parent.get("assignee_id");
    let target = match (assignee_type.as_deref(), assignee_id) {
        (Some("member"), Some(assignee)) => db
            .query_opt("SELECT name FROM \"user\" WHERE id=$1", &[&assignee])
            .await
            .ok()
            .flatten()
            .map(|row| Target {
                kind: "member",
                id: assignee,
                name: row.get(0),
            }),
        (Some("agent"), Some(assignee)) => db
            .query_opt(
                "SELECT name FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user'",
                &[&assignee, &ws],
            )
            .await
            .ok()
            .flatten()
            .map(|row| Target {
                kind: "agent",
                id: assignee,
                name: row.get(0),
            }),
        (Some("squad"), Some(assignee)) => db
            .query_opt(
                "SELECT name FROM squad WHERE id=$1 AND workspace_id=$2",
                &[&assignee, &ws],
            )
            .await
            .ok()
            .flatten()
            .map(|row| Target {
                kind: "squad",
                id: assignee,
                name: row.get(0),
            }),
        _ => None,
    };
    let blocked = if assignee_type.is_none() || assignee_id.is_none() || target.is_none() {
        if assignee_type.as_deref() == Some("member") && assignee_id.is_some() {
            "member_assignee"
        } else {
            "no_assignee"
        }
    } else if assignee_type.as_deref() == Some("member") {
        "member_assignee"
    } else if status == "backlog" {
        "backlog"
    } else {
        ""
    };
    Ok(GoJson(json!([Rule {
        id: rule
            .as_ref()
            .map(|row| row.get::<_, Uuid>("id").to_string())
            .unwrap_or_default(),
        revision: rule.as_ref().map_or(0, |row| row.get("revision")),
        rule: "child_done",
        enabled: rule
            .as_ref()
            .map_or(workspace_default, |row| row.get("enabled")),
        instruction: rule
            .as_ref()
            .map_or(String::new(), |row| row.get("instruction")),
        default_instruction,
        customized: rule.as_ref().is_some_and(|row| row.get("customized")),
        paused_reason: rule.as_ref().and_then(|row| row.get("paused_reason")),
        staged: lowest_stage.is_some(),
        stage: lowest_stage,
        total,
        remaining: in_scope.len(),
        waiting,
        target,
        blocked,
        workspace_default,
    }])))
}
