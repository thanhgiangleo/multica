//! P0 daemon endpoints staged for integration. The parent daemon router must
//! apply `daemon::authenticate` to these routes: they accept Bearer credentials
//! only and MUST NOT be merged under the browser-session router.
use super::{
    ApiError, ApiState, GoJson,
    daemon::DaemonIdentity,
    daemon_claim_build::require_daemon_runtime_access,
    error, json,
    p0_daemon_outcomes::{CREATE_RETRY, retry_budget, settle},
    runtime_chat_daemon::{reconcile_agent, system_event, task_access},
    runtime_chat_task, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tokio_postgres::Row;
use uuid::Uuid;

/// Merge only inside the Bearer-only daemon router (before its auth route layer).
pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/daemon/deregister", post(deregister))
        .route(
            "/api/daemon/runtimes/{runtimeId}/tasks/pending",
            get(list_pending_tasks),
        )
        .route(
            "/api/daemon/runtimes/{runtimeId}/recover-orphans",
            post(recover_orphans),
        )
        .route(
            "/api/daemon/tasks/{taskId}/messages",
            get(list_task_messages),
        )
}

async fn list_pending_tasks(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?;
    let runtime = require_daemon_runtime_access(&**db, &identity, &raw).await?;
    let tasks = db.query(
        "SELECT * FROM agent_task_queue WHERE runtime_id=$1 AND status IN ('queued','dispatched') ORDER BY priority DESC,created_at ASC",
        &[&runtime.id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list pending tasks"))?;
    Ok(GoJson(
        tasks
            .iter()
            .map(|task| {
                let mut response = runtime_chat_task::response(task, runtime.workspace_id);
                // Go taskToResponse formats these in the process-local timezone
                // at whole-second RFC3339 precision, unlike the shared builder.
                response["created_at"] = json!(
                    task.get::<_, Option<DateTime<Utc>>>("created_at")
                        .map(timestamp)
                        .unwrap_or_default()
                );
                for field in ["dispatched_at", "started_at", "completed_at"] {
                    response[field] = task
                        .get::<_, Option<DateTime<Utc>>>(field)
                        .map(|at| json!(timestamp(at)))
                        .unwrap_or(Value::Null);
                }
                response
            })
            .collect::<Vec<_>>(),
    ))
}

// Go's recovery write and receipt settlement share a transaction; retry and
// notifications happen only after the failed rows are committed.
async fn recover_orphans(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?;
    let runtime = require_daemon_runtime_access(&**db, &identity, &raw).await?;
    let failed = || error(StatusCode::INTERNAL_SERVER_ERROR, "recover orphans failed");
    let tx = db.transaction().await.map_err(|_| failed())?;
    let tasks = tx.query(
        "UPDATE agent_task_queue SET status='failed',completed_at=now(),error='daemon restarted while task was in flight',failure_reason='runtime_recovery',wait_reason=NULL,prepare_lease_expires_at=NULL WHERE runtime_id=$1 AND status IN ('dispatched','running','waiting_local_directory') RETURNING *",
        &[&runtime.id],
    ).await.map_err(|_| failed())?;
    for task in &tasks {
        settle(&tx, task.get("id")).await.map_err(|_| failed())?;
    }
    tx.commit().await.map_err(|_| failed())?;

    let mut retried = 0;
    let mut retried_issues = HashSet::new();
    let mut processed_issues = HashSet::new();
    let mut agents = HashSet::new();
    for task in &tasks {
        let id: Uuid = task.get("id");
        let issue: Option<Uuid> = task.get("issue_id");
        let agent: Uuid = task.get("agent_id");
        agents.insert(agent);
        let mut retry_pending = false;
        if let Some((ceiling, delay)) = retry_budget(task, "runtime_recovery") {
            let thread: Option<Uuid> = task.get("comment_thread_id");
            let successor = if let Some(issue) = issue {
                db.query_one("SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND comment_thread_id IS NOT DISTINCT FROM $3 AND id<>$4 AND status IN ('queued','dispatched'))",
                    &[&issue,&agent,&thread,&id]).await.ok().is_some_and(|r| r.get::<_,bool>(0))
            } else {
                false
            };
            if !successor {
                match db
                    .query_opt(CREATE_RETRY, &[&id, &ceiling, &delay, &Uuid::now_v7()])
                    .await
                {
                    Ok(Some(child)) => {
                        retry_pending = true;
                        retried += 1;
                        if let Some(issue) = issue {
                            retried_issues.insert(issue);
                        }
                        if delay == 0 {
                            state.wake_daemon(child.get("runtime_id"), Some(child.get("id")));
                        }
                    }
                    Ok(None) => {}
                    Err(err) => eprintln!("failed to retry recovered task {id}: {err}"),
                }
            }
        }
        let workspace = runtime.workspace_id;
        if !retry_pending {
            if let Some(issue_id) = issue {
                let trigger: Option<Uuid> = task.get("trigger_comment_id");
                if let Err(err) = db.execute(
                    "INSERT INTO comment(id,issue_id,workspace_id,author_type,author_id,content,type,parent_id,source_task_id) VALUES ($1,$2,$3,'agent',$4,$5,'system',$6,$7)",
                    &[&Uuid::now_v7(),&issue_id,&workspace,&agent,&"daemon restarted while task was in flight",&trigger,&id],
                ).await { eprintln!("failed to comment on recovered task {id}: {err}"); }
            }
        }
        if let Some(issue_id) = issue {
            if !retried_issues.contains(&issue_id) && processed_issues.insert(issue_id) {
                let reset = db.execute(
                    "UPDATE issue SET status='todo',updated_at=now() WHERE id=$1 AND workspace_id=$2 AND status='in_progress' AND NOT EXISTS (SELECT 1 FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory'))",
                    &[&issue_id,&workspace],
                ).await;
                match reset {
                    Ok(1) => state.events.publish(system_event("issue:updated",workspace,None,None,
                        json!({"issue_id":issue_id,"status":"todo","previous_status":"in_progress"}))),
                    Err(err) => eprintln!("failed to reset recovered issue {issue_id}: {err}"),
                    _ => {},
                }
            }
        }
        state.events.publish(system_event("task:failed",workspace,task.get("chat_session_id"),None,
            json!({"task_id":id,"issue_id":issue,"agent_id":agent,"error":"daemon restarted while task was in flight","failure_reason":"runtime_recovery","retry_pending":retry_pending})));
    }
    for agent in agents {
        if let Err(err) = reconcile_agent(&**db, agent).await {
            eprintln!("failed to reconcile recovered agent {agent}: {err:?}");
        }
    }
    if !tasks.is_empty() {
        state.wake_daemon(Some(runtime.id), None);
    }
    Ok(GoJson(json!({"orphaned":tasks.len(),"retried":retried})))
}

#[derive(Default, Deserialize)]
struct DeregisterRequest {
    #[serde(default)]
    runtime_ids: Option<Vec<String>>,
    #[serde(default)]
    offline_reasons: Option<HashMap<String, Value>>,
}

type DeregisterInput = (Vec<(String, Uuid)>, HashMap<String, Value>);

fn parse_deregister(body: &[u8]) -> Result<DeregisterInput, ApiError> {
    let req: Option<DeregisterRequest> = serde_json::from_slice(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req = req.unwrap_or_default();
    let ids = req.runtime_ids.unwrap_or_default();
    if ids.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "runtime_ids is required"));
    }
    let ids = ids
        .into_iter()
        .map(|raw| {
            let id = Uuid::parse_str(&raw)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_ids"))?;
            Ok((raw, id))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok((ids, req.offline_reasons.unwrap_or_default()))
}

// Individual missing/unauthorized runtimes and failed offline writes are
// intentionally skipped, as in Go; a failed batch lookup is NOT an empty set.
async fn deregister(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let (ids, reasons) = parse_deregister(&body)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtimes"))?;
    let uuid_ids: Vec<_> = ids.iter().map(|(_, id)| *id).collect();
    let rows = db
        .query(
            "SELECT id,workspace_id,daemon_id,owner_id FROM agent_runtime WHERE id=ANY($1::uuid[])",
            &[&uuid_ids],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtimes"))?;
    let by_id: HashMap<Uuid, &Row> = rows.iter().map(|row| (row.get(0), row)).collect();
    let mut affected = HashSet::new();
    for (raw, id) in &ids {
        let Some(rt) = by_id.get(id) else { continue };
        let workspace: Uuid = rt.get(1);
        if !can_deregister(
            &**db,
            &identity,
            workspace,
            rt.get::<_, Option<String>>(2).as_deref(),
            rt.get(3),
        )
        .await
        {
            continue;
        }
        let reason = reasons.get(raw);
        let updated = if let Some(reason) = reason {
            db.execute("UPDATE agent_runtime SET status='offline', metadata=metadata || jsonb_build_object('offline_reason',$2::jsonb),updated_at=now() WHERE id=$1", &[id, reason]).await
        } else {
            db.execute(
                "UPDATE agent_runtime SET status='offline',updated_at=now() WHERE id=$1",
                &[id],
            )
            .await
        };
        if updated.is_err() {
            continue;
        }
        affected.insert(workspace);
    }
    for workspace in affected {
        state.events.publish(super::runtime_chat_events::Event {
            event_type: "daemon:register".into(),
            workspace_id: workspace,
            actor_type: "system".into(),
            actor_id: Uuid::nil(),
            chat_session_id: None,
            chat_session_owner_id: None,
            payload: json!({"action":"deregister"}),
        });
    }
    Ok(GoJson(json!({"status":"ok"})))
}

async fn can_deregister(
    db: &impl tokio_postgres::GenericClient,
    identity: &DaemonIdentity,
    workspace: Uuid,
    daemon: Option<&str>,
    owner: Option<Uuid>,
) -> bool {
    match identity {
        DaemonIdentity::User(user) => {
            owner == Some(*user)
                && db
                    .query_opt(
                        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                        &[&workspace, user],
                    )
                    .await
                    .ok()
                    .flatten()
                    .is_some()
        }
        DaemonIdentity::Bound {
            workspace: bound,
            daemon: bound_daemon,
            owner: bound_owner,
            hash,
        } => {
            if *bound != workspace
                || bound_daemon.trim().is_empty()
                || daemon != Some(bound_daemon.as_str())
                || (!bound_daemon.starts_with("qoder-managed-")
                    && (bound_owner.is_none() || *bound_owner != owner))
            {
                return false;
            }
            if bound_daemon.starts_with("qoder-managed-") {
                return db.query_one(
                    "SELECT EXISTS(SELECT 1 FROM cloud_agent_connection c JOIN daemon_token d ON d.id=c.token_id JOIN member m ON m.workspace_id=c.workspace_id AND m.user_id=c.owner_id WHERE c.provider='qoder_cloud' AND c.workspace_id=$1 AND c.enabled AND d.daemon_id=$2 AND d.token_hash=$3 AND d.expires_at>now() AND m.role IN ('owner','admin'))",
                    &[&workspace, bound_daemon, hash],
                ).await.ok().is_some_and(|r| r.get::<_, bool>(0));
            }
            true
        }
    }
}

#[derive(Default, Deserialize)]
struct SinceQuery {
    since: Option<String>,
}

fn parse_since(query: &SinceQuery) -> Result<Option<i32>, ApiError> {
    match query.since.as_deref().filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(raw) => {
            // Go strconv.Atoi takes signed native integers; pgx's int32 bind
            // subsequently narrows the parsed value.
            let parsed = raw
                .parse::<i64>()
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid since parameter"))?;
            Ok(Some(parsed as i32))
        }
    }
}

#[derive(Serialize)]
struct TaskMessage {
    task_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    issue_id: String,
    seq: i32,
    #[serde(rename = "type")]
    kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    tool: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    call_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    content: String,
    #[serde(skip_serializing_if = "empty_input")]
    input: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_truncated: Option<bool>,
    created_at: String,
}

fn empty_input(input: &Option<Value>) -> bool {
    input
        .as_ref()
        .is_none_or(|value| value.as_object().is_none_or(|map| map.is_empty()))
}

fn task_message(row: &Row, task_id: &str, issue_id: &str) -> TaskMessage {
    TaskMessage {
        task_id: task_id.into(),
        issue_id: issue_id.into(),
        seq: row.get("seq"),
        kind: row.get("type"),
        tool: row.get::<_, Option<String>>("tool").unwrap_or_default(),
        call_id: row.get::<_, Option<String>>("call_id").unwrap_or_default(),
        content: row.get::<_, Option<String>>("content").unwrap_or_default(),
        input: row.get("input"),
        output: row.get::<_, Option<String>>("output").unwrap_or_default(),
        output_truncated: row.get("output_truncated"),
        created_at: row
            .get::<_, DateTime<Utc>>("created_at")
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
    }
}

async fn list_task_messages(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    Query(query): Query<SinceQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let (task, _, _) = task_access(&**db, &identity, id).await?;
    let since = parse_since(&query)?;
    let messages = match since {
        Some(seq) => {
            db.query(
                "SELECT * FROM task_message WHERE task_id=$1 AND seq>$2 ORDER BY seq ASC",
                &[&id, &seq],
            )
            .await
        }
        None => {
            db.query(
                "SELECT * FROM task_message WHERE task_id=$1 ORDER BY seq ASC",
                &[&id],
            )
            .await
        }
    }
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list task messages",
        )
    })?;
    let issue = task
        .get::<_, Option<Uuid>>("issue_id")
        .map(|u| u.to_string())
        .unwrap_or_default();
    Ok(GoJson(
        messages
            .iter()
            .map(|m| task_message(m, &raw, &issue))
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deregister_validation_and_reason_case() {
        for (body, expected) in [
            ("{}", "runtime_ids is required"),
            ("{", "invalid request body"),
            ("{\"runtime_ids\":[\"x\"]}", "invalid runtime_ids"),
        ] {
            assert_eq!(parse_deregister(body.as_bytes()).err().unwrap().1, expected);
        }
        let id = Uuid::new_v4();
        let body = format!(
            "{{\"runtime_ids\":[\"{id}\"],\"offline_reasons\":{{\"{id}\":{{\"code\":\"offline\"}}}}}}"
        );
        let (ids, reasons) = parse_deregister(body.as_bytes()).unwrap();
        assert_eq!(ids[0].1, id);
        assert_eq!(reasons[&id.to_string()]["code"], "offline");
    }
    #[test]
    fn message_since_and_shape() {
        assert_eq!(parse_since(&SinceQuery { since: None }).unwrap(), None);
        assert_eq!(
            parse_since(&SinceQuery {
                since: Some("-1".into())
            })
            .unwrap(),
            Some(-1)
        );
        assert_eq!(
            parse_since(&SinceQuery {
                since: Some("bad".into())
            })
            .err()
            .unwrap()
            .1,
            "invalid since parameter"
        );
        let encoded = serde_json::to_value(TaskMessage {
            task_id: "t".into(),
            issue_id: String::new(),
            seq: 1,
            kind: "text".into(),
            tool: String::new(),
            call_id: String::new(),
            content: "hi".into(),
            input: None,
            output: String::new(),
            output_truncated: None,
            created_at: "now".into(),
        })
        .unwrap();
        assert_eq!(
            encoded,
            json!({"task_id":"t","seq":1,"type":"text","content":"hi","created_at":"now"})
        );
    }
}
