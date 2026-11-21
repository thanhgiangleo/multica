//! Daemon terminal callbacks. Merge `routes()` only inside the Bearer-only daemon router.
//! Transactional status CAS ensures a replay cannot deliver a second chat outcome.
use super::{
    ApiError, ApiState, GoJson,
    daemon::DaemonIdentity,
    error,
    runtime_chat_daemon::{reconcile_agent, system_event, task_access},
    runtime_chat_task,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json as json_value};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/daemon/tasks/{taskId}/complete", post(complete))
        .route("/api/daemon/tasks/{taskId}/fail", post(fail))
        .route("/api/daemon/tasks/{taskId}/cancel-ack", post(cancel_ack))
}

fn internal(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))
}
fn scrub(s: &mut String) {
    s.retain(|c| c != '\0');
}
fn optional(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

#[derive(Default, Deserialize, Serialize, Clone)]
#[serde(default)]
struct CompleteRequest {
    pr_url: String,
    output: String,
    session_id: String,
    work_dir: String,
    durable_work_dir: String,
    branch_name: String,
    session_rollout_missing: bool,
    retired_session_id: String,
}
#[derive(Default, Deserialize, Serialize, Clone)]
#[serde(default)]
struct FailRequest {
    error: String,
    session_id: String,
    work_dir: String,
    durable_work_dir: String,
    failure_reason: String,
    branch_name: String,
    session_rollout_missing: bool,
    retired_session_id: String,
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct AckRequest {
    process_cleanup_confirmed: Option<bool>,
    branch_name: String,
    durable_work_dir: String,
    error_message: String,
    failure_reason: String,
    retired_session_id: String,
}
fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ApiError> {
    // Go accepts a JSON null object, but rejects an empty or malformed body.
    if bytes.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
    }
    serde_json::from_slice(bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}
impl CompleteRequest {
    fn sanitize(&mut self) {
        for s in [
            &mut self.pr_url,
            &mut self.output,
            &mut self.session_id,
            &mut self.work_dir,
            &mut self.durable_work_dir,
            &mut self.branch_name,
            &mut self.retired_session_id,
        ] {
            scrub(s);
        }
    }
    fn context_exhausted(&self) -> bool {
        let s = self.output.trim();
        if s.is_empty() || s.len() > 320 {
            return false;
        }
        let s = s.to_ascii_lowercase();
        (s.contains("prompt is too long") && s.contains("cannot be compacted"))
            || (s.contains("conversation too long") && s.contains("press esc twice"))
            || (s.contains("compaction failed") && s.contains("reduced below the context limit"))
    }
}
impl FailRequest {
    fn sanitize(&mut self) {
        for s in [
            &mut self.error,
            &mut self.session_id,
            &mut self.work_dir,
            &mut self.durable_work_dir,
            &mut self.failure_reason,
            &mut self.branch_name,
            &mut self.retired_session_id,
        ] {
            scrub(s);
        }
    }
}
impl AckRequest {
    fn sanitize(&mut self) {
        for s in [
            &mut self.error_message,
            &mut self.failure_reason,
            &mut self.branch_name,
            &mut self.durable_work_dir,
            &mut self.retired_session_id,
        ] {
            scrub(s);
        }
    }
}

// Keep chat_session -> task row lock order consistent with the cancel endpoint.
pub(super) async fn lock_chat(tx: &Transaction<'_>, task: Uuid) -> Result<(), ApiError> {
    tx.query_opt("SELECT cs.id FROM chat_session cs JOIN agent_task_queue t ON t.chat_session_id=cs.id WHERE t.id=$1 FOR UPDATE OF cs", &[&task])
        .await.map_err(|_| internal("failed to update task"))?;
    Ok(())
}
pub(super) async fn settle(tx: &Transaction<'_>, task: Uuid) -> Result<(), ApiError> {
    tx.execute("UPDATE task_supplement s SET status='failed',failure_reason='turn_ended',updated_at=now() FROM agent_task_queue t WHERE s.task_id=t.id AND t.id=$1 AND t.status IN ('completed','failed','cancelled') AND s.status IN ('pending','delivering')", &[&task])
        .await.map_err(|_| internal("failed to settle task"))?;
    tx.execute("UPDATE comment c SET recovery_settled_at=now() FROM agent_task_queue t WHERE t.id=$1 AND t.status IN ('completed','failed','cancelled') AND c.id=ANY(t.delivered_comment_ids) AND c.author_type='system' AND c.type='progress_update' AND c.source_task_id IS NOT NULL AND c.recovery_settled_at IS NULL", &[&task])
        .await.map_err(|_| internal("failed to settle task"))?;
    Ok(())
}
pub(super) async fn clear_retired(
    tx: &Transaction<'_>,
    task: &Row,
    retired: &str,
) -> Result<(), ApiError> {
    let Some(chat) = task.get::<_, Option<Uuid>>("chat_session_id") else {
        return Ok(());
    };
    if !retired.is_empty() {
        tx.execute("UPDATE chat_session SET session_id=NULL,runtime_id=NULL,updated_at=now() WHERE id=$1 AND session_id=$2 AND runtime_id=$3",
            &[&chat, &retired, &task.get::<_, Option<Uuid>>("runtime_id")])
            .await.map_err(|_| internal("failed to retire chat session"))?;
    }
    Ok(())
}
pub(super) async fn publish(
    state: &ApiState,
    task: &Row,
    ws: Uuid,
    owner: Option<Uuid>,
    kind: &str,
) {
    let id: Uuid = task.get("id");
    let agent: Uuid = task.get("agent_id");
    let payload = json_value!({"task_id":id, "issue_id":task.get::<_,Option<Uuid>>("issue_id"), "agent_id":agent});
    state.events.publish(system_event(
        kind,
        ws,
        task.get("chat_session_id"),
        owner,
        payload,
    ));
}

async fn complete(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let task_id = id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to load task"))?;
    let (_, ws, owner) = task_access(&**db, &identity, task_id).await?;
    let mut request: CompleteRequest = decode(&bytes)?;
    request.sanitize();
    if request.context_exhausted() {
        let fail = FailRequest {
            error: request.output,
            failure_reason: "agent_error.context_overflow".into(),
            session_id: request.session_id,
            work_dir: request.work_dir,
            durable_work_dir: request.durable_work_dir,
            branch_name: request.branch_name,
            session_rollout_missing: request.session_rollout_missing,
            retired_session_id: request.retired_session_id,
        };
        return fail_transition(&state, &identity, &mut db, task_id, ws, owner, fail).await;
    }
    let result = serde_json::to_value(&request).map_err(|_| internal("failed to complete task"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to complete task"))?;
    lock_chat(&tx, task_id).await?;
    let current = task_access(&*tx, &identity, task_id).await?.0;
    if current.get::<_, String>("status") != "running" {
        let response = runtime_chat_task::response(&current, ws);
        tx.commit()
            .await
            .map_err(|_| internal("failed to complete task"))?;
        return Ok(GoJson(response));
    }
    let row = tx.query_opt("UPDATE agent_task_queue SET status='completed',completed_at=now(),result=$2,session_id=CASE WHEN $6 THEN NULL ELSE $3 END,work_dir=$4,durable_work_dir=COALESCE($5,durable_work_dir),branch_name=COALESCE($7,branch_name),session_rollout_missing=$6,retired_session_id=COALESCE($8,retired_session_id),prepare_lease_expires_at=NULL WHERE id=$1 AND status='running' RETURNING *",
        &[&task_id,&result,&optional(&request.session_id),&optional(&request.work_dir),&optional(&request.durable_work_dir),&request.session_rollout_missing,&optional(&request.branch_name),&optional(&request.retired_session_id)])
        .await.map_err(|_| internal("failed to complete task"))?;
    let Some(row) = row else {
        // A CAS miss is an idempotent replay, never a failed write.
        let existing = tx
            .query_one("SELECT * FROM agent_task_queue WHERE id=$1", &[&task_id])
            .await
            .map_err(|_| internal("failed to complete task"))?;
        let response = runtime_chat_task::response(&existing, ws);
        tx.commit()
            .await
            .map_err(|_| internal("failed to complete task"))?;
        return Ok(GoJson(response));
    };
    settle(&tx, task_id).await?;
    if let Some(chat) = row.get::<_, Option<Uuid>>("chat_session_id") {
        tx.execute("UPDATE chat_session SET session_id=COALESCE($2,session_id),work_dir=COALESCE($3,work_dir),runtime_id=COALESCE($4,runtime_id),updated_at=now() WHERE id=$1",
            &[&chat,&optional(&request.session_id),&optional(&request.work_dir),&if request.session_id.is_empty() {None} else {row.get::<_,Option<Uuid>>("runtime_id")}])
            .await.map_err(|_| internal("failed to complete task"))?;
        clear_retired(&tx, &row, &request.retired_session_id).await?;
        let body = request.output.replace("\\n", "\n");
        let owner_task: Option<Uuid> = row.get("chat_input_task_id");
        let channel = if let Some(root) = owner_task {
            tx.query_one("SELECT EXISTS(SELECT 1 FROM chat_message WHERE task_id=$1 AND role='user' AND channel_ingested)", &[&root])
                .await.map_err(|_| internal("failed to complete task"))?.get::<_,bool>(0)
        } else {
            false
        };
        if !body.trim().is_empty() || owner_task.is_some() && !channel {
            let empty = body.trim().is_empty();
            let content = if empty {
                "The agent finished this turn without a text reply."
            } else {
                body.as_str()
            };
            let kind = if empty { Some("no_response") } else { None };
            tx.execute("INSERT INTO chat_message(id,chat_session_id,role,content,task_id,message_kind) VALUES ($1,$2,'assistant',$3,$4,COALESCE($5,'message'))",
                &[&Uuid::now_v7(),&chat,&content,&task_id,&kind]).await.map_err(|_| internal("failed to complete task"))?;
        }
    }
    let agent: Uuid = row.get("agent_id");
    reconcile_agent(&*tx, agent).await?;
    let response = runtime_chat_task::response(&row, ws);
    tx.commit()
        .await
        .map_err(|_| internal("failed to complete task"))?;
    if let Some(issue_id) = row.get::<_, Option<Uuid>>("issue_id")
        && let Err(err) = db
            .execute(
                "UPDATE issue SET first_executed_at=now() WHERE id=$1 AND workspace_id=$2 AND first_executed_at IS NULL",
                &[&issue_id, &ws],
            )
            .await
    {
        eprintln!("failed to mark issue {issue_id} first executed: {err}");
    }
    // Revocation is best effort: a failure cannot invalidate a committed terminal result.
    let _ = db
        .execute("DELETE FROM task_token WHERE task_id=$1", &[&task_id])
        .await;
    state.wake_daemon(row.get("runtime_id"), None);
    publish(&state, &row, ws, owner, "task:completed").await;
    Ok(GoJson(response))
}

async fn fail(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let task_id = id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to load task"))?;
    let (_, ws, owner) = task_access(&**db, &identity, task_id).await?;
    let mut request: FailRequest = decode(&bytes)?;
    request.sanitize();
    fail_transition(&state, &identity, &mut db, task_id, ws, owner, request).await
}
// Keep the most important legacy-daemon classification at the server boundary:
// an exhausted context must never be repinned as a healthy resume pointer.
fn classify_failure(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if [
        "context length",
        "context_length_exceeded",
        "maximum context",
        "prompt is too long",
        "context window limit",
        "model_context_window_exceeded",
    ]
    .iter()
    .any(|s| lower.contains(s))
        || lower.contains("token") && lower.contains("limit")
    {
        "agent_error.context_overflow"
    } else {
        "agent_error"
    }
}

// The retry is part of the same transaction as failure, as in Go's
// FailTaskWithTransition: an already-terminal callback cannot create another.
pub(super) fn retry_budget(row: &Row, reason: &str) -> Option<(i32, i64)> {
    if !matches!(
        reason,
        "runtime_offline"
            | "runtime_recovery"
            | "timeout"
            | "codex_semantic_inactivity"
            | "agent_error.provider_network"
            | "skill_bundle_unavailable"
    ) || row.get::<_, Option<Uuid>>("autopilot_run_id").is_some()
        || row
            .get::<_, Option<Value>>("context")
            .is_some_and(|c| c["type"] == "triage")
        || row.get::<_, Option<Uuid>>("issue_id").is_none()
            && row.get::<_, Option<Uuid>>("chat_session_id").is_none()
    {
        return None;
    }
    let attempt: i32 = row.get("attempt");
    let max: i32 = row.get("max_attempts");
    let ceiling = if reason == "agent_error.provider_network" && max > 1 {
        max.max(3)
    } else {
        max
    };
    if attempt >= ceiling {
        return None;
    }
    let delay = match reason {
        "runtime_offline" => 1,
        "agent_error.provider_network" if attempt >= 2 => 5,
        _ => 0,
    };
    Some((ceiling, delay))
}

pub(super) const CREATE_RETRY: &str = r#"
INSERT INTO agent_task_queue (
 agent_id,runtime_id,issue_id,chat_session_id,autopilot_run_id,
 status,priority,trigger_comment_id,coalesced_comment_ids,trigger_summary,context,
 session_id,work_dir,attempt,max_attempts,parent_task_id,force_fresh_session,is_leader_task,
 squad_id,originator_user_id,accountable_user_id,runtime_mcp_overlay,runtime_connected_apps,
 originator_source,delegated_from_task_id,rule_version_id,
 trigger_evidence_kind,trigger_evidence_ref_id,retry_of_task_id,
 chat_input_task_id,fire_at,channel_context_revision,handoff_note,workflow_step,id
)
SELECT p.agent_id,p.runtime_id,p.issue_id,p.chat_session_id,p.autopilot_run_id,
 CASE WHEN $3::bigint>0 THEN 'deferred' ELSE 'queued' END,
 CASE WHEN p.chat_session_id IS NOT NULL THEN GREATEST(p.priority,3) ELSE p.priority END,
 p.trigger_comment_id,p.coalesced_comment_ids,p.trigger_summary,p.context,
 CASE WHEN p.failure_reason='codex_semantic_inactivity' THEN NULL ELSE p.session_id END,
 p.work_dir,p.attempt+1,$2,p.id,p.failure_reason='codex_semantic_inactivity',p.is_leader_task,
 p.squad_id,p.originator_user_id,p.accountable_user_id,NULL,NULL,
 p.originator_source,p.delegated_from_task_id,p.rule_version_id,
 p.trigger_evidence_kind,p.trigger_evidence_ref_id,p.id,
 p.chat_input_task_id,CASE WHEN $3::bigint>0 THEN now()+make_interval(secs => $3::double precision) END,
 p.channel_context_revision,
 CASE WHEN p.context->>'wakeup_id' IS NOT NULL OR p.workflow_step IS NOT NULL THEN p.handoff_note END,
 p.workflow_step,$4
FROM agent_task_queue p
WHERE p.id=$1 AND lock_task_owner_rows(p.agent_id,p.issue_id,p.runtime_id)
ON CONFLICT (issue_id,agent_id,(COALESCE(comment_thread_id,'00000000-0000-0000-0000-000000000000'::uuid)))
WHERE status IN ('queued','dispatched') OR (status='deferred' AND context->>'channel_issue_media_pending'='true')
DO NOTHING RETURNING id,runtime_id
"#;

async fn fail_transition(
    state: &ApiState,
    identity: &DaemonIdentity,
    db: &mut deadpool_postgres::Object,
    task_id: Uuid,
    ws: Uuid,
    owner: Option<Uuid>,
    request: FailRequest,
) -> Result<GoJson<Value>, ApiError> {
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to fail task"))?;
    lock_chat(&tx, task_id).await?;
    let current = task_access(&*tx, identity, task_id).await?.0;
    if !matches!(
        current.get::<_, String>("status").as_str(),
        "running" | "dispatched" | "waiting_local_directory"
    ) {
        let response = runtime_chat_task::response(&current, ws);
        tx.commit()
            .await
            .map_err(|_| internal("failed to fail task"))?;
        return Ok(GoJson(response));
    }
    let reason = if request.failure_reason.is_empty() {
        classify_failure(&request.error)
    } else {
        &request.failure_reason
    };
    let row=tx.query_opt("UPDATE agent_task_queue SET status='failed',completed_at=now(),error=$2,failure_reason=$3,session_id=CASE WHEN $8 THEN NULL ELSE COALESCE($4,session_id) END,work_dir=COALESCE($5,work_dir),durable_work_dir=COALESCE($6,durable_work_dir),branch_name=COALESCE($7,branch_name),session_rollout_missing=$8,retired_session_id=COALESCE($9,retired_session_id),prepare_lease_expires_at=NULL WHERE id=$1 AND status IN ('dispatched','running','waiting_local_directory') RETURNING *",
        &[&task_id,&request.error,&reason,&optional(&request.session_id),&optional(&request.work_dir),&optional(&request.durable_work_dir),&optional(&request.branch_name),&request.session_rollout_missing,&optional(&request.retired_session_id)])
        .await.map_err(|_| internal("failed to fail task"))?;
    let Some(row) = row else {
        let existing = tx
            .query_one("SELECT * FROM agent_task_queue WHERE id=$1", &[&task_id])
            .await
            .map_err(|_| internal("failed to fail task"))?;
        let response = runtime_chat_task::response(&existing, ws);
        tx.commit()
            .await
            .map_err(|_| internal("failed to fail task"))?;
        return Ok(GoJson(response));
    };
    settle(&tx, task_id).await?;
    // `delay > 0` means the child was armed as `deferred`: it is not claimable
    // yet, so announcing it would be wrong. Only the queued child is woken.
    let mut retried: Option<(Uuid, Option<Uuid>, i64)> = None;
    if let Some((ceiling, delay)) = retry_budget(&row, reason) {
        // A pre-existing manual rerun owns the pending slot. The unique index
        // in CREATE_RETRY handles the race after this optimization as well.
        let issue: Option<Uuid> = row.get("issue_id");
        let agent: Uuid = row.get("agent_id");
        let thread: Option<Uuid> = row.get("comment_thread_id");
        let has_successor = if let Some(issue) = issue {
            tx.query_one("SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND comment_thread_id IS NOT DISTINCT FROM $3 AND id<>$4 AND status IN ('queued','dispatched'))",
                &[&issue, &agent, &thread, &task_id])
                .await.map_err(|_| internal("failed to schedule task retry"))?.get::<_, bool>(0)
        } else {
            false
        };
        if !has_successor
            && let Some(created) = tx
                .query_opt(CREATE_RETRY, &[&task_id, &ceiling, &delay, &Uuid::now_v7()])
                .await
                .map_err(|_| internal("failed to schedule task retry"))?
        {
            retried = Some((created.get("id"), created.get("runtime_id"), delay));
        }
    }
    if let Some(chat) = row.get::<_, Option<Uuid>>("chat_session_id") {
        clear_retired(&tx, &row, &request.retired_session_id).await?;
        if request.session_rollout_missing || reason.contains("context_overflow") {
            let session: Option<String> = row.get("session_id");
            if let Some(session) = session {
                clear_retired(&tx, &row, &session).await?;
            }
        } else {
            tx.execute("UPDATE chat_session SET session_id=COALESCE($2,session_id),work_dir=COALESCE($3,work_dir),runtime_id=COALESCE($4,runtime_id),updated_at=now() WHERE id=$1",
                &[&chat,&optional(&request.session_id),&optional(&request.work_dir),&if request.session_id.is_empty(){None}else{row.get::<_,Option<Uuid>>("runtime_id")}])
                .await.map_err(|_| internal("failed to fail task"))?;
        }
        // A retried chat turn must not surface a terminal failure in the
        // transcript; the child continues the same sealed user input.
        if retried.is_none() {
            tx.execute("INSERT INTO chat_message(id,chat_session_id,role,content,task_id,failure_reason) VALUES ($1,$2,'assistant',$3,$4,$5)",
                &[&Uuid::now_v7(),&chat,&request.error,&task_id,&reason]).await.map_err(|_| internal("failed to fail task"))?;
        }
    }
    let agent: Uuid = row.get("agent_id");
    reconcile_agent(&*tx, agent).await?;
    let response = runtime_chat_task::response(&row, ws);
    tx.commit()
        .await
        .map_err(|_| internal("failed to fail task"))?;
    let _ = db
        .execute("DELETE FROM task_token WHERE task_id=$1", &[&task_id])
        .await;
    // Go wakes the queued retry child (task ID present) and then the failed
    // parent's runtime with no task ID, because a cleared capacity or
    // serialization slot may have unblocked other queued work.
    if let Some((child, child_runtime, 0)) = retried {
        state.wake_daemon(child_runtime, Some(child));
    }
    state.wake_daemon(row.get("runtime_id"), None);
    publish(state, &row, ws, owner, "task:failed").await;
    Ok(GoJson(response))
}

async fn cancel_ack(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let task_id = id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to load task"))?;
    let (_, ws, owner) = task_access(&**db, &identity, task_id).await?;
    // A malformed or missing body must not prevent an old daemon acknowledging cancellation.
    let mut request = serde_json::from_slice::<AckRequest>(&bytes).unwrap_or_default();
    request.sanitize();
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to acknowledge cancellation"))?;
    lock_chat(&tx, task_id).await?;
    let row = task_access(&*tx, &identity, task_id).await?.0;
    let cancelled = row.get::<_, String>("status") == "cancelled";
    let mut delivered = false;
    if cancelled {
        for (column, value) in [
            ("durable_work_dir", request.durable_work_dir.trim()),
            ("branch_name", request.branch_name.trim()),
        ] {
            if !value.is_empty() {
                let sql = format!(
                    "UPDATE agent_task_queue SET {column}=COALESCE({column},$2) WHERE id=$1 AND status='cancelled'"
                );
                tx.execute(&sql, &[&task_id, &value])
                    .await
                    .map_err(|_| internal("failed to record cancelled task work"))?;
                delivered = true;
            }
        }
        if !request.error_message.trim().is_empty()
            && request.failure_reason != "cleanup_unconfirmed"
        {
            tx.execute("UPDATE agent_task_queue SET error=$2,failure_reason=COALESCE(failure_reason,$3) WHERE id=$1 AND status='cancelled' AND (error IS NULL OR error='')",
                &[&task_id,&request.error_message.trim(),&optional(request.failure_reason.trim())])
                .await.map_err(|_| internal("failed to record task error"))?;
            delivered = true;
        }
        if !request.retired_session_id.trim().is_empty() {
            tx.execute("UPDATE agent_task_queue SET retired_session_id=COALESCE(retired_session_id,$2) WHERE id=$1 AND status='cancelled'",
                &[&task_id,&request.retired_session_id.trim()]).await.map_err(|_| internal("failed to retire session"))?;
            clear_retired(&tx, &row, request.retired_session_id.trim()).await?;
        }
        let pending = row
            .get::<_, Option<Value>>("result")
            .is_some_and(|v| v["process_cleanup_pending"] == true);
        if request.failure_reason == "cleanup_unconfirmed"
            || pending && request.process_cleanup_confirmed != Some(true)
        {
            let message = if request.error_message.trim().is_empty() {
                "Process cleanup is unconfirmed; background processes may still be running. Check them before manually rerunning the task."
            } else {
                request.error_message.trim()
            };
            tx.execute("UPDATE agent_task_queue SET status='failed',error=concat_ws('; ',NULLIF(error,''),$2),failure_reason='cleanup_unconfirmed',completed_at=now(),chat_finalize_deferred_at=NULL WHERE id=$1 AND status='cancelled'",
                &[&task_id,&message]).await.map_err(|_| internal("failed to record process cleanup failure"))?;
        }
    }
    tx.execute("UPDATE agent_task_queue SET result=result-'process_cleanup_pending' WHERE id=$1 AND status IN ('cancelled','failed') AND result->>'process_cleanup_pending'='true'", &[&task_id])
        .await.map_err(|_| internal("failed to acknowledge process cleanup"))?;
    // The deferred marker is claimed once. A crash rolls it back with the message.
    let deferred=tx.query_opt("UPDATE agent_task_queue SET chat_finalize_deferred_at=NULL WHERE id=$1 AND chat_finalize_deferred_at IS NOT NULL AND status='cancelled' RETURNING *", &[&task_id])
        .await.map_err(|_| internal("failed to finalize cancelled chat"))?;
    if let Some(deferred) = deferred
        && let Some(chat) = deferred.get::<_, Option<Uuid>>("chat_session_id")
    {
        let input_owner: Uuid = deferred
            .get::<_, Option<Uuid>>("chat_input_task_id")
            .unwrap_or(task_id);
        let has_output: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM task_message WHERE task_id=$1)",
                &[&task_id],
            )
            .await
            .map_err(|_| internal("failed to finalize cancelled chat"))?
            .get(0);
        let channel: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM chat_message WHERE task_id=$1 AND role='user' AND channel_ingested)", &[&input_owner])
                .await.map_err(|_| internal("failed to finalize cancelled chat"))?.get(0);
        if !has_output && !channel {
            let attachments = tx.query("UPDATE attachment SET chat_message_id=NULL WHERE chat_message_id IN (SELECT id FROM chat_message WHERE task_id=$1 AND role='user' AND message_kind<>'onboarding_kickoff') RETURNING id", &[&input_owner])
                    .await.map_err(|_| internal("failed to finalize cancelled chat"))?;
            let deleted = tx.query_opt("DELETE FROM chat_message WHERE task_id=$1 AND role='user' AND message_kind<>'onboarding_kickoff' RETURNING id,content", &[&input_owner])
                    .await.map_err(|_| internal("failed to finalize cancelled chat"))?;
            if let Some(deleted) = deleted {
                let message_id: Uuid = deleted.get("id");
                let content: String = deleted.get("content");
                let attachment_ids: Vec<Uuid> = attachments.iter().map(|r| r.get("id")).collect();
                tx.execute("INSERT INTO chat_draft_restore(id,chat_session_id,task_id,content,attachment_ids) VALUES ($1,$2,$3,$4,$5)",
                        &[&message_id,&chat,&task_id,&content,&attachment_ids]).await.map_err(|_| internal("failed to finalize cancelled chat"))?;
            }
        } else {
            tx.execute("INSERT INTO chat_message(id,chat_session_id,role,content,task_id) VALUES ($1,$2,'assistant','Stopped.',$3)",
                    &[&Uuid::now_v7(),&chat,&task_id]).await.map_err(|_| internal("failed to finalize cancelled chat"))?;
        }
    }
    let response_row = tx
        .query_one("SELECT * FROM agent_task_queue WHERE id=$1", &[&task_id])
        .await
        .map_err(|_| internal("failed to acknowledge cancellation"))?;
    tx.commit()
        .await
        .map_err(|_| internal("failed to acknowledge cancellation"))?;
    if delivered && response_row.get::<_, String>("status") == "cancelled" {
        publish(&state, &response_row, ws, owner, "task:cancelled").await;
    }
    Ok(GoJson(json_value!({"status":"ok"})))
}

/// DB-only first slice of Go's 30-second runtime sweeper
/// (`server/cmd/server/runtime_sweeper.go`). It reproduces only two stages:
///
/// 1. `sweepStaleRuntimes` without the liveness store: stale online rows are
///    selected and flipped offline with the stale predicate repeated in the
///    UPDATE, so a heartbeat between SELECT and write wins.
/// 2. `sweepOfflineRuntimeTasks`: in-flight tasks are failed only after the
///    reconnect grace, capped at 500 and locked with `SKIP LOCKED`, with the
///    task-status predicate repeated in the UPDATE. As in Go, that UPDATE does
///    not recheck runtime status or heartbeat; a reconnect racing the sweep can
///    still fail the already-recovered runtime's task.
///
/// The terminal write and both application-owned settlements
/// (`task_supplement` pending/delivering -> failed, and delivery-receipted
/// delegated-failure recovery comments -> settled) commit in one transaction,
/// mirroring `SettleTerminalTaskState`. Settlement failure rolls the failure
/// back rather than leaving an unreplayable terminal row.
///
/// Remaining divergence, intentionally Go-owned in this slice:
/// - no Redis liveness filter, analytics events, `stale_sweep` broadcast or
///   liveness `Forget`;
/// - no `HandleFailedTasks` postcommit effects: auto-retry of a
///   `runtime_offline` child, issue failure comment/status reset,
///   `task:failed` publication, agent reconciliation or finish notification.
///   These cannot be made transactional, so they are not faked here;
/// - no queued-task expiry, deferred reconnect-retry expiry, stale
///   dispatched/running backstop, deferred chat finalization or runtime GC.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RuntimeSweepResult {
    pub offline_runtimes: usize,
    pub failed_tasks: usize,
}

pub fn runtime_reconnect_grace() -> i64 {
    let configured = std::env::var("MULTICA_RUNTIME_RECONNECT_GRACE")
        .ok()
        .and_then(|raw| super::parse_go_ttl(&raw))
        .unwrap_or(3 * 60 * 60);
    configured.max(150)
}

pub async fn mark_stale_runtime_candidates(
    pool: &deadpool_postgres::Pool,
    candidates: &[Uuid],
) -> Result<usize, String> {
    if candidates.is_empty() {
        return Ok(0);
    }
    let db = pool.get().await.map_err(|e| e.to_string())?;
    Ok(db.query("UPDATE agent_runtime SET status='offline',updated_at=now() WHERE status='online' AND id=ANY($1::uuid[]) AND last_seen_at < now()-make_interval(secs => 150::double precision) RETURNING id", &[&candidates])
        .await.map_err(|e| e.to_string())?.len())
}

pub async fn sweep_runtime_once(
    pool: &deadpool_postgres::Pool,
    reconnect_grace_seconds: i64,
) -> Result<RuntimeSweepResult, String> {
    let mut db = pool.get().await.map_err(|e| e.to_string())?;
    // The SELECT is intentionally separate: the UPDATE must repeat both
    // predicates so a heartbeat between discovery and write cannot demote it.
    // Unlike Go, this first slice has no Redis liveness override.
    let candidates: Vec<Uuid> = db
        .query("SELECT id FROM agent_runtime WHERE status='online' AND last_seen_at < now()-make_interval(secs => 150::double precision)", &[])
        .await.map_err(|e| e.to_string())?
        .into_iter().map(|row| row.get(0)).collect();
    let offline_runtimes = mark_stale_runtime_candidates(pool, &candidates).await?;

    // Keep the terminal transition and BOTH dependent settlements in one
    // transaction. An error, including an unavailable settlement table, must
    // roll back every failed task in this batch; no SQL-only shortcut is safe.
    let tx = db.transaction().await.map_err(|e| e.to_string())?;
    let victims = tx.query(
        "WITH victims AS (SELECT task.id FROM agent_task_queue task JOIN agent_runtime runtime ON runtime.id=task.runtime_id WHERE task.status IN ('dispatched','running','waiting_local_directory') AND runtime.status='offline' AND COALESCE(runtime.last_seen_at,runtime.updated_at) < now()-make_interval(secs => $1::double precision) ORDER BY COALESCE(runtime.last_seen_at,runtime.updated_at),task.created_at LIMIT 500 FOR UPDATE OF task SKIP LOCKED) UPDATE agent_task_queue task SET status='failed',completed_at=now(),error='runtime went offline',failure_reason='runtime_offline',wait_reason=NULL FROM victims WHERE task.id=victims.id AND task.status IN ('dispatched','running','waiting_local_directory') RETURNING task.id",
        &[&(reconnect_grace_seconds.max(150) as f64)],
    ).await.map_err(|e| e.to_string())?;
    for row in &victims {
        settle(&tx, row.get::<_, Uuid>(0))
            .await
            .map_err(|e| format!("offline task terminal settlement: {e:?}"))?;
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(RuntimeSweepResult {
        offline_runtimes,
        failed_tasks: victims.len(),
    })
}

pub async fn run_runtime_sweeper(
    pool: std::sync::Arc<deadpool_postgres::Pool>,
    reconnect_grace_seconds: i64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
    // Go's ticker waits one interval before its first round; skip the immediate
    // tokio tick so startup does not sweep earlier than the Go service would.
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = ticker.tick() => {
                if let Err(err) = sweep_runtime_once(&pool, reconnect_grace_seconds).await {
                    eprintln!("runtime sweeper: {err}");
                }
            }
        }
        if *shutdown.borrow() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn terminal_payloads_strip_nuls_and_preserve_metadata() {
        let mut request: CompleteRequest=decode(br#"{"output":"hi\u0000ok","branch_name":"feat\u0000x","session_rollout_missing":true}"#).unwrap();
        request.sanitize();
        assert_eq!(request.output, "hiok");
        assert_eq!(request.branch_name, "featx");
        assert!(request.session_rollout_missing);
        assert_eq!(serde_json::to_value(request).unwrap()["output"], "hiok");
        let mut failed: FailRequest =
            decode(br#"{"error":"err\u0000msg","retired_session_id":"s\u0000"}"#).unwrap();
        failed.sanitize();
        assert_eq!(failed.error, "errmsg");
        assert_eq!(failed.retired_session_id, "s");
    }
    #[test]
    fn complete_context_exhaustion_is_bounded() {
        let mut request = CompleteRequest {
            output: "Prompt is too long and cannot be compacted".into(),
            ..Default::default()
        };
        assert!(request.context_exhausted());
        request.output = "Prompt is too long".into();
        assert!(!request.context_exhausted());
        request.output = "conversation too long, press esc twice".into();
        assert!(request.context_exhausted());
        request.output = "conversation too long, press esc twice".repeat(100);
        assert!(!request.context_exhausted());
    }
    #[test]
    fn completion_and_failure_require_valid_json() {
        assert!(decode::<CompleteRequest>(b"").is_err());
        assert!(decode::<FailRequest>(b"{").is_err());
        assert!(decode::<CompleteRequest>(br#"{"output":3}"#).is_err());
        assert_eq!(optional(""), None);
    }
    #[test]
    fn malformed_cancel_ack_is_accepted_as_empty() {
        let ack = serde_json::from_slice::<AckRequest>(b"{").unwrap_or_default();
        assert!(ack.branch_name.is_empty());
        assert_eq!(ack.process_cleanup_confirmed, None);
    }
}
