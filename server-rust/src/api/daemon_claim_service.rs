//! The task-service operations the claim path calls but no user request owns.
//!
//! Go's claim handler reaches `TaskService.CancelTask`, `FailTask`,
//! `RequeueTaskAfterClaimFailure` and `FinalizeTaskClaim`; those take a service
//! receiver and no actor. Rust's equivalents live inside HTTP handlers that
//! authenticate a member first, so they cannot be reused here. These are the
//! same transitions with the actor fixed to `system`, which is what Go writes
//! for an automatic repair path anyway.
use super::{
    ApiError, ApiState,
    p0_daemon_outcomes::{lock_chat, publish, settle},
    runtime_chat_daemon::{agent_snapshot, reconcile_agent, system_event},
};
use chrono::Utc;
use deadpool_postgres::{GenericClient, Transaction};
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

/// Control characters PostgreSQL cannot store would abort the whole terminal
/// transition, leaving a refused task stuck in `dispatched`. Strip them before
/// any statement sees the text, as Go's `SanitizeTextForPostgres` does.
pub(super) fn scrub(value: &str) -> String {
    value.chars().filter(|c| *c != '\0').collect()
}

fn failed(message: &'static str) -> ApiError {
    super::error(axum::http::StatusCode::INTERNAL_SERVER_ERROR, message)
}

/// Go `CancelAgentTask` / `CancelAgentTaskWithReason`: only an in-flight row
/// can be cancelled, and the returned `None` for an already-terminal row is a
/// normal no-op rather than an error.
async fn cancel_row(
    tx: &Transaction<'_>,
    task: Uuid,
    reason: Option<(&str, &str)>,
) -> Result<Option<Row>, ApiError> {
    let row = match reason {
        Some((message, failure_reason)) => {
            tx.query_opt(
                "UPDATE agent_task_queue SET status='cancelled',completed_at=now(),error=$2,failure_reason=$3,prepare_lease_expires_at=NULL,cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL WHERE id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') RETURNING *",
                &[&task, &message, &failure_reason],
            )
            .await
        }
        None => {
            tx.query_opt(
                "UPDATE agent_task_queue SET status='cancelled',completed_at=now(),prepare_lease_expires_at=NULL,cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL WHERE id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') RETURNING *",
                &[&task],
            )
            .await
        }
    };
    row.map_err(|_| failed("failed to cancel task"))
}

/// The whole cancellation flow minus the user-actor gates: status flip, cleanup
/// fence, terminal settlement and chat-pointer advance commit together, then
/// the post-commit broadcasts run. Deliberately independent of
/// `state.task_cancel_events`: Go's service always broadcasts, and a repair
/// path must not be skipped because an optional sink is absent.
pub(super) async fn cancel_task(
    state: &ApiState,
    task: Uuid,
    reason: Option<(&str, &str)>,
) -> Result<Option<Row>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    // chat_session precedes agent_task_queue in the repo-wide lock order.
    lock_chat(&tx, task).await?;
    let reason = reason.map(|(message, failure_reason)| (scrub(message), scrub(failure_reason)));
    let reason = reason
        .as_ref()
        .map(|(message, failure_reason)| (message.as_str(), failure_reason.as_str()));
    let Some(cancelled) = cancel_row(&tx, task, reason).await? else {
        tx.commit()
            .await
            .map_err(|_| failed("failed to cancel task"))?;
        return Ok(None);
    };
    // Only a run that negotiated confirmation at start holds the cleanup fence.
    tx.execute("UPDATE agent_task_queue task SET result=COALESCE(task.result,'{}'::jsonb)||'{\"process_cleanup_pending\":true}'::jsonb WHERE task.id=$1 AND task.status='cancelled' AND task.started_at IS NOT NULL AND task.result->>'process_cleanup_capability'='pi-process-cleanup-v1' AND EXISTS (SELECT 1 FROM agent_runtime runtime WHERE runtime.id=task.runtime_id AND runtime.provider IN ('pi','omp'))", &[&task])
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    settle(&tx, task).await?;
    tx.execute("DELETE FROM task_token WHERE task_id=$1", &[&task])
        .await
        .map_err(|_| failed("failed to revoke task token"))?;
    tx.execute("UPDATE chat_session cs SET session_id=t.session_id,runtime_id=t.runtime_id,work_dir=COALESCE(t.work_dir,cs.work_dir),updated_at=now() FROM agent_task_queue t WHERE t.id=$1 AND t.chat_session_id=cs.id AND t.status='cancelled' AND t.session_id IS NOT NULL AND t.runtime_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM agent_task_queue newer WHERE newer.chat_session_id=t.chat_session_id AND newer.id<>t.id AND newer.session_id IS NOT NULL AND newer.created_at>t.created_at)", &[&task])
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    let agent: Uuid = cancelled.get("agent_id");
    // Capture the chat audience before commit: the row may be archived or
    // deleted concurrently, and a post-commit lookup loses the owner scope.
    let audience = tx
        .query_opt(
            "SELECT cs.id,cs.creator_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type,a.owner_id FROM chat_session cs JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id WHERE cs.id=$1",
            &[&cancelled.get::<_, Option<Uuid>>("chat_session_id")],
        )
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    let status_agent = reconcile_agent(&*tx, agent)
        .await?
        .map(|row| agent_snapshot(&row));
    tx.commit()
        .await
        .map_err(|_| failed("failed to cancel task"))?;
    state.wake_daemon(cancelled.get("runtime_id"), None);
    if let Some(agent_row) = status_agent {
        state.events.publish(system_event(
            "agent:status",
            cancelled
                .get::<_, Option<Uuid>>("workspace_id")
                .unwrap_or(Uuid::nil()),
            None,
            None,
            json!({"agent": agent_row}),
        ));
    }
    let chat: Option<Uuid> = cancelled.get("chat_session_id");
    let workspace: Uuid = cancelled.get("workspace_id");
    match chat {
        None => {
            if let Some(sink) = &state.task_cancel_events {
                sink.publish_task_cancelled(super::issue_write::IssueTaskCancelledEvent {
                    workspace_id: workspace,
                    task_id: task,
                    agent_id: agent,
                    issue_id: cancelled.get("issue_id"),
                });
            }
        }
        Some(chat) => {
            if let Some(audience) = audience {
                let creator: Option<Uuid> = audience.get("creator_id");
                let owner: Option<Uuid> = audience.get("owner_id");
                let creator = creator.unwrap_or(Uuid::nil());
                state.events.publish_chat_audience(
                    super::runtime_chat_events::Event::task_cancelled(
                        workspace, creator, chat, task, agent,
                    ),
                    creator,
                    audience.get("zbs_has_guest_history"),
                    audience
                        .get::<_, Option<String>>("zbs_source_channel_type")
                        .as_deref(),
                    owner,
                );
            }
        }
    }
    Ok(Some(cancelled))
}

/// Go `RequeueTaskAfterClaimFailure`: hand only this exact claim generation back
/// to the queue. The `dispatched_at` CAS is what stops an old handler from
/// rolling back a newer reclaim.
pub(super) async fn requeue_task_after_claim_failure(
    state: &ApiState,
    task: &Row,
) -> Result<Option<Row>, ApiError> {
    let id: Uuid = task.get("id");
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to claim tasks"))?;
    let requeued = db
        .query_opt(
            "UPDATE agent_task_queue SET status='queued',dispatched_at=NULL,prepare_lease_expires_at=NULL,delivered_comment_ids='{}' WHERE id=$1 AND runtime_id=$2 AND status='dispatched' AND started_at IS NULL AND dispatched_at=$3 RETURNING *",
            &[&id, &task.get::<_, Uuid>("runtime_id"), &task.get::<_, Option<chrono::DateTime<Utc>>>("dispatched_at")],
        )
        .await
        .map_err(|_| failed("failed to claim tasks"))?;
    let Some(requeued) = requeued else {
        return Ok(None);
    };
    let agent: Uuid = requeued.get("agent_id");
    if let Ok(Some(row)) = reconcile_agent(&**db, agent).await {
        state.events.publish(system_event(
            "agent:status",
            row.get("workspace_id"),
            None,
            None,
            json!({"agent": agent_snapshot(&row)}),
        ));
    }
    let workspace: Uuid = requeued.get("workspace_id");
    let mut payload = json!({
        "task_id": id,
        "agent_id": agent,
        "issue_id": requeued.get::<_, Option<Uuid>>("issue_id").map(|v| v.to_string()).unwrap_or_default(),
        "status": requeued.get::<_, String>("status"),
    });
    if let Some(session) = requeued.get::<_, Option<Uuid>>("chat_session_id") {
        payload["chat_session_id"] = json!(session);
    }
    state
        .events
        .publish(system_event("task:queued", workspace, None, None, payload));
    state.wake_daemon(requeued.get("runtime_id"), Some(id));
    Ok(Some(requeued))
}

/// Go `FailAgentTask` plus the resume-pointer bookkeeping that must commit with
/// it. The claim path only ever fails a task it has just claimed, so there is no
/// auto-retry child: every reason it uses (`invalid_task_identity`,
/// `runtime_access_denied`, `agent_unknown`) is outside Go's `retryableReasons`,
/// which is why this omits the retry insert rather than approximating it.
pub(super) async fn fail_task(
    state: &ApiState,
    task: Uuid,
    error_message: &str,
    failure_reason: &str,
) -> Result<Option<Row>, ApiError> {
    let message = scrub(error_message);
    let reason = scrub(failure_reason);
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to fail task"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to fail task"))?;
    lock_chat(&tx, task).await?;
    let row = tx
        .query_opt(
            "UPDATE agent_task_queue SET status='failed',completed_at=now(),error=$2,failure_reason=CASE WHEN $3='' THEN 'agent_error' ELSE $3 END,prepare_lease_expires_at=NULL WHERE id=$1 AND status IN ('dispatched','running','waiting_local_directory') RETURNING *",
            &[&task, &message, &reason],
        )
        .await
        .map_err(|_| failed("failed to fail task"))?;
    let Some(row) = row else {
        tx.commit()
            .await
            .map_err(|_| failed("failed to fail task"))?;
        return Ok(None);
    };
    settle(&tx, task).await?;
    tx.execute("DELETE FROM task_token WHERE task_id=$1", &[&task])
        .await
        .map_err(|_| failed("failed to revoke task token"))?;
    // A session the run explicitly retired must go, whatever the terminal
    // status: the fresh-session retry that replaced it may have succeeded, and
    // the pointer would otherwise still name the transcript it retried away
    // from. A resume-unsafe failure clears the pointer for the same reason —
    // leaving it lets the next turn resume a session the server just judged
    // unusable.
    if let Some(chat) = row.get::<_, Option<Uuid>>("chat_session_id") {
        let unsafe_session = super::daemon_claim_pure::resume_unsafe_failure(&reason, &message);
        let dead = row
            .get::<_, Option<String>>("session_id")
            .unwrap_or_default();
        if unsafe_session && !dead.is_empty() {
            tx.execute(
                "UPDATE chat_session SET session_id=NULL,updated_at=now() WHERE id=$1 AND session_id=$2 AND runtime_id=$3",
                &[&chat, &dead, &row.get::<_, Option<Uuid>>("runtime_id")],
            )
            .await
            .map_err(|_| failed("failed to fail task"))?;
        }
        if let Some(retired) = row.get::<_, Option<String>>("retired_session_id")
            && !retired.is_empty()
        {
            tx.execute(
                "UPDATE chat_session SET session_id=NULL,updated_at=now() WHERE id=$1 AND session_id=$2 AND runtime_id=$3",
                &[&chat, &retired, &row.get::<_, Option<Uuid>>("runtime_id")],
            )
            .await
            .map_err(|_| failed("failed to fail task"))?;
        }
    }
    let agent: Uuid = row.get("agent_id");
    let workspace: Uuid = row.get("workspace_id");
    let status_agent = reconcile_agent(&*tx, agent)
        .await?
        .map(|row| agent_snapshot(&row));
    tx.commit()
        .await
        .map_err(|_| failed("failed to fail task"))?;
    if let Some(agent_row) = status_agent {
        state.events.publish(system_event(
            "agent:status",
            workspace,
            None,
            None,
            json!({"agent": agent_row}),
        ));
    }
    let owner = row.get::<_, Option<Uuid>>("chat_session_id");
    publish(state, &row, workspace, owner, "task:failed").await;
    state.wake_daemon(row.get("runtime_id"), None);
    Ok(Some(row))
}

/// Go `FinalizeTaskClaim`: the task token and the comment receipt are the two
/// credentials a claim hands out, and both must land in the same transaction as
/// the delivery authorization — a task that lost its runtime or its agent must
/// not receive either.
///
/// Authorization runs first, inside the transaction, re-reading the runtime
/// and agent under `FOR UPDATE` so a concurrent owner change or agent move
/// cannot slip between the builder's checks and the token write.
/// Go's `FinalizeTaskClaim` takes the same eight values so the credentials and
/// the authorization snapshot commit or roll back as one unit.
#[allow(clippy::too_many_arguments)]
pub(super) async fn finalize_task_claim(
    state: &ApiState,
    task: Uuid,
    runtime: &super::daemon_claim_build::ClaimRuntime,
    token: super::daemon_claim_build::TaskToken<'_>,
    receipt: &[Uuid],
    record_receipt: bool,
    issue_snapshot: Option<&[u8]>,
    daemon_token: Option<&super::daemon_claim_build::RemoteMcpDaemonToken>,
) -> Result<(Vec<Uuid>, Option<(String, String)>), super::daemon_claim_build::FinalizeClaimError> {
    use super::daemon_claim_build::FinalizeClaimError;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    // Go mints the credential for the locked runtime owner, not the builder's guess.
    let locked = tx.query_opt(
        "SELECT owner_id,workspace_id,visibility,shared_with FROM agent_runtime WHERE id=$1 FOR UPDATE",
        &[&runtime.id],
    ).await.map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?
        .ok_or_else(|| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    let agent = tx
        .query_opt(
            "SELECT runtime_id,owner_id FROM agent WHERE id=$1 FOR UPDATE",
            &[&token.agent_id],
        )
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?
        .ok_or_else(|| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    if agent.get::<_, Option<Uuid>>("runtime_id") != Some(runtime.id) {
        return Err(FinalizeClaimError::Authz(
            super::daemon_claim_build::ClaimDeliveryAuthz {
                reason: "error_agent_runtime_changed",
                detail: "agent runtime changed before delivery",
                agent_owner_id: None,
            },
        ));
    }
    let agent_owner: Option<Uuid> = agent.get("owner_id");
    let owner: Option<Uuid> = locked.get("owner_id");
    let visibility: String = locked.get("visibility");
    let shared: Vec<Uuid> = locked
        .get::<_, Option<Vec<Uuid>>>("shared_with")
        .unwrap_or_default();
    if visibility == "private"
        && owner.is_some()
        && agent_owner.is_some()
        && agent_owner != owner
        && !agent_owner.is_some_and(|id| shared.contains(&id))
    {
        return Err(FinalizeClaimError::Authz(
            super::daemon_claim_build::ClaimDeliveryAuthz {
                reason: "error_runtime_access_denied",
                detail: "private runtime does not permit task agent at delivery gate",
                agent_owner_id: agent_owner,
            },
        ));
    }
    let Some(owner) = owner else {
        return Err(FinalizeClaimError::Authz(
            super::daemon_claim_build::ClaimDeliveryAuthz {
                reason: "error_runtime_owner_missing",
                detail: "runtime owner missing before task delivery",
                agent_owner_id: agent_owner,
            },
        ));
    };
    let profile = tx
        .query_opt(
            "SELECT name,profile_description FROM \"user\" WHERE id=$1",
            &[&owner],
        )
        .await
        .ok()
        .flatten()
        .map(|row| {
            (
                row.get::<_, String>("name"),
                row.get::<_, String>("profile_description"),
            )
        });
    let mut token = token;
    token.user_id = owner;
    tx.execute("INSERT INTO task_token(id,token_hash,task_id,agent_id,workspace_id,user_id,expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7)", &[&token.id,&token.hash,&token.task_id,&token.agent_id,&token.workspace_id,&token.user_id,&token.expires_at])
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    if let Some(daemon) = daemon_token {
        // Go binds the credential to the owner the task token was just minted
        // for (the locked runtime owner that `authorize` re-read), so it can
        // never reach a runtime another member later registers under this
        // daemon_id (MUL-8154). The opportunistic sweep keeps these
        // short-lived per-task credentials from accumulating.
        tx.execute("DELETE FROM daemon_token WHERE expires_at <= now()", &[])
            .await
            .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
        tx.execute(
            "INSERT INTO daemon_token(token_hash,workspace_id,daemon_id,expires_at,owner_id) VALUES ($1,$2,$3,$4,$5)",
            &[&daemon.hash, &daemon.workspace_id, &daemon.daemon_id, &daemon.expires_at, &token.user_id],
        )
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    }
    if let Some(snapshot) = issue_snapshot.filter(|raw| !raw.is_empty()) {
        // A zero-row update is not an error: the task moved on, and the claim is
        // about to lose its CAS anyway.
        let snapshot: Value = serde_json::from_slice(snapshot).unwrap_or(Value::Null);
        tx.execute("UPDATE agent_task_queue SET issue_snapshot=$2 WHERE id=$1 AND runtime_id=$3 AND status='dispatched' AND dispatched_at=$4", &[&task,&snapshot,&token.runtime_id,&token.dispatched_at])
            .await
            .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    }
    let mut persisted = receipt.to_vec();
    if record_receipt {
        let row = tx
            .query_opt(
                "UPDATE agent_task_queue SET delivered_comment_ids=$2 WHERE id=$1 AND runtime_id=$3 AND status='dispatched' AND started_at IS NULL AND dispatched_at=$4 AND trigger_comment_id IS NOT DISTINCT FROM $5 AND NOT EXISTS (SELECT 1 FROM unnest($2::uuid[]) AS delivered(id) WHERE delivered.id IS DISTINCT FROM trigger_comment_id AND NOT(delivered.id=ANY(coalesced_comment_ids))) RETURNING delivered_comment_ids",
                &[&task, &receipt, &token.runtime_id, &token.dispatched_at, &token.trigger_comment_id],
            )
            .await
            .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
        let Some(row) = row else {
            // The claim generation moved between the builder and here. Rolling
            // back is correct: no token and no receipt were handed out.
            return Err(FinalizeClaimError::Api(failed("failed to claim tasks")));
        };
        persisted = row
            .get::<_, Option<Vec<Uuid>>>("delivered_comment_ids")
            .unwrap_or_default();
    } else {
        // No receipt to write, but the generation must still be current or the
        // token would authorise a task that has already been reclaimed.
        let row = tx
            .query_opt(
                "SELECT id FROM agent_task_queue WHERE id=$1 AND runtime_id=$2 AND status='dispatched' AND started_at IS NULL AND dispatched_at=$3 FOR UPDATE",
                &[&task, &token.runtime_id, &token.dispatched_at],
            )
            .await
            .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
        if row.is_none() {
            return Err(FinalizeClaimError::Api(failed("failed to claim tasks")));
        }
    }
    tx.commit()
        .await
        .map_err(|_| FinalizeClaimError::Api(failed("failed to claim tasks")))?;
    Ok((persisted, profile))
}
