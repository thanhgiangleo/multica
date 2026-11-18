//! Machine-scoped daemon batch claim. Mount `routes()` ONLY under daemon's
//! Bearer-only authentication layer; this module never accepts browser cookies.
use super::{
    ApiError, ApiState, GoJson,
    daemon::DaemonIdentity,
    daemon_claim_build::{self as build, ClaimRuntime},
    daemon_claim_load as load, daemon_claim_response as response, daemon_claim_service as service,
    error,
    runtime_chat_daemon::{agent_snapshot, reconcile_agent, system_event},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio_postgres::GenericClient;
use tokio_postgres::Row;
use uuid::Uuid;

const CAP: usize = 32;
const FRESH_SECONDS: f64 = 150.0;
const PREPARE_SECONDS: f64 = 45.0;
const RECOVERY_SECONDS: f64 = 90.0;

pub(super) fn routes() -> Router<ApiState> {
    // `/tasks/claim` is the canonical machine-level path the daemon
    // coordinator targets; `/claim` is Go's transitional alias and stays so an
    // older client keeps working. Both reach the same handler.
    Router::new()
        .route("/api/daemon/tasks/claim", post(claim))
        .route("/api/daemon/claim", post(claim))
}

#[derive(Deserialize)]
struct ClaimRequest {
    #[serde(default)]
    daemon_id: String,
    #[serde(default)]
    runtime_ids: Vec<String>,
    #[serde(default)]
    max_tasks: i64,
}

fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn fail(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}

fn parse_request(
    body: &[u8],
    identity: &DaemonIdentity,
) -> Result<(Vec<Uuid>, usize, String), ApiError> {
    let req: ClaimRequest =
        serde_json::from_slice(body).map_err(|_| bad("invalid request body"))?;
    if req.daemon_id.is_empty() {
        return Err(bad("daemon_id is required"));
    }
    if let DaemonIdentity::Bound { daemon, .. } = identity
        && *daemon != req.daemon_id
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "daemon_id does not match token",
        ));
    }
    if req.max_tasks < 0 {
        return Err(bad("max_tasks must not be negative"));
    }
    let mut seen = HashSet::new();
    let ids = req
        .runtime_ids
        .iter()
        .filter_map(|s| Uuid::parse_str(s).ok())
        .filter(|id| seen.insert(*id))
        .collect();
    Ok((ids, (req.max_tasks as usize).min(CAP), req.daemon_id))
}

async fn authorized(
    db: &impl deadpool_postgres::GenericClient,
    identity: &DaemonIdentity,
    ids: &[Uuid],
    daemon_id: &str,
) -> Result<Vec<ClaimRuntime>, ApiError> {
    let rows = db
        .query(
            "SELECT id,workspace_id,daemon_id,owner_id,visibility,shared_with,provider FROM agent_runtime WHERE id=ANY($1::uuid[])",
            &[&ids],
        )
        .await
        .map_err(|_| fail("failed to load runtimes"))?;
    let mut allowed = HashMap::new();
    for row in rows {
        let id: Uuid = row.get("id");
        let workspace: Uuid = row.get("workspace_id");
        let bound: Option<String> = row.get("daemon_id");
        let owner: Option<Uuid> = row.get("owner_id");
        if bound.as_deref().is_some_and(|bound| bound != daemon_id) {
            continue;
        }
        let access = match identity {
            DaemonIdentity::User(user) => {
                if owner != Some(*user) {
                    false
                } else {
                    db.query_opt(
                        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                        &[&workspace, user],
                    )
                    .await
                    .map_err(|_| fail("failed to load runtimes"))?
                    .is_some()
                }
            }
            DaemonIdentity::Bound {
                workspace: scope,
                daemon,
                owner: token_owner,
                hash,
            } => {
                if *scope != workspace
                    || bound.as_deref() != Some(daemon)
                    || daemon.trim().is_empty()
                {
                    false
                } else if daemon.starts_with("qoder-managed-") {
                    let row = db.query_one("SELECT EXISTS(SELECT 1 FROM cloud_agent_connection c JOIN daemon_token d ON d.id=c.token_id JOIN member m ON m.workspace_id=c.workspace_id AND m.user_id=c.owner_id WHERE c.provider='qoder_cloud' AND c.workspace_id=$1 AND c.enabled AND d.daemon_id=$2 AND d.token_hash=$3 AND d.expires_at>now() AND m.role IN ('owner','admin'))", &[&workspace,daemon,hash]).await.map_err(|_| fail("failed to load runtimes"))?;
                    row.get::<_, bool>(0)
                } else {
                    token_owner.is_some() && token_owner == &owner
                }
            }
        };
        if access {
            allowed.insert(id, row);
        }
    }
    // The input order, rather than a HashMap iteration, determines which agent
    // gets a slot when max_tasks cuts a busy batch short.
    Ok(ids
        .iter()
        .filter_map(|id| allowed.get(id).map(|row| build::runtime_from_row(row)))
        .collect())
}

const CANCEL_SUPERSEDED: &str = r#"UPDATE agent_task_queue r
 SET status='cancelled',completed_at=now(),prepare_lease_expires_at=NULL,
 cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL
 WHERE r.runtime_id=ANY($1::uuid[]) AND r.status='deferred' AND r.issue_id IS NOT NULL
 AND r.retry_of_task_id IS NOT NULL
 AND COALESCE(r.context->>'channel_issue_media_pending','')<>'true'
 AND EXISTS(SELECT 1 FROM agent_task_queue successor WHERE successor.issue_id=r.issue_id
 AND successor.agent_id=r.agent_id AND successor.comment_thread_id IS NOT DISTINCT FROM r.comment_thread_id
 AND successor.id<>r.id AND successor.status IN ('queued','dispatched','running','waiting_local_directory'))
 RETURNING r.id,r.agent_id,r.issue_id,r.runtime_id"#;

async fn cancel_superseded(state: &ApiState, db: &impl GenericClient, ids: &[Uuid]) {
    let rows = match db.query(CANCEL_SUPERSEDED, &[&ids]).await {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("cancel superseded deferred retries failed: {err}");
            return;
        }
    };
    for task in rows {
        let agent: Uuid = task.get("agent_id");
        let workspace = db
            .query_opt("SELECT workspace_id FROM agent WHERE id=$1", &[&agent])
            .await;
        if let Ok(Some(row)) = reconcile_agent(db, agent).await {
            state.events.publish(system_event(
                "agent:status",
                row.get("workspace_id"),
                None,
                None,
                json!({"agent":agent_snapshot(&row)}),
            ));
        }
        if let Ok(Some(row)) = workspace {
            let workspace: Uuid = row.get("workspace_id");
            state.events.publish(system_event(
                "task:cancelled",
                workspace,
                None,
                None,
                json!({
                    "task_id":task.get::<_, Uuid>("id"), "agent_id":agent,
                    "issue_id":task.get::<_, Uuid>("issue_id").to_string(), "status":"cancelled",
                }),
            ));
        }
    }
}

async fn promote_due(
    state: &ApiState,
    db: &impl GenericClient,
    ids: &[Uuid],
) -> Result<(), ApiError> {
    cancel_superseded(state, db, ids).await;
    let promoted = match db.query(PROMOTE, &[&ids, &FRESH_SECONDS]).await {
        Ok(rows) => rows,
        Err(err) if err.code().is_some_and(|code| code.code() == "23505") => return Ok(()),
        Err(_) => return Err(fail("failed to claim tasks")),
    };
    for task in promoted {
        let id: Uuid = task.get("id");
        let agent: Uuid = task.get("agent_id");
        let runtime: Uuid = task.get("runtime_id");
        if let Ok(Some(row)) = db
            .query_opt("SELECT workspace_id FROM agent WHERE id=$1", &[&agent])
            .await
        {
            state.events.publish(system_event("task:queued", row.get("workspace_id"), None, None, json!({
                "task_id":id,"agent_id":agent,
                "issue_id":task.get::<_, Option<Uuid>>("issue_id").map(|v|v.to_string()).unwrap_or_default(),
                "status":"queued",
            })));
        }
        state.wake_daemon(Some(runtime), Some(id));
    }
    Ok(())
}

const PROMOTE: &str = r#"WITH due AS (
 SELECT t.id,t.issue_id,row_number() OVER (PARTITION BY t.issue_id,t.agent_id,t.comment_thread_id ORDER BY t.priority DESC,t.created_at ASC,t.id) rn
 FROM agent_task_queue t WHERE t.runtime_id=ANY($1::uuid[]) AND t.status='deferred' AND t.fire_at<=now()
 AND EXISTS(SELECT 1 FROM agent_runtime r WHERE r.id=t.runtime_id AND r.status='online'
   AND COALESCE(r.last_seen_at,r.updated_at)>=now()-make_interval(secs=>$2::double precision))
 AND NOT EXISTS(SELECT 1 FROM agent_task_queue occupant WHERE occupant.issue_id=t.issue_id
   AND occupant.agent_id=t.agent_id AND occupant.comment_thread_id IS NOT DISTINCT FROM t.comment_thread_id AND occupant.id<>t.id
   AND (occupant.status IN ('queued','dispatched') OR (occupant.status='deferred' AND occupant.context->>'channel_issue_media_pending'='true')))
) UPDATE agent_task_queue SET status='queued' WHERE id IN (SELECT id FROM due WHERE issue_id IS NULL OR rn=1) RETURNING *"#;

const RECLAIM: &str = r#"WITH candidates AS MATERIALIZED (
 SELECT t.id FROM agent_task_queue t WHERE t.runtime_id=ANY($1::uuid[]) AND t.status='dispatched'
 AND t.started_at IS NULL AND t.dispatched_at<now()-make_interval(secs=>$2::double precision)
 AND (t.prepare_lease_expires_at IS NULL OR t.prepare_lease_expires_at<now())
 AND EXISTS(SELECT 1 FROM agent a JOIN agent_runtime r ON r.id=t.runtime_id WHERE a.id=t.agent_id
 AND a.runtime_id=t.runtime_id AND (r.visibility='public' OR (r.visibility='private' AND
 (r.owner_id IS NULL OR a.owner_id IS NULL OR r.owner_id=a.owner_id OR a.owner_id=ANY(r.shared_with))))
 AND r.status='online' AND COALESCE(r.last_seen_at,r.updated_at)>=now()-make_interval(secs=>$3::double precision))
 ORDER BY t.priority DESC,t.dispatched_at ASC LIMIT $4 FOR UPDATE SKIP LOCKED
) UPDATE agent_task_queue SET dispatched_at=now(),prepare_lease_expires_at=now()+make_interval(secs=>$5::double precision)
 WHERE id IN (SELECT id FROM candidates) RETURNING *"#;

const CANDIDATES: &str = r#"SELECT t.agent_id,t.runtime_id FROM agent_task_queue t
 WHERE t.runtime_id=ANY($1::uuid[]) AND t.status='queued'
 AND (t.context->>'wakeup_id' IS NULL OR EXISTS(SELECT 1 FROM issue_wakeup w WHERE w.id=(t.context->>'wakeup_id')::uuid
 AND w.disabled_at IS NULL AND w.revision=(t.context->>'wakeup_revision')::bigint))
 AND EXISTS(SELECT 1 FROM agent a JOIN agent_runtime r ON r.id=t.runtime_id
 WHERE a.id=t.agent_id AND a.runtime_id=t.runtime_id AND r.visibility IN ('public','private'))
 ORDER BY t.priority DESC,t.created_at ASC"#;

const CLAIM: &str = r#"UPDATE agent_task_queue SET status='dispatched',dispatched_at=now(),
 prepare_lease_expires_at=now()+make_interval(secs=>$3::double precision)
 WHERE id=(SELECT t.id FROM agent_task_queue t WHERE t.agent_id=$1 AND t.runtime_id=$2 AND t.status='queued'
 AND (t.context->>'wakeup_id' IS NULL OR EXISTS(SELECT 1 FROM issue_wakeup w WHERE w.id=(t.context->>'wakeup_id')::uuid
 AND w.disabled_at IS NULL AND w.revision=(t.context->>'wakeup_revision')::bigint))
 AND EXISTS(SELECT 1 FROM agent a JOIN agent_runtime r ON r.id=t.runtime_id WHERE a.id=t.agent_id
 AND a.runtime_id=t.runtime_id AND r.visibility IN ('public','private') AND r.status='online'
 AND COALESCE(r.last_seen_at,r.updated_at)>=now()-make_interval(secs=>$4::double precision))
 AND NOT EXISTS(SELECT 1 FROM agent_task_queue active WHERE active.agent_id=t.agent_id
 AND active.status IN ('dispatched','running','waiting_local_directory') AND
 ((t.issue_id IS NOT NULL AND active.issue_id=t.issue_id) OR
 (t.chat_session_id IS NOT NULL AND active.chat_session_id=t.chat_session_id) OR
 (t.issue_id IS NULL AND t.chat_session_id IS NULL AND t.autopilot_run_id IS NULL AND
 active.issue_id IS NULL AND active.chat_session_id IS NULL AND active.autopilot_run_id IS NULL)))
 ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *"#;

const REANCHOR_CLAIMED_CHAT_INPUT: &str = r#"WITH latest_visible AS (
    SELECT
        claimed_input.id AS claimed_input_id,
        claimed_input.created_at AS input_created_at,
        prior.created_at AS prior_created_at
    FROM chat_message AS claimed_input
    CROSS JOIN LATERAL (
        SELECT prior.created_at
        FROM chat_message AS prior
        WHERE prior.chat_session_id = claimed_input.chat_session_id
          AND prior.id != claimed_input.id
          AND NOT (
            prior.role = 'user'
            AND EXISTS (
              SELECT 1
              FROM agent_task_queue AS queued_task
              WHERE queued_task.chat_session_id = prior.chat_session_id
                AND queued_task.status = 'queued'
                AND queued_task.id = prior.task_id
                AND queued_task.id <> (
                  SELECT head.id
                  FROM agent_task_queue AS head
                  WHERE head.chat_session_id = claimed_input.chat_session_id
                    AND head.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
                    AND head.regenerate_quick_actions_for IS NULL
                  ORDER BY
                    CASE
                      WHEN head.status IN ('dispatched', 'running', 'waiting_local_directory') THEN 0
                      WHEN head.status = 'deferred' THEN 1
                      ELSE 2
                    END,
                    head.priority DESC,
                    head.created_at ASC,
                    head.id ASC
                  LIMIT 1
                )
            )
          )
        ORDER BY prior.created_at DESC, prior.id DESC
        LIMIT 1
    ) AS prior
    WHERE claimed_input.task_id = $1
      AND claimed_input.role = 'user'
      AND NOT claimed_input.channel_ingested
      AND claimed_input.message_kind <> 'onboarding_kickoff'
)
UPDATE chat_message AS claimed_input
SET created_at = GREATEST(
    $2::timestamptz,
    latest_visible.prior_created_at + interval '1 microsecond'
)
FROM latest_visible
WHERE claimed_input.id = latest_visible.claimed_input_id
  AND latest_visible.prior_created_at >= latest_visible.input_created_at"#;

async fn claim_agent(
    db: &mut deadpool_postgres::Client,
    agent: Uuid,
    runtime: Uuid,
) -> Result<Option<Row>, ApiError> {
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to claim tasks"))?;
    let locked = tx
        .query_opt(
            "SELECT runtime_id,max_concurrent_tasks FROM agent WHERE id=$1 FOR UPDATE",
            &[&agent],
        )
        .await
        .map_err(|_| fail("failed to claim tasks"))?;
    let Some(locked) = locked else {
        return Ok(None);
    };
    if locked.get::<_, Option<Uuid>>(0) != Some(runtime) {
        return Ok(None);
    }
    let count: i64 = tx.query_one("SELECT count(*) FROM agent_task_queue WHERE agent_id=$1 AND status IN ('dispatched','running','waiting_local_directory')", &[&agent])
        .await.map_err(|_| fail("failed to claim tasks"))?.get(0);
    if count >= i64::from(locked.get::<_, i32>(1)) {
        return Ok(None);
    }
    let task = tx
        .query_opt(CLAIM, &[&agent, &runtime, &PREPARE_SECONDS, &FRESH_SECONDS])
        .await
        .map_err(|_| fail("failed to claim tasks"))?;
    if let Some(task) = &task {
        let id: Uuid = task.get("id");
        if task.get::<_, Option<Uuid>>("chat_session_id").is_some()
            && task.get::<_, Option<Uuid>>("chat_input_task_id") == Some(id)
        {
            tx.execute(
                REANCHOR_CLAIMED_CHAT_INPUT,
                &[&id, &task.get::<_, Option<DateTime<Utc>>>("dispatched_at")],
            )
            .await
            .map_err(|_| fail("failed to claim tasks"))?;
        }
    }
    tx.commit()
        .await
        .map_err(|_| fail("failed to claim tasks"))?;
    Ok(task)
}

pub(super) fn capability(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get("x-client-capabilities")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|raw| raw.split(',').any(|part| part.trim() == name))
}

fn token() -> Result<String, ApiError> {
    let mut bytes = [0u8; 20];
    getrandom::fill(&mut bytes).map_err(|_| fail("failed to claim tasks"))?;
    Ok(format!("mat_{}", hex::encode(bytes)))
}

// Both transports and both claim routes use the same response and transactional
// delivery gate. An unsuccessful batch item is recovered by the batch caller;
// singular claims recover immediately after reporting the typed failure.
async fn deliver_claim(
    state: &ApiState,
    task: &Row,
    runtime: &ClaimRuntime,
    headers: &HeaderMap,
) -> Result<Value, build::ClaimBuildFailure> {
    let mut built = response::build_claimed_task_response(state, headers, task, runtime).await?;
    let task_id: Uuid = task.get("id");
    let Some(owner) = runtime.owner_id else {
        let _ = service::cancel_task(state, task_id, None).await;
        return Err(build::ClaimBuildFailure::unsettled(
            "error_token",
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime owner required to mint task token",
        ));
    };
    let raw = token().map_err(|_| {
        build::ClaimBuildFailure::unsettled(
            "error_token",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to mint task token",
        )
    })?;
    let remote = build::remote_mcp_daemon_token_for_claim(
        built.response["remote_mcp_connections"]
            .as_array()
            .is_some_and(|connections| !connections.is_empty()),
        runtime,
    )
    .map_err(|_| {
        build::ClaimBuildFailure::unsettled(
            "error_remote_mcp_token",
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to mint Remote MCP daemon token",
        )
    })?;
    let token = build::mint_task_token(
        &raw,
        task,
        task.get("agent_id"),
        runtime.workspace_id,
        owner,
    );
    let receipt = response::finalize_claim_delivery(
        state,
        task,
        runtime,
        &mut built.response,
        token,
        &built.delivered_comment_ids,
        built.issue_snapshot.as_deref(),
        remote.as_ref(),
    )
    .await?;
    built.response["auth_token"] = json!(raw);
    if let Some(remote) = remote {
        built.response["remote_mcp_daemon_token"] = json!(remote.raw);
    }
    built.response["delivered_comment_ids"] = json!(receipt);
    Ok(built.response)
}

async fn claim_one_runtime(
    state: &ApiState,
    db: &mut deadpool_postgres::Client,
    runtime: Uuid,
) -> Result<Option<Row>, ApiError> {
    let ids = [runtime];
    promote_due(state, &***db, &ids)
        .await
        .map_err(|_| fail("failed to claim task"))?;
    if let Some(task) = db
        .query(
            RECLAIM,
            &[
                &ids.as_slice(),
                &RECOVERY_SECONDS,
                &FRESH_SECONDS,
                &1_i64,
                &PREPARE_SECONDS,
            ],
        )
        .await
        .map_err(|_| fail("failed to claim task"))?
        .into_iter()
        .next()
    {
        return Ok(Some(task));
    }
    let candidates = db
        .query(CANDIDATES, &[&ids.as_slice()])
        .await
        .map_err(|_| fail("failed to claim task"))?;
    let mut tried = HashSet::new();
    for row in candidates {
        let agent = row.get("agent_id");
        if !tried.insert(agent) {
            continue;
        }
        if let Some(task) = claim_agent(db, agent, runtime)
            .await
            .map_err(|_| fail("failed to claim task"))?
        {
            return Ok(Some(task));
        }
    }
    Ok(None)
}

pub(super) async fn claim_by_runtime(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<GoJson<Value>, ApiError> {
    Uuid::parse_str(raw.trim()).map_err(|_| bad("invalid runtime_id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load runtime"))?;
    let runtime = build::require_daemon_runtime_access(&**db, &identity, &raw).await?;
    let Some(task) = claim_one_runtime(&state, &mut db, runtime.id).await? else {
        return Ok(GoJson(json!({"task":null})));
    };
    if let Some(failure) =
        load::repair_stale_comment_plan_if_needed(&state, &task, runtime.workspace_id).await
    {
        return if failure.settled {
            Ok(GoJson(json!({"task":null})))
        } else {
            Err(error(failure.status, failure.message))
        };
    }
    match deliver_claim(&state, &task, &runtime, &headers).await {
        Ok(payload) => Ok(GoJson(json!({"task":payload}))),
        Err(failure) if failure.settled => Ok(GoJson(json!({"task":null}))),
        Err(failure) => {
            if matches!(
                failure.outcome,
                "error_token" | "error_remote_mcp_token" | "error_claim_finalize"
            ) {
                let _ = service::requeue_task_after_claim_failure(&state, &task).await;
            }
            let message = if failure.outcome == "error_claim_finalize" {
                "failed to finalize task claim".to_owned()
            } else {
                failure.message
            };
            Err(error(failure.status, message))
        }
    }
}

async fn claim(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let outcome = claim_tasks(&state, &identity, &headers, &body).await?;
    let mut response = GoJson(outcome.body).into_response();
    *response.status_mut() = outcome.status;
    if outcome.close {
        // Go closes the connection when a claimed task could not be delivered,
        // so the client does not reuse a possibly stale pooled connection.
        response
            .headers_mut()
            .insert("connection", "close".parse().unwrap());
    }
    Ok(response)
}

/// The claim core, shared by the HTTP endpoint and the WebSocket `tasks.claim`
/// RPC so both transports authorize and claim identically. Go's RPC handler
/// replays the HTTP handler with the connection's identity headers, which is
/// what the WS caller synthesizes before invoking this.
pub(super) struct ClaimOutcome {
    pub status: StatusCode,
    pub body: Value,
    pub close: bool,
}

// Go's five-second machine-claim budget reserves time for recovery and the
// response. Rust recovers before constructing the response (there is no
// after-response callback in axum), inside the same reserved window.
fn batch_deadlines(start: Instant) -> (Instant, Instant) {
    let end = start + Duration::from_secs(5);
    let response = end - Duration::from_millis(250);
    (response - Duration::from_secs(1), response)
}

fn poll_hint_ms(next: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    (next - now).num_milliseconds().max(1_000)
}

pub(super) async fn claim_tasks(
    state: &ApiState,
    identity: &DaemonIdentity,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ClaimOutcome, ApiError> {
    let (build_deadline, response_deadline) = batch_deadlines(Instant::now());
    let (ids, max_tasks, daemon_id) = parse_request(body, identity)?;
    if ids.is_empty() || max_tasks == 0 {
        return Ok(ClaimOutcome {
            status: StatusCode::OK,
            body: json!({"tasks":[]}),
            close: false,
        });
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load runtimes"))?;
    let runtimes = authorized(&db, identity, &ids, &daemon_id).await?;
    if runtimes.is_empty() {
        return Ok(ClaimOutcome {
            status: StatusCode::OK,
            body: json!({"tasks":[]}),
            close: false,
        });
    }
    let by_id = runtimes
        .iter()
        .map(|runtime| (runtime.id, runtime))
        .collect::<HashMap<_, _>>();
    let ids = runtimes
        .iter()
        .map(|runtime| runtime.id)
        .collect::<Vec<_>>();
    tokio::time::timeout_at(build_deadline.into(), promote_due(state, &**db, &ids))
        .await
        .map_err(|_| fail("failed to claim tasks"))??;
    let mut claimed = tokio::time::timeout_at(
        build_deadline.into(),
        db.query(
            RECLAIM,
            &[
                &ids,
                &RECOVERY_SECONDS,
                &FRESH_SECONDS,
                &(max_tasks as i64),
                &PREPARE_SECONDS,
            ],
        ),
    )
    .await
    .map_err(|_| fail("failed to claim tasks"))?
    .map_err(|_| fail("failed to claim tasks"))?;
    if claimed.len() < max_tasks && Instant::now() < build_deadline {
        match tokio::time::timeout_at(build_deadline.into(), db.query(CANDIDATES, &[&ids])).await {
            Ok(Ok(candidates)) => {
                let mut tried = HashSet::new();
                for row in candidates {
                    if claimed.len() >= max_tasks || Instant::now() >= build_deadline {
                        break;
                    }
                    let agent: Uuid = row.get("agent_id");
                    if !tried.insert(agent) {
                        continue;
                    }
                    let runtime: Uuid = row.get("runtime_id");
                    match tokio::time::timeout_at(
                        build_deadline.into(),
                        claim_agent(&mut db, agent, runtime),
                    )
                    .await
                    {
                        Ok(Ok(Some(task))) => claimed.push(task),
                        Ok(Ok(None)) => (),
                        Ok(Err(e)) if claimed.is_empty() => return Err(e),
                        _ => break,
                    }
                }
            }
            Ok(Err(_)) if claimed.is_empty() => return Err(fail("failed to claim tasks")),
            _ => (),
        }
    }
    let mut out = Vec::new();
    let mut delivered = HashSet::new();
    for task in &claimed {
        if Instant::now() >= build_deadline {
            break;
        }
        let Some(runtime) = by_id.get(&task.get::<_, Uuid>("runtime_id")) else {
            continue;
        };
        let attempt = async {
            if let Some(failure) =
                load::repair_stale_comment_plan_if_needed(state, task, runtime.workspace_id).await
            {
                return Err(failure);
            }
            deliver_claim(state, task, runtime, headers).await
        };
        match tokio::time::timeout_at(build_deadline.into(), attempt).await {
            Ok(Ok(payload)) => {
                delivered.insert(task.get::<_, Uuid>("id"));
                out.push(payload);
            }
            Ok(Err(failure)) => {
                eprintln!(
                    "batch claim: omitted task {} ({})",
                    task.get::<_, Uuid>("id"),
                    failure.outcome
                );
            }
            Err(_) => break,
        }
    }
    let omitted = out.len() != claimed.len();
    if omitted {
        // Recover only this claim generation; a settled or already-started
        // task is untouched by the service CAS. Keep one deadline for the set.
        for task in &claimed {
            if delivered.contains(&task.get::<_, Uuid>("id")) {
                continue;
            }
            if Instant::now() >= response_deadline {
                break;
            }
            let _ = tokio::time::timeout_at(
                response_deadline.into(),
                service::requeue_task_after_claim_failure(state, task),
            )
            .await;
        }
    }
    let mut result = json!({"tasks":out});
    if !omitted
        && out.len() < max_tasks
        && capability(headers, "claim-poll-hints-v1")
        && Instant::now() < response_deadline
    {
        let hint = async {
            db.query_one("SELECT MIN(t.fire_at)::timestamptz FROM agent_task_queue t WHERE t.runtime_id=ANY($1::uuid[]) AND t.status='deferred' AND EXISTS(SELECT 1 FROM agent_runtime r WHERE r.id=t.runtime_id AND r.status='online' AND COALESCE(r.last_seen_at,r.updated_at)>=now()-make_interval(secs=>$2::double precision)) AND (t.fire_at>now() OR NOT EXISTS(SELECT 1 FROM agent_task_queue o WHERE o.issue_id=t.issue_id AND o.agent_id=t.agent_id AND o.comment_thread_id IS NOT DISTINCT FROM t.comment_thread_id AND o.id<>t.id AND (o.status IN ('queued','dispatched') OR (o.status='deferred' AND o.context->>'channel_issue_media_pending'='true'))))", &[&ids,&FRESH_SECONDS]).await
        };
        if let Ok(Ok(row)) = tokio::time::timeout_at(response_deadline.into(), hint).await {
            result["claim_poll_hint_supported"] = json!(true);
            if let Some(next) = row.get::<_, Option<DateTime<Utc>>>(0) {
                result["next_deferred_task_after_ms"] = json!(poll_hint_ms(next, Utc::now()));
            }
        }
    }
    Ok(ClaimOutcome {
        status: StatusCode::OK,
        body: result,
        close: omitted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_bounds_and_authority() {
        let identity = DaemonIdentity::User(Uuid::new_v4());
        assert_eq!(
            parse_request(br#"{}"#, &identity).err().unwrap().1,
            "daemon_id is required"
        );
        assert_eq!(
            parse_request(br#"{"daemon_id":"d","max_tasks":-1}"#, &identity)
                .err()
                .unwrap()
                .1,
            "max_tasks must not be negative"
        );
        assert_eq!(
            parse_request(br#"{"daemon_id":"d","max_tasks":"1"}"#, &identity)
                .err()
                .unwrap()
                .1,
            "invalid request body"
        );
        let id = Uuid::new_v4();
        let raw =
            format!(r#"{{"daemon_id":"d","max_tasks":999,"runtime_ids":["bad","{id}","{id}"]}}"#);
        let (ids, max, daemon) = parse_request(raw.as_bytes(), &identity).unwrap();
        assert_eq!((ids, max, daemon), (vec![id], CAP, "d".into()));
        let bound = DaemonIdentity::Bound {
            workspace: Uuid::nil(),
            daemon: "actual".into(),
            owner: None,
            hash: String::new(),
        };
        assert_eq!(
            parse_request(br#"{"daemon_id":"other"}"#, &bound)
                .err()
                .unwrap()
                .0,
            StatusCode::FORBIDDEN
        );
    }
    #[test]
    fn token_is_unique_and_scoped() {
        let a = token().unwrap();
        let b = token().unwrap();
        assert!(a.starts_with("mat_") && a.len() == 44 && a != b);
        assert!(!capability(&HeaderMap::new(), "claim-poll-hints-v1"));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-client-capabilities",
            "other, claim-poll-hints-v1, coalesced-comments-v1"
                .parse()
                .unwrap(),
        );
        assert!(capability(&headers, "claim-poll-hints-v1"));
        assert!(capability(&headers, "coalesced-comments-v1"));
        assert!(!capability(&headers, "claim_poll_hints_v1"));
    }

    #[tokio::test]
    async fn both_batch_claim_paths_reach_the_same_handler() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use deadpool_postgres::{Manager, Pool};
        use http_body_util::BodyExt;
        use std::{str::FromStr, sync::Arc};
        use tokio_postgres::{Config, NoTls};
        use tower::ServiceExt;

        // A malformed body is rejected before the pool is touched, so an
        // unreachable URL proves both routes registered the handler.
        let config = Config::from_str("postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
            .expect("synthetic config");
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(1)
                .build()
                .unwrap(),
        );
        let state = ApiState::new(pool, b"synthetic-test-secret".to_vec());
        let app: Router = super::routes()
            .with_state(state)
            .layer(axum::Extension(DaemonIdentity::User(Uuid::nil())));
        for path in ["/api/daemon/tasks/claim", "/api/daemon/claim"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"max_tasks":1}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(body["error"], "daemon_id is required", "{path}");
        }
    }
}
