//! User-facing issue/task lifecycle. This router is mounted under catalog's
//! authenticated workspace guard; it never trusts a task or issue ID as tenancy.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    agent_invoke::{self, InvokeTarget, Invoker},
    catalog::WorkspaceId,
    error, issue_read,
    issue_write_create::{IssueCountPolicy, quick_create_context, quota_response},
    runtime_chat_daemon::system_event,
    runtime_chat_events, runtime_chat_task, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde_json::{Value, json};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/active-task", get(active_task))
        .route("/api/issues/{id}/task-runs", get(task_runs))
        .route("/api/issues/{id}/rerun", post(rerun))
        .route(
            "/api/issues/{id}/tasks/{taskId}/cancel",
            post(cancel_issue_task),
        )
        .route("/api/tasks/{taskId}/cancel", post(cancel_task))
        .route("/api/tasks/{taskId}/rerun", post(rerun_task))
        .route("/api/tasks/{taskId}/messages", get(messages))
        .route(
            "/api/tasks/{taskId}/retry-source-context",
            post(retry_source_context),
        )
}

fn fail(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn blocked(reason: &'static str, message: &'static str) -> Response {
    (
        StatusCode::FORBIDDEN,
        GoJson(json!({"error": message, "reason_code": reason})),
    )
        .into_response()
}
fn not_found() -> ApiError {
    error(StatusCode::NOT_FOUND, "task not found")
}
fn task_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad("invalid task id"))
}

async fn issue(db: &deadpool_postgres::Client, ws: Uuid, raw: &str) -> Result<Uuid, ApiError> {
    Ok(issue_read::load_issue(db, ws, raw).await?.get("id"))
}

async fn task(db: &impl GenericClient, ws: Uuid, id: Uuid) -> Result<Row, ApiError> {
    db.query_opt("SELECT t.* FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id AND a.workspace_id=$2 WHERE t.id=$1 AND (t.issue_id IS NULL OR EXISTS (SELECT 1 FROM issue i WHERE i.id=t.issue_id AND i.workspace_id=$2))", &[&id,&ws])
        .await.map_err(|_| fail("failed to load task"))?.ok_or_else(not_found)
}

fn params(query: Option<&str>) -> std::collections::HashMap<String, String> {
    super::catalog::parse_params(query)
}

fn task_run_filters(
    query: &std::collections::HashMap<String, String>,
) -> Result<(bool, bool), ApiError> {
    let scope = query.get("scope").map(String::as_str).unwrap_or("");
    if !matches!(scope, "" | "issue" | "family") {
        return Err(bad("scope must be 'issue' or 'family'"));
    }
    let active = match query.get("active").map(String::as_str).unwrap_or("") {
        "" | "false" => false,
        "true" => true,
        _ => return Err(bad("invalid active parameter; expected boolean")),
    };
    Ok((scope == "family", active))
}

fn visible_history(status: &str, escalation: Option<Uuid>, started: Option<DateTime<Utc>>) -> bool {
    !(escalation.is_some() && started.is_none() && matches!(status, "deferred" | "cancelled"))
}

// The Go task response includes null timestamp/result/error fields and always
// includes delivered_comment_ids. Reuse the existing projection so all task
// surfaces share the same attribution/kind and workdir redaction rules.
fn response(row: &Row, ws: Uuid) -> Value {
    runtime_chat_task::response(row, ws)
}

pub(super) async fn hydrate_attribution(db: &impl GenericClient, tasks: &mut [Value]) {
    use std::collections::{HashMap, HashSet};
    let ids: Vec<Uuid> = tasks
        .iter()
        .flat_map(|t| {
            ["initiator", "originator"].into_iter().filter_map(|key| {
                t["attribution"][key]["id"]
                    .as_str()
                    .and_then(|id| Uuid::parse_str(id).ok())
            })
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if ids.is_empty() {
        return;
    }
    let Ok(users) = db
        .query(
            "SELECT id,name,email,avatar_url FROM \"user\" WHERE id=ANY($1::uuid[])",
            &[&ids],
        )
        .await
    else {
        return;
    };
    let users: HashMap<Uuid, &Row> = users.iter().map(|r| (r.get("id"), r)).collect();
    for task in tasks {
        for key in ["initiator", "originator"] {
            let Some(id) = task["attribution"][key]["id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
            else {
                continue;
            };
            let Some(user) = users.get(&id) else {
                continue;
            };
            task["attribution"][key]["name"] = json!(user.get::<_, String>("name"));
            let email: String = user.get("email");
            if !email
                .trim()
                .to_ascii_lowercase()
                .ends_with("@channel.invalid")
            {
                task["attribution"][key]["email"] = json!(email);
            }
            // An object key is not a public avatar URL. Only a configured URL
            // policy may resolve it (`resolve_avatar_url` needs ApiState, which
            // this display-only helper deliberately does not thread through), so
            // the stored key is never echoed as metadata.
        }
    }
}

// The execution log exposes whether a running turn can still take a steering
// comment. Metadata is optional: a failed lookup leaves the fields absent
// rather than failing the list, matching Go's fail-closed hydration.
async fn hydrate_supplements(
    db: &tokio_postgres::Client,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    rows: &[Row],
    tasks: &mut [Value],
) {
    use std::collections::HashMap;
    let ids: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.get::<_, Option<Uuid>>("issue_id").is_some())
        .map(|r| r.get("id"))
        .collect();
    if ids.is_empty() {
        return;
    }
    // Go resolves the caller first: a task-token request is judged as the
    // steering comment it would write (an agent may only steer a DIFFERENT
    // agent it may invoke), everything else as the member it authenticated as.
    // The bound task is the only source of the agent's originator, and a
    // terminal run lends none — same rule as invokeOriginatorFromRequest.
    let agent_caller = match actor {
        Actor::Task(task) => Some((task.agent, task.task)),
        Actor::Human | Actor::Cloud | Actor::Daemon => None,
    };
    let originator = match &agent_caller {
        Some((_, task_id)) => {
            let row = db
                .query_opt(
                    "SELECT status,originator_user_id FROM agent_task_queue WHERE id=$1",
                    &[task_id],
                )
                .await
                .ok()
                .flatten();
            row.filter(|row| {
                !matches!(
                    row.get::<_, String>("status").as_str(),
                    "completed" | "failed" | "cancelled"
                )
            })
            .and_then(|row| row.get("originator_user_id"))
        }
        None => None,
    };
    let Ok(metadata) = db
        .query(
            "SELECT cap.task_id,cap.capability, \
             COALESCE(array_agg(s.comment_id ORDER BY s.created_at,s.comment_id) FILTER (WHERE s.comment_id IS NOT NULL),'{}'::uuid[])::uuid[] AS comment_ids \
             FROM task_supplement_capability cap LEFT JOIN task_supplement s ON s.task_id=cap.task_id \
             WHERE cap.workspace_id=$1 AND cap.task_id=ANY($2::uuid[]) GROUP BY cap.task_id,cap.capability",
            &[&ws, &ids],
        )
        .await
    else {
        return;
    };
    let indexed: HashMap<Uuid, &Row> = metadata.iter().map(|r| (r.get("task_id"), r)).collect();
    let agent_ids: Vec<Uuid> = rows
        .iter()
        .map(|r| r.get("agent_id"))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let Ok(agents) = db
        .query(
            "SELECT id,owner_id,permission_mode FROM agent WHERE workspace_id=$1 AND id=ANY($2::uuid[])",
            &[&ws, &agent_ids],
        )
        .await
    else {
        return;
    };
    let agents: HashMap<Uuid, &Row> = agents.iter().map(|r| (r.get("id"), r)).collect();
    let mut permission: HashMap<Uuid, bool> = HashMap::new();
    for (row, task) in rows.iter().zip(tasks.iter_mut()) {
        let Some(meta) = indexed.get(&row.get::<_, Uuid>("id")) else {
            continue;
        };
        let cap: String = meta.get("capability");
        if !cap.is_empty() {
            task["supplement_capability"] = json!(cap);
        }
        let comments: Vec<Uuid> = meta.get("comment_ids");
        if !comments.is_empty() {
            task["supplement_comment_ids"] = json!(comments);
        }
        let agent: Uuid = row.get("agent_id");
        if row.get::<_, String>("status") != "running" || cap != "task-supplement-v1" {
            continue;
        }
        let allowed = match permission.get(&agent) {
            Some(allowed) => *allowed,
            None => {
                let allowed = match agents.get(&agent) {
                    Some(a) => {
                        let target = InvokeTarget {
                            id: agent,
                            owner_id: a.get("owner_id"),
                            permission_mode: &a.get::<_, String>("permission_mode"),
                        };
                        match agent_caller {
                            // agentMaySteer denies a nil or self-referencing
                            // caller before any read: an agent never grants
                            // itself a steer, and an unresolved identity fails
                            // closed rather than falling back to the member
                            // gate it authenticated with.
                            Some((caller, _)) if caller.is_nil() || agent == caller => false,
                            Some((caller, _)) => agent_invoke::can_invoke(
                                db,
                                ws,
                                target,
                                Invoker {
                                    actor_type: "agent",
                                    actor_id: caller,
                                    originator,
                                },
                            )
                            .await
                            .unwrap_or(false),
                            // Go asks canInvokeAgent with "member"/userID/userID
                            // for every other caller: it is the same gate the
                            // other member-initiated surfaces use.
                            None => agent_invoke::can_invoke(
                                db,
                                ws,
                                target,
                                Invoker {
                                    actor_type: "member",
                                    actor_id: user,
                                    originator: Some(user),
                                },
                            )
                            .await
                            .unwrap_or(false),
                        }
                    }
                    None => false,
                };
                permission.insert(agent, allowed);
                allowed
            }
        };
        if allowed {
            task["can_supplement"] = json!(true);
        }
    }
}

async fn active_task(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state.pool.get().await.map_err(|_| not_found())?;
    let id = issue(&db, ws, &raw).await?;
    // The Go active banner is best effort on query failure, unlike task-runs.
    let rows = db.query("SELECT * FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory') ORDER BY created_at DESC", &[&id]).await.unwrap_or_default();
    let mut tasks: Vec<_> = rows.iter().map(|r| response(r, ws)).collect();
    hydrate_attribution(&db, &mut tasks).await;
    Ok(GoJson(json!({"tasks": tasks})))
}

async fn task_runs(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, ApiError> {
    let db = state.pool.get().await.map_err(|_| not_found())?;
    let id = issue(&db, ws, &raw).await?;
    let query = params(raw_query.as_deref());
    let (family, active) = task_run_filters(&query)?;
    if family {
        let root: Uuid = db
            .query_one(
                "SELECT COALESCE(parent_issue_id,id) FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .map_err(|_| fail("failed to list tasks"))?
            .get(0);
        let cap = 20_i64;
        let rows = db.query("SELECT t.id AS task_id,t.issue_id,i.number AS issue_number,i.title AS issue_title,t.agent_id,t.status,t.created_at,t.started_at,w.issue_prefix \
            FROM agent_task_queue t JOIN issue i ON i.id=t.issue_id JOIN workspace w ON w.id=i.workspace_id \
            WHERE i.workspace_id=$1 AND (i.id=$2 OR i.parent_issue_id=$2) \
            AND t.status IN ('queued','dispatched','running','waiting_local_directory') \
            ORDER BY CASE t.status WHEN 'running' THEN 0 WHEN 'dispatched' THEN 1 WHEN 'waiting_local_directory' THEN 2 ELSE 3 END,t.created_at DESC LIMIT $3", &[&ws,&root,&(cap+1)]).await.map_err(|_| fail("failed to list tasks"))?;
        let summaries: Vec<Value> = rows
            .iter()
            .take(cap as usize)
            .map(|r| {
                let prefix: String = r.get("issue_prefix");
                let created: DateTime<Utc> = r.get("created_at");
                let started: Option<DateTime<Utc>> = r.get("started_at");
                let mut summary = json!({"task_id":r.get::<_,Uuid>("task_id"),"issue_id":r.get::<_,Uuid>("issue_id"),
                "issue_identifier":format!("{}-{}",prefix,r.get::<_,i32>("issue_number")),
                "issue_title":r.get::<_,String>("issue_title"),"agent_id":r.get::<_,Uuid>("agent_id"),
                "status":r.get::<_,String>("status"),"created_at":timestamp(created)});
                if let Some(started) = started {
                    summary["started_at"] = json!(timestamp(started));
                }
                summary
            })
            .collect();
        let mut response = GoJson(json!(summaries)).into_response();
        if rows.len() > cap as usize {
            response.headers_mut().insert(
                "X-Active-Runs-Truncated",
                axum::http::HeaderValue::from_static("true"),
            );
        }
        return Ok(response);
    }
    let rows = db.query(if active {
        "SELECT * FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory') ORDER BY created_at DESC"
    } else { "SELECT * FROM agent_task_queue WHERE issue_id=$1 ORDER BY created_at DESC" }, &[&id])
        .await.map_err(|_| fail("failed to list tasks"))?;
    let visible_rows: Vec<Row> = rows
        .iter()
        .filter(|row| {
            let escalation: Option<Uuid> = row.get("escalation_for_task_id");
            let started: Option<DateTime<Utc>> = row.get("started_at");
            let status: String = row.get("status");
            visible_history(&status, escalation, started)
        })
        .cloned()
        .collect();
    let mut tasks: Vec<_> = visible_rows.iter().map(|r| response(r, ws)).collect();
    hydrate_attribution(&db, &mut tasks).await;
    hydrate_supplements(&db, ws, user, &actor, &visible_rows, &mut tasks).await;
    if !active
        && !tasks.is_empty()
        && let Ok(usage) = db.query("SELECT task_id,provider,model,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost_usd_ticks FROM task_usage WHERE task_id IN (SELECT id FROM agent_task_queue WHERE issue_id=$1)", &[&id]).await
    {
            for t in &mut tasks {
                let Some(tid) = t["id"].as_str().and_then(|x| Uuid::parse_str(x).ok()) else { continue; };
                let items: Vec<_> = usage.iter().filter(|r| r.get::<_,Uuid>("task_id") == tid).map(|r| {
                    let mut v = json!({"model":r.get::<_,String>("model"),
                        "input_tokens":r.get::<_,i64>("input_tokens"),"output_tokens":r.get::<_,i64>("output_tokens"),
                        "cache_read_tokens":r.get::<_,i64>("cache_read_tokens"),"cache_write_tokens":r.get::<_,i64>("cache_write_tokens")});
                    let provider: String = r.get("provider");
                    if !provider.is_empty() { v["provider"] = json!(provider); }
                    if let Some(cost) = r.get::<_,Option<i64>>("cost_usd_ticks") { v["cost_usd_ticks"] = json!(cost); }
                    v
                }).collect();
                if !items.is_empty() { t["usage"] = json!(items); }
            }
    }
    Ok(GoJson(json!(tasks)).into_response())
}

async fn chat_access(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    session: Uuid,
    queued_only: bool,
) -> Result<(), ApiError> {
    let row = db.query_opt("SELECT cs.creator_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type,a.owner_id,a.id AS agent_id,a.permission_mode \
        FROM chat_session cs JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id \
        WHERE cs.id=$1 AND cs.workspace_id=$2", &[&session,&ws]).await.map_err(|_| fail("failed to load chat session"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "chat session not found"))?;
    let owner: Option<Uuid> = row.get("owner_id");
    let human = matches!(actor, Actor::Human);
    let guest: bool = row.get("zbs_has_guest_history");
    let creator =
        row.get::<_, Uuid>("creator_id") == user && (!guest || (human && owner == Some(user)));
    let source: Option<String> = row.get("zbs_source_channel_type");
    let owner_view = human
        && owner == Some(user)
        && source
            .as_deref()
            .is_some_and(|s| matches!(s, "telegram" | "zalo"));
    if let Actor::Task(bound) = actor
        && db.query_opt("SELECT 1 FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id WHERE t.id=$1 AND t.agent_id=$2 AND a.workspace_id=$3 AND t.chat_session_id=$4 AND t.status NOT IN ('completed','failed','cancelled')", &[&bound.task,&bound.agent,&ws,&session]).await.map_err(|_| fail("failed to load task"))?.is_none()
    {
        return Err(error(StatusCode::FORBIDDEN,"not your chat session"));
    }
    if !(creator || owner_view && !queued_only) {
        return Err(error(StatusCode::FORBIDDEN, "not your chat session"));
    }
    if creator && owner != Some(user) {
        let role = db
            .query_one(
                "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| fail("failed to resolve agent access"))?
            .get::<_, String>(0);
        if !matches!(role.as_str(), "owner" | "admin") {
            let agent: Uuid = row.get("agent_id");
            if row.get::<_,String>("permission_mode") != "public_to" || db.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1", &[&agent,&ws,&user]).await.map_err(|_| fail("failed to resolve agent access"))?.is_none() {
                return Err(error(StatusCode::FORBIDDEN,"you do not have access to this agent"));
            }
        }
    }
    Ok(())
}

async fn messages(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid task_id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load task"))?;
    let task = task(&db, ws, id).await?;
    if let Some(session) = task.get::<_, Option<Uuid>>("chat_session_id") {
        let creator = db
            .query_opt(
                "SELECT creator_id FROM chat_session WHERE id=$1 AND workspace_id=$2",
                &[&session, &ws],
            )
            .await
            .map_err(|_| fail("failed to load task"))?
            .map(|row| row.get::<_, Uuid>(0));
        if creator != Some(user) {
            return Err(not_found());
        }
    }
    let query = params(query.as_deref());
    let since = query
        .get("since")
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<i64>().map_err(|_| bad("invalid since parameter")))
        .transpose()?;
    let rows = db.query("SELECT seq,type,tool,call_id,content,input,output,output_truncated,created_at FROM task_message \
        WHERE task_id=$1 AND ($2::bigint IS NULL OR seq>$2) ORDER BY seq ASC", &[&id,&since]).await.map_err(|_| fail("failed to list task messages"))?;
    let issue: Option<Uuid> = task.get("issue_id");
    let messages: Vec<Value> = rows
        .iter()
        .map(|r| {
            let mut v =
                json!({"task_id":id,"seq":r.get::<_,i32>("seq"),"type":r.get::<_,String>("type")});
            if let Some(issue) = issue {
                v["issue_id"] = json!(issue);
            }
            for key in ["tool", "call_id", "content", "output"] {
                if let Some(text) = r.get::<_, Option<String>>(key).filter(|s| !s.is_empty()) {
                    v[key] = json!(text);
                }
            }
            if let Some(input) = r.get::<_, Option<Value>>("input")
                && !input.is_null()
            {
                v["input"] = input;
            }
            if let Some(truncated) = r.get::<_, Option<bool>>("output_truncated") {
                v["output_truncated"] = json!(truncated);
            }
            let created: DateTime<Utc> = r.get("created_at");
            v["created_at"] = json!(created.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true));
            v
        })
        .collect();
    Ok(GoJson(json!(messages)))
}

async fn private_agent_access(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    agent: Uuid,
) -> Result<(), ApiError> {
    if matches!(actor, Actor::Task(_)) {
        return Ok(());
    }
    let row = db
        .query_opt(
            "SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2",
            &[&agent, &ws],
        )
        .await
        .map_err(|_| fail("failed to resolve agent access"))?
        .ok_or_else(not_found)?;
    if row.get::<_, Option<Uuid>>("owner_id") == Some(user) {
        return Ok(());
    }
    let role = db
        .query_one(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail("failed to resolve agent access"))?
        .get::<_, String>(0);
    if matches!(role.as_str(), "owner" | "admin") {
        return Ok(());
    }
    if row.get::<_,String>("permission_mode") == "public_to" && db.query_opt(
        "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1", &[&agent,&ws,&user]
    ).await.map_err(|_| fail("failed to resolve agent access"))?.is_some() { return Ok(()); }
    Err(error(
        StatusCode::FORBIDDEN,
        "you do not have access to this agent",
    ))
}

// A CAS against the issue ID is performed inside the transaction as well as
// the preflight. This is not a cross-issue cancellation primitive.
async fn cancel_issue_task(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_issue, raw_task)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<GoJson<Value>, ApiError> {
    let db = state.pool.get().await.map_err(|_| not_found())?;
    let issue_id = issue(&db, ws, &raw_issue).await?;
    // Go parses this path with parseUUID (invalid UUID is an opaque 404).
    let id = Uuid::parse_str(&raw_task).map_err(|_| not_found())?;
    let row = task(&db, ws, id).await?;
    if row.get::<_, Option<Uuid>>("issue_id") != Some(issue_id) {
        return Err(not_found());
    }
    drop(db);
    let result = cancel(
        &state,
        ws,
        user,
        &actor,
        id,
        Some(issue_id),
        false,
        None,
        None,
        &headers,
    )
    .await?;
    Ok(GoJson(result))
}

async fn cancel_task(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Result<GoJson<Value>, ApiError> {
    let id = task_id(&raw)?;
    let params = params(raw_query.as_deref());
    let queued = match params
        .get("expected_status")
        .map(String::as_str)
        .unwrap_or("")
    {
        "" => false,
        "queued" => true,
        _ => return Err(bad("expected_status must be queued")),
    };
    let expected = if queued {
        Some(
            Uuid::parse_str(
                params
                    .get("chat_session_id")
                    .map(String::as_str)
                    .unwrap_or(""),
            )
            .map_err(|_| bad("invalid chat_session_id"))?,
        )
    } else {
        None
    };
    if queued
        && !matches!(
            params.get("queue_action").map(String::as_str),
            Some("edit" | "remove")
        )
    {
        // Go validates this only after loading the task and verifying session.
        let db = state
            .pool
            .get()
            .await
            .map_err(|_| fail("failed to load task"))?;
        let t = task(&db, ws, id).await?;
        if t.get::<_, Option<Uuid>>("chat_session_id") != expected {
            return Err(error(
                StatusCode::CONFLICT,
                "task does not belong to the expected chat session",
            ));
        }
        return Err(bad("queue_action must be edit or remove"));
    }
    let result = cancel(
        &state,
        ws,
        user,
        &actor,
        id,
        None,
        queued,
        expected,
        params.get("queue_action").map(String::as_str),
        &headers,
    )
    .await?;
    Ok(GoJson(result))
}

async fn settle(tx: &impl GenericClient, task: Uuid) -> Result<(), ApiError> {
    tx.execute("UPDATE task_supplement AS supplement SET status='failed',failure_reason='turn_ended',updated_at=now() FROM agent_task_queue AS task WHERE supplement.task_id=task.id AND task.id=$1 AND task.status IN ('completed','failed','cancelled') AND supplement.status IN ('pending','delivering')", &[&task]).await.map_err(|_| fail("failed to settle terminal task state"))?;
    tx.execute("UPDATE comment AS recovery SET recovery_settled_at=now() FROM agent_task_queue AS task WHERE task.id=$1 AND task.status IN ('completed','failed','cancelled') AND recovery.id=ANY(task.delivered_comment_ids) AND recovery.author_type='system' AND recovery.type='progress_update' AND recovery.source_task_id IS NOT NULL AND recovery.recovery_settled_at IS NULL", &[&task]).await.map_err(|_| fail("failed to settle terminal task state"))?;
    tx.execute("DELETE FROM task_token WHERE task_id=$1", &[&task])
        .await
        .map_err(|_| fail("failed to revoke task token"))?;
    Ok(())
}

// A queued direct-chat edit removes its sealed input and persists a draft;
// a remove deletes the input but does not offer it back. Channel-ingested
// inputs are durable provider history and receive a visible stopped reply.
async fn settle_queued_chat_input(
    tx: &impl GenericClient,
    row: &Row,
    action: &str,
) -> Result<Option<Value>, ApiError> {
    let task: Uuid = row.get("id");
    let session: Uuid = row
        .get::<_, Option<Uuid>>("chat_session_id")
        .ok_or_else(|| fail("cancel task failed"))?;
    let input_owner = row
        .get::<_, Option<Uuid>>("chat_input_task_id")
        .unwrap_or(task);
    let channel: bool = tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM chat_message WHERE task_id=$1 AND role='user' AND channel_ingested)",
        &[&input_owner],
    ).await.map_err(|_| fail("cancel task failed"))?.get(0);
    if channel {
        let assistant = tx.query_one(
            "INSERT INTO chat_message (id,chat_session_id,role,content,task_id) VALUES ($1,$2,'assistant','Stopped.',$3) RETURNING created_at",
            &[&Uuid::now_v7(), &session, &task],
        ).await.map_err(|_| fail("cancel task failed"))?;
        let created: DateTime<Utc> = assistant.get("created_at");
        tx.execute("UPDATE chat_message AS queued_input SET created_at=$2::timestamptz+interval '1 microsecond' WHERE queued_input.chat_session_id=$1 AND queued_input.role='user' AND NOT queued_input.channel_ingested AND queued_input.message_kind<>'onboarding_kickoff' AND queued_input.created_at<=$2 AND EXISTS (SELECT 1 FROM agent_task_queue AS queued_task WHERE queued_task.id=queued_input.task_id AND queued_task.chat_session_id=queued_input.chat_session_id AND queued_task.status='queued' AND queued_task.chat_input_task_id=queued_task.id AND queued_task.regenerate_quick_actions_for IS NULL AND queued_task.id=(SELECT head.id FROM agent_task_queue head WHERE head.chat_session_id=queued_input.chat_session_id AND head.status IN ('queued','dispatched','running','waiting_local_directory','deferred') AND head.regenerate_quick_actions_for IS NULL ORDER BY CASE WHEN head.status IN ('dispatched','running','waiting_local_directory') THEN 0 WHEN head.status='deferred' THEN 1 ELSE 2 END,head.priority DESC,head.created_at ASC,head.id ASC LIMIT 1))", &[&session,&created]).await.map_err(|_| fail("cancel task failed"))?;
        return Ok(None);
    }
    tx.execute("UPDATE chat_message SET task_id=(SELECT successor.id FROM agent_task_queue successor WHERE successor.chat_session_id=chat_message.chat_session_id AND successor.status='queued' AND successor.chat_input_task_id=successor.id AND successor.regenerate_quick_actions_for IS NULL AND successor.id<>$1 ORDER BY successor.priority DESC,successor.created_at ASC,successor.id ASC LIMIT 1) WHERE task_id=$1 AND role='user' AND message_kind='onboarding_kickoff'", &[&input_owner]).await.map_err(|_| fail("cancel task failed"))?;
    let attachments = if action == "edit" {
        tx.query("UPDATE attachment SET chat_message_id=NULL WHERE chat_message_id IN (SELECT id FROM chat_message WHERE task_id=$1 AND role='user') RETURNING id", &[&input_owner]).await.map_err(|_| fail("cancel task failed"))?
    } else {
        Vec::new()
    };
    let deleted = tx.query_opt("DELETE FROM chat_message WHERE task_id=$1 AND role='user' AND message_kind<>'onboarding_kickoff' RETURNING id,chat_session_id,content", &[&input_owner]).await.map_err(|_| fail("cancel task failed"))?;
    let Some(deleted) = deleted else {
        return Ok(None);
    };
    let message_id: Uuid = deleted.get("id");
    let content: String = deleted.get("content");
    let restore = action == "edit";
    if restore {
        let attachment_ids: Vec<Uuid> = attachments.iter().map(|r| r.get("id")).collect();
        tx.execute("INSERT INTO chat_draft_restore (id,chat_session_id,task_id,content,attachment_ids) VALUES ($1,$2,$3,$4,$5)", &[&message_id,&session,&task,&content,&attachment_ids]).await.map_err(|_| fail("cancel task failed"))?;
    }
    Ok(Some(
        json!({"chat_session_id": session,"message_id": message_id,"content": content,"restore_to_input": restore}),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn cancel(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    id: Uuid,
    expected_issue: Option<Uuid>,
    queued: bool,
    expected_session: Option<Uuid>,
    queue_action: Option<&str>,
    _headers: &HeaderMap,
) -> Result<Value, ApiError> {
    // The task-service cancellation has broadcasts, settlement and daemon
    // interrupt semantics. Never accept a mutation without an event sink.
    if state.task_cancel_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "task event bus unavailable",
        ));
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load task"))?;
    let row = task(&db, ws, id).await?;
    if expected_issue.is_some() && row.get::<_, Option<Uuid>>("issue_id") != expected_issue {
        return Err(not_found());
    }
    if queued && row.get::<_, Option<Uuid>>("chat_session_id") != expected_session {
        return Err(error(
            StatusCode::CONFLICT,
            "task does not belong to the expected chat session",
        ));
    }
    let agent: Uuid = row.get("agent_id");
    let chat: Option<Uuid> = row.get("chat_session_id");
    // The issue-scoped route is authorized by issue visibility and the
    // issue/task relationship. The bare task route additionally gates private
    // agents and chat ownership, as Go's CancelTaskByUser does.
    if expected_issue.is_none() {
        if let Some(chat) = chat {
            chat_access(&db, ws, user, actor, chat, queued)
                .await
                .map_err(|err| {
                    if err.0 == StatusCode::FORBIDDEN {
                        error(StatusCode::FORBIDDEN, "not your task")
                    } else {
                        err
                    }
                })?;
        } else {
            private_agent_access(&db, ws, user, actor, agent).await?;
        }
    }
    let (actor_type, actor_id, actor_name) = match actor {
        Actor::Task(bound) => {
            let name = db
                .query_opt(
                    "SELECT name FROM agent WHERE id=$1 AND workspace_id=$2",
                    &[&bound.agent, &ws],
                )
                .await
                .ok()
                .flatten()
                .map(|r| r.get::<_, String>(0));
            ("agent", bound.agent, name)
        }
        _ => {
            let name = db
                .query_opt("SELECT name FROM \"user\" WHERE id=$1", &[&user])
                .await
                .ok()
                .flatten()
                .map(|r| r.get::<_, String>(0));
            ("member", user, name)
        }
    };
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("cancel task failed"))?;
    // Chat session precedes task row in the global lock order. Holding both
    // makes the pointer update and cancellation visible atomically.
    if let Some(chat) = chat {
        tx.query_opt(
            "SELECT id FROM chat_session WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&chat, &ws],
        )
        .await
        .map_err(|_| fail("cancel task failed"))?
        .ok_or_else(not_found)?;
    }
    let row = task(&tx, ws, id).await?;
    if expected_issue.is_some() && row.get::<_, Option<Uuid>>("issue_id") != expected_issue {
        return Err(not_found());
    }
    let changed = tx.query_opt(
        "UPDATE agent_task_queue t SET status='cancelled',completed_at=now(),prepare_lease_expires_at=NULL, \
         cancelled_by_type=$3,cancelled_by_id=$4,cancelled_by_name=$5 \
         WHERE t.id=$1 AND t.agent_id=$2 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') \
         AND ($6::uuid IS NULL OR t.issue_id=$6) AND ($7::bool=false OR (t.status='queued' AND t.chat_session_id=$8)) RETURNING t.*",
        &[&id,&agent,&actor_type,&actor_id,&actor_name,&expected_issue,&queued,&expected_session],
    ).await.map_err(|_| fail("cancel task failed"))?;
    let Some(cancelled) = changed else {
        if queued {
            return Err(error(StatusCode::CONFLICT, "task is no longer queued"));
        }
        // Go cancellation is idempotent for already-terminal rows.
        let mut result = response(&row, ws);
        hydrate_attribution(&tx, std::slice::from_mut(&mut result)).await;
        return Ok(result);
    };
    let cancelled_chat_message = if queued {
        settle_queued_chat_input(&tx, &cancelled, queue_action.unwrap_or("remove")).await?
    } else {
        None
    };
    settle(&tx, id).await?;
    if !queued {
        tx.execute("UPDATE agent_task_queue task SET result=COALESCE(task.result,'{}'::jsonb)||'{\"process_cleanup_pending\":true}'::jsonb WHERE task.id=$1 AND task.status='cancelled' AND task.started_at IS NOT NULL AND task.result->>'process_cleanup_capability'='pi-process-cleanup-v1' AND EXISTS (SELECT 1 FROM agent_runtime runtime WHERE runtime.id=task.runtime_id AND runtime.provider IN ('pi','omp'))", &[&id]).await.map_err(|_| fail("cancel task failed"))?;
        if chat.is_some() {
            tx.execute("UPDATE chat_session cs SET session_id=t.session_id,runtime_id=t.runtime_id,work_dir=COALESCE(t.work_dir,cs.work_dir),updated_at=now() FROM agent_task_queue t WHERE t.id=$1 AND t.chat_session_id=cs.id AND t.status='cancelled' AND t.session_id IS NOT NULL AND t.runtime_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM agent_task_queue newer WHERE newer.chat_session_id=t.chat_session_id AND newer.id<>t.id AND newer.session_id IS NOT NULL AND newer.created_at>t.created_at)", &[&id]).await.map_err(|_| fail("cancel task failed"))?;
        }
    }
    tx.execute("WITH desired AS (SELECT CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue t WHERE t.agent_id=$1 AND t.status IN ('dispatched','running')) THEN 'working' ELSE 'idle' END AS status) UPDATE agent a SET status=desired.status,updated_at=now() FROM desired WHERE a.id=$1 AND a.status IS DISTINCT FROM desired.status", &[&agent]).await.map_err(|_| fail("cancel task failed"))?;
    let mut result = response(&cancelled, ws);
    if let Some(message) = cancelled_chat_message {
        result["cancelled_chat_message"] = message;
    }
    hydrate_attribution(&tx, std::slice::from_mut(&mut result)).await;
    let audience = if let Some(chat) = chat {
        tx.query_opt("SELECT cs.creator_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type,a.owner_id FROM chat_session cs JOIN agent a ON a.id=cs.agent_id WHERE cs.id=$1 AND cs.workspace_id=$2", &[&chat,&ws]).await.map_err(|_| fail("cancel task failed"))?
    } else {
        None
    };
    tx.commit().await.map_err(|_| fail("cancel task failed"))?;
    state.wake_daemon(cancelled.get("runtime_id"), None);
    if chat.is_none()
        && let Some(sink) = &state.task_cancel_events
    {
        sink.publish_task_cancelled(super::issue_write::IssueTaskCancelledEvent {
            workspace_id: ws,
            task_id: id,
            agent_id: agent,
            issue_id: cancelled.get("issue_id"),
        });
    }
    if let (Some(chat), Some(audience)) = (chat, audience) {
        let owner: Uuid = audience.get("creator_id");
        let channel: Option<String> = audience.get("zbs_source_channel_type");
        state.events.publish_chat_audience(
            runtime_chat_events::Event::task_cancelled(ws, owner, chat, id, agent),
            owner,
            audience.get("zbs_has_guest_history"),
            channel.as_deref(),
            audience.get("owner_id"),
        );
    }
    Ok(result)
}

// A manual rerun targets the named historical agent (not the now-current
// assignee); absent task_id means the current assignee. A rerun never stops an
// executing run, but replaces the pending plan in the same comment thread.
async fn rerun(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw_issue): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue_id = issue(&db, ws, &raw_issue).await?;
    let req: Value = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&body).map_err(|_| bad("invalid request body"))?
    };
    if !req.is_object() {
        return Err(bad("invalid request body"));
    }
    let source = match req.get("task_id") {
        Some(Value::String(value)) if !value.is_empty() => {
            Some(Uuid::parse_str(value).map_err(|_| bad("invalid task_id"))?)
        }
        Some(Value::String(_)) | Some(Value::Null) | None => None,
        _ => return Err(bad("invalid request body")),
    };
    drop(db);
    rerun_source(&state, ws, user, &actor, issue_id, source, false).await
}

/// Go `RerunTaskByUser`: the same replay as the issue route, addressed by the
/// run itself so a client without the issue identifier can repeat one row.
async fn rerun_task(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let id = task_id(&raw)?;
    let db = state.pool.get().await.map_err(|_| not_found())?;
    // Go looks the run up through the owning agent's workspace, so a task on
    // another tenant is indistinguishable from a missing one.
    let source = db
        .query_opt(
            "SELECT t.issue_id FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id WHERE t.id=$1 AND a.workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    let issue_id: Uuid = source
        .get::<_, Option<Uuid>>("issue_id")
        .ok_or_else(|| bad("only issue-linked runs can be rerun with this endpoint"))?;
    issue(&db, ws, &issue_id.to_string()).await?;
    // A zero-length body and `{}` keep the legacy behaviour; Go tolerates an
    // EOF here, so a whitespace-only body is not a malformed request either.
    let fresh = if body.iter().all(u8::is_ascii_whitespace) {
        false
    } else {
        #[derive(Default, serde::Deserialize)]
        struct RerunBody {
            #[serde(default)]
            fresh: bool,
        }
        let request: RerunBody =
            serde_json::from_slice(&body).map_err(|_| bad("invalid request body"))?;
        request.fresh
    };
    drop(db);
    rerun_source(&state, ws, user, &actor, issue_id, Some(id), fresh).await
}

/// Go `TaskService.RerunIssue` for both handlers. `source` is the named run
/// being repeated (and the lineage stamped on the new row); `fresh` discards
/// session/workdir continuity without losing that lineage.
async fn rerun_source(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
    issue_id: Uuid,
    source: Option<Uuid>,
    fresh: bool,
) -> Result<Response, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("issue rerun failed"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("issue rerun failed"))?;
    let issue = tx.query_opt("SELECT assignee_type,assignee_id,priority,triage_state FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE", &[&issue_id,&ws]).await.map_err(|_| fail("issue rerun failed"))?.ok_or_else(|| error(StatusCode::NOT_FOUND,"issue not found"))?;
    if source.is_none() && issue.get::<_, Option<String>>("triage_state").is_some() {
        return Ok(blocked(
            "issue_in_triage",
            "the issue is in Triage and has no owner to run yet; accept it out of Triage first",
        ));
    }
    let previous = if let Some(source) = source {
        Some(
            task(&tx, ws, source)
                .await
                .map_err(|_| bad("load source task: no rows in result set"))?,
        )
    } else {
        None
    };
    if let Some(ref previous) = previous {
        if previous.get::<_, Option<Uuid>>("issue_id") != Some(issue_id) {
            return Err(bad("source task does not belong to this issue"));
        }
        if previous
            .get::<_, Option<Value>>("context")
            .is_some_and(|v| v["type"] == "triage")
        {
            return Err(bad("triage task cannot be rerun"));
        }
    }
    let (agent, leader, squad) = if let Some(ref previous) = previous {
        (
            previous.get::<_, Uuid>("agent_id"),
            previous.get::<_, bool>("is_leader_task"),
            previous.get::<_, Option<Uuid>>("squad_id"),
        )
    } else {
        let assignee: Option<Uuid> = issue.get("assignee_id");
        match (
            issue.get::<_, Option<String>>("assignee_type").as_deref(),
            assignee,
        ) {
            (Some("agent"), Some(id)) => (id, false, None),
            (Some("squad"), Some(id)) => {
                let leader = tx
                    .query_opt(
                        "SELECT leader_id FROM squad WHERE id=$1 AND workspace_id=$2",
                        &[&id, &ws],
                    )
                    .await
                    .map_err(|_| fail("issue rerun failed"))?
                    .ok_or_else(|| bad("issue is assigned to a squad but squad not found"))?;
                (leader.get("leader_id"), true, Some(id))
            }
            _ => return Err(bad("issue is not assigned to an agent or squad")),
        }
    };
    let target = tx.query_opt("SELECT owner_id,permission_mode,runtime_id,archived_at FROM agent WHERE id=$1 AND workspace_id=$2 FOR SHARE", &[&agent,&ws]).await.map_err(|_| fail("issue rerun failed"))?
        .ok_or_else(|| bad("load target agent: no rows in result set"))?;
    if !super::runtime_chat::can_invoke(
        // Deref to `tokio_postgres::Transaction`, which implements
        // `GenericClient`; the `deadpool_postgres::Transaction` wrapper does not.
        &*tx,
        ws,
        user,
        actor,
        agent,
        target.get("owner_id"),
        &target.get::<_, String>("permission_mode"),
    )
    .await?
    {
        return Ok(blocked(
            "invocation_not_allowed",
            "you don't have permission to use this target",
        ));
    }
    if target
        .get::<_, Option<DateTime<Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("agent is archived"));
    }
    let runtime: Option<Uuid> = target.get("runtime_id");
    let runtime = runtime.ok_or_else(|| bad("agent has no runtime"))?;
    let (trigger, coalesced) = if let Some(ref previous) = previous {
        let trigger: Option<Uuid> = previous.get("trigger_comment_id");
        let coalesced: Vec<Uuid> = previous.get("coalesced_comment_ids");
        let live = if let Some(trigger) = trigger {
            tx.query_opt(
                "SELECT id FROM comment WHERE id=$1 AND issue_id=$2 AND deleted_at IS NULL",
                &[&trigger, &issue_id],
            )
            .await
            .map_err(|_| fail("issue rerun failed"))?
            .is_some()
        } else {
            false
        };
        if live {
            (trigger, coalesced)
        } else {
            let surviving = tx.query("SELECT id FROM comment WHERE issue_id=$1 AND id=ANY($2::uuid[]) AND deleted_at IS NULL ORDER BY created_at DESC,id DESC", &[&issue_id,&coalesced]).await.map_err(|_| fail("issue rerun failed"))?;
            (
                surviving.first().map(|r| r.get("id")),
                surviving.iter().skip(1).map(|r| r.get("id")).collect(),
            )
        }
    } else {
        (None, Vec::new())
    };
    let priority: String = issue.get("priority");
    let priority = match priority.as_str() {
        "urgent" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    };
    // The partial unique index is per issue/agent/thread. Keep cancellation
    // and insert in one transaction, so failure cannot destroy the old plan.
    let cancelled = tx.query("UPDATE agent_task_queue SET status='cancelled',completed_at=now(),prepare_lease_expires_at=NULL, \
        cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL WHERE issue_id=$1 AND agent_id=$2 \
        AND status IN ('queued','dispatched','deferred') \
        AND comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($3::uuid) RETURNING id", &[&issue_id,&agent,&trigger])
        .await.map_err(|_| fail("issue rerun failed"))?;
    for row in &cancelled {
        settle(&tx, row.get("id")).await?;
    }
    let id = Uuid::now_v7();
    // `fresh` rides context.fresh_rerun, which is how Go's CreateAgentTask
    // records a rerun that must not resume the source session or workdir.
    let context = if fresh {
        json!({"fresh_rerun": true})
    } else {
        json!({})
    };
    let row = tx.query_one("INSERT INTO agent_task_queue (id,agent_id,runtime_id,issue_id,status,priority,trigger_comment_id,coalesced_comment_ids,force_fresh_session, \
        is_leader_task,squad_id,originator_user_id,accountable_user_id,originator_source,rerun_of_task_id,context,workflow_step) \
        SELECT $1,$2,$3,$4,'queued',$5,$6,$7,false,$8,$9,$10,$10,'direct_human',$11,$12,i.status FROM issue i WHERE i.id=$4 AND lock_task_owner_rows($2,$4,$3) RETURNING *",
        &[&id,&agent,&runtime,&issue_id,&priority,&trigger,&coalesced,&leader,&squad,&user,&source,&context])
        .await.map_err(|_| bad("create task failed"))?;
    let mut result = response(&row, ws);
    hydrate_attribution(&tx, std::slice::from_mut(&mut result)).await;
    tx.commit().await.map_err(|_| fail("issue rerun failed"))?;
    // Go's RerunIssue wakes once for the task it just enqueued; the rows it
    // replaced are not notified individually because the successor runs on the
    // same runtime.
    state.wake_daemon(Some(runtime), Some(id));
    if let Some(sink) = &state.task_cancel_events {
        for item in cancelled {
            sink.publish_task_cancelled(super::issue_write::IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id: item.get("id"),
                agent_id: agent,
                issue_id: Some(issue_id),
            });
        }
    }
    Ok((StatusCode::ACCEPTED, GoJson(result)).into_response())
}

// A failed issue-less quick-create keeps its immutable source context pending
// until the issue is created. Retrying from the branch point re-enqueues the
// same prompt as a fresh direct_human run and moves that pending row to the new
// task in one transaction, so a double click or a race with automatic retry
// cannot mint two live attempts. Only the original requester may retry: the
// prompt is private to them even if another member learns the task id.
const CREATE_MANUAL_QUICK_CREATE_RETRY_TASK: &str = include_str!("retry_source_context.sql");

const SOURCE_CONTEXT_RETRY_UNAVAILABLE: &str =
    "This context can no longer be retried. Start again from the branch point.";

fn unavailable() -> Response {
    (
        StatusCode::CONFLICT,
        GoJson(json!({
            "code": "source_context_retry_unavailable",
            "error": SOURCE_CONTEXT_RETRY_UNAVAILABLE,
        })),
    )
        .into_response()
}

async fn retry_source_context(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    actor.require_human()?;
    let id = task_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("retry source context quick create"))?;
    // Tenancy guard is the agent's workspace, not the issue's: the whole point
    // of this route is a task that never got an issue.
    let parent = db.query_opt("SELECT atq.* FROM agent_task_queue atq JOIN agent a ON a.id=atq.agent_id WHERE atq.id=$1 AND a.workspace_id=$2", &[&id,&ws]).await.map_err(|_| fail("retry source context quick create"))?;
    let Some(parent) = parent else {
        return Ok(unavailable());
    };
    if parent.get::<_, String>("status") != "failed" {
        return Ok(unavailable());
    }
    // parseQuickCreateContext: any issue/chat/autopilot link disqualifies the
    // task even if it happens to carry a quick_create blob.
    if parent.get::<_, Option<Uuid>>("issue_id").is_some()
        || parent.get::<_, Option<Uuid>>("chat_session_id").is_some()
        || parent.get::<_, Option<Uuid>>("autopilot_run_id").is_some()
    {
        return Ok(unavailable());
    }
    let context = parent
        .get::<_, Option<Value>>("context")
        .and_then(|value| quick_create_context(&value));
    let Some(context) = context else {
        return Ok(unavailable());
    };
    if context.kind != "quick_create" || context.source_context.is_empty() {
        return Ok(unavailable());
    }
    // Go compares against the requester string as stored; a malformed user id
    // therefore mismatches rather than erroring.
    if context.requester != user.to_string() {
        return Ok(unavailable());
    }
    let Ok(context_id) = Uuid::parse_str(&context.source_context) else {
        return Ok(unavailable());
    };
    // GetAgent is not workspace-scoped in Go (the task join already proved
    // tenancy), so neither is this read.
    let agent = db
        .query_opt(
            "SELECT * FROM agent WHERE id=$1",
            &[&parent.get::<_, Uuid>("agent_id")],
        )
        .await
        .map_err(|_| fail("retry source context quick create"))?;
    let Some(agent) = agent else {
        return Ok(unavailable());
    };
    let runtime: Option<Uuid> = agent.get("runtime_id");
    if agent
        .get::<_, Option<DateTime<Utc>>>("archived_at")
        .is_some()
        || runtime.is_none()
    {
        return Ok(unavailable());
    }
    let agent_id: Uuid = agent.get("id");
    // agent_invoke speaks tokio_postgres; the pooled object only derefs to it.
    if !agent_invoke::can_invoke(
        &**db,
        ws,
        InvokeTarget {
            id: agent_id,
            owner_id: agent.get("owner_id"),
            permission_mode: &agent.get::<_, String>("permission_mode"),
        },
        Invoker {
            actor_type: "member",
            actor_id: user,
            originator: Some(user),
        },
    )
    .await
    .map_err(|_| fail("retry source context quick create"))?
    {
        return Ok(blocked(
            "invocation_not_allowed",
            "you don't have permission to use this target",
        ));
    }
    // Capacity is an admission preflight, so it runs outside the transaction
    // and never holds product locks across the entitlement call.
    let policy = policy(&state, ws).await?;
    if policy.limit.is_some_and(|limit| limit <= 0) {
        // Go's resolver downgrades a non-positive enforce limit to "off"; the
        // Option<i64> wire here encodes "off" as None, so a non-positive value
        // means the gate itself is misconfigured and must fail closed.
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "invalid issue count policy",
        ));
    }
    if let Some(limit) = policy.limit {
        let used: i64 = db
            .query_one(
                "SELECT count(*) FROM (SELECT 1 FROM issue WHERE workspace_id=$1 LIMIT $2) limited",
                &[&ws, &limit],
            )
            .await
            .map_err(|_| fail("retry source context quick create"))?
            .get(0);
        if used >= limit {
            return Ok(quota_response(policy));
        }
    }
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("retry source context quick create"))?;
    // The pending row is the serialization point: locking it here is what makes
    // "one live attempt" true, and the identity check rejects a context that
    // was attached or moved by an automatic retry in the meantime.
    let locked = tx.query_opt("SELECT * FROM issue_source_context WHERE workspace_id=$1 AND origin_task_id=$2 AND state='pending' FOR UPDATE", &[&ws,&id]).await.map_err(|_| fail("retry source context quick create"))?;
    if locked.as_ref().map(|row| row.get::<_, Uuid>("id")) != Some(context_id) {
        return Ok(unavailable());
    }
    let new_id = Uuid::now_v7();
    // No Composio overlay exists in Rust, which is exactly the NULL Go writes
    // when the integration is absent or its feature flag is off.
    let overlay: Option<Value> = None;
    let child = tx
        .query_opt(
            CREATE_MANUAL_QUICK_CREATE_RETRY_TASK,
            &[&user, &new_id, &overlay, &overlay, &id],
        )
        .await
        .map_err(|_| fail("retry source context quick create"))?
        .ok_or_else(|| fail("retry source context quick create"))?;
    let moved = tx
        .query_opt(
            "UPDATE issue_source_context SET origin_task_id=$1 WHERE workspace_id=$2 AND id=$3 AND state='pending' AND origin_task_id=$4 RETURNING id",
            &[&child.get::<_, Uuid>("id"), &ws, &context_id, &id],
        )
        .await
        .map_err(|_| fail("retry source context quick create"))?;
    if moved.is_none() {
        return Ok(unavailable());
    }
    let mut result = response(&child, ws);
    hydrate_attribution(&tx, std::slice::from_mut(&mut result)).await;
    tx.commit()
        .await
        .map_err(|_| fail("retry source context quick create"))?;
    let mut payload = json!({
        "task_id": child.get::<_, Uuid>("id"),
        "agent_id": child.get::<_, Uuid>("agent_id"),
        "issue_id": child.get::<_, Option<Uuid>>("issue_id").map(|v| v.to_string()).unwrap_or_default(),
        "status": child.get::<_, String>("status"),
    });
    if let Some(session) = child.get::<_, Option<Uuid>>("chat_session_id") {
        payload["chat_session_id"] = json!(session);
    }
    state
        .events
        .publish(system_event("task:queued", ws, None, None, payload));
    state.wake_daemon(runtime, Some(new_id));
    Ok((StatusCode::ACCEPTED, GoJson(result)).into_response())
}

// Mirrors CheckIssueCreateCapacity's resolution: no runtime is a hard failure
// (no fallback policy is safe), an enforce action with a non-positive limit is
// downgraded to off by Go and therefore must not 402 here either.
async fn policy(state: &ApiState, ws: Uuid) -> Result<IssueCountPolicy, ApiError> {
    let runtime = state.issue_create_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue count policy unavailable",
        )
    })?;
    runtime.count_policy(ws).await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue count policy unavailable",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_query_values_use_first_like_go() {
        assert_eq!(
            params(Some("active=true&active=false"))
                .get("active")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            params(Some("scope=family&scope=issue"))
                .get("scope")
                .map(String::as_str),
            Some("family")
        );
    }
    #[test]
    fn invalid_task_ids_are_bad_requests() {
        assert_eq!(
            task_id("not-a-uuid").unwrap_err().0,
            StatusCode::BAD_REQUEST
        );
    }
    #[test]
    fn scope_and_active_validation_do_not_silently_downgrade() {
        assert_eq!(
            task_run_filters(&params(Some("scope=family&active=false"))).unwrap(),
            (true, false)
        );
        assert_eq!(
            task_run_filters(&params(Some("scope=issue&active=true"))).unwrap(),
            (false, true)
        );
        assert_eq!(
            task_run_filters(&params(Some("scope=family&active=tru")))
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            task_run_filters(&params(Some("scope=unknown")))
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    #[tokio::test]
    #[ignore = "requires child-owned migrated multica_zbsi67 on isolated loopback port"]
    async fn db_read_routes_keep_tenant_and_response_shape() {
        use deadpool_postgres::{Manager, Pool};
        use http_body_util::BodyExt;
        use std::{str::FromStr, sync::Arc};
        use tokio_postgres::{Config, NoTls};
        let config =
            Config::from_str(&std::env::var("RUST_API_TEST_DATABASE_URL").unwrap()).unwrap();
        assert_eq!(config.get_dbname(), Some("multica_zbsi67"));
        assert!(config.get_hosts().iter().all(
            |host| matches!(host, tokio_postgres::config::Host::Tcp(host) if host == "127.0.0.1")
        ));
        assert!(config.get_ports().iter().all(|port| *port != 5432));
        let pool = Arc::new(
            Pool::builder(Manager::new(config, NoTls))
                .max_size(4)
                .build()
                .unwrap(),
        );
        let db = pool.get().await.unwrap();
        let suffix = Uuid::new_v4().simple().to_string();
        let user: Uuid = db
            .query_one(
                "INSERT INTO \"user\"(name,email) VALUES ('P0 direct test',$1) RETURNING id",
                &[&format!("p0-direct-{suffix}@invalid.example")],
            )
            .await
            .unwrap()
            .get(0);
        let ws: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 direct',$1,'P0D') RETURNING id", &[&format!("p0-direct-{suffix}")]).await.unwrap().get(0);
        let other: Uuid = db.query_one("INSERT INTO workspace(name,slug,issue_prefix) VALUES ('P0 other',$1,'P0O') RETURNING id", &[&format!("p0-other-{suffix}")]).await.unwrap().get(0);
        db.execute(
            "INSERT INTO member(workspace_id,user_id,role) VALUES ($1,$2,'owner')",
            &[&ws, &user],
        )
        .await
        .unwrap();
        let issue_id: Uuid = db.query_one("INSERT INTO issue(workspace_id,title,creator_type,creator_id) VALUES ($1,'P0 task issue','member',$2) RETURNING id", &[&ws,&user]).await.unwrap().get(0);
        let agent: Uuid = db.query_one("INSERT INTO agent(name,workspace_id,owner_id,kind,permission_mode,runtime_mode) VALUES ('P0 direct agent',$1,$2,'user','private','cloud') RETURNING id", &[&ws,&user]).await.unwrap().get(0);
        let runtime: Uuid = db.query_one("INSERT INTO agent_runtime(workspace_id,name,runtime_mode,provider,status) VALUES ($1,'P0 direct runtime','cloud','synthetic','online') RETURNING id", &[&ws]).await.unwrap().get(0);
        let running: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status) VALUES ($1,$2,$3,'running') RETURNING id", &[&agent,&runtime,&issue_id]).await.unwrap().get(0);
        let completed: Uuid = db.query_one("INSERT INTO agent_task_queue(agent_id,runtime_id,issue_id,status,started_at,completed_at) VALUES ($1,$2,$3,'completed',now(),now()) RETURNING id", &[&agent,&runtime,&issue_id]).await.unwrap().get(0);
        let state = ApiState::new(pool.clone(), b"synthetic-p0-test".to_vec());
        let GoJson(active) = active_task(
            State(state.clone()),
            Extension(WorkspaceId(ws)),
            Path(issue_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(active["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(active["tasks"][0]["id"], running.to_string());
        let history = task_runs(
            State(state.clone()),
            Extension(WorkspaceId(ws)),
            Extension(Identity(user)),
            Extension(Actor::Human),
            Path(issue_id.to_string()),
            RawQuery(None),
        )
        .await
        .unwrap();
        let bytes = history.into_body().collect().await.unwrap().to_bytes();
        let runs: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            runs.as_array()
                .unwrap()
                .iter()
                .any(|r| r["id"] == completed.to_string())
        );
        let err = active_task(
            State(state),
            Extension(WorkspaceId(other)),
            Path(issue_id.to_string()),
        )
        .await
        .err()
        .expect("foreign issue must be hidden");
        assert_eq!(err.0, StatusCode::NOT_FOUND);
    }

    #[test]
    fn hide_only_unused_escalation_fallbacks() {
        let id = Uuid::new_v4();
        assert!(!visible_history("deferred", Some(id), None));
        assert!(!visible_history("cancelled", Some(id), None));
        assert!(visible_history("cancelled", Some(id), Some(Utc::now())));
        assert!(visible_history("cancelled", None, None));
    }
}
