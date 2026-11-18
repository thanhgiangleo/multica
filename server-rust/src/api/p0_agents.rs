//! Agent archive, bulk cancellation and private task-history endpoints.
//! Integrator: declare `mod p0_agents;` in api.rs and merge `p0_agents::routes()`
//! in catalog.rs. No provider or daemon call is made by these handlers.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, runtime_chat_events,
    runtime_chat_task,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    routing::{get, post},
};
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::{GenericClient, Transaction};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agents/{id}/archive", post(archive))
        .route("/api/agents/{id}/cancel-tasks", post(cancel_tasks))
        .route("/api/agents/{id}/tasks", get(tasks))
}

fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn fail(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn agent_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad("invalid agent id"))
}
fn deny() -> ApiError {
    error(
        StatusCode::FORBIDDEN,
        "you do not have access to this agent",
    )
}

// Never query by ID alone: a caller's selected workspace is an authorization
// boundary. An absent agent and another workspace's agent look identical.
async fn agent(db: &impl GenericClient, ws: Uuid, id: Uuid) -> Result<Row, ApiError> {
    db.query_opt(
        "SELECT id,owner_id,archived_at,system_key,permission_mode FROM agent \
         WHERE id=$1 AND workspace_id=$2 AND kind='user'",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail("failed to load agent"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))
}

async fn membership(db: &impl GenericClient, ws: Uuid, user: Uuid) -> Result<String, ApiError> {
    db.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .map_err(|_| fail("failed to check agent permissions"))?
    .map(|r| r.get("role"))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))
}
fn can_manage(role: &str, owner: Option<Uuid>, user: Uuid) -> Result<(), ApiError> {
    if !matches!(role, "owner" | "admin" | "member") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if matches!(role, "owner" | "admin") || owner == Some(user) {
        Ok(())
    } else {
        Err(error(
            StatusCode::FORBIDDEN,
            "only the agent owner can manage this agent",
        ))
    }
}

// Lock order agrees with existing agent writes: workspace, membership, agent,
// then task rows. Prevent a membership change or workspace deletion mid-write.
async fn writable(tx: &Transaction<'_>, ws: Uuid, user: Uuid, id: Uuid) -> Result<Row, ApiError> {
    tx.query_opt("SELECT id FROM workspace WHERE id=$1 FOR SHARE", &[&ws])
        .await
        .map_err(|_| fail("failed to lock workspace"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail("failed to check agent permissions"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let row = tx
        .query_opt(
            "SELECT id,owner_id,archived_at,system_key,permission_mode FROM agent \
             WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail("failed to load agent"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    can_manage(&member.get::<_, String>("role"), row.get("owner_id"), user)?;
    Ok(row)
}

#[derive(Clone)]
struct Cancelled {
    id: Uuid,
    runtime: Option<Uuid>,
    issue: Option<Uuid>,
    chat: Option<Uuid>,
    creator: Option<Uuid>,
    guest_history: bool,
    channel: Option<String>,
    owner: Option<Uuid>,
}
const CANCEL_SQL: &str = "UPDATE agent_task_queue SET status='cancelled',completed_at=now(), \
prepare_lease_expires_at=NULL,cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL \
WHERE agent_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') \
RETURNING id,runtime_id,issue_id,chat_session_id";

// The state transition and all terminal settlements must commit together:
// a follow-up settlement cannot reliably repair a cancelled delegated run.
async fn cancel_in_tx(tx: &Transaction<'_>, id: Uuid) -> Result<Vec<Cancelled>, ApiError> {
    let rows = tx
        .query(CANCEL_SQL, &[&id])
        .await
        .map_err(|_| fail("failed to cancel tasks"))?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    tx.execute(
        "UPDATE task_supplement s SET status='failed',failure_reason='turn_ended',updated_at=now() \
         FROM agent_task_queue t WHERE s.task_id=t.id AND t.id=ANY($1::uuid[]) \
         AND t.status IN ('completed','failed','cancelled') AND s.status IN ('pending','delivering')",
        &[&ids],
    ).await.map_err(|_| fail("failed to settle terminal task state"))?;
    tx.execute(
        "UPDATE comment recovery SET recovery_settled_at=now() FROM agent_task_queue t \
         WHERE t.id=ANY($1::uuid[]) AND t.status IN ('completed','failed','cancelled') \
         AND recovery.id=ANY(t.delivered_comment_ids) AND recovery.author_type='system' \
         AND recovery.type='progress_update' AND recovery.source_task_id IS NOT NULL \
         AND recovery.recovery_settled_at IS NULL",
        &[&ids],
    )
    .await
    .map_err(|_| fail("failed to settle terminal task state"))?;
    tx.execute(
        "DELETE FROM task_token WHERE task_id=ANY($1::uuid[])",
        &[&ids],
    )
    .await
    .map_err(|_| fail("failed to revoke task tokens"))?;
    // Capture audience *before* committing: chat rows may be archived or deleted
    // concurrently and a post-commit lookup can lose the owner/channel scope.
    let sessions: Vec<Uuid> = rows
        .iter()
        .filter_map(|r| r.get("chat_session_id"))
        .collect();
    let audience = tx.query(
        "SELECT cs.id,cs.creator_id,cs.zbs_has_guest_history,cs.zbs_source_channel_type,a.owner_id \
         FROM chat_session cs JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id \
         WHERE cs.id=ANY($1::uuid[]) AND cs.agent_id=$2",
        &[&sessions, &id],
    ).await.map_err(|_| fail("failed to load chat audience"))?;
    let by_session: HashMap<Uuid, &Row> = audience.iter().map(|r| (r.get("id"), r)).collect();
    Ok(rows
        .iter()
        .map(|r| {
            let chat: Option<Uuid> = r.get("chat_session_id");
            let session = chat.and_then(|id| by_session.get(&id).copied());
            Cancelled {
                id: r.get("id"),
                runtime: r.get("runtime_id"),
                issue: r.get("issue_id"),
                chat,
                creator: session.map(|r| r.get("creator_id")),
                guest_history: session.is_some_and(|r| r.get("zbs_has_guest_history")),
                channel: session.and_then(|r| r.get("zbs_source_channel_type")),
                owner: session.and_then(|r| r.get("owner_id")),
            }
        })
        .collect())
}

fn notify(state: &ApiState, ws: Uuid, agent: Uuid, cancelled: &[Cancelled], only_chat: bool) {
    let mut notified = std::collections::HashSet::new();
    for item in cancelled {
        if let Some(runtime) = item.runtime.filter(|id| notified.insert(*id)) {
            state.wake_daemon(Some(runtime), None);
        }
        if !only_chat
            && item.chat.is_none()
            && let Some(sink) = &state.task_cancel_events
        {
            sink.publish_task_cancelled(super::issue_write::IssueTaskCancelledEvent {
                workspace_id: ws,
                task_id: item.id,
                agent_id: agent,
                issue_id: item.issue,
            });
        }
        if let (Some(chat), Some(creator)) = (item.chat, item.creator) {
            state.events.publish_chat_audience(
                runtime_chat_events::Event::task_cancelled(ws, creator, chat, item.id, agent),
                creator,
                item.guest_history,
                item.channel.as_deref(),
                item.owner,
            );
        }
    }
}

// Project only the AgentResponse fields; custom_env and future secret columns
// must never be serialized by copying the whole database row.
async fn archive_response(
    tx: &Transaction<'_>,
    ws: Uuid,
    id: Uuid,
) -> Result<(Value, bool), ApiError> {
    let data: Value = tx
        .query_one(
            "SELECT to_jsonb(a) AS data FROM agent a WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail("failed to load agent response"))?
        .get("data");
    let targets = tx.query(
        "SELECT target_type,target_id FROM agent_invocation_target WHERE agent_id=$1 ORDER BY created_at,id", &[&id],
    ).await.map_err(|_| fail("failed to load agent invocation targets"))?;
    let skills = tx.query(
        "SELECT s.id,s.name,s.description,b.enabled FROM skill s JOIN agent_skill b ON b.skill_id=s.id WHERE b.agent_id=$1 ORDER BY s.name ASC",
        &[&id],
    ).await.map_err(|_| fail("failed to load agent skills"))?;
    let mut result = serde_json::Map::new();
    for field in [
        "id",
        "workspace_id",
        "name",
        "description",
        "instructions",
        "conversation_starters",
        "avatar_url",
        "runtime_mode",
        "runtime_config",
        "custom_args",
        "visibility",
        "permission_mode",
        "status",
        "max_concurrent_tasks",
        "owner_id",
        "disabled_runtime_skills",
        "created_at",
        "updated_at",
        "archived_at",
        "archived_by",
    ] {
        result.insert(
            field.to_string(),
            data.get(field).cloned().unwrap_or(Value::Null),
        );
    }
    for field in ["model", "thinking_level", "service_tier", "runtime_id"] {
        result.insert(
            field.to_string(),
            data.get(field)
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(json!("")),
        );
    }
    for field in [
        "custom_args",
        "conversation_starters",
        "disabled_runtime_skills",
    ] {
        if result[field].is_null() {
            result.insert(field.to_string(), json!([]));
        }
    }
    if result["runtime_config"].is_null() {
        result.insert("runtime_config".into(), json!({}));
    }
    for field in ["created_at", "updated_at", "archived_at"] {
        if let Some(raw) = data[field].as_str()
            && let Ok(time) = DateTime::parse_from_rfc3339(raw)
        {
            result.insert(
                field.into(),
                json!(super::timestamp(time.with_timezone(&Utc))),
            );
        }
    }
    let count = data["custom_env"].as_object().map_or(0, |env| env.len());
    result.insert("runtime_bound".into(), json!(!data["runtime_id"].is_null()));
    result.insert("has_custom_env".into(), json!(count > 0));
    result.insert("custom_env_key_count".into(), json!(count));
    result.insert(
        "mcp_config".into(),
        data.get("mcp_config").cloned().unwrap_or(Value::Null),
    );
    result.insert("mcp_config_redacted".into(), json!(false));
    if let Some(allowlist) = data
        .get("composio_toolkit_allowlist")
        .filter(|v| !v.is_null() && v.as_array().is_some_and(|items| !items.is_empty()))
    {
        result.insert("composio_toolkit_allowlist".into(), allowlist.clone());
    }
    if let Some(key) = data
        .get("system_key")
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
    {
        result.insert("system_key".into(), json!(key));
    }
    let targets: Vec<Value> = targets.iter().map(|r|json!({
        "target_type":r.get::<_,String>("target_type"),"target_id":r.get::<_,Option<Uuid>>("target_id")
    })).collect();
    result.insert("invocation_targets".into(), json!(targets));
    result.insert(
        "skills".into(),
        json!(skills.iter().map(|r|json!({
        "id":r.get::<_,Uuid>("id"),"name":r.get::<_,String>("name"),
        "description":r.get::<_,String>("description"),"enabled":r.get::<_,bool>("enabled")
    })).collect::<Vec<_>>()),
    );
    if let Some(token) = result
        .get_mut("runtime_config")
        .and_then(|v| v.get_mut("gateway"))
        .and_then(|v| v.get_mut("token"))
        && token.as_str().is_some_and(|s| !s.is_empty())
    {
        *token = json!("***");
    }
    Ok((
        Value::Object(result),
        !data["composio_toolkit_allowlist"].is_null(),
    ))
}

fn broadcast_response(mut response: Value, allowlist_configured: bool) -> Value {
    if !response["mcp_config"].is_null() {
        response["mcp_config"] = Value::Null;
        response["mcp_config_redacted"] = json!(true);
    }
    if allowlist_configured {
        response
            .as_object_mut()
            .unwrap()
            .remove("composio_toolkit_allowlist");
        response["composio_toolkit_allowlist_redacted"] = json!(true);
    }
    response
}

async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let bus = state.agent_event_bus().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "squad event bus unavailable",
        )
    })?;
    let id = agent_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to archive agent"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to start transaction"))?;
    let row = writable(&tx, ws, user, id).await?;
    if row.get::<_, Option<DateTime<Utc>>>("archived_at").is_some() {
        return Err(error(StatusCode::CONFLICT, "agent is already archived"));
    }
    if row
        .get::<_, Option<String>>("system_key")
        .is_some_and(|s| !s.is_empty())
    {
        return Err(bad(
            "this agent is built into Multica and cannot be archived",
        ));
    }
    tx.execute(
        "UPDATE agent SET archived_at=now(),archived_by=$2,updated_at=now() WHERE id=$1",
        &[&id, &user],
    )
    .await
    .map_err(|_| fail("failed to archive agent"))?;
    let cancelled = cancel_in_tx(&tx, id).await?;
    let (response, allowlist_configured) = archive_response(&tx, ws, id).await?;
    tx.commit()
        .await
        .map_err(|_| fail("failed to commit agent archive"))?;
    bus.publish(super::agents_squads_sprint::SquadEvent {
        workspace_id: ws,
        actor_id: user,
        event_type: "agent:archived",
        payload: json!({"agent":broadcast_response(response.clone(), allowlist_configured)}),
    });
    notify(&state, ws, id, &cancelled, true);
    Ok(GoJson(response))
}

async fn cancel_tasks(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    // A missing event sink must not silently leave issue cards showing active
    // work after the DB transition.
    if state.task_cancel_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "task event bus unavailable",
        ));
    }
    let id = agent_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to cancel tasks"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to start transaction"))?;
    writable(&tx, ws, user, id).await?;
    let cancelled = cancel_in_tx(&tx, id).await?;
    let status_agent = super::runtime_chat_daemon::reconcile_agent(&*tx, id)
        .await?
        .map(|row| super::runtime_chat_daemon::agent_snapshot(&row));
    tx.commit()
        .await
        .map_err(|_| fail("failed to commit task cancellation"))?;
    notify(&state, ws, id, &cancelled, false);
    if let Some(agent) = status_agent {
        state
            .events
            .publish(super::runtime_chat_daemon::system_event(
                "agent:status",
                ws,
                None,
                None,
                json!({"agent":agent}),
            ));
    }
    Ok(GoJson(json!({"cancelled":cancelled.len()})))
}

const HISTORY: &str = "SELECT t.* FROM agent_task_queue t WHERE t.agent_id=$1 \
AND (t.chat_session_id IS NULL OR EXISTS (SELECT 1 FROM chat_session c \
JOIN agent a ON a.id=c.agent_id AND a.workspace_id=c.workspace_id \
WHERE c.id=t.chat_session_id AND c.agent_id=t.agent_id AND \
((c.creator_id=$2 AND (NOT c.zbs_has_guest_history OR a.owner_id=$2)) OR \
(c.zbs_source_channel_type IN ('telegram','zalo') AND a.owner_id=$2)))) \
AND NOT (t.escalation_for_task_id IS NOT NULL AND t.started_at IS NULL \
AND t.status IN ('deferred','cancelled')) AND (t.created_at,t.id)<(COALESCE($3::timestamptz,'infinity'::timestamptz),$4::uuid) \
ORDER BY t.created_at DESC,t.id DESC LIMIT $5";

fn params(raw: Option<&str>) -> Vec<(String, String)> {
    form_urlencoded::parse(raw.unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}
fn query_value<'a>(items: &'a [(String, String)], key: &str) -> &'a str {
    items
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}
#[derive(Debug)]
struct TaskPage {
    limit: i64,
    usage: bool,
    before: Option<(DateTime<Utc>, Uuid)>,
}
fn options(raw: Option<&str>) -> Result<TaskPage, ApiError> {
    let values = params(raw);
    let usage = match query_value(&values, "include_usage").trim() {
        "" | "false" => false,
        "true" => true,
        _ => return Err(bad("include_usage must be true or false")),
    };
    let limit = match query_value(&values, "limit") {
        "" => 200,
        raw => raw
            .parse::<u64>()
            .ok()
            .filter(|v| *v > 0)
            .map(|v| v.min(200) as i64)
            .ok_or_else(|| bad("limit must be a positive integer"))?,
    };
    let before = match query_value(&values, "before") {
        "" => None,
        raw => Some(cursor_parse(raw)?),
    };
    Ok(TaskPage {
        limit,
        usage,
        before,
    })
}
fn cursor_parse(raw: &str) -> Result<(DateTime<Utc>, Uuid), ApiError> {
    let (date, id) = raw
        .split_once('|')
        .ok_or_else(|| bad("invalid before cursor"))?;
    let date = DateTime::parse_from_rfc3339(date).map_err(|_| bad("invalid before cursor"))?;
    let id = Uuid::parse_str(id).map_err(|_| bad("invalid before cursor"))?;
    Ok((date.with_timezone(&Utc), id))
}
fn cursor_format(row: &Row) -> String {
    let date: DateTime<Utc> = row.get("created_at");
    let id: Uuid = row.get("id");
    format!("{}|{}", cursor_time(date), id)
}

// Go's RFC3339Nano omits trailing fractional zeroes; even a whole-second
// timestamp must end in Z, not an empty or dangling fractional component.
fn cursor_time(date: DateTime<Utc>) -> String {
    let full = date.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let without_z = full.strip_suffix('Z').expect("UTC timestamp ends in Z");
    let (whole, fractional) = without_z.split_once('.').expect("nanosecond timestamp");
    let fractional = fractional.trim_end_matches('0');
    if fractional.is_empty() {
        format!("{whole}Z")
    } else {
        format!("{whole}.{fractional}Z")
    }
}

async fn can_view(
    db: &impl GenericClient,
    row: &Row,
    user: Uuid,
    role: &str,
    actor: &Actor,
) -> Result<bool, ApiError> {
    if matches!(actor, Actor::Task(_))
        || matches!(role, "owner" | "admin")
        || row.get::<_, Option<Uuid>>("owner_id") == Some(user)
    {
        return Ok(true);
    }
    if row.get::<_, String>("permission_mode") != "public_to" {
        return Ok(false);
    }
    Ok(db
        .query_opt(
            "SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND \
       (target_type='workspace' OR (target_type='member' AND target_id=$2)) LIMIT 1",
            &[&row.get::<_, Uuid>("id"), &user],
        )
        .await
        .map_err(|_| fail("failed to resolve agent access"))?
        .is_some())
}

async fn hydrate_usage(
    db: &impl GenericClient,
    agent: Uuid,
    tasks: &mut [Value],
) -> Result<(), ApiError> {
    let ids: Vec<Uuid> = tasks
        .iter()
        .filter_map(|t| t["id"].as_str().and_then(|s| Uuid::parse_str(s).ok()))
        .collect();
    if ids.is_empty() {
        return Ok(());
    }
    let rows = db
        .query(
            "SELECT tu.task_id,tu.provider,tu.model,tu.input_tokens,tu.output_tokens, \
     tu.cache_read_tokens,tu.cache_write_tokens,tu.cost_usd_ticks FROM task_usage tu \
     JOIN agent_task_queue t ON t.id=tu.task_id WHERE t.agent_id=$1 AND tu.task_id=ANY($2::uuid[]) \
     ORDER BY tu.task_id,tu.model",
            &[&agent, &ids],
        )
        .await
        .map_err(|_| fail("failed to list agent task usage"))?;
    let mut usage: HashMap<Uuid, Vec<Value>> = HashMap::new();
    for row in rows {
        let mut item = json!({"model":row.get::<_,String>("model"),
            "input_tokens":row.get::<_,i64>("input_tokens"),
            "output_tokens":row.get::<_,i64>("output_tokens"),
            "cache_read_tokens":row.get::<_,i64>("cache_read_tokens"),
            "cache_write_tokens":row.get::<_,i64>("cache_write_tokens")});
        let provider: String = row.get("provider");
        if !provider.is_empty() {
            item["provider"] = json!(provider);
        }
        if let Some(cost) = row.get::<_, Option<i64>>("cost_usd_ticks") {
            item["cost_usd_ticks"] = json!(cost);
        }
        usage.entry(row.get("task_id")).or_default().push(item);
    }
    for task in tasks {
        let id = Uuid::parse_str(task["id"].as_str().unwrap_or("")).expect("mapped task has uuid");
        if let Some(items) = usage.remove(&id) {
            task["usage"] = json!(items);
        }
    }
    Ok(())
}

async fn hydrate_attribution(db: &impl GenericClient, tasks: &mut [Value]) {
    let ids: HashSet<Uuid> = tasks
        .iter()
        .flat_map(|t| {
            ["initiator", "originator"].into_iter().filter_map(|field| {
                t["attribution"][field]["id"]
                    .as_str()
                    .and_then(|s| Uuid::parse_str(s).ok())
            })
        })
        .collect();
    if ids.is_empty() {
        return;
    }
    let ids: Vec<Uuid> = ids.into_iter().collect();
    let Ok(rows) = db
        .query(
            "SELECT id,name,email FROM \"user\" WHERE id=ANY($1::uuid[])",
            &[&ids],
        )
        .await
    else {
        return;
    };
    let displays: HashMap<Uuid, Value> = rows
        .iter()
        .map(|row| {
            let id: Uuid = row.get("id");
            let email: String = row.get("email");
            let mut v = json!({"name":row.get::<_,String>("name")});
            if !email
                .trim()
                .to_ascii_lowercase()
                .ends_with("@channel.invalid")
            {
                v["email"] = json!(email);
            }
            (id, v)
        })
        .collect();
    for task in tasks {
        for field in ["initiator", "originator"] {
            if let Some(id) = task["attribution"][field]["id"]
                .as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                && let Some(display) = displays.get(&id)
            {
                for (key, value) in display.as_object().expect("display object") {
                    task["attribution"][field][key] = value.clone();
                }
            }
        }
    }
}

struct SupplementAccess {
    ws: Uuid,
    user: Uuid,
    owner: Option<Uuid>,
    agent: Uuid,
    human: bool,
    permission: String,
}
async fn hydrate_supplements(
    db: &impl GenericClient,
    rows: &[Row],
    tasks: &mut [Value],
    access: &SupplementAccess,
) {
    let ids: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.get::<_, Option<Uuid>>("issue_id").is_some())
        .map(|r| r.get("id"))
        .collect();
    if ids.is_empty() {
        return;
    }
    let Ok(metadata)=db.query("SELECT cap.task_id,cap.capability, \
      COALESCE(array_agg(s.comment_id ORDER BY s.created_at,s.comment_id) FILTER (WHERE s.comment_id IS NOT NULL),'{}'::uuid[])::uuid[] AS comment_ids \
      FROM task_supplement_capability cap LEFT JOIN task_supplement s ON s.task_id=cap.task_id \
      WHERE cap.workspace_id=$1 AND cap.task_id=ANY($2::uuid[]) GROUP BY cap.task_id,cap.capability", &[&access.ws,&ids]).await else {return;};
    let indexed: HashMap<Uuid, &Row> = metadata.iter().map(|r| (r.get("task_id"), r)).collect();
    let can_steer = if !access.human {
        false
    } else if access.owner == Some(access.user) {
        true
    } else if access.permission != "public_to" {
        false
    } else {
        db.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND \
                ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1",
                &[&access.agent,&access.ws,&access.user]).await.ok().flatten().is_some()
    };
    for (row, task) in rows.iter().zip(tasks.iter_mut()) {
        if let Some(meta) = indexed.get(&row.get::<_, Uuid>("id")) {
            let cap: String = meta.get("capability");
            if !cap.is_empty() {
                task["supplement_capability"] = json!(cap);
            }
            let comments: Vec<Uuid> = meta.get("comment_ids");
            if !comments.is_empty() {
                task["supplement_comment_ids"] = json!(comments);
            }
            // A running task is only steerable by its agent owner or a member
            // with invocation permission; machine credentials do not inherit
            // a human's steering right.
            if row.get::<_, String>("status") == "running"
                && cap == "task-supplement-v1"
                && can_steer
            {
                task["can_supplement"] = json!(true);
            }
        }
    }
}

async fn tasks(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<(HeaderMap, GoJson<Vec<Value>>), ApiError> {
    let id = agent_id(&raw)?;
    if matches!(actor, Actor::Daemon | Actor::Cloud) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to list agent tasks"))?;
    let tx = db
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| fail("failed to start agent read"))?;
    let row = agent(&tx, ws, id).await?;
    // A task token is already bound to its workspace and agent by auth, but
    // channel guest tasks can run under a shadow user without a member row.
    // Go lets agent principals inspect agent history; do not require that
    // shadow principal to be a human workspace member.
    let role = if matches!(actor, Actor::Task(_)) {
        String::new()
    } else {
        membership(&tx, ws, user).await?
    };
    if !can_view(&tx, &row, user, &role, &actor).await? {
        return Err(deny());
    }
    let page = options(query.as_deref())?;
    let upper = page.before.map(|v| v.0);
    let upper_id = page.before.map(|v| v.1).unwrap_or(id);
    let limit = page.limit;
    let rows = tx
        .query(HISTORY, &[&id, &user, &upper, &upper_id, &(limit + 1)])
        .await
        .map_err(|_| fail("failed to list agent tasks"))?;
    let next = if rows.len() > limit as usize {
        Some(cursor_format(&rows[limit as usize - 1]))
    } else {
        None
    };
    let visible = &rows[..rows.len().min(limit as usize)];
    let machine = matches!(actor, Actor::Task(_));
    let visible: Vec<Row> = visible
        .iter()
        .filter(|r| !machine || r.get::<_, Option<Uuid>>("chat_session_id").is_none())
        .cloned()
        .collect();
    let mut result: Vec<Value> = visible
        .iter()
        .map(|r| runtime_chat_task::response(r, ws))
        .collect();
    hydrate_supplements(
        &tx,
        &visible,
        &mut result,
        &SupplementAccess {
            ws,
            user,
            owner: row.get("owner_id"),
            agent: id,
            human: matches!(actor, Actor::Human),
            permission: row.get("permission_mode"),
        },
    )
    .await;
    hydrate_attribution(&tx, &mut result).await;
    if page.usage {
        hydrate_usage(&tx, id, &mut result).await?;
    }
    tx.commit()
        .await
        .map_err(|_| fail("failed to finish agent read"))?;
    let mut headers = HeaderMap::new();
    if let Some(next) = next {
        headers.insert(
            "X-Agent-Tasks-Next-Cursor",
            HeaderValue::from_str(&next).map_err(|_| fail("failed to format next cursor"))?,
        );
    }
    Ok((headers, GoJson(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_contract() {
        assert_eq!(options(None).unwrap().limit, 200);
        assert_eq!(
            options(Some("limit=999&include_usage=true")).unwrap().limit,
            200
        );
        assert!(options(Some("limit=0")).is_err());
        assert!(options(Some("limit=-1")).is_err());
        assert!(options(Some("limit=x")).is_err());
        assert!(options(Some("include_usage=yes")).is_err());
        assert!(options(Some("before=bad")).is_err());
        let id = Uuid::new_v4();
        let cursor = format!("2026-11-07T04:05:06.123456Z|{id}");
        assert_eq!(
            options(Some(&format!("before={}", cursor.replace('|', "%7C"))))
                .unwrap()
                .before
                .unwrap()
                .1,
            id
        );
    }
    #[test]
    fn cursor_matches_go_rfc3339nano() {
        let whole = DateTime::parse_from_rfc3339("2026-11-07T04:05:06Z")
            .unwrap()
            .to_utc();
        assert_eq!(cursor_time(whole), "2026-11-07T04:05:06Z");
        assert_eq!(
            cursor_time(whole + chrono::Duration::nanoseconds(123400000)),
            "2026-11-07T04:05:06.1234Z"
        );
    }
    #[test]
    fn private_access_and_management_are_distinct() {
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();
        assert!(can_manage("member", Some(owner), owner).is_ok());
        assert!(can_manage("member", Some(owner), other).is_err());
        assert!(can_manage("admin", Some(owner), other).is_ok());
        assert!(can_manage("viewer", Some(owner), owner).is_err());
        assert!(HISTORY.contains("c.creator_id=$2"));
        assert!(HISTORY.contains("c.zbs_has_guest_history"));
        assert!(HISTORY.contains("c.agent_id=t.agent_id"));
        assert!(
            HISTORY.find("AND NOT (t.escalation_for_task_id").unwrap()
                < HISTORY.find("LIMIT $5").unwrap()
        );
    }
    #[test]
    fn cancellation_restricts_active_states_and_settles_before_commit() {
        assert!(CANCEL_SQL.contains("agent_id=$1"));
        assert!(CANCEL_SQL.contains("'waiting_local_directory','deferred'"));
        assert!(!CANCEL_SQL.contains("status='completed'"));
    }
}
