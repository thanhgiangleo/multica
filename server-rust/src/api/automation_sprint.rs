//! Workspace-scoped autopilot definitions. Dispatch belongs to the task runtime.
mod automation_sprint_cron;
mod automation_sprint_deliveries;
mod automation_sprint_failure_monitor;
mod automation_sprint_ingress;
mod automation_sprint_lifecycle;
mod automation_sprint_manual;
mod automation_sprint_quota;
mod automation_sprint_schedule;
mod automation_sprint_triggers;
mod automation_sprint_worker;
pub use automation_sprint_failure_monitor::{
    DurablePauseEvents, FailureMonitor, FailureMonitorConfig, FailureMonitorPage, PauseNotice,
    PauseTransition,
};
pub(super) use automation_sprint_ingress::{IngressLimits, public_routes};
pub use automation_sprint_lifecycle::{
    DurableLifecycleEvents, QuotaRecoveryFailure, QuotaRecoveryPage, ReconcileFailure,
    ReconcilePage, RunLifecycle, RunTransition,
};
pub use automation_sprint_manual::{ManualDispatchFailure, ManualDispatcher, ManualJob};
pub use automation_sprint_quota::{
    AutopilotQuotaPolicy, AutopilotQuotaPolicyError, AutopilotQuotaPolicySource,
};
pub use automation_sprint_schedule::{ScheduleDispatcher, ScheduleJob, ScheduleWorker};
pub use automation_sprint_worker::{DispatchFailure, WebhookDispatcher, WebhookJob, WebhookWorker};

use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::{WorkspaceId, parse_params},
    error,
    runtime_chat_events::Event,
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::{GenericClient, Transaction};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/autopilots", get(list).post(create))
        .route("/api/autopilots/", get(list).post(create))
        .route(
            "/api/autopilots/cron-preview",
            get(automation_sprint_cron::preview),
        )
        .route(
            "/api/autopilots/{id}",
            get(detail).patch(update).delete(archive),
        )
        .route(
            "/api/autopilots/{id}/",
            get(detail).patch(update).delete(archive),
        )
        .merge(automation_sprint_quota::routes())
        .merge(automation_sprint_manual::routes())
        .merge(automation_sprint_triggers::routes())
        .merge(automation_sprint_deliveries::routes())
}

fn fail(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn parse_id(s: &str, what: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(s).map_err(|_| bad(what))
}
fn time(row: &Row, name: &str) -> Option<String> {
    row.get::<_, Option<DateTime<Utc>>>(name).map(timestamp)
}

async fn acting_member(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<(Uuid, String), ApiError> {
    let acting = match actor {
        Actor::Human | Actor::Cloud => user,
        Actor::Task(task) => {
            let row = db.query_opt("SELECT originator_user_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND status NOT IN ('completed','failed','cancelled')", &[&task.task, &task.agent]).await.map_err(|_| fail("failed to resolve autopilot actor"))?;
            row.and_then(|r| r.get::<_, Option<Uuid>>(0))
                .ok_or_else(|| {
                    error(
                        StatusCode::FORBIDDEN,
                        "no human authorized this change: the calling run records no originator",
                    )
                })?
        }
        Actor::Daemon => {
            return Err(error(
                StatusCode::FORBIDDEN,
                "no human authorized this change: the calling run records no originator",
            ));
        }
    };
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &acting],
        )
        .await
        .map_err(|_| fail("failed to resolve autopilot actor"))?
        .ok_or_else(|| {
            error(
                StatusCode::FORBIDDEN,
                "the person this run acts for is not a member of this workspace",
            )
        })?
        .get::<_, String>(0);
    Ok((acting, role))
}

async fn read_member(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<Option<(Uuid, String)>, ApiError> {
    match acting_member(db, ws, user, actor).await {
        Ok(member) => Ok(Some(member)),
        Err(ApiError(StatusCode::FORBIDDEN, _, _)) => Ok(None),
        Err(e) => Err(e),
    }
}

fn own(row: &Row, member: Uuid, role: &str) -> bool {
    matches!(role, "owner" | "admin")
        || row.get::<_, String>("created_by_type") == "member"
            && row.get::<_, Uuid>("created_by_id") == member
}

async fn can_write(
    db: &impl GenericClient,
    row: &Row,
    member: Uuid,
    role: &str,
) -> Result<bool, ApiError> {
    if own(row, member, role) {
        return Ok(true);
    }
    let id: Uuid = row.get("id");
    let granted: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM autopilot_collaborator WHERE autopilot_id=$1 AND user_type='member' AND user_id=$2)", &[&id, &member]).await.map_err(|_| fail("failed to resolve autopilot access"))?.get(0);
    Ok(granted)
}

async fn required_writer(
    db: &impl GenericClient,
    row: &Row,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<Uuid, ApiError> {
    let (member, role) = acting_member(db, ws, user, actor).await?;
    if !can_write(db, row, member, &role).await? {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the autopilot creator, a workspace admin, or a granted collaborator can manage this autopilot",
        ));
    }
    Ok(member)
}

async fn load(db: &impl GenericClient, ws: Uuid, raw: &str) -> Result<Row, ApiError> {
    let id = parse_id(raw, "invalid autopilot id")?;
    db.query_opt(
        "SELECT * FROM autopilot WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail("failed to load autopilot"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "autopilot not found"))
}

async fn subscribers(
    db: &impl GenericClient,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<Value>>, ApiError> {
    let mut entries = HashMap::<Uuid, Vec<Value>>::new();
    if !ids.is_empty() {
        for row in db.query("SELECT autopilot_id,user_type,user_id,created_at FROM autopilot_subscriber WHERE autopilot_id=ANY($1) ORDER BY autopilot_id,created_at,user_id", &[&ids]).await.map_err(|_| fail("failed to list autopilot subscribers"))? {
            let id: Uuid = row.get("autopilot_id");
            entries.entry(id).or_default().push(json!({"user_type":row.get::<_,String>("user_type"),"user_id":row.get::<_,Uuid>("user_id"),"created_at":time(&row,"created_at")}));
        }
    }
    Ok(entries)
}

fn response(row: &Row, subs: Vec<Value>) -> Value {
    let kind: String = row.get("assignee_type");
    json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "title":row.get::<_,String>("title"), "description":row.get::<_,Option<String>>("description"),
        "project_id":row.get::<_,Option<Uuid>>("project_id"),
        "assignee_type":if kind.is_empty(){"agent"}else{&kind}, "assignee_id":row.get::<_,Uuid>("assignee_id"),
        "status":row.get::<_,String>("status"), "pause_reason":row.get::<_,Option<String>>("pause_reason"),
        "execution_mode":row.get::<_,String>("execution_mode"),
        "issue_title_template":row.get::<_,Option<String>>("issue_title_template"),
        "created_by_type":row.get::<_,String>("created_by_type"), "created_by_id":row.get::<_,Uuid>("created_by_id"),
        "last_run_at":time(row,"last_run_at"), "created_at":time(row,"created_at"),"updated_at":time(row,"updated_at"),
        "subscribers":subs
    })
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to list autopilots"))?;
    let filter = parse_params(query.as_deref())
        .remove("status")
        .filter(|x| !x.is_empty());
    let rows = db
        .query(
            "SELECT a.*,
            (SELECT array_agg(DISTINCT t.kind ORDER BY t.kind) FROM autopilot_trigger t
             WHERE t.autopilot_id=a.id AND t.enabled)::text[] AS trigger_kinds,
            (SELECT min(t.next_run_at) FROM autopilot_trigger t
             WHERE t.autopilot_id=a.id AND t.enabled AND t.kind='schedule') AS next_run_at,
            (SELECT r.status FROM autopilot_run r WHERE r.autopilot_id=a.id
             ORDER BY r.triggered_at DESC LIMIT 1) AS last_run_status
         FROM autopilot a
         WHERE a.workspace_id=$1 AND (($2::text IS NULL AND a.status<>'archived') OR a.status=$2)
         ORDER BY a.created_at DESC",
            &[&ws, &filter],
        )
        .await
        .map_err(|_| fail("failed to list autopilots"))?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let mut subs = subscribers(&db, &ids).await?;
    let writer = read_member(&db, ws, user, &actor).await?;
    let grants = if let Some((member, _)) = &writer {
        db.query("SELECT autopilot_id FROM autopilot_collaborator WHERE user_id=$1 AND user_type='member' AND autopilot_id=ANY($2)", &[member, &ids]).await.map_err(|_| fail("failed to resolve autopilot access"))?.iter().map(|r| r.get::<_,Uuid>(0)).collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let id: Uuid = row.get("id");
        let mut item = response(&row, subs.remove(&id).unwrap_or_default());
        if let Some(kinds) = row.get::<_, Option<Vec<String>>>("trigger_kinds") {
            item["trigger_kinds"] = json!(kinds);
        }
        if let Some(next) = time(&row, "next_run_at") {
            item["next_run_at"] = json!(next);
        }
        if let Some(last) = row.get::<_, Option<String>>("last_run_status") {
            item["last_run_status"] = json!(last);
        }
        if let Some((member, role)) = &writer {
            item["can_write"] = json!(own(&row, *member, role) || grants.contains(&id));
        }
        result.push(item);
    }
    Ok(GoJson(json!({"total":result.len(),"autopilots":result})))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to load autopilot"))?;
    let row = load(&db, ws, &raw).await?;
    let id: Uuid = row.get("id");
    let mut subs = subscribers(&db, &[id]).await?;
    let mut ap = response(&row, subs.remove(&id).unwrap_or_default());
    let writer = read_member(&db, ws, user, &actor).await?;
    let (can_write, can_manage) = if let Some((member, role)) = writer {
        (
            can_write(&db, &row, member, &role).await?,
            own(&row, member, &role),
        )
    } else {
        (false, false)
    };
    ap["can_write"] = json!(can_write);
    ap["can_manage_access"] = json!(can_manage);
    let mut triggers = Vec::new();
    for row in db
        .query(
            "SELECT * FROM autopilot_trigger WHERE autopilot_id=$1 ORDER BY created_at ASC",
            &[&id],
        )
        .await
        .map_err(|_| fail("failed to list autopilot triggers"))?
    {
        triggers.push(automation_sprint_triggers::response(&row, can_write));
    }
    let mut collaborators = Vec::new();
    for row in db.query("SELECT user_type,user_id,granted_by,created_at FROM autopilot_collaborator WHERE autopilot_id=$1 ORDER BY created_at", &[&id]).await.map_err(|_| fail("failed to list collaborators"))? {
        collaborators.push(json!({"user_type":row.get::<_,String>("user_type"),"user_id":row.get::<_,Uuid>("user_id"),"granted_by":row.get::<_,Uuid>("granted_by"),"created_at":time(&row,"created_at")}));
    }
    Ok(GoJson(
        json!({"autopilot":ap,"triggers":triggers,"collaborators":collaborators}),
    ))
}

fn body(bytes: &Bytes) -> Result<Value, ApiError> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| bad("invalid request body"))?;
    if !value.is_object() {
        return Err(bad("invalid request body"));
    }
    Ok(value)
}
fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn optional_text(value: &Value, key: &str) -> Result<Option<String>, ApiError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        _ => Err(bad("invalid request body")),
    }
}

fn validate_strings(value: &Value, keys: &[&str]) -> Result<(), ApiError> {
    for key in keys {
        optional_text(value, key)?;
    }
    Ok(())
}

fn project_id(value: &Value) -> Result<Option<Uuid>, ApiError> {
    match optional_text(value, "project_id")?.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => Ok(Some(parse_id(s, "invalid project_id")?)),
    }
}

async fn validate_project(
    db: &impl GenericClient,
    ws: Uuid,
    project: Option<Uuid>,
) -> Result<(), ApiError> {
    if let Some(id) = project
        && db
            .query_opt(
                "SELECT 1 FROM project WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .map_err(|_| fail("failed to validate project_id"))?
            .is_none()
    {
        return Err(bad("project_id must reference a project in this workspace"));
    }
    Ok(())
}

fn subscriber_ids(value: &Value) -> Result<Vec<(Uuid, usize)>, ApiError> {
    let Some(raw) = value.get("subscribers").filter(|v| !v.is_null()) else {
        return Ok(Vec::new());
    };
    let Some(entries) = raw.as_array() else {
        return Err(bad("invalid subscribers"));
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for (index, entry) in entries.iter().enumerate() {
        if text(entry, "user_type") != Some("member") {
            return Err(bad("subscribers[].user_type must be 'member'"));
        }
        let id = parse_id(
            text(entry, "user_id").unwrap_or(""),
            "invalid subscribers[].user_id",
        )?;
        if seen.insert(id) {
            out.push((id, index));
        }
    }
    Ok(out)
}

async fn lock_subscribers(
    tx: &Transaction<'_>,
    ws: Uuid,
    subs: &[(Uuid, usize)],
) -> Result<(), ApiError> {
    let mut sorted = subs.to_vec();
    sorted.sort_unstable_by_key(|(id, _)| *id);
    for (id, _) in &sorted {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtext($1::uuid::text),hashtext($2::uuid::text))",
            &[&ws, &id],
        )
        .await
        .map_err(|_| fail("failed to validate autopilot subscribers"))?;
    }
    for (id, _) in sorted {
        if tx
            .query_opt(
                "SELECT user_id FROM member WHERE workspace_id=$1 AND user_id=$2 FOR KEY SHARE",
                &[&ws, &id],
            )
            .await
            .map_err(|_| fail("failed to validate autopilot subscribers"))?
            .is_none()
        {
            return Err(bad("subscriber is not a member of this workspace"));
        }
    }
    Ok(())
}

async fn validate_assignee(
    tx: &Transaction<'_>,
    ws: Uuid,
    kind: &str,
    id: Uuid,
    active: bool,
    actor: Uuid,
) -> Result<(), ApiError> {
    let agent = match kind {
        "agent" => tx.query_opt("SELECT archived_at,runtime_id,owner_id,permission_mode FROM agent WHERE workspace_id=$1 AND id=$2 FOR UPDATE", &[&ws,&id]).await.map_err(|_| fail("failed to validate assignee"))?.ok_or_else(|| bad("assignee must be a valid agent in this workspace"))?,
        "squad" => {
            let squad = tx.query_opt("SELECT leader_id,archived_at FROM squad WHERE workspace_id=$1 AND id=$2 FOR UPDATE", &[&ws,&id]).await.map_err(|_| fail("failed to validate assignee"))?.ok_or_else(|| bad("assignee must be a valid squad in this workspace"))?;
            if squad.get::<_,Option<DateTime<Utc>>>("archived_at").is_some() { return Err(error(StatusCode::UNPROCESSABLE_ENTITY,"squad is archived; pick a different squad")); }
            let leader: Uuid = squad.get("leader_id");
            tx.query_opt("SELECT archived_at,runtime_id,owner_id,permission_mode FROM agent WHERE workspace_id=$1 AND id=$2 FOR UPDATE", &[&ws,&leader]).await.map_err(|_| fail("failed to validate assignee"))?.ok_or_else(|| bad("squad leader agent not found"))?
        }
        _ => return Err(bad("assignee_type must be agent or squad")),
    };
    if agent
        .get::<_, Option<DateTime<Utc>>>("archived_at")
        .is_some()
    {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "assignee agent is archived; pick a different agent",
        ));
    }
    if active && agent.get::<_, Option<Uuid>>("runtime_id").is_none() {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "assignee agent needs a runtime before this autopilot can be active",
        ));
    }
    if kind == "squad" {
        let owner: Option<Uuid> = agent.get("owner_id");
        if owner != Some(actor) {
            let mode: String = agent.get("permission_mode");
            let leader: Uuid = tx
                .query_one(
                    "SELECT leader_id FROM squad WHERE workspace_id=$1 AND id=$2",
                    &[&ws, &id],
                )
                .await
                .map_err(|_| fail("failed to validate assignee"))?
                .get(0);
            if mode != "public_to" || !tx.query_one("SELECT EXISTS(SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND (target_type='workspace' AND target_id=$2 OR target_type='member' AND target_id=$3))", &[&leader,&ws,&actor]).await.map_err(|_| fail("failed to validate assignee"))?.get::<_,bool>(0) {
                return Err(error(StatusCode::FORBIDDEN,"cannot assign autopilot to squad with private leader"));
            }
        }
    }
    Ok(())
}

async fn rule_version(tx: &Transaction<'_>, row: &Row, publisher: Uuid) -> Result<(), ApiError> {
    let id: Uuid = row.get("id");
    let ws: Uuid = row.get("workspace_id");
    let summary = json!({"assignee_type":row.get::<_,String>("assignee_type"),"assignee_id":row.get::<_,Uuid>("assignee_id"),"status":row.get::<_,String>("status"),"execution_mode":row.get::<_,String>("execution_mode")});
    tx.execute("INSERT INTO autopilot_rule_version(autopilot_id,workspace_id,published_by_type,published_by_id,config_summary) VALUES ($1,$2,'member',$3,$4)", &[&id,&ws,&publisher,&summary]).await.map_err(|_| fail("failed to publish autopilot rule"))?;
    Ok(())
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    bytes: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let input = body(&bytes)?;
    validate_strings(
        &input,
        &[
            "title",
            "description",
            "project_id",
            "assignee_type",
            "assignee_id",
            "execution_mode",
            "issue_title_template",
            "status",
        ],
    )?;
    let title = text(&input, "title")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad("title is required"))?;
    let assignee = text(&input, "assignee_id")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad("assignee_id is required"))?;
    let mode = text(&input, "execution_mode")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad("execution_mode is required"))?;
    if !matches!(mode, "create_issue" | "run_only") {
        return Err(bad("execution_mode must be create_issue or run_only"));
    }
    let assignee = parse_id(assignee, "invalid assignee_id")?;
    let kind = text(&input, "assignee_type")
        .filter(|s| !s.is_empty())
        .unwrap_or("agent");
    if !matches!(kind, "agent" | "squad") {
        return Err(bad("assignee_type must be agent or squad"));
    }
    let project = project_id(&input)?;
    let description = optional_text(&input, "description")?;
    let template = optional_text(&input, "issue_title_template")?;
    automation_sprint_triggers::validate_template(template.as_deref())?;
    let subs = subscriber_ids(&input)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to create autopilot"))?;
    let (member, _) = acting_member(&db, ws, user, &actor).await?;
    validate_project(&db, ws, project).await?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to create autopilot"))?;
    lock_subscribers(&tx, ws, &subs).await?;
    validate_assignee(&tx, ws, kind, assignee, true, member).await?;
    let row = tx.query_one("INSERT INTO autopilot(workspace_id,title,description,assignee_type,assignee_id,status,execution_mode,issue_title_template,project_id,created_by_type,created_by_id) VALUES ($1,$2,$3,$4,$5,'active',$6,$7,$8,'member',$9) RETURNING *", &[&ws,&title,&description,&kind,&assignee,&mode,&template,&project,&member]).await.map_err(|_| fail("failed to create autopilot"))?;
    rule_version(&tx, &row, member).await?;
    let id: Uuid = row.get("id");
    for (subscriber, _) in &subs {
        tx.execute("INSERT INTO autopilot_subscriber(autopilot_id,user_type,user_id) VALUES ($1,'member',$2)", &[&id,subscriber]).await.map_err(|_| fail("failed to add autopilot subscriber"))?;
    }
    let mut res = subscribers(&tx, &[id]).await?;
    let result = response(&row, res.remove(&id).unwrap_or_default());
    tx.commit()
        .await
        .map_err(|_| fail("failed to create autopilot"))?;
    emit(
        &state,
        "autopilot:created",
        ws,
        member,
        json!({"autopilot":result}),
    );
    Ok((StatusCode::CREATED, GoJson(result)))
}

fn emit(state: &ApiState, event: &str, ws: Uuid, actor: Uuid, payload: Value) {
    state.events.publish(Event {
        event_type: event.to_owned(),
        workspace_id: ws,
        actor_type: "member".into(),
        actor_id: actor,
        chat_session_id: None,
        chat_session_owner_id: None,
        payload,
    });
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let input = body(&bytes)?;
    validate_strings(
        &input,
        &[
            "title",
            "description",
            "project_id",
            "assignee_type",
            "assignee_id",
            "execution_mode",
            "issue_title_template",
            "status",
        ],
    )?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to update autopilot"))?;
    let before = load(&db, ws, &raw).await?;
    let editor = required_writer(&db, &before, ws, user, &actor).await?;
    let id: Uuid = before.get("id");
    let title = text(&input, "title").unwrap_or_else(|| before.get("title"));
    if title.is_empty() {
        return Err(bad("title is required"));
    }
    let description = optional_text(&input, "description")?.or_else(|| before.get("description"));
    let template = if input.get("issue_title_template").is_some() {
        optional_text(&input, "issue_title_template")?
    } else {
        before.get("issue_title_template")
    };
    automation_sprint_triggers::validate_template(template.as_deref())?;
    let project = if input.get("project_id").is_some() {
        project_id(&input)?
    } else {
        before.get("project_id")
    };
    validate_project(&db, ws, project).await?;
    let status = text(&input, "status").unwrap_or_else(|| before.get("status"));
    if !matches!(status, "active" | "paused" | "archived") {
        return Err(bad("invalid autopilot status"));
    }
    let mode = text(&input, "execution_mode").unwrap_or_else(|| before.get("execution_mode"));
    if !matches!(mode, "create_issue" | "run_only") {
        return Err(bad("execution_mode must be create_issue or run_only"));
    }
    let old_kind: String = before.get("assignee_type");
    let kind = text(&input, "assignee_type")
        .filter(|s| !s.is_empty())
        .unwrap_or(&old_kind);
    if !matches!(kind, "agent" | "squad") {
        return Err(bad("assignee_type must be agent or squad"));
    }
    if kind != old_kind && input.get("assignee_id").is_none() {
        return Err(bad("assignee_id is required when changing assignee_type"));
    }
    let assignee: Uuid = if let Some(value) = input.get("assignee_id") {
        parse_id(
            value
                .as_str()
                .ok_or_else(|| bad("assignee_id cannot be null"))?,
            "invalid assignee_id",
        )?
    } else {
        before.get("assignee_id")
    };
    let replace = input.get("subscribers").is_some();
    let subs = subscriber_ids(&input)?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to update autopilot"))?;
    lock_subscribers(&tx, ws, &subs).await?;
    if input.get("assignee_type").is_some()
        || input.get("assignee_id").is_some()
        || input.get("status").is_some_and(|s| s == "active")
    {
        validate_assignee(&tx, ws, kind, assignee, status == "active", editor).await?;
    }
    let locked = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &locked, ws, user, &actor).await?;
    if locked.get::<_, DateTime<Utc>>("updated_at") != before.get::<_, DateTime<Utc>>("updated_at")
    {
        return Err(error(
            StatusCode::CONFLICT,
            "the autopilot changed while it was being edited; reload and try again.",
        ));
    }
    // Rule-version snapshot, trigger publishers and subscriber replacement commit atomically.
    let row = tx.query_one("UPDATE autopilot SET title=$2,description=$3,project_id=$4,assignee_type=$5,assignee_id=$6,status=$7,pause_reason=CASE WHEN $8::bool THEN NULL ELSE pause_reason END,execution_mode=$9,issue_title_template=$10,updated_at=now() WHERE id=$1 RETURNING *", &[&id,&title,&description,&project,&kind,&assignee,&status,&input.get("status").is_some(),&mode,&template]).await.map_err(|_| fail("failed to update autopilot"))?;
    let substantive = ["assignee_type", "status", "execution_mode"]
        .into_iter()
        .any(|k| before.get::<_, String>(k) != row.get::<_, String>(k))
        || before.get::<_, Uuid>("assignee_id") != assignee
        || before.get::<_, Option<String>>("description") != description
        || before.get::<_, Option<String>>("issue_title_template") != template;
    if substantive {
        rule_version(&tx, &row, editor).await?;
        tx.execute("UPDATE autopilot_trigger SET published_by_type='member',published_by_id=$2,updated_at=now() WHERE autopilot_id=$1", &[&id,&editor]).await.map_err(|_| fail("failed to update autopilot"))?;
    }
    if replace {
        tx.execute(
            "DELETE FROM autopilot_subscriber WHERE autopilot_id=$1",
            &[&id],
        )
        .await
        .map_err(|_| fail("failed to update subscribers"))?;
        for (member, _) in &subs {
            tx.execute("INSERT INTO autopilot_subscriber(autopilot_id,user_type,user_id) VALUES ($1,'member',$2)", &[&id,member]).await.map_err(|_|fail("failed to add autopilot subscriber"))?;
        }
    }
    let mut actual = subscribers(&tx, &[id]).await?;
    let result = response(&row, actual.remove(&id).unwrap_or_default());
    tx.commit()
        .await
        .map_err(|_| fail("failed to update autopilot"))?;
    emit(
        &state,
        "autopilot:updated",
        ws,
        editor,
        json!({"autopilot":result}),
    );
    Ok(GoJson(result))
}

async fn load_locked(tx: &Transaction<'_>, ws: Uuid, id: Uuid) -> Result<Row, ApiError> {
    tx.query_opt(
        "SELECT * FROM autopilot WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail("failed to update autopilot"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "autopilot not found"))
}

async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| fail("failed to delete autopilot"))?;
    let prev = load(&db, ws, &raw).await?;
    let editor = required_writer(&db, &prev, ws, user, &actor).await?;
    let id: Uuid = prev.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| fail("failed to delete autopilot"))?;
    let locked = load_locked(&tx, ws, id).await?;
    required_writer(&tx, &locked, ws, user, &actor).await?;
    let row = tx.query_opt("UPDATE autopilot SET status='archived',pause_reason=NULL,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws]).await.map_err(|_|fail("failed to delete autopilot"))?.ok_or_else(||error(StatusCode::NOT_FOUND,"autopilot not found"))?;
    rule_version(&tx, &row, editor).await?;
    tx.commit()
        .await
        .map_err(|_| fail("failed to delete autopilot"))?;
    emit(
        &state,
        "autopilot:deleted",
        ws,
        editor,
        json!({"autopilot_id":id}),
    );
    Ok(StatusCode::NO_CONTENT)
}
