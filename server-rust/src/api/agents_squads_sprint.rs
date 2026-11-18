//! Transactional squad configuration. Workspace events must be wired before writes.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Json, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{GenericClient, Row, Transaction, error::SqlState};
use uuid::Uuid;

#[path = "agents_squads_sprint_bindings.rs"]
pub(in crate::api) mod bindings;

#[derive(Clone, Debug, PartialEq)]
pub struct SquadEvent {
    pub workspace_id: Uuid,
    pub actor_id: Uuid,
    pub event_type: &'static str,
    pub payload: Value,
}

/// Integrator connects this to authenticated workspace fanout, not a no-op.
pub trait SquadEventSink: Send + Sync {
    fn publish(&self, event: SquadEvent);
}
pub type SharedSquadEventSink = Arc<dyn SquadEventSink>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/squads", post(create))
        .route("/api/squads/", post(create))
        .route(
            "/api/squads/{id}",
            axum::routing::put(update).delete(archive),
        )
        .route(
            "/api/squads/{id}/",
            axum::routing::put(update).delete(archive),
        )
        .route(
            "/api/squads/{id}/members",
            post(add_member).delete(remove_member),
        )
        .route(
            "/api/squads/{id}/members/role",
            axum::routing::patch(change_role),
        )
        .merge(bindings::routes())
}

fn bad(field: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, field)
}
fn failed(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn parse(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| bad(field))
}
fn decode<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(value)| value)
        .map_err(|_| bad("invalid request body"))
}
fn sink(state: &ApiState, actor: &Actor) -> Result<SharedSquadEventSink, ApiError> {
    actor.require_human()?;
    state.squad_events.clone().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "squad event bus unavailable",
        )
    })
}
async fn role(db: &impl GenericClient, ws: Uuid, user: Uuid) -> Result<String, ApiError> {
    db.query_opt(
        "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
        &[&ws, &user],
    )
    .await
    .map_err(|_| failed("failed to check squad permissions"))?
    .map(|row| row.get(0))
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))
}
fn manages(role: &str, user: Uuid, squad: &Row) -> Result<(), ApiError> {
    if matches!(role, "owner" | "admin") || squad.get::<_, Uuid>("creator_id") == user {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "insufficient permissions"))
    }
}
async fn locked_squad(tx: &Transaction<'_>, ws: Uuid, raw: &str) -> Result<Row, ApiError> {
    let id = parse(raw, "invalid squad id")?;
    tx.query_opt(
        "SELECT * FROM squad WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| failed("failed to load squad"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "squad not found"))
}
async fn leader(
    tx: &Transaction<'_>,
    ws: Uuid,
    user: Uuid,
    role: &str,
    id: Uuid,
) -> Result<Row, ApiError> {
    let row = tx.query_opt("SELECT id,owner_id,permission_mode,runtime_id FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR SHARE", &[&id, &ws])
        .await.map_err(|_| failed("failed to resolve leader"))?
        .ok_or_else(|| bad("leader must be a valid agent in this workspace"))?;
    if !matches!(role, "owner" | "admin")
        && !super::runtime_chat::can_invoke(
            tx,
            ws,
            user,
            &Actor::Human,
            id,
            row.get("owner_id"),
            row.get::<_, String>("permission_mode").as_str(),
        )
        .await?
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you can only use an agent you have access to as leader",
        ));
    }
    Ok(row)
}
// Storage-backed avatars require attachment ownership validation from files_ops.
// Refuse these until the integrator supplies that contract, rather than signing
// an issue/comment attachment as a public avatar.
fn avatar(raw: &str) -> Result<&str, ApiError> {
    if raw.is_empty() {
        Ok(raw)
    } else {
        Err(error(
            StatusCode::FORBIDDEN,
            "avatar storage validation unavailable",
        ))
    }
}
async fn response(db: &impl GenericClient, squad: &Row) -> Result<Value, ApiError> {
    let id: Uuid = squad.get("id");
    let rows = db.query("SELECT sm.member_type,sm.member_id,sm.role FROM squad_member sm WHERE sm.squad_id=$1 ORDER BY (sm.member_type='agent' AND sm.member_id=$2) DESC,sm.created_at ASC", &[&id, &squad.get::<_, Uuid>("leader_id")])
        .await.map_err(|_| failed("failed to load squad member preview"))?;
    let preview: Vec<Value> = rows.iter().take(3).map(|row| json!({"member_type":row.get::<_, String>("member_type"),"member_id":row.get::<_, Uuid>("member_id"),"role":row.get::<_, String>("role")})).collect();
    Ok(
        json!({"id":id,"workspace_id":squad.get::<_, Uuid>("workspace_id"),"name":squad.get::<_, String>("name"),"description":squad.get::<_, String>("description"),"instructions":squad.get::<_, String>("instructions"),"avatar_url":squad.get::<_, Option<String>>("avatar_url"),"leader_id":squad.get::<_, Uuid>("leader_id"),"creator_id":squad.get::<_, Uuid>("creator_id"),"created_at":timestamp(squad.get("created_at")),"updated_at":timestamp(squad.get("updated_at")),"archived_at":squad.get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at").map(timestamp),"archived_by":squad.get::<_, Option<Uuid>>("archived_by"),"member_count":rows.len(),"member_preview":if rows.is_empty() {Value::Null} else {json!(preview)}}),
    )
}
fn autopilot_response(row: &Row) -> Value {
    json!({
        "id":row.get::<_, Uuid>("id"), "workspace_id":row.get::<_, Uuid>("workspace_id"),
        "title":row.get::<_, String>("title"), "description":row.get::<_, Option<String>>("description"),
        "project_id":row.get::<_, Option<Uuid>>("project_id"),
        "assignee_type":row.get::<_, String>("assignee_type"), "assignee_id":row.get::<_, Uuid>("assignee_id"),
        "status":row.get::<_, String>("status"), "pause_reason":row.get::<_, Option<String>>("pause_reason"),
        "execution_mode":row.get::<_, String>("execution_mode"),
        "issue_title_template":row.get::<_, Option<String>>("issue_title_template"),
        "created_by_type":row.get::<_, String>("created_by_type"),
        "created_by_id":row.get::<_, Uuid>("created_by_id"),
        "last_run_at":row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("last_run_at").map(timestamp),
        "created_at":timestamp(row.get("created_at")), "updated_at":timestamp(row.get("updated_at")),
        "subscribers":[],
    })
}
fn member_response(row: &Row) -> Value {
    json!({"id":row.get::<_, Uuid>("id"),"squad_id":row.get::<_, Uuid>("squad_id"),"member_type":row.get::<_, String>("member_type"),"member_id":row.get::<_, Uuid>("member_id"),"role":row.get::<_, String>("role"),"created_at":timestamp(row.get("created_at"))})
}
fn publish(
    bus: &SharedSquadEventSink,
    ws: Uuid,
    user: Uuid,
    event_type: &'static str,
    payload: Value,
) {
    bus.publish(SquadEvent {
        workspace_id: ws,
        actor_id: user,
        event_type,
        payload,
    });
}

#[derive(Deserialize)]
struct Create {
    name: String,
    #[serde(default)]
    description: String,
    leader_id: String,
    avatar_url: Option<String>,
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<Create>, JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let bus = sink(&state, &actor)?;
    let req = decode(body)?;
    if req.name.is_empty() {
        return Err(bad("name is required"));
    }
    if req.leader_id.is_empty() {
        return Err(bad("leader_id is required"));
    }
    let leader_id = parse(&req.leader_id, "invalid leader_id")?;
    let avatar = req.avatar_url.as_deref().map(avatar).transpose()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to create squad"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to create squad"))?;
    let role = role(&*tx, ws, user).await?;
    leader(&tx, ws, user, &role, leader_id).await?;
    let row = tx.query_one("INSERT INTO squad(workspace_id,name,description,leader_id,creator_id,avatar_url) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *", &[&ws,&req.name,&req.description,&leader_id,&user,&avatar])
        .await.map_err(|_| failed("failed to create squad"))?;
    let id: Uuid = row.get("id");
    tx.execute("INSERT INTO squad_member(squad_id,member_type,member_id,role) VALUES ($1,'agent',$2,'leader')", &[&id,&leader_id])
        .await.map_err(|_| failed("failed to create squad"))?;
    let data = response(&*tx, &row).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to create squad"))?;
    publish(&bus, ws, user, "squad:created", json!({"squad":data}));
    Ok((StatusCode::CREATED, GoJson(data)))
}

#[derive(Deserialize)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    instructions: Option<String>,
    leader_id: Option<String>,
    avatar_url: Option<String>,
}
async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<Update>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let bus = sink(&state, &actor)?;
    let req = decode(body)?;
    let avatar = req.avatar_url.as_deref().map(avatar).transpose()?;
    let new_leader = req
        .leader_id
        .as_deref()
        .map(|raw| parse(raw, "invalid leader_id"))
        .transpose()?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update squad"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to update squad"))?;
    let role = role(&*tx, ws, user).await?;
    let squad = locked_squad(&tx, ws, &raw).await?;
    manages(&role, user, &squad)?;
    if squad
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("squad is archived"));
    }
    let id: Uuid = squad.get("id");
    let mut paused = Vec::new();
    if let Some(lid) = new_leader {
        let agent = leader(&tx, ws, user, &role, lid).await?;
        tx.execute("INSERT INTO squad_member(squad_id,member_type,member_id,role) VALUES ($1,'agent',$2,'leader') ON CONFLICT (squad_id,member_type,member_id) DO NOTHING", &[&id,&lid])
            .await.map_err(|_| failed("failed to update squad"))?;
        if agent.get::<_, Option<Uuid>>("runtime_id").is_none() {
            paused = tx.query("UPDATE autopilot SET status='paused',pause_reason='agent_runtime_required',updated_at=now() WHERE workspace_id=$2 AND status='active' AND assignee_type='squad' AND assignee_id=$1 RETURNING *", &[&id,&ws])
                .await.map_err(|_| failed("failed to update squad"))?.iter().map(autopilot_response).collect();
        }
    }
    let row = tx.query_one("UPDATE squad SET name=COALESCE($3,name),description=COALESCE($4,description),instructions=COALESCE($5,instructions),leader_id=COALESCE($6,leader_id),avatar_url=COALESCE($7,avatar_url),updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws,&req.name,&req.description,&req.instructions,&new_leader,&avatar])
        .await.map_err(|_| failed("failed to update squad"))?;
    let data = response(&*tx, &row).await?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to update squad"))?;
    publish(&bus, ws, user, "squad:updated", json!({"squad":data}));
    for autopilot in paused {
        publish(
            &bus,
            ws,
            user,
            "autopilot:updated",
            json!({"autopilot":autopilot}),
        );
    }
    Ok(GoJson(data))
}

async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let bus = sink(&state, &actor)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to archive squad"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to archive squad"))?;
    let role = role(&*tx, ws, user).await?;
    let squad = locked_squad(&tx, ws, &raw).await?;
    manages(&role, user, &squad)?;
    if squad
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("squad is already archived"));
    }
    let id: Uuid = squad.get("id");
    let lid: Uuid = squad.get("leader_id");
    // Historical malformed links must not transfer local work to a foreign agent.
    leader(&tx, ws, user, "owner", lid).await?;
    // No orphaned issue/automation assignees even if one transfer fails.
    tx.execute("UPDATE issue SET assignee_type='agent',assignee_id=$2,revision=revision+1,updated_at=now() WHERE workspace_id=$3 AND assignee_type='squad' AND assignee_id=$1", &[&id,&lid,&ws])
        .await.map_err(|_| failed("failed to archive squad"))?;
    tx.execute("UPDATE autopilot SET assignee_type='agent',assignee_id=$2,updated_at=now() WHERE workspace_id=$3 AND assignee_type='squad' AND assignee_id=$1", &[&id,&lid,&ws])
        .await.map_err(|_| failed("failed to archive squad"))?;
    tx.execute(
        "UPDATE squad SET archived_at=now(),archived_by=$2,updated_at=now() WHERE id=$1",
        &[&id, &user],
    )
    .await
    .map_err(|_| failed("failed to archive squad"))?;
    tx.commit()
        .await
        .map_err(|_| failed("failed to archive squad"))?;
    publish(
        &bus,
        ws,
        user,
        "squad:deleted",
        json!({"squad_id":id,"leader_id":lid}),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct MemberBody {
    member_type: String,
    member_id: String,
    #[serde(default)]
    role: String,
}
async fn add_member(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<MemberBody>, JsonRejection>,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let bus = sink(&state, &actor)?;
    let req = decode(body)?;
    if !matches!(req.member_type.as_str(), "agent" | "member") {
        return Err(bad("member_type must be 'agent' or 'member'"));
    }
    if req.member_id.is_empty() {
        return Err(bad("member_id is required"));
    }
    let mid = parse(&req.member_id, "invalid member_id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to add squad member"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to add squad member"))?;
    let role = role(&*tx, ws, user).await?;
    let squad = locked_squad(&tx, ws, &raw).await?;
    manages(&role, user, &squad)?;
    if squad
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("squad is archived"));
    }
    if req.member_type == "agent" {
        let agent = tx.query_opt("SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2 AND kind='user' FOR SHARE", &[&mid,&ws]).await.map_err(|_| failed("failed to add squad member"))?
            .ok_or_else(|| bad("agent not found in this workspace"))?;
        if !matches!(role.as_str(), "owner" | "admin")
            && !super::runtime_chat::can_invoke(
                &*tx,
                ws,
                user,
                &Actor::Human,
                mid,
                agent.get("owner_id"),
                agent.get::<_, String>("permission_mode").as_str(),
            )
            .await?
        {
            return Err(error(
                StatusCode::FORBIDDEN,
                "you can only add an agent you have access to",
            ));
        }
    } else if tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &mid],
        )
        .await
        .map_err(|_| failed("failed to add squad member"))?
        .is_none()
    {
        return Err(bad("member not found in this workspace"));
    }
    let id: Uuid = squad.get("id");
    let row = tx.query_one("INSERT INTO squad_member(squad_id,member_type,member_id,role) VALUES ($1,$2,$3,$4) RETURNING *", &[&id,&req.member_type,&mid,&req.role]).await.map_err(|err| {
        if err.code()==Some(&SqlState::UNIQUE_VIOLATION) { error(StatusCode::CONFLICT, "member already in squad") } else { failed("failed to add squad member") }
    })?;
    let value = member_response(&row);
    tx.commit()
        .await
        .map_err(|_| failed("failed to add squad member"))?;
    publish(&bus, ws, user, "squad:updated", json!({"squad_id":id}));
    Ok((StatusCode::CREATED, GoJson(value)))
}

#[derive(Deserialize)]
struct RemoveBody {
    member_type: String,
    member_id: String,
}
async fn remove_member(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<RemoveBody>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let bus = sink(&state, &actor)?;
    let req = decode(body)?;
    let mid = parse(&req.member_id, "invalid member_id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to remove squad member"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to remove squad member"))?;
    let role = role(&*tx, ws, user).await?;
    let squad = locked_squad(&tx, ws, &raw).await?;
    manages(&role, user, &squad)?;
    if squad
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("squad is archived"));
    }
    if req.member_type == "agent" && squad.get::<_, Uuid>("leader_id") == mid {
        return Err(bad("cannot remove the squad leader; change leader first"));
    }
    let id: Uuid = squad.get("id");
    let n = tx
        .execute(
            "DELETE FROM squad_member WHERE squad_id=$1 AND member_type=$2 AND member_id=$3",
            &[&id, &req.member_type, &mid],
        )
        .await
        .map_err(|_| failed("failed to remove squad member"))?;
    if n == 0 {
        return Err(error(StatusCode::NOT_FOUND, "squad member not found"));
    }
    tx.commit()
        .await
        .map_err(|_| failed("failed to remove squad member"))?;
    publish(&bus, ws, user, "squad:updated", json!({"squad_id":id}));
    Ok(StatusCode::NO_CONTENT)
}

async fn change_role(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Json<MemberBody>, JsonRejection>,
) -> Result<GoJson<Value>, ApiError> {
    let bus = sink(&state, &actor)?;
    let req = decode(body)?;
    let mid = parse(&req.member_id, "invalid member_id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to update squad member role"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failed("failed to update squad member role"))?;
    let role = role(&*tx, ws, user).await?;
    let squad = locked_squad(&tx, ws, &raw).await?;
    manages(&role, user, &squad)?;
    if squad
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(bad("squad is archived"));
    }
    if req.member_type == "agent"
        && squad.get::<_, Uuid>("leader_id") == mid
        && req.role != "leader"
    {
        return Err(bad(
            "cannot change the squad leader role; change leader first",
        ));
    }
    let id: Uuid = squad.get("id");
    let row = tx.query_opt("UPDATE squad_member SET role=$4 WHERE squad_id=$1 AND member_type=$2 AND member_id=$3 RETURNING *", &[&id,&req.member_type,&mid,&req.role]).await.map_err(|_| failed("failed to update squad member role"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "squad member not found"))?;
    let value = member_response(&row);
    tx.commit()
        .await
        .map_err(|_| failed("failed to update squad member role"))?;
    publish(&bus, ws, user, "squad:updated", json!({"squad_id":id}));
    Ok(GoJson(value))
}
