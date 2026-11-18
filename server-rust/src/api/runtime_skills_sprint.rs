//! Global skill writes. The integrator supplies a real workspace event sink before registration.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use deadpool_postgres::GenericClient;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{Row, error::SqlState};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct SkillEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

pub trait SkillEventSink: Send + Sync {
    fn publish(&self, event: SkillEvent);
}

pub type SharedSkillEventSink = Arc<dyn SkillEventSink>;

type ResultJson = Result<Response, ApiError>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/skills", post(create))
        .route("/api/skills/", post(create))
        .route(
            "/api/skills/{id}",
            axum::routing::put(update).delete(delete),
        )
        .route(
            "/api/skills/{id}/",
            axum::routing::put(update).delete(delete),
        )
        .route("/api/skills/{id}/files", axum::routing::put(upsert_file))
        .route(
            "/api/skills/{id}/files/{fileId}",
            axum::routing::delete(delete_file),
        )
        .route("/api/skills/{id}/labels", post(attach_label))
        .route(
            "/api/skills/{id}/labels/{labelId}",
            axum::routing::delete(detach_label),
        )
}

fn json_response(status: StatusCode, payload: Value) -> Response {
    let mut response = GoJson(payload).into_response();
    *response.status_mut() = status;
    response
}

fn parse_body<T: for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, ApiError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    serde_json::from_value(if value.is_null() { json!({}) } else { value })
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}

fn uuid(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, field))
}

fn clean(value: &str) -> String {
    value.replace('\0', "")
}

// Go's path.Clean contract: slash-only canonical relative path; SKILL.md is primary content.
pub(super) fn safe_path(path: &str) -> bool {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['\0', '\\'])
        || (path.as_bytes().get(1) == Some(&b':') && path.as_bytes()[0].is_ascii_alphabetic())
    {
        return false;
    }
    !path
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
}

fn reserved_path(path: &str) -> bool {
    path.eq_ignore_ascii_case("SKILL.md")
}

fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Deserialize)]
struct SupportingFile {
    #[serde(default, deserialize_with = "null_default")]
    path: String,
    #[serde(default, deserialize_with = "null_default")]
    content: String,
}

#[derive(Deserialize)]
struct CreateSkill {
    #[serde(default, deserialize_with = "null_default")]
    name: String,
    #[serde(default, deserialize_with = "null_default")]
    description: String,
    #[serde(default, deserialize_with = "null_default")]
    content: String,
    config: Option<Value>,
    #[serde(default, deserialize_with = "null_default")]
    files: Vec<SupportingFile>,
}

#[derive(Deserialize)]
struct UpdateSkill {
    name: Option<String>,
    description: Option<String>,
    content: Option<String>,
    config: Option<Value>,
    files: Option<Vec<SupportingFile>>,
}

pub(super) fn skill(row: &Row) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"),
        "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "name":row.get::<_,String>("name"),
        "description":row.get::<_,String>("description"),
        "content":row.get::<_,String>("content"),
        "config":row.get::<_,Option<Value>>("config").unwrap_or_else(||json!({})),
        "created_by":row.get::<_,Option<Uuid>>("created_by"),
        "created_at":timestamp(row.get("created_at")),
        "updated_at":timestamp(row.get("updated_at")),
    })
}

pub(super) fn file(row: &Row) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"),
        "skill_id":row.get::<_,Uuid>("skill_id"),
        "path":row.get::<_,String>("path"),
        "content":row.get::<_,String>("content"),
        "created_at":timestamp(row.get("created_at")),
        "updated_at":timestamp(row.get("updated_at")),
    })
}

async fn files(db: &impl GenericClient, id: Uuid) -> Result<Vec<Value>, ApiError> {
    db.query(
        "SELECT * FROM skill_file WHERE skill_id=$1 ORDER BY path ASC",
        &[&id],
    )
    .await
    .map(|rows| rows.iter().map(file).collect())
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list skill files",
        )
    })
}

async fn current_skill(db: &impl GenericClient, id: Uuid, ws: Uuid) -> Result<Row, ApiError> {
    db.query_opt(
        "SELECT * FROM skill WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load skill"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "skill not found"))
}

async fn authorize(
    db: &impl GenericClient,
    id: Uuid,
    ws: Uuid,
    user: Uuid,
) -> Result<Row, ApiError> {
    // Recheck membership inside the write transaction. A concurrent removal cannot
    // commit between this lock and the write; the workspace middleware alone is not enough.
    let role: String = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load membership",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "skill not found"))?
        .get("role");
    let row = current_skill(db, id, ws).await?;
    if !matches!(role.as_str(), "owner" | "admin" | "member") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let creator: Option<Uuid> = row.get("created_by");
    if role != "owner" && role != "admin" && creator != Some(user) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only the skill creator can manage this skill",
        ));
    }
    Ok(row)
}

fn conflict(error: &tokio_postgres::Error, fallback: &'static str) -> ApiError {
    if error.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        super::error(
            StatusCode::CONFLICT,
            "a skill with this name already exists",
        )
    } else {
        super::error(StatusCode::INTERNAL_SERVER_ERROR, fallback)
    }
}

fn emit(
    state: &ApiState,
    event_type: &'static str,
    ws: Uuid,
    actor: &Actor,
    user: Uuid,
    payload: Value,
) {
    // Go's label handlers explicitly attribute mutations to the member, even
    // for task credentials; skill CRUD uses resolveActor instead.
    let (actor_type, actor_id) = match actor {
        Actor::Task(task) if event_type != "label:updated" => ("agent", task.agent),
        _ => ("member", user),
    };
    // Registration is gated on this being a real sink, never an internal no-op.
    state
        .skill_events
        .as_ref()
        .expect("skill routes require event sink")
        .publish(SkillEvent {
            event_type,
            workspace_id: ws,
            actor_type,
            actor_id,
            payload,
        });
}

fn check_actor(actor: &Actor) -> Result<(), ApiError> {
    if matches!(actor, Actor::Daemon) {
        Err(error(StatusCode::FORBIDDEN, "invalid actor"))
    } else {
        Ok(())
    }
}

fn invalid_file_response(files: &[SupportingFile]) -> Option<Response> {
    files.iter().find(|f| !safe_path(&f.path)).map(|f| {
        json_response(
            StatusCode::BAD_REQUEST,
            json!({"error":format!("invalid file path: {}", f.path)}),
        )
    })
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> ResultJson {
    check_actor(&actor)?;
    let req: CreateSkill = parse_body(&body)?;
    if req.name.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "name is required"));
    }
    if let Some(response) = invalid_file_response(&req.files) {
        return Ok(response);
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create skill"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    let member = tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create skill"))?;
    if member.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    let row = tx.query_one("INSERT INTO skill (workspace_id,name,description,content,config,created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *",
        &[&ws,&clean(&req.name),&clean(&req.description),&clean(&req.content),&req.config.unwrap_or_else(||json!({})),&user])
        .await.map_err(|e| conflict(&e,"failed to create skill"))?;
    let id: Uuid = row.get("id");
    let mut result_files = Vec::new();
    for f in &req.files {
        if reserved_path(&f.path) {
            continue;
        }
        let row = tx.query_one("INSERT INTO skill_file(skill_id,path,content) VALUES ($1,$2,$3) ON CONFLICT (skill_id,path) DO UPDATE SET content=EXCLUDED.content,updated_at=now() RETURNING *",
            &[&id,&f.path,&clean(&f.content)])
            .await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to create skill"))?;
        result_files.push(file(&row));
    }
    let mut response = skill(&row);
    response["files"] = json!(result_files);
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    emit(
        &state,
        "skill:created",
        ws,
        &actor,
        user,
        json!({"skill":response}),
    );
    Ok(json_response(StatusCode::CREATED, response))
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update skill"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    let req: UpdateSkill = parse_body(&body)?;
    if let Some(ref files) = req.files
        && let Some(response) = invalid_file_response(files)
    {
        return Ok(response);
    }
    let config = req.config;
    let row = tx.query_one("UPDATE skill SET name=COALESCE($3,name),description=COALESCE($4,description),content=COALESCE($5,content),config=COALESCE($6,config),updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",
        &[&id,&ws,&req.name.as_deref().map(clean),&req.description.as_deref().map(clean),&req.content.as_deref().map(clean),&config])
        .await.map_err(|e|conflict(&e,"failed to update skill"))?;
    let result_files = if let Some(files) = req.files {
        tx.execute("DELETE FROM skill_file WHERE skill_id=$1", &[&id])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to delete old skill files",
                )
            })?;
        let mut written = Vec::new();
        for f in files {
            if reserved_path(&f.path) {
                continue;
            }
            let row = tx.query_one("INSERT INTO skill_file(skill_id,path,content) VALUES($1,$2,$3) ON CONFLICT (skill_id,path) DO UPDATE SET content=EXCLUDED.content,updated_at=now() RETURNING *",
                &[&id,&f.path,&clean(&f.content)])
                .await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to upsert skill file"))?;
            written.push(file(&row));
        }
        written
    } else {
        files(&tx, id).await?
    };
    let mut response = skill(&row);
    response["files"] = json!(result_files);
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    emit(
        &state,
        "skill:updated",
        ws,
        &actor,
        user,
        json!({"skill":response}),
    );
    Ok(json_response(StatusCode::OK, response))
}

async fn delete(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete skill"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    tx.execute("DELETE FROM skill_to_label WHERE skill_id=$1", &[&id])
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to remove skill label assignments",
            )
        })?;
    tx.execute("DELETE FROM skill_file WHERE skill_id=$1", &[&id])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete skill"))?;
    tx.execute("DELETE FROM agent_skill WHERE skill_id=$1", &[&id])
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete skill"))?;
    let affected = tx
        .execute(
            "DELETE FROM skill WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete skill"))?;
    if affected != 1 {
        return Err(error(StatusCode::NOT_FOUND, "skill not found"));
    }
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit skill deletion",
        )
    })?;
    emit(
        &state,
        "skill:deleted",
        ws,
        &actor,
        user,
        json!({"skill_id":id}),
    );
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn upsert_file(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to upsert skill file",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    let req: SupportingFile = parse_body(&body)?;
    if !safe_path(&req.path) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid file path"));
    }
    if reserved_path(&req.path) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "SKILL.md is reserved for the primary skill content",
        ));
    }
    let row=tx.query_one("INSERT INTO skill_file(skill_id,path,content) VALUES($1,$2,$3) ON CONFLICT (skill_id,path) DO UPDATE SET content=EXCLUDED.content,updated_at=now() RETURNING *",
        &[&id,&req.path,&clean(&req.content)]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to upsert skill file"))?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    Ok(json_response(StatusCode::OK, file(&row)))
}

async fn delete_file(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_file)): Path<(String, String)>,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete skill file",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    let file_id = uuid(&raw_file, "invalid file id")?;
    let count = tx
        .execute(
            "DELETE FROM skill_file WHERE id=$1 AND skill_id=$2",
            &[&file_id, &id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete skill file",
            )
        })?;
    if count == 0 {
        return Err(error(StatusCode::NOT_FOUND, "skill file not found"));
    }
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct LabelId {
    #[serde(default, deserialize_with = "null_default")]
    label_id: String,
}

async fn labels(db: &impl GenericClient, id: Uuid, ws: Uuid) -> Result<Value, ApiError> {
    let rows=db.query("SELECT l.* FROM issue_label l JOIN skill_to_label stl ON l.id=stl.label_id WHERE stl.skill_id=$1 AND l.workspace_id=$2 AND l.resource_type='skill' ORDER BY lower(l.name) ASC", &[&id,&ws])
        .await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list skill labels"))?;
    Ok(json!({"labels":rows.iter().map(|r|json!({
        "id":r.get::<_,Uuid>("id"), "workspace_id":r.get::<_,Uuid>("workspace_id"),
        "resource_type":r.get::<_,String>("resource_type"), "name":r.get::<_,String>("name"),
        "description":r.get::<_,String>("description"), "color":r.get::<_,String>("color"), "usage_count":0,
        "created_at":timestamp(r.get("created_at")), "updated_at":timestamp(r.get("updated_at"))
    })).collect::<Vec<_>>()}))
}

async fn attach_label(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to attach skill label",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    let req: LabelId =
        parse_body(&body).map_err(|_| error(StatusCode::BAD_REQUEST, "label_id is required"))?;
    if req.label_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "label_id is required"));
    }
    let label_id = uuid(&req.label_id, "invalid label_id")?;
    let label=tx.query_opt("SELECT * FROM issue_label WHERE id=$1 AND workspace_id=$2 AND resource_type='skill' FOR SHARE", &[&label_id,&ws])
        .await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to attach skill label"))?
        .ok_or_else(||error(StatusCode::NOT_FOUND,"skill label not found"))?;
    tx.execute(
        "INSERT INTO skill_to_label(skill_id,label_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
        &[&id, &label_id],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to attach skill label",
        )
    })?;
    let response = labels(&tx, id, ws).await?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    emit(
        &state,
        "label:updated",
        ws,
        &actor,
        user,
        json!({"label":{
            "id":label.get::<_,Uuid>("id"),"workspace_id":ws,"resource_type":"skill",
            "name":label.get::<_,String>("name"),"description":label.get::<_,String>("description"),
            "color":label.get::<_,String>("color"),"usage_count":0,"created_at":timestamp(label.get("created_at")),
            "updated_at":timestamp(label.get("updated_at"))
        }}),
    );
    Ok(json_response(StatusCode::OK, response))
}

async fn detach_label(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, raw_label)): Path<(String, String)>,
) -> ResultJson {
    check_actor(&actor)?;
    let id = uuid(&raw, "invalid skill id")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to detach skill label",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    authorize(&tx, id, ws, user).await?;
    let label_id = uuid(&raw_label, "invalid label id")?;
    tx.execute("DELETE FROM skill_to_label WHERE skill_id=$1 AND label_id=$2 AND EXISTS (SELECT 1 FROM issue_label WHERE id=$2 AND workspace_id=$3 AND resource_type='skill')", &[&id,&label_id,&ws])
        .await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to detach skill label"))?;
    let response = labels(&tx, id, ws).await?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit"))?;
    emit(
        &state,
        "label:updated",
        ws,
        &actor,
        user,
        json!({"label_id":label_id,"resource_type":"skill"}),
    );
    Ok(json_response(StatusCode::OK, response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supporting_paths_match_canonical_go_contract() {
        for value in [
            "", ".", "..", "a/..", "a/../b", "a//b", "a/", "/a", "./a", "C:x", "D:/x", "x:y",
            "a\\b", "x\0y",
        ] {
            assert!(!safe_path(value), "{value:?}");
        }
        for value in [
            "references/guide.md",
            "scripts/test.py",
            ".hidden/file",
            "xx:y",
            "🍕/文档.md",
        ] {
            assert!(safe_path(value), "{value:?}");
        }
        assert!(reserved_path("sKiLl.Md"));
        assert!(!reserved_path("references/SKILL.md"));
    }

    #[test]
    fn null_and_missing_fields_follow_go_struct_defaults() {
        let request: CreateSkill = parse_body(&Bytes::from_static(
            br#"{"name":"x","description":null,"content":null,"files":null}"#,
        ))
        .unwrap();
        assert_eq!(request.name, "x");
        assert!(
            request.description.is_empty()
                && request.content.is_empty()
                && request.files.is_empty()
        );
        let file: SupportingFile =
            parse_body(&Bytes::from_static(br#"{"path":"guide.md"}"#)).unwrap();
        assert_eq!(file.content, "");
        assert!(parse_body::<CreateSkill>(&Bytes::from_static(br#"{"name":12}"#)).is_err());
        assert!(
            parse_body::<CreateSkill>(&Bytes::from_static(b"null"))
                .unwrap()
                .name
                .is_empty()
        );
    }
}
