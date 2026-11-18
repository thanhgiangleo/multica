//! Local skill import RPC. Bundle and terminal result commit atomically in PostgreSQL.
//! Register only with real heartbeat delivery AND workspace skill event publication.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    daemon::DaemonIdentity,
    error,
    runtime_skills_sprint::{self, SkillEvent},
    runtime_skills_sprint_control::{daemon_access, human_access, runtime_id},
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn user_routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/runtimes/{runtimeId}/local-skills/import",
            post(initiate),
        )
        .route(
            "/api/runtimes/{runtimeId}/local-skills/import/{requestId}",
            get(poll),
        )
}
pub(super) fn daemon_routes() -> Router<ApiState> {
    Router::new().route(
        "/api/daemon/runtimes/{runtimeId}/local-skills/import/{requestId}/result",
        post(report),
    )
}
fn failure(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
#[derive(Default)]
struct ImportInput {
    skill_key: String,
    name: Option<String>,
    description: Option<String>,
    action: String,
    target_skill_id: String,
    supports_conflict: bool,
}
#[derive(Default)]
struct File {
    path: String,
    content: String,
}
#[derive(Default)]
struct Bundle {
    name: String,
    description: String,
    content: String,
    source_path: String,
    provider: String,
    files: Vec<File>,
}
#[derive(Default)]
struct Report {
    status: String,
    skill: Option<Bundle>,
    error: String,
}
// Go struct decoding: case-insensitive keys, last assignment in wire order,
// null preserves scalars but clears pointers/slices. Unknown fields never survive.
macro_rules! wire_object {
    ($ty:ty; preserve [$($scalar:ident),*]; nullable [$($pointer:ident),*]; arrays [$($array:ident),*]) => {
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self,D::Error> {
                struct Object;
                impl<'de> serde::de::Visitor<'de> for Object {
                    type Value = $ty;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("object or null") }
                    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value,E> { Ok(<$ty>::default()) }
                    fn visit_map<M: serde::de::MapAccess<'de>>(self,mut map:M)->Result<Self::Value,M::Error> {
                        let mut value=<$ty>::default();
                        while let Some(key)=map.next_key::<String>()? {
                            match key.to_ascii_lowercase().as_str() {
                                $(stringify!($scalar) => {
                                    if let Some(next)=map.next_value::<Option<_>>()? { value.$scalar=next; }
                                },)*
                                $(stringify!($pointer) => { value.$pointer=map.next_value()?; },)*
                                $(stringify!($array) => { value.$array=map.next_value::<Option<_>>()?.unwrap_or_default(); },)*
                                _ => { map.next_value::<serde::de::IgnoredAny>()?; }
                            }
                        }
                        Ok(value)
                    }
                }
                decoder.deserialize_any(Object)
            }
        }
    };
}
wire_object!(ImportInput; preserve [skill_key,action,target_skill_id,supports_conflict]; nullable [name,description]; arrays []);
wire_object!(File; preserve [path,content]; nullable []; arrays []);
wire_object!(Bundle; preserve [name,description,content,source_path,provider]; nullable []; arrays [files]);
wire_object!(Report; preserve [status,error]; nullable [skill]; arrays []);
fn decode<T: for<'de> Deserialize<'de>>(body: &Bytes) -> Result<T, ApiError> {
    T::deserialize(&mut serde_json::Deserializer::from_slice(body))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))
}
fn clean(value: &str) -> String {
    value.replace('\0', "")
}
fn trim(value: Option<String>) -> Option<String> {
    value.map(|s| clean(s.trim())).filter(|s| !s.is_empty())
}
fn safe_daemon_copy(value: &str) -> Option<String> {
    let clean = super::runtime_chat_redact::text(value);
    (clean == value.replace('\0', "")).then_some(clean)
}
const COLUMNS: &str = "id,runtime_id,workspace_id,creator_id,skill_key,name,description,action,target_skill_id,supports_conflict,status,skill,conflict,error,created_at,updated_at";
const TIMEOUTS: &str = "UPDATE runtime_rpc_local_skill_import SET status='timeout',error=CASE WHEN status='pending' THEN 'daemon did not respond within 3 minutes' ELSE 'daemon did not finish within 60 seconds' END,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE runtime_id=$1 AND expires_at>now() AND ((status='pending' AND created_at < now()-interval '3 minutes') OR (status='running' AND started_at < now()-interval '60 seconds'))";
fn projection(row: &Row) -> Value {
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "runtime_id":row.get::<_,Uuid>("runtime_id"),
        "skill_key":row.get::<_,String>("skill_key"), "status":row.get::<_,String>("status"),
        "created_at":timestamp(row.get("created_at")), "updated_at":timestamp(row.get("updated_at")),
    });
    for key in ["name", "description"] {
        if let Some(text) = row.get::<_, Option<String>>(key) {
            value[key] = json!(text);
        }
    }
    let action: String = row.get("action");
    if !action.is_empty() {
        value["action"] = json!(action);
    }
    if let Some(target) = row.get::<_, Option<Uuid>>("target_skill_id") {
        value["target_skill_id"] = json!(target);
    }
    if row.get::<_, bool>("supports_conflict") {
        value["supports_conflict"] = json!(true);
    }
    for key in ["skill", "conflict"] {
        if let Some(v) = row.get::<_, Option<Value>>(key) {
            value[key] = v;
        }
    }
    let message: String = row.get("error");
    if !message.is_empty() {
        value["error"] = json!(message);
    }
    value
}
/// Heartbeat must verify daemon token, workspace and current runtime owner before claim.
/// A claim is a durable transition: never use it as a speculative work probe.
pub async fn claim_pending_local_skill_imports(
    db: &impl GenericClient,
    runtime: Uuid,
    limit: i64,
) -> Result<Vec<Value>, tokio_postgres::Error> {
    db.execute(TIMEOUTS, &[&runtime]).await?;
    if limit <= 0 {
        return Ok(Vec::new());
    }
    let rows = db.query(
        "WITH next AS (SELECT id FROM runtime_rpc_local_skill_import WHERE runtime_id=$1 AND status='pending' AND expires_at>now() ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $2) UPDATE runtime_rpc_local_skill_import r SET status='running',started_at=now(),updated_at=now(),expires_at=now()+interval '5 minutes' FROM next WHERE r.id=next.id RETURNING r.id,r.skill_key,r.name,r.description,r.action,r.target_skill_id", &[&runtime,&limit.min(10)]
    ).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut result =
                json!({"id":row.get::<_,Uuid>("id"),"skill_key":row.get::<_,String>("skill_key")});
            for key in ["name", "description"] {
                if let Some(v) = row.get::<_, Option<String>>(key) {
                    result[key] = json!(v);
                }
            }
            let action: String = row.get("action");
            if !action.is_empty() {
                result["action"] = json!(action);
            }
            if let Some(target) = row.get::<_, Option<Uuid>>("target_skill_id") {
                result["target_skill_id"] = json!(target);
            }
            result
        })
        .collect())
}
pub async fn has_pending_local_skill_imports(
    db: &impl GenericClient,
    runtime: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    Ok(db.query_one("SELECT EXISTS(SELECT 1 FROM runtime_rpc_local_skill_import WHERE runtime_id=$1 AND status='pending' AND created_at>=now()-interval '3 minutes' AND expires_at>now())",&[&runtime]).await?.get(0))
}
async fn owner_access(
    db: &impl GenericClient,
    runtime: Uuid,
    workspace: Uuid,
    user: Uuid,
) -> Result<(), ApiError> {
    let rt = human_access(db, runtime, workspace, user).await?;
    if rt.owner != Some(user) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(())
}
async fn initiate(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to enqueue local skill import"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to enqueue local skill import"))?;
    let rt = human_access(&*tx, runtime, workspace, user).await?;
    if rt.owner != Some(user) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    if rt.status != "online" {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "runtime is offline"));
    }
    let input: ImportInput = decode(&body)?;
    let key = input.skill_key.trim();
    if key.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "skill_key is required"));
    }
    if safe_daemon_copy(key).is_none() || key.contains('\0') {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "skill_key contains credentials or invalid characters",
        ));
    }
    let target = match input.action.as_str() {
        "" => None,
        "overwrite" => Some(
            Uuid::parse_str(input.target_skill_id.trim())
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid target_skill_id"))?,
        ),
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid action")),
    };
    let name = trim(input.name);
    let description = trim(input.description);
    // A local skill name/description may become workspace-visible after import.
    if name
        .as_deref()
        .is_some_and(|s| safe_daemon_copy(s).is_none())
        || description
            .as_deref()
            .is_some_and(|s| safe_daemon_copy(s).is_none())
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "skill metadata contains credentials",
        ));
    }
    tx.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to enqueue local skill import"))?;
    tx.execute(
        "DELETE FROM runtime_rpc_local_skill_import WHERE runtime_id=$1 AND expires_at<=now()",
        &[&runtime],
    )
    .await
    .map_err(|_| failure("failed to enqueue local skill import"))?;
    let sql = format!(
        "INSERT INTO runtime_rpc_local_skill_import(id,runtime_id,workspace_id,creator_id,skill_key,name,description,action,target_skill_id,supports_conflict) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING {COLUMNS}"
    );
    let row = tx
        .query_one(
            &sql,
            &[
                &Uuid::new_v4(),
                &runtime,
                &workspace,
                &user,
                &key,
                &name,
                &description,
                &input.action,
                &target,
                &(input.supports_conflict || target.is_some()),
            ],
        )
        .await
        .map_err(|_| failure("failed to enqueue local skill import"))?;
    let result = projection(&row);
    tx.commit()
        .await
        .map_err(|_| failure("failed to enqueue local skill import"))?;
    state
        .runtime_rpc_transport
        .as_ref()
        .expect("gated routes")
        .notify_local_skill_import_pending(runtime);
    Ok(GoJson(result))
}
async fn poll(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, request)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load request"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    owner_access(&*tx, runtime, workspace, user).await?;
    let id =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    tx.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load request"))?;
    let sql = format!(
        "SELECT {COLUMNS} FROM runtime_rpc_local_skill_import WHERE id=$1 AND runtime_id=$2 AND workspace_id=$3 AND creator_id=$4 AND expires_at>now()"
    );
    let row = tx
        .query_opt(&sql, &[&id, &runtime, &workspace, &user])
        .await
        .map_err(|_| failure("failed to load request"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    let result = projection(&row);
    tx.commit()
        .await
        .map_err(|_| failure("failed to load request"))?;
    Ok(GoJson(result))
}
async fn fail(db: &impl GenericClient, id: Uuid, message: &str) -> Result<(), ApiError> {
    let message = super::runtime_chat_redact::text(message);
    db.execute("UPDATE runtime_rpc_local_skill_import SET status='failed',error=$2,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE id=$1",&[&id,&message]).await.map_err(|_|failure("failed to persist failure"))?;
    Ok(())
}
async fn conflict(
    db: &impl GenericClient,
    id: Uuid,
    creator: Uuid,
    supports: bool,
    existing: &Row,
) -> Result<(), ApiError> {
    if !supports {
        return fail(db, id, "a skill with this name already exists").await;
    }
    let owner: Option<Uuid> = existing.get("created_by");
    let mut info = json!({"existing_skill_id":existing.get::<_,Uuid>("id"),"can_overwrite":owner==Some(creator)});
    if let Some(owner) = owner {
        info["existing_created_by"] = json!(owner);
    }
    db.execute("UPDATE runtime_rpc_local_skill_import SET status='conflict',conflict=$2,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE id=$1",&[&id,&info]).await.map_err(|_|failure("failed to persist conflict"))?;
    Ok(())
}
async fn report(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((raw, request)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let runtime = runtime_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failure("failed to load request"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| failure("failed to load request"))?;
    let rt = daemon_access(&*tx, &identity, runtime).await?;
    let id =
        Uuid::parse_str(&request).map_err(|_| error(StatusCode::NOT_FOUND, "request not found"))?;
    tx.execute(TIMEOUTS, &[&runtime])
        .await
        .map_err(|_| failure("failed to load request"))?;
    let sql = format!(
        "SELECT {COLUMNS} FROM runtime_rpc_local_skill_import WHERE id=$1 AND runtime_id=$2 AND workspace_id=$3 AND expires_at>now() FOR UPDATE"
    );
    let req = tx
        .query_opt(&sql, &[&id, &runtime, &rt.workspace])
        .await
        .map_err(|_| failure("failed to load request"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "request not found"))?;
    let status: String = req.get("status");
    if status != "pending" && status != "running" {
        tx.commit()
            .await
            .map_err(|_| failure("failed to load request"))?;
        return Ok(GoJson(json!({"status":"ok"})));
    }
    let creator: Uuid = req.get("creator_id");
    // Never import from a transferred runtime on behalf of its former owner.
    let member = tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&rt.workspace, &creator],
        )
        .await
        .map_err(|_| failure("failed to check import permission"))?;
    if rt.owner != Some(creator) || member.is_none() {
        fail(
            &*tx,
            id,
            "you no longer own this runtime or workspace membership",
        )
        .await?;
        tx.commit()
            .await
            .map_err(|_| failure("failed to persist failure"))?;
        return Ok(GoJson(json!({"status":"ok"})));
    }
    let reported: Report = decode(&body)?;
    let bundle = if reported.status == "completed" {
        reported.skill
    } else {
        None
    };
    let Some(bundle) = bundle else {
        let message = if reported.status == "completed" {
            "daemon returned an empty skill bundle"
        } else {
            &reported.error
        };
        fail(&*tx, id, message).await?;
        tx.commit()
            .await
            .map_err(|_| failure("failed to persist failure"))?;
        return Ok(GoJson(json!({"status":"ok"})));
    };
    let name = req.get::<_, Option<String>>("name").unwrap_or(bundle.name);
    let description = req
        .get::<_, Option<String>>("description")
        .unwrap_or(bundle.description);
    if [
        &name,
        &description,
        &bundle.content,
        &bundle.provider,
        &bundle.source_path,
    ]
    .iter()
    .any(|s| safe_daemon_copy(s).is_none())
        || bundle
            .files
            .iter()
            .any(|f| safe_daemon_copy(&f.content).is_none() || safe_daemon_copy(&f.path).is_none())
    {
        fail(&*tx, id, "local skill bundle contains credentials").await?;
        tx.commit()
            .await
            .map_err(|_| failure("failed to persist failure"))?;
        return Ok(GoJson(json!({"status":"ok"})));
    }
    let name = clean(&name);
    let config = json!({"origin":{"type":"runtime_local","runtime_id":runtime,"provider":clean(&bundle.provider),"source_path":clean(&bundle.source_path)}});
    let action: String = req.get("action");
    let existing = if action == "overwrite" {
        let target: Option<Uuid> = req.get("target_skill_id");
        let existing = if let Some(target) = target {
            tx.query_opt(
                "SELECT * FROM skill WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
                &[&target, &rt.workspace],
            )
            .await
            .map_err(|_| failure("failed to overwrite skill"))?
        } else {
            None
        };
        let Some(existing) = existing else {
            fail(&*tx, id, "target skill no longer exists").await?;
            tx.commit()
                .await
                .map_err(|_| failure("failed to persist failure"))?;
            return Ok(GoJson(json!({"status":"ok"})));
        };
        if existing.get::<_, Option<Uuid>>("created_by") != Some(creator) {
            fail(
                &*tx,
                id,
                "you no longer have permission to overwrite this skill",
            )
            .await?;
            tx.commit()
                .await
                .map_err(|_| failure("failed to persist failure"))?;
            return Ok(GoJson(json!({"status":"ok"})));
        }
        if !name.is_empty() && existing.get::<_, String>("name") != name {
            fail(
                &*tx,
                id,
                "target skill name no longer matches the imported skill",
            )
            .await?;
            tx.commit()
                .await
                .map_err(|_| failure("failed to persist failure"))?;
            return Ok(GoJson(json!({"status":"ok"})));
        }
        Some(existing)
    } else {
        // ON CONFLICT handles concurrent imports and global CRUD without a check-then-insert race.
        None
    };
    let (skill, event_type) = if let Some(existing) = existing {
        let target: Uuid = existing.get("id");
        let row=tx.query_one("UPDATE skill SET description=$2,content=$3,config=$4,updated_at=now() WHERE id=$1 RETURNING *",&[&target,&clean(&description),&clean(&bundle.content),&config]).await.map_err(|_|failure("failed to overwrite skill"))?;
        tx.execute("DELETE FROM skill_file WHERE skill_id=$1", &[&target])
            .await
            .map_err(|_| failure("failed to overwrite skill"))?;
        (row, "skill:updated")
    } else {
        let row=tx.query_opt("INSERT INTO skill(workspace_id,name,description,content,config,created_by) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(workspace_id,name) DO NOTHING RETURNING *",&[&rt.workspace,&name,&clean(&description),&clean(&bundle.content),&config,&creator]).await.map_err(|_|failure("failed to create skill"))?;
        let Some(row) = row else {
            let existing = tx
                .query_opt(
                    "SELECT * FROM skill WHERE workspace_id=$1 AND name=$2 FOR SHARE",
                    &[&rt.workspace, &name],
                )
                .await
                .map_err(|_| failure("failed to check for existing skill"))?
                .ok_or_else(|| failure("failed to check for existing skill"))?;
            conflict(&*tx, id, creator, req.get("supports_conflict"), &existing).await?;
            tx.commit()
                .await
                .map_err(|_| failure("failed to persist conflict"))?;
            return Ok(GoJson(json!({"status":"ok"})));
        };
        (row, "skill:created")
    };
    let skill_id: Uuid = skill.get("id");
    let mut files = Vec::new();
    for file in bundle.files {
        if !runtime_skills_sprint::safe_path(&file.path)
            || file.path.eq_ignore_ascii_case("SKILL.md")
        {
            continue;
        }
        let row=tx.query_one("INSERT INTO skill_file(skill_id,path,content) VALUES($1,$2,$3) ON CONFLICT(skill_id,path) DO UPDATE SET content=EXCLUDED.content,updated_at=now() RETURNING *",&[&skill_id,&file.path,&clean(&file.content)]).await.map_err(|_|failure("failed to import skill file"))?;
        files.push(runtime_skills_sprint::file(&row));
    }
    let mut output = runtime_skills_sprint::skill(&skill);
    output["files"] = json!(files);
    tx.execute("UPDATE runtime_rpc_local_skill_import SET status='completed',skill=$2,updated_at=now(),expires_at=now()+interval '5 minutes' WHERE id=$1",&[&id,&output]).await.map_err(|_|failure("failed to persist import completion"))?;
    tx.commit()
        .await
        .map_err(|_| failure("failed to persist import completion"))?;
    state
        .skill_events
        .as_ref()
        .expect("gated routes")
        .publish(SkillEvent {
            event_type,
            workspace_id: rt.workspace,
            actor_type: "member",
            actor_id: creator,
            payload: json!({"skill":output}),
        });
    Ok(GoJson(json!({"status":"ok"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decode_rejects_bad_import_action_types_and_projects_bundle() {
        let body=Bytes::from_static(br#"{"skill_key":"  s  ","action":"overwrite","target_skill_id":"bad","supports_conflict":true,"secret":"unused"} trailing"#);
        let input: ImportInput = decode(&body).unwrap();
        assert_eq!(input.skill_key.trim(), "s");
        assert_eq!(input.action, "overwrite");
        assert!(input.supports_conflict);
        assert!(decode::<ImportInput>(&Bytes::from_static(br#"{"action":99}"#)).is_err());
        let result:Report=decode(&Bytes::from_static(br#"{"status":"completed","skill":{"name":"s","files":[{"path":"ref.txt","content":"ok","secret":"discard"}],"unknown":"discard"}}"#)).unwrap();
        assert_eq!(result.skill.unwrap().files[0].path, "ref.txt");
        assert!(safe_daemon_copy("TOKEN=secret-value").is_none());
        let input: ImportInput = decode(&Bytes::from_static(br#"{"SKILL_KEY":"first","skill_key":"last","skill_key":null,"SUPPORTS_CONFLICT":true,"supports_conflict":null,"NAME":"first","name":null}"#)).unwrap();
        assert_eq!(input.skill_key, "last");
        assert!(input.supports_conflict);
        assert!(input.name.is_none());
        let empty: ImportInput = decode(&Bytes::from_static(b"null trailing")).unwrap();
        assert!(empty.skill_key.is_empty());
        let result: Report = decode(&Bytes::from_static(br#"{"STATUS":"completed","SKILL":{"NAME":"s","FILES":[{"PATH":"ref","CONTENT":"data"}],"files":null}}"#)).unwrap();
        assert_eq!(result.status, "completed");
        assert!(result.skill.unwrap().files.is_empty());
    }
}
