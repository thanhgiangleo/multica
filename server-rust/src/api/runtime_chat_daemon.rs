//! Daemon-only preparation mutations; runtime binding never falls back to membership.
use super::{
    ApiError, ApiState, GoJson,
    daemon::{self, DaemonIdentity},
    error, json,
    runtime_chat_events::Event,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/daemon/runtimes/{runtimeId}/tasks/{taskId}/prepare-lease",
            post(prepare_lease),
        )
        .route(
            "/api/daemon/tasks/{taskId}/wait-local-directory",
            post(wait_local_directory),
        )
        .route("/api/daemon/tasks/{taskId}/progress", post(progress))
        .merge(super::runtime_chat_start::routes())
        .merge(super::runtime_chat_stream::routes())
        .merge(super::runtime_chat_usage::routes())
        .merge(super::runtime_chat_delivery::routes())
}

fn failed(_: tokio_postgres::Error) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task")
}
fn task_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid task_id"))
}

fn binding(identity: &DaemonIdentity, daemon: Option<&str>, owner: Option<Uuid>) -> bool {
    match identity {
        DaemonIdentity::User(user) => owner == Some(*user),
        DaemonIdentity::Bound {
            daemon: bound,
            owner: bound_owner,
            ..
        } => {
            !bound.trim().is_empty()
                && daemon == Some(bound.as_str())
                && (bound.starts_with("qoder-managed-")
                    || bound_owner.is_some() && *bound_owner == owner)
        }
    }
}

// Resolve the source, not agent.workspace_id. Missing runtime/source means absent;
// a failed lookup stays 500 because daemons interrupt execution on a 404.
pub(super) async fn task_access(
    db: &impl GenericClient,
    identity: &DaemonIdentity,
    id: Uuid,
) -> Result<(Row, Uuid, Option<Uuid>), ApiError> {
    let task = db.query_opt(
        "SELECT q.*,i.workspace_id AS issue_workspace,cs.workspace_id AS chat_workspace,cs.creator_id AS chat_owner,ap.workspace_id AS autopilot_workspace,r.workspace_id AS runtime_workspace,r.daemon_id AS runtime_daemon,r.owner_id AS runtime_owner,r.provider AS runtime_provider FROM agent_task_queue q LEFT JOIN issue i ON i.id=q.issue_id LEFT JOIN chat_session cs ON cs.id=q.chat_session_id LEFT JOIN autopilot_run ar ON ar.id=q.autopilot_run_id LEFT JOIN autopilot ap ON ap.id=ar.autopilot_id LEFT JOIN agent_runtime r ON r.id=q.runtime_id WHERE q.id=$1",
        &[&id],
    ).await.map_err(failed)?.ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    let issue: Option<Uuid> = task.get("issue_workspace");
    let chat: Option<Uuid> = task.get("chat_workspace");
    let autopilot: Option<Uuid> = task.get("autopilot_workspace");
    let quick_create = if task.get::<_, Option<Uuid>>("issue_id").is_none()
        && task.get::<_, Option<Uuid>>("chat_session_id").is_none()
        && task.get::<_, Option<Uuid>>("autopilot_run_id").is_none()
    {
        task.get::<_, Option<Value>>("context")
            .filter(|c| c["type"] == "quick_create")
            .and_then(|c| {
                c["workspace_id"]
                    .as_str()
                    .and_then(|raw| Uuid::parse_str(raw).ok())
            })
    } else {
        None
    };
    let workspace = issue
        .or(chat)
        .or(autopilot)
        .or(quick_create)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "task not found"))?;
    daemon::workspace_access(db, identity, &workspace.to_string()).await?;
    if task.get::<_, Option<Uuid>>("runtime_workspace") != Some(workspace)
        || !binding(
            identity,
            task.get::<_, Option<String>>("runtime_daemon").as_deref(),
            task.get("runtime_owner"),
        )
    {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let owner = if issue.is_none() && chat.is_some() {
        task.get("chat_owner")
    } else {
        None
    };
    Ok((task, workspace, owner))
}

async fn prepare_lease(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path((runtime_raw, task_raw)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    let runtime = Uuid::parse_str(&runtime_raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid runtime_id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?;
    let rt = db
        .query_opt(
            "SELECT workspace_id,daemon_id,owner_id FROM agent_runtime WHERE id=$1",
            &[&runtime],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load runtime"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let runtime_workspace: Uuid = rt.get("workspace_id");
    daemon::workspace_access(&**db, &identity, &runtime_workspace.to_string()).await?;
    if !binding(
        &identity,
        rt.get::<_, Option<String>>("daemon_id").as_deref(),
        rt.get("owner_id"),
    ) {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    let id = task_id(&task_raw)?;
    let tx = db.transaction().await.map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM agent_runtime WHERE id=$1 FOR SHARE",
        &[&runtime],
    )
    .await
    .map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(failed)?;
    let (task, workspace, _) = task_access(&*tx, &identity, id).await?;
    if workspace != runtime_workspace
        || task.get::<_, Option<Uuid>>("runtime_id") != Some(runtime)
        || runtime_raw != runtime.to_string()
    {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let row = tx.query_opt("UPDATE agent_task_queue SET prepare_lease_expires_at=now()+interval '45 seconds' WHERE id=$1 AND runtime_id=$2 AND status IN ('dispatched','waiting_local_directory') AND started_at IS NULL RETURNING *", &[&id, &runtime]).await
        .map_err(|_| error(StatusCode::BAD_REQUEST, "extend task prepare lease: database operation failed"))?
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "extend task prepare lease: no rows in result set"))?;
    let response = super::runtime_chat_task::response(&row, workspace);
    tx.commit().await.map_err(failed)?;
    Ok(GoJson(response))
}

// json.Decoder accepts null, case-insensitive fields, unknown fields and
// repeated fields. Normalize in wire order: the last spelling must win.
struct RequestFields(Vec<(String, Value)>);
impl<'de> serde::Deserialize<'de> for RequestFields {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> serde::de::Visitor<'de> for Fields {
            type Value = RequestFields;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(RequestFields(Vec::new()))
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut fields = Vec::new();
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    fields.push((key.to_ascii_lowercase(), value));
                }
                Ok(RequestFields(fields))
            }
        }
        decoder.deserialize_any(Fields)
    }
}
fn request(bytes: &[u8], optional: bool) -> Result<Value, ApiError> {
    if bytes.is_empty() && optional {
        return Ok(Value::Null);
    }
    let mut values = serde_json::Deserializer::from_slice(bytes).into_iter::<RequestFields>();
    let parsed = values
        .next()
        .and_then(Result::ok)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let mut fields = serde_json::Map::new();
    for (key, value) in parsed.0 {
        let string_field = if optional {
            key == "reason"
        } else {
            key == "summary"
        };
        let int_field = !optional && matches!(key.as_str(), "step" | "total");
        if value.is_null() {
            continue;
        }
        if (string_field && !value.is_string()) || (int_field && value.as_i64().is_none()) {
            return Err(error(StatusCode::BAD_REQUEST, "invalid request body"));
        }
        if string_field || int_field {
            fields.insert(key, value);
        }
    }
    Ok(Value::Object(fields))
}
fn field<'a>(body: &'a Value, name: &str) -> Option<&'a Value> {
    body.get(name)
}
fn string(body: &Value, name: &str) -> Result<String, ApiError> {
    match field(body, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(error(StatusCode::BAD_REQUEST, "invalid request body")),
    }
}
fn integer(body: &Value, name: &str) -> Result<i64, ApiError> {
    match field(body, name) {
        None | Some(Value::Null) => Ok(0),
        Some(v) => v
            .as_i64()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body")),
    }
}

fn sanitize_reason(reason: &str) -> &str {
    let reason = reason.trim();
    if let Some(rest) = reason.strip_prefix("local_directory ") {
        let bytes = rest.as_bytes();
        if rest.starts_with('/')
            || rest.starts_with("\\\\")
            || (bytes.len() >= 3
                && bytes[0].is_ascii_alphabetic()
                && bytes[1] == b':'
                && matches!(bytes[2], b'/' | b'\\'))
        {
            return "";
        }
    }
    reason
}

async fn wait_local_directory(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let id = task_id(&raw)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let (original, workspace, owner) = task_access(&**db, &identity, id).await?;
    let body = request(&bytes, true)?;
    let reason = string(&body, "reason")?;
    let reason = sanitize_reason(&reason);
    let tx = db.transaction().await.map_err(failed)?;
    // Freeze runtime ownership, then use the agent-then-task lock order of
    // queue cancellation. Rebinding cannot borrow a previous authorization.
    let runtime: Option<Uuid> = original.get("runtime_id");
    tx.query_opt(
        "SELECT id FROM agent_runtime WHERE id=$1 FOR SHARE",
        &[&runtime],
    )
    .await
    .map_err(failed)?;
    tx.query_opt("SELECT id FROM agent WHERE id=(SELECT agent_id FROM agent_task_queue WHERE id=$1) FOR UPDATE", &[&id]).await.map_err(failed)?;
    tx.query_opt(
        "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(failed)?;
    let (before, current_workspace, current_owner) = task_access(&*tx, &identity, id).await?;
    if workspace != current_workspace
        || owner != current_owner
        || before.get::<_, Option<Uuid>>("runtime_id") != runtime
    {
        return Err(error(StatusCode::NOT_FOUND, "task not found"));
    }
    let row = tx.query_opt("UPDATE agent_task_queue SET status='waiting_local_directory',wait_reason=NULLIF($2,''),prepare_lease_expires_at=now()+interval '45 seconds' WHERE id=$1 AND status='dispatched' RETURNING *", &[&id, &reason]).await
        .map_err(|_| error(StatusCode::BAD_REQUEST, "mark task waiting_local_directory: database operation failed"))?
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "mark task waiting_local_directory: no rows in result set"))?;
    let agent: Uuid = row.get("agent_id");
    let changed = reconcile_agent(&*tx, agent).await?;
    let response = super::runtime_chat_task::response(&row, workspace);
    let mut payload = json!({"task_id":id,"agent_id":agent,"issue_id":before.get::<_, Option<Uuid>>("issue_id").map(|v| v.to_string()).unwrap_or_default(),"status":"waiting_local_directory"});
    let session: Option<Uuid> = row.get("chat_session_id");
    if let Some(session) = session {
        payload["chat_session_id"] = json!(session);
    }
    if !reason.is_empty() {
        payload["wait_reason"] = json!(reason);
    }
    tx.commit().await.map_err(failed)?;
    if let Some(agent_row) = changed {
        state.events.publish(system_event(
            "agent:status",
            agent_row.get("workspace_id"),
            None,
            None,
            json!({"agent":agent_snapshot(&agent_row)}),
        ));
    }
    state.events.publish(system_event(
        "task:waiting_local_directory",
        workspace,
        session,
        owner,
        payload,
    ));
    Ok(GoJson(response))
}

pub(super) async fn reconcile_agent(
    db: &impl GenericClient,
    agent: Uuid,
) -> Result<Option<Row>, ApiError> {
    db.query_opt("WITH desired AS (SELECT CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue WHERE agent_id=$1 AND status IN ('dispatched','running')) THEN 'working' ELSE 'idle' END AS status) UPDATE agent a SET status=desired.status,updated_at=now() FROM desired WHERE a.id=$1 AND a.status IS DISTINCT FROM desired.status RETURNING a.*", &[&agent]).await.map_err(failed)
}

pub(super) fn system_event(
    kind: &str,
    workspace: Uuid,
    session: Option<Uuid>,
    owner: Option<Uuid>,
    payload: Value,
) -> Event {
    Event {
        event_type: kind.into(),
        workspace_id: workspace,
        actor_type: "system".into(),
        actor_id: Uuid::nil(),
        chat_session_id: session,
        chat_session_owner_id: owner,
        payload,
    }
}

async fn progress(
    State(state): State<ApiState>,
    Extension(identity): Extension<DaemonIdentity>,
    Path(raw): Path<String>,
    bytes: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let body = request(&bytes, false)?;
    let summary = string(&body, "summary")?;
    let step = integer(&body, "step")?;
    let total = integer(&body, "total")?;
    let id = task_id(&raw)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load task"))?;
    let (task, _, _) = task_access(&**db, &identity, id).await?;
    // Frozen Go resolves progress through issue_id only. Chat/autopilot progress
    // is not promoted to workspace fanout; that would leak private summaries.
    if let Some(workspace) = task.get::<_, Option<Uuid>>("issue_workspace")
        && task.get::<_, Option<Uuid>>("chat_session_id").is_none()
    {
        state.events.publish(system_event(
            "task:progress",
            workspace,
            None,
            None,
            json!({"task_id":raw,"summary":summary,"step":step,"total":total}),
        ));
    }
    Ok(GoJson(json!({"status":"ok"})))
}

fn time(row: &Row, name: &str) -> Value {
    row.get::<_, Option<DateTime<Utc>>>(name)
        .map(|v| json!(v.to_rfc3339_opts(SecondsFormat::Secs, true)))
        .unwrap_or(Value::Null)
}
pub(super) fn agent_snapshot(row: &Row) -> Value {
    json!({"id":row.get::<_,Uuid>("id"),"workspace_id":row.get::<_,Uuid>("workspace_id"),"runtime_id":row.get::<_,Option<Uuid>>("runtime_id").map(|v|v.to_string()).unwrap_or_default(),"name":row.get::<_,String>("name"),"description":row.get::<_,String>("description"),"avatar_url":row.get::<_,Option<String>>("avatar_url"),"runtime_mode":row.get::<_,String>("runtime_mode"),"runtime_config":row.get::<_,Option<Value>>("runtime_config"),"visibility":row.get::<_,String>("visibility"),"status":row.get::<_,String>("status"),"max_concurrent_tasks":row.get::<_,i32>("max_concurrent_tasks"),"owner_id":row.get::<_,Option<Uuid>>("owner_id"),"skills":[],"created_at":time(row,"created_at"),"updated_at":time(row,"updated_at"),"archived_at":time(row,"archived_at"),"archived_by":row.get::<_,Option<Uuid>>("archived_by")})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wait_reason_drops_only_legacy_absolute_paths() {
        for s in [
            "local_directory /home/name/repo (held by task abcd)",
            r"local_directory C:\Users\name\repo",
            "local_directory d:/repo",
            r"local_directory \\host\share",
        ] {
            assert_eq!(sanitize_reason(s), "");
        }
        for s in [
            "repo (held by task abcd)",
            "local_directory (held by task abcd)",
            "local_directory relative",
            "local_directory 4:/repo",
        ] {
            assert_eq!(sanitize_reason(s), s);
        }
        assert_eq!(sanitize_reason("  repo  "), "repo");
    }
    #[test]
    fn requests_keep_go_null_unknown_and_case_rules() {
        assert!(request(b"", false).is_err());
        assert!(request(b"", true).unwrap().is_null());
        for raw in [b"[]".as_slice(), b"1", b"{broken"] {
            assert!(request(raw, true).is_err());
        }
        let b = request(
            br#"{"SUMMARY":null,"Step":-2,"total":3,"extra":true} {}"#,
            false,
        )
        .unwrap();
        assert_eq!(string(&b, "summary").unwrap(), "");
        assert_eq!(integer(&b, "step").unwrap(), -2);
        assert_eq!(integer(&b, "total").unwrap(), 3);
        let repeated = request(br#"{"Reason":"first","REASON":"last"}"#, true).unwrap();
        assert_eq!(string(&repeated, "reason").unwrap(), "last");
        assert!(integer(&json!({"step":1.5}), "step").is_err());
        assert!(string(&json!({"reason":1}), "reason").is_err());
    }
    #[test]
    fn binding_needs_current_owner_and_nonempty_daemon() {
        let owner = Uuid::new_v4();
        let other = Uuid::new_v4();
        assert!(binding(&DaemonIdentity::User(owner), None, Some(owner)));
        assert!(!binding(
            &DaemonIdentity::User(other),
            Some("daemon"),
            Some(owner)
        ));
        let token = DaemonIdentity::Bound {
            workspace: Uuid::new_v4(),
            daemon: "daemon".into(),
            owner: Some(owner),
            hash: "hash".into(),
        };
        assert!(binding(&token, Some("daemon"), Some(owner)));
        assert!(!binding(&token, Some("daemon"), Some(other)));
        assert!(!binding(&token, None, Some(owner)));
    }
}
