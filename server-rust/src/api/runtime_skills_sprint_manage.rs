//! Human runtime metadata mutations. No daemon, task, or agent binding side effects.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    runtime_skills_sprint_control::runtime_id,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::patch,
};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeMetadataEvent {
    pub event_type: &'static str,
    pub actor_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_id: Uuid,
    pub action: &'static str,
}

pub trait RuntimeMetadataEventSink: Send + Sync {
    fn publish(&self, event: RuntimeMetadataEvent);
}

pub type SharedRuntimeMetadataEventSink = Arc<dyn RuntimeMetadataEventSink>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/runtimes/{runtimeId}/", patch(update))
}

#[derive(Default)]
struct UpdateRequest {
    visibility: Option<String>,
    shared_with: Option<Vec<String>>,
    custom_name: Option<String>,
    apply_to_machine: bool,
}

// Go's Decoder reads one JSON object, folds ASCII key case, treats null pointer
// fields as absent, ignores unknown fields, and accepts trailing JSON values.
fn decode(body: &[u8]) -> Result<UpdateRequest, ApiError> {
    struct Seed;
    impl<'de> DeserializeSeed<'de> for Seed {
        type Value = UpdateRequest;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            decoder: D,
        ) -> Result<Self::Value, D::Error> {
            decoder.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Seed {
        type Value = UpdateRequest;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("object or null")
        }
        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(UpdateRequest::default())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut req = UpdateRequest::default();
            while let Some(key) = map.next_key::<String>()? {
                if key.eq_ignore_ascii_case("visibility") {
                    req.visibility = map.next_value::<Option<String>>()?;
                } else if key.eq_ignore_ascii_case("shared_with") {
                    req.shared_with = map.next_value::<Option<Vec<String>>>()?;
                } else if key.eq_ignore_ascii_case("custom_name") {
                    req.custom_name = map.next_value::<Option<String>>()?;
                } else if key.eq_ignore_ascii_case("apply_to_machine") {
                    if let Some(value) = map.next_value::<Option<bool>>()? {
                        req.apply_to_machine = value;
                    }
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            Ok(req)
        }
    }
    Seed.deserialize(&mut serde_json::Deserializer::from_slice(body))
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid JSON body"))
}

fn failure() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to update runtime",
    )
}

fn response(status: StatusCode, payload: Value) -> Response {
    let mut response = GoJson(payload).into_response();
    *response.status_mut() = status;
    response
}

fn editable(role: &str, owner: Option<Uuid>, user: Uuid) -> bool {
    matches!(role, "owner" | "admin") || owner == Some(user)
}

fn strip_sensitive_keys(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|key, _| {
                let key = key.to_ascii_lowercase();
                ![
                    "token",
                    "secret",
                    "password",
                    "credential",
                    "authorization",
                    "cookie",
                    "api_key",
                    "apikey",
                    "private_key",
                    "env",
                    "headers",
                ]
                .iter()
                .any(|needle| key.contains(needle))
            });
            for value in map.values_mut() {
                strip_sensitive_keys(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                strip_sensitive_keys(value);
            }
        }
        _ => {}
    }
}

fn runtime_view(row: &Row) -> Value {
    // Bound recursion and scrub text before removing credential-bearing metadata keys.
    let mut view = super::runtime_chat_redact::input_map(&super::runtime::runtime_row(row));
    strip_sensitive_keys(&mut view["metadata"]);
    view
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    actor.require_human()?;
    let id = runtime_id(&raw)?;
    let mut db = state.pool.get().await.map_err(|_| failure())?;
    // Preflight authorization before decoding, as Go does. The transaction below
    // repeats it under row locks; this read alone never authorizes a write.
    let prior = db
        .query_opt(
            "SELECT workspace_id, owner_id FROM agent_runtime WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    if prior.get::<_, Uuid>("workspace_id") != ws {
        return Err(error(StatusCode::NOT_FOUND, "runtime not found"));
    }
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?
        .get::<_, String>("role");
    if !editable(&role, prior.get("owner_id"), user) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you can only edit your own runtimes",
        ));
    }
    let req = decode(&body)?;
    if req
        .visibility
        .as_deref()
        .is_some_and(|v| !matches!(v, "private" | "public"))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "visibility must be 'private' or 'public'",
        ));
    }
    if req
        .custom_name
        .as_deref()
        .is_some_and(|name| name.trim().chars().count() > 100)
    {
        return Err(error(StatusCode::BAD_REQUEST, "custom name is too long"));
    }
    let mut recipients = Vec::new();
    if let Some(shared) = &req.shared_with {
        if shared.len() > 1000 {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "shared_with has too many members",
            ));
        }
        for raw in shared {
            recipients.push(
                Uuid::parse_str(raw)
                    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid shared_with"))?,
            );
        }
    }

    let tx = db.transaction().await.map_err(|_| failure())?;
    // Lock all machine rows in one stable order before editing any row, avoiding
    // a rename-versus-rename lock cycle on two providers of the same daemon.
    let candidates = if req.custom_name.is_some() && req.apply_to_machine {
        tx.query_opt(
            "SELECT daemon_id FROM agent_runtime WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?
        .get::<_, Option<String>>("daemon_id")
    } else {
        None
    };
    let locked = if let Some(daemon) = &candidates {
        tx.query("SELECT * FROM agent_runtime WHERE workspace_id=$1 AND daemon_id=$2 ORDER BY id FOR NO KEY UPDATE", &[&ws, daemon])
            .await.map_err(|_| failure())?
    } else {
        tx.query(
            "SELECT * FROM agent_runtime WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| failure())?
    };
    let rt = locked
        .iter()
        .find(|row| row.get::<_, Uuid>("id") == id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?;
    let role = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failure())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "runtime not found"))?
        .get::<_, String>("role");
    let owner: Option<Uuid> = rt.get("owner_id");
    if !editable(&role, owner, user) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you can only edit your own runtimes",
        ));
    }
    let mut current = rt.clone();
    let mut changed = false;
    if req.visibility.is_some() || req.shared_with.is_some() {
        let previous_visibility: String = current.get("visibility");
        let previous_shared: Vec<Option<Uuid>> = current.get("shared_with");
        let next_visibility = req.visibility.as_deref().unwrap_or(&previous_visibility);
        let mut next_shared: Vec<Uuid> = if req.visibility.is_some() {
            Vec::new()
        } else {
            previous_shared.into_iter().flatten().collect()
        };
        if req.shared_with.is_some() {
            next_shared = recipients
                .into_iter()
                .filter(|recipient| Some(*recipient) != owner)
                .collect();
            next_shared.sort_unstable();
            next_shared.dedup();
        }
        if next_visibility == "public" && !next_shared.is_empty() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "shared_with requires private visibility",
            ));
        }
        if !next_shared.is_empty() {
            let members = tx.query(
                "SELECT user_id FROM member WHERE workspace_id=$1 AND user_id=ANY($2::uuid[]) ORDER BY user_id FOR SHARE",
                &[&ws, &next_shared],
            ).await.map_err(|_| failure())?;
            if members.len() != next_shared.len() {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "every recipient must be a workspace member",
                ));
            }
        }
        let old_shared: Vec<Uuid> = current
            .get::<_, Vec<Option<Uuid>>>("shared_with")
            .into_iter()
            .flatten()
            .collect();
        let same = previous_visibility == next_visibility && old_shared == next_shared;
        if !same {
            if owner != Some(user) {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "only the runtime owner can change its visibility",
                ));
            }
            if current.get::<_, String>("provider") == "qoder_cloud" {
                let managed: bool = tx.query_one(
                    "SELECT EXISTS(SELECT 1 FROM cloud_agent_connection WHERE provider='qoder_cloud' AND workspace_id=$1)",
                    &[&ws],
                ).await.map_err(|_| failure())?.get(0);
                if managed {
                    return Ok(response(
                        StatusCode::CONFLICT,
                        json!({"error":"the Qoder runtime is always shared with the workspace","code":"runtime_managed_by_connection"}),
                    ));
                }
            }
            let narrows = next_visibility != "public"
                && (previous_visibility == "public"
                    || old_shared.iter().any(|recipient| {
                        Some(*recipient) != owner && !next_shared.contains(recipient)
                    }));
            if narrows {
                let agents = tx.query(
                    "SELECT id,name FROM agent WHERE runtime_id=$1 AND owner_id IS DISTINCT FROM $2::uuid AND NOT COALESCE(owner_id=ANY($3::uuid[]),false) ORDER BY name ASC",
                    &[&id, &owner, &next_shared],
                ).await.map_err(|_| failure())?;
                let tasks = tx.query(
                    "SELECT task.id FROM agent_task_queue task JOIN agent a ON a.id=task.agent_id WHERE task.runtime_id=$1 AND a.owner_id IS DISTINCT FROM $2::uuid AND NOT COALESCE(a.owner_id=ANY($3::uuid[]),false) AND task.status IN ('queued','dispatched','running','waiting_local_directory','deferred') ORDER BY task.id",
                    &[&id, &owner, &next_shared],
                ).await.map_err(|_| failure())?;
                if !agents.is_empty() || !tasks.is_empty() {
                    return Ok(response(
                        StatusCode::CONFLICT,
                        json!({
                            "error":"restricting runtime access will unbind affected agents, cancel their active runs, and pause their active autopilots; review and confirm first.",
                            "code":"runtime_has_nonowner_dependents",
                            "nonowner_agents":agents.iter().map(|a| json!({"id":a.get::<_,Uuid>("id"),"name":a.get::<_,String>("name")})).collect::<Vec<_>>(),
                            "active_task_ids":tasks.iter().map(|t| t.get::<_,Uuid>("id")).collect::<Vec<_>>(),
                        }),
                    ));
                }
            }
            current = tx.query_one(
                "UPDATE agent_runtime SET visibility=$2, shared_with=$3::uuid[], updated_at=now() WHERE id=$1 AND workspace_id=$4 RETURNING *",
                &[&id, &next_visibility, &next_shared, &ws],
            ).await.map_err(|_| failure())?;
            changed = true;
        }
    }
    if let Some(custom_name) = req.custom_name {
        let name = custom_name.trim();
        let value = if name.is_empty() { None } else { Some(name) };
        if let Some(daemon) = candidates {
            let owner_filter = if matches!(role.as_str(), "owner" | "admin") {
                None
            } else {
                Some(user)
            };
            let rows = tx.query("UPDATE agent_runtime SET custom_name=$1,updated_at=now() WHERE workspace_id=$2 AND daemon_id=$3 AND ($4::uuid IS NULL OR owner_id=$4) RETURNING *",
                &[&value, &ws, &daemon, &owner_filter]).await.map_err(|_| failure())?;
            if let Some(row) = rows.into_iter().find(|row| row.get::<_, Uuid>("id") == id) {
                current = row;
                changed = true;
            } else {
                return Err(error(StatusCode::CONFLICT, "runtime changed; retry"));
            }
        } else {
            current = tx.query_one("UPDATE agent_runtime SET custom_name=$1,updated_at=now() WHERE id=$2 AND workspace_id=$3 RETURNING *",
                &[&value, &id, &ws]).await.map_err(|_| failure())?;
            changed = true;
        }
    }
    tx.commit().await.map_err(|_| failure())?;
    if changed {
        state
            .runtime_metadata_events
            .as_ref()
            .expect("runtime metadata routes require event sink")
            .publish(RuntimeMetadataEvent {
                event_type: "daemon:register",
                actor_type: "member",
                workspace_id: ws,
                actor_id: user,
                action: "update",
            });
    }
    Ok(response(StatusCode::OK, runtime_view(&current)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn update_payload_matches_pointer_and_name_contract() {
        let request = decode(br#"{"VISIBILITY":"private","visibility":null,"CUSTOM_NAME":" hello ","apply_to_machine":true,"other":42} trailing"#).unwrap();
        assert!(request.visibility.is_none());
        assert_eq!(request.custom_name.as_deref(), Some(" hello "));
        assert!(request.apply_to_machine);
        assert!(decode(br#"{"custom_name":22}"#).is_err());
        assert!(decode(br#"[]"#).is_err());
    }

    #[test]
    fn credential_keys_in_daemon_metadata_are_removed() {
        let mut metadata = json!({"hostname":"test","AUTH_TOKEN":"synthetic-secret","nested":{"db_password":"synthetic-secret","safe":1},"items":[{"headers":{"authorization":"synthetic-secret"},"ok":true}]});
        strip_sensitive_keys(&mut metadata);
        assert_eq!(
            metadata,
            json!({"hostname":"test","nested":{"safe":1},"items":[{"ok":true}]})
        );
    }
}
