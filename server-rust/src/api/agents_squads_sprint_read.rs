//! Workspace-scoped agent inventory. Viewing an agent never grants invocation rights.
use super::*;
use axum::extract::RawQuery;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio_postgres::IsolationLevel;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agents", axum::routing::get(list))
        .route("/api/agents/", axum::routing::get(list))
        .route("/api/agents/{id}", axum::routing::get(detail))
        .route("/api/agents/{id}/", axum::routing::get(detail))
}

fn can_view(data: &Value, targets: &[Value], user: Uuid, role: &str, actor: &Actor) -> bool {
    if matches!(actor, Actor::Task(_)) || matches!(role, "owner" | "admin") {
        return true;
    }
    if data["owner_id"] == json!(user) {
        return true;
    }
    data["permission_mode"] == "public_to"
        && targets.iter().any(|target| {
            target["target_type"] == "workspace"
                || (target["target_type"] == "member" && target["target_id"] == json!(user))
        })
}

fn runtime_availability(
    status: &str,
    seen: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> &'static str {
    if status == "online" {
        "online"
    } else if seen.is_some_and(|seen| now.signed_duration_since(seen).num_seconds() < 300) {
        "unstable"
    } else {
        "offline"
    }
}

fn project_runtime(
    data: &Value,
    runtime: Option<&Row>,
    user: Uuid,
    role: &str,
    now: DateTime<Utc>,
) -> Option<&'static str> {
    if matches!(role, "owner" | "admin") || !data["archived_at"].is_null() {
        return None;
    }
    let rt = runtime?;
    if data["workspace_id"] != json!(rt.get::<_, Uuid>("workspace_id")) {
        return None;
    }
    let owner: Option<Uuid> = rt.get("owner_id");
    let visibility: String = rt.get("visibility");
    let shared: Vec<Uuid> = rt.get("shared_with");
    if visibility == "public"
        || owner == Some(user)
        || (owner.is_some() && visibility == "private" && shared.contains(&user))
    {
        return None;
    }
    Some(runtime_availability(
        &rt.get::<_, String>("status"),
        rt.get("last_seen_at"),
        now,
    ))
}

// A repeatable-read transaction keeps permissions and their targets in one DB
// snapshot. Batched summaries avoid one query per agent and never read SKILL.md.
async fn read_agents(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    id: Option<Uuid>,
    include_archived: bool,
) -> Result<Value, ApiError> {
    if matches!(actor, Actor::Cloud | Actor::Daemon) {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| failed("failed to load agents"))?;
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(|_| failed("failed to start agent read"))?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| failed("failed to check agent permissions"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role: String = member.get("role");
    let workspace = tx
        .query_one("SELECT settings FROM workspace WHERE id=$1", &[&ws])
        .await
        .map_err(|_| failed("failed to load workspace settings"))?;
    let settings: Value = workspace.get("settings");
    let always_redact = settings["always_redact_env"] == true;
    let rows = if let Some(id) = id {
        tx.query("SELECT to_jsonb(a) AS data FROM agent a WHERE a.id=$1 AND a.workspace_id=$2 AND a.kind='user'", &[&id, &ws]).await
    } else if include_archived {
        tx.query("SELECT to_jsonb(a) AS data FROM agent a WHERE a.workspace_id=$1 AND a.kind='user' ORDER BY a.created_at,a.id", &[&ws]).await
    } else {
        tx.query("SELECT to_jsonb(a) AS data FROM agent a WHERE a.workspace_id=$1 AND a.kind='user' AND a.archived_at IS NULL ORDER BY a.created_at,a.id", &[&ws]).await
    }.map_err(|_| failed("failed to load agents"))?;
    if id.is_some() && rows.is_empty() {
        return Err(error(StatusCode::NOT_FOUND, "agent not found"));
    }
    let ids: Vec<Uuid> = rows
        .iter()
        .map(|r| {
            serde_json::from_value(r.get::<_, Value>("data")["id"].clone())
                .map_err(|_| failed("invalid stored agent id"))
        })
        .collect::<Result<_, _>>()?;
    if ids.is_empty() {
        return Ok(json!([]));
    }
    let targets = tx.query("SELECT agent_id,target_type,target_id FROM agent_invocation_target WHERE agent_id=ANY($1) ORDER BY created_at,id", &[&ids])
        .await.map_err(|_| failed("failed to load agent invocation targets"))?;
    let mut target_map: HashMap<Uuid, Vec<Value>> = HashMap::new();
    for row in targets {
        target_map
            .entry(row.get("agent_id"))
            .or_default()
            .push(json!({
                "target_type":row.get::<_, String>("target_type"),
                "target_id":row.get::<_, Option<Uuid>>("target_id")
            }));
    }
    let skills = tx.query("SELECT b.agent_id,s.id,s.name,s.description,b.enabled FROM agent_skill b JOIN skill s ON s.id=b.skill_id WHERE b.agent_id=ANY($1) AND s.workspace_id=$2 ORDER BY s.name,s.id", &[&ids,&ws])
        .await.map_err(|_| failed("failed to load agent skills"))?;
    let mut skill_map: HashMap<Uuid, Vec<Value>> = HashMap::new();
    for row in skills {
        skill_map
            .entry(row.get("agent_id"))
            .or_default()
            .push(json!({
                "id":row.get::<_, Uuid>("id"),
                "name":row.get::<_, String>("name"),
                "description":row.get::<_, String>("description"),
                "enabled":row.get::<_, bool>("enabled")
            }));
    }
    let runtime_ids: Vec<Uuid> = if matches!(role.as_str(), "owner" | "admin") {
        Vec::new()
    } else {
        rows.iter()
            .filter_map(|row| {
                let data: Value = row.get("data");
                if data["archived_at"].is_null() {
                    serde_json::from_value::<Uuid>(data["runtime_id"].clone()).ok()
                } else {
                    None
                }
            })
            .collect()
    };
    let runtimes = if runtime_ids.is_empty() {
        Vec::new()
    } else {
        tx.query("SELECT id,workspace_id,owner_id,visibility,shared_with,status,last_seen_at FROM agent_runtime WHERE workspace_id=$1 AND id=ANY($2)", &[&ws,&runtime_ids])
            .await.map_err(|_| failed("failed to load agent runtime availability"))?
    };
    let runtime_map: HashMap<Uuid, &Row> = runtimes.iter().map(|r| (r.get("id"), r)).collect();
    let now = Utc::now();
    let mut visible = Vec::new();
    for row in rows {
        let data: Value = row.get("data");
        let agent_id: Uuid = serde_json::from_value(data["id"].clone())
            .map_err(|_| failed("invalid stored agent id"))?;
        let targets = target_map.remove(&agent_id).unwrap_or_default();
        if !can_view(&data, &targets, user, &role, &actor) {
            if id.is_some() {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "you do not have access to this agent",
                ));
            }
            continue;
        }
        let mut out = settings::public_agent(
            &data,
            json!(targets),
            json!(skill_map.remove(&agent_id).unwrap_or_default()),
        );
        // A product-defined system agent carries its read-only prompt from the
        // binary; public_agent adds `system_instructions` for it below. Nothing
        // is written to the row, so a release updates the prompt in place.
        let runtime_id = serde_json::from_value::<Uuid>(data["runtime_id"].clone()).ok();
        if let Some(bucket) = project_runtime(
            &data,
            runtime_id.and_then(|id| runtime_map.get(&id).copied()),
            user,
            &role,
            now,
        ) {
            out["runtime_availability"] = json!(bucket);
        }
        out["mcp_config_redacted"] = json!(false);
        if !data["mcp_config"].is_null() {
            if always_redact
                || matches!(actor, Actor::Task(_))
                || (data["owner_id"] != json!(user) && !matches!(role.as_str(), "owner" | "admin"))
            {
                out["mcp_config_redacted"] = json!(true);
            } else {
                out["mcp_config"] = data["mcp_config"].clone();
            }
        }
        // Until feature-flag and provider wiring is supplied, never expose
        // another owner's integration footprint (nor claim an enabled overlay).
        out.as_object_mut()
            .unwrap()
            .remove("composio_toolkit_allowlist_redacted");
        visible.push(out);
    }
    if id.is_some() {
        Ok(visible.remove(0))
    } else {
        Ok(json!(visible))
    }
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let include_archived = raw.as_deref().is_some_and(|query| {
        form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == "include_archived")
            .is_some_and(|(_, value)| value == "true")
    });
    Ok(GoJson(
        read_agents(state, ws, user, actor, None, include_archived).await?,
    ))
}

async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let id = parse(&raw, "invalid agent id")?;
    Ok(GoJson(
        read_agents(state, ws, user, actor, Some(id), true).await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn runtime_buckets_match_five_minute_boundary() {
        let now = Utc::now();
        let recent = now - chrono::Duration::seconds(299);
        let expired = now - chrono::Duration::seconds(300);
        assert_eq!(runtime_availability("online", None, now), "online");
        assert_eq!(
            runtime_availability("offline", Some(recent), now),
            "unstable"
        );
        assert_eq!(
            runtime_availability("offline", Some(expired), now),
            "offline"
        );
        assert_eq!(runtime_availability("offline", None, now), "offline");
    }

    #[test]
    fn public_projection_masks_secrets_and_normalizes_wire_defaults() {
        let data = json!({"id":Uuid::now_v7(),"runtime_id":null,"runtime_config":{"gateway":{"token":"synthetic-secret"}},"custom_env":{"API_TOKEN":"secret"},"mcp_config":{"token":"secret"},"future_secret":"secret","created_at":"2026-11-02T12:00:00.123456+00:00","permission_mode":"private"});
        let out = settings::public_agent(&data, json!([]), json!([]));
        assert!(!out["runtime_bound"].as_bool().unwrap());
        assert_eq!(out["runtime_id"], "");
        assert_eq!(out["runtime_config"]["gateway"]["token"], "***");
        assert!(out["mcp_config"].is_null());
        assert_eq!(out["custom_args"], json!([]));
        assert_eq!(out["conversation_starters"], json!([]));
        assert_eq!(out["has_custom_env"], true);
        assert_eq!(out["custom_env_key_count"], 1);
        assert!(out.get("custom_env").is_none());
        assert!(out.get("future_secret").is_none());
        assert_eq!(
            data["runtime_config"]["gateway"]["token"],
            "synthetic-secret"
        );
        let parsed = DateTime::parse_from_rfc3339(out["created_at"].as_str().unwrap()).unwrap();
        assert_eq!(parsed.timestamp_subsec_nanos(), 0);
    }
    #[test]
    fn view_is_not_invocation_and_teams_are_inert() {
        let owner = Uuid::now_v7();
        let viewer = Uuid::now_v7();
        let private = json!({"owner_id":owner,"permission_mode":"private"});
        assert!(can_view(&private, &[], viewer, "admin", &Actor::Human));
        assert!(!can_view(&private, &[], viewer, "member", &Actor::Human));
        assert!(can_view(
            &private,
            &[],
            viewer,
            "member",
            &Actor::Task(crate::api::actor::TaskIdentity {
                user: viewer,
                agent: owner,
                task: Uuid::now_v7(),
                workspace: Uuid::now_v7()
            })
        ));
        let public = json!({"owner_id":owner,"permission_mode":"public_to"});
        assert!(!can_view(
            &public,
            &[json!({"target_type":"team","target_id":viewer})],
            viewer,
            "member",
            &Actor::Human
        ));
        assert!(can_view(
            &public,
            &[json!({"target_type":"member","target_id":viewer})],
            viewer,
            "member",
            &Actor::Human
        ));
    }
}
