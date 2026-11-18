//! Workspace quick-action catalog. Private prompts belong to their creator.
use super::{ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    extract::{Extension, RawQuery, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Serialize)]
struct QuickAction {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    description: String,
    assignee_type: String,
    assignee_id: String,
    prompt: String,
    visibility: String,
    status: String,
    last_used_at: Option<String>,
    use_count: i64,
    created_by_id: Uuid,
    created_at: String,
    updated_at: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    target_name: String,
    target_public: bool,
    target_missing: bool,
}

fn include_archived(raw: Option<&str>) -> bool {
    raw.into_iter()
        .flat_map(|raw| form_urlencoded::parse(raw.as_bytes()))
        .find(|(key, _)| key == "include_archived")
        .is_some_and(|(_, value)| value == "true")
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<serde_json::Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list quick actions",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db.query(
        "SELECT * FROM quick_action WHERE workspace_id=$1 AND ($2 OR status='active') AND (visibility='public' OR created_by_id=$3) ORDER BY use_count DESC, lower(name) ASC",
        &[&ws, &include_archived(raw.as_deref()), &user],
    ).await.map_err(|_| fail())?;

    // The frozen Go catalog degrades display metadata on query failures. A
    // missing target never leaks its former name or permissions to the viewer.
    let mut agents: HashMap<Uuid, (String, String)> = HashMap::new();
    let mut squads: HashMap<Uuid, (String, Uuid)> = HashMap::new();
    let mut public_agents: HashSet<Uuid> = HashSet::new();
    if let Ok(agent_rows) = db.query("SELECT id,name,permission_mode FROM agent WHERE workspace_id=$1 AND archived_at IS NULL AND kind='user' ORDER BY created_at", &[&ws]).await {
        for agent in &agent_rows {
            agents.insert(agent.get("id"), (agent.get("name"), agent.get("permission_mode")));
        }
        if let Ok(squad_rows) = db.query("SELECT id,name,leader_id FROM squad WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY created_at", &[&ws]).await {
            for squad in &squad_rows {
                squads.insert(squad.get("id"), (squad.get("name"), squad.get("leader_id")));
            }
        }
        let ids: Vec<Uuid> = agent_rows.iter().map(|a| a.get("id")).collect();
        if !ids.is_empty()
            && let Ok(targets) = db.query("SELECT agent_id,target_type FROM agent_invocation_target WHERE agent_id=ANY($1::uuid[]) ORDER BY agent_id,target_type,created_at", &[&ids]).await {
            for target in targets {
                if target.get::<_, String>("target_type") == "workspace" {
                    public_agents.insert(target.get("agent_id"));
                }
            }
        }
    }
    let actions: Vec<_> = rows
        .iter()
        .map(|row| response(row, &agents, &squads, &public_agents))
        .collect();
    Ok(GoJson(serde_json::json!({"quick_actions": actions})))
}

fn response(
    row: &Row,
    agents: &HashMap<Uuid, (String, String)>,
    squads: &HashMap<Uuid, (String, Uuid)>,
    public_agents: &HashSet<Uuid>,
) -> QuickAction {
    let assignee_type: String = row.get("assignee_type");
    let assignee_id: Option<Uuid> = row.get("assignee_id");
    let target = match assignee_type.as_str() {
        "" => Some((String::new(), None)),
        "squad" => assignee_id
            .and_then(|id| squads.get(&id))
            .and_then(|(name, leader)| {
                agents
                    .contains_key(leader)
                    .then(|| (name.clone(), Some(*leader)))
            }),
        _ => assignee_id.and_then(|id| agents.get(&id).map(|(name, _)| (name.clone(), Some(id)))),
    };
    let target_public = match &target {
        Some((_, None)) => true,
        Some((_, Some(id))) => {
            public_agents.contains(id)
                && agents.get(id).is_some_and(|(_, mode)| mode == "public_to")
        }
        None => false,
    };
    QuickAction {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        description: row.get("description"),
        assignee_type,
        assignee_id: assignee_id.map_or_else(String::new, |id| id.to_string()),
        prompt: row.get("prompt"),
        visibility: row.get("visibility"),
        status: row.get("status"),
        last_used_at: row
            .get::<_, Option<DateTime<Utc>>>("last_used_at")
            .map(timestamp),
        use_count: row.get("use_count"),
        created_by_id: row.get("created_by_id"),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
        target_name: target
            .as_ref()
            .map_or_else(String::new, |(name, _)| name.clone()),
        target_public,
        target_missing: target.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn include_archived_uses_first_query_value() {
        assert!(!include_archived(None));
        assert!(!include_archived(Some(
            "include_archived=false&include_archived=true"
        )));
        assert!(include_archived(Some(
            "include_archived=true&include_archived=false"
        )));
        assert!(!include_archived(Some("include_archived=TRUE")));
    }
}
