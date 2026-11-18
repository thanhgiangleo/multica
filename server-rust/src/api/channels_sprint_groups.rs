//! DingTalk group inventory. Presence is observation, never an agent routing grant.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    channels_sprint::{self, Channel, Params, admin, channel_time, db_error, id, membership},
    error,
};
use axum::{
    Router,
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    routing::{delete, get},
};
use chrono::{Duration, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use tokio_postgres::GenericClient;
use uuid::Uuid;

const VISIBLE: &str = "WITH visible AS (SELECT ci.id,ci.agent_id,ci.installed_at FROM channel_installation ci LEFT JOIN agent a ON a.id=ci.agent_id AND a.workspace_id=ci.workspace_id WHERE ci.workspace_id=$1 AND ci.channel_type='dingtalk' AND ci.status='active' AND ($3::bool OR (a.id IS NOT NULL AND (a.owner_id=$2 OR (a.permission_mode='public_to' AND EXISTS (SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND ((t.target_type='workspace' AND t.target_id=$1) OR (t.target_type='member' AND t.target_id=$2))))))) AND ($4::uuid IS NULL OR ci.agent_id=$4)) ";
const ACTIVE_WINDOW_DAYS: i64 = 90;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/workspaces/{id}/dingtalk/groups", get(list_workspace))
        .route(
            "/api/workspaces/{id}/dingtalk/installations/{installation}/groups/{conversation}",
            delete(forget),
        )
        .route("/api/agents/{id}/dingtalk/groups", get(list_agent))
}

#[derive(Default, Deserialize)]
struct Filters {
    activity: Option<String>,
    installation_id: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

struct InventoryFilter {
    inactive: bool,
    installation: Option<Uuid>,
    limit: i64,
    offset: i64,
}

impl InventoryFilter {
    fn parse(q: Filters) -> Result<Self, ApiError> {
        let inactive = match q.activity.as_deref().map(str::trim) {
            None | Some("") => false,
            Some("inactive") => true,
            _ => {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "activity must be inactive when provided",
                ));
            }
        };
        if !inactive {
            return Ok(Self {
                inactive,
                installation: None,
                limit: 0,
                offset: 0,
            });
        }
        let installation = q
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "installation_id is required for inactive groups",
                )
            })?;
        let installation = Uuid::parse_str(installation)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid installation_id"))?;
        let limit = match q.limit.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) => raw
                .parse::<i64>()
                .ok()
                .filter(|n| (1..=100).contains(n))
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "limit must be between 1 and 100"))?,
            None => 20,
        };
        let offset = match q.offset.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) => raw
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0 && n.checked_add(limit).is_some())
                .ok_or_else(|| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "offset must be a non-negative integer",
                    )
                })?,
            None => 0,
        };
        Ok(Self {
            inactive,
            installation: Some(installation),
            limit,
            offset,
        })
    }
}

async fn list_workspace(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    Query(q): Query<Filters>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let workspace = id(&params, "id")?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let role = membership(&**db, workspace, user).await?;
    if channels_sprint::runtime(&state, Channel::DingTalk).is_err() {
        return Ok(GoJson(empty_inventory()));
    }
    let filter = InventoryFilter::parse(q)?;
    Ok(GoJson(
        inventory(&**db, workspace, user, admin(&role), None, filter).await?,
    ))
}

async fn list_agent(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
    Query(q): Query<Filters>,
) -> Result<GoJson<Value>, ApiError> {
    actor.require_human()?;
    let agent_id = id(&params, "id")?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let agent = db
        .query_opt(
            "SELECT workspace_id FROM agent WHERE id=$1 AND kind='user'",
            &[&agent_id],
        )
        .await
        .map_err(db_error)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "agent not found"))?;
    let workspace: Uuid = agent.get("workspace_id");
    let role = membership(&**db, workspace, user).await?;
    let owner = db
        .query_one(
            "SELECT owner_id,permission_mode FROM agent WHERE id=$1 AND workspace_id=$2",
            &[&agent_id, &workspace],
        )
        .await
        .map_err(db_error)?;
    let can_view = admin(&role) || owner.get::<_, Uuid>("owner_id") == user ||
        (owner.get::<_, String>("permission_mode") == "public_to" &&
        db.query_opt("SELECT 1 FROM agent_invocation_target WHERE agent_id=$1 AND ((target_type='workspace' AND target_id=$2) OR (target_type='member' AND target_id=$3)) LIMIT 1", &[&agent_id,&workspace,&user]).await.map_err(db_error)?.is_some());
    if !can_view {
        return Err(error(
            StatusCode::FORBIDDEN,
            "you do not have access to this agent",
        ));
    }
    if channels_sprint::runtime(&state, Channel::DingTalk).is_err() {
        return Ok(GoJson(empty_inventory()));
    }
    let filter = InventoryFilter::parse(q)?;
    // No installation is required for an accessible agent; empty is a valid inventory.
    Ok(GoJson(
        inventory(&**db, workspace, user, admin(&role), Some(agent_id), filter).await?,
    ))
}

fn empty_inventory() -> Value {
    json!({"groups":[],"group_discovery_supported":true,"inactive_group_counts":{},"bot_identities":{}})
}

async fn inventory<C: GenericClient + Sync>(
    db: &C,
    workspace: Uuid,
    user: Uuid,
    is_admin: bool,
    agent: Option<Uuid>,
    filter: InventoryFilter,
) -> Result<Value, ApiError> {
    let since = Utc::now() - Duration::days(ACTIVE_WINDOW_DAYS);
    let authorized = if let Some(installation) = filter.installation {
        db.query_opt(
            &format!("{VISIBLE} SELECT 1 FROM visible WHERE id=$5 LIMIT 1"),
            &[&workspace, &user, &is_admin, &agent, &installation],
        )
        .await
        .map_err(db_error)?
        .is_some()
    } else {
        true
    };
    let mut out = empty_inventory();
    let rows = if authorized {
        db.query(&format!("{VISIBLE} SELECT p.conversation_id,p.conversation_title,p.installation_id,v.agent_id,COALESCE(b.bot_name,p.bot_name) AS bot_name,COALESCE(b.bot_identity_issue,p.bot_identity_issue) AS bot_identity_issue,p.last_active_at,p.mention_count FROM dingtalk_group_presence p JOIN visible v ON v.id=p.installation_id LEFT JOIN dingtalk_bot_identity b ON b.installation_id=p.installation_id AND b.workspace_id=$1 WHERE p.workspace_id=$1 AND ($5::uuid IS NULL OR p.installation_id=$5) AND (CASE WHEN $6::bool THEN (p.last_active_at < $7 OR p.last_active_at IS NULL) ELSE p.last_active_at >= $7 END) ORDER BY p.last_active_at DESC NULLS LAST,p.conversation_title,p.conversation_id,v.installed_at,v.id LIMIT NULLIF($8::bigint,0) OFFSET $9"), &[&workspace,&user,&is_admin,&agent,&filter.installation,&filter.inactive,&since,&(if filter.inactive {filter.limit+1} else {0}),&filter.offset]).await.map_err(db_error)?
    } else {
        Vec::new()
    };
    if filter.inactive && rows.len() > filter.limit as usize {
        out["next_offset"] = json!(filter.offset + filter.limit);
    }
    let mut groups: Vec<Value> = Vec::new();
    let mut indexes = HashMap::<String, usize>::new();
    for row in rows.iter().take(if filter.inactive {
        filter.limit as usize
    } else {
        rows.len()
    }) {
        let conversation: String = row.get("conversation_id");
        let title: String = row.get("conversation_title");
        let idx = *indexes.entry(conversation.clone()).or_insert_with(|| {
            let index = groups.len();
            groups
                .push(json!({"conversation_id":conversation,"conversation_title":title,"bots":[]}));
            index
        });
        if groups[idx]["conversation_title"] == "" && !title.is_empty() {
            groups[idx]["conversation_title"] = json!(title);
        }
        let last: Option<chrono::DateTime<Utc>> = row.get("last_active_at");
        groups[idx]["bots"].as_array_mut().unwrap().push(json!({
            "installation_id":row.get::<_,Uuid>("installation_id"),"agent_id":row.get::<_,Uuid>("agent_id"),
            "bot_name":row.get::<_,String>("bot_name"),"bot_identity_issue":row.get::<_,String>("bot_identity_issue"),
            "last_active_at":last.map(channel_time).unwrap_or_default(),"mention_count":row.get::<_,i64>("mention_count")
        }));
    }
    for group in &mut groups {
        group["bots"].as_array_mut().unwrap().sort_by(|a, b| {
            a["installation_id"]
                .as_str()
                .cmp(&b["installation_id"].as_str())
        });
    }
    groups.sort_by(|a, b| {
        let left = a["conversation_title"].as_str().unwrap_or("");
        let right = b["conversation_title"].as_str().unwrap_or("");
        match (left.is_empty(), right.is_empty()) {
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => left.cmp(right).then_with(|| {
                a["conversation_id"]
                    .as_str()
                    .cmp(&b["conversation_id"].as_str())
            }),
        }
    });
    out["groups"] = json!(groups);
    let counts = db.query(&format!("{VISIBLE} SELECT p.installation_id,count(*)::bigint AS total FROM dingtalk_group_presence p JOIN visible v ON v.id=p.installation_id WHERE p.workspace_id=$1 AND (p.last_active_at < $5 OR p.last_active_at IS NULL) GROUP BY p.installation_id"), &[&workspace,&user,&is_admin,&agent,&since]).await.map_err(db_error)?;
    let mut inactive_counts = Map::new();
    for row in counts {
        inactive_counts.insert(
            row.get::<_, Uuid>("installation_id").to_string(),
            json!(row.get::<_, i64>("total")),
        );
    }
    out["inactive_group_counts"] = Value::Object(inactive_counts);
    let identities = db.query(&format!("{VISIBLE} SELECT v.id,v.agent_id,b.bot_name,b.bot_identity_issue FROM visible v JOIN dingtalk_bot_identity b ON b.installation_id=v.id AND b.workspace_id=$1"), &[&workspace,&user,&is_admin,&agent]).await.map_err(db_error)?;
    let mut bot_identities = Map::new();
    for row in identities {
        let id: Uuid = row.get("id");
        bot_identities.insert(id.to_string(), json!({"installation_id":id,"agent_id":row.get::<_,Uuid>("agent_id"),"bot_name":row.get::<_,String>("bot_name"),"bot_identity_issue":row.get::<_,String>("bot_identity_issue"),"last_active_at":"","mention_count":0}));
    }
    out["bot_identities"] = Value::Object(bot_identities);
    Ok(out)
}

async fn forget(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(params): Path<Params>,
) -> Result<StatusCode, ApiError> {
    actor.require_human()?;
    let workspace = id(&params, "id")?;
    let installation = id(&params, "installation")?;
    let conversation = params
        .get("conversation")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "conversation id is required"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "channel database unavailable",
        )
    })?;
    let tx = db.transaction().await.map_err(db_error)?;
    let role = membership(&*tx, workspace, user).await?;
    if !admin(&role) {
        return Err(error(StatusCode::FORBIDDEN, "dingtalk group not found"));
    }
    let row = tx.query_opt("SELECT 1 FROM channel_installation WHERE id=$1 AND workspace_id=$2 AND channel_type='dingtalk' FOR UPDATE", &[&installation,&workspace]).await.map_err(db_error)?;
    if row.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "dingtalk group not found"));
    }
    let n = tx.execute("DELETE FROM dingtalk_group_presence WHERE workspace_id=$1 AND installation_id=$2 AND conversation_id=$3", &[&workspace,&installation,&conversation]).await.map_err(db_error)?;
    if n == 0 {
        return Err(error(StatusCode::NOT_FOUND, "dingtalk group not found"));
    }
    tx.commit().await.map_err(db_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inactive_pagination_rejects_unbounded_and_overflow() {
        let parse = |limit: &str, offset: &str| {
            InventoryFilter::parse(Filters {
                activity: Some("inactive".into()),
                installation_id: Some(Uuid::nil().to_string()),
                limit: Some(limit.into()),
                offset: Some(offset.into()),
            })
        };
        assert!(parse("100", "0").is_ok());
        for (limit, offset) in [
            ("0", "0"),
            ("101", "0"),
            ("1", "-1"),
            ("1", "9223372036854775807"),
            ("nan", "0"),
        ] {
            assert!(parse(limit, offset).is_err());
        }
    }
}
