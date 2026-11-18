//! Workspace wakeup board from the frozen Go SQL, scoped by actor and member.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    agent_activity::allowed_agents,
    catalog::{WorkspaceId, parse_params},
    error,
};
use axum::{
    extract::{Extension, RawQuery, State},
    http::StatusCode,
};
use serde_json::Value;
use uuid::Uuid;

fn pagination(
    raw: Option<&String>,
    default: i32,
    maximum: i32,
    is_limit: bool,
) -> Result<i32, ApiError> {
    let Some(raw) = raw.filter(|s| !s.is_empty()) else {
        return Ok(default);
    };
    let number = raw
        .parse::<i32>()
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid pagination"))?;
    if number < i32::from(is_limit) || number > maximum {
        return Err(error(StatusCode::BAD_REQUEST, "invalid pagination"));
    }
    Ok(number)
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let param = |name: &str| params.get(name).map(String::as_str).unwrap_or("");
    let scope = if param("scope").is_empty() {
        "active"
    } else {
        param("scope")
    };
    let kind = if param("kind").is_empty() {
        "all"
    } else {
        param("kind")
    };
    if !["active", "all", "paused", "disabled", "ended"].contains(&scope) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup scope"));
    }
    if !["all", "event", "at", "recurring"].contains(&kind) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup kind"));
    }
    let offset = pagination(params.get("offset"), 0, 1_000_000, false)?;
    let limit = pagination(params.get("limit"), 50, 100, true)?;
    let source = param("source");
    if !["", "member", "agent", "system"].contains(&source) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid wakeup source"));
    }
    let search = param("search").trim();
    if search.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST, "search too long"));
    }
    let agent_id = param("agent_id");
    let agent_id = if agent_id.is_empty() {
        String::new()
    } else {
        Uuid::parse_str(agent_id)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid agent id"))?
            .to_string()
    };
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "could not save wakeup");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let ids = allowed_agents(&db, ws, user, &actor).await?;

    // Only a live, verified task may inherit the initiating human's management
    // rights; the authenticated runtime owner alone does not grant them.
    let originator: Option<Uuid> = match actor {
        Actor::Task(task) => db
            .query_opt(
                "SELECT originator_user_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2 AND status NOT IN ('completed','failed','cancelled')",
                &[&task.task, &task.agent],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to resolve management access"))?
            .and_then(|row| row.get(0)),
        Actor::Human | Actor::Cloud => Some(user),
        Actor::Daemon => None,
    };
    let manager = if let Some(originator) = originator {
        db.query_opt(
            "SELECT user_id,role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &originator],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve management access",
            )
        })?
    } else {
        None
    };
    let manager_id: Option<Uuid> = manager.as_ref().map(|row| row.get("user_id"));
    let is_admin = manager
        .as_ref()
        .is_some_and(|row| matches!(row.get::<_, &str>("role"), "owner" | "admin"));
    let result: Value = db
        .query_one(
            include_str!("workspace_wakeup.sql"),
            &[
                &ws,
                &ids,
                &manager_id,
                &is_admin,
                &scope,
                &kind,
                &source,
                &agent_id,
                &search,
                &limit,
                &offset,
            ],
        )
        .await
        .map_err(|_| fail())?
        .get("result");
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_rejects_out_of_range_and_non_numeric() {
        for raw in ["-1", "101", "", "1.5", " 1", "2147483648"] {
            if raw.is_empty() {
                continue;
            }
            assert!(pagination(Some(&raw.to_owned()), 50, 100, true).is_err());
        }
        assert_eq!(pagination(None, 50, 100, true).unwrap(), 50);
        assert_eq!(pagination(Some(&"+1".into()), 50, 100, true).unwrap(), 1);
        assert_eq!(
            pagination(Some(&"1000000".into()), 0, 1_000_000, false).unwrap(),
            1_000_000
        );
    }
}
