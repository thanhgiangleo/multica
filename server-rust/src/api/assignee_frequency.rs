//! Current actor's assignee history from the frozen Go activity/issue SQL.
use super::{ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error};
use axum::{
    extract::{Extension, State},
    http::StatusCode,
};
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

const ACTIVITY_COUNTS: &str = "SELECT details->>'to_type' AS assignee_type, details->>'to_id' AS assignee_id, COUNT(*)::bigint AS frequency FROM activity_log WHERE workspace_id=$1 AND actor_id=$2 AND actor_type='member' AND action='assignee_changed' AND details->>'to_type' IS NOT NULL AND details->>'to_id' IS NOT NULL GROUP BY details->>'to_type', details->>'to_id'";
const ISSUE_COUNTS: &str = "SELECT assignee_type, assignee_id, COUNT(*)::bigint AS frequency FROM issue WHERE workspace_id=$1 AND creator_id=$2 AND creator_type='member' AND assignee_type IS NOT NULL AND assignee_id IS NOT NULL GROUP BY assignee_type, assignee_id";

#[derive(Serialize)]
pub(super) struct Frequency {
    assignee_type: String,
    assignee_id: String,
    frequency: i64,
}

pub(super) async fn get(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Vec<Frequency>>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get assignee frequency",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let activities = db
        .query(ACTIVITY_COUNTS, &[&workspace, &user])
        .await
        .map_err(|_| fail())?;
    let issues = db
        .query(ISSUE_COUNTS, &[&workspace, &user])
        .await
        .map_err(|_| fail())?;
    let mut counts: HashMap<(String, String), i64> = HashMap::new();
    for row in activities {
        let kind: Option<String> = row.get("assignee_type");
        let id: Option<String> = row.get("assignee_id");
        if let (Some(kind), Some(id)) = (kind, id)
            && !kind.is_empty()
            && !id.is_empty()
        {
            *counts.entry((kind, id)).or_default() += row.get::<_, i64>("frequency");
        }
    }
    for row in issues {
        let kind: Option<String> = row.get("assignee_type");
        let id: Option<Uuid> = row.get("assignee_id");
        if let (Some(kind), Some(id)) = (kind, id) {
            *counts.entry((kind, id.to_string())).or_default() += row.get::<_, i64>("frequency");
        }
    }
    let mut result: Vec<_> = counts
        .into_iter()
        .map(|((assignee_type, assignee_id), frequency)| Frequency {
            assignee_type,
            assignee_id,
            frequency,
        })
        .collect();
    result.sort_by(|a, b| {
        b.frequency
            .cmp(&a.frequency)
            .then_with(|| a.assignee_type.cmp(&b.assignee_type))
            .then_with(|| a.assignee_id.cmp(&b.assignee_id))
    });
    Ok(GoJson(result))
}
