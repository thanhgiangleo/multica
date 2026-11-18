//! Workspace system-rule defaults and customized open-issue count from frozen Go.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    catalog::WorkspaceId,
    error,
    issue_system_wakeup::{baseline, human_role, parse_input, save_failure},
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) const CHILD_DONE_INSTRUCTION: &str = "Sub-issues of this issue have closed; the trigger facts list each stage and which one is next. \
If a later stage is waiting, check that its dependencies are met, then move its sub-issues out of backlog so they start. \
If a sub-issue in the closed stages was cancelled rather than finished, decide whether its work is still needed before advancing. \
When every sub-issue is closed, bring their results together on this issue and move it forward, or mark it ready for review when nothing remains.";

const CUSTOMIZED: &str = r#"SELECT count(*) FROM issue_wakeup w
JOIN issue i ON i.id=w.issue_id AND i.workspace_id=w.workspace_id
WHERE w.workspace_id=$1 AND w.system_rule='child_done' AND w.customized_at IS NOT NULL
 AND i.status NOT IN ('done','cancelled')
 AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id
   AND s.key=i.status AND s.category IN ('done','closed'))"#;

#[derive(Serialize)]
pub(super) struct SystemWakeup {
    rule: &'static str,
    enabled: bool,
    instruction: String,
    builtin_instruction: &'static str,
    customized: i64,
}

// Go treats malformed settings as defaults and only JSON boolean false disables.
pub(super) fn default(settings: &Value) -> (bool, String) {
    let enabled = settings.get("system_wakeup_child_done") != Some(&Value::Bool(false));
    let instruction = settings
        .get("system_wakeup_child_done_instruction")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    (enabled, instruction)
}

pub(super) async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
) -> Result<GoJson<Vec<SystemWakeup>>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not load system wakeups",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let row = db
        .query_opt("SELECT settings FROM workspace WHERE id=$1", &[&workspace])
        .await
        .map_err(|_| fail())?
        .ok_or_else(fail)?;
    let settings: Value = row.get(0);
    let customized: i64 = db
        .query_one(CUSTOMIZED, &[&workspace])
        .await
        .map_err(|_| fail())?
        .get(0);
    let (enabled, instruction) = default(&settings);
    Ok(GoJson(vec![SystemWakeup {
        rule: "child_done",
        enabled,
        instruction,
        builtin_instruction: CHILD_DONE_INSTRUCTION,
        customized,
    }]))
}

pub(super) async fn put(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(rule): Path<String>,
    body: Bytes,
) -> Result<GoJson<Vec<SystemWakeup>>, ApiError> {
    if rule != "child_done" {
        return Err(error(StatusCode::NOT_FOUND, "system wakeup not found"));
    }
    let input = parse_input(&body)?;
    let mut db = state.pool.get().await.map_err(|_| save_failure())?;
    let role = human_role(&**db, workspace, user, &actor).await?;
    if role != "owner" && role != "admin" {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only owners and admins can change workspace defaults",
        ));
    }
    let enable = input.enabled;
    let instruction = input.instruction.map(|s| s.trim().to_owned());
    if instruction.as_ref().is_some_and(|s| s.len() > 4000) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid wakeup: instruction must be at most 4000 bytes",
        ));
    }
    if enable.is_some() || instruction.is_some() {
        let mut patch = serde_json::Map::new();
        if let Some(enabled) = enable {
            patch.insert("system_wakeup_child_done".into(), json!(enabled));
        }
        if let Some(instruction) = &instruction {
            patch.insert(
                "system_wakeup_child_done_instruction".into(),
                json!(instruction),
            );
        }
        let tx = db.transaction().await.map_err(|_| save_failure())?;
        tx.execute(
            "UPDATE workspace SET settings=COALESCE(settings,'{}'::jsonb)||$2::jsonb,updated_at=now() WHERE id=$1",
            &[&workspace, &Value::Object(patch)],
        )
        .await
        .map_err(|_| save_failure())?;
        // Only rules nobody customized and the platform did not pause follow it.
        if let Some(enabled) = enable {
            let changed = tx
                .query(
                    "UPDATE issue_wakeup SET enabled=$2,updated_at=clock_timestamp() WHERE workspace_id=$1 AND system_rule='child_done' AND customized_at IS NULL AND paused_reason IS NULL AND enabled<>$2 RETURNING id,issue_id",
                    &[&workspace, &enabled],
                )
                .await
                .map_err(|_| save_failure())?;
            for row in &changed {
                let id: Uuid = row.get(0);
                if enabled {
                    baseline(&tx, workspace, row.get(1), id, false).await?;
                } else {
                    tx.execute(
                        "UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id=$1 AND processed_at IS NULL",
                        &[&id],
                    )
                    .await
                    .map_err(|_| save_failure())?;
                }
            }
        }
        tx.commit().await.map_err(|_| save_failure())?;
    }
    list(State(state), Extension(WorkspaceId(workspace))).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_match_frozen_go_types_and_whitespace() {
        assert_eq!(default(&json!({})), (true, String::new()));
        assert_eq!(
            default(&json!({"system_wakeup_child_done": "false"})),
            (true, String::new())
        );
        assert_eq!(
            default(
                &json!({"system_wakeup_child_done": false, "system_wakeup_child_done_instruction": "  Wrap up.  "})
            ),
            (false, "Wrap up.".into())
        );
    }
}
