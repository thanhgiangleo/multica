//! Issue hierarchy and member/project edits. Agent handoff and status writes remain gated.
use super::{ApiError, error};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

/// Must process committed issue_child_event rows for these parents. The DB
/// trigger persists the events in the same transaction; the scheduler retries
/// them if a post-commit notification is lost.
pub trait IssueChildRuntime: Send + Sync {
    fn process_child_events(&self, workspace_id: Uuid, parent_ids: Vec<Uuid>);
}
pub type SharedIssueChildRuntime = Arc<dyn IssueChildRuntime>;

#[derive(Default, Deserialize)]
#[serde(default)]
pub(super) struct Fields {
    pub parent_issue_id: Option<String>,
    pub project_id: Option<String>,
    pub stage: Option<i32>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
}

pub(super) struct Plan {
    pub parent: Option<Uuid>,
    pub project: Option<Uuid>,
    pub stage: Option<i32>,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<Uuid>,
}

pub(super) fn touched(raw: &Value) -> bool {
    [
        "parent_issue_id",
        "project_id",
        "stage",
        "assignee_type",
        "assignee_id",
    ]
    .iter()
    .any(|key| raw.get(key).is_some())
}

pub(super) fn child_touched(raw: &Value) -> bool {
    raw.get("parent_issue_id").is_some() || raw.get("stage").is_some()
}

pub(super) async fn lock_graph(tx: &Transaction<'_>, workspace: Uuid) -> Result<(), ApiError> {
    // Shared by create/update/delete and batch within this issue-write
    // branch. Acquire before any issue row lock to avoid parent/delete races.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("issue-topology|{workspace}")],
    )
    .await
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue"))?;
    Ok(())
}

fn uuid(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, field))
}

/// Called with the topology lock and the current issue row locked. Explicit
/// null clears a nullable field; omitted fields retain values from that row.
pub(super) async fn plan(
    tx: &Transaction<'_>,
    workspace: Uuid,
    issue_id: Uuid,
    current: &Row,
    fields: &Fields,
    raw: &Value,
) -> Result<Plan, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue");
    let parent = if raw.get("parent_issue_id").is_some() {
        fields
            .parent_issue_id
            .as_deref()
            .map(|id| uuid(id, "invalid parent_issue_id"))
            .transpose()?
    } else {
        current.get("parent_issue_id")
    };
    if raw.get("parent_issue_id").is_some() {
        if parent == Some(issue_id) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "an issue cannot be its own parent",
            ));
        }
        if let Some(mut cursor) = parent {
            // Frozen Go examines ten ancestors. The topology lock also keeps
            // concurrent parent moves from passing each other's checks.
            for _ in 0..10 {
                let row = tx.query_opt(
                    "SELECT parent_issue_id FROM issue WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
                    &[&cursor, &workspace],
                ).await.map_err(|_| fail())?
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "parent issue not found in this workspace"))?;
                let Some(ancestor) = row.get::<_, Option<Uuid>>(0) else {
                    break;
                };
                if ancestor == issue_id {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "circular parent relationship detected",
                    ));
                }
                cursor = ancestor;
            }
        }
    }
    let project = if raw.get("project_id").is_some() {
        fields
            .project_id
            .as_deref()
            .map(|id| uuid(id, "invalid project_id"))
            .transpose()?
    } else {
        current.get("project_id")
    };
    if raw.get("project_id").is_some()
        && let Some(id) = project
    {
        // Validate explicit no-ops too. Workflow admission follows the complete plan.
        tx.query_opt(
            "SELECT id FROM project WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to validate project",
            )
        })?
        .ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "project not found in this workspace",
            )
        })?;
    }
    let stage = if raw.get("stage").is_some() {
        fields.stage
    } else {
        current.get("stage")
    };
    if stage.is_some_and(|s| s < 1) {
        return Err(error(StatusCode::BAD_REQUEST, "stage must be >= 1"));
    }
    let assignee_type: Option<String> = if raw.get("assignee_type").is_some() {
        fields.assignee_type.clone()
    } else {
        current.get("assignee_type")
    };
    let assignee_id = if raw.get("assignee_id").is_some() {
        fields
            .assignee_id
            .as_deref()
            .map(|id| uuid(id, "invalid assignee_id"))
            .transpose()?
    } else {
        current.get("assignee_id")
    };
    if raw.get("assignee_type").is_some() || raw.get("assignee_id").is_some() {
        match (&assignee_type, assignee_id) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "assignee_type and assignee_id must be provided together",
                ));
            }
            (Some(kind), Some(id)) if kind == "member" => {
                let exists = tx
                    .query_opt(
                        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR KEY SHARE",
                        &[&workspace, &id],
                    )
                    .await
                    .map_err(|_| fail())?;
                if exists.is_none() {
                    return Err(error(
                        StatusCode::BAD_REQUEST,
                        "assignee_id does not refer to a member of this workspace",
                    ));
                }
            }
            (Some(kind), Some(_)) if kind == "agent" || kind == "squad" => {
                return Err(error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "agent assignment runtime unavailable",
                ));
            }
            (Some(_), Some(_)) => {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "assignee_type must be 'member', 'agent', or 'squad'",
                ));
            }
        }
    }
    Ok(Plan {
        parent,
        project,
        stage,
        assignee_type,
        assignee_id,
    })
}
