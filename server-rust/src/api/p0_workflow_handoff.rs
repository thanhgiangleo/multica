//! Read-only preview of entering a project's workflow step, mirroring Go's
//! PreviewWorkflowHandoff without invoking mutation or dispatch adapters.
use super::super::{ApiError, ApiState, GoJson, error, issue_read, issue_workflow, timestamp};
use super::{WorkspaceId, parse_params};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/workflow-handoff", get(preview))
}

fn failure() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to resolve project workflow",
    )
}

fn category(key: &str, custom: &HashMap<String, String>) -> String {
    match key {
        "backlog" | "todo" => "unstarted",
        "in_progress" | "in_review" | "blocked" => "started",
        "done" => "done",
        "cancelled" => "closed",
        // Go's Category yields "" for a catalog row whose stored category is
        // not one of the four lifecycle values, so a legacy behavior value
        // never matches a step here.
        _ => custom
            .get(key)
            .map(String::as_str)
            .filter(|value| matches!(*value, "unstarted" | "started" | "done" | "closed"))
            .unwrap_or(""),
    }
    .to_owned()
}

async fn mapped_status(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    definition: &issue_workflow::Definition,
    previous: &str,
) -> Result<String, ApiError> {
    if definition.has(previous) {
        return Ok(previous.to_owned());
    }
    let keys: Vec<String> = std::iter::once(previous.to_owned())
        .chain(definition.status_keys())
        .collect();
    // Go's status Category returns the empty category when a catalog lookup
    // fails; a missing custom status therefore falls back to the start status.
    let custom: HashMap<String, String> = db
        .query(
            "SELECT key,category FROM issue_status WHERE workspace_id=$1 AND key=ANY($2::text[])",
            &[&ws, &keys],
        )
        .await
        .map_err(|_| failure())?
        .iter()
        .map(|row| (row.get("key"), row.get("category")))
        .collect();
    let previous_category = category(previous, &custom);
    Ok(definition
        .steps
        .iter()
        .find(|step| {
            !previous_category.is_empty()
                && category(&step.status_key, &custom) == previous_category
        })
        .map_or("todo", |step| step.status_key.as_str())
        .to_owned())
}

fn rejected_status(definition: &issue_workflow::Definition, status: &str) -> Response {
    let allowed = definition.status_keys();
    (
        StatusCode::BAD_REQUEST,
        GoJson(json!({
            "error": format!("status {status:?} is not part of this project's workflow {:?}; use one of: {}", definition.name, allowed.join(", ")),
            "code": "status_not_in_workflow",
            "workflow_name": definition.name,
            "allowed_statuses": allowed,
        })),
    )
        .into_response()
}

async fn brief(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    issue: &Row,
    status: &str,
    definition: &issue_workflow::Definition,
    project: &issue_workflow::ProjectRef,
) -> String {
    let custom: HashMap<String, String> = db
        .query(
            "SELECT key,name FROM issue_status WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .unwrap_or_default()
        .iter()
        .map(|row| (row.get("key"), row.get("name")))
        .collect();
    let status_name = |key: &str| {
        if super::is_builtin_status(key) {
            String::new()
        } else {
            custom.get(key).cloned().unwrap_or_default()
        }
    };
    let creator_type: String = issue.get("creator_type");
    let creator_id: Uuid = issue.get("creator_id");
    let mut handler_names = HashMap::new();
    for step in definition.steps.iter().filter(|step| step.hands_off()) {
        let name = match issue_workflow::resolve_handler(
            step,
            project,
            Some(&creator_type),
            Some(creator_id),
        ) {
            Some((kind, id)) => {
                let sql = match kind.as_str() {
                    "agent" => Some("SELECT name FROM agent WHERE id=$1 AND workspace_id=$2"),
                    "squad" => Some("SELECT name FROM squad WHERE id=$1 AND workspace_id=$2"),
                    "member" => Some("SELECT name FROM \"user\" WHERE id=$1"),
                    _ => None,
                };
                let resolved = if let Some(sql) = sql {
                    let row = if kind == "member" {
                        db.query_opt(sql, &[&id]).await
                    } else {
                        db.query_opt(sql, &[&id, &ws]).await
                    };
                    row.ok()
                        .flatten()
                        .map(|row| row.get("name"))
                        .unwrap_or(kind)
                } else {
                    kind
                };
                match step.handler.r#type.as_str() {
                    "project_lead" => format!("{resolved} (project lead)"),
                    "creator" => format!("{resolved} (issue creator)"),
                    _ => resolved,
                }
            }
            None => match step.handler.r#type.as_str() {
                "project_lead" => "the project lead (none set)".to_owned(),
                "creator" => "the issue creator".to_owned(),
                _ => String::new(),
            },
        };
        handler_names.insert(step.status_key.clone(), name);
    }
    let handler_name = |step: &super::super::workflow::Step| {
        handler_names
            .get(&step.status_key)
            .cloned()
            .unwrap_or_default()
    };
    let prefix: String = db
        .query_opt(
            "SELECT name,issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .ok()
        .flatten()
        .map(|row| {
            issue_workflow::issue_prefix_for_workspace(
                &row.get::<_, String>("issue_prefix"),
                &row.get::<_, String>("name"),
            )
        })
        .unwrap_or_default();
    let identifier = format!("{prefix}-{}", issue.get::<_, i32>("number"));
    issue_workflow::brief(&issue_workflow::BriefInput {
        workflow: definition,
        project_name: &project.title,
        status_key: status,
        issue_identifier: &identifier,
        status_name: Some(&status_name),
        handler_name: Some(&handler_name),
        observer: false,
        assignee_name: "",
        legacy_runtime_rules: false,
    })
}

async fn previous_runs(db: &deadpool_postgres::Client, issue: &Row) -> Vec<Value> {
    let Some(assignee) = issue.get::<_, Option<Uuid>>("assignee_id") else {
        return Vec::new();
    };
    let kind: Option<String> = issue.get("assignee_type");
    if !matches!(kind.as_deref(), Some("agent" | "squad")) {
        return Vec::new();
    }
    let id: Uuid = issue.get("id");
    // ListActiveTasksByIssue is ordered newest first and includes queued and
    // waiting_local_directory, not just running tasks.
    db.query(
        "SELECT id,agent_id,squad_id,status,started_at,created_at FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory') ORDER BY created_at DESC",
        &[&id],
    )
    .await
    .unwrap_or_default()
    .iter()
    .filter(|task| {
        if kind.as_deref() == Some("agent") {
            task.get::<_, Uuid>("agent_id") == assignee
        } else {
            task.get::<_, Option<Uuid>>("squad_id") == Some(assignee)
        }
    })
    .map(|task| {
        let started: DateTime<Utc> = task
            .get::<_, Option<DateTime<Utc>>>("started_at")
            .unwrap_or_else(|| task.get("created_at"));
        json!({
            "task_id": task.get::<_, Uuid>("id"),
            "agent_id": task.get::<_, Uuid>("agent_id"),
            "status": task.get::<_, String>("status"),
            "started_at": timestamp(started),
        })
    })
    .collect()
}

async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let id: Uuid = issue_read::load_issue(&db, ws, &raw).await?.get("id");
    let issue = db
        .query_one(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let params = parse_params(query.as_deref());
    let requested = params.get("status").filter(|value| !value.is_empty());
    let project_param = params.get("project_id").filter(|value| !value.is_empty());
    if requested.is_none() && project_param.is_none() {
        return Err(error(StatusCode::BAD_REQUEST, "status is required"));
    }
    let previous: String = issue.get("status");
    let previous_project: Option<Uuid> = issue.get("project_id");
    let mut project = previous_project;
    if let Some(raw) = project_param {
        let id = Uuid::parse_str(raw)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid project_id"))?;
        if db
            .query_opt(
                "SELECT id FROM project WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .ok()
            .flatten()
            .is_none()
        {
            return Err(error(StatusCode::NOT_FOUND, "project not found"));
        }
        project = Some(id);
    }
    let found = issue_workflow::for_project(&db, ws, project)
        .await
        .map_err(|_| failure())?;
    let mut status = requested.cloned().unwrap_or_else(|| previous.clone());
    let mut handoff = None;
    if let Some((definition, project_ref)) = found.as_ref() {
        let moving = project != previous_project;
        if moving && requested.is_none() && !definition.is_default {
            status = mapped_status(&db, ws, definition, &previous).await?;
        }
        if (moving || requested.is_some()) && !definition.has(&status) && !definition.is_default {
            return Ok(rejected_status(definition, &status));
        }
        let entering = (moving && project_param.is_some())
            || (!moving && requested.is_some() && status != previous);
        if entering
            && issue.get::<_, Option<String>>("triage_state").is_none()
            && let Some(step) = definition.step(&status).filter(|step| step.hands_off())
        {
            let creator_type: String = issue.get("creator_type");
            let creator_id: Uuid = issue.get("creator_id");
            if let Some((kind, assignee)) = issue_workflow::resolve_handler(
                step,
                project_ref,
                Some(&creator_type),
                Some(creator_id),
            ) {
                handoff = Some((definition, project_ref, kind, assignee));
            }
        }
    }
    let old_type: Option<String> = issue.get("assignee_type");
    let old_id: Option<Uuid> = issue.get("assignee_id");
    let mut body = json!({
        "handoff": false,
        "from_status": previous,
        "to_status": status,
        "previous_assignee_type": old_type,
        "previous_assignee_id": old_id,
        "previous_runs": [],
    });
    if let Some((definition, project_ref, kind, assignee)) = handoff {
        body["handoff"] = json!(true);
        body["workflow_name"] = json!(definition.name);
        body["handler_type"] = json!(kind);
        body["handler_id"] = json!(assignee);
        if kind == "agent" || kind == "squad" {
            body["brief"] = json!(brief(&db, ws, &issue, &status, definition, project_ref).await);
        }
        if old_type.as_deref() != Some(&kind) || old_id != Some(assignee) {
            body["previous_runs"] = json!(previous_runs(&db, &issue).await);
        }
    }
    Ok(GoJson(body).into_response())
}
