//! Project definition mutations with post-commit publication to the shared bus adapter.
use super::account_catalog_write_catalog_publish::{CatalogChange, publish};
use super::account_catalog_write_resources as resources;
#[path = "account_catalog_write_project_decode.rs"]
mod decode;
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use deadpool_postgres::GenericClient;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::{Row, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/projects", post(create))
        .route("/api/projects/", post(create))
        .route(
            "/api/projects/{id}",
            axum::routing::put(update).delete(remove),
        )
        .route(
            "/api/projects/{id}/",
            axum::routing::put(update).delete(remove),
        )
        .layer(DefaultBodyLimit::disable())
}

pub(super) fn workflow_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/projects/{id}/workflow", post(set_workflow))
        .route("/api/projects/{id}/workflow/", post(set_workflow))
        .layer(DefaultBodyLimit::disable())
}

fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn internal(message: &'static str) -> ApiError {
    error(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn optional_uuid(raw: Option<&str>, field: &'static str) -> Result<Option<Uuid>, ApiError> {
    raw.filter(|v| !v.is_empty())
        .map(|v| Uuid::parse_str(v).map_err(|_| bad(field)))
        .transpose()
}
fn date(raw: Option<&str>, field: &'static str) -> Result<Option<NaiveDate>, ApiError> {
    raw.filter(|v| !v.is_empty())
        .map(|v| {
            if let Ok(day) = NaiveDate::parse_from_str(v, "%Y-%m-%d") {
                return Ok(day);
            }
            let instant = DateTime::parse_from_rfc3339(v).map_err(|_| bad(field))?;
            let utc = instant.with_timezone(&Utc);
            (utc.time().num_seconds_from_midnight() == 0 && utc.timestamp_subsec_nanos() == 0)
                .then_some(utc.date_naive())
                .ok_or_else(|| bad(field))
        })
        .transpose()
}
#[cfg(test)]
mod date_tests {
    use super::date;
    use chrono::NaiveDate;

    #[test]
    fn legacy_rfc3339_requires_utc_midnight() {
        let expected = Some(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());
        for accepted in [
            "2026-03-01",
            "2026-03-01T00:00:00Z",
            "2026-03-01T01:00:00+01:00",
            "2026-03-01T00:00:00.000Z",
        ] {
            assert_eq!(date(Some(accepted), "bad date").unwrap(), expected);
        }
        assert_eq!(date(Some(""), "bad date").unwrap(), None);
        assert_eq!(date(None, "bad date").unwrap(), None);
        for rejected in [
            "2026-02-28T16:00:00Z",
            "2026-03-01T00:00:00+08:00",
            "2026-03-01T00:00:00.000000001Z",
            "2026-03-01T00:00:01Z",
            "2026-02-30",
        ] {
            assert!(date(Some(rejected), "bad date").is_err(), "{rejected}");
        }
    }
}

fn choice(
    value: Option<&str>,
    default: &'static str,
    allowed: &[&str],
    field: &'static str,
) -> Result<String, ApiError> {
    let value = value.filter(|s| !s.is_empty()).unwrap_or(default);
    if allowed.contains(&value) {
        Ok(value.to_owned())
    } else {
        Err(bad(field))
    }
}
fn project_response(row: &Row, issue_count: i64, done_count: i64, resource_count: i64) -> Value {
    let date = |name| row.get::<_, Option<NaiveDate>>(name).map(|d| d.to_string());
    json!({"id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "title":row.get::<_,String>("title"),"description":row.get::<_,Option<String>>("description"),
        "icon":row.get::<_,Option<String>>("icon"),"status":row.get::<_,String>("status"),
        "priority":row.get::<_,String>("priority"),"lead_type":row.get::<_,Option<String>>("lead_type"),
        "lead_id":row.get::<_,Option<Uuid>>("lead_id"),"start_date":date("start_date"),
        "due_date":date("due_date"),"workflow_id":row.get::<_,Option<Uuid>>("workflow_id"),
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
        "issue_count":issue_count,"done_count":done_count,"resource_count":resource_count})
}
pub(super) async fn response<C: GenericClient>(
    db: &C,
    ws: Uuid,
    row: &Row,
    resource_count: Option<i64>,
) -> Value {
    let id: Uuid = row.get("id");
    let terminal = db
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND category = ANY($2::text[])",
            &[&ws, &vec!["done", "closed"]],
        )
        .await
        .map(|rows| {
            let mut keys: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            for key in ["done", "cancelled"] {
                if !keys.iter().any(|k| k == key) {
                    keys.push(key.to_owned());
                }
            }
            keys
        })
        .unwrap_or_else(|_| vec!["done".into(), "cancelled".into()]);
    let counts = db.query_opt("SELECT count(*)::bigint, count(*) FILTER (WHERE status=ANY($3::text[]))::bigint FROM issue WHERE workspace_id=$1 AND project_id=$2", &[&ws,&id,&terminal]).await.ok().flatten();
    let resources = if let Some(count) = resource_count {
        count
    } else {
        db.query_one(
            "SELECT count(*)::bigint FROM project_resource WHERE project_id=$1",
            &[&id],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(0)
    };
    project_response(
        row,
        counts.as_ref().map_or(0, |r| r.get(0)),
        counts.as_ref().map_or(0, |r| r.get(1)),
        resources,
    )
}
fn member(actor: Actor) -> Result<(), ApiError> {
    if matches!(actor, Actor::Human | Actor::Task(_)) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "not a workspace member"))
    }
}
async fn workflow<C: GenericClient>(
    db: &C,
    ws: Uuid,
    raw: Option<&str>,
) -> Result<Option<Uuid>, ApiError> {
    let id = optional_uuid(raw, "invalid workflow_id")?;
    if let Some(id) = id {
        let row = db
            .query_opt(
                "SELECT is_default FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .map_err(|_| internal("failed to load workflow"))?
            .ok_or_else(|| bad("workflow not found in this workspace"))?;
        if row.get::<_, bool>(0) {
            return Ok(None);
        }
        return Ok(Some(id));
    }
    Ok(None)
}
fn write_error(err: tokio_postgres::Error, action: &'static str) -> ApiError {
    if err.code() == Some(&SqlState::CHECK_VIOLATION) {
        bad("project rejected: a field value failed a database constraint")
    } else {
        internal(action)
    }
}
struct ProjectFailure(StatusCode, String);
impl From<ApiError> for ProjectFailure {
    fn from(err: ApiError) -> Self {
        Self(err.0, err.1.to_owned())
    }
}
impl IntoResponse for ProjectFailure {
    fn into_response(self) -> Response {
        let mut response = GoJson(json!({"error": self.1})).into_response();
        *response.status_mut() = self.0;
        response
    }
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<Response, ProjectFailure> {
    member(actor)?;
    let body = body.map_err(|_| bad("invalid request body"))?;
    let req: decode::Create = super::account_catalog_write_profile::account_request(&body)?;
    let title = req.title.as_str();
    if title.is_empty() {
        return Err(bad("title is required").into());
    }
    let status = choice(
        Some(req.status.as_str()),
        "planned",
        &["planned", "in_progress", "paused", "completed", "cancelled"],
        "invalid status",
    )?;
    let priority = choice(
        Some(req.priority.as_str()),
        "none",
        &["urgent", "high", "medium", "low", "none"],
        "invalid priority",
    )?;
    let lead = optional_uuid(req.lead_id.as_deref(), "invalid lead_id")?;
    let start = date(
        req.start_date.as_deref(),
        "invalid start_date format, expected YYYY-MM-DD",
    )?;
    let due = date(
        req.due_date.as_deref(),
        "invalid due_date format, expected YYYY-MM-DD",
    )?;
    let mut normalized = Vec::with_capacity(req.resources.len());
    let mut local_daemons = HashMap::new();
    for (index, item) in req.resources.iter().enumerate() {
        let kind = item.resource_type.trim();
        if kind.is_empty() {
            return Err(bad("resources[].resource_type is required").into());
        }
        let reference = resources::decode::normalize(kind, item.resource_ref.as_deref())
            .map_err(|err| ProjectFailure(err.0, format!("resources[{index}]: {}", err.1)))?;
        if kind == "local_directory" {
            let daemon = reference["daemon_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if let Some(previous) = local_daemons.insert(daemon, index) {
                return Err(ProjectFailure(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "resources[{index}]: duplicate local_directory for daemon (already at index {previous}); each daemon may attach at most one local_directory per project"
                    ),
                ));
            }
        }
        let label = item
            .label
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        normalized.push((kind.to_owned(), reference, label, item.position));
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to create project"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to start transaction"))?;
    // Match workflow deletion/switch serialization for a non-default selection.
    if req.workflow_id.as_deref().is_some_and(|id| !id.is_empty()) {
        tx.query_one(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("{ws}:issue_status")],
        )
        .await
        .map_err(|_| internal("failed to lock workflow catalog"))?;
    }
    let workflow = workflow(&tx, ws, req.workflow_id.as_deref()).await?;
    for (kind, reference, _, _) in &normalized {
        if let Some(rejection) = resources::worktree_capable(&tx, ws, kind, reference).await? {
            return Ok((StatusCode::UNPROCESSABLE_ENTITY, GoJson(rejection)).into_response());
        }
    }
    let sql = "INSERT INTO project(workspace_id,title,description,icon,status,lead_type,lead_id,priority,start_date,due_date,workflow_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *";
    let row = tx
        .query_one(
            sql,
            &[
                &ws,
                &title,
                &req.description.as_deref(),
                &req.icon.as_deref(),
                &status,
                &req.lead_type.as_deref(),
                &lead,
                &priority,
                &start,
                &due,
                &workflow,
            ],
        )
        .await
        .map_err(|err| write_error(err, "failed to create project"))?;
    let project_id: Uuid = row.get("id");
    let mut attached = Vec::with_capacity(normalized.len());
    for (i, (kind, reference, label, pos)) in normalized.into_iter().enumerate() {
        let sql = format!(
            "INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,label,position,created_by) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING {}",
            resources::COLUMNS
        );
        let row = tx
            .query_one(
                &sql,
                &[
                    &project_id,
                    &ws,
                    &kind,
                    &reference,
                    &label,
                    &pos.unwrap_or(i as i32),
                    &user,
                ],
            )
            .await
            .map_err(|err| {
                if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
                    ProjectFailure(
                        StatusCode::CONFLICT,
                        format!("resources[{i}]: this resource is already attached"),
                    )
                } else {
                    ProjectFailure(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to attach resource at index {i}"),
                    )
                }
            })?;
        attached.push(resources::response(&row));
    }
    let mut result = project_response(&row, 0, 0, attached.len() as i64);
    tx.commit()
        .await
        .map_err(|_| internal("failed to commit project create"))?;
    drop(db);
    // The transient resources echo belongs only to HTTP, not project:created.
    publish(
        &state,
        ws,
        user,
        CatalogChange::ProjectCreated {
            project: result.clone(),
        },
    )
    .await;
    for resource in &attached {
        publish(
            &state,
            ws,
            user,
            CatalogChange::ResourceCreated {
                resource: resource.clone(),
                project_id,
            },
        )
        .await;
    }
    if !attached.is_empty() {
        result["resources"] = json!(attached);
    }
    Ok((StatusCode::CREATED, GoJson(result)).into_response())
}

#[derive(Default, Debug, PartialEq)]
struct UpdateProject {
    title: Option<String>,
    description: Option<String>,
    icon: Option<String>,
    status: Option<String>,
    priority: Option<String>,
    lead_type: Option<String>,
    lead_id: Option<String>,
    start_date: Option<String>,
    due_date: Option<String>,
}
impl<'de> Deserialize<'de> for UpdateProject {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (key, value) in super::account_catalog_write_profile::decode::fields(
            decoder,
            &[
                "title",
                "description",
                "icon",
                "status",
                "priority",
                "lead_type",
                "lead_id",
                "start_date",
                "due_date",
            ],
        )? {
            match key {
                "title" => request.title = value,
                "description" => request.description = value,
                "icon" => request.icon = value,
                "status" => request.status = value,
                "priority" => request.priority = value,
                "lead_type" => request.lead_type = value,
                "lead_id" => request.lead_id = value,
                "start_date" => request.start_date = value,
                "due_date" => request.due_date = value,
                _ => unreachable!(),
            }
        }
        Ok(request)
    }
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, ApiError> {
    member(actor)?;
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid project id"))?;
    // Frozen Go resolves the scoped project before reading or validating the body.
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to load project"))?;
    db.query_opt(
        "SELECT 1 FROM project WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "project not found"))?;
    drop(db);
    let body = body.map_err(|_| bad("failed to read request body"))?;
    let req: UpdateProject = super::account_catalog_write_profile::full_request(&body)?;
    let raw_fields: Option<HashMap<String, Box<serde_json::value::RawValue>>> =
        super::account_catalog_write_profile::full_request(&body)?;
    let raw_fields = raw_fields.unwrap_or_default();
    // Go's map lookup is case-sensitive even though the struct decoder folds names.
    let title = req.title.as_deref();
    let status = req
        .status
        .as_deref()
        .map(|s| {
            choice(
                Some(s),
                "",
                &["planned", "in_progress", "paused", "completed", "cancelled"],
                "invalid status",
            )
        })
        .transpose()?;
    let priority = req
        .priority
        .as_deref()
        .map(|s| {
            choice(
                Some(s),
                "",
                &["urgent", "high", "medium", "low", "none"],
                "invalid priority",
            )
        })
        .transpose()?;
    let description = req.description.as_deref();
    let icon = req.icon.as_deref();
    let lead_type = req.lead_type.as_deref();
    let lead_id = if raw_fields.contains_key("lead_id") {
        req.lead_id
            .as_deref()
            .map(|v| Uuid::parse_str(v).map_err(|_| bad("invalid lead_id")))
            .transpose()?
    } else {
        None
    };
    let start = if raw_fields.contains_key("start_date") {
        date(
            req.start_date.as_deref(),
            "invalid start_date format, expected YYYY-MM-DD",
        )?
    } else {
        None
    };
    let due = if raw_fields.contains_key("due_date") {
        date(
            req.due_date.as_deref(),
            "invalid due_date format, expected YYYY-MM-DD",
        )?
    } else {
        None
    };
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to update project"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to start transaction"))?;
    let _existing = tx
        .query_opt(
            "SELECT * FROM project WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal("failed to load project"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "project not found"))?;
    let row=tx.query_one("UPDATE project SET title=COALESCE($3,title), description=CASE WHEN $4 THEN $5 ELSE description END, icon=CASE WHEN $6 THEN $7 ELSE icon END, status=COALESCE($8,status), priority=COALESCE($9,priority), lead_type=CASE WHEN $10 THEN $11 ELSE lead_type END, lead_id=CASE WHEN $12 THEN $13 ELSE lead_id END, start_date=CASE WHEN $14 THEN $15 ELSE start_date END, due_date=CASE WHEN $16 THEN $17 ELSE due_date END, updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",
        &[&id,&ws,&title,&raw_fields.contains_key("description"),&description,&raw_fields.contains_key("icon"),&icon,&status,&priority,&raw_fields.contains_key("lead_type"),&lead_type,&raw_fields.contains_key("lead_id"),&lead_id,&raw_fields.contains_key("start_date"),&start,&raw_fields.contains_key("due_date"),&due]).await.map_err(|err|write_error(err,"failed to update project"))?;
    let response = response(&tx, ws, &row, None).await;
    tx.commit()
        .await
        .map_err(|_| internal("failed to commit project update"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::ProjectUpdated {
            project: response.clone(),
        },
    )
    .await;
    Ok(GoJson(response))
}

struct WorkflowFailure(StatusCode, Value);
impl From<ApiError> for WorkflowFailure {
    fn from(error: ApiError) -> Self {
        Self(error.0, json!({"error":error.1}))
    }
}
impl IntoResponse for WorkflowFailure {
    fn into_response(self) -> Response {
        let mut response = GoJson(self.1).into_response();
        *response.status_mut() = self.0;
        response
    }
}

use super::account_catalog_write_workflows::request_decode::SetProjectWorkflowRequest;

async fn set_workflow(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, WorkflowFailure> {
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid project id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to change workflow"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to change workflow"))?;
    // Match the exclusive catalog lock used by workflow edits and deletion.
    tx.query_one(
        "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| internal("failed to change workflow"))?;
    let project = tx
        .query_opt(
            "SELECT * FROM project WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal("failed to change workflow"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "project not found"))?;
    let role: String = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal("failed to change workflow"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let lead = project.get::<_, Option<String>>("lead_type").as_deref() == Some("member")
        && project.get::<_, Option<Uuid>>("lead_id") == Some(user);
    if !matches!(actor, Actor::Human) || (!matches!(role.as_str(), "owner" | "admin") && !lead) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "only workspace admins and the project lead can change this project's workflow",
        )
        .into());
    }
    let body = body.map_err(|_| bad("invalid request body"))?;
    let body: SetProjectWorkflowRequest =
        super::account_catalog_write_profile::account_request(&body)
            .map_err(|_| bad("invalid request body"))?;
    let mut target = optional_uuid(body.workflow_id.as_deref(), "invalid workflow_id")?;
    let mut steps = None;
    if let Some(workflow) = target {
        let row = tx
            .query_opt(
                "SELECT is_default,steps FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
                &[&workflow, &ws],
            )
            .await
            .map_err(|_| internal("failed to change workflow"))?
            .ok_or_else(|| bad("workflow not found in this workspace"))?;
        // Decode Default too: a corrupt workflow is not silently accepted.
        let raw: Value = row.get("steps");
        let decoded = super::workflow::decode_step_values(raw)
            .map_err(|_| internal("failed to read workflow"))?;
        if row.get::<_, bool>("is_default") {
            target = None;
        } else {
            steps = Some(decoded);
        }
    }
    tx.execute(super::account_catalog_write_workspaces::SEED, &[&ws])
        .await
        .map_err(|_| internal("failed to load issue statuses"))?;
    let catalog = tx
        .query(
            "SELECT key,category,archived_at IS NOT NULL FROM issue_status WHERE workspace_id=$1",
            &[&ws],
        )
        .await
        .map_err(|_| internal("failed to load issue statuses"))?;
    let categories: HashMap<String, String> =
        catalog.iter().map(|r| (r.get(0), r.get(1))).collect();
    let archived: HashSet<String> = catalog
        .iter()
        .filter(|r| r.get::<_, bool>(2))
        .map(|r| r.get(0))
        .collect();
    let allowed = |key: &str| {
        steps.as_ref().map_or_else(
            || categories.contains_key(key),
            |steps| steps.iter().any(|s| s["status_key"] == key),
        )
    };
    let accepts = |key: &str| allowed(key) && (steps.is_some() || !archived.contains(key));
    let counts = tx.query("SELECT status,count(*)::bigint FROM issue WHERE workspace_id=$1 AND project_id=$2 GROUP BY status ORDER BY status", &[&ws,&id])
        .await.map_err(|_| internal("failed to change workflow"))?;
    let mut required = Vec::new();
    let mut unchanged = Vec::new();
    let mut total = 0i64;
    let mut affected = 0i64;
    for count in counts {
        let key: String = count.get(0);
        let n: i64 = count.get(1);
        total += n;
        if allowed(&key) {
            unchanged.push(json!({"status_key":key,"issue_count":n}));
        } else {
            affected += n;
            let suggested = steps
                .as_ref()
                .and_then(|steps| {
                    steps
                        .iter()
                        .filter_map(|s| s["status_key"].as_str())
                        .find(|s| {
                            categories.get(&key).is_some_and(|category| {
                                !category.is_empty() && categories.get(*s) == Some(category)
                            })
                        })
                })
                .unwrap_or("todo");
            required
                .push(json!({"status_key":key,"issue_count":n,"suggested_status_key":suggested}));
        }
    }
    let plan = json!({"required":required,"unchanged":unchanged,"total_issues":total,"affected_issues":affected});
    if body.dry_run {
        return Ok(GoJson(json!({"dry_run":true,"plan":plan})));
    }
    for requirement in &required {
        let key = requirement["status_key"].as_str().unwrap_or_default();
        let n = requirement["issue_count"].as_i64().unwrap_or_default();
        let destination = body
            .status_mapping
            .get(key)
            .map(String::as_str)
            .unwrap_or("");
        let message = if destination.is_empty() {
            format!("choose a destination status for the {n} issue(s) on {key:?}")
        } else if !accepts(destination) {
            format!(
                "destination status {destination:?} for {key:?} is not part of the target workflow"
            )
        } else {
            continue;
        };
        return Err(WorkflowFailure(
            StatusCode::CONFLICT,
            json!({"error":message,"code":"workflow_status_mapping_required","plan":plan}),
        ));
    }
    if !required.is_empty() && state.workflow_publisher.is_none() {
        // Never commit a remap without the shared post-commit publishers.
        return Err(WorkflowFailure(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"workflow issue remapping is not available yet","plan":plan}),
        ));
    }
    let updated = tx.query_one("UPDATE project SET workflow_id=$3,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws,&target])
        .await.map_err(|_| internal("failed to change workflow"))?;
    let mapping: Vec<(String, String)> = required
        .iter()
        .map(|requirement| {
            let from = requirement["status_key"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let to = body.status_mapping[&from].clone();
            (from, to)
        })
        .collect();
    let effects =
        super::account_catalog_write_workflow_remap::remap_issues(&tx, ws, &[id], &mapping)
            .await
            .map_err(|_| internal("failed to move issues to the new statuses"))?;
    let updated_issues = effects.changed.len();
    let project = response(&tx, ws, &updated, None).await;
    tx.commit()
        .await
        .map_err(|_| internal("failed to change workflow"))?;
    drop(db);
    if let Some(publisher) = state.workflow_publisher.as_deref() {
        use super::account_catalog_write_workflow_publish::{
            WorkflowPublication, WorkflowPublicationContext, publish_remap,
        };
        let context = WorkflowPublicationContext {
            workspace_id: ws,
            actor_user_id: user,
        };
        publish_remap(publisher, &context, effects).await;
        publisher
            .publish(
                &context,
                WorkflowPublication::ProjectUpdated(project.clone()),
            )
            .await;
    } else {
        state
            .events
            .publish(super::runtime_chat_events::Event::project_updated(
                ws,
                user,
                project.clone(),
            ));
    }
    Ok(GoJson(
        json!({"project":project,"updated_issues":updated_issues,"plan":plan}),
    ))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid project id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to delete project"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to start transaction"))?;
    if tx
        .query_opt(
            "SELECT id FROM project WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal("failed to lock project"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "project not found"));
    }
    let role: String = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal("failed to load workspace role"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "project not found"))?
        .get(0);
    if !matches!(actor, Actor::Human) || !matches!(role.as_str(), "owner" | "admin") {
        return Err(error(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    tx.execute(
        "UPDATE chat_session SET project_id=NULL WHERE project_id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| internal("failed to clear project chat context"))?;
    tx.execute("WITH deleted AS (DELETE FROM issue_view WHERE workspace_id=$1 AND scope_type='project' AND scope_id=$2 RETURNING id) DELETE FROM pinned_item WHERE item_type='view' AND workspace_id=$1 AND item_id IN (SELECT id FROM deleted)", &[&ws,&id]).await.map_err(|_|internal("failed to delete project views"))?;
    tx.execute(
        "DELETE FROM project WHERE id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| internal("failed to delete project"))?;
    tx.commit()
        .await
        .map_err(|_| internal("failed to commit project delete"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::ProjectDeleted { project_id: id },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod project_request_tests {
    use super::*;

    #[test]
    fn update_decodes_go_pointer_and_exact_map_presence() {
        let cases = [
            (br#"{"TITLE":"renamed","title":null,"DESCRIPTION":"kept","description":null,"Due_Date":"2026-11-02"}"# as &[u8], None, None, true, false),
            (br#"{"title":"first","TITLE":null,"due_date":""} trailing"#, None, None, false, true),
        ];
        for (body, title, description, desc_set, due_set) in cases {
            let json_body = body.split(|b| *b == b' ').next().unwrap();
            let req: UpdateProject =
                super::super::account_catalog_write_profile::full_request(json_body).unwrap();
            let raw: HashMap<String, Box<serde_json::value::RawValue>> =
                super::super::account_catalog_write_profile::full_request(json_body).unwrap();
            assert_eq!(req.title.as_deref(), title);
            assert_eq!(req.description.as_deref(), description);
            assert_eq!(raw.contains_key("description"), desc_set);
            assert_eq!(raw.contains_key("due_date"), due_set);
        }
        let req: UpdateProject = super::super::account_catalog_write_profile::full_request(
            br#"{"lead_id":"00000000-0000-0000-0000-000000000001","LEAD_ID":null}"#,
        )
        .unwrap();
        assert!(req.lead_id.is_none());
        assert!(
            super::super::account_catalog_write_profile::full_request::<UpdateProject>(
                br#"{"title":"x"} trailing"#,
            )
            .is_err()
        );
        assert!(
            super::super::account_catalog_write_profile::full_request::<UpdateProject>(
                br#"{"description":5}"#,
            )
            .is_err()
        );
    }
}
