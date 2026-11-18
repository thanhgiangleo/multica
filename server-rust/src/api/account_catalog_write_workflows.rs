//! Workflow definition mutations; project assignment and issue transitions are separate.
#[path = "account_catalog_write_workflow_brief.rs"]
mod brief;
#[path = "account_catalog_write_decode.rs"]
pub(super) mod decode;
#[cfg(test)]
#[path = "account_catalog_write_decode_tests.rs"]
mod decode_tests;
#[path = "account_catalog_write_workflow_handoff.rs"]
mod handoff;
use super::account_catalog_write::view_preference::parse_go_uuid;
use super::account_catalog_write_catalog_publish::{
    CatalogChange, StatusAction, WorkflowAction, publish,
};
use super::{Actor, ApiState, Identity, catalog::WorkspaceId};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::{Row, Transaction, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issue-workflows", post(create))
        .route("/api/issue-workflows/", post(create))
        .route("/api/issue-workflows/{id}", delete(remove).patch(update))
        .route("/api/issue-workflows/{id}/", delete(remove).patch(update))
        .layer(DefaultBodyLimit::disable())
        .merge(handoff::routes())
}

pub(super) fn brief_routes() -> Router<ApiState> {
    brief::routes()
}

struct Failure(StatusCode, Value);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = super::GoJson(self.1).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn fail(status: StatusCode, message: impl Into<String>) -> Failure {
    Failure(status, json!({"error":message.into()}))
}
fn internal() -> Failure {
    fail(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to delete workflow",
    )
}

#[path = "account_catalog_write_workflow_decode.rs"]
pub(super) mod request_decode;
#[cfg(test)]
use request_decode::HandlerInput;
use request_decode::{NewStatus, StepInput, WriteRequest};

fn invalid(message: impl Into<String>) -> Failure {
    Failure(
        StatusCode::BAD_REQUEST,
        json!({"error":message.into(), "code":"invalid_workflow"}),
    )
}
fn save_failure() -> Failure {
    fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to save workflow")
}
fn fixed(key: &str) -> bool {
    matches!(key, "backlog" | "todo" | "blocked" | "done" | "cancelled")
}
fn step(key: &str) -> Value {
    json!({"status_key":key,"handler":{"type":"none"},"instructions":""})
}
fn arrange(steps: Vec<Value>) -> Vec<Value> {
    let mut head = Vec::new();
    let mut middle = Vec::new();
    let mut tail = Vec::new();
    for value in steps {
        let key = value["status_key"].as_str().unwrap_or_default();
        if fixed(key) {
            let bucket = if matches!(key, "backlog" | "todo") {
                &mut head
            } else {
                &mut tail
            };
            if !bucket.iter().any(|s: &Value| s["status_key"] == key) {
                bucket.push(value);
            }
        } else {
            middle.push(value);
        }
    }
    let mut output = Vec::new();
    for key in ["backlog", "todo"] {
        if let Some(pos) = head.iter().position(|s| s["status_key"] == key) {
            output.push(head.remove(pos));
        } else if key == "todo" {
            output.push(step(key));
        }
    }
    output.extend(middle);
    for key in ["blocked", "done", "cancelled"] {
        if let Some(pos) = tail.iter().position(|s| s["status_key"] == key) {
            output.push(tail.remove(pos));
        } else if matches!(key, "done" | "cancelled") {
            output.push(step(key));
        }
    }
    output
}
fn slug(name: &str, category: &str, taken: &HashSet<String>) -> Result<String, Failure> {
    let mut value = String::new();
    for c in name.trim().chars().flat_map(char::to_lowercase) {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            value.push(c);
        } else if !value.is_empty() && !value.ends_with('_') {
            value.push('_');
        }
    }
    value = value
        .trim_end_matches('_')
        .chars()
        .take(32)
        .collect::<String>()
        .trim_end_matches('_')
        .to_owned();
    if !value.is_empty() && reserved(&value) {
        return Err(invalid(format!(
            "{value:?} is a reserved status or category key and cannot be reused; rename the status or pass an explicit key"
        )));
    }
    let base = if value.is_empty() { category } else { &value };
    let occupied = |candidate: &str| reserved(candidate) || taken.contains(candidate);
    if !occupied(base) {
        return Ok(base.to_owned());
    }
    for i in 2..=(taken.len() + 13) {
        let suffix = format!("_{i}");
        let prefix = base.chars().take(32 - suffix.len()).collect::<String>();
        let candidate = format!("{}{suffix}", prefix.trim_end_matches('_'));
        if !occupied(&candidate) {
            return Ok(candidate);
        }
    }
    Err(invalid(
        "could not derive an unused status key from that name; provide one explicitly",
    ))
}
fn reserved(key: &str) -> bool {
    matches!(
        key,
        "backlog"
            | "todo"
            | "in_progress"
            | "in_review"
            | "blocked"
            | "done"
            | "cancelled"
            | "unstarted"
            | "started"
            | "closed"
    )
}
fn category(raw: &str) -> &str {
    match raw {
        "backlog" | "todo" | "unstarted" => "unstarted",
        "in_progress" | "in_review" | "blocked" | "started" => "started",
        "done" | "completed" => "done",
        "cancelled" | "canceled" | "closed" => "closed",
        _ => "started",
    }
}
async fn resolve_status(
    tx: &Transaction<'_>,
    ws: Uuid,
    input: NewStatus,
    admin: bool,
) -> Result<(String, bool), Failure> {
    let name = input.name.trim();
    if name.is_empty() || name.chars().count() > 64 {
        return Err(invalid("status name must be 1-64 characters"));
    }
    let category = category(&input.category);
    let color = if input.color.trim().is_empty() {
        match category {
            "unstarted" => "#94a3b8",
            "started" => "#3b82f6",
            "done" => "#22c55e",
            _ => "#64748b",
        }
        .to_owned()
    } else {
        let value = input.color.trim().trim_start_matches('#');
        if value.len() != 6 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid("color must be a 6-digit hex value like #3b82f6"));
        }
        format!("#{}", value.to_ascii_lowercase())
    };
    // Reuse active names, independent of the requested category/color.
    if let Some(row)=tx.query_opt("SELECT key FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL AND lower(name)=lower($2) ORDER BY key LIMIT 1", &[&ws,&name]).await.map_err(|_| save_failure())? {
        return Ok((row.get(0),false));
    }
    if !admin {
        return Err(Failure(
            StatusCode::FORBIDDEN,
            json!({"error":"only workspace admins can create statuses","code":"status_create_forbidden"}),
        ));
    }
    let taken: HashSet<String> = tx
        .query("SELECT key FROM issue_status WHERE workspace_id=$1", &[&ws])
        .await
        .map_err(|_| save_failure())?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    let key = slug(name, category, &taken)?;
    tx.execute("INSERT INTO issue_status(workspace_id,key,name,description,category,color,icon,position) VALUES($1,$2,$3,'',$4,$5,'',COALESCE((SELECT max(position)+1 FROM issue_status WHERE workspace_id=$1 AND category=$4),0))",&[&ws,&key,&name,&category,&color]).await.map_err(|e| {
        if e.code()==Some(&SqlState::UNIQUE_VIOLATION) { invalid(format!("a status named {name:?} already exists")) } else { save_failure() }
    })?;
    Ok((key, true))
}
fn validate(
    name: &str,
    description: &str,
    steps: &[Value],
    catalog: &HashSet<String>,
) -> Result<(), Failure> {
    if name.is_empty() || name.chars().count() > 64 {
        return Err(invalid("name must be 1-64 characters"));
    }
    if name.eq_ignore_ascii_case("Default") {
        return Err(invalid(
            "\"Default\" is reserved for the workspace default workflow",
        ));
    }
    if description.chars().count() > 256 {
        return Err(invalid("description must be at most 256 characters"));
    }
    if steps.len() > 50 {
        return Err(invalid("a workflow can have at most 50 statuses"));
    }
    let mut seen = HashSet::new();
    for s in steps {
        let key = s["status_key"].as_str().unwrap_or_default();
        if !catalog.contains(key) && !reserved_builtin(key) {
            return Err(invalid(format!("status {key:?} does not exist")));
        }
        if !seen.insert(key) {
            return Err(invalid(format!("status {key:?} appears more than once")));
        }
    }
    for s in steps {
        let key = s["status_key"].as_str().unwrap_or_default();
        let handler = s["handler"]["type"].as_str().unwrap_or_default();
        match handler {
            "none" | "project_lead" => {}
            "agent" | "member" if s["handler"]["id"].as_str().unwrap_or_default().is_empty() => {
                return Err(invalid(format!("choose a {handler} to hand off to")));
            }
            "agent" | "member" => {}
            "squad" | "creator" => {
                return Err(invalid(
                    "hand off to an agent, a member or the project lead",
                ));
            }
            _ => return Err(invalid(format!("unknown handler type {handler:?}"))),
        }
        if handler != "none" && matches!(key, "backlog" | "done" | "cancelled") {
            return Err(invalid(format!(
                "issues entering {key:?} cannot be handed off"
            )));
        }
        if s["instructions"]
            .as_str()
            .unwrap_or_default()
            .chars()
            .count()
            > 8000
        {
            return Err(invalid("instructions must be at most 8000 characters"));
        }
        for field in ["next_status_key", "back_status_key"] {
            if let Some(reference) = s[field].as_str().filter(|v| !v.is_empty())
                && (reference == key || !seen.contains(reference))
            {
                return Err(invalid(format!(
                    "{reference:?} is not another status of this workflow"
                )));
            }
        }
    }
    Ok(())
}
fn reserved_builtin(key: &str) -> bool {
    matches!(
        key,
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
    )
}
fn response(row: &Row, steps: &[Value]) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "name":row.get::<_,String>("name"), "description":row.get::<_,String>("description"),
        "is_default":false,"owner_id":row.get::<_,Option<Uuid>>("owner_id"),
        "steps":steps, "project_ids":[], "created_at":super::timestamp(row.get("created_at")),
        "updated_at":super::timestamp(row.get("updated_at"))
    })
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, super::GoJson<Value>), Failure> {
    let body = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let body: WriteRequest = super::account_catalog_write_profile::account_request(&body)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let mut db = state.pool.get().await.map_err(|_| save_failure())?;
    let tx = db.transaction().await.map_err(|_| save_failure())?;
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| save_failure())?;
    let role: String = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| save_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?
        .get(0);
    let admin = matches!(actor, Actor::Human) && matches!(role.as_str(), "owner" | "admin");
    super::workflow::ensure_default(&*tx, ws)
        .await
        .map_err(|_| save_failure())?;
    // Go seeds catalog rows in the write transaction before resolving status inputs.
    tx.execute("INSERT INTO issue_status(workspace_id,key,name,description,category,color,is_system,position) VALUES ($1,'backlog','Backlog','Parked. Assigning an issue here never starts an agent run.','unstarted','#6b7280',true,0),($1,'todo','Todo','Queued for work. Moving an issue here starts the assigned agent.','unstarted','#6b7280',true,0),($1,'in_progress','In Progress','Actively being worked on.','started','#f59e0b',true,0),($1,'in_review','In Review','Work delivered, waiting on human review. Finalizes the autopilot run.','started','#22c55e',true,0),($1,'done','Done','Completed.','done','#3b82f6',true,0),($1,'blocked','Blocked','Stalled on an external dependency.','started','#ef4444',true,0),($1,'cancelled','Cancelled','Decided not to do.','closed','#6b7280',true,0) ON CONFLICT DO NOTHING", &[&ws]).await.map_err(|_| save_failure())?;
    let mut raw = Vec::new();
    let mut created = false;
    for input in body.steps.unwrap_or_default() {
        let key = if let Some(status) = input.new_status {
            let (key, is_new) = resolve_status(&tx, ws, status, admin).await?;
            created |= is_new;
            key
        } else {
            input.status_key
        };
        let handler = input.handler.unwrap_or_default();
        let kind = if handler.r#type.trim().is_empty() {
            "none"
        } else {
            handler.r#type.trim()
        };
        let id = if matches!(kind, "member" | "agent" | "squad") {
            handler.id.trim()
        } else {
            ""
        };
        let instructions = if kind == "none" {
            ""
        } else {
            input.instructions.trim()
        };
        let mut value =
            json!({"status_key":key.trim(),"handler":{"type":kind},"instructions":instructions});
        if !id.is_empty() {
            value["handler"]["id"] = json!(id);
        }
        if !input.next_status_key.trim().is_empty() {
            value["next_status_key"] = json!(input.next_status_key.trim());
        }
        if !input.back_status_key.trim().is_empty() {
            value["back_status_key"] = json!(input.back_status_key.trim());
        }
        raw.push(value);
    }
    let steps = arrange(raw);
    let name = body.name.unwrap_or_default().trim().to_owned();
    let description = body.description.unwrap_or_default().trim().to_owned();
    let catalog: HashSet<String> = tx
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL",
            &[&ws],
        )
        .await
        .map_err(|_| save_failure())?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    validate(&name, &description, &steps, &catalog)?;
    for value in &steps {
        let kind = value["handler"]["type"].as_str().unwrap_or_default();
        if matches!(kind, "member" | "agent") {
            let raw = value["handler"]["id"].as_str().unwrap_or_default();
            let id = parse_go_uuid(raw).ok_or_else(|| {
                Failure(
                    StatusCode::BAD_REQUEST,
                    json!({"error":"invalid handler id","code":"invalid_handoff"}),
                )
            })?;
            if kind == "member" {
                if tx
                    .query_opt(
                        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                        &[&ws, &id],
                    )
                    .await
                    .map_err(|_| save_failure())?
                    .is_none()
                {
                    return Err(Failure(
                        StatusCode::BAD_REQUEST,
                        json!({"error":"assignee_id does not refer to a member of this workspace","code":"invalid_handoff"}),
                    ));
                }
            } else {
                // Invoker ACL belongs to the shared auth owner. Until it is available,
                // never persist an agent handoff whose invocation rights are unknown.
                return Err(Failure(
                    StatusCode::FORBIDDEN,
                    json!({"error":"you do not have permission to assign work to this agent","code":"invalid_handoff"}),
                ));
            }
        }
    }
    let stored: Vec<Value> = steps
        .iter()
        .map(|s| {
            let mut value = json!({"status_key":s["status_key"]});
            for field in ["next_status_key", "back_status_key"] {
                if let Some(text) = s[field].as_str().filter(|v| !v.is_empty()) {
                    value[field] = json!(text);
                }
            }
            if s["handler"]["type"] != "none" {
                value["handler"] = s["handler"].clone();
                if let Some(text) = s["instructions"].as_str().filter(|v| !v.is_empty()) {
                    value["instructions"] = json!(text);
                }
            }
            value
        })
        .collect();
    let row=tx.query_one("INSERT INTO issue_workflow(workspace_id,name,description,initial_status_key,steps,owner_id) VALUES($1,$2,$3,'todo',$4,$5) RETURNING *",&[&ws,&name,&description,&json!(stored),&user]).await.map_err(|e| {
        if e.code()==Some(&SqlState::UNIQUE_VIOLATION) {
            Failure(StatusCode::CONFLICT,json!({"error":"a workflow with this name already exists","code":"workflow_name_taken"}))
        } else {save_failure()}
    })?;
    let result = response(&row, &steps);
    tx.commit().await.map_err(|_| save_failure())?;
    drop(db);
    if created {
        publish(
            &state,
            ws,
            user,
            CatalogChange::StatusChanged {
                action: StatusAction::Created,
            },
        )
        .await;
    }
    publish(
        &state,
        ws,
        user,
        CatalogChange::WorkflowChanged {
            action: WorkflowAction::Created,
        },
    )
    .await;
    Ok((StatusCode::CREATED, super::GoJson(result)))
}

// Mapped writes require an adapter to the shared issue/task/catalog publishers.
async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<super::GoJson<Value>, Failure> {
    let id =
        parse_go_uuid(&raw).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workflow id"))?;
    let body = body.map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let body: WriteRequest = super::account_catalog_write_profile::account_request(&body)
        .map_err(|_| fail(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let mut db = state.pool.get().await.map_err(|_| save_failure())?;
    let tx = db.transaction().await.map_err(|_| save_failure())?;
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| save_failure())?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| save_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    let role: String = member.get(0);
    let row = tx
        .query_opt(
            "SELECT * FROM issue_workflow WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| save_failure())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workflow not found"))?;
    if !matches!(actor, Actor::Human)
        || !matches!(role.as_str(), "owner" | "admin")
            && row.get::<_, Option<Uuid>>("owner_id") != Some(user)
    {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "only the workflow owner and workspace admins can change this workflow",
        ));
    }
    let is_default: bool = row.get("is_default");
    let previous: Value = row.get("steps");
    let previous_handlers: HashSet<(String, String)> = previous
        .as_array()
        .into_iter()
        .flatten()
        .map(|step| {
            (
                step["handler"]["type"]
                    .as_str()
                    .unwrap_or("none")
                    .to_owned(),
                step["handler"]["id"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect();
    let mut name: String = row.get("name");
    let mut description: String = row.get("description");
    if !is_default && let Some(value) = body.name {
        name = value.trim().into();
    }
    if let Some(value) = body.description {
        description = value.trim().into();
    }
    let mut created = false;
    let steps = if let Some(inputs) = body.steps {
        // Go snapshots Default before adding any new catalog statuses.
        super::workflow::ensure_default(&*tx, ws)
            .await
            .map_err(|_| save_failure())?;
        let mut raw = Vec::new();
        for input in inputs {
            let key = if let Some(status) = input.new_status {
                let (key, was_created) =
                    resolve_status(&tx, ws, status, matches!(role.as_str(), "owner" | "admin"))
                        .await?;
                created |= was_created;
                key
            } else {
                input.status_key
            };
            let handler = input.handler.unwrap_or_default();
            let kind = if handler.r#type.trim().is_empty() {
                "none"
            } else {
                handler.r#type.trim()
            };
            let mut value = json!({
                "status_key":key.trim(), "handler":{"type":kind},
                "instructions":if kind=="none" { "" } else { input.instructions.trim() }
            });
            if matches!(kind, "member" | "agent" | "squad") && !handler.id.trim().is_empty() {
                value["handler"]["id"] = json!(handler.id.trim());
            }
            if !input.next_status_key.trim().is_empty() {
                value["next_status_key"] = json!(input.next_status_key.trim());
            }
            if !input.back_status_key.trim().is_empty() {
                value["back_status_key"] = json!(input.back_status_key.trim());
            }
            raw.push(value);
        }
        arrange(raw)
    } else {
        let raw = previous
            .as_array()
            .ok_or_else(|| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to read workflow"))?;
        arrange(raw.iter().map(|s| {
            let kind = s["handler"]["type"].as_str().unwrap_or("none");
            let mut value=json!({"status_key":s["status_key"], "handler":{"type":kind},
                "instructions":if kind=="none" { "" } else { s["instructions"].as_str().unwrap_or("") }});
            if let Some(id)=s["handler"]["id"].as_str() { value["handler"]["id"]=json!(id); }
            for field in ["next_status_key","back_status_key"] {
                if let Some(text)=s[field].as_str() { value[field]=json!(text); }
            }
            value
        }).collect())
    };
    let catalog_rows = tx
        .query(
            "SELECT key,category FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL",
            &[&ws],
        )
        .await
        .map_err(|_| save_failure())?;
    let catalog: HashSet<String> = catalog_rows.iter().map(|r| r.get(0)).collect();
    let categories: HashMap<String, String> =
        catalog_rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    if is_default {
        // Default is exempt from name validation, but its other fields are not.
        validate("Default placeholder", &description, &steps, &catalog)?;
    } else {
        validate(&name, &description, &steps, &catalog)?;
    }
    for value in &steps {
        let kind = value["handler"]["type"].as_str().unwrap_or("none");
        let raw = value["handler"]["id"].as_str().unwrap_or("");
        if matches!(kind, "member" | "agent")
            && !previous_handlers.contains(&(kind.to_owned(), raw.to_owned()))
        {
            let handler_id = parse_go_uuid(raw).ok_or_else(|| {
                Failure(
                    StatusCode::BAD_REQUEST,
                    json!({"error":"invalid handler id","code":"invalid_handoff"}),
                )
            })?;
            if kind == "agent" {
                return Err(Failure(
                    StatusCode::FORBIDDEN,
                    json!({"error":"you do not have permission to assign work to this agent","code":"invalid_handoff"}),
                ));
            }
            if tx
                .query_opt(
                    "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&ws, &handler_id],
                )
                .await
                .map_err(|_| save_failure())?
                .is_none()
            {
                return Err(Failure(
                    StatusCode::BAD_REQUEST,
                    json!({"error":"assignee_id does not refer to a member of this workspace","code":"invalid_handoff"}),
                ));
            }
        }
    }
    let projects=if is_default {
        tx.query("SELECT id FROM project WHERE workspace_id=$1 AND (workflow_id IS NULL OR workflow_id=$2 OR NOT EXISTS (SELECT 1 FROM issue_workflow WHERE id=project.workflow_id AND workspace_id=$1)) ORDER BY created_at,id", &[&ws,&id]).await
    } else {
        tx.query("SELECT id FROM project WHERE workspace_id=$1 AND workflow_id=$2 ORDER BY created_at,id", &[&ws,&id]).await
    }.map_err(|_|save_failure())?;
    let project_ids: Vec<Uuid> = projects.iter().map(|p| p.get(0)).collect();
    let allowed: HashSet<String> = steps
        .iter()
        .filter_map(|s| s["status_key"].as_str().map(str::to_owned))
        .collect();
    let mut required = Vec::new();
    let mut unchanged = Vec::new();
    let mut total = 0i64;
    let mut affected = 0i64;
    if !is_default && !project_ids.is_empty() {
        for count in tx.query("SELECT status,count(*)::bigint FROM issue WHERE workspace_id=$1 AND project_id=ANY($2::uuid[]) GROUP BY status ORDER BY status", &[&ws,&project_ids]).await.map_err(|_|save_failure())? {
            let status:String=count.get(0);
            let n:i64=count.get(1);
            total+=n;
            if allowed.contains(&status) {
                unchanged.push(json!({"status_key":status,"issue_count":n}));
            } else {
                affected+=n;
                let category=categories.get(&status);
                let suggested=steps.iter().filter_map(|s|s["status_key"].as_str()).find(|s| category.is_some() && categories.get(*s)==category).unwrap_or("todo");
                required.push(json!({"status_key":status,"issue_count":n,"suggested_status_key":suggested}));
            }
        }
    }
    let plan = json!({"required":required,"unchanged":unchanged,"total_issues":total,"affected_issues":affected});
    if body.dry_run {
        // New statuses, if any, are rolled back as part of the dry run.
        return Ok(super::GoJson(json!({"dry_run":true,"plan":plan})));
    }
    for requirement in &required {
        let key = requirement["status_key"].as_str().unwrap_or_default();
        let n = requirement["issue_count"].as_i64().unwrap_or_default();
        let target = body
            .status_mapping
            .get(key)
            .map(String::as_str)
            .unwrap_or("");
        let message = if target.is_empty() {
            format!("choose a destination status for the {n} issue(s) on {key:?}")
        } else if !allowed.contains(target) {
            format!("destination status {target:?} for {key:?} is not part of the target workflow")
        } else {
            continue;
        };
        return Err(Failure(
            StatusCode::CONFLICT,
            json!({"error":message,"code":"workflow_status_mapping_required","plan":plan}),
        ));
    }
    if !required.is_empty() && state.workflow_publisher.is_none() {
        // Never strand issues without shared snapshots, wakeup cancellation and broadcasts.
        return Err(Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"workflow issue remapping is not available yet","plan":plan}),
        ));
    }
    let stored: Vec<Value> = steps
        .iter()
        .map(|s| {
            let mut value = json!({"status_key":s["status_key"]});
            for field in ["next_status_key", "back_status_key"] {
                if let Some(text) = s[field].as_str().filter(|v| !v.is_empty()) {
                    value[field] = json!(text);
                }
            }
            if s["handler"]["type"] != "none" {
                value["handler"] = s["handler"].clone();
                if let Some(text) = s["instructions"].as_str().filter(|v| !v.is_empty()) {
                    value["instructions"] = json!(text);
                }
            }
            value
        })
        .collect();
    let updated=tx.query_one("UPDATE issue_workflow SET name=$3,description=$4,initial_status_key='todo',steps=$5,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws,&name,&description,&json!(stored)]).await.map_err(|e| {
        if e.code()==Some(&SqlState::UNIQUE_VIOLATION) { Failure(StatusCode::CONFLICT,json!({"error":"a workflow with this name already exists","code":"workflow_name_taken"})) } else {save_failure()}
    })?;
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
        super::account_catalog_write_workflow_remap::remap_issues(&tx, ws, &project_ids, &mapping)
            .await
            .map_err(|_| {
                fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to move issues to the new statuses",
                )
            })?;
    tx.commit().await.map_err(|_| save_failure())?;
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
        if created {
            publisher
                .publish(&context, WorkflowPublication::StatusCatalogCreated)
                .await;
        }
        publisher
            .publish(&context, WorkflowPublication::WorkflowUpdated)
            .await;
    }
    let mut result = response(&updated, &steps);
    result["is_default"] = json!(is_default);
    result["project_ids"] = json!(project_ids);
    Ok(super::GoJson(result))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, Failure> {
    let id =
        parse_go_uuid(&raw).ok_or_else(|| fail(StatusCode::BAD_REQUEST, "invalid workflow id"))?;
    let mut db = state.pool.get().await.map_err(|_| internal())?;
    let tx = db.transaction().await.map_err(|_| internal())?;
    // Serialize with status creation/archive, workflow edits and project workflow switches.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| internal())?;
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    // Task/cloud credentials must never inherit the human member's edit permissions.
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "only the workflow owner and workspace admins can change this workflow",
        ));
    }
    let row = tx.query_opt(
        "SELECT is_default,owner_id FROM issue_workflow WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&id, &ws],
    ).await.map_err(|_| internal())?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND,"workflow not found"))?;
    let role: String = member.get("role");
    if role != "owner" && role != "admin" && row.get::<_, Option<Uuid>>("owner_id") != Some(user) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "only the workflow owner and workspace admins can change this workflow",
        ));
    }
    if row.get::<_, bool>("is_default") {
        return Err(Failure(
            StatusCode::BAD_REQUEST,
            json!({
                "error":"the Default workflow cannot be deleted", "code":"workflow_is_default"
            }),
        ));
    }
    let count: i64 = tx
        .query_one(
            "SELECT count(*) FROM project WHERE workspace_id=$1 AND workflow_id=$2",
            &[&ws, &id],
        )
        .await
        .map_err(|_| internal())?
        .get(0);
    if count > 0 {
        return Err(Failure(
            StatusCode::CONFLICT,
            json!({
                "error":format!("{count} project(s) still use this workflow; switch them to another workflow first"),
                "code":"workflow_in_use", "project_count":count,
            }),
        ));
    }
    let deleted = tx
        .execute(
            "DELETE FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal())?;
    if deleted == 0 {
        return Err(fail(StatusCode::NOT_FOUND, "workflow not found"));
    }
    tx.commit().await.map_err(|_| internal())?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::WorkflowChanged {
            action: WorkflowAction::Deleted,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}
