//! Workspace-scoped catalogs mirrored from the staged Go query/handler contracts.
use super::{Actor, ApiError, ApiState, GoJson, Identity, WorkspaceSelectors, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

#[path = "p0_issue_trigger_preview.rs"]
mod p0_issue_trigger_preview;
#[path = "p0_workflow_handoff.rs"]
mod p0_workflow_handoff;

#[derive(Clone)]
pub(super) struct WorkspaceId(pub(super) Uuid);

type Params = HashMap<String, String>;

// net/url.Values.Get selects the first value of a repeated query key.
pub(super) fn parse_params(raw: Option<&str>) -> Params {
    let mut values = Params::new();
    if let Some(raw) = raw {
        for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
            values
                .entry(key.into_owned())
                .or_insert_with(|| value.into_owned());
        }
    }
    values
}

pub(super) fn routes(state: ApiState) -> Router<ApiState> {
    Router::new()
        .merge(if state.issue_events.is_some() {
            super::issue_write::routes()
        } else {
            Router::new()
        })
        .route("/api/projects", get(projects))
        .route("/api/projects/", get(projects))
        .route("/api/projects/search", get(search_projects))
        .route("/api/projects/{id}", get(project))
        .route("/api/projects/{id}/", get(project))
        .route("/api/projects/{id}/resources", get(project_resources))
        .merge(super::account_catalog_write_resources::routes())
        .merge(super::account_catalog_write_projects::routes())
        .merge(
            if state.issue_events.is_some() || state.catalog_publisher.is_some() {
                super::account_catalog_write_projects::workflow_routes()
            } else {
                Router::new()
            },
        )
        .merge(super::workflow::routes())
        .merge(super::account_catalog_write_workflows::brief_routes())
        .merge(
            if state.issue_events.is_some() || state.catalog_publisher.is_some() {
                super::account_catalog_write_workflows::routes()
            } else {
                Router::new()
            },
        )
        .merge(super::squad::routes())
        .route(
            "/api/issues/{id}/squad-evaluated",
            post(super::squad_evaluation::record),
        )
        .merge(super::agents_squads_sprint::routes())
        .merge(super::p0_agents::routes())
        .merge(super::skill::routes())
        .merge(if state.skill_events.is_some() {
            super::runtime_skills_sprint::routes()
        } else {
            Router::new()
        })
        .route("/api/runtimes", get(super::runtime::list))
        .route("/api/runtimes/", get(super::runtime::list))
        .merge(if state.runtime_metadata_events.is_some() {
            super::runtime_skills_sprint_manage::routes()
        } else {
            Router::new()
        })
        .merge(if state.runtime_rpc_transport.is_some() {
            super::runtime_skills_sprint_control::user_routes()
        } else {
            Router::new()
        })
        .merge(
            if state.runtime_rpc_transport.is_some() && state.skill_events.is_some() {
                super::runtime_skills_sprint_local_import::user_routes()
            } else {
                Router::new()
            },
        )
        .merge(super::dashboard_daily::routes())
        .merge(super::issue_read::routes())
        .merge(p0_workflow_handoff::routes())
        .merge(p0_issue_trigger_preview::routes())
        .merge(super::files_ops_sprint::workspace_routes())
        .merge(super::issue_relations_read::routes())
        .merge(super::autopilot_history_read::routes())
        .merge(super::automation_sprint::routes())
        .route(
            "/api/issues/child-progress",
            get(super::child_progress::get),
        )
        .merge(super::issue_wakeup::routes())
        .route("/api/issue-wakeups", get(super::workspace_wakeup::list))
        .merge(super::issue_view::routes())
        .merge(super::account_catalog_write_views::routes())
        .merge(super::chat_pin::routes())
        .merge(super::runtime_chat::routes())
        .merge(super::runtime_chat_supplement::routes())
        .merge(super::runtime_chat_pinned::routes())
        .merge(super::runtime_chat_queue::routes())
        .merge(super::chat_pending::routes())
        .merge(super::chat_draft_restore::routes())
        .merge(super::p0_chat::routes())
        .merge(super::p0_tasks::routes())
        .merge(super::inbox::routes())
        .merge(super::queries_sprint_inbox::routes())
        .merge(super::queries_sprint_search::routes())
        .merge(super::queries_sprint_issue_search::routes())
        .merge(super::queries_sprint_issue_list::routes())
        .merge(super::queries_sprint_issue_detail::routes())
        .merge(super::queries_sprint_pull_requests::routes())
        .merge(super::queries_sprint_comments::routes())
        .merge(super::queries_sprint_timeline::routes())
        .merge(super::queries_sprint_table_rows::routes())
        .merge(super::queries_sprint_task_snapshot::routes())
        .merge(super::queries_sprint_pins::routes())
        .merge(super::queries_sprint_quick_action_writes::routes())
        .route("/api/inbox/archived/page", get(super::archived_page::get))
        .route(
            "/api/inbox/archived/facets",
            get(super::archived_facets::get),
        )
        .merge(super::agent_activity::routes())
        .route("/api/issue-wakeup-paused", get(super::paused_wakeup::list))
        .route("/api/system-wakeups", get(super::system_wakeup::list))
        .route(
            "/api/system-wakeups/{rule}",
            axum::routing::put(super::system_wakeup::put),
        )
        .route(
            "/api/issue-wakeup-summaries",
            get(super::wakeup_summary::list),
        )
        .route(
            "/api/assignee-frequency",
            get(super::assignee_frequency::get),
        )
        .route("/api/labels", get(labels))
        .route("/api/labels/", get(labels))
        .route("/api/labels/{id}/", get(label))
        .merge(super::account_catalog_write_labels::routes())
        .route("/api/properties", get(properties))
        .route("/api/properties/", get(properties))
        .route("/api/properties/{id}/", get(property))
        .merge(super::account_catalog_write_properties::routes())
        .route("/api/issue-statuses", get(issue_statuses))
        .route("/api/issue-statuses/", get(issue_statuses))
        .merge(
            if state.issue_events.is_some() || state.catalog_publisher.is_some() {
                super::account_catalog_write_statuses::routes()
            } else {
                Router::new()
            },
        )
        .route(
            "/api/issue-view-preferences",
            get(super::personal_read::issue_view_preference)
                .put(super::account_catalog_write::put_issue_view_preference),
        )
        .route(
            "/api/notification-preferences",
            get(super::personal_read::notifications)
                .put(super::account_catalog_write::put_notification_preferences)
                .patch(super::account_catalog_write::patch_notification_preferences),
        )
        .route(
            "/api/notification-preferences/",
            get(super::personal_read::notifications)
                .put(super::account_catalog_write::put_notification_preferences)
                .patch(super::account_catalog_write::patch_notification_preferences),
        )
        .route("/api/quick-actions", get(super::quick_action::list))
        .route("/api/quick-actions/", get(super::quick_action::list))
        .route("/api/pins", get(super::personal_read::pins))
        .route("/api/pins/", get(super::personal_read::pins))
        .route_layer(middleware::from_fn_with_state(state, workspace_guard))
}

async fn workspace_guard(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(selectors): Extension<WorkspaceSelectors>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    // Go's workspace middleware gives query selectors priority over headers,
    // except for a task token: its verified DB binding is authoritative.
    let params = if actor.workspace().is_some() {
        Params::new()
    } else {
        parse_params(request.uri().query())
    };
    let slug = params
        .get("workspace_slug")
        .filter(|s| !s.is_empty())
        .map(String::as_str)
        .or_else(|| selectors.slug.as_deref().filter(|s| !s.is_empty()));
    let id = params
        .get("workspace_id")
        .filter(|s| !s.is_empty())
        .map(String::as_str)
        .or_else(|| selectors.id.as_deref().filter(|s| !s.is_empty()));
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let workspace = if let Some(slug) = slug {
        db.query_opt("SELECT id FROM workspace WHERE slug = $1", &[&slug])
            .await
            .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
            .map(|row| row.get::<_, Uuid>(0))
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
    } else {
        let raw = id.ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "workspace_id or workspace_slug is required",
            )
        })?;
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?
    };
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id = $1 AND user_id = $2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    drop(db);
    request.extensions_mut().insert(WorkspaceId(workspace));
    Ok(next.run(request).await)
}

fn required_id(raw: &str, name: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, name))
}

fn opt_timestamp(row: &Row, col: &str) -> Option<String> {
    row.get::<_, Option<DateTime<Utc>>>(col).map(timestamp)
}

#[derive(Serialize)]
struct Project {
    id: Uuid,
    workspace_id: Uuid,
    title: String,
    description: Option<String>,
    icon: Option<String>,
    status: String,
    priority: String,
    lead_type: Option<String>,
    lead_id: Option<Uuid>,
    start_date: Option<String>,
    due_date: Option<String>,
    created_at: String,
    updated_at: String,
    issue_count: i64,
    done_count: i64,
    resource_count: i64,
    workflow_id: Option<Uuid>,
}
fn project_row(r: &Row) -> Project {
    Project {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        title: r.get("title"),
        description: r.get("description"),
        icon: r.get("icon"),
        status: r.get("status"),
        priority: r.get("priority"),
        lead_type: r.get("lead_type"),
        lead_id: r.get("lead_id"),
        start_date: r
            .get::<_, Option<NaiveDate>>("start_date")
            .map(|d| d.to_string()),
        due_date: r
            .get::<_, Option<NaiveDate>>("due_date")
            .map(|d| d.to_string()),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
        issue_count: 0,
        done_count: 0,
        resource_count: 0,
        workflow_id: r.get("workflow_id"),
    }
}

async fn project_counts(db: &impl GenericClient, ws: Uuid, projects: &mut [Project]) {
    if projects.is_empty() {
        return;
    }
    let ids: Vec<Uuid> = projects.iter().map(|p| p.id).collect();
    // Go falls back to canonical terminal keys if catalog expansion fails.
    let terminal = db
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND category = ANY($2::text[])",
            &[&ws, &vec!["done", "closed"]],
        )
        .await
        .map(|rows| {
            let mut keys: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
            for built_in in ["done", "cancelled"] {
                if !keys.iter().any(|key| key == built_in) {
                    keys.push(built_in.to_owned());
                }
            }
            keys
        })
        .unwrap_or_else(|_| vec!["done".into(), "cancelled".into()]);
    if let Ok(rows) = db.query("SELECT project_id, count(*)::bigint AS total_count, count(*) FILTER (WHERE status = ANY($3::text[]))::bigint AS done_count FROM issue WHERE workspace_id=$1 AND project_id = ANY($2::uuid[]) GROUP BY project_id",
        &[&ws, &ids, &terminal]).await {
        for row in rows {
            if let Some(p) = projects.iter_mut().find(|p| p.id == row.get::<_, Uuid>(0)) {
                p.issue_count = row.get(1);
                p.done_count = row.get(2);
            }
        }
    }
    if let Ok(rows) = db.query("SELECT project_id, count(*)::bigint FROM project_resource WHERE project_id = ANY($1::uuid[]) GROUP BY project_id", &[&ids]).await {
        for row in rows {
            if let Some(p) = projects.iter_mut().find(|p| p.id == row.get::<_, Uuid>(0)) {
                p.resource_count = row.get(1);
            }
        }
    }
}

async fn projects(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let status = params.get("status").filter(|v| !v.is_empty());
    let priority = params.get("priority").filter(|v| !v.is_empty());
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list projects"))?;
    let rows = db.query("SELECT * FROM project WHERE workspace_id=$1 AND ($2::text IS NULL OR status=$2) AND ($3::text IS NULL OR priority=$3) ORDER BY created_at DESC",
        &[&ws, &status, &priority]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list projects"))?;
    let mut out: Vec<_> = rows.iter().map(project_row).collect();
    project_counts(&**db, ws, &mut out).await;
    Ok(GoJson(json!({"total":out.len(),"projects":out})))
}
#[derive(Serialize)]
struct SearchProject {
    #[serde(flatten)]
    project: Project,
    match_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_snippet: Option<String>,
}

fn escape_search_like(s: &str) -> String {
    s.to_lowercase()
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn search_snippet(content: &str, query: &str) -> String {
    let chars: Vec<_> = content.chars().collect();
    let lower: Vec<_> = content.to_lowercase().chars().collect();
    let phrase: Vec<_> = query.to_lowercase().chars().collect();
    let find = |needle: &[char]| lower.windows(needle.len()).position(|s| s == needle);
    let mut hit = find(&phrase).map(|pos| (pos, phrase.len()));
    if hit.is_none() {
        let terms: Vec<_> = query.split_whitespace().collect();
        if terms.len() > 1 {
            hit = terms
                .iter()
                .filter_map(|term| {
                    let needle: Vec<_> = term.to_lowercase().chars().collect();
                    find(&needle).map(|pos| (pos, needle.len()))
                })
                .min_by_key(|(pos, _)| *pos);
        }
    }
    let Some((pos, length)) = hit else {
        return if chars.len() > 120 {
            format!("{}...", chars[..120].iter().collect::<String>())
        } else {
            content.to_owned()
        };
    };
    let start = pos.saturating_sub(40);
    let end = (pos + length + 80).min(chars.len());
    format!(
        "{}{}{}",
        if start > 0 { "..." } else { "" },
        chars[start..end].iter().collect::<String>(),
        if end < chars.len() { "..." } else { "" }
    )
}

fn search_work_mem_mb(raw: Option<&str>) -> i32 {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<i32>()
                .ok()
                .filter(|v| (0..=64).contains(v))
                .unwrap_or(64)
        })
        .unwrap_or(64)
}

async fn search_projects(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let query = params.get("q").map(String::as_str).unwrap_or("");
    if query.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "q parameter is required"));
    }
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(20)
        .min(50);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(0);
    let include_closed = params.get("include_closed").is_some_and(|v| v == "true");
    let phrase = escape_search_like(query);
    let terms: Vec<_> = query.split_whitespace().map(escape_search_like).collect();
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to search projects",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to search projects",
        )
    })?;
    let mem = search_work_mem_mb(std::env::var("DATABASE_SEARCH_WORK_MEM_MB").ok().as_deref());
    let settings = if mem == 0 {
        "SET LOCAL statement_timeout = 8000; SET LOCAL transaction_read_only = on".to_owned()
    } else {
        format!(
            "SET LOCAL statement_timeout = 8000; SET LOCAL work_mem = '{mem}MB'; SET LOCAL transaction_read_only = on"
        )
    };
    tx.batch_execute(&settings).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to search projects",
        )
    })?;
    // Keep the SELECT within the transaction so timeout and read-only settings do not
    // leak to the next borrower of this pooled connection.
    let rows = tx.query(
        "SELECT p.id,p.workspace_id,p.title,p.description,p.icon,p.status,p.priority,
                p.lead_type,p.lead_id,p.start_date,p.due_date,p.created_at,p.updated_at,
                NULL::uuid AS workflow_id,
                CASE WHEN lower(p.title) LIKE '%' || $1::text || '%'
                       OR (cardinality($3::text[]) > 1 AND NOT EXISTS (
                           SELECT 1 FROM unnest($3::text[]) t WHERE lower(p.title) NOT LIKE '%' || t || '%'
                       )) THEN 'title' ELSE 'description' END AS match_source
         FROM project p WHERE p.workspace_id=$2
           AND ($6::bool OR p.status NOT IN ('completed','cancelled'))
           AND (lower(p.title) LIKE '%' || $1 || '%'
                OR lower(coalesce(p.description,'')) LIKE '%' || $1 || '%'
                OR (cardinality($3::text[]) > 1 AND NOT EXISTS (
                    SELECT 1 FROM unnest($3::text[]) t WHERE
                        lower(p.title) NOT LIKE '%' || t || '%'
                        AND lower(coalesce(p.description,'')) NOT LIKE '%' || t || '%'
                )))
         ORDER BY CASE WHEN p.status='cancelled' AND lower(p.title)<>$1 THEN 1 ELSE 0 END,
                  CASE WHEN lower(p.title)=$1 THEN 0
                       WHEN lower(p.title) LIKE $1 || '%' THEN 1
                       WHEN lower(p.title) LIKE '%' || $1 || '%' THEN 2
                       WHEN cardinality($3::text[]) > 1 AND NOT EXISTS (
                           SELECT 1 FROM unnest($3::text[]) t WHERE lower(p.title) NOT LIKE '%' || t || '%'
                       ) THEN 3
                       WHEN lower(coalesce(p.description,'')) LIKE '%' || $1 || '%' THEN 4
                       ELSE 5 END,
                  p.updated_at DESC LIMIT $4::bigint OFFSET $5::bigint",
        &[&phrase, &ws, &terms, &limit, &offset, &include_closed],
    ).await.map_err(|err| {
        if err.code().is_some_and(|code| code.code() == "57014") {
            error(StatusCode::SERVICE_UNAVAILABLE, "search timed out; please refine your query or try again")
        } else {
            error(StatusCode::INTERNAL_SERVER_ERROR, "failed to search projects")
        }
    })?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to search projects",
        )
    })?;
    let mut out: Vec<_> = rows
        .iter()
        .map(|r| SearchProject {
            project: project_row(r),
            match_source: r.get("match_source"),
            matched_snippet: None,
        })
        .collect();
    let mut projects: Vec<_> = rows.iter().map(project_row).collect();
    project_counts(&**db, ws, &mut projects).await;
    for (item, project) in out.iter_mut().zip(projects) {
        item.project = project;
        if item.match_source == "description" {
            item.matched_snippet = item
                .project
                .description
                .as_deref()
                .filter(|description| !description.is_empty())
                .map(|description| search_snippet(description, query));
        }
    }
    Ok(GoJson(json!({"projects":out})))
}

async fn project(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Project>, ApiError> {
    let id = required_id(&raw, "invalid project id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?;
    let row = db
        .query_opt(
            "SELECT * FROM project WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "project not found"))?;
    let mut out = vec![project_row(&row)];
    project_counts(&**db, ws, &mut out).await;
    Ok(GoJson(out.remove(0)))
}

#[derive(Serialize)]
struct ProjectResource {
    id: Uuid,
    project_id: Uuid,
    workspace_id: Uuid,
    resource_type: String,
    resource_ref: Value,
    label: Option<String>,
    position: i32,
    created_at: String,
    created_by: Option<Uuid>,
}

async fn project_resources(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let id = required_id(&raw, "invalid project id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?;
    if db
        .query_opt(
            "SELECT id FROM project WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "project not found"));
    }
    let rows = db.query("SELECT id, project_id, workspace_id, resource_type, resource_ref, label, position, created_at, created_by FROM project_resource WHERE project_id=$1 AND workspace_id=$2 ORDER BY position, created_at", &[&id, &ws])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list project resources"))?;
    let resources: Vec<_> = rows
        .iter()
        .map(|r| ProjectResource {
            id: r.get("id"),
            project_id: r.get("project_id"),
            workspace_id: r.get("workspace_id"),
            resource_type: r.get("resource_type"),
            resource_ref: r.get("resource_ref"),
            label: r.get("label"),
            position: r.get("position"),
            created_at: timestamp(r.get("created_at")),
            created_by: r.get("created_by"),
        })
        .collect();
    Ok(GoJson(
        json!({"total":resources.len(),"resources":resources}),
    ))
}

#[derive(Serialize)]
pub(super) struct Label {
    id: Uuid,
    workspace_id: Uuid,
    resource_type: String,
    name: String,
    description: String,
    color: String,
    usage_count: i64,
    created_at: String,
    updated_at: String,
}
pub(super) fn label_row(r: &Row, usage_count: i64) -> Label {
    Label {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        resource_type: r.get("resource_type"),
        name: r.get("name"),
        description: r.get("description"),
        color: r.get("color"),
        usage_count,
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }
}
fn label_resource_type(raw: Option<&str>) -> Result<&str, ApiError> {
    let value = raw.unwrap_or("").trim();
    match value {
        "" => Ok("issue"),
        "issue" | "agent" | "skill" => Ok(value),
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "resource_type must be issue, agent, or skill",
        )),
    }
}
async fn labels(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let resource = label_resource_type(params.get("resource_type").map(String::as_str))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list labels"))?;
    let rows = db.query("SELECT l.*, CASE l.resource_type WHEN 'issue' THEN (SELECT count(*) FROM issue_to_label x WHERE x.label_id=l.id) WHEN 'agent' THEN (SELECT count(*) FROM agent_to_label x WHERE x.label_id=l.id) WHEN 'skill' THEN (SELECT count(*) FROM skill_to_label x WHERE x.label_id=l.id) ELSE 0 END::bigint AS usage_count FROM issue_label l WHERE l.workspace_id=$1 AND l.resource_type=$2 ORDER BY lower(name) ASC",
        &[&ws,&resource]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list labels"))?;
    let out: Vec<_> = rows
        .iter()
        .map(|r| label_row(r, r.get("usage_count")))
        .collect();
    Ok(GoJson(json!({"total":out.len(),"labels":out})))
}
async fn label(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Label>, ApiError> {
    let id = required_id(&raw, "invalid label id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get label"))?;
    let row = db
        .query_opt(
            "SELECT * FROM issue_label WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get label"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "label not found"))?;
    Ok(GoJson(label_row(&row, 0)))
}

#[derive(Serialize)]
pub(super) struct Property {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    description: String,
    icon: String,
    config: Value,
    position: f64,
    archived: bool,
    archived_at: Option<String>,
    usage_count: i64,
    created_at: String,
    updated_at: String,
}
pub(super) fn property_row(r: &Row, usage_count: i64) -> Property {
    let config: Value = r.get("config");
    // Go decodes only the typed options field; unknown config keys are not returned.
    let options = config
        .get("options")
        .and_then(Value::as_array)
        .and_then(|array| {
            array
                .iter()
                .map(|option| {
                    Some(json!({
                        "id": option.get("id")?.as_str()?,
                        "name": option.get("name")?.as_str()?,
                        "color": option.get("color")?.as_str()?,
                    }))
                })
                .collect::<Option<Vec<_>>>()
        });
    let config = options
        .filter(|options| !options.is_empty())
        .map(|options| json!({"options":options}))
        .unwrap_or_else(|| json!({}));
    let archived_at = opt_timestamp(r, "archived_at");
    Property {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        name: r.get("name"),
        kind: r.get("type"),
        description: r.get("description"),
        icon: r.get("icon"),
        config,
        position: r.get("position"),
        archived: archived_at.is_some(),
        archived_at,
        usage_count,
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }
}
async fn properties(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let include_archived = params.get("include_archived").is_some_and(|s| s == "true");
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list properties",
        )
    })?;
    let rows=db.query("SELECT p.*, (SELECT count(*) FROM issue i WHERE i.workspace_id=p.workspace_id AND i.properties ? p.id::text)::bigint AS usage_count FROM issue_property p WHERE p.workspace_id=$1 AND ($2::bool OR p.archived_at IS NULL) ORDER BY p.position ASC, lower(p.name) ASC",
        &[&ws,&include_archived]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list properties"))?;
    let out: Vec<_> = rows
        .iter()
        .map(|r| property_row(r, r.get("usage_count")))
        .collect();
    Ok(GoJson(json!({"total":out.len(),"properties":out})))
}
async fn property(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Property>, ApiError> {
    let id = required_id(&raw, "invalid property id")?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get property"))?;
    let row = db
        .query_opt(
            "SELECT * FROM issue_property WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get property"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "property not found"))?;
    Ok(GoJson(property_row(&row, 0)))
}

#[derive(Serialize)]
struct IssueStatus {
    id: Uuid,
    workspace_id: Uuid,
    key: String,
    name: String,
    description: String,
    category: String,
    color: String,
    icon: String,
    is_system: bool,
    position: f64,
    archived_at: Option<String>,
    created_at: String,
    updated_at: String,
}
/// The seven canonical, platform-owned status keys. A built-in has no display
/// name on the wire: every client localizes it from the key, so echoing the
/// seeded English name would be the one string a non-English workspace ignores.
pub(super) fn is_builtin_status(key: &str) -> bool {
    matches!(
        key,
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
    )
}

pub(super) fn wire_category<'a>(key: &'a str, category: &'a str) -> &'a str {
    match key {
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled" => key,
        _ => match category {
            "unstarted" => "todo",
            "started" => "in_progress",
            "done" => "done",
            "closed" => "cancelled",
            "completed" => "done",
            "canceled" => "cancelled",
            "backlog" | "todo" => "todo",
            "in_progress" | "in_review" | "blocked" => "in_progress",
            "cancelled" => "cancelled",
            _ => category,
        },
    }
}
fn status_row(r: &Row) -> IssueStatus {
    let key: String = r.get("key");
    let category: String = r.get("category");
    IssueStatus {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        category: wire_category(&key, &category).into(),
        key,
        name: r.get("name"),
        description: r.get("description"),
        color: r.get("color"),
        icon: r.get("icon"),
        is_system: r.get("is_system"),
        position: r.get("position"),
        archived_at: opt_timestamp(r, "archived_at"),
        created_at: timestamp(r.get("created_at")),
        updated_at: timestamp(r.get("updated_at")),
    }
}
const STATUS_SEED: &str = "INSERT INTO issue_status (workspace_id,key,name,description,category,color,is_system,position) VALUES ($1,'backlog','Backlog','Parked. Assigning an issue here never starts an agent run.','unstarted','#6b7280',TRUE,0), ($1,'todo','Todo','Queued for work. Moving an issue here starts the assigned agent.','unstarted','#6b7280',TRUE,0), ($1,'in_progress','In Progress','Actively being worked on.','started','#f59e0b',TRUE,0), ($1,'in_review','In Review','Work delivered, waiting on human review. Finalizes the autopilot run.','started','#22c55e',TRUE,0), ($1,'done','Done','Completed.','done','#3b82f6',TRUE,0), ($1,'blocked','Blocked','Stalled on an external dependency.','started','#ef4444',TRUE,0), ($1,'cancelled','Cancelled','Decided not to do.','closed','#6b7280',TRUE,0) ON CONFLICT DO NOTHING";
async fn issue_statuses(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let include_archived = params
        .get("include_archived")
        .is_some_and(|s| s.eq_ignore_ascii_case("true"));
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list issue statuses",
        )
    })?;
    // Ensure is an idempotent write on read. Go logs a seed failure, then still lists.
    let _ = db.execute(STATUS_SEED, &[&ws]).await;
    let rows=db.query("SELECT * FROM issue_status WHERE workspace_id=$1 AND ($2::bool OR archived_at IS NULL) ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END, position, CASE WHEN is_system THEN 0 ELSE 1 END, CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END, key",
        &[&ws,&include_archived]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list issue statuses"))?;
    let out: Vec<_> = rows.iter().map(status_row).collect();
    Ok(GoJson(
        json!({"total":out.len(),"statuses":out,"categories":["backlog","todo","in_progress","in_review","blocked","done","cancelled"]}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filter_and_category_contract() {
        assert_eq!(
            parse_params(Some("status=planned&status=paused&name=Some+Name"))["status"],
            "planned"
        );
        assert_eq!(parse_params(Some("name=Some+Name"))["name"], "Some Name");
        assert_eq!(label_resource_type(None).unwrap(), "issue");
        assert_eq!(label_resource_type(Some(" skill ")).unwrap(), "skill");
        assert_eq!(
            label_resource_type(Some("invalid")).unwrap_err().1,
            "resource_type must be issue, agent, or skill"
        );
        assert_eq!(wire_category("blocked", "started"), "blocked");
        assert_eq!(wire_category("custom", "closed"), "cancelled");
    }
}
