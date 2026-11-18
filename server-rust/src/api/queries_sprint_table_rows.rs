//! Frozen Go table transport: flat and hierarchical rows with workspace-scoped keyset pages.
#[path = "queries_sprint_table_facets.rs"]
mod facets;
#[path = "queries_sprint_table_groups.rs"]
mod groups;
use super::{
    ApiState, GoJson, Identity, catalog::WorkspaceId, queries_sprint_issue_list as issues,
    queries_sprint_search_shapes as shape,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, State},
    http::StatusCode,
    routing::post,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use tokio_postgres::{GenericClient, IsolationLevel, types::ToSql};
use uuid::Uuid;

#[derive(Debug)]
struct TableError(StatusCode, Value);
impl axum::response::IntoResponse for TableError {
    fn into_response(self) -> axum::response::Response {
        (self.0, GoJson(self.1)).into_response()
    }
}
fn failure(status: StatusCode, message: &str) -> TableError {
    TableError(status, json!({"error":message}))
}
fn invalid(message: &str) -> TableError {
    failure(StatusCode::BAD_REQUEST, message)
}
fn unavailable(message: &str) -> TableError {
    failure(StatusCode::NOT_IMPLEMENTED, message)
}
fn db_failure(message: &str) -> TableError {
    failure(StatusCode::INTERNAL_SERVER_ERROR, message)
}
fn query_timeout() -> TableError {
    TableError(
        StatusCode::GATEWAY_TIMEOUT,
        json!({"error":"query_timeout","message":"The table query took too long. Narrow the filters and retry."}),
    )
}
fn query_failure(error: tokio_postgres::Error, message: &str) -> TableError {
    if error.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) {
        query_timeout()
    } else {
        db_failure(message)
    }
}
type Args = Vec<Box<dyn ToSql + Sync + Send>>;
fn arg<T: ToSql + Sync + Send + 'static>(args: &mut Args, value: T) -> String {
    args.push(Box::new(value));
    format!("${}", args.len())
}
fn refs(args: &Args) -> Vec<&(dyn ToSql + Sync)> {
    args.iter()
        .map(|x| x.as_ref() as &(dyn ToSql + Sync))
        .collect()
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct ActorRef {
    #[serde(rename = "type")]
    kind: String,
    id: String,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Scope {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    assignee_types: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    actor: Option<ActorRef>,
    #[serde(skip_serializing_if = "String::is_empty")]
    relation: String,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct DateFilter {
    field: String,
    start: String,
    end: String,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Filters {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    statuses: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    priorities: Vec<String>,
    #[serde(skip_serializing_if = "option_empty")]
    assignees: Option<Vec<ActorRef>>,
    #[serde(skip_serializing_if = "is_false")]
    include_no_assignee: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    creators: Vec<ActorRef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    project_ids: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    include_no_project: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    project_statuses: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    label_ids: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<DateFilter>,
    #[serde(skip_serializing_if = "is_false")]
    working_only: bool,
    #[serde(skip_serializing_if = "option_empty")]
    working_issue_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_sub_issues: Option<bool>,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Sort {
    field: String,
    direction: String,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Query {
    scope: Scope,
    filters: Filters,
    #[serde(skip_serializing_if = "String::is_empty")]
    search: String,
    sort: Sort,
}
fn is_false(v: &bool) -> bool {
    !v
}
fn option_empty<T>(v: &Option<Vec<T>>) -> bool {
    v.as_ref().is_none_or(Vec::is_empty)
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Group {
    kind: String,
    property_id: String,
    include_empty: bool,
    primary: String,
    secondary: String,
    secondary_values: Option<Vec<String>>,
    category_format: String,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Hierarchy {
    enabled: bool,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Page {
    limit: i64,
    cursor: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Request {
    query: Query,
    group: Group,
    group_key: Option<String>,
    hierarchy: Hierarchy,
    parent_id: Option<String>,
    page: Page,
}
#[derive(Deserialize, Serialize)]
struct Cursor {
    v: u8,
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    #[serde(default)]
    branch_identity: String,
    #[serde(default)]
    sort_value: String,
    #[serde(default)]
    row_created_at: String,
    #[serde(default)]
    row_id: String,
}

fn actor(value: &ActorRef, field: &str) -> Result<String, TableError> {
    if !matches!(value.kind.as_str(), "member" | "agent" | "squad")
        || Uuid::parse_str(&value.id).is_err()
    {
        return Err(invalid(&format!("invalid {field}")));
    }
    Ok(format!("{}:{}", value.kind, value.id))
}
fn ids(values: &[String], field: &str) -> Result<Vec<Uuid>, TableError> {
    values
        .iter()
        .map(|v| Uuid::parse_str(v).map_err(|_| invalid(&format!("invalid {field}"))))
        .collect()
}
fn normalize_strings(values: &mut Vec<String>) {
    for value in values.iter_mut() {
        *value = value.trim().to_owned();
    }
    values.retain(|value| !value.is_empty());
    values.sort();
    values.dedup();
}
fn fingerprint(ws: Uuid, query: &Query) -> String {
    #[derive(Serialize)]
    struct Fingerprint<'a> {
        workspace_id: Uuid,
        query: &'a Query,
        #[serde(skip_serializing_if = "is_false")]
        explicit_empty_assignees: bool,
        #[serde(skip_serializing_if = "is_false")]
        explicit_empty_working_issues: bool,
    }
    let mut normalized = query.clone();
    normalized.search = normalized.search.trim().to_owned();
    for values in [
        &mut normalized.scope.assignee_types,
        &mut normalized.filters.statuses,
        &mut normalized.filters.priorities,
        &mut normalized.filters.project_ids,
        &mut normalized.filters.project_statuses,
        &mut normalized.filters.label_ids,
    ] {
        normalize_strings(values);
    }
    if let Some(values) = &mut normalized.filters.working_issue_ids {
        normalize_strings(values);
    }
    if let Some(values) = &mut normalized.filters.assignees {
        values.sort_by_key(|v| format!("{}:{}", v.kind, v.id));
        values.dedup_by(|a, b| a.kind == b.kind && a.id == b.id);
    }
    normalized
        .filters
        .creators
        .sort_by_key(|v| format!("{}:{}", v.kind, v.id));
    normalized
        .filters
        .creators
        .dedup_by(|a, b| a.kind == b.kind && a.id == b.id);
    for values in normalized.filters.properties.values_mut() {
        values.sort_by_key(|v| serde_json::to_string(v).unwrap_or_default());
        values.dedup();
    }
    let encoded = serde_json::to_vec(&Fingerprint {
        workspace_id: ws,
        query: &normalized,
        explicit_empty_assignees: query.filters.assignees.as_ref().is_some_and(Vec::is_empty),
        explicit_empty_working_issues: query
            .filters
            .working_issue_ids
            .as_ref()
            .is_some_and(Vec::is_empty),
    })
    .expect("serializable table query");
    let encoded = String::from_utf8(encoded)
        .expect("JSON UTF-8")
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    format!("sha256:{}", hex::encode(Sha256::digest(encoded.as_bytes())))
}
fn decode_cursor(raw: &str) -> Result<Cursor, TableError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| invalid("invalid cursor"))?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid("invalid cursor"))?;
    if cursor.v != 1 {
        return Err(invalid("invalid cursor"));
    }
    Ok(cursor)
}
fn page_limit(limit: i64) -> Result<i64, TableError> {
    match limit {
        0 => Ok(50),
        1..=100 => Ok(limit),
        _ => Err(invalid("page.limit must be between 1 and 100")),
    }
}

async fn compile(
    db: &impl GenericClient,
    ws: Uuid,
    user: Uuid,
    query: &Query,
    args: &mut Args,
) -> Result<String, TableError> {
    let mut params = HashMap::<String, String>::new();
    let f = &query.filters;
    for (field, values) in [
        ("priorities", &f.priorities),
        ("project_ids", &f.project_ids),
        ("label_ids", &f.label_ids),
    ] {
        if !values.is_empty() {
            params.insert(field.into(), values.join(","));
        }
    }
    if f.statuses.iter().any(|v| v.is_empty() || v.len() > 64) {
        return Err(invalid("invalid filters.statuses"));
    }
    if f.priorities
        .iter()
        .any(|v| !matches!(v.as_str(), "urgent" | "high" | "medium" | "low" | "none"))
    {
        return Err(invalid("invalid filters.priorities"));
    }
    ids(&f.project_ids, "filters.project_ids")?;
    ids(&f.label_ids, "filters.label_ids")?;
    if let Some(assignees) = &f.assignees {
        if assignees.is_empty() && !f.include_no_assignee {
            params.insert("ids".into(), String::new());
        } else if !assignees.is_empty() {
            params.insert(
                "assignee_filters".into(),
                assignees
                    .iter()
                    .map(|a| actor(a, "filters.assignees"))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(","),
            );
        }
    }
    if f.include_no_assignee {
        params.insert("include_no_assignee".into(), "true".into());
    }
    if !f.creators.is_empty() {
        params.insert(
            "creator_filters".into(),
            f.creators
                .iter()
                .map(|a| actor(a, "filters.creators"))
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
        );
    }
    if f.include_no_project {
        params.insert("include_no_project".into(), "true".into());
    }
    if let Some(date) = &f.date {
        if !matches!(date.field.as_str(), "created_at" | "updated_at") {
            return Err(invalid("invalid filters.date.field"));
        }
        let start = DateTime::parse_from_rfc3339(&date.start)
            .map_err(|_| invalid("invalid filters.date range"))?;
        let end = DateTime::parse_from_rfc3339(&date.end)
            .map_err(|_| invalid("invalid filters.date range"))?;
        if start >= end {
            return Err(invalid("invalid filters.date range"));
        }
        params.insert("date_field".into(), date.field.clone());
        params.insert("date_start".into(), date.start.clone());
        params.insert("date_end".into(), date.end.clone());
    }
    if !f.properties.is_empty() {
        params.insert(
            "properties".into(),
            serde_json::to_string(&f.properties)
                .map_err(|_| invalid("invalid filters.properties"))?,
        );
    }
    if !f.include_sub_issues.unwrap_or(true) {
        params.insert("top_level_only".into(), "true".into());
    }
    if !query.search.trim().is_empty() {
        params.insert("q".into(), query.search.trim().into());
    }
    let mut where_ = issues::table_filter(db, ws, &params, args)
        .await
        .map_err(|err| TableError(err.0, json!({"error":err.1})))?;
    if !f.statuses.is_empty() {
        let mut statuses = f.statuses.clone();
        normalize_strings(&mut statuses);
        where_.push_str(&format!(
            " AND i.status=ANY({}::text[])",
            arg(args, statuses)
        ));
    }
    match query.scope.kind.as_str() {
        "workspace" | "project" => {
            if query.scope.kind == "project" {
                let id = Uuid::parse_str(&query.scope.project_id)
                    .map_err(|_| invalid("invalid scope.project_id"))?;
                where_.push_str(&format!(" AND i.project_id={}::uuid", arg(args, id)));
            }
            if !query.scope.assignee_types.is_empty() {
                if query
                    .scope
                    .assignee_types
                    .iter()
                    .any(|v| !matches!(v.as_str(), "member" | "agent" | "squad"))
                {
                    return Err(invalid("invalid scope.assignee_types"));
                }
                where_.push_str(&format!(
                    " AND i.assignee_type=ANY({}::text[])",
                    arg(args, query.scope.assignee_types.clone())
                ));
            }
        }
        "assignee" | "creator" => {
            let value = query
                .scope
                .actor
                .as_ref()
                .ok_or_else(|| invalid("scope.actor is required"))?;
            actor(value, "scope.actor")?;
            let id = Uuid::parse_str(&value.id).map_err(|_| invalid("invalid scope.actor"))?;
            where_.push_str(&format!(
                " AND i.{}_type={} AND i.{}_id={}::uuid",
                query.scope.kind,
                arg(args, value.kind.clone()),
                query.scope.kind,
                arg(args, id)
            ));
        }
        "my" => match query.scope.relation.as_str() {
            "assigned" => where_.push_str(&format!(
                " AND i.assignee_type='member' AND i.assignee_id={}::uuid",
                arg(args, user)
            )),
            "created" => where_.push_str(&format!(
                " AND i.creator_type='member' AND i.creator_id={}::uuid",
                arg(args, user)
            )),
            "involved" | "any" | "" => {
                let id = arg(args, user);
                let involved = format!(
                    "((i.assignee_type='agent' AND i.assignee_id IN (SELECT a.id FROM agent a WHERE a.workspace_id=$1 AND a.owner_id={id}::uuid)) OR (i.assignee_type='squad' AND i.assignee_id IN (SELECT sm.squad_id FROM squad_member sm JOIN squad s ON s.id=sm.squad_id WHERE s.workspace_id=$1 AND sm.member_type='member' AND sm.member_id={id}::uuid UNION SELECT s.id FROM squad s JOIN agent a ON a.id=s.leader_id WHERE s.workspace_id=$1 AND a.workspace_id=$1 AND a.owner_id={id}::uuid UNION SELECT sm.squad_id FROM squad_member sm JOIN squad s ON s.id=sm.squad_id JOIN agent a ON a.id=sm.member_id WHERE s.workspace_id=$1 AND sm.member_type='agent' AND a.workspace_id=$1 AND a.owner_id={id}::uuid)))"
                );
                if query.scope.relation == "involved" {
                    where_.push_str(&format!(" AND {involved}"));
                } else {
                    where_.push_str(&format!(" AND ((i.assignee_type='member' AND i.assignee_id={id}::uuid) OR (i.creator_type='member' AND i.creator_id={id}::uuid) OR {involved})"));
                }
            }
            _ => return Err(invalid("invalid scope.relation")),
        },
        _ => return Err(invalid("invalid scope.kind")),
    }
    if !f.project_statuses.is_empty() {
        if f.project_statuses.iter().any(|s| {
            !matches!(
                s.as_str(),
                "planned" | "in_progress" | "paused" | "completed" | "cancelled"
            )
        }) {
            return Err(invalid("invalid filters.project_statuses"));
        }
        where_.push_str(&format!(" AND EXISTS (SELECT 1 FROM project p WHERE p.id=i.project_id AND p.workspace_id=i.workspace_id AND p.status=ANY({}::text[]))",arg(args,f.project_statuses.clone())));
    }
    if f.working_only {
        where_.push_str(" AND EXISTS (SELECT 1 FROM agent_task_queue atq WHERE atq.issue_id=i.id AND atq.status='running')");
    }
    if let Some(working) = &f.working_issue_ids {
        let working = ids(working, "filters.working_issue_ids")?;
        where_.push_str(&format!(" AND i.id=ANY({}::uuid[])", arg(args, working)));
    }
    Ok(where_)
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/issues/table/rows",
            post(rows).layer(DefaultBodyLimit::max(1 << 20)),
        )
        .merge(facets::routes())
        .merge(groups::routes())
}
async fn rows(
    state: State<ApiState>,
    workspace: Extension<WorkspaceId>,
    user: Extension<Identity>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    tokio::time::timeout(
        std::time::Duration::from_secs(8),
        execute(state, workspace, user, body),
    )
    .await
    .map_err(|_| query_timeout())?
}
async fn execute(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, TableError> {
    let body = body.map_err(|_| invalid("invalid issue table query"))?;
    let mut decoder = serde_json::Deserializer::from_slice(&body);
    let mut req =
        Request::deserialize(&mut decoder).map_err(|_| invalid("invalid issue table query"))?;
    decoder
        .end()
        .map_err(|_| invalid("invalid issue table query: request must contain one JSON object"))?;
    let limit = page_limit(req.page.limit)?;
    req.group_key = groups::normalize_key(&req.group, req.group_key.as_deref())?;
    if !req.hierarchy.enabled && req.parent_id.is_some() {
        return Err(invalid("parent_id requires hierarchy.enabled=true"));
    }
    let parent = req
        .parent_id
        .as_deref()
        .map(|s| Uuid::parse_str(s).map_err(|_| invalid("invalid parent_id")))
        .transpose()?;
    let sort_field = req.query.sort.field.trim();
    let sort_direction = req.query.sort.direction.trim().to_ascii_lowercase();
    let (column, cast, direction) = match (sort_field, sort_direction.as_str()) {
        ("" | "position", "" | "asc" | "desc") => ("i.position", "double precision", "asc"),
        ("title", "" | "asc") => ("i.title", "text", "asc"),
        ("title", "desc") => ("i.title", "text", "desc"),
        ("created_at", "" | "asc") => ("i.created_at", "timestamptz", "asc"),
        ("created_at", "desc") => ("i.created_at", "timestamptz", "desc"),
        ("updated_at", "" | "asc") => ("i.updated_at", "timestamptz", "asc"),
        ("updated_at", "desc") => ("i.updated_at", "timestamptz", "desc"),
        ("priority", "" | "asc") => (
            "CASE i.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END",
            "integer",
            "asc",
        ),
        ("priority", "desc") => (
            "CASE i.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END",
            "integer",
            "desc",
        ),
        ("position" | "title" | "created_at" | "updated_at" | "priority", _) => {
            return Err(invalid("invalid query.sort.direction"));
        }
        ("status" | "last_activity" | "start_date" | "due_date", _) => {
            return Err(unavailable("table sort field is not ported"));
        }
        (value, _) if value.starts_with("property:") => {
            return Err(unavailable("table property sort is not ported"));
        }
        _ => return Err(invalid("invalid query.sort.field")),
    };
    let identity = format!(
        "{}:hierarchy={}",
        groups::identity(&req.group),
        req.hierarchy.enabled
    );
    let fp = fingerprint(ws, &req.query);
    let cursor = req
        .page
        .cursor
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(decode_cursor)
        .transpose()?;
    if let Some(c) = &cursor {
        if c.query != fp || c.parent_id != req.parent_id || c.group_key != req.group_key {
            return Err(TableError(
                StatusCode::CONFLICT,
                json!({"error":"cursor_query_mismatch","message":"cursor does not belong to this table query"}),
            ));
        }
        if c.branch_identity != identity {
            return Err(TableError(
                StatusCode::CONFLICT,
                json!({"error":"cursor_query_mismatch","message":"cursor does not belong to this table branch"}),
            ));
        }
        Uuid::parse_str(&c.row_id).map_err(|_| invalid("invalid cursor"))?;
        DateTime::parse_from_rfc3339(&c.row_created_at).map_err(|_| invalid("invalid cursor"))?;
        match cast {
            "double precision" => {
                c.sort_value
                    .parse::<f64>()
                    .map_err(|_| invalid("invalid cursor"))?;
            }
            "integer" => {
                c.sort_value
                    .parse::<i64>()
                    .map_err(|_| invalid("invalid cursor"))?;
            }
            "timestamptz"
                if DateTime::parse_from_rfc3339(&c.sort_value).is_err()
                    && DateTime::parse_from_str(&c.sort_value, "%Y-%m-%d %H:%M:%S%.f%#z")
                        .is_err() =>
            {
                return Err(invalid("invalid cursor"));
            }
            _ => (),
        }
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| db_failure("failed to start table query"))?;
    let tx = db
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await
        .map_err(|_| db_failure("failed to start table query"))?;
    tx.batch_execute("SET LOCAL statement_timeout = '8s'")
        .await
        .map_err(|_| db_failure("failed to start table query"))?;
    let mut args = Args::new();
    let where_ = compile(&*tx, ws, user, &req.query, &mut args).await?;
    let total: i64 = if cursor.is_none() && parent.is_none() && req.group.kind == "none" {
        tx.query_one(
            &format!("SELECT count(*)::bigint FROM issue i WHERE {where_}"),
            &refs(&args),
        )
        .await
        .map_err(|e| query_failure(e, "failed to count table rows"))?
        .get(0)
    } else {
        0
    };
    let group = groups::resolve(&*tx, ws, &req.group, true).await?;
    let group_predicate = group.predicate(req.group_key.as_deref().unwrap_or(""), &mut args)?;
    let mut branch = String::from("TRUE");
    let mut source = if req.hierarchy.enabled {
        "membership"
    } else {
        "issue"
    };
    if req.hierarchy.enabled {
        branch = if let Some(parent) = parent {
            format!(
                "i.parent_issue_id={}::uuid AND EXISTS (SELECT 1 FROM membership p WHERE p.id={}::uuid)",
                arg(&mut args, parent),
                arg(&mut args, parent)
            )
        } else {
            "i.parent_issue_id IS NULL OR (SELECT p.id FROM membership p WHERE p.id=i.parent_issue_id) IS NULL".into()
        };
    }
    let mut cursor_predicate = String::from("TRUE");
    if let Some(c) = &cursor {
        let id = Uuid::parse_str(&c.row_id).map_err(|_| invalid("invalid cursor"))?;
        let created = DateTime::parse_from_rfc3339(&c.row_created_at)
            .map_err(|_| invalid("invalid cursor"))?
            .with_timezone(&Utc);
        let value = arg(&mut args, c.sort_value.clone());
        let created = arg(&mut args, created);
        let id = arg(&mut args, id);
        let cmp = if direction == "asc" { ">" } else { "<" };
        cursor_predicate = format!(
            "({column} {cmp} {value}::text::{cast} OR ({column}={value}::text::{cast} AND (i.created_at<{created}::timestamptz OR (i.created_at={created}::timestamptz AND i.id<{id}::uuid))))"
        );
        if column == "i.position" {
            cursor_predicate = format!("({column}>={value}::text::{cast} AND {cursor_predicate})");
        }
    }
    let page_size = arg(&mut args, limit + 1);
    let mut prefix = if req.hierarchy.enabled {
        format!(
            "WITH membership AS NOT MATERIALIZED (SELECT i.* FROM issue i WHERE {where_} AND ({group_predicate})), "
        )
    } else {
        "WITH ".to_owned()
    };
    if !req.hierarchy.enabled
        && let Some(keys) = group.promoted_keys()
    {
        let visible = arg(&mut args, keys);
        prefix = format!(
            "WITH membership AS NOT MATERIALIZED (SELECT i.* FROM issue i WHERE {where_}), promoted_parents AS (SELECT DISTINCT child.parent_issue_id AS id FROM membership child WHERE child.parent_issue_id IS NOT NULL AND child.status=ANY({visible}::text[])), "
        );
        source = "membership";
        branch = "NOT (i.parent_issue_id IS NULL AND EXISTS(SELECT 1 FROM promoted_parents p WHERE p.id=i.id))".into();
    }
    let cte = format!(
        "{prefix}page AS MATERIALIZED (SELECT i.*, {column}::text AS table_sort_key FROM {source} i WHERE {} AND ({branch}) AND {cursor_predicate} ORDER BY {column} {direction}, i.created_at DESC, i.id DESC LIMIT {page_size})",
        if req.hierarchy.enabled {
            "TRUE".to_owned()
        } else {
            format!("({where_}) AND ({group_predicate})")
        }
    );
    let count = if req.hierarchy.enabled {
        "(SELECT count(*)::bigint FROM membership child WHERE child.parent_issue_id=i.id)"
    } else {
        "0::bigint"
    };
    let select = shape::ISSUE_SELECT
        .replacen(
            "SELECT i.*,",
            &format!("SELECT i.*, {count} AS direct_child_count,"),
            1,
        )
        .replacen("FROM issue i", "FROM page i", 1);
    let sql = format!("{cte} {select} ORDER BY {column} {direction}, i.created_at DESC, i.id DESC");
    let mut result = tx
        .query(&sql, &refs(&args))
        .await
        .map_err(|e| query_failure(e, "failed to list table rows"))?;
    let has_more = result.len() > limit as usize;
    result.truncate(limit as usize);
    let next_cursor = if has_more {
        let last = result.last().expect("positive page limit");
        let sort_value = if cast == "timestamptz" {
            shape::nano(last.get::<_, DateTime<Utc>>(if column == "i.updated_at" {
                "updated_at"
            } else {
                "created_at"
            }))
        } else {
            last.get::<_, String>("table_sort_key")
        };
        Some(
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&Cursor {
                    v: 1,
                    query: fp.clone(),
                    group_key: req.group_key.clone(),
                    parent_id: req.parent_id.clone(),
                    branch_identity: identity,
                    sort_value,
                    row_created_at: shape::nano(last.get("created_at")),
                    row_id: last.get::<_, Uuid>("id").to_string(),
                })
                .map_err(|_| db_failure("failed to encode table cursor"))?,
            ),
        )
    } else {
        None
    };
    let projected = issues::project_rows(&*tx, ws, &result)
        .await
        .map_err(|err| TableError(err.0, json!({"error":err.1})))?;
    let response_rows: Vec<Value> = projected.into_iter().zip(result.iter()).map(|(issue,row)| {
        json!({"issue":issue,"direct_child_count":row.get::<_,i64>("direct_child_count")})
    }).collect();
    tx.commit()
        .await
        .map_err(|_| db_failure("failed to finish table query"))?;
    Ok(GoJson(
        json!({"query_fingerprint":fp,"group_key":req.group_key,"parent_id":req.parent_id,
        "total":total,"branch_total":response_rows.len(),"rows":response_rows,"next_cursor":next_cursor}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_request_and_page_bounds() {
        assert!(serde_json::from_value::<Request>(json!({"unknown":true})).is_err());
        assert!(
            serde_json::from_value::<Request>(json!({"query":{"filters":{"unknown":true}}}))
                .is_err()
        );
        assert_eq!(page_limit(0).unwrap(), 50);
        assert!(page_limit(-1).is_err());
        assert!(page_limit(101).is_err());
        assert!(decode_cursor("not-base64!").is_err());
        assert!(decode_cursor(&URL_SAFE_NO_PAD.encode(b"{\"v\":2}")).is_err());
    }
    #[test]
    fn fingerprint_scope_and_empty_predicates() {
        let ws = Uuid::from_u128(1);
        let mut query = Query::default();
        query.scope.kind = "workspace".into();
        assert_eq!(
            fingerprint(ws, &query),
            "sha256:a6e58db1571e1b57f39ca10525539d2d146e6f7824016c4a1b6e57dacb60840b"
        );
        let base = fingerprint(ws, &query);
        assert_ne!(fingerprint(Uuid::from_u128(2), &query), base);
        query.filters.assignees = Some(vec![]);
        assert_ne!(fingerprint(ws, &query), base);
        query.filters.assignees = None;
        query.filters.statuses = vec!["todo".into(), "done".into(), "todo".into()];
        let ordered = fingerprint(ws, &query);
        query.filters.statuses = vec!["done".into(), "todo".into()];
        assert_eq!(fingerprint(ws, &query), ordered);
    }
}
