//! Workspace-scoped issue list and its POST transport for large filter sets.
use super::{
    ApiState, GoJson,
    catalog::{WorkspaceId, parse_params},
    queries_sprint_issue_search::query_number,
    queries_sprint_search_shapes as shape,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::{GenericClient, types::ToSql};
use uuid::Uuid;

#[derive(Debug)]
pub(super) struct ApiError(pub(super) StatusCode, pub(super) String);
impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, GoJson(json!({"error":self.1}))).into_response()
    }
}
fn error(status: StatusCode, message: &str) -> ApiError {
    ApiError(status, message.to_owned())
}

type Params = HashMap<String, String>;
type Args = Vec<Box<dyn ToSql + Sync + Send>>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues", get(list))
        .route("/api/issues/", get(list))
        .route("/api/issues/grouped", get(grouped))
        .route(
            "/api/issues/query",
            axum::routing::post(query).layer(DefaultBodyLimit::max(1 << 20)),
        )
}

fn bad(message: &str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}
fn bind<T: ToSql + Sync + Send + 'static>(args: &mut Args, value: T) -> String {
    args.push(Box::new(value));
    format!("${}", args.len())
}
fn borrowed(args: &Args) -> Vec<&(dyn ToSql + Sync)> {
    args.iter()
        .map(|arg| arg.as_ref() as &(dyn ToSql + Sync))
        .collect()
}
fn csv(raw: Option<&String>) -> Vec<String> {
    raw.unwrap_or(&String::new())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
fn values(params: &Params, plural: &str, singular: &str) -> Vec<String> {
    let items = csv(params.get(plural));
    if items.is_empty() {
        csv(params.get(singular))
    } else {
        items
    }
}
fn uuid(params: &Params, key: &str) -> Result<Option<Uuid>, ApiError> {
    params
        .get(key)
        .filter(|s| !s.is_empty())
        .map(|s| Uuid::parse_str(s).map_err(|_| bad(&format!("invalid {key}"))))
        .transpose()
}
fn uuids(raw: Option<&String>, key: &str) -> Result<Vec<Uuid>, ApiError> {
    csv(raw)
        .iter()
        .map(|s| Uuid::parse_str(s).map_err(|_| bad(&format!("invalid {key}"))))
        .collect()
}
fn actors(raw: Option<&String>, key: &str) -> Result<Vec<(String, Uuid)>, ApiError> {
    csv(raw)
        .iter()
        .map(|part| {
            let (kind, id) = part
                .split_once(':')
                .ok_or_else(|| bad(&format!("invalid {key}")))?;
            if !matches!(kind, "member" | "agent" | "squad") {
                return Err(bad(&format!("invalid {key}")));
            }
            let id = Uuid::parse_str(id.trim()).map_err(|_| bad(&format!("invalid {key}")))?;
            Ok((kind.to_owned(), id))
        })
        .collect()
}
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
fn positive(params: &Params, key: &str, default: i64, max: i64) -> i64 {
    params
        .get(key)
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
        .min(max)
}
fn offset(params: &Params) -> i64 {
    params
        .get("offset")
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(0)
}
fn date_range(params: &Params, where_: &mut Vec<String>, args: &mut Args) -> Result<(), ApiError> {
    let field = params
        .get("date_field")
        .map(String::as_str)
        .unwrap_or("")
        .trim();
    let start = params
        .get("date_start")
        .map(String::as_str)
        .unwrap_or("")
        .trim();
    let end = params
        .get("date_end")
        .map(String::as_str)
        .unwrap_or("")
        .trim();
    if field.is_empty() && start.is_empty() && end.is_empty() {
        return Ok(());
    }
    if field.is_empty() || start.is_empty() || end.is_empty() {
        return Err(bad(
            "date_field, date_start, and date_end are required together",
        ));
    }
    if !matches!(field, "created_at" | "updated_at") {
        return Err(bad("invalid date_field"));
    }
    let start = DateTime::parse_from_rfc3339(start)
        .map_err(|_| bad("invalid date_start"))?
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)
        .map_err(|_| bad("invalid date_end"))?
        .with_timezone(&Utc);
    if start >= end {
        return Err(bad("date_start must be before date_end"));
    }
    let start = bind(args, start);
    let end = bind(args, end);
    where_.push(format!("i.{field} >= {start} AND i.{field} < {end}"));
    Ok(())
}
fn metadata(params: &Params, where_: &mut Vec<String>, args: &mut Args) -> Result<(), ApiError> {
    let Some(raw) = params.get("metadata").filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let value: Value =
        serde_json::from_str(raw).map_err(|_| bad("metadata filter must be a JSON object"))?;
    if value.is_null() {
        where_.push(format!("i.metadata @> {}::jsonb", bind(args, Value::Null)));
        return Ok(());
    }
    let object = value
        .as_object()
        .ok_or_else(|| bad("metadata filter must be a JSON object"))?;
    for (key, value) in object {
        let mut chars = key.chars();
        if key.len() > 64
            || !chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
        {
            return Err(bad(
                "metadata filter key must match ^[a-zA-Z_][a-zA-Z0-9_.-]{0,63}$",
            ));
        }
        if !value.is_string() && !value.is_boolean() && !value.is_number() {
            return Err(bad(
                "metadata filter values must be primitives (string, number, bool)",
            ));
        }
    }
    where_.push(format!("i.metadata @> {}::jsonb", bind(args, value)));
    Ok(())
}
fn property_filters(
    params: &Params,
    where_: &mut Vec<String>,
    args: &mut Args,
) -> Result<(), ApiError> {
    let Some(raw) = params.get("properties").filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let value: Value = serde_json::from_str(raw)
        .map_err(|_| bad("properties filter must be a JSON object of {definitionId: [values]}"))?;
    if value.is_null() {
        return Ok(());
    }
    let map = value.as_object().ok_or_else(|| {
        bad("properties filter must be a JSON object of {definitionId: [values]}")
    })?;
    if map.len() > 20 {
        return Err(bad(
            "properties filter cannot cover more than 20 definitions",
        ));
    }
    let mut and = Vec::new();
    let mut total = 0;
    for (id, values) in map {
        Uuid::parse_str(id).map_err(|_| {
            bad(&format!(
                "properties filter key {id:?} is not a definition id"
            ))
        })?;
        if values.is_null() {
            continue;
        }
        let values = values.as_array().ok_or_else(|| {
            bad("properties filter must be a JSON object of {definitionId: [values]}")
        })?;
        if values.len() > 50 {
            return Err(bad(&format!(
                "properties filter for {id} cannot list more than 50 values"
            )));
        }
        let mut ors = Vec::new();
        let mut no_value = false;
        for value in values {
            if let Some(s) = value.as_str() {
                if s.is_empty() {
                    return Err(bad("properties filter values cannot be empty"));
                }
                if s == "__none__" {
                    if !no_value {
                        ors.push(format!("NOT (i.properties ? {})", bind(args, id.clone())));
                        no_value = true;
                    }
                    continue;
                }
                for item in [json!({id:s}), json!({id:[s]})] {
                    ors.push(format!("i.properties @> {}::jsonb", bind(args, item)));
                }
                if matches!(s, "true" | "false") {
                    ors.push(format!(
                        "i.properties @> {}::jsonb",
                        bind(args, json!({id:s=="true"}))
                    ));
                }
                if let Ok(n) = s.parse::<f64>()
                    && n.is_finite()
                {
                    ors.push(format!(
                        "i.properties @> {}::jsonb",
                        bind(args, json!({id:n}))
                    ));
                }
            } else if let Some(op) = value.as_object() {
                let kind = op.get("op").and_then(Value::as_str).ok_or_else(|| {
                    bad("properties filter values must be strings or {op, value} objects")
                })?;
                let v = op
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or_else(|| bad("properties filter operator values cannot be empty"))?;
                if v.is_empty() {
                    return Err(bad("properties filter operator values cannot be empty"));
                }
                let definition = bind(args, id.clone());
                let pred = match kind {
                    "contains" => {
                        if v.chars().count() > 2000 {
                            return Err(bad("properties filter value is too long"));
                        }
                        let escaped = escape_like(v);
                        let per_key = format!(
                            "jsonb_typeof(i.properties -> {definition}) = 'string' AND (i.properties ->> {definition}) ILIKE '%' || {} || '%'",
                            bind(args, escaped.clone())
                        );
                        if !v.chars().any(|c| c == '"' || c == '\\' || c < ' ') {
                            format!(
                                "(LOWER(i.properties::text) LIKE LOWER('%' || {} || '%') AND {per_key})",
                                bind(args, escaped)
                            )
                        } else {
                            format!("({per_key})")
                        }
                    }
                    "gt" | "gte" | "lt" | "lte" => {
                        let n = v.parse::<f64>().map_err(|_| {
                            bad(&format!(
                                "properties filter op {kind:?} requires a finite number"
                            ))
                        })?;
                        if !n.is_finite() {
                            return Err(bad(&format!(
                                "properties filter op {kind:?} requires a finite number"
                            )));
                        }
                        let op = match kind {
                            "gt" => ">",
                            "gte" => ">=",
                            "lt" => "<",
                            _ => "<=",
                        };
                        format!(
                            "(CASE WHEN jsonb_typeof(i.properties -> {definition}) = 'number' THEN (i.properties ->> {definition})::numeric END {op} {}::text::numeric)",
                            bind(args, n.to_string())
                        )
                    }
                    "before" | "after" => {
                        let date = NaiveDate::parse_from_str(v, "%Y-%m-%d").map_err(|_| {
                            bad(&format!(
                                "properties filter op {kind:?} requires a YYYY-MM-DD date"
                            ))
                        })?;
                        if date.format("%Y-%m-%d").to_string() != v {
                            return Err(bad(&format!(
                                "properties filter op {kind:?} requires a YYYY-MM-DD date"
                            )));
                        }
                        let op = if kind == "before" { "<" } else { ">" };
                        format!(
                            "(jsonb_typeof(i.properties -> {definition}) = 'string' AND i.properties ->> {definition} {op} {})",
                            bind(args, v.to_owned())
                        )
                    }
                    _ => {
                        return Err(bad(&format!(
                            "properties filter op {kind:?} is not supported"
                        )));
                    }
                };
                ors.push(pred);
            } else {
                return Err(bad(
                    "properties filter values must be strings or {op, value} objects",
                ));
            }
        }
        total += ors.len();
        if !ors.is_empty() {
            and.push(format!("({})", ors.join(" OR ")));
        }
    }
    if total > 256 {
        return Err(bad("properties filter is too large"));
    }
    if !and.is_empty() {
        where_.push(format!("({})", and.join(" AND ")));
    }
    Ok(())
}
async fn categories(
    db: &impl GenericClient,
    ws: Uuid,
    requested: &[String],
) -> Result<Vec<String>, ApiError> {
    let mut cats = Vec::new();
    for cat in requested {
        let c = match cat.as_str() {
            "unstarted" | "backlog" | "todo" => "unstarted",
            "started" | "in_progress" | "in_review" | "blocked" => "started",
            "done" => "done",
            "closed" | "cancelled" => "closed",
            _ => continue,
        };
        if !cats.contains(&c.to_owned()) {
            cats.push(c.to_owned());
        }
    }
    if cats.is_empty() {
        return Ok(vec![]);
    }
    let rows = db
        .query(
            "SELECT key FROM issue_status WHERE workspace_id=$1 AND category=ANY($2::text[])",
            &[&ws, &cats],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to resolve status categories",
            )
        })?;
    let mut keys: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    for (cat, key) in [
        ("unstarted", "backlog"),
        ("unstarted", "todo"),
        ("started", "in_progress"),
        ("started", "in_review"),
        ("started", "blocked"),
        ("done", "done"),
        ("closed", "cancelled"),
    ] {
        if cats.iter().any(|c| c == cat) && !keys.iter().any(|k| k == key) {
            keys.push(key.into());
        }
    }
    Ok(keys)
}
pub(super) async fn table_filter(
    db: &impl GenericClient,
    ws: Uuid,
    params: &Params,
    args: &mut Args,
) -> Result<String, ApiError> {
    filter(db, ws, params, args, false).await
}

async fn filter(
    db: &impl GenericClient,
    ws: Uuid,
    params: &Params,
    args: &mut Args,
    open: bool,
) -> Result<String, ApiError> {
    bind(args, ws);
    let mut where_ = vec!["i.workspace_id=$1".to_owned()];
    let statuses = values(params, "statuses", "status");
    let cats = values(params, "status_categories", "status_category");
    if !open {
        if !cats.is_empty() {
            where_.push(format!(
                "i.status=ANY({}::text[])",
                bind(args, categories(db, ws, &cats).await?)
            ));
        }
        if !statuses.is_empty() {
            where_.push(format!("i.status=ANY({}::text[])", bind(args, statuses)));
        }
        let priorities = values(params, "priorities", "priority");
        if !priorities.is_empty() {
            where_.push(format!(
                "i.priority=ANY({}::text[])",
                bind(args, priorities)
            ));
        }
    } else {
        let terminal = categories(db, ws, &["done".into(), "closed".into()]).await?;
        where_.push(format!("i.status <> ALL({}::text[])", bind(args, terminal)));
        if let Some(priority) = params.get("priority").filter(|s| !s.is_empty()) {
            where_.push(format!("i.priority={}", bind(args, priority.clone())));
        }
    }
    for key in ["assignee_id", "creator_id", "project_id"] {
        if let Some(id) = uuid(params, key)? {
            where_.push(format!("i.{key}={}::uuid", bind(args, id)));
        }
    }
    let assignee_ids = uuids(params.get("assignee_ids"), "assignee_ids")?;
    if !assignee_ids.is_empty() {
        where_.push(format!(
            "i.assignee_id=ANY({}::uuid[])",
            bind(args, assignee_ids)
        ));
    }
    if !open {
        let kinds = csv(params.get("assignee_types"));
        if kinds
            .iter()
            .any(|k| !matches!(k.as_str(), "member" | "agent" | "squad"))
        {
            return Err(bad("invalid assignee_types"));
        }
        if !kinds.is_empty() {
            where_.push(format!(
                "i.assignee_type=ANY({}::text[])",
                bind(args, kinds)
            ));
        }
        let assignees = actors(params.get("assignee_filters"), "assignee_filters")?;
        let mut ors = Vec::new();
        for (kind, id) in assignees {
            let k = bind(args, kind);
            let id = bind(args, id);
            ors.push(format!(
                "(i.assignee_type={k} AND i.assignee_id={id}::uuid)"
            ));
        }
        if params
            .get("include_no_assignee")
            .is_some_and(|v| v == "true")
        {
            ors.push("(i.assignee_type IS NULL AND i.assignee_id IS NULL)".into());
        }
        if !ors.is_empty() {
            where_.push(format!("({})", ors.join(" OR ")));
        }
        let creators = actors(params.get("creator_filters"), "creator_filters")?;
        if !creators.is_empty() {
            let mut ors = Vec::new();
            for (kind, id) in creators {
                let k = bind(args, kind);
                let id = bind(args, id);
                ors.push(format!("(i.creator_type={k} AND i.creator_id={id}::uuid)"));
            }
            where_.push(format!("({})", ors.join(" OR ")));
        }
        let projects = uuids(params.get("project_ids"), "project_ids")?;
        let mut ors = Vec::new();
        if !projects.is_empty() {
            ors.push(format!(
                "i.project_id=ANY({}::uuid[])",
                bind(args, projects)
            ));
        }
        if params
            .get("include_no_project")
            .is_some_and(|v| v == "true")
        {
            ors.push("i.project_id IS NULL".into());
        }
        if !ors.is_empty() {
            where_.push(format!("({})", ors.join(" OR ")));
        }
        let labels = uuids(params.get("label_ids"), "label_ids")?;
        if !labels.is_empty() {
            where_.push(format!("EXISTS(SELECT 1 FROM issue_to_label il WHERE il.issue_id=i.id AND il.label_id=ANY({}::uuid[]))",bind(args,labels)));
        }
        if params.contains_key("ids") {
            where_.push(format!(
                "i.id=ANY({}::uuid[])",
                bind(args, uuids(params.get("ids"), "ids")?)
            ));
        }
        if params.get("top_level_only").is_some_and(|v| v == "true") {
            where_.push("i.parent_issue_id IS NULL".into());
        }
        if let Some(q) = params.get("q").map(|s| s.trim()).filter(|s| !s.is_empty()) {
            let mut or = Vec::new();
            let words: Vec<_> = q.split_whitespace().collect();
            if !words.is_empty() {
                let terms: Vec<_> = words
                    .iter()
                    .map(|word| {
                        format!(
                            "lower(i.title) LIKE {}",
                            bind(args, format!("%{}%", escape_like(&word.to_lowercase())))
                        )
                    })
                    .collect();
                or.push(format!("({})", terms.join(" AND ")));
            }
            if let Some(number) = query_number(q) {
                or.push(format!("i.number={}", bind(args, number)));
            }
            where_.push(format!("({})", or.join(" OR ")));
        }
        if params.get("scheduled").is_some_and(|v| v == "true") {
            where_.push("(i.start_date IS NOT NULL OR i.due_date IS NOT NULL)".into());
        }
    }
    metadata(params, &mut where_, args)?;
    property_filters(params, &mut where_, args)?;
    if open {
        date_range(params, &mut Vec::new(), &mut Args::new())?;
    } else {
        date_range(params, &mut where_, args)?;
    }
    if let Some(user) = uuid(params, "involves_user_id")? {
        let id = bind(args, user);
        where_.push(format!("( (i.assignee_type='agent' AND i.assignee_id IN(SELECT a.id FROM agent a WHERE a.workspace_id=$1 AND a.owner_id={id}::uuid)) OR (i.assignee_type='squad' AND i.assignee_id IN(SELECT sm.squad_id FROM squad_member sm JOIN squad s ON s.id=sm.squad_id WHERE s.workspace_id=$1 AND sm.member_type='member' AND sm.member_id={id}::uuid UNION SELECT s.id FROM squad s JOIN agent a ON a.id=s.leader_id WHERE s.workspace_id=$1 AND a.workspace_id=$1 AND a.owner_id={id}::uuid UNION SELECT sm.squad_id FROM squad_member sm JOIN squad s ON s.id=sm.squad_id JOIN agent a ON a.id=sm.member_id WHERE s.workspace_id=$1 AND sm.member_type='agent' AND a.workspace_id=$1 AND a.owner_id={id}::uuid)))"));
    }
    Ok(where_.join(" AND "))
}
async fn order(
    db: &impl GenericClient,
    ws: Uuid,
    params: &Params,
    args: &mut Args,
) -> Result<String, ApiError> {
    let mut col=match params.get("sort").map(String::as_str).filter(|s|!s.is_empty()).unwrap_or("position"){
        "position"=>"i.position".to_owned(),"title"=>"i.title".into(),"created_at"=>"i.created_at".into(),"updated_at"=>"i.updated_at".into(),"start_date"=>"i.start_date".into(),"due_date"=>"i.due_date".into(),"last_activity"=>"i.last_activity_at".into(),
        "priority"=>"CASE i.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END".into(),
        "status"=>{
            let keys:Vec<String>=db.query("WITH builtin(key,category,rank) AS (VALUES ('backlog','unstarted',0),('todo','unstarted',1),('in_progress','started',2),('in_review','started',3),('blocked','started',4),('done','done',5),('cancelled','closed',6)), entries AS (SELECT key,category,position,is_system FROM issue_status WHERE workspace_id=$1 UNION ALL SELECT b.key,b.category,0,true FROM builtin b WHERE NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=$1 AND s.key=b.key)) SELECT e.key FROM entries e LEFT JOIN builtin b ON b.key=e.key ORDER BY CASE e.category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 0 END,e.position,CASE WHEN e.is_system THEN 0 ELSE 1 END,CASE WHEN e.is_system THEN COALESCE(b.rank,0) ELSE 0 END,e.key",&[&ws]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to resolve sort"))?.iter().map(|r|r.get(0)).collect();
            format!("COALESCE(array_position({}::text[],i.status),100000)",bind(args,keys))
        },
        s if s.starts_with("property:")=>{
            let id=Uuid::parse_str(&s[9..]).map_err(|_|bad("invalid sort value"))?;
            let row=db.query_opt("SELECT type,config FROM issue_property WHERE id=$1 AND workspace_id=$2 AND archived_at IS NULL",&[&id,&ws]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to resolve sort"))?;
            if let Some(row)=row {
                let kind:&str=row.get("type");let key=id.to_string();
                match kind {
                    "number"=>format!("CASE WHEN jsonb_typeof(i.properties->'{key}')='number' THEN (i.properties->>'{key}')::numeric END"),
                    "text"|"url"|"date"=>format!("NULLIF(i.properties->>'{key}','')"),
                    "select"=>{
                        let cfg:Value=row.get("config");let options=cfg.get("options").and_then(Value::as_array);
                        let arms=options.into_iter().flatten().filter_map(|o|o.get("id").and_then(Value::as_str)).filter(|s|Uuid::parse_str(s).is_ok()).enumerate().map(|(n,s)|format!(" WHEN '{s}' THEN {n}")).collect::<String>();
                        if arms.is_empty(){"i.position".into()}else{format!("(CASE i.properties->>'{key}'{arms} END)::numeric")}
                    },
                    _=>"i.position".into(),
                }
            }else{"i.position".into()}
        },
        _=>return Err(bad("invalid sort value")),
    };
    let mut dir = if col == "i.last_activity_at" {
        "DESC"
    } else {
        "ASC"
    };
    if col != "i.position"
        && let Some(value) = params.get("direction").filter(|s| !s.is_empty())
    {
        dir = match value.to_lowercase().as_str() {
            "asc" => "ASC",
            "desc" => "DESC",
            _ => return Err(bad("invalid direction value")),
        };
    }
    let sparse = matches!(
        col.as_str(),
        "i.start_date" | "i.due_date" | "i.last_activity_at"
    ) || col.contains("properties->");
    if sparse {
        col.push_str(&format!(" {dir} NULLS LAST"));
    } else {
        col.push_str(&format!(" {dir}"));
    }
    if col.starts_with("i.last_activity_at") {
        col.push_str(",i.id DESC");
    } else {
        col.push_str(",i.created_at DESC,i.id DESC");
    }
    Ok(col)
}
async fn execute(state: ApiState, ws: Uuid, params: Params) -> Result<GoJson<Value>, ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list issues"))?;
    let tx = db
        .build_transaction()
        .read_only(true)
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list issues"))?;
    let open = params.get("open_only").is_some_and(|v| v == "true");
    let mut args = Args::new();
    let where_ = filter(&*tx, ws, &params, &mut args, open).await?;
    let count_sql = format!("SELECT count(*)::bigint FROM issue i WHERE {where_}");
    let total: i64 = tx
        .query_one(&count_sql, &borrowed(&args))
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to count issues"))?
        .get(0);
    let sort = if open {
        "i.position ASC,i.created_at DESC".into()
    } else {
        order(&*tx, ws, &params, &mut args).await?
    };
    let paging = if open {
        String::new()
    } else {
        let limit = bind(&mut args, positive(&params, "limit", 100, 100));
        let offset = bind(&mut args, offset(&params));
        format!(" LIMIT {limit} OFFSET {offset}")
    };
    let sql = format!(
        "{} WHERE {where_} ORDER BY {sort}{paging}",
        shape::ISSUE_SELECT
    );
    let rows = tx
        .query(&sql, &borrowed(&args))
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list issues"))?;
    let issues = project_rows(&*tx, ws, &rows).await?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list issues"))?;
    Ok(GoJson(json!({"issues":issues,"total":total})))
}
pub(super) async fn project_rows(
    db: &impl GenericClient,
    ws: Uuid,
    rows: &[tokio_postgres::Row],
) -> Result<Vec<Value>, ApiError> {
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let label_rows=db.query("SELECT il.issue_id,l.* FROM issue_to_label il JOIN issue_label l ON l.id=il.label_id AND l.workspace_id=$1 AND l.resource_type='issue' WHERE il.issue_id=ANY($2::uuid[]) ORDER BY lower(l.name)",&[&ws,&ids]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list labels"))?;
    let mut labels: HashMap<Uuid, Vec<Value>> = HashMap::new();
    for row in label_rows {
        let issue: Uuid = row.get("issue_id");
        labels.entry(issue).or_default().push(json!({
            "id":row.get::<_,Uuid>("id"),"workspace_id":row.get::<_,Uuid>("workspace_id"),"resource_type":row.get::<_,String>("resource_type"),
            "name":row.get::<_,String>("name"),"description":row.get::<_,String>("description"),"color":row.get::<_,String>("color"),"usage_count":0,
            "created_at":super::timestamp(row.get("created_at")),"updated_at":super::timestamp(row.get("updated_at"))
        }));
    }
    let issues: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut value = shape::issue(row);
            value.as_object_mut().unwrap().remove("search_updated_at");
            value["labels"] = json!(labels.remove(&row.get::<_, Uuid>("id")).unwrap_or_default());
            value
        })
        .collect();
    Ok(issues)
}
async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    execute(state, ws, parse_params(query.as_deref())).await
}
async fn query(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Value>, ApiError> {
    use serde::Deserialize;
    let body = body.map_err(|_| bad("invalid request body"))?;
    let mut decoder = serde_json::Deserializer::from_slice(&body);
    let params = Option::<Params>::deserialize(&mut decoder)
        .map_err(|_| bad("invalid request body"))?
        .unwrap_or_default();
    execute(state, ws, params).await
}

async fn grouped(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let mut params = parse_params(query.as_deref());
    if !matches!(
        params.get("group_by").map(String::as_str),
        None | Some("") | Some("assignee")
    ) {
        return Err(bad("unsupported group_by"));
    }
    // The grouped endpoint has its own filter vocabulary; ignore list-only options.
    for key in ["open_only", "ids", "top_level_only", "q", "scheduled"] {
        params.remove(key);
    }
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list grouped issues",
        )
    })?;
    let tx = db
        .build_transaction()
        .read_only(true)
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list grouped issues",
            )
        })?;
    let mut args = Args::new();
    let mut where_ = filter(&*tx, ws, &params, &mut args, false).await?;
    if let Some(kind) = params.get("group_assignee_type").filter(|s| !s.is_empty()) {
        if kind == "none" {
            where_.push_str(" AND i.assignee_type IS NULL AND i.assignee_id IS NULL");
        } else {
            if !matches!(kind.as_str(), "member" | "agent" | "squad") {
                return Err(bad("invalid group_assignee_type"));
            }
            let id = uuid(&params, "group_assignee_id")?
                .ok_or_else(|| bad("invalid group_assignee_id"))?;
            let kind = bind(&mut args, kind.clone());
            let id = bind(&mut args, id);
            where_.push_str(&format!(
                " AND i.assignee_type={kind} AND i.assignee_id={id}::uuid"
            ));
        }
    }
    let sort = order(&*tx, ws, &params, &mut args).await?;
    let skip = bind(&mut args, offset(&params));
    let max = bind(&mut args, positive(&params, "limit", 50, 100));
    let select = shape::ISSUE_SELECT.replacen("FROM issue i", "FROM ranked i", 1);
    let sql = format!(
        "WITH ranked AS (SELECT i.*, COUNT(*) OVER (PARTITION BY i.assignee_type,i.assignee_id) AS group_total, ROW_NUMBER() OVER (PARTITION BY i.assignee_type,i.assignee_id ORDER BY {sort}) AS rn FROM issue i WHERE {where_}) {select} WHERE i.rn>{skip} AND i.rn<={skip}+{max} ORDER BY CASE i.assignee_type WHEN 'member' THEN 0 WHEN 'agent' THEN 1 WHEN 'squad' THEN 2 ELSE 3 END, i.assignee_type NULLS LAST, i.assignee_id NULLS LAST, i.rn"
    );
    let rows = tx.query(&sql, &borrowed(&args)).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list grouped issues",
        )
    })?;
    let issues = project_rows(&*tx, ws, &rows).await?;
    let mut groups: Vec<Value> = Vec::new();
    let mut indices = HashMap::new();
    for (row, issue) in rows.iter().zip(issues) {
        let kind: Option<&str> = row.get("assignee_type");
        let id: Option<Uuid> = row.get("assignee_id");
        let key = match (kind, id) {
            (Some(kind), Some(id)) => format!("assignee:{kind}:{id}"),
            _ => "assignee:unassigned".to_owned(),
        };
        let index = *indices.entry(key.clone()).or_insert_with(|| {
            let index = groups.len();
            groups.push(json!({"id": key, "assignee_type": kind, "assignee_id": id,
                "issues": [], "total": row.get::<_, i64>("group_total")}));
            index
        });
        groups[index]["issues"].as_array_mut().unwrap().push(issue);
    }
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list grouped issues",
        )
    })?;
    Ok(GoJson(json!({"groups": groups})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_filter_escapes_wildcards_and_empty_ids() {
        assert_eq!(escape_like("a%_\\"), "a\\%\\_\\\\");
        assert_eq!(query_number("MUL-52"), Some(52));
        assert_eq!(query_number("MUL-0"), None);
        assert_eq!(query_number("+1"), Some(1));
        assert_eq!(query_number("MUL-+1"), None);
        assert_eq!(query_number("2147483648"), None);
        assert_eq!(offset(&Params::from([("offset".into(), "-1".into())])), 0);
        assert!(uuids(Some(&String::new()), "ids").unwrap().is_empty());
    }
    #[test]
    fn date_range_is_bound_and_half_open() {
        let params = Params::from([
            ("date_field".into(), "created_at".into()),
            ("date_start".into(), "2026-10-01T07:00:00+07:00".into()),
            ("date_end".into(), "2026-10-02T00:00:00Z".into()),
        ]);
        let mut where_ = Vec::new();
        let mut args = Args::new();
        date_range(&params, &mut where_, &mut args).unwrap();
        assert_eq!(where_, ["i.created_at >= $1 AND i.created_at < $2"]);
        assert_eq!(args.len(), 2);
        let mut invalid = params.clone();
        invalid.insert("date_field".into(), "created_at; SELECT 1".into());
        assert!(date_range(&invalid, &mut Vec::new(), &mut Args::new()).is_err());
        invalid = params;
        invalid.insert("date_end".into(), "2026-10-01T00:00:00Z".into());
        assert!(date_range(&invalid, &mut Vec::new(), &mut Args::new()).is_err());
    }

    #[test]
    fn property_alternatives_are_bound_and_fanout_is_limited() {
        let id = Uuid::new_v4().to_string();
        let dangerous = "' OR true --";
        let params = Params::from([(
            "properties".into(),
            json!({id.clone():[dangerous,"__none__"]}).to_string(),
        )]);
        let mut where_ = Vec::new();
        let mut args = Args::new();
        property_filters(&params, &mut where_, &mut args).unwrap();
        assert!(!where_[0].contains(dangerous));
        assert!(where_[0].contains(" OR "));
        assert!(where_[0].contains("NOT (i.properties ?"));
        assert_eq!(args.len(), 3);
        let values = vec!["42"; 50];
        let many = json!({
            Uuid::new_v4().to_string():values,
            Uuid::new_v4().to_string():values,
            Uuid::new_v4().to_string():values,
        });
        let params = Params::from([("properties".into(), many.to_string())]);
        let err = property_filters(&params, &mut Vec::new(), &mut Args::new()).unwrap_err();
        assert_eq!(err.1, "properties filter is too large");
    }

    #[test]
    fn reject_invalid_property_and_metadata_filters() {
        let mut where_ = vec![];
        let mut args = Args::new();
        assert!(
            property_filters(
                &Params::from([("properties".into(), "{\"bad\":[\"a\"]}".into())]),
                &mut where_,
                &mut args
            )
            .is_err()
        );
        assert!(
            metadata(
                &Params::from([("metadata".into(), "{\"x\":[]}".into())]),
                &mut where_,
                &mut args
            )
            .is_err()
        );
    }
}
