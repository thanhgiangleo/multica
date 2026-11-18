//! Issue status definition mutations; issue-level assignments belong to issue-write.
use super::{
    Actor, ApiState, GoJson, Identity,
    account_catalog_write_catalog_publish::{CatalogChange, StatusAction, WorkflowAction, publish},
    account_catalog_write_profile::{account_request, decode::matches_go_field},
    catalog::WorkspaceId,
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{patch, post},
};
use serde::{
    Deserialize, Deserializer,
    de::{IgnoredAny, MapAccess, Visitor},
};
use serde_json::{Value, json};
use std::{collections::HashSet, fmt};
use tokio_postgres::{Row, Transaction, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issue-statuses", post(create))
        .route("/api/issue-statuses/", post(create))
        .route("/api/issue-statuses/{id}", patch(update).delete(archive))
        .route("/api/issue-statuses/reorder", patch(reorder))
        .layer(DefaultBodyLimit::disable())
}

#[derive(Debug)]
struct Failure(StatusCode, Value);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = GoJson(self.1).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn fail(status: StatusCode, message: impl Into<String>) -> Failure {
    Failure(status, json!({"error":message.into()}))
}
fn bad(message: impl Into<String>) -> Failure {
    fail(StatusCode::BAD_REQUEST, message)
}
fn internal(message: &'static str) -> Failure {
    fail(StatusCode::INTERNAL_SERVER_ERROR, message)
}

const BUILT_INS: [&str; 7] = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "cancelled",
];
const CATEGORIES: [&str; 4] = ["unstarted", "started", "done", "closed"];
const ORDER: &str = "ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END, position, CASE WHEN is_system THEN 0 ELSE 1 END, CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END, key";

fn output(row: &Row) -> Value {
    let key: String = row.get("key");
    let category: String = row.get("category");
    let wire = if BUILT_INS.contains(&key.as_str()) {
        key.as_str()
    } else {
        match category.as_str() {
            "unstarted" => "todo",
            "started" => "in_progress",
            "done" => "done",
            "closed" => "cancelled",
            _ => category.as_str(),
        }
    };
    json!({
        "id": row.get::<_, Uuid>("id"), "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "key": key, "name": row.get::<_, String>("name"), "description": row.get::<_, String>("description"),
        "category": wire, "color": row.get::<_, String>("color"), "icon": row.get::<_, String>("icon"),
        "is_system": row.get::<_, bool>("is_system"), "position": row.get::<_, f64>("position"),
        "archived_at": row.get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at").map(timestamp),
        "created_at": timestamp(row.get("created_at")), "updated_at": timestamp(row.get("updated_at"))
    })
}
async fn admin(state: &ApiState, ws: Uuid, user: Uuid, actor: Actor) -> Result<(), Failure> {
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to check workspace role"))?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| internal("failed to check workspace role"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !["owner", "admin"].contains(&row.get::<_, String>(0).as_str()) {
        return Err(fail(StatusCode::FORBIDDEN, "insufficient permissions"));
    }
    Ok(())
}
fn parse_category(raw: &str) -> Result<&'static str, Failure> {
    match raw {
        "unstarted" | "backlog" | "todo" => Ok("unstarted"),
        "started" | "in_progress" | "in_review" | "blocked" => Ok("started"),
        "done" | "completed" => Ok("done"),
        "closed" | "cancelled" | "canceled" => Ok("closed"),
        _ => Err(bad(
            "category must be one of: unstarted, started, done, closed",
        )),
    }
}
fn reserved(key: &str) -> bool {
    BUILT_INS.contains(&key) || CATEGORIES.contains(&key)
}
fn valid_key(key: &str) -> bool {
    key.len() <= 32
        && !key.is_empty()
        && key
            .bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_lowercase() || b.is_ascii_digit() || (i > 0 && b == b'_'))
}
fn simple_lower(c: char) -> char {
    // Go strings.ToLower maps each rune individually, without Unicode expansion.
    match c {
        'İ' => 'i',
        _ => c.to_lowercase().next().unwrap_or(c),
    }
}
fn key(raw: &str) -> Result<String, Failure> {
    let key: String = raw.trim().chars().map(simple_lower).collect();
    if !valid_key(&key) {
        return Err(bad(
            "status key must be 1-32 characters of lowercase letters, digits or underscore, starting with a letter or digit",
        ));
    }
    if reserved(&key) {
        return Err(bad(format!(
            "{key:?} is a reserved status or category key and cannot be reused"
        )));
    }
    Ok(key)
}
fn derive(name: &str, category: &str, taken: &[Row]) -> Result<String, Failure> {
    let mut slug = String::new();
    for c in name.trim().chars().map(simple_lower) {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('_') {
            slug.push('_');
        }
    }
    slug = slug
        .trim_end_matches('_')
        .chars()
        .take(32)
        .collect::<String>()
        .trim_end_matches('_')
        .to_owned();
    if !slug.is_empty() && reserved(&slug) {
        return Err(bad(format!(
            "{slug:?} is a reserved status or category key and cannot be reused; rename the status or pass an explicit key"
        )));
    }
    let base = if slug.is_empty() { category } else { &slug };
    let occupied = |v: &str| reserved(v) || taken.iter().any(|r| r.get::<_, String>(0) == v);
    if !occupied(base) {
        return key(base);
    }
    for n in 2..=(taken.len() + BUILT_INS.len() + CATEGORIES.len() + 2) {
        let suffix = format!("_{n}");
        let prefix = base.chars().take(32 - suffix.len()).collect::<String>();
        let candidate = format!("{}{suffix}", prefix.trim_end_matches('_'));
        if !occupied(&candidate) {
            return key(&candidate);
        }
    }
    Err(bad(
        "could not derive an unused status key from that name; provide one explicitly",
    ))
}
fn color(raw: &str) -> Result<String, Failure> {
    let stripped = raw.trim().strip_prefix('#').unwrap_or(raw.trim());
    if stripped.len() != 6 || !stripped.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad("color must be a 6-digit hex value like #3b82f6"));
    }
    Ok(format!("#{}", stripped.to_ascii_lowercase()))
}
fn icon(raw: &str) -> Result<&str, Failure> {
    if ![
        "",
        "dotted",
        "circle",
        "half",
        "three_quarters",
        "check",
        "slash",
        "cross",
    ]
    .contains(&raw)
    {
        return Err(bad("invalid status icon"));
    }
    Ok(raw)
}
fn name(raw: &str) -> Result<&str, Failure> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.chars().count() > 64 {
        return Err(bad("name must be 1-64 characters"));
    }
    Ok(trimmed)
}
fn description(raw: &str) -> Result<&str, Failure> {
    if raw.chars().count() > 256 {
        return Err(bad("description must be at most 256 characters"));
    }
    Ok(raw)
}
async fn lock(tx: &Transaction<'_>, ws: Uuid, operation: &'static str) -> Result<(), Failure> {
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&format!("{ws}:issue_status")],
    )
    .await
    .map_err(|_| internal(operation))?;
    Ok(())
}
fn write_failure(
    err: tokio_postgres::Error,
    duplicate: &'static str,
    operation: &'static str,
) -> Failure {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        fail(StatusCode::CONFLICT, duplicate)
    } else {
        internal(operation)
    }
}
#[derive(Default)]
struct Create {
    key: String,
    name: String,
    description: String,
    category: String,
    color: String,
    icon: String,
}

impl<'de> Deserialize<'de> for Create {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (field, value) in super::account_catalog_write_profile::decode::fields(
            decoder,
            &["key", "name", "description", "category", "color", "icon"],
        )? {
            // Go keeps an earlier non-pointer string when a duplicate is null.
            if let Some(value) = value {
                match field {
                    "key" => request.key = value,
                    "name" => request.name = value,
                    "description" => request.description = value,
                    "category" => request.category = value,
                    "color" => request.color = value,
                    "icon" => request.icon = value,
                    _ => unreachable!(),
                }
            }
        }
        Ok(request)
    }
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<(StatusCode, GoJson<Value>), Failure> {
    admin(&state, ws, user, actor).await?;
    let body: Create = account_request(&body).map_err(|_| bad("invalid request body"))?;
    let name = name(&body.name)?;
    let description = description(&body.description)?;
    let category = parse_category(&body.category)?;
    let color = color(&body.color)?;
    let icon = icon(&body.icon)?;
    let explicit = if body.key.trim().is_empty() {
        None
    } else {
        Some(key(&body.key)?)
    };
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to create issue status"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to create issue status"))?;
    lock(&tx, ws, "failed to create issue status").await?;
    let key = if let Some(explicit) = explicit {
        explicit
    } else {
        let taken = tx
            .query("SELECT key FROM issue_status WHERE workspace_id=$1", &[&ws])
            .await
            .map_err(|_| internal("failed to create issue status"))?;
        derive(name, category, &taken)?
    };
    let row = tx.query_one("INSERT INTO issue_status(workspace_id,key,name,description,category,color,icon,position) VALUES($1,$2,$3,$4,$5,$6,$7,COALESCE((SELECT max(position)+1 FROM issue_status WHERE workspace_id=$1 AND category=$5),0)) RETURNING *", &[&ws,&key,&name,&description,&category,&color,&icon]).await
        .map_err(|e| write_failure(e,"a status with this key or name already exists","failed to create issue status"))?;
    tx.commit()
        .await
        .map_err(|_| internal("failed to create issue status"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::StatusChanged {
            action: StatusAction::Created,
        },
    )
    .await;
    Ok((StatusCode::CREATED, GoJson(output(&row))))
}
#[derive(Default)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    color: Option<String>,
    icon: Option<String>,
    position: Option<f64>,
}

impl<'de> Deserialize<'de> for Update {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct UpdateVisitor;
        impl<'de> Visitor<'de> for UpdateVisitor {
            type Value = Update;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a status request object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Update, E> {
                Ok(Update::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Update, M::Error> {
                let mut request = Update::default();
                while let Some(key) = map.next_key::<String>()? {
                    if matches_go_field(&key, "position") {
                        request.position = map.next_value::<Option<f64>>()?;
                    } else if matches_go_field(&key, "name") {
                        request.name = map.next_value::<Option<String>>()?;
                    } else if matches_go_field(&key, "description") {
                        request.description = map.next_value::<Option<String>>()?;
                    } else if matches_go_field(&key, "color") {
                        request.color = map.next_value::<Option<String>>()?;
                    } else if matches_go_field(&key, "icon") {
                        request.icon = map.next_value::<Option<String>>()?;
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(UpdateVisitor)
    }
}
async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, Failure> {
    admin(&state, ws, user, actor).await?;
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid issue status id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to load issue status"))?;
    let old = db
        .query_opt(
            "SELECT * FROM issue_status WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal("failed to load issue status"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "issue status not found"))?;
    let body: Update = account_request(&body).map_err(|_| bad("invalid request body"))?;
    if old.get::<_, bool>("is_system") {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "built-in statuses cannot be modified",
        ));
    }
    if old
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Err(fail(
            StatusCode::CONFLICT,
            "archived statuses cannot be modified",
        ));
    }
    let name = body.name.as_deref().map(name).transpose()?;
    let description = body.description.as_deref().map(description).transpose()?;
    let color = body.color.as_deref().map(color).transpose()?;
    let icon = body.icon.as_deref().map(icon).transpose()?;
    let row = db.query_opt("UPDATE issue_status SET name=COALESCE($3,name),description=COALESCE($4,description),color=COALESCE($5,color),icon=COALESCE($6,icon),position=COALESCE($7,position),updated_at=now() WHERE id=$1 AND workspace_id=$2 AND NOT is_system AND archived_at IS NULL RETURNING *",
        &[&id,&ws,&name,&description,&color,&icon,&body.position]).await
        .map_err(|e| write_failure(e,"a status with this name already exists","failed to update issue status"))?
        .ok_or_else(|| fail(StatusCode::CONFLICT,"status is no longer editable"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::StatusChanged {
            action: StatusAction::Updated,
        },
    )
    .await;
    Ok(GoJson(output(&row)))
}
async fn archive(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, Failure> {
    admin(&state, ws, user, actor).await?;
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid issue status id"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to archive issue status"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to archive issue status"))?;
    lock(&tx, ws, "failed to archive issue status").await?;
    let row = tx
        .query_opt(
            "SELECT * FROM issue_status WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| internal("failed to load issue status"))?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "issue status not found"))?;
    if row.get::<_, bool>("is_system") {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "built-in statuses cannot be archived",
        ));
    }
    if row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
        .is_some()
    {
        return Ok(GoJson(output(&row)));
    }
    let key: String = row.get("key");
    let count: i64 = tx
        .query_one(
            "SELECT count(*) FROM issue WHERE workspace_id=$1 AND status=$2",
            &[&ws, &key],
        )
        .await
        .map_err(|_| internal("failed to check issues using status"))?
        .get(0);
    if count > 0 {
        return Err(Failure(
            StatusCode::CONFLICT,
            json!({
                "error": format!("cannot archive status: {count} issues still use it; move them to another status first"),
                "code": "issue_status_in_use", "issue_count": count
            }),
        ));
    }
    let workflow_names: Vec<String> = tx.query("SELECT name FROM issue_workflow WHERE workspace_id=$1 AND NOT is_default AND steps @> jsonb_build_array(jsonb_build_object('status_key',$2::text)) ORDER BY lower(name)", &[&ws,&key]).await
        .map_err(|_| internal("failed to check workflows using status"))?.iter().map(|r|r.get(0)).collect();
    if let Some(first) = workflow_names.first() {
        return Err(Failure(
            StatusCode::CONFLICT,
            json!({
                "error": format!("cannot archive status: workflow {first:?} still uses it; remove it from the workflow first"),
                "code": "issue_status_in_workflow", "workflows": workflow_names
            }),
        ));
    }
    let row = tx.query_opt("UPDATE issue_status SET archived_at=now(),updated_at=now() WHERE id=$1 AND workspace_id=$2 AND NOT is_system AND archived_at IS NULL RETURNING *", &[&id,&ws]).await
        .map_err(|_| internal("failed to archive issue status"))?.ok_or_else(|| fail(StatusCode::CONFLICT,"status is no longer archivable"))?;
    tx.execute("UPDATE issue_workflow SET steps=COALESCE((SELECT jsonb_agg(step ORDER BY ord) FROM jsonb_array_elements(steps) WITH ORDINALITY AS s(step,ord) WHERE step->>'status_key'<>$2),'[]'::jsonb),updated_at=now() WHERE workspace_id=$1 AND is_default AND steps @> jsonb_build_array(jsonb_build_object('status_key',$2::text))", &[&ws,&key]).await
        .map_err(|_| internal("failed to archive issue status"))?;
    tx.commit()
        .await
        .map_err(|_| internal("failed to archive issue status"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::StatusChanged {
            action: StatusAction::Archived,
        },
    )
    .await;
    publish(
        &state,
        ws,
        user,
        CatalogChange::WorkflowChanged {
            action: WorkflowAction::Updated,
        },
    )
    .await;
    Ok(GoJson(output(&row)))
}
#[derive(Default)]
struct Reorder {
    category: String,
    ids: Vec<String>,
    include_system: bool,
}

impl<'de> Deserialize<'de> for Reorder {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct ReorderVisitor;
        impl<'de> Visitor<'de> for ReorderVisitor {
            type Value = Reorder;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a status reorder object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Reorder, E> {
                Ok(Reorder::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Reorder, M::Error> {
                let mut request = Reorder::default();
                while let Some(key) = map.next_key::<String>()? {
                    if matches_go_field(&key, "category") {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            request.category = value;
                        }
                    } else if matches_go_field(&key, "ids") {
                        // Go decodes null slice elements as empty strings.
                        request.ids = map
                            .next_value::<Option<Vec<Option<String>>>>()?
                            .unwrap_or_default()
                            .into_iter()
                            .map(Option::unwrap_or_default)
                            .collect();
                    } else if matches_go_field(&key, "include_system") {
                        if let Some(value) = map.next_value::<Option<bool>>()? {
                            request.include_system = value;
                        }
                    } else {
                        let _: IgnoredAny = map.next_value()?;
                    }
                }
                Ok(request)
            }
        }
        decoder.deserialize_any(ReorderVisitor)
    }
}
async fn reorder(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<GoJson<Value>, Failure> {
    admin(&state, ws, user, actor).await?;
    let body: Reorder = account_request(&body).map_err(|_| bad("invalid request body"))?;
    let category = parse_category(&body.category)?;
    if body.ids.is_empty() {
        return Err(bad("ids must not be empty"));
    }
    let mut ids = Vec::with_capacity(body.ids.len());
    let mut seen = HashSet::new();
    for raw in &body.ids {
        if !seen.insert(raw) {
            return Err(bad("duplicate ids"));
        }
        ids.push(Uuid::parse_str(raw).map_err(|_| bad("invalid issue status id"))?);
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| internal("failed to reorder issue statuses"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| internal("failed to reorder issue statuses"))?;
    lock(&tx, ws, "failed to reorder issue statuses").await?;
    let all = tx
        .query(
            &format!(
                "SELECT * FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL {ORDER}"
            ),
            &[&ws],
        )
        .await
        .map_err(|_| internal("failed to reorder issue statuses"))?;
    let active: Vec<_> = all
        .iter()
        .filter(|r| {
            r.get::<_, String>("category") == category
                && (body.include_system || !r.get::<_, bool>("is_system"))
        })
        .collect();
    for id in &ids {
        if active.iter().any(|r| r.get::<_, Uuid>("id") == *id) {
            continue;
        }
        let row = tx
            .query_opt(
                "SELECT * FROM issue_status WHERE id=$1 AND workspace_id=$2",
                &[id, &ws],
            )
            .await
            .map_err(|_| internal("failed to reorder issue statuses"))?
            .ok_or_else(|| fail(StatusCode::NOT_FOUND, "issue status not found"))?;
        if row.get::<_, bool>("is_system") && !body.include_system {
            return Err(fail(
                StatusCode::FORBIDDEN,
                "include_system is required to reorder built-in statuses",
            ));
        }
        if row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
        {
            return Err(fail(
                StatusCode::CONFLICT,
                "archived statuses cannot be reordered",
            ));
        }
        if row.get::<_, String>("category") != category {
            return Err(bad("ids must all belong to the requested category"));
        }
        return Err(fail(
            StatusCode::CONFLICT,
            "issue status catalog changed during reorder",
        ));
    }
    if active.len() != ids.len() {
        return Err(fail(
            StatusCode::CONFLICT,
            "ids must name every active status in the requested reorder scope",
        ));
    }
    let positions: Vec<f64> = active
        .iter()
        .enumerate()
        .map(|(i, r)| {
            if body.include_system {
                (i + 1) as f64
            } else {
                r.get("position")
            }
        })
        .collect();
    let affected = tx.execute("UPDATE issue_status s SET position=($3::float8[])[v.ordinality], updated_at=now() FROM unnest($2::uuid[]) WITH ORDINALITY AS v(id,ordinality) WHERE s.id=v.id AND s.workspace_id=$1 AND ($4 OR NOT s.is_system) AND s.archived_at IS NULL", &[&ws,&ids,&positions,&body.include_system]).await
        .map_err(|_| internal("failed to reorder issue statuses"))?;
    if affected as usize != ids.len() {
        return Err(fail(
            StatusCode::CONFLICT,
            "issue status catalog changed during reorder",
        ));
    }
    let rows = tx
        .query(
            &format!("SELECT * FROM issue_status WHERE workspace_id=$1 {ORDER}"),
            &[&ws],
        )
        .await
        .map_err(|_| internal("failed to list issue statuses"))?;
    tx.commit()
        .await
        .map_err(|_| internal("failed to reorder issue statuses"))?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::StatusChanged {
            action: StatusAction::Reordered,
        },
    )
    .await;
    let statuses: Vec<_> = rows.iter().map(output).collect();
    Ok(GoJson(
        json!({"total":statuses.len(),"statuses":statuses,"categories":BUILT_INS}),
    ))
}

#[cfg(test)]
mod request_tests {
    use super::{Create, Reorder, Update, key};
    use crate::api::account_catalog_write_profile::account_request;

    #[test]
    fn explicit_key_uses_go_simple_unicode_lowercase() {
        assert_eq!(key("İnceleme").unwrap(), "inceleme");
        assert!(key("ÄBC").is_err());
    }

    #[test]
    fn create_uses_go_decoder_first_value_and_scalar_null_rules() {
        let create: Create = account_request(br#"{"NAME":"First","name":null,"naMe":"Second","key":"draft","key":null,"category":"started"} trailing"#).unwrap();
        assert_eq!(create.name, "Second");
        assert_eq!(create.key, "draft");
        assert_eq!(create.category, "started");
        assert!(account_request::<Create>(br#"{"name":true}"#).is_err());
        assert!(account_request::<Create>(br#"["name"]"#).is_err());
    }

    #[test]
    fn update_pointer_null_clears_prior_value() {
        let update: Update = account_request(br##"{"Name":"Set","name":null,"POSITION":1.5,"position":null,"color":"#aabbcc"} garbage"##).unwrap();
        assert_eq!(update.name, None);
        assert_eq!(update.position, None);
        assert_eq!(update.color.as_deref(), Some("#aabbcc"));
        assert!(account_request::<Update>(br#"{"position":"1.5"}"#).is_err());
    }

    #[test]
    fn reorder_null_and_duplicate_fields_keep_go_defaults() {
        let reorder: Reorder = account_request(br#"{"CATEGORY":"started","category":null,"IDS":[null,"a"],"include_system":true,"INCLUDE_SYSTEM":null} trailing"#).unwrap();
        assert_eq!(reorder.category, "started");
        assert_eq!(reorder.ids, ["", "a"]);
        assert!(reorder.include_system);
        assert!(account_request::<Reorder>(br#"{"ids":[42]}"#).is_err());
    }
}
