//! Workspace property definitions; value writes belong to the issue-write branch.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    account_catalog_write_catalog_publish::{CatalogChange, publish},
    catalog::{WorkspaceId, property_row},
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{patch, post},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio_postgres::{Row, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/properties", post(create))
        .route("/api/properties/", post(create))
        .route("/api/properties/{id}", patch(update))
        .route("/api/properties/{id}/", patch(update))
        .layer(DefaultBodyLimit::disable())
}

#[derive(Debug)]
struct Failure(StatusCode, String);
impl From<ApiError> for Failure {
    fn from(err: ApiError) -> Self {
        Self(err.0, err.1.into())
    }
}
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let mut response = GoJson(json!({"error":self.1})).into_response();
        *response.status_mut() = self.0;
        response
    }
}
fn bad(message: impl Into<String>) -> Failure {
    Failure(StatusCode::BAD_REQUEST, message.into())
}
fn fail(status: StatusCode, message: &'static str) -> Failure {
    Failure(status, message.into())
}

#[path = "account_catalog_write_properties_decode.rs"]
mod decode;

#[derive(Default, Serialize)]
struct OptionInput {
    id: String,
    name: String,
    color: String,
}
#[derive(Default, Serialize)]
struct Config {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<OptionInput>,
    #[serde(skip)]
    option_tail: Vec<OptionInput>,
}
#[derive(Default, Serialize)]
struct Create {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    description: String,
    icon: String,
    config: Option<Config>,
}
#[derive(Default, Serialize)]
struct Update {
    name: Option<String>,
    description: Option<String>,
    icon: Option<String>,
    config: Option<Config>,
    archived: Option<bool>,
}

fn name(raw: &str) -> Result<String, Failure> {
    if raw.chars().any(char::is_control) {
        return Err(bad(
            "name cannot contain tabs, newlines, or control characters",
        ));
    }
    let value = raw.trim();
    if value.is_empty() {
        return Err(bad("name is required"));
    }
    if value.chars().count() > 32 {
        return Err(bad("name must be 32 characters or fewer"));
    }
    let normalized = value.to_lowercase().replace(' ', "_");
    if [
        "status",
        "priority",
        "assignee",
        "project",
        "parent",
        "stage",
        "label",
        "labels",
        "start_date",
        "due_date",
        "title",
        "description",
        "creator",
        "created_at",
        "updated_at",
        "metadata",
        "properties",
    ]
    .contains(&normalized.as_str())
    {
        return Err(bad(format!(
            "{value:?} is reserved for a built-in issue field"
        )));
    }
    Ok(value.to_owned())
}
fn icon(raw: &str) -> Result<String, Failure> {
    if raw.chars().any(char::is_control) {
        return Err(bad(
            "icon cannot contain tabs, newlines, or control characters",
        ));
    }
    let value = raw.trim();
    if value.chars().count() > 32 {
        return Err(bad("icon must be 32 characters or fewer"));
    }
    if !value.is_empty()
        && ![
            "circle-dot",
            "signal-high",
            "user-round",
            "folder-kanban",
            "calendar-days",
            "tag",
            "milestone",
            "flag",
            "bookmark",
            "star",
            "target",
            "shield",
            "bug",
            "zap",
            "rocket",
            "sparkles",
            "lightbulb",
            "globe-2",
            "link",
            "hash",
            "list-checks",
            "circle-check",
            "clock-3",
            "briefcase-business",
            "layers-3",
            "gauge",
            "database",
            "code-2",
            "palette",
            "megaphone",
            "map-pin",
            "package",
            "wrench",
            "heart",
            "circle-alert",
            "lock-keyhole",
        ]
        .contains(&value)
    {
        return Err(bad("icon must be a supported icon key"));
    }
    Ok(value.into())
}
fn description(raw: &str) -> Result<String, Failure> {
    if raw.chars().count() > 500 {
        return Err(bad("description must be 500 characters or fewer"));
    }
    Ok(raw.trim().replace('\0', ""))
}
fn config(kind: &str, input: Option<Config>) -> Result<Value, Failure> {
    let select = kind == "select" || kind == "multi_select";
    let options = input.map_or_else(Vec::new, |c| c.options);
    if !select {
        if !options.is_empty() {
            return Err(bad(format!("type {kind:?} does not accept options")));
        }
        return Ok(json!({}));
    }
    if options.is_empty() {
        return Err(bad("select properties require at least one option"));
    }
    if options.len() > 200 {
        return Err(bad("a property cannot have more than 200 options"));
    }
    let (mut names, mut ids) = (HashSet::new(), HashSet::new());
    let mut out = Vec::with_capacity(options.len());
    for option in options {
        if option.name.chars().any(char::is_control) {
            return Err(bad(
                "option name cannot contain tabs, newlines, or control characters",
            ));
        }
        let label = option.name.trim();
        if label.is_empty() {
            return Err(bad("option name is required"));
        }
        if label.chars().count() > 32 {
            return Err(bad("option name must be 32 characters or fewer"));
        }
        if !names.insert(label.to_lowercase()) {
            return Err(bad(format!("duplicate option name {label:?}")));
        }
        let color = option.color.trim();
        let hex = color.strip_prefix('#').unwrap_or(color);
        if hex.len() != 6 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(bad(format!(
                "option {label:?}: color must be a 6-digit hex value like #3b82f6"
            )));
        }
        let id = if option.id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            Uuid::parse_str(option.id.trim())
                .map_err(|_| bad(format!("option {label:?}: id must be a UUID")))?;
            option.id.trim().to_owned()
        };
        if !ids.insert(id.clone()) {
            return Err(bad(format!("duplicate option id {id:?}")));
        }
        out.push(json!({"id":id,"name":label,"color":format!("#{}",hex.to_ascii_lowercase())}));
    }
    Ok(json!({"options":out}))
}
fn unique(err: tokio_postgres::Error, operation: &'static str) -> Failure {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        fail(
            StatusCode::CONFLICT,
            "a property with that name already exists",
        )
    } else {
        fail(StatusCode::INTERNAL_SERVER_ERROR, operation)
    }
}
async fn admin(state: &ApiState, ws: Uuid, user: Uuid, actor: &Actor) -> Result<(), Failure> {
    if !matches!(actor, Actor::Human) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "agents cannot manage property definitions",
        ));
    }
    let db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to check workspace role",
        )
    })?;
    let row = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to check workspace role",
            )
        })?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "workspace not found"))?;
    if !["owner", "admin"].contains(&row.get::<_, String>(0).as_str()) {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "insufficient workspace permissions",
        ));
    }
    Ok(())
}
async fn lock(tx: &tokio_postgres::Transaction<'_>, key: String) -> Result<(), Failure> {
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        &[&key],
    )
    .await
    .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock property"))?;
    Ok(())
}
fn response(row: &Row) -> super::catalog::Property {
    property_row(row, 0)
}
const CAP: &str =
    "a workspace cannot have more than 20 active properties; archive unused ones first";

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<super::catalog::Property>), Failure> {
    admin(&state, ws, user, &actor).await?;
    let body = body.map_err(|_| bad("invalid request body"))?;
    let req: Create = super::account_catalog_write_profile::account_request(&body)?;
    let name = name(&req.name)?;
    if ![
        "text",
        "number",
        "select",
        "multi_select",
        "date",
        "checkbox",
        "url",
        "actor",
        "multi_actor",
        "multi_text",
        "multi_url",
    ]
    .contains(&req.kind.as_str())
    {
        return Err(bad(format!(
            "invalid type {:?}; valid types: text, number, select, multi_select, date, checkbox, url, actor, multi_actor, multi_text, multi_url",
            req.kind
        )));
    }
    let desc = description(&req.description)?;
    let icon = icon(&req.icon)?;
    let cfg = config(&req.kind, req.config)?;
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create property",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create property",
        )
    })?;
    lock(&tx, format!("props:{ws}")).await?;
    let active: i64 = tx
        .query_one(
            "SELECT count(*) FROM issue_property WHERE workspace_id=$1 AND archived_at IS NULL",
            &[&ws],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create property",
            )
        })?
        .get(0);
    if active >= 20 {
        return Err(bad(CAP));
    }
    let row=tx.query_one("INSERT INTO issue_property(workspace_id,name,type,description,icon,config,position) SELECT $1,$2,$3,$4,$5,$6,COALESCE((SELECT max(position) FROM issue_property WHERE workspace_id=$1),0)+1 RETURNING *", &[&ws,&name,&req.kind,&desc,&icon,&cfg])
        .await.map_err(|e| unique(e,"failed to create property"))?;
    let result = response(&row);
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create property",
        )
    })?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::PropertyCreated {
            property: serde_json::to_value(&result).expect("serializable property"),
        },
    )
    .await;
    Ok((StatusCode::CREATED, GoJson(result)))
}
async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<super::catalog::Property>, Failure> {
    admin(&state, ws, user, &actor).await?;
    let id = Uuid::parse_str(&raw).map_err(|_| bad("invalid property id"))?;
    let body = body.map_err(|_| bad("invalid request body"))?;
    let req: Update = super::account_catalog_write_profile::account_request(&body)?;
    let mut db = state.pool.get().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update property",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update property",
        )
    })?;
    lock(&tx, format!("props:{ws}")).await?;
    lock(&tx, format!("prop:{id}")).await?;
    let existing = tx
        .query_opt(
            "SELECT * FROM issue_property WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update property",
            )
        })?
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, "property not found"))?;
    let name = req.name.as_deref().map(name).transpose()?;
    let desc = req.description.as_deref().map(description).transpose()?;
    let icon = req.icon.as_deref().map(icon).transpose()?;
    let cfg = match req.config {
        Some(c) => Some(config(existing.get::<_, String>("type").as_str(), Some(c))?),
        None => None,
    };
    if let Some(next) = &cfg {
        let old: Value = existing.get("config");
        let old_options = old.get("options").and_then(Value::as_array);
        let next_ids: HashSet<&str> = next
            .get("options")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|o| o.get("id").and_then(Value::as_str))
            .collect();
        let removed: Vec<&str> = old_options
            .into_iter()
            .flatten()
            .filter_map(|o| o.get("id").and_then(Value::as_str))
            .filter(|id| !next_ids.contains(id))
            .collect();
        if !removed.is_empty() {
            let rows=tx.query("SELECT opt::text AS option_id, count(i.id) AS usage_count FROM unnest($1::text[]) AS opt LEFT JOIN issue i ON i.workspace_id=$2 AND (i.properties -> $3::text) ? opt GROUP BY opt HAVING count(i.id)>0", &[&removed,&ws,&id.to_string()]).await
                .map_err(|_| fail(StatusCode::INTERNAL_SERVER_ERROR,"failed to update property"))?;
            if !rows.is_empty() {
                let mut used: Vec<String> = rows
                    .iter()
                    .map(|r| {
                        let option_id: String = r.get("option_id");
                        let label = old_options
                            .unwrap()
                            .iter()
                            .find(|o| o["id"] == option_id)
                            .and_then(|o| o["name"].as_str())
                            .unwrap_or(&option_id);
                        format!("{label:?} ({} issues)", r.get::<_, i64>("usage_count"))
                    })
                    .collect();
                used.sort();
                return Err(Failure(
                    StatusCode::CONFLICT,
                    format!(
                        "cannot remove options still in use: {}; clear or change those values first",
                        used.join(", ")
                    ),
                ));
            }
        }
    }
    if req.archived == Some(false)
        && existing
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
    {
        let active: i64 = tx
            .query_one(
                "SELECT count(*) FROM issue_property WHERE workspace_id=$1 AND archived_at IS NULL",
                &[&ws],
            )
            .await
            .map_err(|_| {
                fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to update property",
                )
            })?
            .get(0);
        if active >= 20 {
            return Err(bad(CAP));
        }
    }
    let archived_at = if req.archived == Some(true) {
        Some(chrono::Utc::now())
    } else {
        None
    };
    let row=tx.query_one("UPDATE issue_property SET name=COALESCE($3,name),description=COALESCE($4,description),icon=COALESCE($5,icon),config=COALESCE($6,config),archived_at=CASE WHEN $7::bool THEN $8::timestamptz ELSE archived_at END,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws,&name,&desc,&icon,&cfg,&req.archived.is_some(),&archived_at]).await
        .map_err(|e| unique(e,"failed to update property"))?;
    let result = response(&row);
    tx.commit().await.map_err(|_| {
        fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update property",
        )
    })?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::PropertyUpdated {
            property: serde_json::to_value(&result).expect("serializable property"),
        },
    )
    .await;
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn property_validation() {
        assert!(name("Due Date").is_err());
        assert_eq!(name("Urgency").unwrap(), "Urgency");
        assert!(icon("🧪").is_err());
        assert!(
            config(
                "text",
                Some(Config {
                    options: vec![OptionInput {
                        id: String::new(),
                        name: "x".into(),
                        color: "#ff00ff".into()
                    }],
                    ..Config::default()
                })
            )
            .is_err()
        );
        assert_eq!(config("text", None).unwrap(), json!({}));
        let option = config(
            "select",
            Some(Config {
                options: vec![OptionInput {
                    id: String::new(),
                    name: "x".into(),
                    color: "FF00ff".into(),
                }],
                ..Config::default()
            }),
        )
        .unwrap();
        assert_eq!(option["options"][0]["color"], "#ff00ff");
    }
}
