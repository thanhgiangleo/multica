//! Project resource mutations. Shared event publication is wired by the integration owner.
use super::account_catalog_write::view_preference::parse_go_uuid;
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    account_catalog_write_catalog_publish::{CatalogChange, publish},
    catalog::WorkspaceId,
    error, timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{post, put},
};
use deadpool_postgres::GenericClient;
use serde_json::{Map, Value, json};
use tokio_postgres::{Row, error::SqlState};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/projects/{id}/resources", post(create))
        .route(
            "/api/projects/{id}/resources/{resource_id}",
            put(update).delete(remove),
        )
        .layer(DefaultBodyLimit::disable())
}

#[path = "account_catalog_write_resource_decode.rs"]
pub(super) mod decode;
#[path = "account_catalog_write_resource_gate.rs"]
mod gate;

pub(super) const COLUMNS: &str = "id, project_id, workspace_id, resource_type, resource_ref, label, position, created_at, created_by";

pub(super) fn response(row: &Row) -> Value {
    json!({
        "id": row.get::<_, Uuid>("id"),
        "project_id": row.get::<_, Uuid>("project_id"),
        "workspace_id": row.get::<_, Uuid>("workspace_id"),
        "resource_type": row.get::<_, String>("resource_type"),
        "resource_ref": row.get::<_, Value>("resource_ref"),
        "label": row.get::<_, Option<String>>("label"),
        "position": row.get::<_, i32>("position"),
        "created_at": timestamp(row.get("created_at")),
        "created_by": row.get::<_, Option<Uuid>>("created_by"),
    })
}

fn id(raw: &str, kind: &'static str) -> Result<Uuid, ApiError> {
    parse_go_uuid(raw).ok_or_else(|| error(StatusCode::BAD_REQUEST, kind))
}

fn member(actor: Actor) -> Result<(), ApiError> {
    if matches!(actor, Actor::Human | Actor::Task(_)) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "not a workspace member"))
    }
}

async fn project<C: GenericClient>(db: &C, ws: Uuid, id: Uuid) -> Result<(), ApiError> {
    if db
        .query_opt(
            "SELECT 1 FROM project WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project not found"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "project not found"));
    }
    Ok(())
}

fn string<'a>(obj: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Value::as_str)
}

pub(super) fn normalize(kind: &str, ref_value: &Value) -> Result<Value, ApiError> {
    let bad = |message| error(StatusCode::BAD_REQUEST, message);
    // Go JSON decoding accepts unknown fields and ignores them on normalization.
    let obj = ref_value
        .as_object()
        .ok_or_else(|| bad("invalid resource_ref"))?;
    let fields: &[&str] = match kind {
        "github_repo" => &["url", "default_branch_hint", "ref"],
        "local_directory" => &["local_path", "daemon_id", "label", "execution_mode"],
        _ => return Err(bad("unknown resource_type")),
    };
    if fields.iter().any(|key| {
        obj.get(*key)
            .is_some_and(|v| !v.is_null() && !v.is_string())
    }) {
        return Err(bad("invalid resource_ref"));
    }
    match kind {
        "github_repo" => {
            let url = string(obj, "url").unwrap_or("").trim();
            if url.is_empty() {
                return Err(bad("github_repo: url is required"));
            }
            let valid_url = reqwest::Url::parse(url).ok().is_some_and(|parsed| {
                matches!(parsed.scheme(), "http" | "https" | "ssh" | "git")
                    && parsed.host_str().is_some_and(|host| !host.is_empty())
            }) || (!url.contains(' ')
                && !url.contains("://")
                && url.split_once(':').is_some_and(|(host_part, path)| {
                    // Go only treats the first @ before the colon as a user separator.
                    let host = host_part
                        .split_once('@')
                        .map_or(host_part, |(_, host)| host);
                    !host.is_empty()
                        && !path.is_empty()
                        && (host_part.contains('@') || !path.contains('@'))
                }));
            if !valid_url {
                return Err(bad(
                    "github_repo: url must be a valid http(s) or ssh git URL",
                ));
            }
            let hint = string(obj, "default_branch_hint").unwrap_or("").trim();
            let reference = string(obj, "ref").unwrap_or("").trim();
            if reference.len() > 255 {
                return Err(bad("github_repo: ref must be at most 255 characters"));
            }
            if !reference.is_empty()
                && (reference
                    .chars()
                    .any(|c| c <= ' ' || c == '\u{7f}' || "~^:?*[\\".contains(c))
                    || reference.contains("..")
                    || reference.contains("@{")
                    || reference == "@"
                    || reference.starts_with('/')
                    || reference.ends_with('/')
                    || reference.contains("//")
                    || reference.ends_with('.')
                    || reference
                        .split('/')
                        .any(|s| s.starts_with('.') || s.ends_with(".lock")))
            {
                return Err(bad(
                    "github_repo: ref is not a valid branch, tag, or commit",
                ));
            }
            let mut out = Map::new();
            out.insert("url".into(), json!(url));
            if !hint.is_empty() {
                out.insert("default_branch_hint".into(), json!(hint));
            }
            if !reference.is_empty() {
                out.insert("ref".into(), json!(reference));
            }
            Ok(Value::Object(out))
        }
        "local_directory" => {
            let path = string(obj, "local_path").unwrap_or("").trim();
            let daemon = string(obj, "daemon_id").unwrap_or("").trim();
            if path.is_empty() {
                return Err(bad("local_directory: local_path is required"));
            }
            let bytes = path.as_bytes();
            if !(path.starts_with('/')
                || path.starts_with("\\\\")
                || (bytes.len() >= 3
                    && bytes[0].is_ascii_alphabetic()
                    && bytes[1] == b':'
                    && (bytes[2] == b'/' || bytes[2] == b'\\')))
            {
                return Err(bad("local_directory: local_path must be an absolute path"));
            }
            if daemon.is_empty() {
                return Err(bad("local_directory: daemon_id is required"));
            }
            let mode = string(obj, "execution_mode").unwrap_or("").trim();
            if !matches!(mode, "" | "in_place" | "worktree") {
                return Err(bad(
                    "local_directory: execution_mode must be in_place or worktree",
                ));
            }
            let mut out = Map::new();
            out.insert("local_path".into(), json!(path));
            out.insert("daemon_id".into(), json!(daemon));
            for key in ["label", "execution_mode"] {
                if let Some(value) = string(obj, key).map(str::trim).filter(|v| !v.is_empty()) {
                    out.insert(key.into(), json!(value));
                }
            }
            Ok(Value::Object(out))
        }
        _ => Err(bad("unknown resource_type")),
    }
}

async fn daemon_conflict<C: GenericClient>(
    db: &C,
    project: Uuid,
    kind: &str,
    ref_value: &Value,
    except: Option<Uuid>,
) -> Result<bool, ApiError> {
    if kind != "local_directory" {
        return Ok(false);
    }
    let daemon = ref_value["daemon_id"].as_str().unwrap_or_default();
    let rows = db.query("SELECT id FROM project_resource WHERE project_id=$1 AND resource_type='local_directory' AND resource_ref->>'daemon_id'=$2 AND ($3::uuid IS NULL OR id<>$3)", &[&project, &daemon, &except])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check existing resources"))?;
    Ok(!rows.is_empty())
}

pub(super) async fn worktree_capable<C: GenericClient>(
    db: &C,
    ws: Uuid,
    kind: &str,
    ref_value: &Value,
) -> Result<Option<Value>, ApiError> {
    if kind != "local_directory" || ref_value["execution_mode"] != "worktree" {
        return Ok(None);
    }
    let daemon = ref_value["daemon_id"].as_str().unwrap_or_default();
    // Go reads every provider row in creation order. Equal last_seen_at values
    // keep the first row, not the highest UUID. Version selection is independent.
    let rows = db.query("SELECT metadata::text,last_seen_at FROM agent_runtime WHERE workspace_id=$1 AND daemon_id=$2 ORDER BY created_at ASC", &[&ws, &daemon])
        .await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to check runtime capabilities"))?;
    let runtimes: Vec<_> = rows
        .iter()
        .map(|row| gate::Runtime {
            metadata: row.get(0),
            seen: row.get(1),
        })
        .collect();
    Ok(gate::rejection(ref_value, &runtimes))
}

fn write_error(err: tokio_postgres::Error, action: &'static str) -> ApiError {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        error(
            StatusCode::CONFLICT,
            "this resource is already attached to the project",
        )
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, action)
    }
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<Response, ApiError> {
    member(actor)?;
    let project_id = id(&raw, "invalid project id")?;
    {
        let db = state.pool.get().await.map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create project resource",
            )
        })?;
        project(&db, ws, project_id).await?;
    }
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let request: decode::Create = super::account_catalog_write_profile::account_request(&body)?;
    let kind = request.resource_type.trim();
    if kind.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "resource_type is required"));
    }
    let ref_value = decode::normalize(kind, request.resource_ref.as_deref())?;
    let label = request
        .label
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let pos = request.position;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create project resource",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create project resource",
        )
    })?;
    project(&tx, ws, project_id).await?;
    // Serialize creates and updates for a project; the Go application-only daemon check races otherwise.
    tx.query_one(
        "SELECT id FROM project WHERE id=$1 FOR UPDATE",
        &[&project_id],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create project resource",
        )
    })?;
    if daemon_conflict(&tx, project_id, kind, &ref_value, None).await? {
        return Err(error(
            StatusCode::CONFLICT,
            "this daemon already has a local_directory attached to the project; remove it before adding another",
        ));
    }
    if let Some(rejection) = worktree_capable(&tx, ws, kind, &ref_value).await? {
        return Ok((StatusCode::UNPROCESSABLE_ENTITY, GoJson(rejection)).into_response());
    }
    let position = if let Some(pos) = pos {
        pos
    } else {
        tx.query_one(
            "SELECT count(*)::int FROM project_resource WHERE project_id=$1",
            &[&project_id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to create project resource",
            )
        })?
        .get(0)
    };
    let sql = format!(
        "INSERT INTO project_resource(project_id,workspace_id,resource_type,resource_ref,label,position,created_by) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING {COLUMNS}"
    );
    let row = tx
        .query_one(
            &sql,
            &[
                &project_id,
                &ws,
                &kind,
                &ref_value,
                &label,
                &position,
                &user,
            ],
        )
        .await
        .map_err(|err| write_error(err, "failed to create project resource"))?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create project resource",
        )
    })?;
    drop(db);
    let resource = response(&row);
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
    Ok((StatusCode::CREATED, GoJson(resource)).into_response())
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_project, raw_resource)): Path<(String, String)>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<Response, ApiError> {
    member(actor)?;
    let project_id = id(&raw_project, "invalid project id")?;
    let resource_id = id(&raw_resource, "invalid resource id")?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update project resource",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start update transaction",
        )
    })?;
    project(&tx, ws, project_id).await?;
    // Project lock precedes row lock in both paths to prevent concurrent daemon duplicates.
    tx.query_one(
        "SELECT id FROM project WHERE id=$1 FOR UPDATE",
        &[&project_id],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update project resource",
        )
    })?;
    let sql = format!(
        "SELECT {COLUMNS} FROM project_resource WHERE id=$1 AND project_id=$2 AND workspace_id=$3 FOR UPDATE"
    );
    let existing = tx
        .query_opt(&sql, &[&resource_id, &project_id, &ws])
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "project resource not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "project resource not found"))?;
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let fields: Option<std::collections::BTreeMap<String, Box<serde_json::value::RawValue>>> =
        super::account_catalog_write_profile::account_request(&body)?;
    let obj = fields.unwrap_or_default();
    let kind: String = existing.get("resource_type");
    let stored: Value = existing.get("resource_ref");
    let next = if let Some(value) = obj.get("resource_ref") {
        decode::normalize(&kind, Some(value))?
    } else {
        stored.clone()
    };
    if daemon_conflict(&tx, project_id, &kind, &next, Some(resource_id)).await? {
        return Err(error(
            StatusCode::CONFLICT,
            "another local_directory on this daemon is already attached to the project",
        ));
    }
    let old_label = stored["label"].as_str().unwrap_or_default().trim();
    let new_label = next["label"].as_str().unwrap_or_default().trim();
    let rename_only = kind == "local_directory" && obj.contains_key("resource_ref") && {
        let mut old = stored.clone();
        let mut new = next.clone();
        old.as_object_mut().map(|o| o.remove("label"));
        new.as_object_mut().map(|o| o.remove("label"));
        old == new
    };
    if obj.contains_key("resource_ref")
        && !rename_only
        && let Some(rejection) = worktree_capable(&tx, ws, &kind, &next).await?
    {
        return Ok((StatusCode::UNPROCESSABLE_ENTITY, GoJson(rejection)).into_response());
    }
    let mut next_label: Option<String> = existing.get("label");
    let mut cleared = false;
    if let Some(value) = obj.get("label") {
        let value: Option<String> = serde_json::from_str(value.get())
            .map_err(|_| error(StatusCode::BAD_REQUEST, "label must be a string or null"))?;
        next_label = value
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        cleared = next_label.is_none();
    } else if rename_only && old_label != new_label {
        next_label = (!new_label.is_empty()).then(|| new_label.to_owned());
        cleared = next_label.is_none();
    }
    let next_position = if let Some(value) = obj.get("position") {
        serde_json::from_str::<Option<i32>>(value.get())
            .map_err(|_| error(StatusCode::BAD_REQUEST, "position must be an integer"))?
            .unwrap_or(existing.get("position"))
    } else {
        existing.get("position")
    };
    let mut next_ref = next;
    if kind == "local_directory" {
        let actual_name = next_label
            .as_deref()
            .or_else(|| (!cleared && !old_label.is_empty()).then_some(old_label));
        if (obj.contains_key("resource_ref") || next_label.is_some() || cleared)
            && let Some(map) = next_ref.as_object_mut()
        {
            if let Some(name) = actual_name {
                map.insert("label".into(), json!(name));
            } else {
                map.remove("label");
            }
        }
    }
    let sql = format!(
        "UPDATE project_resource SET resource_ref=$2,label=$3,position=$4 WHERE id=$1 RETURNING {COLUMNS}"
    );
    let row = tx
        .query_one(
            &sql,
            &[&resource_id, &next_ref, &next_label, &next_position],
        )
        .await
        .map_err(|err| write_error(err, "failed to update project resource"))?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update project resource",
        )
    })?;
    drop(db);
    let resource = response(&row);
    publish(
        &state,
        ws,
        user,
        CatalogChange::ResourceUpdated {
            resource: resource.clone(),
            project_id,
        },
    )
    .await;
    Ok(GoJson(resource).into_response())
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw_project, raw_resource)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    member(actor)?;
    let project_id = id(&raw_project, "invalid project id")?;
    let resource_id = id(&raw_resource, "invalid resource id")?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete project resource",
        )
    })?;
    project(&db, ws, project_id).await?;
    let count = db
        .execute(
            "DELETE FROM project_resource WHERE id=$1 AND project_id=$2 AND workspace_id=$3",
            &[&resource_id, &project_id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete project resource",
            )
        })?;
    if count == 0 {
        return Err(error(StatusCode::NOT_FOUND, "project resource not found"));
    }
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::ResourceDeleted {
            project_id,
            resource_id,
        },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}
