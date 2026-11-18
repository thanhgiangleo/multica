//! Workspace label definitions. Event publication is an integration dependency.
use super::{
    Actor, ApiError, ApiState, GoJson, Identity,
    account_catalog_write_catalog_publish::{CatalogChange, publish},
    catalog::{Label, WorkspaceId, label_row},
    error,
};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension, Path, State},
    http::StatusCode,
    routing::{post, put},
};
use serde::{Deserialize, Deserializer};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/labels", post(create))
        .route("/api/labels/", post(create))
        .route("/api/labels/{id}", put(update).delete(remove))
        .route("/api/labels/{id}/", put(update).delete(remove))
        .layer(DefaultBodyLimit::disable())
}

#[derive(Default)]
struct NewLabel {
    resource_type: String,
    name: String,
    description: String,
    color: String,
}

impl<'de> Deserialize<'de> for NewLabel {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (field, value) in super::account_catalog_write_profile::decode::fields(
            decoder,
            &["resource_type", "name", "description", "color"],
        )? {
            // Go's non-pointer string fields ignore null, including duplicate nulls.
            if let Some(value) = value {
                match field {
                    "resource_type" => request.resource_type = value,
                    "name" => request.name = value,
                    "description" => request.description = value,
                    "color" => request.color = value,
                    _ => unreachable!(),
                }
            }
        }
        Ok(request)
    }
}

#[derive(Default)]
struct LabelChanges {
    name: Option<String>,
    description: Option<String>,
    color: Option<String>,
}

impl<'de> Deserialize<'de> for LabelChanges {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut request = Self::default();
        for (field, value) in super::account_catalog_write_profile::decode::fields(
            decoder,
            &["name", "description", "color"],
        )? {
            match field {
                "name" => request.name = value,
                "description" => request.description = value,
                "color" => request.color = value,
                _ => unreachable!(),
            }
        }
        Ok(request)
    }
}

fn resource_type(raw: &str) -> Result<&str, ApiError> {
    match raw.trim() {
        "" => Ok("issue"),
        "issue" | "agent" | "skill" => Ok(raw.trim()),
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "resource_type must be issue, agent, or skill",
        )),
    }
}

fn name(raw: &str) -> Result<String, ApiError> {
    if raw.chars().any(char::is_control) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name cannot contain tabs, newlines, or control characters",
        ));
    }
    let value = raw.trim();
    if value.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "name is required"));
    }
    if value.chars().count() > 32 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name must be 32 characters or fewer",
        ));
    }
    Ok(value.to_owned())
}

fn color(raw: &str) -> Result<String, ApiError> {
    let value = raw.trim().strip_prefix('#').unwrap_or(raw.trim());
    if value.len() != 6 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "color must be a 6-digit hex value like #3b82f6",
        ));
    }
    Ok(format!("#{}", value.to_ascii_lowercase()))
}

fn description(raw: &str) -> String {
    raw.trim().replace('\0', "")
}

fn write_error(err: tokio_postgres::Error, operation: &'static str) -> ApiError {
    if err.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        error(
            StatusCode::CONFLICT,
            "a label with that name already exists",
        )
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, operation)
    }
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<Label>), ApiError> {
    // The frozen Go route is member-level, not human-only. Task credentials
    // remain workspace-bound by the shared auth and workspace guard.
    if !matches!(actor, Actor::Human | Actor::Task(_)) {
        return Err(error(StatusCode::FORBIDDEN, "not a workspace member"));
    }
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: NewLabel = super::account_catalog_write_profile::account_request(&body)?;
    let name = name(&req.name)?;
    let color = color(&req.color)?;
    let resource = resource_type(&req.resource_type)?;
    let description = description(&req.description);
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create label"))?;
    let row = db.query_one("INSERT INTO issue_label (workspace_id, resource_type, name, description, color) VALUES ($1,$2,$3,$4,$5) RETURNING *", &[&ws, &resource, &name, &description, &color])
        .await.map_err(|e| write_error(e, "failed to create label"))?;
    drop(db);
    let label = label_row(&row, 0);
    publish(
        &state,
        ws,
        user,
        CatalogChange::LabelCreated {
            label: serde_json::to_value(&label).expect("serializable label"),
        },
    )
    .await;
    Ok((StatusCode::CREATED, GoJson(label)))
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<GoJson<Label>, ApiError> {
    // The frozen Go route is member-level, not human-only. Task credentials
    // remain workspace-bound by the shared auth and workspace guard.
    if !matches!(actor, Actor::Human | Actor::Task(_)) {
        return Err(error(StatusCode::FORBIDDEN, "not a workspace member"));
    }
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: LabelChanges = super::account_catalog_write_profile::account_request(&body)?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid label id"))?;
    let name = req.name.as_deref().map(name).transpose()?;
    let description = req.description.as_deref().map(description);
    let color = req.color.as_deref().map(color).transpose()?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update label"))?;
    let row = db.query_opt("UPDATE issue_label SET name=COALESCE($3,name), description=COALESCE($4,description), color=COALESCE($5,color), updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id, &ws, &name, &description, &color])
        .await.map_err(|e| write_error(e, "failed to update label"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "label not found"))?;
    drop(db);
    let label = label_row(&row, 0);
    publish(
        &state,
        ws,
        user,
        CatalogChange::LabelUpdated {
            label: serde_json::to_value(&label).expect("serializable label"),
        },
    )
    .await;
    Ok(GoJson(label))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    // The frozen Go route is member-level, not human-only. Task credentials
    // remain workspace-bound by the shared auth and workspace guard.
    if !matches!(actor, Actor::Human | Actor::Task(_)) {
        return Err(error(StatusCode::FORBIDDEN, "not a workspace member"));
    }
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid label id"))?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start transaction",
        )
    })?;
    // Deleting issue assignments fires the wakeup trigger. Use transaction-local
    // identity, never client-provided actor headers or connection-level settings.
    let (kind, actor_id, task_id) = match &actor {
        Actor::Task(task) => ("agent", task.agent.to_string(), task.task.to_string()),
        _ => ("member", user.to_string(), String::new()),
    };
    tx.query_one(
        "SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)",
        &[&kind, &actor_id, &task_id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to start transaction"))?;
    // Verify ownership before touching global assignment tables. A foreign label
    // must not lose its assignments when this endpoint returns 404.
    if tx
        .query_opt(
            "SELECT id FROM issue_label WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load label"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "label not found"));
    }
    for table in ["issue_to_label", "agent_to_label", "skill_to_label"] {
        tx.execute(&format!("DELETE FROM {table} WHERE label_id=$1"), &[&id])
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to remove label assignments",
                )
            })?;
    }
    let deleted = tx
        .query_opt(
            "DELETE FROM issue_label WHERE id=$1 AND workspace_id=$2 RETURNING id",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete label"))?;
    if deleted.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "label not found"));
    }
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit label deletion",
        )
    })?;
    drop(db);
    publish(
        &state,
        ws,
        user,
        CatalogChange::LabelDeleted { label_id: id },
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matches_frozen_go_label_requests_and_validation() {
        use super::super::account_catalog_write_profile::account_request;
        use base64::Engine;
        use serde_json::{Value, json};

        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/account_catalog_write_labels_oracle.json"
        ))
        .unwrap();
        for case in cases {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(case["body"].as_str().unwrap())
                .unwrap();
            let label = format!("{} {}", case["method"], case["label"]);
            let result = if case["method"] == "create" {
                account_request::<NewLabel>(&raw).map(|req| {
                    let decoded = json!({"resource_type":req.resource_type,"name":req.name,"description":req.description,"color":req.color});
                    let normalized = (|| {
                        let name = name(&req.name)?;
                        let color = color(&req.color)?;
                        let resource = resource_type(&req.resource_type)?;
                        Ok::<_, ApiError>(json!({"resource_type":resource,"name":name,"description":description(&req.description),"color":color}))
                    })();
                    (decoded, normalized)
                })
            } else {
                account_request::<LabelChanges>(&raw).map(|req| {
                    let decoded =
                        json!({"name":req.name,"description":req.description,"color":req.color});
                    let normalized = (|| {
                        let name = req.name.as_deref().map(name).transpose()?;
                        let description = req.description.as_deref().map(description);
                        let color = req.color.as_deref().map(color).transpose()?;
                        Ok::<_, ApiError>(
                            json!({"name":name,"description":description,"color":color}),
                        )
                    })();
                    (decoded, normalized)
                })
            };
            if case["decode_error"] == true {
                assert!(result.is_err(), "{label}");
            } else {
                let (decoded, normalized) = result.unwrap_or_else(|_| panic!("{label}"));
                assert_eq!(decoded, case["decoded"], "{label}");
                match normalized {
                    Ok(values) => assert_eq!(values, case["validated"], "{label}"),
                    Err(err) => assert_eq!(err.1, case["error"].as_str().unwrap(), "{label}"),
                }
            }
        }
    }

    #[test]
    fn validates_catalog_input() {
        assert_eq!(color(" AA11ff ").unwrap(), "#aa11ff");
        assert!(color("url(x)").is_err());
        assert!(name("hi\nworld").is_err());
        assert!(name(&"a".repeat(33)).is_err());
        assert_eq!(resource_type(" ").unwrap(), "issue");
    }
}
