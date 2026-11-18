//! Personal pin writes: scoped validation, durable ordering, and post-commit events.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error, timestamp,
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State, rejection::JsonRejection},
    http::StatusCode,
    routing::{post, put},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinEventKind {
    Created,
    Deleted,
    Reordered,
}

impl PinEventKind {
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Created => "pin:created",
            Self::Deleted => "pin:deleted",
            Self::Reordered => "pin:reordered",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PinEvent {
    pub kind: PinEventKind,
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub payload: Value,
}

pub trait PinEventSink: Send + Sync {
    fn publish(&self, event: PinEvent);
}

pub type SharedPinEventSink = Arc<dyn PinEventSink>;

#[derive(Serialize)]
struct Pin {
    id: Uuid,
    workspace_id: Uuid,
    user_id: Uuid,
    item_type: String,
    item_id: Uuid,
    position: f64,
    created_at: String,
}

fn pin(row: &Row) -> Pin {
    Pin {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        user_id: row.get("user_id"),
        item_type: row.get("item_type"),
        item_id: row.get("item_id"),
        position: row.get("position"),
        created_at: timestamp(row.get::<_, DateTime<Utc>>("created_at")),
    }
}

#[derive(Deserialize)]
struct CreatePin {
    #[serde(default)]
    item_type: String,
    #[serde(default)]
    item_id: String,
}

#[derive(Deserialize, Serialize)]
struct ReorderItem {
    id: String,
    position: f64,
}

#[derive(Deserialize, Default)]
struct ReorderPins {
    #[serde(default)]
    items: Vec<ReorderItem>,
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/pins", post(create))
        .route("/api/pins/", post(create))
        .route("/api/pins/reorder", put(reorder))
        .route(
            "/api/pins/{itemType}/{itemId}",
            axum::routing::delete(delete),
        )
}

fn require_sink<'a>(
    state: &'a ApiState,
    actor: &Actor,
) -> Result<&'a SharedPinEventSink, ApiError> {
    actor.require_human()?;
    state.pin_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "pin event delivery is unavailable",
        )
    })
}

fn parse_id(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, field))
}

fn bad_body(_: JsonRejection) -> ApiError {
    error(StatusCode::BAD_REQUEST, "invalid request body")
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<CreatePin>, JsonRejection>,
) -> Result<(StatusCode, GoJson<Pin>), ApiError> {
    let sink = require_sink(&state, &actor)?;
    let Json(body) = body.map_err(bad_body)?;
    if !matches!(body.item_type.as_str(), "issue" | "project" | "view") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "item_type must be 'issue', 'project' or 'view'",
        ));
    }
    if body.item_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "item_id is required"));
    }
    let id = parse_id(&body.item_id, "invalid item_id")?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create pin");
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    // Serializing a user's inserts prevents two simultaneous first pins from
    // receiving the same position; the scope and target are checked again here.
    tx.query_one(
        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR UPDATE",
        &[&ws, &user],
    )
    .await
    .map_err(|_| fail())?;
    let (table, missing) = match body.item_type.as_str() {
        "issue" => ("issue", "issue not found"),
        "project" => ("project", "project not found"),
        _ => ("issue_view", "view not found"),
    };
    let query = if table == "issue_view" {
        "SELECT 1 FROM issue_view WHERE id=$1 AND workspace_id=$2 AND (owner_id=$3 OR visibility='workspace') FOR KEY SHARE".to_owned()
    } else {
        format!("SELECT 1 FROM {table} WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE")
    };
    let target = if table == "issue_view" {
        tx.query_opt(&query, &[&id, &ws, &user]).await
    } else {
        tx.query_opt(&query, &[&id, &ws]).await
    };
    if target.map_err(|_| fail())?.is_none() {
        return Err(error(StatusCode::NOT_FOUND, missing));
    }
    let position: f64 = tx
        .query_one(
            "SELECT COALESCE(MAX(position),0)::float8+1 FROM pinned_item WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to get position"))?
        .get(0);
    let row = tx
        .query_one(
            "INSERT INTO pinned_item(workspace_id,user_id,item_type,item_id,position) VALUES ($1,$2,$3,$4,$5) RETURNING *",
            &[&ws, &user, &body.item_type, &id, &position],
        )
        .await
        .map_err(|err| {
            if err.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                error(StatusCode::CONFLICT, "item already pinned")
            } else {
                fail()
            }
        })?;
    let response = pin(&row);
    tx.commit().await.map_err(|_| fail())?;
    sink.publish(PinEvent {
        kind: PinEventKind::Created,
        workspace_id: ws,
        user_id: user,
        payload: json!({"pin": response}),
    });
    Ok((StatusCode::CREATED, GoJson(response)))
}

async fn delete(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((item_type, raw_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let sink = require_sink(&state, &actor)?;
    let id = parse_id(&raw_id, "invalid item id")?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete pin");
    let db = state.pool.get().await.map_err(|_| fail())?;
    db.execute(
        "DELETE FROM pinned_item WHERE workspace_id=$1 AND user_id=$2 AND item_type=$3 AND item_id=$4",
        &[&ws, &user, &item_type, &id],
    )
    .await
    .map_err(|_| fail())?;
    sink.publish(PinEvent {
        kind: PinEventKind::Deleted,
        workspace_id: ws,
        user_id: user,
        payload: json!({"item_type": item_type, "item_id": raw_id}),
    });
    Ok(StatusCode::NO_CONTENT)
}

async fn reorder(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Json<ReorderPins>, JsonRejection>,
) -> Result<StatusCode, ApiError> {
    let sink = require_sink(&state, &actor)?;
    let Json(body) = body.map_err(bad_body)?;
    let parsed: Vec<Uuid> = body
        .items
        .iter()
        .map(|item| parse_id(&item.id, "invalid items[].id"))
        .collect::<Result<_, _>>()?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to reorder pins");
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.query_one(
        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR UPDATE",
        &[&ws, &user],
    )
    .await
    .map_err(|_| fail())?;
    for (item, id) in body.items.iter().zip(parsed) {
        tx.execute(
            "UPDATE pinned_item SET position=$1 WHERE id=$2 AND workspace_id=$3 AND user_id=$4",
            &[&item.position, &id, &ws, &user],
        )
        .await
        .map_err(|_| fail())?;
    }
    tx.commit().await.map_err(|_| fail())?;
    sink.publish(PinEvent {
        kind: PinEventKind::Reordered,
        workspace_id: ws,
        user_id: user,
        payload: json!({"items": body.items}),
    });
    Ok(StatusCode::NO_CONTENT)
}
