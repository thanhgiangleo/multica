//! Workspace-isolated, per-user pins and preferences from frozen Go reads.
use super::{
    ApiError, ApiState, GoJson, Identity,
    catalog::{WorkspaceId, parse_params},
    error, timestamp,
};
use axum::{
    extract::{Extension, RawQuery, State},
    http::StatusCode,
};
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Serialize)]
pub(super) struct Pin {
    id: Uuid,
    workspace_id: Uuid,
    user_id: Uuid,
    item_type: String,
    item_id: Uuid,
    position: f64,
    created_at: String,
}

pub(super) async fn pins(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Vec<Pin>>, ApiError> {
    let include_views = parse_params(raw.as_deref())
        .get("include")
        .is_some_and(|value| value.contains("view"));
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list pins");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db.query("SELECT * FROM pinned_item WHERE workspace_id=$1 AND user_id=$2 ORDER BY position ASC, created_at ASC", &[&ws,&user]).await.map_err(|_| fail())?;
    Ok(GoJson(
        rows.iter()
            .filter_map(|r| {
                let item_type: String = r.get("item_type");
                (include_views || item_type != "view").then(|| Pin {
                    id: r.get("id"),
                    workspace_id: r.get("workspace_id"),
                    user_id: r.get("user_id"),
                    item_type,
                    item_id: r.get("item_id"),
                    position: r.get("position"),
                    created_at: timestamp(r.get("created_at")),
                })
            })
            .collect(),
    ))
}

pub(super) async fn notifications(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get notification preferences",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let row = db
        .query_opt(
            "SELECT preferences FROM notification_preference WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?;
    // json.Unmarshal into map[string]string accepts null as an empty string
    // value and a top-level null as a nil map. A bad value discards the map.
    let prefs = row.map_or(json!({}), |r| {
        let raw: Value = r.get("preferences");
        match raw {
            Value::Null => Value::Null,
            Value::Object(map) if map.values().all(|v| v.is_string() || v.is_null()) => {
                Value::Object(
                    map.into_iter()
                        .map(|(k, v)| (k, json!(v.as_str().unwrap_or_default())))
                        .collect(),
                )
            }
            _ => json!({}),
        }
    });
    Ok(GoJson(json!({"workspace_id":ws,"preferences":prefs})))
}

pub(super) async fn issue_view_preference(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(raw.as_deref());
    let scope_type = params
        .get("scope_type")
        .map(String::as_str)
        .unwrap_or_default();
    let scope_id = match scope_type {
        "workspace" => ws,
        "my" => user,
        "project" => {
            let raw = params
                .get("scope_id")
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "scope_id is required for project scope",
                    )
                })?;
            let id = super::account_catalog_write::view_preference::parse_go_uuid(raw)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid scope_id"))?;
            let fail = || error(StatusCode::NOT_FOUND, "project not found");
            let db = state.pool.get().await.map_err(|_| fail())?;
            if db
                .query_opt(
                    "SELECT 1 FROM project WHERE id=$1 AND workspace_id=$2",
                    &[&id, &ws],
                )
                .await
                .map_err(|_| fail())?
                .is_none()
            {
                return Err(fail());
            }
            id
        }
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid scope_type")),
    };
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load preference",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let row = db.query_opt("SELECT scope_type,scope_id,prefs,updated_at FROM issue_view_preference WHERE workspace_id=$1 AND user_id=$2 AND scope_type=$3 AND scope_id=$4", &[&ws,&user,&scope_type,&scope_id]).await.map_err(|_| fail())?;
    Ok(GoJson(match row {
        Some(r) => {
            json!({"scope_type":r.get::<_, String>("scope_type"),"scope_id":r.get::<_, Uuid>("scope_id"),"prefs":r.get::<_, Value>("prefs"),"updated_at":timestamp(r.get("updated_at"))})
        }
        None => json!({"scope_type":scope_type,"scope_id":scope_id,"prefs":{},"updated_at":""}),
    }))
}
