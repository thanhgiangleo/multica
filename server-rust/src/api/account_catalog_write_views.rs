//! Saved issue-view catalog mutations, scoped to the authenticated workspace.
use super::{
    ApiError, ApiState, GoJson, Identity, account_catalog_write::view_preference,
    catalog::WorkspaceId, error, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, Request, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::value::RawValue;

#[path = "account_catalog_write_views_decode.rs"]
mod decode;
use decode::{Create, Update};
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issue-views", post(create))
        .route("/api/issue-views/", post(create))
        .route(
            "/api/issue-views/{id}/",
            axum::routing::patch(update).delete(remove),
        )
}

#[derive(Serialize)]
struct View {
    id: Uuid,
    workspace_id: Uuid,
    owner_id: Uuid,
    name: String,
    scope_type: String,
    scope_id: Option<Uuid>,
    scope_variant: Option<String>,
    visibility: String,
    definition_version: i32,
    query: Box<RawValue>,
    display: Box<RawValue>,
    revision: i32,
    created_at: String,
    updated_at: String,
}

impl From<&Row> for View {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            owner_id: row.get("owner_id"),
            name: row.get("name"),
            scope_type: row.get("scope_type"),
            scope_id: row.get("scope_id"),
            scope_variant: row.get("scope_variant"),
            visibility: row.get("visibility"),
            definition_version: row.get("definition_version"),
            query: RawValue::from_string(row.get("query_json")).expect("valid database JSONB"),
            display: RawValue::from_string(row.get("display_json")).expect("valid database JSONB"),
            revision: row.get("revision"),
            created_at: timestamp(row.get::<_, DateTime<Utc>>("created_at")),
            updated_at: timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
        }
    }
}

fn variant(scope: &str, value: Option<&str>) -> Result<Option<String>, ApiError> {
    match scope {
        "my" if matches!(value, Some("assigned" | "created" | "involved" | "any")) => {
            Ok(value.map(str::to_owned))
        }
        "workspace" | "project" if matches!(value, None | Some("" | "all")) => Ok(None),
        "workspace" | "project" if matches!(value, Some("members" | "agents")) => {
            Ok(value.map(str::to_owned))
        }
        _ => Err(error(
            StatusCode::BAD_REQUEST,
            "invalid scope_variant for this scope_type",
        )),
    }
}

fn name_valid(name: &str) -> bool {
    (1..=80).contains(&name.chars().count())
}

fn document(raw: &[u8], message: &'static str) -> Result<Vec<u8>, ApiError> {
    if raw.first() != Some(&b'{') {
        return Err(error(StatusCode::BAD_REQUEST, message));
    }
    Ok(raw.to_vec())
}

fn storage_document(raw: Vec<u8>, message: &'static str) -> Result<String, ApiError> {
    // Keep RawMessage bytes opaque: PostgreSQL, not a floating-point JSON value,
    // decides whether numeric ranges and Unicode escapes can be stored as JSONB.
    String::from_utf8(raw).map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, message))
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    request: Request,
) -> Result<(StatusCode, GoJson<View>), ApiError> {
    let bytes = view_preference::read_prefix(request.into_body())
        .await
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req = Create::decode(&bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if !name_valid(&req.name) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name must be between 1 and 80 characters",
        ));
    }
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to check view quota",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    // Serialize quota checks and insertion for one owner in this workspace.
    // A count outside this transaction lets concurrent creates exceed the cap.
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtextextended('issue-view-quota:' || $1::uuid::text || ':' || $2::uuid::text, 0))",
        &[&ws, &user],
    )
    .await
    .map_err(|_| fail())?;
    let count: i64 = tx
        .query_one(
            "SELECT COUNT(*) FROM issue_view WHERE workspace_id=$1 AND owner_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    if count >= 100 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "view limit reached for this workspace",
        ));
    }
    if !matches!(req.scope_type.as_str(), "workspace" | "my" | "project") {
        return Err(error(StatusCode::BAD_REQUEST, "invalid scope_type"));
    }
    let mut visibility = if req.visibility.is_empty() {
        "private"
    } else {
        &req.visibility
    };
    if !matches!(visibility, "private" | "workspace") {
        return Err(error(StatusCode::BAD_REQUEST, "invalid visibility"));
    }
    let query = document(
        req.query.as_deref().unwrap_or_default(),
        "query must be a JSON object",
    )?;
    let display = document(
        req.display.as_deref().unwrap_or(b"{}"),
        "display must be a JSON object",
    )?;
    let scope_variant = variant(&req.scope_type, req.scope_variant.as_deref())?;
    let scope_id = match req.scope_type.as_str() {
        "project" => {
            let raw = req.scope_id.as_deref().ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "scope_id is required for project views",
                )
            })?;
            let id = view_preference::parse_go_uuid(raw)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid scope_id"))?;
            if tx
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
            Some(id)
        }
        "my" => {
            visibility = "private";
            None
        }
        _ => None,
    };
    let version = req.definition_version.max(1);
    let query = storage_document(query, "failed to create view")?;
    let display = storage_document(display, "failed to create view")?;
    let row = tx.query_one(
        "INSERT INTO issue_view (workspace_id,owner_id,name,scope_type,scope_id,scope_variant,visibility,definition_version,query,display)          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::text::jsonb,$10::text::jsonb) RETURNING *,query::text AS query_json,display::text AS display_json",
        &[&ws,&user,&req.name,&req.scope_type,&scope_id,&scope_variant,&visibility,&version,&query,&display],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create view"))?;
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create view"))?;
    Ok((StatusCode::CREATED, GoJson(View::from(&row))))
}

async fn load(
    db: &tokio_postgres::Client,
    ws: Uuid,
    user: Uuid,
    raw: &str,
) -> Result<Row, ApiError> {
    let id = view_preference::parse_go_uuid(raw)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid view id"))?;
    let not_found = || error(StatusCode::NOT_FOUND, "view not found");
    let row = db
        .query_opt(
            "SELECT *,query::text AS query_json,display::text AS display_json FROM issue_view WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| not_found())?
        .ok_or_else(not_found)?;
    if row.get::<_, Uuid>("owner_id") != user && row.get::<_, String>("visibility") != "workspace" {
        return Err(not_found());
    }
    Ok(row)
}

async fn can_manage(
    db: &tokio_postgres::Client,
    row: &Row,
    ws: Uuid,
    user: Uuid,
) -> Result<(), ApiError> {
    if row.get::<_, Uuid>("owner_id") == user {
        return Ok(());
    }
    let role = db
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::FORBIDDEN, "insufficient permissions"))?
        .map(|row| row.get::<_, String>(0));
    if matches!(role.as_deref(), Some("owner" | "admin")) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "insufficient permissions"))
    }
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
    request: Request,
) -> Result<GoJson<View>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "view not found"))?;
    let row = load(&db, ws, user, &raw).await?;
    can_manage(&db, &row, ws, user).await?;
    let bytes = view_preference::read_prefix(request.into_body())
        .await
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req = Update::decode(&bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if req.expected_revision <= 0 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "expected_revision is required",
        ));
    }
    let name = req.name.as_deref().unwrap_or_else(|| row.get("name"));
    if !name_valid(name) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name must be between 1 and 80 characters",
        ));
    }
    let visibility = req
        .visibility
        .as_deref()
        .unwrap_or_else(|| row.get("visibility"));
    if !matches!(visibility, "private" | "workspace") {
        return Err(error(StatusCode::BAD_REQUEST, "invalid visibility"));
    }
    let scope: String = row.get("scope_type");
    if scope == "my" && visibility != "private" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "my views are always private",
        ));
    }
    let query = match req.query {
        Some(raw) => document(&raw, "query must be a JSON object")?,
        None => row.get::<_, String>("query_json").into_bytes(),
    };
    let display = match req.display {
        Some(raw) => document(&raw, "display must be a JSON object")?,
        None => row.get::<_, String>("display_json").into_bytes(),
    };
    let scope_variant = match req.scope_variant {
        Some(next) => variant(&scope, Some(&next))?,
        None => row.get("scope_variant"),
    };
    let id: Uuid = row.get("id");
    let query = storage_document(query, "failed to update view")?;
    let display = storage_document(display, "failed to update view")?;
    let updated = db.query_opt(
        "UPDATE issue_view SET name=$3, visibility=$4, scope_variant=$5, query=$6::text::jsonb, display=$7::text::jsonb, revision=revision+1, updated_at=now()          WHERE id=$1 AND workspace_id=$2 AND revision=$8 RETURNING *,query::text AS query_json,display::text AS display_json",
        &[&id,&ws,&name,&visibility,&scope_variant,&query,&display,&req.expected_revision],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update view"))?
        .ok_or_else(|| error(StatusCode::CONFLICT, "view was modified by someone else"))?;
    Ok(GoJson(View::from(&updated)))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "view not found"))?;
    let row = load(&db, ws, user, &raw).await?;
    can_manage(&db, &row, ws, user).await?;
    let id: Uuid = row.get("id");
    db.query_one(
        "WITH deleted AS (DELETE FROM issue_view WHERE id=$1 AND workspace_id=$2 RETURNING id),          swept_pins AS (DELETE FROM pinned_item WHERE item_type='view' AND workspace_id=$2          AND item_id IN (SELECT id FROM deleted)) SELECT id FROM deleted",
        &[&id,&ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete view"))?;
    Ok(StatusCode::NO_CONTENT)
}
