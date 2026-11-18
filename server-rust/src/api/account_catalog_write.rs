//! Account and catalog mutations against the frozen Go contract.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, post},
};
use chrono::{DateTime, Duration, Utc};
use getrandom::fill;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/tokens", post(create_pat))
        .route("/api/tokens/", post(create_pat))
        .route("/api/tokens/current/renew", post(renew_pat))
        .route("/api/tokens/{id}", delete(revoke_pat))
}

#[path = "account_catalog_write_pat.rs"]
mod pat;

use pat::NewPat;

#[path = "account_catalog_write_notification.rs"]
mod notification;
#[path = "account_catalog_write_view_preference.rs"]
pub(super) mod view_preference;

#[derive(Serialize)]
struct CreatedPat {
    id: Uuid,
    name: String,
    token_prefix: String,
    expires_at: Option<String>,
    last_used_at: Option<String>,
    created_at: String,
    token: String,
}

async fn non_shadow(db: &tokio_postgres::Client, user: Uuid) -> Result<(), ApiError> {
    match db
        .query_opt("SELECT zbs_is_shadow FROM \"user\" WHERE id = $1", &[&user])
        .await
    {
        Ok(Some(row)) if row.get::<_, bool>(0) => {
            Err(error(StatusCode::FORBIDDEN, "account disabled"))
        }
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(error(StatusCode::UNAUTHORIZED, "user not found")),
        Err(_) => Err(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to look up user",
        )),
    }
}

async fn create_pat(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Result<(StatusCode, GoJson<CreatedPat>), ApiError> {
    actor.require_human()?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to look up user"))?;
    non_shadow(&db, user).await?;
    let body = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req: NewPat = super::account_catalog_write_profile::account_request(&body)?;
    if req.name.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "name is required"));
    }
    let mut bytes = [0u8; 20];
    fill(&mut bytes).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate token",
        )
    })?;
    let token = format!("mul_{}", hex::encode(bytes));
    let hash = hex::encode(Sha256::digest(token.as_bytes()));
    let expires = pat::expiry(Utc::now(), req.expires_in_days);
    let row = db.query_one(
        "INSERT INTO personal_access_token (user_id, name, token_hash, token_prefix, expires_at) VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at, expires_at",
        &[&user, &req.name, &hash, &&token[..12], &expires],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create token"))?;
    Ok((
        StatusCode::CREATED,
        GoJson(CreatedPat {
            id: row.get("id"),
            name: req.name,
            token_prefix: token[..12].to_owned(),
            expires_at: row
                .get::<_, Option<DateTime<Utc>>>("expires_at")
                .map(timestamp),
            last_used_at: None,
            created_at: timestamp(row.get("created_at")),
            token,
        }),
    ))
}

#[derive(Serialize)]
struct RenewedPat {
    expires_at: String,
    renewed: bool,
}

async fn renew_pat(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
) -> Result<GoJson<RenewedPat>, ApiError> {
    actor.require_human()?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to look up user"))?;
    non_shadow(&db, user).await?;
    let raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| v.starts_with("mul_"))
        .ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "only personal access tokens can be renewed",
            )
        })?;
    let hash = hex::encode(Sha256::digest(raw.as_bytes()));
    let fetch = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to look up token");
    let row = db.query_opt("SELECT id, user_id, expires_at FROM personal_access_token WHERE token_hash=$1 AND NOT revoked AND (expires_at IS NULL OR expires_at > now())", &[&hash]).await.map_err(|_| fetch())?
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "token is no longer valid"))?;
    if row.get::<_, Uuid>("user_id") != user {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "token does not belong to caller",
        ));
    }
    let Some(expiry) = row.get::<_, Option<DateTime<Utc>>>("expires_at") else {
        return Ok(GoJson(RenewedPat {
            expires_at: String::new(),
            renewed: false,
        }));
    };
    let now = Utc::now();
    if expiry > now + Duration::days(7) {
        return Ok(GoJson(RenewedPat {
            expires_at: timestamp(expiry),
            renewed: false,
        }));
    }
    let id: Uuid = row.get("id");
    let updated = db.query_opt("UPDATE personal_access_token SET expires_at=$2 WHERE id=$1 AND NOT revoked AND expires_at IS NOT NULL AND expires_at > now() AND expires_at <= $3 RETURNING expires_at", &[&id, &(now + Duration::days(90)), &(now + Duration::days(7))]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to renew token"))?;
    if let Some(row) = updated {
        return Ok(GoJson(RenewedPat {
            expires_at: timestamp(row.get("expires_at")),
            renewed: true,
        }));
    }
    let current = db.query_opt("SELECT expires_at FROM personal_access_token WHERE token_hash=$1 AND NOT revoked AND (expires_at IS NULL OR expires_at > now())", &[&hash]).await
        .map_err(|_| error(StatusCode::UNAUTHORIZED, "token is no longer valid"))?
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "token is no longer valid"))?;
    Ok(GoJson(RenewedPat {
        expires_at: current
            .get::<_, Option<DateTime<Utc>>>("expires_at")
            .map(timestamp)
            .unwrap_or_default(),
        renewed: false,
    }))
}

async fn revoke_pat(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    actor.require_human()?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid token id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to revoke token"))?;
    db.execute(
        "UPDATE personal_access_token SET revoked=TRUE WHERE id=$1 AND user_id=$2",
        &[&id, &user],
    )
    .await
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to revoke token"))?;
    Ok(StatusCode::NO_CONTENT)
}

// The workspace guard in catalog.rs supplies WorkspaceId after validating the
// selector against membership. Keep this data in account scope per user.
fn preference_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, GoJson(json!({"error": message.into()}))).into_response()
}

async fn save_notification_preferences(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    bytes: Bytes,
    merge: bool,
) -> Result<GoJson<Value>, Response> {
    let prefs = notification::decode(&bytes)
        .map_err(|message| preference_error(StatusCode::BAD_REQUEST, message))?;
    let db = state.pool.get().await.map_err(|_| {
        preference_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update notification preferences",
        )
    })?;
    let sql = if merge {
        "INSERT INTO notification_preference (workspace_id,user_id,preferences) VALUES ($1,$2,$3) ON CONFLICT (workspace_id,user_id) DO UPDATE SET preferences=notification_preference.preferences || EXCLUDED.preferences,updated_at=now() RETURNING preferences"
    } else {
        "INSERT INTO notification_preference (workspace_id,user_id,preferences) VALUES ($1,$2,$3) ON CONFLICT (workspace_id,user_id) DO UPDATE SET preferences=EXCLUDED.preferences,updated_at=now() RETURNING preferences"
    };
    let row = db
        .query_one(sql, &[&ws, &user, &prefs])
        .await
        .map_err(|_| {
            preference_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to update notification preferences",
            )
        })?;
    Ok(GoJson(
        json!({"workspace_id": ws, "preferences": row.get::<_, Value>(0)}),
    ))
}

pub(super) async fn put_notification_preferences(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    bytes: Bytes,
) -> Result<GoJson<Value>, Response> {
    save_notification_preferences(state, ws, user, bytes, false).await
}

pub(super) async fn patch_notification_preferences(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    bytes: Bytes,
) -> Result<GoJson<Value>, Response> {
    save_notification_preferences(state, ws, user, bytes, true).await
}

#[derive(Serialize)]
struct IssueViewPreferenceResponse {
    scope_type: String,
    scope_id: Uuid,
    prefs: Box<serde_json::value::RawValue>,
    updated_at: String,
}

pub(super) async fn put_issue_view_preference(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    request: axum::extract::Request,
) -> Result<Response, ApiError> {
    let bytes = view_preference::read_prefix(request.into_body())
        .await
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let req = view_preference::decode(&bytes)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to save preference",
        )
    })?;
    let scope_id = match req.scope_type.as_str() {
        "workspace" => ws,
        "my" => user,
        "project" => {
            let raw = req.scope_id.filter(|s| !s.is_empty()).ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "scope_id is required for project scope",
                )
            })?;
            let id = view_preference::parse_go_uuid(&raw)
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid scope_id"))?;
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
            id
        }
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid scope_type")),
    };
    if !view_preference::prefs_is_object(&req.prefs) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "prefs must be a JSON object",
        ));
    }
    // Keep the client-owned document out of Value/f64: PostgreSQL JSONB accepts
    // numbers beyond f64 and applies its own Unicode/numeric validation.
    let prefs = String::from_utf8(req.prefs).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to save preference",
        )
    })?;
    let row = db.query_one("INSERT INTO issue_view_preference (workspace_id,user_id,scope_type,scope_id,prefs) VALUES ($1,$2,$3,$4,$5::text::jsonb) ON CONFLICT (workspace_id,user_id,scope_type,scope_id) DO UPDATE SET prefs=EXCLUDED.prefs,updated_at=now() RETURNING scope_type,scope_id,prefs::text AS prefs_json,updated_at",
        &[&ws,&user,&req.scope_type,&scope_id,&prefs]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to save preference"))?;
    Ok(GoJson(IssueViewPreferenceResponse {
        scope_type: row.get("scope_type"),
        scope_id: row.get("scope_id"),
        prefs: serde_json::value::RawValue::from_string(row.get("prefs_json")).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to save preference",
            )
        })?,
        updated_at: timestamp(row.get("updated_at")),
    })
    .into_response())
}
