//! Authenticated access to standalone user uploads, including non-image files.
//! Raw `/uploads/users` URLs never serve bytes to an anonymous caller.
use super::{ApiError, ApiState, Identity, error, owned_object_key, stream_object};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{delete, get},
};
use deadpool_postgres::GenericClient;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/user-assets/{id}/download", get(download))
        .route("/api/user-assets/{id}", delete(remove))
}

async fn download(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid user asset id"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    // No workspace fallback: other users, missing intents, unfinished writes and
    // deleted owners all look the same to callers. The row owns the object URL.
    let row = db
        .query_opt(
            "SELECT a.object_url,a.content_type,a.filename,a.size_bytes FROM user_asset_object a JOIN \"user\" u ON u.id=a.user_id WHERE a.id=$1 AND a.user_id=$2 AND a.ready AND NOT a.deleting",
            &[&id, &user],
        )
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "user asset ledger unavailable"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user asset not found"))?;
    let url: String = row.get(0);
    let content_type: String = row.get(1);
    let filename: String = row.get(2);
    let size_bytes: i64 = row.get(3);
    let store = state
        .files_storage
        .as_ref()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "file storage unavailable"))?;
    owned_object_key(store.as_ref(), &url, "users", user, id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user asset not found"))?;
    stream_object(
        &state,
        &url,
        &content_type,
        &filename,
        size_bytes,
        &headers,
        true,
    )
    .await
}

/// Explicitly retire an unreferenced user upload. Deletion is a durable intent:
/// failures in storage never resurrect a URL or silently drop cleanup work.
async fn remove(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = Uuid::parse_str(&raw)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid user asset id"))?;
    retire(&state, user, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Called after a committed avatar replacement, never before the new reference
/// is durable. A still-bound old asset is retained; third-party URLs are ignored.
/// The integrator must invoke this on profile/workspace/squad avatar changes,
/// serialize cross-owner avatar writes with retirement, and schedule
/// `reconcile_user_assets` to retry storage failures.
pub async fn retire_replaced_avatar(
    state: &ApiState,
    owner: Uuid,
    previous: &str,
    replacement: &str,
) -> Result<bool, ApiError> {
    if previous == replacement {
        return Ok(false);
    }
    let Some(store) = state.files_storage.as_ref() else {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "file storage unavailable",
        ));
    };
    let key = super::avatar::avatar_key(previous, &state.jwt_secret)
        .or_else(|| store.key_from_url(previous).ok());
    let Some(key) = key else { return Ok(false) };
    let Some(id) = key
        .strip_prefix(&format!("users/{owner}/"))
        .and_then(|name| name.split('.').next())
        .and_then(|raw| Uuid::parse_str(raw).ok())
    else {
        return Ok(false);
    };
    let Ok(url) = store.object_url(&key) else {
        return Ok(false);
    };
    if super::owned_object_key(store.as_ref(), &url, "users", owner, id).is_none() {
        return Ok(false);
    }
    match retire(state, owner, id).await {
        Ok(()) => Ok(true),
        Err(error) if error.0 == StatusCode::CONFLICT || error.0 == StatusCode::NOT_FOUND => {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

async fn retire(state: &ApiState, user: Uuid, id: Uuid) -> Result<(), ApiError> {
    let store = state
        .files_storage
        .as_ref()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "file storage unavailable"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    // Serialize against user profile avatar writes, then the upload finalizer.
    // A referenced avatar must first be replaced through the profile API.
    tx.query_opt("SELECT id FROM \"user\" WHERE id=$1 FOR UPDATE", &[&user])
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user asset not found"))?;
    let row = tx.query_opt(
        "SELECT object_url FROM user_asset_object WHERE id=$1 AND user_id=$2 AND ready AND NOT deleting FOR UPDATE",
        &[&id, &user],
    ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "user asset ledger unavailable"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "user asset not found"))?;
    let url: String = row.get(0);
    let key = super::owned_object_key(store.as_ref(), &url, "users", user, id)
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "invalid user asset object"))?;
    // Check every avatar holder, not only the uploader. Another user or a
    // workspace may be displaying a shared asset. Signed projections are also
    // recognized; a raw SQL equality check would silently miss those.
    if avatar_referenced(&tx, &url, &key, &state.jwt_secret)
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "avatar references unavailable",
            )
        })?
    {
        return Err(error(
            StatusCode::CONFLICT,
            "replace avatar before deleting asset",
        ));
    }
    tx.execute(
        "UPDATE user_asset_object SET deleting=true, cleanup_checked_at=NULL WHERE id=$1 AND user_id=$2",
        &[&id, &user],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "user asset ledger unavailable",
        )
    })?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "failed to retire user asset",
        )
    })?;
    // Reconciliation checks out its own connection, including with a one-slot
    // pool. Release the committed caller's slot before invoking it.
    drop(db);
    // A reconciler retries after crashes and transient storage errors.
    let _ = super::reconcile_user_assets(state, 1).await;
    Ok(())
}

/// A user asset may be displayed by a different owner or a workspace/squad.
/// The same guard applies to explicit retirement and orphan reclamation.
pub(super) async fn avatar_referenced(
    db: &impl GenericClient,
    url: &str,
    key: &str,
    secret: &[u8],
) -> Result<bool, tokio_postgres::Error> {
    let candidates = db
        .query(
            "SELECT avatar_url FROM \"user\" WHERE avatar_url=$1 OR avatar_url LIKE $2 UNION ALL SELECT avatar_url FROM workspace WHERE avatar_url=$1 OR avatar_url LIKE $2 UNION ALL SELECT avatar_url FROM squad WHERE avatar_url=$1 OR avatar_url LIKE $2",
            &[&url, &format!("%/{key}%")],
        )
        .await?;
    Ok(candidates.iter().any(|row| {
        let avatar: String = row.get(0);
        avatar == url || super::avatar::avatar_key(&avatar, secret).as_deref() == Some(key)
    }))
}
