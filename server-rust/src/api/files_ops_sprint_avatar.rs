//! Avatar capabilities for standalone workspace image uploads. Read checks are
//! repeated on every request so binding or deleting the row revokes the URL.
use super::{ApiError, ApiState, ObjectStore, error, security_headers};
use axum::{
    Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const PREFIX: &str = "/api/avatars/";
// Bounded in-memory proxy until streaming storage readers are integrated.
const MAX_AVATAR: i64 = 8 << 20;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/avatars/{sig}/{*key}", get(serve_avatar))
        .route("/uploads/{*key}", get(serve_local_upload))
}

fn content_type(key: &str) -> Option<&'static str> {
    match key.rsplit_once('.')?.1.to_ascii_lowercase().as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "avif" => Some("image/avif"),
        "bmp" => Some("image/bmp"),
        "ico" => Some("image/x-icon"),
        _ => None,
    }
}

fn workspace_key(key: &str) -> Option<(Uuid, Uuid)> {
    let parts: Vec<_> = key.split('/').collect();
    if parts.len() != 3 || parts[0] != "workspaces" {
        return None;
    }
    let ws = Uuid::parse_str(parts[1]).ok()?;
    let name = parts[2];
    let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
    let id = Uuid::parse_str(stem).ok()?;
    // Only canonical, server-minted keys. Never normalize an untrusted path.
    if ws.to_string() != parts[1] || id.to_string() != stem {
        return None;
    }
    if name
        .bytes()
        .any(|b| !b.is_ascii_alphanumeric() && b != b'-' && b != b'.')
    {
        return None;
    }
    Some((ws, id))
}

fn user_key(key: &str) -> Option<(Uuid, Uuid)> {
    let parts: Vec<_> = key.split('/').collect();
    if parts.len() != 3 || parts[0] != "users" {
        return None;
    }
    let user = Uuid::parse_str(parts[1]).ok()?;
    let (stem, _) = parts[2].rsplit_once('.')?;
    let id = Uuid::parse_str(stem).ok()?;
    if user.to_string() != parts[1] || id.to_string() != stem || content_type(key).is_none() {
        return None;
    }
    Some((user, id))
}

fn mac(key: &str, secret: &[u8]) -> Hmac<Sha256> {
    let mut digest = Sha256::new();
    digest.update(b"avatar-url:");
    digest.update(secret);
    let mut mac = Hmac::<Sha256>::new_from_slice(&digest.finalize()).expect("HMAC key");
    mac.update(key.as_bytes());
    mac
}

fn signature(key: &str, secret: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(mac(key, secret).finalize().into_bytes())
}

fn valid_signature(key: &str, sig: &str, secret: &[u8]) -> bool {
    URL_SAFE_NO_PAD
        .decode(sig)
        .is_ok_and(|decoded| mac(key, secret).verify_slice(&decoded).is_ok())
}

pub(super) fn avatar_key(raw: &str, secret: &[u8]) -> Option<String> {
    let path = if raw.starts_with('/') {
        if raw.contains(['?', '#']) {
            return None;
        }
        raw.to_owned()
    } else {
        let url = reqwest::Url::parse(raw).ok()?;
        if !matches!(url.scheme(), "https" | "http")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        url.path().to_owned()
    };
    let (sig, key) = path.strip_prefix(PREFIX)?.split_once('/')?;
    valid_signature(key, sig, secret).then(|| key.to_owned())
}

fn owned_key(store: &dyn ObjectStore, raw: &str) -> Option<String> {
    let key = store.key_from_url(raw).ok()?;
    (store.object_url(&key).ok()?.as_str() == raw).then_some(key)
}

async fn publishable_size(state: &ApiState, key: &str) -> Result<Option<i64>, ApiError> {
    let Some(kind) = content_type(key) else {
        return Ok(None);
    };
    let Some(store) = &state.files_storage else {
        return Ok(None);
    };
    let Ok(url) = store.object_url(key) else {
        return Ok(None);
    };
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    if let Some((user, id)) = user_key(key) {
        // A user object has no attachment row. The durable upload ledger is
        // proof the server minted this specific object for an existing user;
        // arbitrary operator keys never become public via avatar signing.
        let row = db.query_opt(
            "SELECT a.size_bytes,a.content_type,EXISTS(SELECT 1 FROM \"user\" u WHERE u.id=a.user_id) FROM user_asset_object a WHERE a.id=$1 AND a.user_id=$2 AND a.object_url=$3 AND a.ready AND NOT a.deleting",
            &[&id, &user, &url],
        ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
        let Some(row) = row else { return Ok(None) };
        let size: i64 = row.get(0);
        let ct: String = row.get(1);
        if !ct
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(kind)
            || !(1..=MAX_AVATAR).contains(&size)
        {
            return Ok(None);
        }
        // Deleted owners cannot authenticate, but another account or workspace
        // can still hold this avatar. Serve it only while that reference exists;
        // the orphan reconciler uses the same guard before reclaiming bytes.
        if !row.get::<_, bool>(2)
            && !super::user_assets::avatar_referenced(&db, &url, key, &state.jwt_secret)
                .await
                .map_err(|_| {
                    error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "avatar references unavailable",
                    )
                })?
        {
            return Ok(None);
        }
        return Ok(Some(size));
    }
    let Some((ws, id)) = workspace_key(key) else {
        return Ok(None);
    };
    let row = db.query_opt(
        "SELECT size_bytes,content_type FROM attachment WHERE id=$1 AND workspace_id=$2 AND url=$3 AND issue_id IS NULL AND comment_id IS NULL AND chat_session_id IS NULL AND chat_message_id IS NULL AND task_id IS NULL AND source_context_id IS NULL",
        &[&id, &ws, &url],
    ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    Ok(row.and_then(|row| {
        let size: i64 = row.get(0);
        let ct: String = row.get(1);
        (ct.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(kind)
            && (1..=MAX_AVATAR).contains(&size))
        .then_some(size)
    }))
}

// A signed capability is never persisted as an untrusted external URL. Keep
// normalization independent of DB access: transactional validation must work
// even with a single-connection pool while holding the caller's transaction.
fn canonical_avatar_value(state: &ApiState, raw: &str) -> Result<String, ApiError> {
    let raw = raw.trim();
    if raw.starts_with(PREFIX) && avatar_key(raw, &state.jwt_secret).is_none() {
        return Err(error(StatusCode::FORBIDDEN, "invalid avatar capability"));
    }
    if let Some(key) = avatar_key(raw, &state.jwt_secret) {
        let store = state
            .files_storage
            .as_ref()
            .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "file storage unavailable"))?;
        return store
            .object_url(&key)
            .map_err(|_| error(StatusCode::FORBIDDEN, "invalid avatar_url"));
    }
    Ok(raw.to_owned())
}

/// Integrator calls this for every avatar write before persisting the returned
/// durable reference, with the authenticated human and the target workspace (if any).
/// Unchanged values and third-party/emoji avatars pass through. A user asset
/// belongs only to its uploader; a workspace object requires membership and
/// cannot be rebound to another workspace, even with a valid signed URL.
pub async fn accept_avatar_url(
    state: &ApiState,
    raw: &str,
    current: &str,
    actor: Uuid,
    target_workspace: Option<Uuid>,
) -> Result<String, ApiError> {
    let value = canonical_avatar_value(state, raw)?;
    let Some(store) = &state.files_storage else {
        return Ok(value);
    };
    // An unchanged value is still checked: membership may have been removed
    // since the original write, or the caller may be copying another owner.
    let _ = current;
    if let Some(key) = owned_key(store.as_ref(), &value) {
        if publishable_size(state, &key).await?.is_none() {
            return Err(error(
                StatusCode::FORBIDDEN,
                "avatar_url must reference a standalone image upload, not a file attached to an issue, comment, or chat",
            ));
        }
        if let Some((owner, _)) = user_key(&key) {
            if owner != actor {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "avatar asset belongs to another user",
                ));
            }
        } else if let Some((workspace, _)) = workspace_key(&key) {
            if target_workspace.is_some_and(|target| target != workspace) {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "avatar asset belongs to another workspace",
                ));
            }
            let db = state
                .pool
                .get()
                .await
                .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
            if db
                .query_opt(
                    "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                    &[&workspace, &actor],
                )
                .await
                .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
                .is_none()
            {
                return Err(error(
                    StatusCode::FORBIDDEN,
                    "avatar asset belongs to another workspace",
                ));
            }
        }
    }
    Ok(value)
}

/// Use this from the SAME database transaction that persists an avatar value.
/// The ordinary preflight checker cannot prevent a concurrent retirement or
/// membership revocation between validation and commit. This rechecks the
/// immutable asset row under a lock held until the caller commits. The caller
/// must also serialize its target avatar row with competing avatar writes.
pub async fn accept_avatar_url_in_transaction(
    state: &ApiState,
    tx: &tokio_postgres::Transaction<'_>,
    raw: &str,
    current: &str,
    actor: Uuid,
    target_workspace: Option<Uuid>,
) -> Result<String, ApiError> {
    let value = canonical_avatar_value(state, raw)?;
    let _ = current;
    let Some(store) = &state.files_storage else {
        return Ok(value);
    };
    let Some(key) = owned_key(store.as_ref(), &value) else {
        return Ok(value);
    };
    if let Some((owner, id)) = user_key(&key) {
        if owner != actor {
            return Err(error(
                StatusCode::FORBIDDEN,
                "avatar asset belongs to another user",
            ));
        }
        let row = tx.query_opt(
            "SELECT a.size_bytes,a.content_type FROM user_asset_object a JOIN \"user\" u ON u.id=a.user_id WHERE a.id=$1 AND a.user_id=$2 AND a.object_url=$3 AND a.ready AND NOT a.deleting FOR SHARE OF a, u",
            &[&id, &owner, &value],
        ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "avatar asset unavailable"))?;
        let Some(row) = row else {
            return Err(error(StatusCode::FORBIDDEN, "avatar asset unavailable"));
        };
        let size: i64 = row.get(0);
        let mime: String = row.get(1);
        if !valid_avatar_metadata(&key, size, &mime) {
            return Err(error(StatusCode::FORBIDDEN, "avatar asset unavailable"));
        }
    } else if let Some((workspace, id)) = workspace_key(&key) {
        if target_workspace.is_some_and(|target| target != workspace) {
            return Err(error(
                StatusCode::FORBIDDEN,
                "avatar asset belongs to another workspace",
            ));
        }
        tx.query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR KEY SHARE",
            &[&workspace, &actor],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "avatar membership unavailable",
            )
        })?
        .ok_or_else(|| error(StatusCode::FORBIDDEN, "avatar membership unavailable"))?;
        let row = tx.query_opt(
            "SELECT size_bytes,content_type FROM attachment WHERE id=$1 AND workspace_id=$2 AND url=$3 AND issue_id IS NULL AND comment_id IS NULL AND chat_session_id IS NULL AND chat_message_id IS NULL AND task_id IS NULL AND source_context_id IS NULL FOR SHARE",
            &[&id, &workspace, &value],
        ).await.map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "avatar asset unavailable"))?;
        let Some(row) = row else {
            return Err(error(StatusCode::FORBIDDEN, "avatar asset unavailable"));
        };
        let size: i64 = row.get(0);
        let mime: String = row.get(1);
        if !valid_avatar_metadata(&key, size, &mime) {
            return Err(error(StatusCode::FORBIDDEN, "avatar asset unavailable"));
        }
    }
    Ok(value)
}

fn valid_avatar_metadata(key: &str, size: i64, mime: &str) -> bool {
    content_type(key).is_some_and(|kind| {
        mime.split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(kind)
            && (1..=MAX_AVATAR).contains(&size)
    })
}

/// Integrator uses this in avatar DTO projections, not in persisted columns.
/// A storage-backed value which cannot be proved public fails closed.
pub async fn resolve_avatar_url(state: &ApiState, raw: &str) -> Result<String, ApiError> {
    let Some(store) = &state.files_storage else {
        return Ok(raw.to_owned());
    };
    let key = avatar_key(raw, &state.jwt_secret).or_else(|| owned_key(store.as_ref(), raw));
    let Some(key) = key else {
        return Ok(raw.to_owned());
    };
    if publishable_size(state, &key).await?.is_none() {
        return Err(error(
            StatusCode::FORBIDDEN,
            "avatar object is not publishable",
        ));
    }
    Ok(format!(
        "{}{PREFIX}{}/{key}",
        state
            .files_public_url
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('/'),
        signature(&key, &state.jwt_secret)
    ))
}

async fn serve_avatar(
    State(state): State<ApiState>,
    Path((sig, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    if !valid_signature(&key, &sig, &state.jwt_secret) {
        return Err(error(StatusCode::NOT_FOUND, "avatar not found"));
    }
    let size = publishable_size(&state, &key)
        .await?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "avatar not found"))?;
    let store = state
        .files_storage
        .clone()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "avatar not found"))?;
    let kind = content_type(&key).unwrap();
    // Check the complete private object against the ledger before publishing an
    // avatar. A Range GET cannot otherwise prove no trailing or missing bytes.
    let bytes = tokio::task::spawn_blocking(move || {
        store.read_verified(&key, 0, size as usize, size as u64)
    })
    .await
    .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to read avatar object"))?
    .map_err(|_| error(StatusCode::NOT_FOUND, "avatar not found"))?;
    if bytes.len() != size as usize {
        return Err(error(StatusCode::BAD_GATEWAY, "incomplete avatar object"));
    }
    let mut response = Body::from(bytes).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(kind));
    headers.insert(header::CONTENT_LENGTH, (size as u64).into());
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("inline"),
    );
    security_headers(headers);
    // Stricter than Go's private 300s cache, so binding revokes the very next
    // request rather than leaving a cached public representation behind.
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    Ok(response)
}

async fn serve_local_upload(
    State(state): State<ApiState>,
    Path(key): Path<String>,
) -> Result<Response, ApiError> {
    let store = state
        .files_storage
        .as_ref()
        .filter(|store| store.is_local())
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "file not found"))?;
    let (_, id) =
        workspace_key(&key).ok_or_else(|| error(StatusCode::NOT_FOUND, "file not found"))?;
    store
        .object_url(&key)
        .map_err(|_| error(StatusCode::NOT_FOUND, "file not found"))?;
    // Never serve workspace bytes or metadata publicly. Authenticated route
    // performs membership/private-object checks, including for signed avatars.
    let location = format!(
        "{}/api/attachments/{id}/download",
        state
            .files_public_url
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('/')
    );
    let mut response = StatusCode::FOUND.into_response();
    response.headers_mut().insert(
        header::LOCATION,
        location
            .parse()
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "invalid public URL"))?,
    );
    security_headers(response.headers_mut());
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avatar_signatures_are_domain_scoped_and_strict() {
        // Independent SHA-256/HMAC/base64url vector, matching frozen Go.
        assert_eq!(
            signature(
                "workspaces/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222.png",
                b"synthetic"
            ),
            "CSrF9tMiic6oqJuVfwBxAnw5r0SPUnFZnvwmZo2_Lmg"
        );
        let ws = Uuid::new_v4();
        let id = Uuid::new_v4();
        let key = format!("workspaces/{ws}/{id}.png");
        let sig = signature(&key, b"synthetic");
        assert!(valid_signature(&key, &sig, b"synthetic"));
        assert!(!valid_signature(&key, &sig, b"rotated"));
        assert!(!valid_signature(&format!("{key}x"), &sig, b"synthetic"));
        assert!(!valid_signature(&key, "invalid", b"synthetic"));
        let relative = format!("{PREFIX}{sig}/{key}");
        assert_eq!(avatar_key(&relative, b"synthetic"), Some(key.clone()));
        assert_eq!(
            avatar_key(&format!("https://api.invalid{relative}"), b"synthetic"),
            Some(key.clone())
        );
        assert!(avatar_key(&relative, b"rotated").is_none());
        for noncanonical in [
            format!("{relative}?exp=0"),
            format!("{relative}#fragment"),
            format!("https://api.invalid{relative}?utm=1"),
            format!("https://api.invalid{relative}#fragment"),
            format!("https://user:password@api.invalid{relative}"),
            format!("file://api.invalid{relative}"),
        ] {
            assert!(avatar_key(&noncanonical, b"synthetic").is_none());
        }
        assert_eq!(workspace_key(&key), Some((ws, id)));
        for key in [
            format!("workspaces/{ws}/../{id}.png"),
            format!("workspaces/{ws}/{id}.png.meta.json"),
            format!("users/{ws}/{id}.png"),
            format!("workspaces/{ws}/lark/{id}.png"),
        ] {
            assert!(workspace_key(&key).is_none());
        }
        assert_eq!(user_key(&format!("users/{ws}/{id}.png")), Some((ws, id)));
        assert!(user_key(&format!("operators/{ws}/{id}.png")).is_none());
        assert!(user_key(&format!("users/{ws}/{id}.svg")).is_none());
        assert_eq!(content_type("file.SVG"), None);
        assert_eq!(content_type("file.JPG"), Some("image/jpeg"));
    }
}
