//! Write-side avatar admission. Storage reads and signed response URLs stay shared-owned.
use super::ApiState;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio_postgres::GenericClient;
use uuid::Uuid;

const DENIED: &str = "avatar_url must reference a standalone image upload, not a file attached to an issue, comment, or chat";

/// The shared storage backend's existing URL mapping, without object reads or writes.
/// `key_from_url` may be lossy; admission always verifies the `object_url` round trip.
pub trait AvatarStorageUrls: Send + Sync {
    fn key_from_url(&self, url: &str) -> String;
    fn object_url(&self, key: &str) -> String;
}

pub(super) async fn accept<C: GenericClient + Sync>(
    state: &ApiState,
    db: &C,
    raw: &str,
    current: &str,
) -> Result<String, &'static str> {
    let value = raw.trim();
    let Some(storage) = &state.avatar_storage_urls else {
        // Until shared storage/read policy is wired, preserve the checkpoint's
        // fail-closed behavior rather than minting public workspace references.
        return if value != current.trim()
            && (value.starts_with("workspaces/")
                || value.contains("/workspaces/")
                || value.contains("/api/avatars/"))
        {
            Err(DENIED)
        } else {
            Ok(value.to_owned())
        };
    };
    let value = served_key(value, &state.jwt_secret)
        .map(|key| storage.object_url(&key))
        .unwrap_or_else(|| value.to_owned());
    // Go permits unchanged re-sends even after an upload becomes bound.
    if value == current.trim() {
        return Ok(value);
    }
    let key = storage.key_from_url(&value);
    if key.is_empty() || key == value || storage.object_url(&key) != value {
        // Third-party URLs, data URIs and emoji are never signed by this policy.
        return Ok(value);
    }
    if !publishable(db, &key).await {
        return Err(DENIED);
    }
    Ok(value)
}

async fn publishable<C: GenericClient + Sync>(db: &C, key: &str) -> bool {
    let base = key.rsplit('/').next().unwrap_or("");
    let Some((stem, ext)) = base.rsplit_once('.') else {
        return false;
    };
    if !matches!(
        ext.to_ascii_lowercase().as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "bmp" | "ico"
    ) {
        return false;
    }
    if !key.starts_with("workspaces/") {
        return true;
    }
    // pgtype.UUID accepts only the compact or hyphenated shape, not urn/braces.
    if !matches!(stem.len(), 32 | 36) {
        return false;
    }
    let Ok(id) = Uuid::parse_str(stem) else {
        return false;
    };
    db.query_opt(
        "SELECT content_type, issue_id IS NOT NULL OR comment_id IS NOT NULL OR chat_session_id IS NOT NULL OR chat_message_id IS NOT NULL OR task_id IS NOT NULL AS bound FROM attachment WHERE id=$1",
        &[&id],
    )
    .await
    .ok()
    .flatten()
    .is_some_and(|row| {
        row.get::<_, String>("content_type")
            .to_lowercase()
            .starts_with("image/")
            && !row.get::<_, bool>("bound")
    })
}

fn served_key(raw: &str, secret: &[u8]) -> Option<String> {
    let path = if raw.starts_with('/') {
        raw.to_owned()
    } else {
        let url: axum::http::Uri = raw.parse().ok()?;
        percent_encoding::percent_decode_str(url.path())
            .decode_utf8()
            .ok()?
            .into_owned()
    };
    let (signature, key) = path.strip_prefix("/api/avatars/")?.split_once('/')?;
    if key.is_empty() {
        return None;
    }
    let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
    let signing_key = Sha256::digest([b"avatar-url:".as_slice(), secret].concat());
    let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key).ok()?;
    mac.update(key.as_bytes());
    mac.verify_slice(&signature).ok()?;
    Some(key.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_round_trip_uses_avatar_domain_and_rejects_tampering() {
        let secret = b"synthetic-avatar-secret";
        let key = "workspaces/w/00000000-0000-0000-0000-000000000001.png";
        let signing_key = Sha256::digest([b"avatar-url:".as_slice(), secret].concat());
        let mut mac = Hmac::<Sha256>::new_from_slice(&signing_key).unwrap();
        mac.update(key.as_bytes());
        let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        assert_eq!(sig, "V7kqLwohzj7BpAZBfvOyrytfE25iv3udWKBCCkWxQGM");
        let path = format!("/api/avatars/{sig}/{key}");
        assert_eq!(served_key(&path, secret).as_deref(), Some(key));
        assert_eq!(
            served_key(&format!("https://api.invalid{path}"), secret).as_deref(),
            Some(key)
        );
        assert_eq!(
            served_key(
                &format!("https://api.invalid{}", path.replace(".png", "%2epng")),
                secret
            )
            .as_deref(),
            Some(key)
        );
        assert!(served_key(&path.replace(".png", ".jpg"), secret).is_none());
        assert!(served_key(&path, b"rotated-secret").is_none());
        assert!(served_key("/api/avatars/bad/key.png", secret).is_none());
        assert!(served_key(&format!("/api/avatars/{sig}/"), secret).is_none());
        assert!(served_key(&format!("/api/avatars/{sig}=/{key}"), secret).is_none());
    }
}
