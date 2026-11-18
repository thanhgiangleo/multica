//! Attachment read, private download, local-object cleanup and capability contracts.
//! Registration and storage/event wiring belong to the integrator.
#[path = "files_ops_sprint_avatar.rs"]
mod avatar;
#[path = "files_ops_sprint_feedback.rs"]
mod feedback;
#[path = "files_ops_sprint_health.rs"]
mod health;
#[cfg(unix)]
#[path = "files_ops_sprint_local.rs"]
mod local;
#[path = "files_ops_sprint_s3.rs"]
mod s3;
#[path = "files_ops_sprint_upload.rs"]
mod upload;
#[path = "files_ops_sprint_user_assets.rs"]
mod user_assets;
use super::{
    ApiError, ApiState, GoJson, Identity, WorkspaceSelectors, catalog::WorkspaceId, error,
    timestamp,
};
pub use avatar::{accept_avatar_url, accept_avatar_url_in_transaction, resolve_avatar_url};
use axum::{
    Router,
    body::Body,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
pub use feedback::{
    FeedbackEvent, FeedbackEventSink, SharedFeedbackEventSink,
    normalized_kind as feedback_metric_kind, normalized_platform as feedback_metric_platform,
};
pub use health::{RealtimeMetrics, SharedRealtimeMetrics};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::{Digest, Sha256};
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom, Write};
use std::{
    path::{Component, Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio_postgres::Row;
pub use user_assets::retire_replaced_avatar;
use uuid::Uuid;

const MAX_PREVIEW: usize = 2 << 20;
const MAX_OBJECT: i64 = 100 << 20;
const CAPABILITY_TTL: i64 = 60;
const CAPABILITY_DOMAIN: &[u8] = b"attachment-download-capability:";

/// Only trusted, configured object backends implement this interface. Keys are
/// server-generated or derived from an authenticated attachment row, never a URL.
pub trait ObjectStore: Send + Sync {
    fn is_local(&self) -> bool {
        false
    }
    fn object_url(&self, key: &str) -> Result<String, String>;
    fn key_from_url(&self, url: &str) -> Result<String, String>;
    fn read(&self, key: &str, start: u64, limit: usize) -> Result<Vec<u8>, String>;
    /// Downloads require the object to match the durable ledger, even for a
    /// partial range whose response bytes alone cannot reveal the total size.
    fn read_verified(
        &self,
        key: &str,
        start: u64,
        limit: usize,
        expected_size: u64,
    ) -> Result<Vec<u8>, String>;
    fn write(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
        filename: &str,
    ) -> Result<(), String>;
    fn remove(&self, key: &str) -> Result<(), String>;
}

pub type SharedObjectStore = Arc<dyn ObjectStore>;
pub use s3::S3Objects;

// Cleanup ledger URLs are not authority to delete arbitrary storage keys.
// Both the row identity and the canonical storage URL must match a minted key.
fn owned_object_key(
    storage: &dyn ObjectStore,
    url: &str,
    namespace: &str,
    owner: Uuid,
    id: Uuid,
) -> Option<String> {
    let key = storage.key_from_url(url).ok()?;
    let prefix = format!("{namespace}/{owner}/{id}");
    let suffix = key.strip_prefix(&prefix)?;
    if !(suffix.is_empty()
        || suffix.starts_with('.')
            && (1..=12).contains(&suffix[1..].len())
            && suffix[1..].bytes().all(|c| c.is_ascii_alphanumeric()))
    {
        return None;
    }
    (storage.object_url(&key).ok()?.as_str() == url).then_some(key)
}

/// Integrator must publish committed revisions on real workspace bus; do not
/// wire a no-op sink. Comment updates require the full comment projection.
#[derive(Clone, Debug, PartialEq)]
pub struct AttachmentEvent {
    pub workspace_id: Uuid,
    pub actor_id: Uuid,
    pub issue_id: Option<Uuid>,
    pub issue_revision: i64,
    pub comment_id: Option<Uuid>,
    pub comment_revision: i64,
    /// Full committed comment projection for `comment:updated`, never an empty
    /// placeholder which would erase attachments/reactions on connected clients.
    pub comment: Option<serde_json::Value>,
}
pub trait AttachmentEventSink: Send + Sync {
    fn publish_attachment_changed(&self, event: AttachmentEvent);
    fn request_wakeup_dispatch(&self, workspace_id: Uuid, issue_id: Uuid);
}
pub type SharedAttachmentEventSink = Arc<dyn AttachmentEventSink>;

#[derive(Clone)]
pub struct LocalObjects {
    root: PathBuf,
    #[cfg(unix)]
    root_fd: Arc<std::fs::File>,
    base_url: Option<String>,
}
impl LocalObjects {
    pub fn new(root: PathBuf, base_url: Option<String>) -> Result<Self, String> {
        #[cfg(unix)]
        let root_fd = local::open_or_create_root(&root)?;
        #[cfg(not(unix))]
        std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
        let root = root.canonicalize().map_err(|e| e.to_string())?;
        if !root.is_dir() {
            return Err("upload root must be a directory".into());
        }
        let base_url = base_url
            .filter(|v| !v.is_empty())
            .map(|v| {
                let v = v.trim_end_matches('/');
                let url = reqwest::Url::parse(v).map_err(|_| "invalid local upload base URL")?;
                if !matches!(url.scheme(), "https" | "http")
                    || url.host().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.query().is_some()
                    || url.fragment().is_some()
                    || url.as_str().trim_end_matches('/') != v
                {
                    return Err("invalid local upload base URL".to_owned());
                }
                Ok(v.to_owned())
            })
            .transpose()?;
        Ok(Self {
            root,
            #[cfg(unix)]
            root_fd: Arc::new(root_fd),
            base_url,
        })
    }
    fn path(&self, key: &str) -> Result<PathBuf, String> {
        if key.is_empty()
            || key.contains('\\')
            || key.contains('%')
            || key.contains('?')
            || key.contains('#')
            || key.ends_with(".meta.json")
            || key
                .split('/')
                .next_back()
                .is_some_and(|s| s.starts_with('.') && s.ends_with(".tmp"))
        {
            return Err("invalid object key".into());
        }
        let path = FsPath::new(key);
        if path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err("invalid object key".into());
        }
        // Reject symlinks in every existing component, including leaf and
        // parents: canonicalizing only the root does not protect later reads.
        let mut current = self.root.clone();
        for component in path.components() {
            current.push(component);
            if let Ok(meta) = std::fs::symlink_metadata(&current)
                && meta.file_type().is_symlink()
            {
                return Err("symlink in object key".into());
            }
        }
        Ok(current)
    }
}
#[cfg(not(unix))]
impl ObjectStore for LocalObjects {
    fn is_local(&self) -> bool {
        true
    }
    fn object_url(&self, key: &str) -> Result<String, String> {
        self.path(key)?;
        Ok(format!(
            "{}/uploads/{key}",
            self.base_url.as_deref().unwrap_or("")
        ))
    }
    fn key_from_url(&self, url: &str) -> Result<String, String> {
        let relative = if let Some(base) = &self.base_url {
            url.strip_prefix(&format!("{base}/uploads/"))
        } else {
            url.strip_prefix("/uploads/")
        }
        .ok_or("object URL is not in configured local storage".to_owned())?;
        self.path(relative)?;
        Ok(relative.to_owned())
    }
    fn read(&self, key: &str, start: u64, limit: usize) -> Result<Vec<u8>, String> {
        let mut file = std::fs::File::open(self.path(key)?).map_err(|e| e.to_string())?;
        if !file.metadata().map_err(|e| e.to_string())?.is_file() {
            return Err("not a regular object".into());
        }
        file.seek(SeekFrom::Start(start))
            .map_err(|e| e.to_string())?;
        let mut data = Vec::new();
        file.take(limit as u64)
            .read_to_end(&mut data)
            .map_err(|e| e.to_string())?;
        Ok(data)
    }
    fn read_verified(
        &self,
        key: &str,
        start: u64,
        limit: usize,
        expected_size: u64,
    ) -> Result<Vec<u8>, String> {
        let mut file = std::fs::File::open(self.path(key)?).map_err(|e| e.to_string())?;
        let metadata = file.metadata().map_err(|e| e.to_string())?;
        if !metadata.is_file() || metadata.len() != expected_size {
            return Err("object size differs from ledger".into());
        }
        file.seek(SeekFrom::Start(start))
            .map_err(|e| e.to_string())?;
        let mut data = Vec::new();
        file.take(limit as u64)
            .read_to_end(&mut data)
            .map_err(|e| e.to_string())?;
        Ok(data)
    }
    fn write(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
        filename: &str,
    ) -> Result<(), String> {
        let path = self.path(key)?;
        std::fs::create_dir_all(path.parent().ok_or("invalid key")?).map_err(|e| e.to_string())?;
        // Recheck after creating parents. Root is exclusively service-owned.
        self.path(key)?;
        if path.exists() {
            return Err("object already exists".into());
        }
        let temp = path.with_file_name(format!(
            ".{}.tmp",
            path.file_name().unwrap().to_string_lossy()
        ));
        let meta_path = PathBuf::from(format!("{}.meta.json", path.display()));
        let mut owns_temp = false;
        let mut owns_object = false;
        let mut owns_meta = false;
        let result = (|| {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(|e| e.to_string())?;
            owns_temp = true;
            file.write_all(data).map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            // Atomic no-clobber publish: rename would replace a concurrent
            // writer's immutable object. create_new also refuses sidecar links.
            let mut meta_file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&meta_path)
                .map_err(|e| e.to_string())?;
            owns_meta = true;
            let meta = serde_json::to_vec(
                &serde_json::json!({"filename":filename,"content_type":content_type}),
            )
            .map_err(|e| e.to_string())?;
            meta_file.write_all(&meta).map_err(|e| e.to_string())?;
            meta_file.sync_all().map_err(|e| e.to_string())?;
            std::fs::hard_link(&temp, &path).map_err(|e| e.to_string())?;
            owns_object = true;
            Ok(())
        })();
        if owns_temp {
            let _ = std::fs::remove_file(&temp);
        }
        if result.is_err() {
            if owns_object {
                let _ = std::fs::remove_file(&path);
            }
            if owns_meta {
                let _ = std::fs::remove_file(&meta_path);
            }
        }
        result
    }
    fn remove(&self, key: &str) -> Result<(), String> {
        let path = self.path(key)?;
        for path in [
            path.clone(),
            PathBuf::from(format!("{}.meta.json", path.display())),
            path.with_file_name(format!(
                ".{}.tmp",
                path.file_name().unwrap().to_string_lossy()
            )),
        ] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.to_string()),
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Attachment {
    id: Uuid,
    workspace_id: Uuid,
    issue_id: Option<Uuid>,
    comment_id: Option<Uuid>,
    chat_session_id: Option<Uuid>,
    chat_message_id: Option<Uuid>,
    source_context_id: Option<Uuid>,
    task_id: Option<Uuid>,
    uploader_type: String,
    uploader_id: Uuid,
    filename: String,
    url: String,
    content_type: String,
    size_bytes: i64,
    created_at: DateTime<Utc>,
}
impl From<Row> for Attachment {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            issue_id: row.get("issue_id"),
            comment_id: row.get("comment_id"),
            chat_session_id: row.get("chat_session_id"),
            chat_message_id: row.get("chat_message_id"),
            source_context_id: row.get("source_context_id"),
            task_id: row.get("task_id"),
            uploader_type: row.get("uploader_type"),
            uploader_id: row.get("uploader_id"),
            filename: row.get("filename"),
            url: row.get("url"),
            content_type: row.get("content_type"),
            size_bytes: row.get("size_bytes"),
            created_at: row.get("created_at"),
        }
    }
}
#[derive(Serialize)]
struct AttachmentResponse {
    id: Uuid,
    workspace_id: Uuid,
    issue_id: Option<Uuid>,
    comment_id: Option<Uuid>,
    chat_session_id: Option<Uuid>,
    chat_message_id: Option<Uuid>,
    uploader_type: String,
    uploader_id: Uuid,
    filename: String,
    url: String,
    download_url: String,
    markdown_url: String,
    content_type: String,
    size_bytes: i64,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachment_download_url: Option<String>,
}
fn metadata(att: &Attachment, signed: bool, state: &ApiState) -> AttachmentResponse {
    let durable = format!("/api/attachments/{}/download", att.id);
    let markdown_url = state
        .files_public_url
        .as_deref()
        .filter(|v| !v.is_empty())
        .map(|v| format!("{}{durable}", v.trim_end_matches('/')))
        .unwrap_or_else(|| durable.clone());
    let (download_url, attachment_download_url) = if signed && state.files_storage.is_some() {
        (
            capability_url(att.id, false, &state.jwt_secret, Utc::now().timestamp()),
            Some(capability_url(
                att.id,
                true,
                &state.jwt_secret,
                Utc::now().timestamp(),
            )),
        )
    } else {
        (durable, None)
    };
    AttachmentResponse {
        id: att.id,
        workspace_id: att.workspace_id,
        issue_id: att.issue_id,
        comment_id: att.comment_id,
        chat_session_id: att.chat_session_id,
        chat_message_id: att.chat_message_id,
        uploader_type: att.uploader_type.clone(),
        uploader_id: att.uploader_id,
        filename: att.filename.clone(),
        url: att.url.clone(),
        download_url,
        markdown_url,
        content_type: att.content_type.clone(),
        size_bytes: att.size_bytes,
        created_at: timestamp(att.created_at),
        attachment_download_url,
    }
}

fn capability_signature(id: Uuid, exp: i64, dl: bool, secret: &[u8]) -> Vec<u8> {
    let mut key = Sha256::new();
    key.update(CAPABILITY_DOMAIN);
    key.update(secret);
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.finalize()).expect("HMAC key");
    let message = if dl {
        format!("v1|{id}|{exp}|attachment")
    } else {
        format!("v1|{id}|{exp}")
    };
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}
fn capability_url(id: Uuid, dl: bool, secret: &[u8], now: i64) -> String {
    let exp = now + CAPABILITY_TTL;
    let mut url = format!(
        "/api/attachments/{id}/signed-download?exp={exp}&sig={}",
        hex::encode(capability_signature(id, exp, dl, secret))
    );
    if dl {
        url.push_str("&dl=1");
    }
    url
}
// A capability's query has one wire representation. Do not parse aliases such
// as percent-encoded keys, reordered fields or noncanonical timestamps: a
// proxy and this handler must agree on precisely the same authority string.
fn capability_download(id: Uuid, raw: &str, secret: &[u8], now: i64) -> Option<bool> {
    let (exp_raw, rest) = raw.strip_prefix("exp=")?.split_once("&sig=")?;
    let (sig_raw, dl) = match rest.strip_suffix("&dl=1") {
        Some(sig) => (sig, true),
        None => (rest, false),
    };
    let exp = exp_raw.parse::<i64>().ok()?;
    if exp.to_string() != exp_raw
        || exp < now
        || exp > now.saturating_add(CAPABILITY_TTL)
        || sig_raw.len() != 64
        || !sig_raw
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return None;
    }
    let sig = hex::decode(sig_raw).ok()?;
    let mut key = Sha256::new();
    key.update(CAPABILITY_DOMAIN);
    key.update(secret);
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.finalize()).ok()?;
    let message = if dl {
        format!("v1|{id}|{exp}|attachment")
    } else {
        format!("v1|{id}|{exp}")
    };
    mac.update(message.as_bytes());
    mac.verify_slice(&sig).ok()?;
    Some(dl)
}

fn private_metadata<T: Serialize>(value: T) -> Response {
    let mut response = GoJson(value).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    response
}

// Membership alone must never turn task/cloud identities into unrestricted
// readers or member-attributed deleters. Machine-specific file policy is pending.
async fn human_only(
    Extension(actor): Extension<super::actor::Actor>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<Response, ApiError> {
    if !matches!(actor, super::actor::Actor::Human) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "machine attachment policy unavailable",
        ));
    }
    Ok(next.run(request).await)
}

pub(super) fn workspace_routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/attachments/{id}",
            get(get_attachment).delete(delete_attachment),
        )
        .route("/api/attachments/{id}/content", get(preview))
        .route("/api/issues/{id}/attachments", get(list_attachments))
        .route_layer(axum::middleware::from_fn(human_only))
}
pub(super) fn authenticated_routes() -> Router<ApiState> {
    Router::new()
        .route("/api/attachments/{id}/download", get(download))
        .route(
            "/api/attachments/{id}/download-metadata",
            get(download_metadata),
        )
        .merge(upload::routes())
        .merge(user_assets::routes())
        .merge(feedback::routes())
        .route_layer(axum::middleware::from_fn(human_only))
}
pub(super) fn public_routes() -> Router<ApiState> {
    Router::new()
        .route(
            "/api/attachments/{id}/signed-download",
            get(signed_download),
        )
        .merge(avatar::routes())
        .merge(health::routes())
}

async fn load(state: &ApiState, id: Uuid, ws: Option<Uuid>) -> Result<Attachment, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let row = if let Some(ws) = ws {
        db.query_opt(
            "SELECT * FROM attachment WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
    } else {
        db.query_opt("SELECT * FROM attachment WHERE id=$1", &[&id])
            .await
    }
    .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?;
    let att = Attachment::from(row);
    // The channel/guest/private-agent policy is not interchangeable with
    // workspace membership. Until its runtime contract is wired, deny all
    // chat and captured-context rows instead of leaking them via raw IDs.
    if att.chat_session_id.is_some()
        || att.chat_message_id.is_some()
        || att.task_id.is_some()
        || att.source_context_id.is_some()
    {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    // A detached issue or tombstoned comment must revoke even an unexpired
    // capability. The attachment row may outlive its owner during cleanup.
    if att.comment_id.is_some() && att.issue_id.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    if let Some(issue) = att.issue_id
        && db
            .query_opt(
                "SELECT 1 FROM issue i WHERE i.id=$1 AND i.workspace_id=$2 AND ($3::uuid IS NULL OR EXISTS (SELECT 1 FROM comment c WHERE c.id=$3 AND c.issue_id=i.id AND c.workspace_id=i.workspace_id AND c.deleted_at IS NULL))",
                &[&issue, &att.workspace_id, &att.comment_id],
            )
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "attachment owner unavailable"))?
            .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    let storage = state
        .files_storage
        .as_ref()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "storage not configured"))?;
    owned_object_key(
        storage.as_ref(),
        &att.url,
        "workspaces",
        att.workspace_id,
        att.id,
    )
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment object not found"))?;
    Ok(att)
}
async fn load_for_member(state: &ApiState, id: Uuid, user: Uuid) -> Result<Attachment, ApiError> {
    let att = load(state, id, None).await?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&att.workspace_id, &user],
        )
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    Ok(att)
}
fn parse_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid attachment id"))
}
async fn get_attachment(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    let att = load(&state, parse_id(&raw)?, Some(ws)).await?;
    Ok(private_metadata(metadata(&att, true, &state)))
}
async fn download_metadata(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    let att = load_for_member(&state, parse_id(&raw)?, user).await?;
    Ok(private_metadata(metadata(&att, true, &state)))
}
async fn list_attachments(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let issue = super::issue_read::load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let rows = db.query("SELECT * FROM attachment WHERE issue_id=$1 AND workspace_id=$2 AND chat_session_id IS NULL AND chat_message_id IS NULL AND task_id IS NULL AND source_context_id IS NULL AND (comment_id IS NULL OR EXISTS (SELECT 1 FROM comment c WHERE c.id=attachment.comment_id AND c.issue_id=attachment.issue_id AND c.workspace_id=attachment.workspace_id AND c.deleted_at IS NULL)) ORDER BY created_at ASC", &[&id,&ws]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list attachments"))?;
    let storage = state
        .files_storage
        .as_ref()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "storage not configured"))?;
    Ok(private_metadata(
        rows.into_iter()
            .map(Attachment::from)
            .filter(|att| {
                owned_object_key(storage.as_ref(), &att.url, "workspaces", ws, att.id).is_some()
            })
            .map(|att| metadata(&att, false, &state))
            .collect::<Vec<_>>(),
    ))
}
async fn download(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let att = load_for_member(&state, parse_id(&raw)?, user).await?;
    stream_object(
        &state,
        &att.url,
        &att.content_type,
        &att.filename,
        att.size_bytes,
        &headers,
        false,
    )
    .await
}
async fn signed_download(
    State(state): State<ApiState>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let result: Result<Response, ApiError> = async {
        let id = parse_id(&raw)?;
        let query = query.unwrap_or_default();
        let dl = capability_download(id, &query, &state.jwt_secret, Utc::now().timestamp())
            .ok_or_else(|| error(StatusCode::FORBIDDEN, "invalid or expired download link"))?;
        let att = load(&state, id, None).await?;
        stream_object(
            &state,
            &att.url,
            &att.content_type,
            &att.filename,
            att.size_bytes,
            &headers,
            dl,
        )
        .await
    }
    .await;
    // Even failed capability requests must not let a browser or intermediary
    // cache the response or send the signed URL as a Referer.
    let mut response = result.into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    Ok(response)
}
async fn stream_object(
    state: &ApiState,
    url: &str,
    content_type: &str,
    filename: &str,
    size_bytes: i64,
    headers: &HeaderMap,
    force: bool,
) -> Result<Response, ApiError> {
    let storage = state
        .files_storage
        .clone()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "storage not configured"))?;
    let key = storage
        .key_from_url(url)
        .map_err(|_| error(StatusCode::NOT_FOUND, "attachment object not found"))?;
    if !(0..=MAX_OBJECT).contains(&size_bytes) {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "attachment exceeds proxy limit",
        ));
    }
    let size = size_bytes as u64;
    // Multiple Range headers are ambiguous. Treat them like an unsupported
    // multi-range request rather than silently serving the first one.
    let range = requested_range(headers, size);
    let (start, len, status) = match range {
        Some(Ok((start, len))) => (start, len, StatusCode::PARTIAL_CONTENT),
        Some(Err(())) => {
            let mut response = error(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "requested range not satisfiable",
            )
            .into_response();
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes */{size}").parse().unwrap(),
            );
            security_headers(response.headers_mut());
            return Ok(response);
        }
        None => (0, size, StatusCode::OK),
    };
    let body =
        tokio::task::spawn_blocking(move || storage.read_verified(&key, start, len as usize, size))
            .await
            .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to read attachment object"))?
            .map_err(|_| error(StatusCode::NOT_FOUND, "attachment object not found"))?;
    if body.len() != len as usize {
        return Err(error(
            StatusCode::BAD_GATEWAY,
            "incomplete attachment object",
        ));
    }
    let mut response = Body::from(body).into_response();
    *response.status_mut() = status;
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        content_type
            .parse()
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(header::CONTENT_LENGTH, len.into());
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if status == StatusCode::PARTIAL_CONTENT {
        h.insert(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{size}", start + len - 1)
                .parse()
                .unwrap(),
        );
    }
    let disposition = if force || !inline_content_type(content_type) {
        "attachment"
    } else {
        "inline"
    };
    h.insert(
        header::CONTENT_DISPOSITION,
        format!("{disposition}; filename=\"{}\"", safe_filename(filename))
            .parse()
            .unwrap(),
    );
    security_headers(h);
    Ok(response)
}
fn requested_range(headers: &HeaderMap, size: u64) -> Option<Result<(u64, u64), ()>> {
    // Multiple Range headers are ambiguous. Treat them like an unsupported
    // multi-range request rather than silently serving the first one.
    match headers
        .get_all(header::RANGE)
        .iter()
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => None,
        [raw] => parse_range(raw.to_str().unwrap_or(""), size),
        _ => Some(Err(())),
    }
}

fn parse_range(raw: &str, size: u64) -> Option<Result<(u64, u64), ()>> {
    let Some(spec) = raw.strip_prefix("bytes=") else {
        return Some(Err(()));
    };
    if spec.contains(',') {
        return Some(Err(()));
    }
    let Some((start, end)) = spec.split_once('-') else {
        return Some(Err(()));
    };
    if size == 0 {
        return Some(Err(()));
    }
    let (start, end) = if start.is_empty() {
        let Ok(suffix) = end.parse::<u64>() else {
            return Some(Err(()));
        };
        if suffix == 0 {
            return Some(Err(()));
        }
        (size.saturating_sub(suffix), size - 1)
    } else {
        let Ok(start) = start.parse::<u64>() else {
            return Some(Err(()));
        };
        let end = if end.is_empty() {
            size - 1
        } else {
            match end.parse::<u64>() {
                Ok(v) => v.min(size - 1),
                Err(_) => return Some(Err(())),
            }
        };
        (start, end)
    };
    if start >= size || end < start {
        return Some(Err(()));
    }
    Some(Ok((start, end - start + 1)))
}
fn inline_content_type(content_type: &str) -> bool {
    let kind = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    matches!(
        kind.as_str(),
        "image/png"
            | "image/jpeg"
            | "image/gif"
            | "image/webp"
            | "image/avif"
            | "image/bmp"
            | "image/x-icon"
            | "application/pdf"
    ) || kind.starts_with("video/")
        || kind.starts_with("audio/")
}
fn safe_filename(raw: &str) -> String {
    // HeaderValue accepts visible ASCII only. A Unicode or oversized upload
    // name must not panic a later authenticated download.
    let name: String = raw
        .chars()
        .take(255)
        .map(|c| {
            if !c.is_ascii_graphic() || matches!(c, '\\' | '/' | '"') {
                '_'
            } else {
                c
            }
        })
        .collect();
    if name.is_empty() {
        "file".to_owned()
    } else {
        name
    }
}
fn security_headers(headers: &mut HeaderMap) {
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("content-security-policy", HeaderValue::from_static("default-src 'none'; img-src 'self' data:; media-src 'self'; frame-ancestors 'self'; object-src 'none'; base-uri 'none'; form-action 'none'"));
}
fn previewable(content_type: &str, filename: &str) -> bool {
    // Match the frozen Go whitelist's input normalization: parameters and
    // casing do not change the media type, and basenames are path-independent.
    let kind = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let basename = filename
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    kind.starts_with("text/")
        || matches!(
            kind.as_str(),
            "application/json"
                | "application/x-ndjson"
                | "application/javascript"
                | "application/xml"
                | "application/x-yaml"
                | "application/yaml"
                | "application/toml"
                | "application/x-sh"
                | "application/x-httpd-php"
        )
        || basename.rsplit_once('.').is_some_and(|(_, ext)| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "md" | "markdown"
                    | "txt"
                    | "log"
                    | "csv"
                    | "tsv"
                    | "html"
                    | "htm"
                    | "json"
                    | "jsonl"
                    | "ndjson"
                    | "xml"
                    | "yml"
                    | "yaml"
                    | "toml"
                    | "ini"
                    | "conf"
                    | "sh"
                    | "bash"
                    | "zsh"
                    | "py"
                    | "rb"
                    | "go"
                    | "rs"
                    | "ts"
                    | "tsx"
                    | "js"
                    | "jsx"
                    | "mjs"
                    | "cjs"
                    | "css"
                    | "scss"
                    | "sass"
                    | "less"
                    | "sql"
                    | "java"
                    | "kt"
                    | "swift"
                    | "c"
                    | "cc"
                    | "cpp"
                    | "h"
                    | "hpp"
                    | "cs"
                    | "php"
                    | "lua"
                    | "vim"
            )
        })
        || matches!(
            basename.as_str(),
            "dockerfile" | "makefile" | ".env" | ".gitignore"
        )
}
async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    let att = load(&state, parse_id(&raw)?, Some(ws)).await?;
    if !previewable(&att.content_type, &att.filename) {
        return Err(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "preview not supported for this file type",
        ));
    }
    let storage = state
        .files_storage
        .clone()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "storage not configured"))?;
    let key = owned_object_key(
        storage.as_ref(),
        &att.url,
        "workspaces",
        att.workspace_id,
        att.id,
    )
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment object not found"))?;
    // Preview must not silently serve a truncated prefix of a larger object.
    // Check the ledger bound before IO, then require the actual object to match
    // it, including for S3 where a bounded Range alone hides trailing bytes.
    if !(0..=MAX_PREVIEW as i64).contains(&att.size_bytes) {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "file too large for inline preview",
        ));
    }
    let expected = att.size_bytes as usize;
    let bytes = tokio::task::spawn_blocking(move || {
        storage.read_verified(&key, 0, expected, expected as u64)
    })
    .await
    .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to read attachment body"))?
    .map_err(|_| error(StatusCode::BAD_GATEWAY, "failed to read attachment body"))?;
    if bytes.len() != expected {
        return Err(error(
            StatusCode::BAD_GATEWAY,
            "incomplete attachment preview",
        ));
    }
    let mut response = Body::from(bytes).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if let Ok(v) = att.content_type.parse() {
        response.headers_mut().insert("x-original-content-type", v);
    }
    security_headers(response.headers_mut());
    Ok(response)
}
async fn delete_attachment(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Path(raw): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = parse_id(&raw)?;
    let sink = state.files_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "attachment event bus unavailable",
        )
    })?;
    let storage = state
        .files_storage
        .as_ref()
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "storage not configured"))?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete attachment",
        )
    })?;
    tx.query_one("SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true),set_config('multica.source_task_id','',true)", &[&user.to_string()]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to set attachment actor"))?;
    // Owner-first lock avoids inversion against issue/comment teardown. Pending
    // chat and source-context objects remain denied until their full policy is wired.
    let owner = tx
        .query_opt(
            "SELECT issue_id, comment_id FROM attachment WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete attachment",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?;
    let issue_id: Option<Uuid> = owner.get(0);
    let comment_id: Option<Uuid> = owner.get(1);
    if comment_id.is_some() && issue_id.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    if let Some(issue) = issue_id {
        tx.query_opt(
            "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&issue, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete attachment",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?;
    }
    if let Some(comment) = comment_id {
        tx.query_opt(
            "SELECT id FROM comment WHERE id=$1 AND workspace_id=$2 AND issue_id=$3 AND deleted_at IS NULL FOR UPDATE",
            &[&comment, &ws, &issue_id.expect("validated comment parent")],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock comment"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?;
    }
    let row = tx
        .query_opt(
            "SELECT * FROM attachment WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to delete attachment",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?;
    let att = Attachment::from(row);
    if att.source_context_id.is_some()
        || att.chat_session_id.is_some()
        || att.chat_message_id.is_some()
        || att.task_id.is_some()
        || att.issue_id != issue_id
        || att.comment_id != comment_id
    {
        return Err(error(StatusCode::NOT_FOUND, "attachment not found"));
    }
    // Hold membership until commit: a concurrent revocation must either
    // finish first (deny the delete) or wait for this authorized mutation.
    let role = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "attachment not found"))?
        .get::<_, String>(0);
    if !(att.uploader_type == "member" && att.uploader_id == user
        || role == "admin"
        || role == "owner")
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "not authorized to delete this attachment",
        ));
    }
    owned_object_key(storage.as_ref(), &att.url, "workspaces", ws, id).ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "object cleanup unavailable",
        )
    })?;
    // Persist cleanup intent in the same transaction as the DB delete. A
    // missing migration fails closed; a crashed worker leaves a retryable row.
    tx.execute("INSERT INTO attachment_object_cleanup (attachment_id, workspace_id, object_url, ready) VALUES ($1,$2,$3,true) ON CONFLICT (attachment_id) DO UPDATE SET ready=true, cleanup_checked_at=NULL", &[&id,&ws,&att.url]).await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE,"attachment cleanup ledger unavailable"))?;
    let deleted = tx.query_one("WITH deleted AS (DELETE FROM attachment WHERE id=$1 AND workspace_id=$2 RETURNING issue_id,comment_id), bumped AS (UPDATE issue SET revision=revision+1 WHERE id IN (SELECT issue_id FROM deleted WHERE issue_id IS NOT NULL) RETURNING revision), bumped_comment AS (UPDATE comment SET revision=revision+1 WHERE id IN (SELECT comment_id FROM deleted WHERE comment_id IS NOT NULL) RETURNING revision) SELECT COALESCE((SELECT revision FROM bumped),0)::bigint AS issue_revision,COALESCE((SELECT revision FROM bumped_comment),0)::bigint AS comment_revision", &[&id,&ws]).await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to delete attachment"))?;
    let revision: i64 = deleted.get("issue_revision");
    let comment_revision: i64 = deleted.get("comment_revision");
    let comment_projection = if let Some(comment) = comment_id {
        let row = tx
            .query_one(
                "SELECT * FROM comment WHERE id=$1 AND workspace_id=$2",
                &[&comment, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load comment"))?;
        Some(super::issue_write_resolution::response_in_transaction(&tx, &row, ws).await?)
    } else {
        None
    };
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete attachment",
        )
    })?;
    sink.publish_attachment_changed(AttachmentEvent {
        workspace_id: ws,
        actor_id: user,
        issue_id,
        issue_revision: revision,
        comment_id,
        comment_revision,
        comment: comment_projection,
    });
    if let Some(issue) = issue_id {
        sink.request_wakeup_dispatch(ws, issue);
    }
    drop(db);
    // Immediate best effort plus a durable retry path. Scheduler wiring is an
    // explicit integrator hook, not an unowned background loop.
    let _ = reconcile_cleanup(&state, 1).await;
    Ok(StatusCode::NO_CONTENT)
}

pub const CLEANUP_MIGRATION: &str = include_str!("files_ops_sprint_cleanup.sql");
pub const USER_ASSETS_MIGRATION: &str = include_str!("files_ops_sprint_user_assets.sql");
pub const USER_ASSETS_RETENTION_MIGRATION: &str =
    include_str!("files_ops_sprint_user_assets_retention.sql");
pub const USER_ASSETS_INDEX_MIGRATION: &str =
    include_str!("files_ops_sprint_user_assets_index.sql");

/// Invoke from the real lifecycle scheduler. Row locks allow safe concurrent
/// reconcilers; storage removal is idempotent, and failures retain the intent.
/// Failed or poisoned intents cool down for five minutes before another pass;
/// a newly committed delete resets the timestamp for an immediate attempt.
pub async fn reconcile_cleanup(state: &ApiState, limit: i64) -> Result<u64, String> {
    let storage = state
        .files_storage
        .clone()
        .ok_or("storage not configured")?;
    let mut db = state.pool.get().await.map_err(|e| e.to_string())?;
    let tx = db.transaction().await.map_err(|e| e.to_string())?;
    let rows = tx.query("SELECT c.attachment_id, c.workspace_id, c.object_url FROM attachment_object_cleanup c WHERE (c.ready OR c.created_at < now()-interval '10 minutes') AND (c.cleanup_checked_at IS NULL OR c.cleanup_checked_at < clock_timestamp()-interval '5 minutes') AND NOT EXISTS (SELECT 1 FROM attachment a WHERE a.id=c.attachment_id) ORDER BY COALESCE(c.cleanup_checked_at,c.created_at), c.created_at, c.attachment_id LIMIT $1 FOR UPDATE OF c SKIP LOCKED", &[&limit.clamp(1,100)]).await.map_err(|e| e.to_string())?;
    let mut cleaned = 0;
    for row in rows {
        let id: Uuid = row.get(0);
        let workspace: Uuid = row.get(1);
        let url: String = row.get(2);
        let Some(key) = owned_object_key(storage.as_ref(), &url, "workspaces", workspace, id)
        else {
            // Rotate poisoned intents rather than starving all later cleanup.
            tx.execute("UPDATE attachment_object_cleanup SET cleanup_checked_at=clock_timestamp() WHERE attachment_id=$1", &[&id])
                .await.map_err(|e| e.to_string())?;
            continue;
        };
        let store = storage.clone();
        if tokio::task::spawn_blocking(move || store.remove(&key))
            .await
            .map_err(|e| e.to_string())?
            .is_ok()
        {
            tx.execute(
                "DELETE FROM attachment_object_cleanup WHERE attachment_id=$1",
                &[&id],
            )
            .await
            .map_err(|e| e.to_string())?;
            cleaned += 1;
        } else {
            tx.execute("UPDATE attachment_object_cleanup SET cleanup_checked_at=clock_timestamp() WHERE attachment_id=$1", &[&id])
                .await.map_err(|e| e.to_string())?;
        }
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(cleaned)
}

/// Reclaim stale unfinished uploads and assets whose owning user is gone.
/// Same locking contract as `reconcile_cleanup`;
/// a fresh intent (under 10 minutes) is always left to its live writer.
pub async fn reconcile_user_assets(state: &ApiState, limit: i64) -> Result<u64, String> {
    let storage = state
        .files_storage
        .clone()
        .ok_or("storage not configured")?;
    let mut db = state.pool.get().await.map_err(|e| e.to_string())?;
    let tx = db.transaction().await.map_err(|e| e.to_string())?;
    let rows = tx.query("SELECT a.id, a.user_id, a.object_url FROM user_asset_object a WHERE (a.deleting OR (NOT a.ready AND a.created_at < now()-interval '10 minutes') OR (a.ready AND NOT EXISTS (SELECT 1 FROM \"user\" u WHERE u.id=a.user_id))) AND (a.cleanup_checked_at IS NULL OR a.cleanup_checked_at < clock_timestamp()-interval '5 minutes') ORDER BY COALESCE(a.cleanup_checked_at,a.created_at), a.created_at, a.id LIMIT $1 FOR UPDATE OF a SKIP LOCKED", &[&limit.clamp(1,100)]).await.map_err(|e| e.to_string())?;
    let mut cleaned = 0;
    for row in rows {
        let id: Uuid = row.get(0);
        let user: Uuid = row.get(1);
        let url: String = row.get(2);
        let Some(key) = owned_object_key(storage.as_ref(), &url, "users", user, id) else {
            tx.execute(
                "UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp() WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(|e| e.to_string())?;
            continue;
        };
        // Orphaned owners can still have assets used by another user, workspace,
        // or squad. Retain the bytes and ledger until those references are gone.
        if user_assets::avatar_referenced(&tx, &url, &key, &state.jwt_secret)
            .await
            .map_err(|e| e.to_string())?
        {
            tx.execute(
                "UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp() WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(|e| e.to_string())?;
            continue;
        }
        let store = storage.clone();
        if tokio::task::spawn_blocking(move || store.remove(&key))
            .await
            .map_err(|e| e.to_string())?
            .is_err()
        {
            tx.execute(
                "UPDATE user_asset_object SET cleanup_checked_at=clock_timestamp() WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(|e| e.to_string())?;
            continue;
        }
        tx.execute("DELETE FROM user_asset_object WHERE id=$1", &[&id])
            .await
            .map_err(|e| e.to_string())?;
        cleaned += 1;
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(cleaned)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capability_is_scoped_and_expires() {
        let id = Uuid::now_v7();
        let other = Uuid::now_v7();
        let secret = b"test-secret";
        for dl in [false, true] {
            let url = capability_url(id, dl, secret, 100);
            let query = url.split_once('?').unwrap().1;
            assert_eq!(capability_download(id, query, secret, 100), Some(dl));
            assert_eq!(capability_download(id, query, secret, 160), Some(dl));
            assert!(capability_download(id, query, secret, 161).is_none());
            assert!(capability_download(other, query, secret, 100).is_none());
            assert!(capability_download(id, query, b"wrong", 100).is_none());
            assert!(
                capability_download(id, &query.replace("exp=160", "exp=360"), secret, 100)
                    .is_none()
            );
            let flipped = if dl {
                query.replace("&dl=1", "&dl=0")
            } else {
                format!("{query}&dl=1")
            };
            assert!(capability_download(id, &flipped, secret, 100).is_none());
        }
        assert!(capability_download(id, "", secret, 100).is_none());
        let query = capability_url(id, false, secret, 100);
        let query = query.split_once('?').unwrap().1;
        for tampered in [
            format!("{query}&dl=0"),
            format!("{query}&exp=160"),
            format!("{query}&sig=deadbeef"),
            format!("{query}&unknown=1"),
            format!("{query}&dl=1&dl=1"),
            query.replacen("exp=", "e%78p=", 1),
            query.replacen("exp=160", "exp=0160", 1),
            query.replacen("exp=160", "exp=+160", 1),
            query.replacen("exp=160", "exp=%31%36%30", 1),
            query.replace("&sig=", "&SIG="),
            query.replace("&sig=", "&sig=%"),
            query.replace("&sig=", "&sig=+"),
            query.replace("&sig=", "&sig=0&sig="),
            query.replace("&sig=", "&dl=1&sig="),
            format!("{query}&"),
        ] {
            assert!(
                capability_download(id, &tampered, secret, 100).is_none(),
                "{tampered}"
            );
        }
        // An uppercase signature must not be accepted as an alias of the lowercase
        // hex mint produced by `capability_url`.
        let upper_sig = query.to_ascii_uppercase();
        assert!(capability_download(id, &upper_sig, secret, 100).is_none());
        let forced = capability_url(id, true, secret, 100);
        let forced_query = forced.split_once('?').unwrap().1;
        assert_eq!(
            capability_download(id, forced_query, secret, 100),
            Some(true)
        );
        assert!(
            capability_download(id, &forced_query.replace("&dl=1", "&dl=%31"), secret, 100)
                .is_none()
        );
        assert!(
            capability_download(
                id,
                &forced_query.replace("&dl=1", "&dl=1&dl=1"),
                secret,
                100
            )
            .is_none()
        );
    }
    #[tokio::test]
    async fn machine_identity_never_inherits_human_file_access() {
        use tower::ServiceExt;
        let app = Router::new()
            .route("/", get(|| async { "private" }))
            .route_layer(axum::middleware::from_fn(human_only))
            .layer(Extension(super::super::actor::Actor::Cloud));
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    #[test]
    fn duplicate_range_headers_never_select_a_single_range() {
        let mut headers = HeaderMap::new();
        assert!(requested_range(&headers, 13).is_none());
        headers.append(header::RANGE, HeaderValue::from_static("bytes=0-3"));
        assert!(matches!(requested_range(&headers, 13), Some(Ok((0, 4)))));
        headers.append(header::RANGE, HeaderValue::from_static("bytes=5-7"));
        assert!(matches!(requested_range(&headers, 13), Some(Err(()))));
    }

    #[test]
    fn ranges_and_sanitizers() {
        assert_eq!(parse_range("bytes=2-4", 10), Some(Ok((2, 3))));
        assert_eq!(parse_range("bytes=-3", 10), Some(Ok((7, 3))));
        assert_eq!(parse_range("bytes=10-", 10), Some(Err(())));
        assert_eq!(parse_range("bytes=0-1,3-4", 10), Some(Err(())));
        assert_eq!(parse_range("bytes=0-", 0), Some(Err(())));
        assert_eq!(safe_filename("evil\r\n\"name"), "evil___name");
        assert_eq!(safe_filename("tab\tcontrol\u{1}"), "tab_control_");
        assert_eq!(safe_filename("báo cáo.png"), "b_o_c_o.png");
        assert_eq!(safe_filename(&"x".repeat(400)).len(), 255);
        assert_eq!(safe_filename(""), "file");
        assert_eq!(parse_range("bytes=-invalid", 10), Some(Err(())));
        assert!(!previewable("application/pdf", "file.pdf"));
        assert!(previewable("text/html", "file.html"));
        assert!(previewable("Application/JSON; Charset=UTF-8", "payload"));
        assert!(previewable(" TEXT/PLAIN ; charset=utf-8", "payload.bin"));
        assert!(previewable("application/octet-stream", r"nested\MAIN.GO"));
        assert!(previewable("application/octet-stream", "nested/Dockerfile"));
        assert!(previewable(
            "application/octet-stream",
            r"nested\.gitignore"
        ));
        assert!(!previewable(
            "application/octet-stream",
            "nested/Dockerfile.bin"
        ));
        assert!(!inline_content_type("image/svg+xml"));
        assert!(!inline_content_type("text/html"));
        assert!(inline_content_type("image/png"));
    }
    #[test]
    fn local_storage_requires_canonical_public_base_url() {
        let root = std::env::temp_dir().join(format!("files-ops-base-url-{}", Uuid::now_v7()));
        for url in [
            "https://example.invalid/?override=1",
            "https://example.invalid/#fragment",
            "https://user:pass@example.invalid",
            "https://example.invalid\r\nX-Injected: yes",
            "file:///uploads",
            "http://",
        ] {
            assert!(
                LocalObjects::new(root.clone(), Some(url.to_owned())).is_err(),
                "{url}"
            );
        }
        let store =
            LocalObjects::new(root.clone(), Some("https://example.invalid/app/".into())).unwrap();
        assert_eq!(
            store.object_url("users/id/object").unwrap(),
            "https://example.invalid/app/uploads/users/id/object"
        );
        assert_eq!(
            store
                .key_from_url("https://example.invalid/app/uploads/users/id/object")
                .unwrap(),
            "users/id/object"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn cleanup_keys_require_matching_row_identity_and_canonical_url() {
        let root = std::env::temp_dir().join(format!("files-ops-cleanup-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let owner = Uuid::now_v7();
        let id = Uuid::now_v7();
        let key = format!("users/{owner}/{id}.png");
        let url = store.object_url(&key).unwrap();
        assert_eq!(
            owned_object_key(&store, &url, "users", owner, id),
            Some(key)
        );
        assert!(owned_object_key(&store, &url, "users", Uuid::now_v7(), id).is_none());
        assert!(owned_object_key(&store, &url, "workspaces", owner, id).is_none());
        assert!(owned_object_key(&store, &url, "users", owner, Uuid::now_v7()).is_none());
        assert!(owned_object_key(&store, "/uploads/users/../bad", "users", owner, id).is_none());
        assert!(
            owned_object_key(
                &store,
                &format!("/uploads/users/{owner}/{id}.meta.json"),
                "users",
                owner,
                id
            )
            .is_none()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn concurrent_local_writes_never_clobber_an_existing_object() {
        let root = std::env::temp_dir().join(format!("files-ops-write-{}", Uuid::now_v7()));
        let store = Arc::new(LocalObjects::new(root.clone(), None).unwrap());
        let key = format!("workspaces/{}/{}.png", Uuid::now_v7(), Uuid::now_v7());
        let first = store.clone();
        let second = store.clone();
        let key1 = key.clone();
        let key2 = key.clone();
        let a = std::thread::spawn(move || first.write(&key1, b"first", "image/png", "a.png"));
        let b = std::thread::spawn(move || second.write(&key2, b"second", "image/png", "b.png"));
        let results = [a.join().unwrap(), b.join().unwrap()];
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        let bytes = store.read(&key, 0, 32).unwrap();
        assert!(bytes == b"first" || bytes == b"second");
        assert!(store.read(&key, 0, 32).is_ok());
        store.remove(&key).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn blocked_local_sidecar_never_overwrites_a_symlink_target() {
        let root = std::env::temp_dir().join(format!("files-ops-sidecar-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let key = format!("workspaces/{}/{}.png", Uuid::now_v7(), Uuid::now_v7());
        let path = store.path(&key).unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let target = root.join("marker");
        std::fs::write(&target, b"untouched").unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, format!("{}.meta.json", path.display())).unwrap();
            assert!(
                store
                    .write(&key, b"image", "image/png", "image.png")
                    .is_err()
            );
            assert_eq!(std::fs::read(&target).unwrap(), b"untouched");
            assert!(!path.exists());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn local_storage_never_follows_swapped_parent_or_leaf() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("files-ops-fd-{}", Uuid::now_v7()));
        let outside = std::env::temp_dir().join(format!("files-ops-outside-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&outside).unwrap();
        let marker = outside.join("item.png");
        std::fs::write(&marker, b"private").unwrap();
        let store = LocalObjects::new(root.clone(), None).unwrap();
        let key = "users/pointer/item.png";
        std::fs::create_dir_all(root.join("users")).unwrap();
        std::fs::set_permissions(root.join("users"), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        // Check a valid key, then replace its parent with a link before I/O.
        store.object_url(key).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("users/pointer")).unwrap();
        assert!(store.read(key, 0, 99).is_err());
        assert!(
            store
                .write(key, b"attack", "image/png", "item.png")
                .is_err()
        );
        assert!(store.remove(key).is_err());
        assert_eq!(std::fs::read(&marker).unwrap(), b"private");
        std::fs::remove_file(root.join("users/pointer")).unwrap();

        let key = "users/safe/item.png";
        store.write(key, b"image", "image/png", "item.png").unwrap();
        std::fs::remove_file(root.join(key)).unwrap();
        std::os::unix::fs::symlink(&marker, root.join(key)).unwrap();
        assert!(store.read(key, 0, 99).is_err());
        assert!(store.remove(key).is_err());
        assert_eq!(std::fs::read(&marker).unwrap(), b"private");
        std::fs::remove_file(root.join(key)).unwrap();
        assert!(store.remove("users/missing/item.png").is_ok());
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(outside).unwrap();
    }
    #[test]
    fn local_keys_reject_traversal_and_internal_objects() {
        let root = std::env::temp_dir().join(format!("files-ops-{}", Uuid::now_v7()));
        let store = LocalObjects::new(root.clone(), None).unwrap();
        for key in [
            "../secret",
            "/etc/passwd",
            "a/../../x",
            "workspaces/%2e%2e/key",
            "x.meta.json",
            ".x.tmp",
            "x?sig=1",
        ] {
            assert!(store.object_url(key).is_err(), "{key}");
        }
        std::fs::create_dir(root.join("users")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc", root.join("users/evil")).unwrap();
        #[cfg(unix)]
        assert!(store.read("users/evil/passwd", 0, 128).is_err());
        assert!(store.key_from_url("http://evil/uploads/users/a").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
