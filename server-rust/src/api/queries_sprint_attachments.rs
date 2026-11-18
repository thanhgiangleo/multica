//! Read-only attachment projection. The storage owner supplies deployment-specific URLs.
use super::{ApiError, error, timestamp};
use axum::http::{HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

/// Only rows selected by an authorized workspace/issue query reach this policy.
/// The implementation must generate a safe current URL and a durable markdown URL
/// for the configured storage mode; raw private object keys are not returned.
#[derive(Debug)]
pub struct AttachmentRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub issue_id: Option<Uuid>,
    pub comment_id: Option<Uuid>,
    pub chat_session_id: Option<Uuid>,
    pub chat_message_id: Option<Uuid>,
    pub uploader_type: String,
    pub uploader_id: Uuid,
    pub filename: String,
    pub storage_url: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
}

impl From<&Row> for AttachmentRecord {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            workspace_id: row.get("workspace_id"),
            issue_id: row.get("issue_id"),
            comment_id: row.get("comment_id"),
            chat_session_id: row.get("chat_session_id"),
            chat_message_id: row.get("chat_message_id"),
            uploader_type: row.get("uploader_type"),
            uploader_id: row.get("uploader_id"),
            filename: row.get("filename"),
            storage_url: row.get("url"),
            content_type: row.get("content_type"),
            size_bytes: row.get("size_bytes"),
            created_at: row.get("created_at"),
        }
    }
}

pub struct AttachmentUrls {
    pub url: String,
    pub download_url: String,
    pub markdown_url: String,
}

pub trait AttachmentUrlPolicy: Send + Sync {
    fn resolve(&self, row: &AttachmentRecord, stable: bool) -> Option<AttachmentUrls>;
}
pub type SharedAttachmentUrlPolicy = Arc<dyn AttachmentUrlPolicy>;

pub(super) fn stable_requested(headers: &HeaderMap) -> bool {
    headers
        .get("x-client-capabilities")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim() == "stable_attachment_urls")
        })
}

fn project(row: &Row, policy: &SharedAttachmentUrlPolicy, stable: bool) -> Result<Value, ApiError> {
    let record = AttachmentRecord::from(row);
    let urls = policy.resolve(&record, stable).ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "attachment URL policy unavailable",
        )
    })?;
    if urls.url.is_empty() || urls.download_url.is_empty() || urls.markdown_url.is_empty() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "attachment URL policy unavailable",
        ));
    }
    Ok(json!({
        "id":record.id,"workspace_id":record.workspace_id,
        "issue_id":record.issue_id,"comment_id":record.comment_id,
        "chat_session_id":record.chat_session_id,"chat_message_id":record.chat_message_id,
        "uploader_type":record.uploader_type,"uploader_id":record.uploader_id,
        "filename":record.filename,"url":urls.url,"download_url":urls.download_url,
        "markdown_url":urls.markdown_url,"content_type":record.content_type,
        "size_bytes":record.size_bytes,"created_at":timestamp(record.created_at),
    }))
}

pub(super) async fn for_issue(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    policy: Option<&SharedAttachmentUrlPolicy>,
    stable: bool,
) -> Result<Vec<Value>, ApiError> {
    let rows = tx
        .query(
            "SELECT * FROM attachment WHERE workspace_id=$1 AND issue_id=$2 ORDER BY created_at,id",
            &[&ws, &issue],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load issue attachments",
            )
        })?;
    project_rows(rows, policy, stable)
}

fn project_rows(
    rows: Vec<Row>,
    policy: Option<&SharedAttachmentUrlPolicy>,
    stable: bool,
) -> Result<Vec<Value>, ApiError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let policy = policy.ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "attachment URL policy unavailable",
        )
    })?;
    rows.iter()
        .map(|row| project(row, policy, stable))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_capability_requires_exact_token() {
        for input in ["stable_attachment_urls", "chat, stable_attachment_urls "] {
            let mut headers = HeaderMap::new();
            headers.insert("x-client-capabilities", input.parse().unwrap());
            assert!(stable_requested(&headers));
        }
        for input in [
            "stable_attachment_urls_extra",
            "prefixstable_attachment_urls",
            "STABLE_ATTACHMENT_URLS",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("x-client-capabilities", input.parse().unwrap());
            assert!(!stable_requested(&headers));
        }
    }
}
