use super::super::actor::Actor;
use super::{
    ApiError, ApiState, Attachment, AttachmentEvent, GoJson, Identity, WorkspaceSelectors, error,
    metadata,
};
use axum::{
    Router,
    extract::Extension,
    extract::{DefaultBodyLimit, Multipart, RawQuery, State},
    http::StatusCode,
    routing::post,
};
use serde_json::Value;
use uuid::Uuid;

const MAX_UPLOAD: usize = 100 << 20;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/upload-file",
        post(upload).layer(DefaultBodyLimit::max(MAX_UPLOAD)),
    )
}

fn avif_brand(data: &[u8]) -> bool {
    let Some(size) = data
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_be_bytes)
    else {
        return false;
    };
    let size = size as usize;
    size >= 16
        && size <= data.len()
        && (data.get(8..12) == Some(b"avif".as_slice())
            || data.get(8..12) == Some(b"avis".as_slice())
            || data[16..size]
                .chunks_exact(4)
                .any(|brand| brand == b"avif" || brand == b"avis"))
        && data.get(4..8) == Some(b"ftyp".as_slice())
}

fn sniff(data: &[u8], name: &str) -> &'static str {
    let name = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match name.as_str() {
        "svg" => return "image/svg+xml",
        "css" => return "text/css",
        "js" | "mjs" => return "application/javascript",
        "json" => return "application/json",
        "wasm" => return "application/wasm",
        _ => {}
    }
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if data.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        "image/gif"
    } else if data.starts_with(b"%PDF-") {
        "application/pdf"
    } else if data.len() >= 12 && &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        "image/webp"
    } else if data.starts_with(b"BM") && data.len() >= 14 {
        "image/bmp"
    } else if data.starts_with(b"\0\0\x01\0") && data.len() >= 6 {
        "image/x-icon"
    } else if avif_brand(data) {
        "image/avif"
    } else if data.iter().take(512).all(|v| {
        // Go's http.DetectContentType text signature rejects C0 binary
        // controls, not non-ASCII bytes or DEL. Inspect only its first 512
        // bytes so multilingual text uploads keep their previewable MIME.
        !matches!(*v, 0x00..=0x08 | 0x0b | 0x0e..=0x1a | 0x1c..=0x1f)
    }) {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

fn agree_selector(
    query: Option<String>,
    header: Option<String>,
) -> Result<Option<String>, ApiError> {
    let query = query.filter(|value| !value.is_empty());
    let header = header.filter(|value| !value.is_empty());
    if let (Some(query), Some(header)) = (&query, &header)
        && query != header
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "workspace selectors disagree",
        ));
    }
    Ok(query.or(header))
}

async fn upload(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(selectors): Extension<WorkspaceSelectors>,
    RawQuery(query): RawQuery,
    mut fields: Multipart,
) -> Result<GoJson<Value>, ApiError> {
    // Task, cloud and daemon uploads need actor-specific ownership/private-chat
    // checks not available here; never trust caller-supplied task/agent headers.
    if !matches!(actor, Actor::Human) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "machine upload policy unavailable",
        ));
    }
    let mut file = None;
    let mut issue_raw = None;
    let mut comment_raw = None;
    let mut unsupported = false;
    while let Some(field) = fields.next_field().await.map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "file too large or invalid multipart form",
        )
    })? {
        match field.name().unwrap_or("") {
            "file" => {
                if file.is_some() {
                    return Err(error(StatusCode::BAD_REQUEST, "multiple file fields"));
                }
                let name = field.file_name().unwrap_or("").to_owned();
                if name.is_empty() {
                    return Err(error(StatusCode::BAD_REQUEST, "filename is required"));
                }
                let bytes = field.bytes().await.map_err(|_| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "file too large or invalid multipart form",
                    )
                })?;
                if bytes.len() > MAX_UPLOAD {
                    return Err(error(StatusCode::PAYLOAD_TOO_LARGE, "file too large"));
                }
                file = Some((name, bytes));
            }
            "issue_id" => {
                if issue_raw.is_some() {
                    return Err(error(StatusCode::BAD_REQUEST, "multiple issue_id fields"));
                }
                issue_raw = Some(
                    field
                        .text()
                        .await
                        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid issue_id"))?,
                );
            }
            "comment_id" => {
                if comment_raw.is_some() {
                    return Err(error(StatusCode::BAD_REQUEST, "multiple comment_id fields"));
                }
                comment_raw = Some(
                    field
                        .text()
                        .await
                        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment_id"))?,
                );
            }
            "chat_session_id" | "chat_message_id" | "task_id" | "source_context_id" => {
                unsupported = true;
            }
            _ => {}
        }
    }
    if unsupported {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "linked upload policy unavailable",
        ));
    }
    let (filename, data) =
        file.ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing file field"))?;
    let storage = state.files_storage.clone().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "file upload not configured",
        )
    })?;
    // A multipart upload must have one unambiguous workspace. A query value
    // cannot silently override a conflicting header (or a repeated query key).
    let mut query_slug = None;
    let mut query_id = None;
    if let Some(raw) = query.as_deref() {
        for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
            let target = match key.as_ref() {
                "workspace_slug" => &mut query_slug,
                "workspace_id" => &mut query_id,
                _ => continue,
            };
            if target.replace(value.into_owned()).is_some() {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "repeated workspace selector",
                ));
            }
        }
    }
    let slug = agree_selector(query_slug, selectors.slug)?;
    let raw_ws = agree_selector(query_id, selectors.id)?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    let selected_id = raw_ws
        .map(|raw| {
            Uuid::parse_str(&raw)
                .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))
        })
        .transpose()?;
    let ws = if let Some(slug) = slug {
        let resolved: Uuid = db
            .query_opt("SELECT id FROM workspace WHERE slug=$1", &[&slug])
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
            .get(0);
        if selected_id.is_some_and(|id| id != resolved) {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "workspace selectors disagree",
            ));
        }
        Some(resolved)
    } else {
        selected_id
    };
    let mut issue = issue_raw
        .filter(|v| !v.is_empty())
        .map(|raw| {
            Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid issue_id"))
        })
        .transpose()?;
    let comment = comment_raw
        .filter(|v| !v.is_empty())
        .map(|raw| {
            Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment_id"))
        })
        .transpose()?;
    if (issue.is_some() || comment.is_some()) && ws.is_none() {
        return Err(error(StatusCode::BAD_REQUEST, "workspace_id is required"));
    }
    if let (Some(ws), Some(comment)) = (ws, comment) {
        let parent: Uuid = db
            .query_opt(
                "SELECT issue_id FROM comment WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL",
                &[&comment, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
            .ok_or_else(|| error(StatusCode::FORBIDDEN, "invalid comment_id"))?
            .get(0);
        if issue.is_some_and(|id| id != parent) {
            return Err(error(StatusCode::FORBIDDEN, "invalid comment_id"));
        }
        issue = Some(parent);
    }
    if let Some(ws) = ws {
        if db
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &user],
            )
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
            .is_none()
        {
            return Err(error(
                StatusCode::FORBIDDEN,
                "not a member of this workspace",
            ));
        }
        if let Some(issue) = issue
            && db
                .query_opt(
                    "SELECT 1 FROM issue WHERE id=$1 AND workspace_id=$2",
                    &[&issue, &ws],
                )
                .await
                .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
                .is_none()
        {
            return Err(error(StatusCode::FORBIDDEN, "invalid issue_id"));
        }
    }
    if issue.is_some() && state.files_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "attachment event bus unavailable",
        ));
    }
    drop(db);
    let id = Uuid::now_v7();
    let extension = filename.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
    let extension = if extension.len() <= 12
        && extension.bytes().all(|c| c.is_ascii_alphanumeric())
        && !extension.is_empty()
    {
        format!(".{extension}")
    } else {
        String::new()
    };
    let key = if let Some(ws) = ws {
        format!("workspaces/{ws}/{id}{extension}")
    } else {
        format!("users/{user}/{id}{extension}")
    };
    let url = storage
        .object_url(&key)
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "file storage unavailable"))?;
    let content_type = sniff(&data, &filename).to_owned();
    let size_bytes = data.len() as i64;
    if let Some(ws) = ws {
        // Record object intent BEFORE writing bytes. An upload which crashes or
        // fails DB insertion remains reclaimable; the reconciler skips rows
        // locked by a live writer and waits 10 minutes for abandoned intents.
        let mut db = state
            .pool
            .get()
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
        db.execute("INSERT INTO attachment_object_cleanup (attachment_id,workspace_id,object_url) VALUES ($1,$2,$3)", &[&id,&ws,&url]).await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE,"attachment cleanup ledger unavailable"))?;
        let tx = db
            .transaction()
            .await
            .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
        tx.query_one("SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true),set_config('multica.source_task_id','',true)", &[&user.to_string()]).await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to set attachment actor"))?;
        tx.query_one(
            "SELECT attachment_id FROM attachment_object_cleanup WHERE attachment_id=$1 FOR UPDATE",
            &[&id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "attachment cleanup ledger unavailable",
            )
        })?;
        // An issue/comment may be removed while the object uploads. Lock the
        // owner first, then validate again in the same transaction.
        if let Some(issue) = issue {
            tx.query_opt(
                "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
                &[&issue, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock issue"))?
            .ok_or_else(|| error(StatusCode::FORBIDDEN, "invalid issue_id"))?;
        }
        if let Some(comment) = comment {
            tx.query_opt(
                "SELECT id FROM comment WHERE id=$1 AND workspace_id=$2 AND issue_id=$3 AND deleted_at IS NULL FOR UPDATE",
                &[&comment, &ws, &issue.expect("validated comment parent")],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to lock comment"))?
            .ok_or_else(|| error(StatusCode::FORBIDDEN, "invalid comment_id"))?;
        }
        // Authorization checked before streaming can be revoked while the
        // request waits for an owner lock. Hold membership until commit so a
        // concurrent removal either finishes first or waits for this upload.
        tx.query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR KEY SHARE",
            &[&ws, &user],
        )
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
        .ok_or_else(|| error(StatusCode::FORBIDDEN, "not a member of this workspace"))?;
        let store = storage.clone();
        let object_key = key.clone();
        let name = filename.clone();
        let kind = content_type.clone();
        tokio::task::spawn_blocking(move || store.write(&object_key, &data, &kind, &name))
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed"))?
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed"))?;
        let row = tx.query_one("WITH inserted AS (INSERT INTO attachment (id,workspace_id,issue_id,comment_id,uploader_type,uploader_id,filename,url,content_type,size_bytes) VALUES ($1,$2,$3,$4,'member',$5,$6,$7,$8,$9) RETURNING *), bumped AS (UPDATE issue SET revision=revision+1 WHERE id IN (SELECT issue_id FROM inserted WHERE issue_id IS NOT NULL) RETURNING revision), bumped_comment AS (UPDATE comment SET revision=revision+1 WHERE id IN (SELECT comment_id FROM inserted WHERE comment_id IS NOT NULL) RETURNING revision) SELECT inserted.*,COALESCE((SELECT revision FROM bumped),0)::bigint AS issue_revision,COALESCE((SELECT revision FROM bumped_comment),0)::bigint AS comment_revision FROM inserted", &[&id,&ws,&issue,&comment,&user,&filename,&url,&content_type,&size_bytes]).await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to create attachment"))?;
        let revision: i64 = row.get("issue_revision");
        let comment_revision: i64 = row.get("comment_revision");
        let att = Attachment::from(row);
        let comment_projection = if let Some(comment) = comment {
            let row = tx
                .query_one(
                    "SELECT * FROM comment WHERE id=$1 AND workspace_id=$2",
                    &[&comment, &ws],
                )
                .await
                .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load comment"))?;
            Some(
                super::super::issue_write_resolution::response_in_transaction(&tx, &row, ws)
                    .await?,
            )
        } else {
            None
        };
        tx.execute(
            "DELETE FROM attachment_object_cleanup WHERE attachment_id=$1",
            &[&id],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to finalize upload",
            )
        })?;
        tx.commit()
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to commit upload"))?;
        if let Some(issue) = issue
            && let Some(sink) = &state.files_events
        {
            sink.publish_attachment_changed(AttachmentEvent {
                workspace_id: ws,
                actor_id: user,
                issue_id: Some(issue),
                issue_revision: revision,
                comment_id: comment,
                comment_revision,
                comment: comment_projection,
            });
            sink.request_wakeup_dispatch(ws, issue);
        }
        return Ok(GoJson(
            serde_json::to_value(metadata(&att, false, &state)).unwrap(),
        ));
    }
    // Persist an intent before touching storage. A crashed writer leaves a
    // reclaimable row; only a committed ready row can be read or signed.
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    db.execute("INSERT INTO user_asset_object(id,user_id,object_url,filename,content_type,size_bytes) VALUES($1,$2,$3,$4,$5,$6)", &[&id,&user,&url,&filename,&content_type,&size_bytes]).await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "user asset ledger unavailable"))?;
    // The intent is already committed. Reuse its connection rather than waiting
    // for another checkout while retaining the only slot in a bounded pool.
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?;
    // A JWT may outlive its user. Lock the owner before writing bytes so
    // concurrent account deletion either wins and rejects the upload or waits
    // until the ready ledger commits. The stale intent remains reclaimable.
    tx.query_opt(
        "SELECT id FROM \"user\" WHERE id=$1 FOR KEY SHARE",
        &[&user],
    )
    .await
    .map_err(|_| error(StatusCode::SERVICE_UNAVAILABLE, "database unavailable"))?
    .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "user not found"))?;
    tx.query_one(
        "SELECT id FROM user_asset_object WHERE id=$1 AND user_id=$2 FOR UPDATE",
        &[&id, &user],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "user asset intent missing",
        )
    })?;
    let store = storage.clone();
    let object_key = key.clone();
    let name = filename.clone();
    let kind = content_type.clone();
    tokio::task::spawn_blocking(move || store.write(&object_key, &data, &kind, &name))
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed"))?
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "upload failed"))?;
    tx.execute(
        "UPDATE user_asset_object SET ready=true WHERE id=$1 AND user_id=$2",
        &[&id, &user],
    )
    .await
    .map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to finalize user asset",
        )
    })?;
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to commit user asset",
        )
    })?;
    Ok(GoJson(serde_json::json!({
        "id": id.to_string(),
        "url": url,
        "filename": filename,
        "download_url": format!("/api/user-assets/{id}/download"),
    })))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sniff_overrides_client_mime() {
        assert_eq!(sniff(b"\x89PNG\r\n\x1a\n", "wrong.txt"), "image/png");
        assert_eq!(sniff(b"hello", "test.json"), "application/json");
        assert_eq!(
            sniff("Ghi chú tiếng Việt 📝".as_bytes(), "note.txt"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            sniff(b"valid\x7ftext", "note.txt"),
            "text/plain; charset=utf-8"
        );
        assert_eq!(
            sniff(b"binary\0text", "note.txt"),
            "application/octet-stream"
        );
        assert_eq!(
            sniff(b"BM\0\0\0\0\0\0\0\0\0\0\0\0", "photo.bmp"),
            "image/bmp"
        );
        assert_eq!(sniff(b"\0\0\x01\0\x01\0", "favicon.ico"), "image/x-icon");
        assert_eq!(
            sniff(b"\0\0\0\x14ftypavif\0\0\0\0mif1", "photo.avif"),
            "image/avif"
        );
        assert_eq!(
            sniff(b"\0\0\0\x14ftypmif1\0\0\0\0avif", "photo.avif"),
            "image/avif"
        );
        assert_eq!(
            sniff(b"\0\0\0\x14ftypmif1\0\0\0\0xxxx", "photo.avif"),
            "application/octet-stream"
        );
    }
}
