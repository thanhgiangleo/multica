//! Frozen-Go source-context preview and text-only manual capture. Attachment cloning
//! and agent enqueue are deliberately unavailable, never silently omitted.
use super::{Actor, ApiError, ApiState, GoJson, Identity, WorkspaceId, error};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tokio_postgres::Client;
use uuid::Uuid;

const MAX_COMMENTS: usize = 256;
const MAX_TEXT: usize = 1 << 20;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/comments/{commentId}/sub-issue-preview", get(preview))
        .route(
            "/api/comments/{commentId}/sub-issues",
            axum::routing::post(create),
        )
}

#[derive(Serialize, Clone)]
struct Attachment {
    id: String,
    owner_type: &'static str,
    owner_id: String,
    filename: String,
    content_type: String,
    size_bytes: i64,
    created_at: String,
}
#[derive(Serialize, Clone)]
struct Author {
    #[serde(rename = "type")]
    kind: String,
    id: String,
    name: String,
}
#[derive(Serialize, Clone)]
struct IssueSnapshot {
    id: String,
    identifier: String,
    number: i32,
    title: String,
    description: Option<String>,
    created_at: String,
    updated_at: String,
    revision: i64,
    attachments: Vec<Attachment>,
}
#[derive(Serialize, Clone)]
struct CommentSnapshot {
    id: String,
    parent_id: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    content: String,
    author: Author,
    created_at: String,
    updated_at: String,
    revision: i64,
    attachments: Vec<Attachment>,
    #[serde(skip_serializing_if = "is_false")]
    deleted: bool,
}
fn is_false(value: &bool) -> bool {
    !*value
}
#[derive(Serialize, Clone)]
struct Snapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    captured_by_user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    captured_at: Option<String>,
    source_issue: IssueSnapshot,
    comment_thread: Vec<CommentSnapshot>,
    anchor_comment_id: String,
}
#[derive(Serialize)]
struct Limits {
    comment_count: usize,
    text_bytes: usize,
    attachment_count: usize,
    attachment_bytes: i64,
}

pub(crate) struct Capture {
    pub id: Uuid,
    pub source_issue: Uuid,
    pub anchor: Uuid,
    pub snapshot: Value,
    pub digest: String,
    pub at: DateTime<Utc>,
}
struct Build {
    snapshot: Snapshot,
    digest: String,
    limits: Limits,
    source: Uuid,
}

// Go json.Marshal escapes HTML and line separators even in otherwise ordinary strings.
fn go_json<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serde_json::to_string(value)
        .map(|s| {
            s.replace('&', "\\u0026")
                .replace('<', "\\u003c")
                .replace('>', "\\u003e")
                .replace('\u{2028}', "\\u2028")
                .replace('\u{2029}', "\\u2029")
        })
        .map_err(|_| internal())
}
fn stamp(at: DateTime<Utc>) -> String {
    let raw = at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let (time, _) = raw.split_once('.').expect("nanosecond format");
    let fraction = &raw[time.len() + 1..raw.len() - 1];
    let digits = fraction.trim_end_matches('0');
    if digits.is_empty() {
        format!("{time}Z")
    } else {
        format!("{time}.{digits}Z")
    }
}
fn internal() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to capture source context",
    )
}
fn conflict(code: &'static str, message: &'static str) -> Response {
    (
        StatusCode::CONFLICT,
        GoJson(json!({"code":code,"error":message})),
    )
        .into_response()
}
fn too_large(limits: Limits) -> Response {
    (StatusCode::UNPROCESSABLE_ENTITY,GoJson(json!({"code":"source_context_too_large","error":"source context too large","limits":limits}))).into_response()
}
fn unsupported() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE,GoJson(json!({"code":"source_context_attachments_unsupported","error":"source context attachment cloning is not available"}))).into_response()
}
// All recursive edges are constrained by workspace AND issue; the 257-row probe
// rejects oversized histories rather than silently truncating captured context.
async fn build(
    db: &Client,
    ws: Uuid,
    anchor_id: Uuid,
) -> Result<Result<Build, Response>, ApiError> {
    let Some(anchor) = db
        .query_opt(
            "SELECT * FROM comment WHERE workspace_id=$1 AND id=$2",
            &[&ws, &anchor_id],
        )
        .await
        .map_err(|_| internal())?
    else {
        return Ok(Err(conflict(
            "anchor_comment_deleted",
            "anchor comment deleted",
        )));
    };
    if anchor
        .get::<_, Option<DateTime<Utc>>>("deleted_at")
        .is_some()
    {
        return Ok(Err(conflict(
            "anchor_comment_deleted",
            "anchor comment deleted",
        )));
    }
    if anchor.get::<_, String>("type") != "comment" {
        return Ok(Err(conflict(
            "source_context_invalid_path",
            "source context invalid comment thread",
        )));
    }
    let issue_id: Uuid = anchor.get("issue_id");
    let Some(issue) = db
        .query_opt(
            "SELECT * FROM issue WHERE workspace_id=$1 AND id=$2",
            &[&ws, &issue_id],
        )
        .await
        .map_err(|_| internal())?
    else {
        return Ok(Err(conflict(
            "source_issue_deleted",
            "source issue deleted",
        )));
    };
    let prefix: String = db
        .query_one("SELECT issue_prefix FROM workspace WHERE id=$1", &[&ws])
        .await
        .map_err(|_| internal())?
        .get(0);
    let ancestors=db.query("WITH RECURSIVE path AS (SELECT c.id,c.parent_id,ARRAY[c.id]::uuid[] visited,1 depth,false cycle FROM comment c WHERE c.id=$1 AND c.workspace_id=$2 AND c.issue_id=$3 UNION ALL SELECT p.id,p.parent_id,path.visited || p.id,path.depth+1,p.id=ANY(path.visited) FROM path JOIN comment p ON p.id=path.parent_id WHERE p.workspace_id=$2 AND p.issue_id=$3 AND path.depth<=256 AND NOT path.cycle) SELECT id,parent_id,cycle FROM path ORDER BY depth DESC", &[&anchor_id,&ws,&issue_id]).await.map_err(|_|internal())?;
    if ancestors.len() > MAX_COMMENTS {
        return Ok(Err(too_large(Limits {
            comment_count: ancestors.len(),
            text_bytes: 0,
            attachment_count: 0,
            attachment_bytes: 0,
        })));
    }
    if ancestors
        .first()
        .is_none_or(|r| r.get::<_, Option<Uuid>>("parent_id").is_some())
        || ancestors
            .last()
            .is_none_or(|r| r.get::<_, Uuid>("id") != anchor_id)
        || ancestors.iter().any(|r| r.get::<_, bool>("cycle"))
    {
        return Ok(Err(conflict(
            "source_context_invalid_path",
            "source context invalid comment thread",
        )));
    }
    let root: Uuid = ancestors[0].get("id");
    let created: DateTime<Utc> = anchor.get("created_at");
    let rows=db.query("WITH RECURSIVE history AS (SELECT c.* FROM comment c WHERE c.id=$1 AND c.workspace_id=$2 AND c.issue_id=$3 AND c.parent_id IS NULL AND (c.created_at,c.id)<=($4,$5) UNION ALL SELECT c.* FROM comment c JOIN history p ON c.parent_id=p.id WHERE c.workspace_id=$2 AND c.issue_id=$3 AND (c.created_at,c.id)<=($4,$5)) SELECT * FROM history ORDER BY created_at,id LIMIT 257", &[&root,&ws,&issue_id,&created,&anchor_id]).await.map_err(|_|internal())?;
    if rows.len() > MAX_COMMENTS {
        return Ok(Err(too_large(Limits {
            comment_count: rows.len(),
            text_bytes: 0,
            attachment_count: 0,
            attachment_bytes: 0,
        })));
    }
    if rows.first().is_none_or(|r| r.get::<_, Uuid>("id") != root)
        || rows
            .last()
            .is_none_or(|r| r.get::<_, Uuid>("id") != anchor_id)
        || rows
            .iter()
            .map(|r| r.get::<_, Uuid>("id"))
            .collect::<HashSet<_>>()
            .len()
            != rows.len()
    {
        return Ok(Err(conflict(
            "source_context_invalid_path",
            "source context invalid comment thread",
        )));
    }
    let issue_files=db.query("SELECT * FROM attachment WHERE workspace_id=$1 AND issue_id=$2 AND comment_id IS NULL AND source_context_id IS NULL ORDER BY created_at,id", &[&ws,&issue_id]).await.map_err(|_|internal())?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let comment_files=db.query("SELECT * FROM attachment WHERE workspace_id=$1 AND issue_id=$2 AND comment_id=ANY($3::uuid[]) AND source_context_id IS NULL ORDER BY created_at,id", &[&ws,&issue_id,&ids]).await.map_err(|_|internal())?;
    // No storage clone/intent adapter in this slice. Fail before issuing a token
    // that could falsely promise an attachment-complete capture.
    if !issue_files.is_empty() || !comment_files.is_empty() {
        return Ok(Err(unsupported()));
    }
    let mut comments = Vec::with_capacity(rows.len());
    for row in rows {
        let author_type: String = row.get("author_type");
        let author_id: Uuid = row.get("author_id");
        let name = match author_type.as_str() {
            "member" => db
                .query_opt("SELECT name FROM \"user\" WHERE id=$1", &[&author_id])
                .await
                .map_err(|_| internal())?,
            "agent" => db
                .query_opt(
                    "SELECT name FROM agent WHERE id=$1 AND workspace_id=$2",
                    &[&author_id, &ws],
                )
                .await
                .map_err(|_| internal())?,
            _ => {
                return Ok(Err(conflict(
                    "source_context_invalid_path",
                    "source context invalid comment thread",
                )));
            }
        }
        .ok_or_else(internal)?
        .get(0);
        comments.push(CommentSnapshot {
            id: row.get::<_, Uuid>("id").to_string(),
            parent_id: row
                .get::<_, Option<Uuid>>("parent_id")
                .map(|id| id.to_string()),
            kind: row.get("type"),
            content: row.get("content"),
            author: Author {
                kind: author_type,
                id: author_id.to_string(),
                name,
            },
            created_at: stamp(row.get("created_at")),
            updated_at: stamp(row.get("updated_at")),
            revision: row.get("revision"),
            attachments: Vec::new(),
            deleted: row.get::<_, Option<DateTime<Utc>>>("deleted_at").is_some(),
        });
    }
    let snapshot = Snapshot {
        version: None,
        captured_by_user_id: None,
        captured_at: None,
        source_issue: IssueSnapshot {
            id: issue_id.to_string(),
            identifier: format!("{prefix}-{}", issue.get::<_, i32>("number")),
            number: issue.get("number"),
            title: issue.get("title"),
            description: issue.get("description"),
            created_at: stamp(issue.get("created_at")),
            updated_at: stamp(issue.get("updated_at")),
            revision: issue.get("revision"),
            attachments: Vec::new(),
        },
        comment_thread: comments,
        anchor_comment_id: anchor_id.to_string(),
    };
    let text_bytes = go_json(&snapshot)?.len();
    let limits = Limits {
        comment_count: snapshot.comment_thread.len(),
        text_bytes,
        attachment_count: 0,
        attachment_bytes: 0,
    };
    if text_bytes > MAX_TEXT {
        return Ok(Err(too_large(limits)));
    }
    let mut canonical = snapshot.clone();
    canonical.source_issue.updated_at.clear();
    canonical.source_issue.revision = 0;
    for item in &mut canonical.comment_thread {
        item.author.name.clear();
        item.updated_at.clear();
        item.revision = 0;
    }
    let digest = format!("{:x}", Sha256::digest(go_json(&canonical)?.as_bytes()));
    Ok(Ok(Build {
        snapshot,
        digest,
        limits,
        source: issue_id,
    }))
}

fn human(actor: &Actor) -> Result<(), ApiError> {
    if matches!(actor, Actor::Human | Actor::Cloud) {
        Ok(())
    } else {
        Err(error(StatusCode::FORBIDDEN, "human actor required"))
    }
}
fn parse_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment_id"))
}
async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    human(&actor)?;
    let db = state.pool.get().await.map_err(|_| internal())?;
    let build = match build(&db, ws, parse_id(&raw)?).await? {
        Ok(build) => build,
        Err(response) => return Ok(response),
    };
    Ok((StatusCode::OK,GoJson(json!({"source_issue":build.snapshot.source_issue,
        "comment_thread":build.snapshot.comment_thread,"anchor_comment_id":build.snapshot.anchor_comment_id,
        "capture_token":format!("sha256:{}:{}",build.digest,build.source),"limits":build.limits}))).into_response())
}
fn parse_token(raw: &str) -> Option<(String, Uuid)> {
    let parts: Vec<_> = raw.trim().split(':').collect();
    if parts.len() != 3
        || parts[0] != "sha256"
        || parts[1].len() != 64
        || !parts[1].bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    Some((parts[1].to_owned(), Uuid::parse_str(parts[2]).ok()?))
}
async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    human(&actor)?;
    let req: Value = serde_json::from_slice(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let mode = req.get("mode").and_then(Value::as_str).unwrap_or("");
    // Agent enqueue is not implemented. Reject it even with a valid token.
    if mode == "agent" {
        return Ok((StatusCode::SERVICE_UNAVAILABLE,GoJson(json!({"code":"source_context_agent_unsupported","error":"agent sub-issue creation is not available"}))).into_response());
    }
    if mode != "manual" {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "mode must be manual or agent",
        ));
    }
    let mut issue = req
        .get("issue")
        .filter(|v| v.is_object())
        .cloned()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "issue is required for manual mode"))?;
    // Never silently discard fields not supported by the text-only slice.
    if issue
        .get("attachment_ids")
        .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
    {
        return Ok(unsupported());
    }
    let title = issue
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    if title.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            GoJson(json!({"code":"invalid_request","error":"title is required"})),
        )
            .into_response());
    }
    let obj = issue.as_object_mut().expect("checked object");
    obj.insert("title".into(), json!(title));
    // Frozen handler overwrites parent with capture.SourceIssueID; it never
    // validates the caller-supplied parent_issue_id.
    obj.remove("parent_issue_id");
    // Frozen Go derives lineage from the captured source, not caller-controlled origin.
    obj.remove("origin_type");
    obj.remove("origin_id");
    for key in [
        "priority",
        "status",
        "assignee_type",
        "assignee_id",
        "project_id",
    ] {
        if let Some(Value::String(value)) = obj.get_mut(key) {
            *value = value.trim().to_owned();
        }
    }
    let (want, source) = match req
        .get("capture_token")
        .and_then(Value::as_str)
        .and_then(parse_token)
    {
        Some(token) => token,
        None => {
            return Ok(conflict(
                "source_context_changed",
                "source context preview is invalid; refresh and try again",
            ));
        }
    };
    let anchor = parse_id(&raw)?;
    let db = state.pool.get().await.map_err(|_| internal())?;
    let build = match build(&db, ws, anchor).await? {
        Ok(build) => build,
        Err(response) => {
            // Go only disambiguates an absent anchor by checking whether the
            // preview's source issue itself has been deleted.
            if db
                .query_opt(
                    "SELECT 1 FROM comment WHERE workspace_id=$1 AND id=$2 AND deleted_at IS NULL",
                    &[&ws, &anchor],
                )
                .await
                .map_err(|_| internal())?
                .is_none()
            {
                let exists = db
                    .query_opt(
                        "SELECT 1 FROM issue WHERE workspace_id=$1 AND id=$2",
                        &[&ws, &source],
                    )
                    .await
                    .map_err(|_| internal())?;
                if exists.is_none() {
                    return Ok(conflict("source_issue_deleted", "source issue deleted"));
                }
            }
            return Ok(response);
        }
    };
    if build.digest != want || build.source != source {
        return Ok(conflict("source_context_changed", "source context changed"));
    }
    let at = Utc::now();
    let mut snapshot = build.snapshot;
    snapshot.version = Some(1);
    snapshot.captured_by_user_id = Some(user.to_string());
    snapshot.captured_at = Some(stamp(at));
    let capture = Capture {
        id: Uuid::now_v7(),
        source_issue: source,
        anchor,
        snapshot: serde_json::to_value(snapshot).map_err(|_| internal())?,
        digest: build.digest,
        at,
    };
    super::super::issue_write_create::create_from_comment(
        state,
        ws,
        user,
        headers,
        &serde_json::to_vec(&issue).map_err(|_| internal())?,
        capture,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_and_go_timestamp_projection() {
        let id = Uuid::new_v4();
        assert_eq!(
            parse_token(&format!("sha256:{}:{id}", "a".repeat(64))),
            Some(("a".repeat(64), id))
        );
        assert!(parse_token("sha256:abc:invalid").is_none());
        let time = DateTime::parse_from_rfc3339("2026-11-04T00:00:01.123400Z")
            .unwrap()
            .to_utc();
        assert_eq!(stamp(time), "2026-11-04T00:00:01.1234Z");
        assert_eq!(
            stamp(
                DateTime::parse_from_rfc3339("2026-11-04T00:00:00Z")
                    .unwrap()
                    .to_utc()
            ),
            "2026-11-04T00:00:00Z"
        );
        assert_eq!(
            go_json(&json!({"body":"<&>"})).unwrap(),
            "{\"body\":\"\\u003c\\u0026\\u003e\"}"
        );
    }
}
