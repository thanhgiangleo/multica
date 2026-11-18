//! Regular issue creation for member/unassigned, non-workflow issues.
//! Unsupported dispatch, media presentation and workflow effects fail before writes.
use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_write::write_actor, issue_write_labels, issue_write_update, issue_write_value,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::NaiveDate;
#[path = "issue_write_create_decode.rs"]
mod decode;
use decode::Request;
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::{Transaction, error::SqlState};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct IssueCreatedEvent {
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IssueCountPolicy {
    /// None means Cloud returned action=off; positive value means action=enforce.
    pub limit: Option<i64>,
    pub revision: i64,
}

#[derive(Clone, Debug)]
pub struct IssueCreateAttachment {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub issue_id: Option<Uuid>,
    pub comment_id: Option<Uuid>,
    pub chat_session_id: Option<Uuid>,
    pub chat_message_id: Option<Uuid>,
    pub uploader_type: String,
    pub uploader_id: Uuid,
    pub filename: String,
    pub url: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub struct IssueCreateAttachmentUrls {
    pub download_url: String,
    pub markdown_url: String,
}

/// Real deployment-aware CF signing/public storage/Markdown policy, not a
/// stable-path fallback. Bulk responses never mint attachment_download_url.
pub trait IssueCreateAttachmentPresenter: Send + Sync {
    fn urls(
        &self,
        attachment: &IssueCreateAttachment,
        stable_download_urls: bool,
    ) -> IssueCreateAttachmentUrls;
}

/// Integrator supplies real Cloud entitlement gate and analytics publisher.
/// Resolve gate before starting the DB transaction. No fallback policy is safe.
#[async_trait]
pub trait IssueCreateRuntime: Send + Sync {
    async fn count_policy(&self, workspace: Uuid) -> Result<IssueCountPolicy, ()>;
    fn created(&self, event: &IssueCreatedEvent);
    fn quick_create_runtime(
        &self,
    ) -> Option<&dyn super::issue_write_quick_create::IssueQuickCreateRuntime> {
        None
    }
    fn attachment_presenter(&self) -> Option<&dyn IssueCreateAttachmentPresenter> {
        None
    }
}
pub type SharedIssueCreateRuntime = Arc<dyn IssueCreateRuntime>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues", post(create))
        .route("/api/issues/", post(create))
}

const BUILT_IN: [&str; 7] = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "cancelled",
];

// Go strings.ToLower uses simple per-rune mapping, not final-sigma context or
// Unicode full lowercase expansion (notably U+0130).
fn normalize_title(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect()
}

#[derive(Debug)]
struct Validated {
    req: Request,
    status: String,
    priority: String,
    assignee: Option<Uuid>,
    parent: Option<Uuid>,
    project: Option<Uuid>,
    labels: Vec<Uuid>,
    start: Option<NaiveDate>,
    due: Option<NaiveDate>,
    origin: Option<Uuid>,
    attachments: Vec<Uuid>,
}
fn parse_uuid(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, field))
}
fn parse_date(raw: &Option<String>, field: &'static str) -> Result<Option<NaiveDate>, ApiError> {
    let Some(raw) = raw.as_ref().filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    super::issue_write::calendar::parse(raw)
        .map(Some)
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, field))
}
fn parse(body: &[u8]) -> Result<Validated, ApiError> {
    let invalid = || error(StatusCode::BAD_REQUEST, "invalid request body");
    let req = decode::request(body).map_err(|_| invalid())?;
    if req.title.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "title is required"));
    }
    let priority = if req.priority.is_empty() {
        "none"
    } else {
        req.priority.as_str()
    };
    if !["urgent", "high", "medium", "low", "none"].contains(&priority) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "priority must be one of: urgent, high, medium, low, none",
        ));
    }
    if req.stage.is_some_and(|s| s < 1) {
        return Err(error(StatusCode::BAD_REQUEST, "stage must be >= 1"));
    }
    let assignee = req
        .assignee_id
        .as_deref()
        .map(|raw| parse_uuid(raw, "invalid assignee_id"))
        .transpose()?;
    let parent = req
        .parent_issue_id
        .as_deref()
        .map(|raw| parse_uuid(raw, "invalid parent_issue_id"))
        .transpose()?;
    let project = req
        .project_id
        .as_deref()
        .map(|raw| parse_uuid(raw, "invalid project_id"))
        .transpose()?;
    let attachments = req
        .attachment_ids
        .iter()
        .map(|raw| parse_uuid(raw, "invalid attachment_ids"))
        .collect::<Result<_, _>>()?;
    let labels = req
        .label_ids
        .iter()
        .map(|raw| parse_uuid(raw, "invalid label_ids"))
        .collect::<Result<_, _>>()?;
    let start = parse_date(
        &req.start_date,
        "invalid start_date format, expected YYYY-MM-DD",
    )?;
    let due = parse_date(
        &req.due_date,
        "invalid due_date format, expected YYYY-MM-DD",
    )?;
    let status = if req.status.is_empty() {
        "todo"
    } else {
        req.status.as_str()
    }
    .trim()
    .to_lowercase();
    let origin = match (&req.origin_type, &req.origin_id) {
        (None, None) => None,
        (Some(kind), Some(raw)) => {
            if kind != "quick_create" {
                return Err(error(StatusCode::BAD_REQUEST, "unsupported origin_type"));
            }
            Some(parse_uuid(raw, "invalid origin_id")?)
        }
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "origin_type and origin_id must be provided together",
            ));
        }
    };
    Ok(Validated {
        status,
        priority: priority.to_owned(),
        assignee,
        parent,
        project,
        labels,
        start,
        due,
        origin,
        attachments,
        req,
    })
}

fn partial(req: &Validated) -> bool {
    // Agent/squad dispatch still requires its own service contract.
    matches!(req.req.assignee_type.as_deref(), Some("agent" | "squad"))
}

#[derive(Default)]
pub(super) struct QuickCreateContext {
    pub(super) kind: String,
    pub(super) workspace: String,
    pub(super) requester: String,
    pub(super) source_context: String,
}

// Decode the stored Go QuickCreateContext contract, including fields not used
// below: a malformed prompt or attachment list also makes Go Unmarshal fail.
// Go accepts null scalar/array entries and case-insensitive struct field names.
pub(super) fn quick_create_context(value: &Value) -> Option<QuickCreateContext> {
    let mut context = QuickCreateContext::default();
    if value.is_null() {
        return Some(context);
    }
    for (key, value) in value.as_object()? {
        // encoding/json's foldName also folds long-s and Kelvin sign to ASCII.
        let key: String = key
            .chars()
            .map(|c| match c {
                'ſ' => 's',
                'K' => 'k',
                _ => c.to_ascii_lowercase(),
            })
            .collect();
        if key == "attachment_ids" {
            if !value.is_null()
                && !value
                    .as_array()?
                    .iter()
                    .all(|v| v.is_null() || v.is_string())
            {
                return None;
            }
        } else if [
            "type",
            "prompt",
            "requester_id",
            "workspace_id",
            "priority",
            "due_date",
            "project_id",
            "squad_id",
            "parent_issue_id",
            "source_context_id",
        ]
        .contains(&key.as_str())
        {
            if value.is_null() {
                continue;
            }
            let text = value.as_str()?;
            match key.as_str() {
                "type" => context.kind = text.to_owned(),
                "workspace_id" => context.workspace = text.to_owned(),
                "requester_id" => context.requester = text.to_owned(),
                "source_context_id" => context.source_context = text.to_owned(),
                _ => {}
            }
        }
    }
    Some(context)
}

// Origin validation belongs inside the create transaction, after the insert,
// like IssueService.Create. Failure rolls back issue, labels and counter; the
// ordinary HTTP create handler maps these service errors to a generic 500.
async fn attach_quick_create_origin(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    origin: Uuid,
    creator_type: &str,
    creator: Uuid,
) -> Result<(), ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create issue");
    let task = tx.query_opt(
        "SELECT t.agent_id,t.context,t.originator_user_id FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id WHERE t.id=$1 AND a.workspace_id=$2",
        &[&origin, &ws],
    ).await.map_err(|_| fail())?.ok_or_else(fail)?;
    if creator_type != "agent" || task.get::<_, Uuid>("agent_id") != creator {
        return Err(fail());
    }
    let stored = task.get::<_, Option<Value>>("context").ok_or_else(fail)?;
    let context = quick_create_context(&stored).ok_or_else(fail)?;
    if context.kind != "quick_create" || Uuid::parse_str(&context.workspace).ok() != Some(ws) {
        return Err(fail());
    }
    if context.source_context.is_empty() {
        return Ok(());
    }
    let source = Uuid::parse_str(&context.source_context).map_err(|_| fail())?;
    let requester = Uuid::parse_str(&context.requester).map_err(|_| fail())?;
    if task.get::<_, Option<Uuid>>("originator_user_id") != Some(requester) {
        return Err(fail());
    }
    let pending = tx.query_opt(
        "SELECT id,captured_by_user_id FROM issue_source_context WHERE workspace_id=$1 AND origin_task_id=$2 AND state='pending' FOR UPDATE",
        &[&ws, &origin],
    ).await.map_err(|_| fail())?.ok_or_else(fail)?;
    if pending.get::<_, Uuid>("id") != source
        || pending.get::<_, Uuid>("captured_by_user_id") != requester
    {
        return Err(fail());
    }
    tx.query_opt(
        "UPDATE issue_source_context SET issue_id=$1,state='attached',attached_at=now() WHERE workspace_id=$2 AND id=$3 AND state='pending' AND origin_task_id=$4 RETURNING id",
        &[&issue, &ws, &source, &origin],
    ).await.map_err(|_| fail())?.ok_or_else(fail)?;
    Ok(())
}

fn stable_attachment_urls(headers: &HeaderMap) -> bool {
    headers
        .get("x-client-capabilities")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim() == "stable_attachment_urls")
        })
}

impl IssueCreateAttachment {
    fn from_row(row: &tokio_postgres::Row) -> Self {
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
            url: row.get("url"),
            content_type: row.get("content_type"),
            size_bytes: row.get("size_bytes"),
            created_at: row.get("created_at"),
        }
    }

    fn response(&self, urls: IssueCreateAttachmentUrls) -> Value {
        json!({
            "id": self.id,
            "workspace_id": self.workspace_id,
            "issue_id": self.issue_id,
            "comment_id": self.comment_id,
            "chat_session_id": self.chat_session_id,
            "chat_message_id": self.chat_message_id,
            "uploader_type": self.uploader_type,
            "uploader_id": self.uploader_id,
            "filename": self.filename,
            "url": self.url,
            "download_url": urls.download_url,
            "markdown_url": urls.markdown_url,
            "content_type": self.content_type,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        })
    }
}

// Go intentionally links uploaded media after issue commit, without bumping
// revision; an unavailable attachment or linking/list error does not make a
// committed create fail (and prompt the client to create a duplicate).
async fn link_attachments(
    db: &tokio_postgres::Client,
    ws: Uuid,
    issue: Uuid,
    ids: &[Uuid],
) -> Vec<IssueCreateAttachment> {
    if ids.is_empty() {
        return Vec::new();
    }
    if db.execute(
        "UPDATE attachment SET issue_id=$1 WHERE workspace_id=$2 AND issue_id IS NULL AND source_context_id IS NULL AND id=ANY($3::uuid[])",
        &[&issue, &ws, &ids],
    ).await.is_err() {
        eprintln!("issue create attachment linking failed for issue {issue}");
        return Vec::new();
    }
    match db.query(
        "SELECT * FROM attachment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at ASC",
        &[&issue, &ws],
    ).await {
        Ok(rows) => rows.iter().map(IssueCreateAttachment::from_row).collect(),
        Err(_) => {
            eprintln!("issue create attachment listing failed for issue {issue}");
            Vec::new()
        }
    }
}

fn property_error(id: &str, message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        GoJson(json!({"code":"invalid_issue_property","property_id":id,"error":message.into()})),
    )
        .into_response()
}

// Lock definitions in UUID order before reading any of them. Catalog writers
// use the same advisory key, so an archive cannot commit during validation.
async fn create_properties(
    tx: &Transaction<'_>,
    ws: Uuid,
    values: Option<&serde_json::Map<String, Value>>,
) -> Result<Value, Response> {
    let mut ids = Vec::new();
    for (raw, value) in values.into_iter().flat_map(|v| v.iter()) {
        let id =
            Uuid::parse_str(raw).map_err(|_| property_error(raw, "property id must be a UUID"))?;
        if ids.iter().any(|(other, _)| *other == id) {
            return Err(property_error(raw, "property was provided more than once"));
        }
        ids.push((id, value));
    }
    ids.sort_by_key(|(id, _)| id.to_string());
    let internal =
        || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create issue").into_response();
    for (id, _) in &ids {
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("prop:{id}")],
        )
        .await
        .map_err(|_| internal())?;
    }
    let mut canonical = serde_json::Map::new();
    for (id, raw) in ids {
        let key = id.to_string();
        let definition = tx
            .query_opt(
                "SELECT name,type,config,archived_at FROM issue_property WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .map_err(|_| internal())?
            .ok_or_else(|| property_error(&key, "property not found in this workspace"))?;
        let name: String = definition.get("name");
        if definition
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
            .is_some()
        {
            return Err(property_error(
                &key,
                format!("property {name:?} is archived and cannot receive new values"),
            ));
        }
        let kind: String = definition.get("type");
        let value = issue_write_value::validate(&kind, &definition.get("config"), Some(raw))
            .map_err(|msg| property_error(&key, msg))?;
        if matches!(kind.as_str(), "actor" | "multi_actor") {
            let refs: Vec<_> = if kind == "actor" {
                vec![value.as_str().expect("validated actor")]
            } else {
                value
                    .as_array()
                    .expect("validated actors")
                    .iter()
                    .map(|v| v.as_str().expect("validated actor"))
                    .collect()
            };
            for reference in refs {
                let member =
                    Uuid::parse_str(reference.strip_prefix("member:").expect("validated member"))
                        .expect("validated UUID");
                let exists = tx
                    .query_opt(
                        "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                        &[&ws, &member],
                    )
                    .await
                    .map_err(|_| internal())?;
                if exists.is_none() {
                    return Err(property_error(
                        &key,
                        format!("{reference:?} does not refer to a member of this workspace"),
                    ));
                }
            }
        }
        canonical.insert(key, value);
    }
    Ok(Value::Object(canonical))
}

pub(super) fn quota_response(policy: IssueCountPolicy) -> Response {
    (StatusCode::PAYMENT_REQUIRED, GoJson(json!({"code":"issue_limit_reached", "limit":policy.limit,"policy_revision":policy.revision,"error":"workspace has reached its issue limit"}))).into_response()
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let parsed = parse(&body)?;
    create_validated(state, ws, user, actor, headers, parsed, None).await
}

pub(super) async fn create_from_comment(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    headers: HeaderMap,
    body: &[u8],
    capture: super::issue_write::source_context::Capture,
) -> Result<Response, ApiError> {
    let mut parsed = parse(body)?;
    // The source issue, not a client-provided parent, determines lineage.
    parsed.parent = Some(capture.source_issue);
    create_validated(
        state,
        ws,
        user,
        Actor::Human,
        headers,
        parsed,
        Some(capture),
    )
    .await
}

async fn create_validated(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    headers: HeaderMap,
    parsed: Validated,
    capture: Option<super::issue_write::source_context::Capture>,
) -> Result<Response, ApiError> {
    let (creator_type, creator_id, task) = write_actor(actor, user)?;
    if partial(&parsed) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue create runtime unavailable",
        ));
    }
    if parsed.req.assignee_type.is_some() != parsed.assignee.is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "assignee_type and assignee_id must be provided together",
        ));
    }
    if let Some(kind) = &parsed.req.assignee_type
        && kind != "member"
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "assignee_type must be 'member', 'agent', or 'squad'",
        ));
    }
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let runtime = state.issue_create_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue create runtime unavailable",
        )
    })?;
    // Fail closed before the insert if the real storage/URL policy is missing.
    let presenter = if parsed.attachments.is_empty() {
        None
    } else {
        Some(runtime.attachment_presenter().ok_or_else(|| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "issue create runtime unavailable",
            )
        })?)
    };
    let policy = runtime.count_policy(ws).await.map_err(|_| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue count policy unavailable",
        )
    })?;
    if policy.limit.is_some_and(|limit| limit <= 0) {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "invalid issue count policy",
        ));
    }
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create issue"))?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create issue");
    let built_in = BUILT_IN.contains(&parsed.status.as_str());
    if !built_in {
        // Go reports unknown status with the workspace's display-ordered key
        // labels, before its create transaction. Archive after this check is
        // handled under the shared status lock below.
        let status = db
            .query_opt(
                "SELECT archived_at FROM issue_status WHERE workspace_id=$1 AND key=$2",
                &[&ws, &parsed.status],
            )
            .await
            .map_err(|_| fail())?;
        if status.as_ref().is_none_or(|row| {
            row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
                .is_some()
        }) {
            let rows = db.query(
                "SELECT key,name,is_system FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END,position,CASE WHEN is_system THEN 0 ELSE 1 END,CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END,key",
                &[&ws],
            ).await.map_err(|_| fail())?;
            let mut seen = HashSet::new();
            let mut allowed: Vec<String> = rows
                .iter()
                .map(|row| {
                    let key: String = row.get("key");
                    let name: String = row.get("name");
                    let system: bool = row.get("is_system");
                    seen.insert(key.clone());
                    if system || name.trim().is_empty() {
                        key
                    } else {
                        format!("{key} ({name})")
                    }
                })
                .collect();
            for key in BUILT_IN {
                if !seen.contains(key) {
                    allowed.push(key.to_owned());
                }
            }
            return Ok((StatusCode::BAD_REQUEST, GoJson(json!({"error":format!("invalid status {:?}; valid values: {}", parsed.status, allowed.join(", "))}))).into_response());
        }
    }
    // Frozen handler stamps implicit agent_create only after reloading the
    // trusted task and checking its agent. Missing/mismatched task is not fatal.
    let mut origin_id = parsed.origin;
    let mut origin_type = parsed.origin.map(|_| "quick_create");
    if origin_id.is_none()
        && creator_type == "agent"
        && let Ok(task_id) = Uuid::parse_str(&task)
        && let Ok(Some(row)) = db
            .query_opt(
                "SELECT agent_id FROM agent_task_queue WHERE id=$1",
                &[&task_id],
            )
            .await
        && row.get::<_, Uuid>("agent_id") == creator_id
    {
        origin_id = Some(task_id);
        origin_type = Some("agent_create");
    }
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to create issue"))?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&creator_type,&creator_id.to_string(),&task]).await.map_err(|_|fail())?;
    super::issue_write_structure::lock_graph(&tx, ws).await?;
    let properties = match create_properties(&tx, ws, parsed.req.properties.as_ref()).await {
        Ok(properties) => properties,
        Err(response) => return Ok(response),
    };
    if !built_in {
        tx.execute(
            "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))",
            &[&format!("{ws}:issue_status")],
        )
        .await
        .map_err(|_| fail())?;
    }
    let status = tx
        .query_opt(
            "SELECT archived_at FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&ws, &parsed.status],
        )
        .await
        .map_err(|_| fail())?;
    if !built_in
        && status.as_ref().is_none_or(|row| {
            row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(0)
                .is_some()
        })
    {
        return Err(error(
            StatusCode::CONFLICT,
            "the target status was archived while this request was in flight; reload the status list and retry",
        ));
    }
    let mut project = parsed.project;
    if let Some(parent) = parsed.parent {
        let row = tx
            .query_opt(
                "SELECT project_id FROM issue WHERE id=$1 AND workspace_id=$2 FOR KEY SHARE",
                &[&parent, &ws],
            )
            .await
            .map_err(|_| fail())?
            .ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "parent issue not found in this workspace",
                )
            })?;
        if project.is_none() {
            project = row.get("project_id");
        }
    }
    let handoff = match issue_write_update::workflow::create(
        &tx,
        ws,
        project,
        &parsed.status,
        parsed.req.assignee_type.is_some(),
        (creator_type, creator_id),
    )
    .await?
    {
        Ok(handoff) => handoff,
        Err(response) => return Ok(response),
    };
    let assignee_type = handoff.as_ref().map_or_else(
        || parsed.req.assignee_type.clone(),
        |h| Some(h.kind.clone()),
    );
    let assignee = handoff.as_ref().map_or(parsed.assignee, |h| Some(h.id));
    if let Some(assignee) = assignee {
        let member = tx
            .query_opt(
                "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2",
                &[&ws, &assignee],
            )
            .await
            .map_err(|_| fail())?;
        if member.is_none() {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "assignee_id does not refer to a member of this workspace",
            ));
        }
    }
    let mut labels = Vec::new();
    let mut seen = HashSet::new();
    for id in parsed.labels.iter().copied().filter(|id| seen.insert(*id)) {
        let row=tx.query_opt("SELECT * FROM issue_label WHERE id=$1 AND workspace_id=$2 AND resource_type='issue' FOR SHARE", &[&id,&ws]).await.map_err(|_|fail())?
            .ok_or_else(||error(StatusCode::BAD_REQUEST,"one or more labels not found in this workspace"))?;
        labels.push(row);
    }
    let normalized = normalize_title(&parsed.req.title);
    if !normalized.is_empty() {
        let lock = format!(
            "issue-active-duplicate|{ws}|{}|{}|{normalized}",
            project.map(|id| id.to_string()).unwrap_or_default(),
            parsed.parent.map(|id| id.to_string()).unwrap_or_default()
        );
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&lock],
        )
        .await
        .map_err(|_| fail())?;
        if !parsed.req.allow_duplicate {
            let duplicate = tx
                .query_opt(
                    include_str!("issue_write_create_duplicate.sql"),
                    &[&ws, &project, &parsed.parent, &normalized],
                )
                .await
                .map_err(|_| fail())?;
            if let Some(duplicate) = duplicate {
                let issue = issue_write_update::response(&tx, &duplicate, ws).await?;
                let identifier = issue["identifier"].as_str().unwrap_or_default();
                let title = issue["title"].as_str().unwrap_or_default();
                let status = issue["status"].as_str().unwrap_or_default();
                return Ok((StatusCode::CONFLICT,GoJson(json!({"code":"active_duplicate_issue","error":format!("Active duplicate issue exists: {identifier} {title} (status: {status}). Set allow_duplicate=true or use --allow-duplicate to create another."),"issue":issue}))).into_response());
            }
        }
    }
    let number:i32=tx.query_one("UPDATE workspace SET issue_counter=issue_counter+1 WHERE id=$1 RETURNING issue_counter", &[&ws]).await.map_err(|_|fail())?.get(0);
    if let Some(limit) = policy.limit {
        let count:i64=tx.query_one("SELECT COUNT(*)::bigint FROM (SELECT 1 FROM issue WHERE workspace_id=$1 LIMIT $2) bounded_issues", &[&ws,&limit]).await.map_err(|_|fail())?.get(0);
        if count >= limit {
            return Ok(quota_response(policy));
        }
    }
    let position: f64 = tx
        .query_one(
            "SELECT COALESCE(MIN(position),0)-1 FROM issue WHERE workspace_id=$1 AND status=$2",
            &[&ws, &parsed.status],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    let id = Uuid::now_v7();
    let row = tx
        .query_one(
            include_str!("issue_write_create.sql"),
            &[
                &id,
                &ws,
                &parsed.req.title,
                &parsed.req.description,
                &parsed.status,
                &parsed.priority,
                &assignee_type,
                &assignee,
                &creator_type,
                &creator_id,
                &parsed.parent,
                &position,
                &parsed.start,
                &parsed.due,
                &number,
                &project,
                &parsed.req.stage,
                &origin_type,
                &origin_id,
                &properties,
            ],
        )
        .await;
    let row = match row {
        Ok(row) => row,
        Err(err)
            if err.as_db_error().is_some_and(|db| {
                db.code() == &SqlState::CHECK_VIOLATION
                    && db.constraint() == Some("issue_properties_size_limit")
            }) =>
        {
            return Ok((StatusCode::BAD_REQUEST, GoJson(json!({"code":"issue_properties_too_large","error":"issue properties exceed the 16KB size limit"}))).into_response());
        }
        Err(_) => return Err(fail()),
    };
    if let Some(origin) = parsed.origin {
        attach_quick_create_origin(&tx, ws, id, origin, creator_type, creator_id).await?;
    }
    for label in &labels {
        let label_id: Uuid = label.get("id");
        tx.execute(
            "INSERT INTO issue_to_label(issue_id,label_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
            &[&id, &label_id],
        )
        .await
        .map_err(|_| fail())?;
    }
    if let Some(capture) = capture {
        tx.execute(
            "INSERT INTO issue_source_context(id,workspace_id,issue_id,source_issue_id,anchor_comment_id,captured_by_user_id,snapshot_version,snapshot,capture_digest,state,captured_at,attached_at) VALUES($1,$2,$3,$4,$5,$6,1,$7,$8,'attached',$9,$9)",
            &[&capture.id,&ws,&id,&capture.source_issue,&capture.anchor,&user,&capture.snapshot,&capture.digest,&capture.at],
        ).await.map_err(|_|fail())?;
    }
    let label_values: Vec<Value> = labels.iter().map(issue_write_labels::label).collect();
    let mut issue = issue_write_update::response(&tx, &row, ws).await?;
    issue["labels"] = json!(label_values);
    tx.commit().await.map_err(|_| fail())?;
    if let Some(presenter) = presenter {
        let attachments = link_attachments(&db, ws, id, &parsed.attachments).await;
        if !attachments.is_empty() {
            let stable = stable_attachment_urls(&headers);
            issue["attachments"] = json!(
                attachments
                    .iter()
                    .map(|attachment| { attachment.response(presenter.urls(attachment, stable)) })
                    .collect::<Vec<_>>()
            );
        }
    }
    let event = IssueCreatedEvent {
        workspace_id: ws,
        actor_type: creator_type,
        actor_id: creator_id,
        payload: json!({"issue":issue}),
    };
    sink.publish_issue_created(event.clone());
    runtime.created(&event);
    Ok((StatusCode::CREATED, GoJson(issue)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_origin_pairs_and_go_context_decoding() {
        for (body, message) in [
            (
                json!({"title":"x","origin_id":Uuid::nil()}),
                "origin_type and origin_id must be provided together",
            ),
            (
                json!({"title":"x","origin_type":"quick_create"}),
                "origin_type and origin_id must be provided together",
            ),
            (
                json!({"title":"x","origin_type":"agent_create","origin_id":Uuid::nil()}),
                "unsupported origin_type",
            ),
            (
                json!({"title":"x","origin_type":"quick_create","origin_id":"bad"}),
                "invalid origin_id",
            ),
        ] {
            assert_eq!(
                parse(&serde_json::to_vec(&body).unwrap()).unwrap_err().1,
                message
            );
        }
        let valid = parse(br#"{"title":"x","origin_type":"quick_create","origin_id":"00000000-0000-0000-0000-000000000000"}"#).unwrap();
        assert_eq!(valid.origin, Some(Uuid::nil()));
        assert!(!partial(&valid));
        let mut headers = HeaderMap::new();
        assert!(!stable_attachment_urls(&headers));
        headers.insert(
            "x-client-capabilities",
            "signed, stable_attachment_urls ".parse().unwrap(),
        );
        assert!(stable_attachment_urls(&headers));
        headers.insert(
            "x-client-capabilities",
            "Stable_Attachment_Urls".parse().unwrap(),
        );
        assert!(!stable_attachment_urls(&headers));
        assert!(
            parse(br#"{"title":"x","origin_type":null,"origin_id":null}"#)
                .unwrap()
                .origin
                .is_none()
        );
        let context = quick_create_context(&json!({
            "TYPE":"quick_create","WORKSPACE_ID":"workspace","requester_id":"requester",
            "ſource_context_id":"source","prompt":null,"attachment_ids":[null,"id"],
            "unknown_field":{},
        }))
        .unwrap();
        assert_eq!(context.kind, "quick_create");
        assert_eq!(context.workspace, "workspace");
        assert_eq!(context.requester, "requester");
        assert_eq!(context.source_context, "source");
        let context = quick_create_context(&json!({"Type":"quick_create","type":null})).unwrap();
        assert_eq!(context.kind, "quick_create");
        for bad in [
            json!(12),
            json!([]),
            json!({"prompt":false}),
            json!({"attachment_ids":{}}),
            json!({"attachment_ids":[12]}),
            json!({"due_date":12}),
        ] {
            assert!(quick_create_context(&bad).is_none(), "{bad}");
        }
        assert!(quick_create_context(&Value::Null).unwrap().kind.is_empty());
    }

    #[test]
    fn validates_create_fields_and_duplicate_json_members() {
        // Go decodes one JSON value and ignores trailing bytes. Only a
        // malformed first value is a 400.
        assert_eq!(
            parse(br#"{"title":"x","title":"y"}"#).unwrap_err().1,
            "invalid request body"
        );
        assert_eq!(normalize_title(" \tİ ΟΣ\u{00a0}X\n"), "i οσ x");
        assert_eq!(parse(br#"{}"#).unwrap_err().1, "title is required");
        assert_eq!(
            parse(br#"{"title":"x","title":"y"}"#).unwrap_err().1,
            "invalid request body"
        );
        assert_eq!(
            parse(br#"{"title":"x","properties":{"a":1,"a":2}}"#)
                .unwrap_err()
                .1,
            "invalid request body"
        );
        assert_eq!(
            parse(br#"{"title":"x","due_date":"2025-02-30"}"#)
                .unwrap_err()
                .1,
            "invalid due_date format, expected YYYY-MM-DD"
        );
        assert_eq!(
            parse(br#"{"title":"x","stage":0}"#).unwrap_err().1,
            "stage must be >= 1"
        );
        assert_eq!(
            parse(br#"{"title":"x","priority":"wrong"}"#).unwrap_err().1,
            "priority must be one of: urgent, high, medium, low, none"
        );
        // Go's encoding/json leaves an explicit null at the zero value for a
        // non-pointer field, so the create path must accept the same shape.
        let value = parse(br#"{"title":"Hello","status":"TODO","priority":null,"attachment_ids":null,"label_ids":null,"allow_duplicate":null,"properties":null,"description":null}"#)
            .expect("explicit nulls stay decodable");
        assert_eq!(value.status, "todo");
        assert_eq!(value.priority, "none");
        assert!(value.labels.is_empty());
        assert!(value.req.attachment_ids.is_empty());
        assert!(!value.req.allow_duplicate);
        assert_eq!(
            parse(br#"{"title":null}"#).unwrap_err().1,
            "title is required"
        );
        assert_eq!(
            parse(br#"{"title":"x","properties":["a"]}"#).unwrap_err().1,
            "invalid request body"
        );
        assert_eq!(
            parse(br#"{"title":"x","attachment_ids":["nope"]}"#)
                .unwrap_err()
                .1,
            "invalid attachment_ids"
        );
        assert_eq!(parse(br#"{}"#).unwrap_err().1, "title is required");
        assert_eq!(parse(br#"{"title":"x"} trailing"#).unwrap().req.title, "x");
    }
}
