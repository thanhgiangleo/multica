//! Workspace-scoped metadata writes. Registration requires the runtime event sink.
#[path = "issue_write_calendar.rs"]
pub(super) mod calendar;
#[path = "issue_write_raw_field.rs"]
pub(super) mod raw_field;
#[path = "issue_write_scalar_decode.rs"]
pub(super) mod scalar_decode;
#[path = "issue_write_source_context.rs"]
pub(super) mod source_context;

use super::{
    ApiError, ApiState, GoJson, Identity, actor::Actor, catalog::WorkspaceId, error,
    issue_read::load_issue,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::put,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

/// Implemented by the integrator's real workspace event bus. Never use a no-op sink.
#[derive(Clone, Debug, PartialEq)]
pub struct IssueMetadataEvent {
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub issue_id: Uuid,
    pub metadata: Value,
    pub issue_revision: i64,
}

pub use super::issue_write_comment::{
    CommentCancelledBatch, CommentCancelledTask, CommentChangedAction, CommentCreatedAction,
    CommentFeedback, CommentReplayInput, CommentSupplement, CommentTriggerOutcome,
    IssueCommentChangedEvent, IssueCommentCreatedEvent, IssueCommentLegacyRuntime,
    IssueCommentRuntime, SharedIssueCommentRuntime,
};

pub use super::issue_write_create::{
    IssueCountPolicy, IssueCreateAttachment, IssueCreateAttachmentPresenter,
    IssueCreateAttachmentUrls, IssueCreateRuntime, IssueCreatedEvent, SharedIssueCreateRuntime,
};

pub use super::issue_write_delete::{
    IssueDeletePreparation, IssueDeleteRuntime, IssueDeletedEvent, SharedIssueDeleteRuntime,
};

pub use super::issue_write_pull_request::{
    IssuePullRequestActivity, IssuePullRequestRuntime, SharedIssuePullRequestRuntime,
};
pub use super::issue_write_quick_create::{
    IssueQuickCreateAdmission, IssueQuickCreatePrepared, IssueQuickCreateQueued,
    IssueQuickCreateReadinessError, IssueQuickCreateRuntime,
};
pub use super::issue_write_structure::{IssueChildRuntime, SharedIssueChildRuntime};

pub trait IssueEventSink: Send + Sync {
    fn publish_issue_deleted(&self, event: IssueDeletedEvent);
    fn publish_comment_created(&self, event: IssueCommentCreatedEvent);
    fn publish_comment_changed(&self, event: IssueCommentChangedEvent);
    fn publish_metadata_changed(&self, event: IssueMetadataEvent);
    fn publish_reaction_changed(&self, event: IssueReactionEvent);
    fn publish_assignment_changed(&self, event: IssueAssignmentEvent);
    fn publish_subscriber_changed(&self, event: IssueSubscriberEvent);
    fn publish_issue_updated(&self, event: IssueUpdatedEvent);
    fn publish_issue_created(&self, event: IssueCreatedEvent);
    fn publish_comment_resolution(&self, event: IssueCommentResolutionEvent);
    fn publish_wakeup_activity(&self, event: IssueWakeupActivityEvent);
    fn publish_task_cancelled(&self, event: IssueTaskCancelledEvent);
    fn request_wakeup_dispatch(&self, request: IssueWakeupDispatchRequest);
}

/// A committed receipt is durable; the scheduler must also be nudged to dispatch now.
/// Production wiring must use the real wakeup dispatcher, not an event-only sink.
#[derive(Clone, Debug, PartialEq)]
pub struct IssueWakeupDispatchRequest {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub wakeup_id: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueTaskCancelledEvent {
    pub workspace_id: Uuid,
    pub task_id: Uuid,
    pub agent_id: Uuid,
    pub issue_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueWakeupActivityEvent {
    pub workspace_id: Uuid,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueCommentResolutionEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

/// Catalog assignments only. Definitions remain owned by account/catalog.
#[derive(Clone, Debug, PartialEq)]
pub struct IssueAssignmentEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueUpdatedEvent {
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueSubscriberEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Debug)]
pub(super) struct WriteError(pub StatusCode, pub String);

impl From<ApiError> for WriteError {
    fn from(value: ApiError) -> Self {
        Self(value.0, value.1)
    }
}

impl axum::response::IntoResponse for WriteError {
    fn into_response(self) -> axum::response::Response {
        let mut response = GoJson(json!({"error":self.1})).into_response();
        *response.status_mut() = self.0;
        response
    }
}

pub(super) fn write_actor(
    actor: Actor,
    user: Uuid,
) -> Result<(&'static str, Uuid, String), ApiError> {
    match actor {
        Actor::Task(task) => Ok(("agent", task.agent, task.task.to_string())),
        Actor::Human | Actor::Cloud => Ok(("member", user, String::new())),
        Actor::Daemon => Err(error(StatusCode::FORBIDDEN, "invalid actor")),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueReactionEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

pub type SharedIssueEventSink = Arc<dyn IssueEventSink>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .merge(super::issue_write_comment::routes())
        .merge(super::issue_write_comment_mutate::routes())
        .merge(super::issue_write_reaction::routes())
        .merge(super::issue_write_issue_reaction::routes())
        .merge(super::issue_write_resolution::routes())
        .merge(super::issue_write_wakeup::routes())
        .merge(super::issue_write_manage::routes())
        .merge(super::issue_write_create_wakeup::routes())
        .merge(super::issue_write_labels::routes())
        .merge(super::issue_write_properties::routes())
        .merge(super::issue_write_subscriber::routes())
        .merge(super::issue_write_update::routes())
        .merge(super::issue_write_batch::routes())
        .merge(super::issue_write_move::routes())
        .merge(super::issue_write_create::routes())
        .merge(super::issue_write_quick_create::routes())
        .merge(source_context::routes())
        .merge(super::issue_write_delete::routes())
        .merge(super::issue_write_pull_request::routes())
        .route(
            "/api/issues/{id}/metadata/{key}",
            put(set_key).delete(delete_key),
        )
}

fn validate_key(key: &str) -> Result<(), ApiError> {
    let bytes = key.as_bytes();
    if bytes.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "key is required"));
    }
    if bytes.len() > 64
        || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        || !bytes[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "key must match ^[a-zA-Z_][a-zA-Z0-9_.-]{0,63}$",
        ));
    }
    Ok(())
}

pub(super) fn overflowing_number(raw: &str) -> Option<&str> {
    let bytes = raw.as_bytes();
    let (mut index, mut quoted) = (0, false);
    while index < bytes.len() {
        match bytes[index] {
            b'"' => quoted = !quoted,
            b'\\' if quoted => index += 1,
            b'-' | b'0'..=b'9' if !quoted => {
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'-' | b'+' | b'.' | b'0'..=b'9' | b'e' | b'E')
                {
                    index += 1;
                }
                let number = &raw[start..index];
                if number.parse::<f64>().is_ok_and(|value| !value.is_finite()) {
                    return Some(number);
                }
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn validate_value(body: &[u8]) -> Result<Vec<u8>, WriteError> {
    let bad = |message: &str| WriteError(StatusCode::BAD_REQUEST, message.to_owned());
    let raw = scalar_decode::metadata(body)
        .map_err(|_| bad("invalid request body"))?
        .ok_or_else(|| bad("value is required"))?;
    // Go validates RawMessage through an interface{} (float64), but sends the
    // original number to JSONB, not the rounded float. Preserve both behaviors.
    let normalized = super::issue_write_comment_decode::normalized_strings(&raw);
    if let Some(number) = overflowing_number(std::str::from_utf8(&normalized).unwrap()) {
        return Err(bad(&format!(
            "value must be valid JSON: json: cannot unmarshal number {number} into Go value of type float64"
        )));
    }
    let parsed: Value =
        serde_json::from_slice(&normalized).map_err(|_| bad("invalid request body"))?;
    match parsed {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(raw),
        Value::Null => Err(bad("value cannot be null (use DELETE to remove a key)")),
        _ => Err(bad("value must be a primitive: string, number, or bool")),
    }
}

async fn set_key(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, key)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<GoJson<Value>, WriteError> {
    validate_key(&key)?;
    let value = validate_value(&body)?;
    mutate(state, ws, user, actor, raw, key, Some(value))
        .await
        .map_err(Into::into)
}

async fn delete_key(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, key)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    validate_key(&key)?;
    mutate(state, ws, user, actor, raw, key, None).await
}

// Go decodes the JSONB metadata through map[string]any before publishing or
// responding. Its decoder turns every JSON number into float64, while the
// stored JSONB retains the original precision for equality and filtering.
pub(super) fn metadata_projection(value: Value) -> Value {
    fn convert(value: Value) -> Option<Value> {
        match value {
            Value::Number(number) => {
                let float = number.to_string().parse::<f64>().ok()?;
                if !float.is_finite() {
                    return None;
                }
                // Reparse the rounded decimal so integer-shaped values keep
                // Go's integer JSON shape, including values above 2^53.
                float.to_string().parse().ok().map(Value::Number)
            }
            Value::Object(map) => Some(Value::Object(
                map.into_iter()
                    .map(|(key, value)| Some((key, convert(value)?)))
                    .collect::<Option<_>>()?,
            )),
            Value::Array(items) => Some(Value::Array(
                items.into_iter().map(convert).collect::<Option<_>>()?,
            )),
            other => Some(other),
        }
    }

    if value.is_object() {
        convert(value).unwrap_or_else(|| json!({}))
    } else {
        json!({})
    }
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    key: String,
    value: Option<Vec<u8>>,
) -> Result<GoJson<Value>, ApiError> {
    // The route cannot be installed without a real bus. An unexpected missing
    // dependency is a 503, never a successful write without the event.
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let mut db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load issue metadata",
        )
    })?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let (actor_type, actor_id, task_id) = match actor {
        Actor::Task(task) => ("agent", task.agent, task.task.to_string()),
        // Cloud PAT, if authorized for this endpoint, acts for its verified user.
        Actor::Human | Actor::Cloud => ("member", user, String::new()),
        Actor::Daemon => return Err(error(StatusCode::FORBIDDEN, "invalid actor")),
    };
    let tx = db.transaction().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to set metadata key",
        )
    })?;
    tx.execute(
        "SELECT set_config('multica.actor_type',$1,true), set_config('multica.actor_id',$2,true), set_config('multica.source_task_id',$3,true)",
        &[&actor_type, &actor_id.to_string(), &task_id],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to set metadata key"))?;
    // Serialize the cap with concurrent metadata writes; no whole-blob update.
    let current = tx.query_opt(
        "SELECT metadata, revision FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
        &[&id, &ws],
    ).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load issue metadata"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let metadata: Value = current.get("metadata");
    let existing = metadata.as_object().cloned().unwrap_or_default();
    if value.is_some() && !existing.contains_key(&key) && existing.len() >= 50 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "metadata cannot exceed 50 keys",
        ));
    }
    // PostgreSQL compares the raw JSONB values. Converting a large number to
    // serde_json::Number first would round it and wrongly classify a write as a no-op.
    if value.is_none() && !existing.contains_key(&key) {
        tx.commit().await.map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load issue metadata",
            )
        })?;
        return Ok(GoJson(
            json!({"metadata": metadata_projection(metadata), "issue_revision": current.get::<_, i64>("revision")}),
        ));
    }
    let is_set = value.is_some();
    let result = if let Some(v) = value {
        tx.query_opt(
            "UPDATE issue SET metadata=jsonb_set(metadata,ARRAY[$3::text],convert_from($4::bytea,'UTF8')::jsonb),revision=revision+1,last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()),updated_at=now() WHERE id=$1 AND workspace_id=$2 AND metadata->$3::text IS DISTINCT FROM convert_from($4::bytea,'UTF8')::jsonb RETURNING metadata,revision",
            &[&id, &ws, &key, &v],
        ).await
    } else {
        tx.query_opt(
            "UPDATE issue SET metadata=metadata-$3::text,revision=revision+1,last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()),updated_at=now() WHERE id=$1 AND workspace_id=$2 AND metadata ? $3::text RETURNING metadata,revision",
            &[&id, &ws, &key],
        ).await
    }.map_err(|err| {
        if err.as_db_error().is_some_and(|e| e.code() == &SqlState::CHECK_VIOLATION) {
            error(StatusCode::BAD_REQUEST, "metadata exceeds the 8KB size limit")
        } else if is_set {
            error(StatusCode::INTERNAL_SERVER_ERROR, "failed to set metadata key")
        } else {
            error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete metadata key")
        }
    })?;
    let Some(result) = result else {
        tx.commit().await.map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load issue metadata",
            )
        })?;
        return Ok(GoJson(
            json!({"metadata": metadata_projection(metadata), "issue_revision": current.get::<_, i64>("revision")}),
        ));
    };
    let metadata = metadata_projection(result.get("metadata"));
    let revision: i64 = result.get("revision");
    tx.commit().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to set metadata key",
        )
    })?;
    sink.publish_metadata_changed(IssueMetadataEvent {
        workspace_id: ws,
        actor_type,
        actor_id,
        issue_id: id,
        metadata: metadata.clone(),
        issue_revision: revision,
    });
    Ok(GoJson(
        json!({"metadata":metadata,"issue_revision":revision}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_public_projection_matches_go_oracle() {
        let cases: Value = serde_json::from_str(include_str!(
            "../../tests/issue_write_metadata_projection_oracle.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let raw = case["raw"].as_str().unwrap();
            assert_eq!(
                metadata_projection(serde_json::from_str(raw).unwrap()),
                case["expected"],
                "{raw}"
            );
        }
    }

    #[test]
    fn keys_and_values_match_frozen_go_contract() {
        for good in ["a", "_key", "p.r-9", &format!("a{}", "b".repeat(63))] {
            assert!(validate_key(good).is_ok());
        }
        for bad in ["", "1a", "a b", "a/../b", &format!("a{}", "b".repeat(64))] {
            assert!(validate_key(bad).is_err());
        }
        for good in [
            br#"{"value":0}"#.as_slice(),
            br#"{"value":true}"#,
            br#"{"value":"hello"}"#,
        ] {
            assert!(validate_value(good).is_ok());
        }
        for bad in [
            br#"{"value":null}"#.as_slice(),
            br#"{"value":[]}"#,
            br#"{"value":{}}"#,
            br#"{}"#,
            br#"garbage"#,
        ] {
            assert!(validate_value(bad).is_err());
        }
    }
}
