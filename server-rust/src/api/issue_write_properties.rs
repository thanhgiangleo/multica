//! Typed single-key property values; definitions share Go's advisory lock.
use super::{
    ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueAssignmentEvent, WriteError, overflowing_number, raw_field, write_actor},
    issue_write_comment_decode::normalized_strings,
    issue_write_value,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::put,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/issues/{id}/properties/{propertyId}",
        put(set).delete(unset),
    )
}

fn parse_value(raw: Option<&[u8]>) -> Result<Option<Value>, String> {
    let Some(raw) = raw else { return Ok(None) };
    let normalized = normalized_strings(raw);
    if let Some(number) = overflowing_number(std::str::from_utf8(&normalized).unwrap()) {
        return Err(format!(
            "value must be valid JSON: json: cannot unmarshal number {number} into Go value of type float64"
        ));
    }
    serde_json::from_slice(&normalized)
        .map(Some)
        .map_err(|_| "value must be valid JSON".to_owned())
}

async fn set(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, property)): Path<(String, String)>,
    body: Bytes,
) -> Result<GoJson<Value>, WriteError> {
    let property = Uuid::parse_str(&property)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid property id"))?;
    let request = raw_field::raw_field(&body, "value")
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    // The outer option distinguishes SET-with-missing-value from DELETE. Go
    // validates the selected RawMessage only after locking/loading its definition.
    mutate(state, ws, user, actor, raw, property, Some(request)).await
}

async fn unset(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, property)): Path<(String, String)>,
) -> Result<GoJson<Value>, WriteError> {
    let property = Uuid::parse_str(&property)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid property id"))?;
    mutate(state, ws, user, actor, raw, property, None).await
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    property_id: Uuid,
    request: Option<Option<Vec<u8>>>,
) -> Result<GoJson<Value>, WriteError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let fail = if request.is_some() {
        "failed to set property"
    } else {
        "failed to unset property"
    };
    let (actor_type, actor_id, task_id) = write_actor(actor, user)?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,fail))?;
    if request.is_some() {
        // Exact shared key/hash used by Go withPropertyLock. Definition edits
        // must use this same lock before their usage census, never a Rust hash.
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            &[&format!("prop:{property_id}")],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    }
    let def = tx.query_opt("SELECT name,type,config,archived_at FROM issue_property WHERE id=$1 AND workspace_id=$2", &[&property_id,&ws]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,fail))?.ok_or_else(||error(StatusCode::NOT_FOUND,"property not found"))?;
    let key = property_id.to_string();
    let updated = if let Some(request) = request {
        if def.get::<_, Option<DateTime<Utc>>>("archived_at").is_some() {
            return Err(WriteError(
                StatusCode::BAD_REQUEST,
                format!(
                    "property {:?} is archived and cannot receive new values",
                    def.get::<_, String>("name")
                ),
            ));
        }
        let kind: String = def.get("type");
        let parsed = parse_value(request.as_deref())
            .map_err(|msg| WriteError(StatusCode::BAD_REQUEST, msg))?;
        let value =
            issue_write_value::validate(&kind, &def.get::<_, Value>("config"), parsed.as_ref())
                .map_err(|msg| WriteError(StatusCode::BAD_REQUEST, msg))?;
        if kind == "actor" || kind == "multi_actor" {
            let refs = if kind == "actor" {
                vec![value.as_str().unwrap()]
            } else {
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect()
            };
            for reference in refs {
                let member = Uuid::parse_str(reference.strip_prefix("member:").unwrap()).unwrap();
                if tx
                    .query_opt(
                        "SELECT 1 FROM member WHERE user_id=$1 AND workspace_id=$2",
                        &[&member, &ws],
                    )
                    .await
                    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?
                    .is_none()
                {
                    return Err(WriteError(
                        StatusCode::BAD_REQUEST,
                        format!("{reference:?} does not refer to a member of this workspace"),
                    ));
                }
            }
        }
        tx.query_opt(
            include_str!("issue_write_property_set.sql"),
            &[&id, &ws, &key, &value],
        )
        .await
    } else {
        // Archived definitions still allow cleanup. Unknown/foreign ids do not.
        tx.query_opt(
            include_str!("issue_write_property_unset.sql"),
            &[&id, &ws, &key],
        )
        .await
    }
    .map_err(|err| {
        if err
            .as_db_error()
            .is_some_and(|e| e.code() == &SqlState::CHECK_VIOLATION)
        {
            error(
                StatusCode::BAD_REQUEST,
                "issue properties exceed the 16KB size limit",
            )
        } else {
            error(StatusCode::INTERNAL_SERVER_ERROR, fail)
        }
    })?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let properties: Value = updated.get("properties");
    let revision: i64 = updated.get("revision");
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    // Unlike metadata/labels, Go emits the properties event on a no-op, too.
    sink.publish_assignment_changed(IssueAssignmentEvent {
        event_type: "issue_properties:changed",
        workspace_id: ws,
        actor_type,
        actor_id,
        payload: json!({"issue_id":id,"properties":properties,"issue_revision":revision}),
    });
    Ok(GoJson(
        json!({"properties":properties,"issue_revision":revision}),
    ))
}
