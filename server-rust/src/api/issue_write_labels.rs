//! Issue-to-label assignments, not catalog definitions.
use super::issue_write_comment_decode::{decode, folded};
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueAssignmentEvent, write_actor},
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::{
    Deserialize,
    de::{self, MapAccess, Visitor},
};
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/labels", post(attach))
        .route(
            "/api/issues/{id}/labels/{labelId}",
            axum::routing::delete(detach),
        )
}

#[derive(Default)]
struct LabelRequest {
    label_id: String,
}

impl<'de> Deserialize<'de> for LabelRequest {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RequestVisitor;
        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = LabelRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(LabelRequest::default())
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut request = LabelRequest::default();
                while let Some(key) = map.next_key::<String>()? {
                    if folded(&key) == "label_id" {
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            request.label_id = value;
                        }
                    } else {
                        map.next_value::<de::IgnoredAny>()?;
                    }
                }
                Ok(request)
            }
        }
        deserializer.deserialize_any(RequestVisitor)
    }
}

fn parse_label(body: &[u8]) -> Result<String, ApiError> {
    let request: LabelRequest =
        decode(body).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if request.label_id.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "label_id is required"));
    }
    Ok(request.label_id)
}

pub(super) fn label(row: &Row) -> Value {
    json!({
        "id":row.get::<_,Uuid>("id"),
        "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "resource_type":row.get::<_,String>("resource_type"),
        "name":row.get::<_,String>("name"),
        "description":row.get::<_,String>("description"),
        "color":row.get::<_,String>("color"),
        "usage_count":0,
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
    })
}

async fn issue_and_label(
    tx: &Transaction<'_>,
    issue: Uuid,
    ws: Uuid,
    label: Uuid,
    fail: &'static str,
) -> Result<(), ApiError> {
    // Lock order: issue, then label. Both checks are repeated inside the write
    // transaction so a deleted/foreign definition can never become an assignment.
    let live = tx
        .query_opt(
            "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",
            &[&issue, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    if live.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let row = tx
        .query_opt(
            "SELECT resource_type FROM issue_label WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[&label, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    let row = row.ok_or_else(|| error(StatusCode::NOT_FOUND, "label not found"))?;
    if row.get::<_, String>(0) != "issue" {
        return Err(error(StatusCode::NOT_FOUND, "issue label not found"));
    }
    Ok(())
}

async fn attach(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<GoJson<Value>, ApiError> {
    let label = parse_label(&body)?;
    mutate(state, ws, user, actor, raw, label, true).await
}

async fn detach(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path((raw, label)): Path<(String, String)>,
) -> Result<GoJson<Value>, ApiError> {
    mutate(state, ws, user, actor, raw, label, false).await
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    label_id: String,
    attaching: bool,
) -> Result<GoJson<Value>, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let (actor_type, actor_id, task_id) = write_actor(actor, user)?;
    let fail = if attaching {
        "failed to attach label"
    } else {
        "failed to detach label"
    };
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let issue_id: Uuid = issue.get("id");
    let label_id = Uuid::parse_str(&label_id).map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            if attaching {
                "invalid label_id"
            } else {
                "invalid label id"
            },
        )
    })?;
    let tx = db
        .transaction()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,fail))?;
    issue_and_label(&tx, issue_id, ws, label_id, fail).await?;
    let changed = if attaching {
        tx.execute(
            "INSERT INTO issue_to_label (issue_id,label_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
            &[&issue_id, &label_id],
        )
        .await
    } else {
        tx.execute(
            "DELETE FROM issue_to_label WHERE issue_id=$1 AND label_id=$2",
            &[&issue_id, &label_id],
        )
        .await
    }
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?
        > 0;
    let revision = if changed {
        tx.query_one("UPDATE issue SET revision=revision+1,last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()) WHERE id=$1 AND workspace_id=$2 RETURNING revision", &[&issue_id,&ws]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,fail))?.get::<_,i64>(0)
    } else {
        0
    };
    tx.commit()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, fail))?;
    // The write is durable now. A failed response projection must not undo it
    // or publish an incomplete label replacement to subscribers.
    let labels = match db.query("SELECT l.* FROM issue_label l JOIN issue_to_label il ON il.label_id=l.id WHERE il.issue_id=$1 AND l.workspace_id=$2 AND l.resource_type='issue' ORDER BY lower(l.name)", &[&issue_id,&ws]).await {
        Ok(labels) => labels.iter().map(label).collect::<Vec<Value>>(),
        Err(_) => return Ok(GoJson(json!({}))),
    };
    if changed {
        // Go labels events always use the requesting member's identity, even
        // when a verified agent task made the write. Wakeup attribution uses
        // the transaction-local agent identity separately.
        sink.publish_assignment_changed(IssueAssignmentEvent {
            event_type: "issue_labels:changed",
            workspace_id: ws,
            actor_type: "member",
            actor_id: user,
            payload: json!({"issue_id":issue_id,"labels":labels,"issue_revision":revision}),
        });
    }
    let mut result = json!({"labels":labels});
    if revision > 0 {
        result["issue_revision"] = json!(revision);
    }
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    #[test]
    fn frozen_go_label_decoder() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../../tests/issue_write_label_oracle.json"))
                .unwrap();
        for case in cases {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let actual = parse_label(&body);
            if case["error"] == true {
                assert_eq!(
                    actual.unwrap_err().1,
                    "invalid request body",
                    "{}",
                    case["name"]
                );
            } else if case["label_id"] == "" {
                assert_eq!(
                    actual.unwrap_err().1,
                    "label_id is required",
                    "{}",
                    case["name"]
                );
            } else {
                assert_eq!(actual.unwrap(), case["label_id"], "{}", case["name"]);
            }
        }
    }

    #[test]
    fn request_errors() {
        assert_eq!(parse_label(br#"{}"#).unwrap_err().1, "label_id is required");
        assert_eq!(
            parse_label(br#"{"label_id":"not-uuid"}"#).unwrap(),
            "not-uuid"
        );
        assert_eq!(
            parse_label(br#"{"label_id":true}"#).unwrap_err().1,
            "invalid request body"
        );
    }
}
