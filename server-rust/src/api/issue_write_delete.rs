//! Issue deletion keeps Go's pre-delete runtime boundary and atomic DB sweep.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueUpdatedEvent, write_actor},
    issue_write_update,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, post},
};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct IssueDeletePreparation {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub source_task_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IssueDeletedEvent {
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    /// Always the resolved UUID, never the request's human-readable key.
    pub issue_id: Uuid,
}

/// Requires actual task cancellation/reconciliation and autopilot accounting.
/// `prepare_delete` runs before the DB delete transaction, as in frozen Go:
/// cancellation may survive a later delete rollback. Implementations must scope
/// every lookup to this workspace and preserve source-task/actor attribution.
#[async_trait]
pub trait IssueDeleteRuntime: Send + Sync {
    async fn prepare_delete(&self, action: &IssueDeletePreparation) -> Result<(), ()>;
    /// Same storage policy as Go Storage.KeyFromURL; None means no owned key.
    fn storage_key(&self, url: &str) -> Option<String>;
    /// Best effort after commit. Source-context clones retain durable intents.
    fn delete_objects(&self, urls: &[String]);
}
pub type SharedIssueDeleteRuntime = Arc<dyn IssueDeleteRuntime>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}", delete(delete_one))
        .route("/api/issues/{id}/", delete(delete_one))
        .route("/api/issues/batch-delete", post(delete_batch))
}

async fn delete_one(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<Response, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete issue"))?;
    let id: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let issue = db
        .query_one(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    drop(db);
    delete_issues(&state, ws, user, actor, vec![issue], false).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Default)]
struct BatchRequest {
    issue_ids: Option<Vec<String>>,
}

impl<'de> serde::Deserialize<'de> for BatchRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Fields;
        impl<'de> serde::de::Visitor<'de> for Fields {
            type Value = BatchRequest;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(BatchRequest::default())
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut request = BatchRequest::default();
                let mut backing = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    if super::issue_write_comment_decode::folded(&key) == "issue_ids" {
                        request.issue_ids =
                            super::issue_write_comment_decode::strings(&mut map, &mut backing)?;
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }
                Ok(request)
            }
        }
        deserializer.deserialize_any(Fields)
    }
}
async fn delete_batch(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let req: BatchRequest = super::issue_write_comment_decode::decode(&body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let ids = req
        .issue_ids
        .filter(|ids| !ids.is_empty())
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "issue_ids is required"))?;
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete issues"))?;
    let mut seen = HashSet::new();
    let mut issues = Vec::new();
    for raw in ids {
        // Frozen Go skips invalid UUIDs, foreign/missing issues and duplicates.
        let Ok(id) = Uuid::parse_str(&raw) else {
            continue;
        };
        if !seen.insert(id) {
            continue;
        }
        if let Some(row) = db
            .query_opt(
                "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete issues"))?
        {
            issues.push(row);
        }
    }
    drop(db);
    let deleted = issues.len();
    delete_issues(&state, ws, user, actor, issues, true).await?;
    Ok(GoJson(json!({"deleted":deleted})).into_response())
}

async fn delete_issues(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    issues: Vec<Row>,
    batch: bool,
) -> Result<(), ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let runtime = state.issue_delete_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue deletion runtime unavailable",
        )
    })?;
    let (actor_type, actor_id, task) = write_actor(actor, user)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            if batch {
                "failed to delete issues"
            } else {
                "failed to delete issue"
            },
        )
    };
    for issue in &issues {
        runtime
            .prepare_delete(&IssueDeletePreparation {
                workspace_id: ws,
                issue_id: issue.get("id"),
                actor_type,
                actor_id,
                source_task_id: Uuid::parse_str(&task).ok(),
            })
            .await
            .map_err(|_| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "issue deletion runtime unavailable",
                )
            })?;
    }
    // Preserve the caller's first-occurrence order for Go's deletion and
    // event sequence. Lock rows in a stable order separately to serialize
    // concurrent deletes without changing observable order.
    let ids: Vec<Uuid> = issues.iter().map(|row| row.get("id")).collect();
    let excluded = if batch { ids.clone() } else { Vec::new() };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task]).await.map_err(|_| fail())?;
    super::issue_write_structure::lock_graph(&tx, ws).await?;
    if !ids.is_empty() {
        let locked = tx.query("SELECT id FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[]) ORDER BY id FOR UPDATE", &[&ws,&ids]).await.map_err(|_| fail())?;
        if locked.len() != ids.len() {
            return Err(fail());
        }
    }
    let mut urls = Vec::new();
    let mut detached = Vec::new();
    let mut cleared = Vec::new();
    for original in &issues {
        let id: Uuid = original.get("id");
        detached.extend(tx.query("UPDATE issue SET parent_issue_id=NULL,stage=NULL,revision=revision+1,updated_at=now(),last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()) WHERE workspace_id=$1 AND parent_issue_id=$2 AND NOT COALESCE(id=ANY($3::uuid[]),false) RETURNING *", &[&ws,&id,&excluded]).await.map_err(|_| fail())?);
        let duplicates = tx.query("UPDATE issue SET duplicate_of_issue_id=NULL,revision=revision+1,updated_at=now(),last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()) WHERE workspace_id=$1 AND duplicate_of_issue_id=$2 AND NOT COALESCE(id=ANY($3::uuid[]),false) RETURNING *", &[&ws,&id,&excluded]).await.map_err(|_| fail())?;
        for row in duplicates {
            cleared.push((row, original));
        }
        urls.extend(tx.query("SELECT a.url FROM attachment a WHERE a.issue_id=$1 OR a.comment_id IN (SELECT c.id FROM comment c WHERE c.issue_id=$1)", &[&id]).await.map_err(|_| fail())?.into_iter().map(|r| r.get::<_,String>(0)));
        if let Some(context) = tx.query_opt("SELECT id FROM issue_source_context WHERE workspace_id=$1 AND issue_id=$2 AND state='attached'", &[&ws,&id]).await.map_err(|_| fail())? {
            let context_id: Uuid = context.get(0);
            let attachments = tx.query("SELECT id,url FROM attachment WHERE workspace_id=$1 AND source_context_id=$2 ORDER BY created_at,id", &[&ws,&context_id]).await.map_err(|_| fail())?;
            for attachment in &attachments {
                let url: String = attachment.get("url");
                if let Some(key) = runtime.storage_key(&url).filter(|key| !key.is_empty()) {
                    let attachment_id: Uuid = attachment.get("id");
                    tx.execute("INSERT INTO issue_source_context_object_intent(storage_key,workspace_id,source_context_id,attachment_id,object_url) VALUES($1,$2,$3,$4,$5)", &[&key,&ws,&context_id,&attachment_id,&url]).await.map_err(|_| fail())?;
                }
            }
            urls.extend(tx.query("DELETE FROM attachment WHERE workspace_id=$1 AND source_context_id=$2 RETURNING url", &[&ws,&context_id]).await.map_err(|_| fail())?.into_iter().map(|r| r.get::<_,String>(0)));
            tx.execute("DELETE FROM issue_source_context WHERE workspace_id=$1 AND issue_id=$2 AND state='attached'", &[&ws,&id]).await.map_err(|_| fail())?;
        }
        tx.execute(include_str!("issue_write_delete.sql"), &[&id, &ws])
            .await
            .map_err(|_| fail())?;
    }
    // Build canonical snapshots while all delete-side rewrites are still locked.
    let mut updates = Vec::new();
    for row in &detached {
        updates.push(IssueUpdatedEvent {
            workspace_id: ws,
            actor_type,
            actor_id,
            payload: json!({"issue":issue_write_update::response(&tx,row,ws).await?}),
        });
    }
    for (row, original) in cleared {
        let response = issue_write_update::response(&tx, &row, ws).await?;
        let identifier = response["identifier"].as_str().unwrap();
        let prefix = identifier.rsplit_once('-').unwrap().0;
        let previous = if row.get::<_, String>("status") == "cancelled" {
            json!(original.get::<_, Uuid>("id"))
        } else {
            Value::Null
        };
        updates.push(IssueUpdatedEvent { workspace_id:ws,actor_type,actor_id,payload:json!({"issue":response,"duplicate_of_issue_id":null,"prev_duplicate_of_issue_id":previous,"prev_duplicate_of_identifier":format!("{prefix}-{}",original.get::<_,i32>("number"))}) });
    }
    tx.commit().await.map_err(|_| fail())?;
    runtime.delete_objects(&urls);
    for id in ids {
        sink.publish_issue_deleted(IssueDeletedEvent {
            workspace_id: ws,
            actor_type,
            actor_id,
            issue_id: id,
        });
    }
    for event in updates {
        sink.publish_issue_updated(event);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    #[test]
    fn batch_delete_decoder_matches_go_oracle() {
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/issue_write_delete_subscribe_oracle.json"
        ))
        .unwrap();
        for case in cases.iter().filter(|c| c["kind"] == "batch") {
            let body = STANDARD
                .decode(case["body_base64"].as_str().unwrap())
                .unwrap();
            let request = super::super::issue_write_comment_decode::decode::<BatchRequest>(&body);
            assert_eq!(
                request.is_err(),
                case["decode_error"].as_bool().unwrap(),
                "{case}"
            );
            if let Ok(request) = request {
                assert_eq!(
                    request.issue_ids.unwrap_or_default(),
                    serde_json::from_value::<Vec<String>>(
                        case.get("issue_ids").cloned().unwrap_or_else(|| json!([]))
                    )
                    .unwrap(),
                    "{case}"
                );
            }
        }
    }
}
