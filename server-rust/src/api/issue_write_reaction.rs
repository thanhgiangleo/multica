//! Comment reaction writes: issue -> live comment -> reaction lock order.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_write::{IssueReactionEvent, scalar_decode},
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
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route(
        "/api/comments/{commentId}/reactions",
        post(add).delete(remove),
    )
}

pub(super) fn emoji(body: &[u8]) -> Result<String, ApiError> {
    let emoji = scalar_decode::emoji(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if emoji.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "emoji is required"));
    }
    Ok(emoji)
}

async fn add(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let (response, _) = mutate(state, ws, user, actor, raw, body, true).await?;
    Ok((StatusCode::CREATED, GoJson(response)))
}

async fn remove(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    mutate(state, ws, user, actor, raw, body, false).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn mutate(
    state: ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    raw: String,
    body: Bytes,
    adding: bool,
) -> Result<(Value, bool), ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let (actor_type, actor_id, task_id) = match actor {
        Actor::Task(task) => ("agent", task.agent, task.task.to_string()),
        Actor::Human | Actor::Cloud => ("member", user, String::new()),
        Actor::Daemon => return Err(error(StatusCode::FORBIDDEN, "invalid actor")),
    };
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "comment not found"))?;
    let comment = db.query_opt("SELECT issue_id, author_type, author_id FROM comment WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL", &[&id,&ws]).await
        .map_err(|_| error(StatusCode::NOT_FOUND,"comment not found"))?
        .ok_or_else(|| error(StatusCode::NOT_FOUND,"comment not found"))?;
    let emoji = emoji(&body)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            if adding {
                "failed to add reaction"
            } else {
                "failed to remove reaction"
            },
        )
    };
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true), set_config('multica.actor_id',$2,true), set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_| fail())?;
    // Equivalent to Go LockLiveComment. The materialized fence forces the
    // issue lock before a comment lock even under planner reordering.
    let locked = tx.query_opt("WITH locked_issue AS MATERIALIZED (SELECT i.id FROM issue i JOIN comment c ON c.issue_id=i.id AND c.workspace_id=i.workspace_id WHERE c.id=$1 AND c.workspace_id=$2 FOR NO KEY UPDATE OF i), issue_fence AS MATERIALIZED (SELECT count(*) AS locked_count FROM locked_issue) SELECT c.id FROM comment c CROSS JOIN issue_fence WHERE c.id=$1 AND c.workspace_id=$2 AND c.deleted_at IS NULL AND issue_fence.locked_count>=0 FOR NO KEY UPDATE OF c", &[&id,&ws]).await.map_err(|_| fail())?;
    if locked.is_none() {
        return Err(error(StatusCode::NOT_FOUND, "comment not found"));
    }
    let issue_id: Uuid = comment.get("issue_id");
    let mut response = Value::Null;
    let changed;
    let revision: i64;
    if adding {
        let reaction = tx
            .query_one(
                include_str!("issue_write_reaction_add.sql"),
                &[&id, &ws, &actor_type, &actor_id, &emoji],
            )
            .await
            .map_err(|_| fail())?;
        revision = reaction.get("comment_revision");
        changed = revision > 0;
        response = json!({
            "id":reaction.get::<_,Uuid>("id"),"comment_id":id,"actor_type":actor_type,
            "actor_id":actor_id,"emoji":emoji,"created_at":timestamp(reaction.get::<_,DateTime<Utc>>("created_at"))
        });
        if changed {
            response["comment_revision"] = json!(revision);
        }
    } else {
        let removed = tx
            .query_one(
                include_str!("issue_write_reaction_remove.sql"),
                &[&id, &actor_type, &actor_id, &emoji],
            )
            .await
            .map_err(|_| fail())?;
        changed = removed.get("changed");
        revision = removed.get("comment_revision");
    }
    tx.commit().await.map_err(|_| fail())?;
    if changed {
        let payload = if adding {
            let issue = db
                .query_opt(
                    "SELECT title,status FROM issue WHERE id=$1 AND workspace_id=$2",
                    &[&issue_id, &ws],
                )
                .await
                .ok()
                .flatten();
            json!({"reaction":response,"issue_id":issue_id,"issue_title":issue.as_ref().map(|r|r.get::<_,String>("title")).unwrap_or_default(),
                "issue_status":issue.as_ref().map(|r|r.get::<_,String>("status")).unwrap_or_default(),
                "comment_id":id,"comment_author_type":comment.get::<_,String>("author_type"),
                "comment_author_id":comment.get::<_,Uuid>("author_id"),"comment_revision":revision})
        } else {
            json!({"comment_id":id,"issue_id":issue_id,"emoji":emoji,"actor_type":actor_type,
                "actor_id":actor_id,"comment_revision":revision})
        };
        sink.publish_reaction_changed(IssueReactionEvent {
            event_type: if adding {
                "reaction:added"
            } else {
                "reaction:removed"
            },
            workspace_id: ws,
            actor_type,
            actor_id,
            payload,
        });
    }
    Ok((response, changed))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn emoji_requires_a_string() {
        assert_eq!(emoji(r#"{"emoji":"👍"}"#.as_bytes()).unwrap(), "👍");
        assert!(emoji(br#"{"emoji":""}"#).is_err());
        assert!(emoji(br#"{"emoji":null}"#).is_err());
        assert!(emoji(br#"not-json"#).is_err());
    }
}
