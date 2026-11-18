//! Idempotent issue reactions, serialized with other issue mutations.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueReactionEvent, write_actor},
    issue_write_reaction::emoji,
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
    Router::new().route("/api/issues/{id}/reactions", post(add).delete(remove))
}

async fn add(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, GoJson<Value>), ApiError> {
    let result = mutate(state, ws, user, actor, raw, body, true).await?;
    Ok((StatusCode::CREATED, GoJson(result)))
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
) -> Result<Value, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let (actor_type, actor_id, task_id) = write_actor(actor, user)?;
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
    tx.execute("SELECT set_config('multica.actor_type',$1,true), set_config('multica.actor_id',$2,true), set_config('multica.source_task_id',$3,true)",
        &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_|fail())?;
    // Lock first, then insert/delete so concurrent add/remove and issue delete
    // cannot lose a revision or publish an event from a rolled-back write.
    let current=tx.query_opt("SELECT title,status,creator_type,creator_id FROM issue WHERE id=$1 AND workspace_id=$2 FOR NO KEY UPDATE",&[&id,&ws]).await.map_err(|_|fail())?
        .ok_or_else(||error(StatusCode::NOT_FOUND,"issue not found"))?;
    let mut response = Value::Null;
    let changed;
    let revision;
    if adding {
        let reaction = tx
            .query_one(
                include_str!("issue_write_issue_reaction_add.sql"),
                &[&id, &ws, &actor_type, &actor_id, &emoji],
            )
            .await
            .map_err(|_| fail())?;
        revision = reaction.get::<_, i64>("issue_revision");
        changed = revision > 0;
        response = json!({"id":reaction.get::<_,Uuid>("id"),"issue_id":id,"actor_type":actor_type,
            "actor_id":actor_id,"emoji":emoji,"created_at":timestamp(reaction.get::<_,DateTime<Utc>>("created_at"))});
        if changed {
            response["issue_revision"] = json!(revision);
        }
    } else {
        let removed = tx
            .query_one(
                include_str!("issue_write_issue_reaction_remove.sql"),
                &[&id, &actor_type, &actor_id, &emoji],
            )
            .await
            .map_err(|_| fail())?;
        revision = removed.get::<_, i64>("issue_revision");
        changed = removed.get::<_, bool>("changed");
    }
    tx.commit().await.map_err(|_| fail())?;
    if changed {
        let payload = if adding {
            json!({"reaction":response,"issue_id":id,"issue_title":current.get::<_,String>("title"),
                "issue_status":current.get::<_,String>("status"),"creator_type":current.get::<_,String>("creator_type"),
                "creator_id":current.get::<_,Uuid>("creator_id"),"issue_revision":revision})
        } else {
            json!({"issue_id":id,"emoji":emoji,"actor_type":actor_type,"actor_id":actor_id,"issue_revision":revision})
        };
        sink.publish_reaction_changed(IssueReactionEvent {
            event_type: if adding {
                "issue_reaction:added"
            } else {
                "issue_reaction:removed"
            },
            workspace_id: ws,
            actor_type,
            actor_id,
            payload,
        });
    }
    Ok(response)
}
