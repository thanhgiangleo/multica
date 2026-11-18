//! Comment edits and non-cascading deletion. Runtime replay remains an explicit gate.
use super::issue_write_comment_decode::{EditRequest, decode};
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_write::{
        CommentCancelledBatch, CommentCancelledTask, CommentChangedAction, CommentCreatedAction,
        IssueCommentChangedEvent, WriteError, write_actor,
    },
    issue_write_resolution::response,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::put,
};
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

#[path = "issue_write_comment_replay.rs"]
mod legacy_replay;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/comments/{commentId}", put(update).delete(delete))
        .route(
            "/api/comments/{commentId}/keep-replies",
            axum::routing::delete(delete),
        )
}

fn parse_edit(body: &[u8]) -> Result<EditRequest, ApiError> {
    let mut req = decode::<EditRequest>(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    req.content = req.content.replace('\0', "");
    if req.content.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "content is required"));
    }
    req.content_base = req.content_base.map(|s| s.replace('\0', ""));
    if req.expected_revision.is_some_and(|v| v < 1) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "expected_revision must be a positive integer",
        ));
    }
    Ok(req)
}
fn ids(raw: Option<Vec<String>>, field: &str) -> Result<Vec<Uuid>, WriteError> {
    raw.unwrap_or_default()
        .into_iter()
        .map(|s| {
            Uuid::parse_str(&s)
                .map_err(|_| WriteError(StatusCode::BAD_REQUEST, format!("invalid {field}")))
        })
        .collect()
}
async fn lock_target(tx: &Transaction<'_>, id: Uuid, ws: Uuid) -> Result<Row, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load comment");
    let owner = tx
        .query_opt(
            "SELECT issue_id FROM comment WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "comment not found"))?;
    let issue: Uuid = owner.get(0);
    tx.query_opt(
        "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
        &[&issue, &ws],
    )
    .await
    .map_err(|_| fail())?;
    tx.query_opt(
        "SELECT * FROM comment WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NULL FOR UPDATE",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail())?
    .ok_or_else(|| error(StatusCode::NOT_FOUND, "comment not found"))
}
async fn authorize(
    tx: &Transaction<'_>,
    row: &Row,
    ws: Uuid,
    user: Uuid,
    kind: &str,
    actor: Uuid,
    verb: &str,
) -> Result<bool, WriteError> {
    let member = tx
        .query_opt(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load workspace member",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let is_author =
        row.get::<_, String>("author_type") == kind && row.get::<_, Uuid>("author_id") == actor;
    let admin = matches!(member.get::<_, String>("role").as_str(), "owner" | "admin");
    if !is_author && !admin {
        return Err(WriteError(
            StatusCode::FORBIDDEN,
            format!("only comment author or admin can {verb}"),
        ));
    }
    Ok(is_author)
}
async fn stamp(tx: &Transaction<'_>, kind: &str, id: Uuid, task: &str) -> Result<(), ApiError> {
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)",&[&kind,&id.to_string(),&task]).await
        .map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to prepare comment edit"))?;
    Ok(())
}
async fn has_active_batches(tx: &Transaction<'_>, id: Uuid, ws: Uuid) -> Result<bool, ApiError> {
    tx.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent_task_queue task JOIN comment c ON c.id=$1 AND c.workspace_id=$2 JOIN issue i ON i.id=c.issue_id AND i.workspace_id=c.workspace_id WHERE task.issue_id=i.id AND (task.trigger_comment_id=c.id OR c.id=ANY(task.coalesced_comment_ids)) AND task.status IN ('queued','dispatched','running','waiting_local_directory','deferred'))",
        &[&id, &ws],
    )
    .await
    .map(|r| r.get(0))
    .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to prepare comment edit"))
}
async fn cancel_batches(tx: &Transaction<'_>, id: Uuid, ws: Uuid) -> Result<Vec<Uuid>, ApiError> {
    Ok(cancel_batch_snapshot(tx, id, ws)
        .await?
        .into_iter()
        .map(|t| t.id)
        .collect())
}

async fn cancel_batch_snapshot(
    tx: &Transaction<'_>,
    id: Uuid,
    ws: Uuid,
) -> Result<Vec<CommentCancelledTask>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to prepare comment edit",
        )
    };
    let rows = tx
        .query(include_str!("issue_write_comment_cancel.sql"), &[&id, &ws])
        .await
        .map_err(|_| fail())?;
    let tasks: Vec<CommentCancelledTask> = rows
        .into_iter()
        .map(|r| CommentCancelledTask {
            id: r.get("id"),
            agent_id: r.get("agent_id"),
            trigger_comment_id: r.get("trigger_comment_id"),
            coalesced_comment_ids: r.get("coalesced_comment_ids"),
        })
        .collect();
    if tasks.is_empty() {
        return Ok(tasks);
    }
    let task_ids: Vec<Uuid> = tasks.iter().map(|t| t.id).collect();
    tx.execute("UPDATE task_supplement s SET status='failed',failure_reason='turn_ended',updated_at=now() FROM agent_task_queue t WHERE s.task_id=t.id AND t.id=ANY($1::uuid[]) AND t.status IN ('completed','failed','cancelled') AND s.status IN ('pending','delivering')",&[&task_ids]).await.map_err(|_|fail())?;
    tx.execute("UPDATE comment recovery SET recovery_settled_at=now() FROM agent_task_queue t WHERE t.id=ANY($1::uuid[]) AND t.status IN ('completed','failed','cancelled') AND recovery.id=ANY(t.delivered_comment_ids) AND recovery.author_type='system' AND recovery.type='progress_update' AND recovery.source_task_id IS NOT NULL AND recovery.recovery_settled_at IS NULL",&[&task_ids]).await.map_err(|_|fail())?;
    Ok(tasks)
}
/// Go commits cancellation before legacy edits and all deletes. A later SQL
/// failure must replay the original batch; successful writes exclude this input.
/// Strict edits keep cancellation in the edit transaction instead.
async fn prepare_legacy(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    id: Uuid,
    body: Option<&[u8]>,
) -> Result<Option<CommentCancelledBatch>, WriteError> {
    let (kind, actor_id, task_id) = write_actor(actor, user)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to prepare comment edit",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    stamp(&tx, kind, actor_id, &task_id).await?;
    let current = lock_target(&tx, id, ws).await?;
    if body.is_some() {
        let bound: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM task_supplement WHERE comment_id=$1 AND workspace_id=$2)",
                &[&id, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to verify additional message"))?
            .get(0);
        if bound {
            return Err(error(StatusCode::CONFLICT, "additional messages cannot be edited").into());
        }
    }
    authorize(
        &tx,
        &current,
        ws,
        user,
        kind,
        actor_id,
        if body.is_some() { "edit" } else { "delete" },
    )
    .await?;
    if let Some(body) = body {
        let req = parse_edit(body)?;
        if req.expected_revision.is_some() || req.content_base.is_some() {
            return Ok(None);
        }
        ids(req.attachment_ids, "attachment_ids")?;
        ids(req.suppress_agent_ids, "suppress_agent_ids")?;
        if req.content == current.get::<_, String>("content") {
            return Ok(None);
        }
    }
    if !has_active_batches(&tx, id, ws).await? {
        return Ok(None);
    }
    let runtime = state
        .issue_comment_runtime
        .as_ref()
        .and_then(|runtime| runtime.legacy_batches())
        .ok_or_else(|| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "legacy comment cancellation runtime unavailable",
            )
        })?;
    let issue_id: Uuid = current.get("issue_id");
    // Snapshot UPDATE RETURNING, not a separate SELECT: a concurrent enqueue
    // must never be cancelled without retaining its original replay inputs.
    let tasks = cancel_batch_snapshot(&tx, id, ws).await?;
    let task_ids = tasks.iter().map(|t| t.id).collect();
    tx.commit().await.map_err(|_| fail())?;
    let batch = CommentCancelledBatch {
        workspace_id: ws,
        issue_id,
        comment_id: id,
        task_ids,
        tasks,
    };
    runtime.cancelled(&batch).await;
    Ok(Some(batch))
}

fn action(
    row: &Row,
    ws: Uuid,
    (kind, actor, user): (&'static str, Uuid, Uuid),
) -> CommentCreatedAction {
    CommentCreatedAction {
        workspace_id: ws,
        issue_id: row.get("issue_id"),
        comment_id: row.get("id"),
        parent_id: row.get("parent_id"),
        actor_type: kind,
        actor_id: actor,
        member_originator: if kind == "member" { Some(user) } else { None },
        source_task_id: row.get("source_task_id"),
        content: row.get("content"),
        suppressed_agent_ids: row
            .get::<_, Option<Vec<Uuid>>>("suppressed_agent_ids")
            .unwrap_or_default(),
        steer_task_ids: vec![],
        steer_all: false,
    }
}

async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: axum::body::Bytes,
) -> Result<(StatusCode, GoJson<Value>), WriteError> {
    require_runtime(&state)?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let batch = prepare_legacy(&state, ws, user, actor.clone(), id, Some(&body)).await?;
    let result = update_inner(&state, ws, user, actor, id, body, batch.as_ref()).await;
    if !matches!(&result, Ok((StatusCode::OK, _))) {
        replay(&state, batch.as_ref(), None).await;
    }
    result
}

fn require_runtime(state: &ApiState) -> Result<(), ApiError> {
    if state.issue_events.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        ));
    }
    if state.issue_comment_runtime.is_none() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment runtime unavailable",
        ));
    }
    Ok(())
}

async fn replay(state: &ApiState, batch: Option<&CommentCancelledBatch>, excluded: Option<Uuid>) {
    if let Some(batch) = batch
        && let Some(runtime) = state
            .issue_comment_runtime
            .as_ref()
            .and_then(|r| r.legacy_batches())
    {
        match legacy_replay::inputs(state, batch, excluded).await {
            Ok(inputs) => runtime.replay(batch, excluded, inputs).await,
            Err(_) => eprintln!(
                "failed to select cancelled batch survivors for comment {}",
                batch.comment_id
            ),
        }
    }
}

async fn update_inner(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    id: Uuid,
    body: axum::body::Bytes,
    batch: Option<&CommentCancelledBatch>,
) -> Result<(StatusCode, GoJson<Value>), WriteError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let runtime = state.issue_comment_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment runtime unavailable",
        )
    })?;
    let (kind, actor_id, task_id) = write_actor(actor, user)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to update comment",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    stamp(&tx, kind, actor_id, &task_id).await?;
    let current = lock_target(&tx, id, ws).await?;
    let bound: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM task_supplement WHERE comment_id=$1 AND workspace_id=$2)",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to verify additional message",
            )
        })?
        .get(0);
    if bound {
        return Err(error(StatusCode::CONFLICT, "additional messages cannot be edited").into());
    }
    let is_author = authorize(&tx, &current, ws, user, kind, actor_id, "edit").await?;
    let req = parse_edit(&body)?;
    let strict = req.expected_revision.is_some() || req.content_base.is_some();
    let content = req.content;
    let revision: i64 = current.get("revision");
    if let Some(expected) = req.expected_revision
        && expected != revision
    {
        return Ok((
            StatusCode::CONFLICT,
            GoJson(
                json!({"error":"resource changed since it was loaded","code":"revision_conflict","resource_type":"comment","resource_id":id,"expected_revision":expected,"actual_revision":revision}),
            ),
        ));
    }
    let old: String = current.get("content");
    if req.content_base.is_some_and(|s| s != old && content != old) {
        return Ok((
            StatusCode::CONFLICT,
            GoJson(
                json!({"error":"resource field changed since it was loaded","code":"revision_conflict","resource_type":"comment","resource_id":id}),
            ),
        ));
    }
    let replace_attachments = req.attachment_ids.is_some();
    let attachments = ids(req.attachment_ids, "attachment_ids")?;
    let suppressed = ids(req.suppress_agent_ids, "suppress_agent_ids")?;
    let text_changed = old != content;
    // Go's legacy path commits cancellation before editing and replays survivors
    // on failure. Do not substitute strict transactional semantics for that path.
    if text_changed && !strict && batch.is_none() && has_active_batches(&tx, id, ws).await? {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "legacy comment edit cancellation runtime unavailable",
        )
        .into());
    }
    let source: Option<Uuid> = if text_changed {
        if kind == "agent" && is_author {
            Uuid::parse_str(&task_id).ok()
        } else {
            None
        }
    } else {
        current.get("source_task_id")
    };
    let did_change = text_changed || source != current.get::<_, Option<Uuid>>("source_task_id");
    let issue: Uuid = current.get("issue_id");
    if text_changed
        && tx
            .query_opt(
                "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&issue, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load issue"))?
            .is_none()
    {
        return Err(error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load issue").into());
    }
    let updated=tx.query_one("UPDATE comment SET content=$3,source_task_id=$4,suppressed_agent_ids=CASE WHEN content IS DISTINCT FROM $3 THEN $5::uuid[] ELSE suppressed_agent_ids END,revision=revision+CASE WHEN $6 THEN 1 ELSE 0 END,updated_at=CASE WHEN $6 THEN now() ELSE updated_at END WHERE id=$1 AND workspace_id=$2 RETURNING *",&[&id,&ws,&content,&source,&suppressed,&did_change]).await.map_err(|_|fail())?;
    let mut issue_revision = 0i64;
    if did_change {
        issue_revision=tx.query_opt("UPDATE issue SET revision=revision+1,last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()) WHERE id=$1 AND workspace_id=$2 RETURNING revision",&[&issue,&ws]).await.map_err(|_|fail())?.map_or(0,|r|r.get(0));
    }
    // Strict edits cancel stale inputs in the same transaction, as frozen Go does.
    let cancelled = if text_changed && batch.is_none() {
        cancel_batches(&tx, id, ws).await?
    } else {
        vec![]
    };
    let mut updated = updated;
    if replace_attachments {
        let changed=tx.execute("UPDATE attachment SET comment_id=CASE WHEN id=ANY($3::uuid[]) THEN $1 ELSE NULL END WHERE issue_id=$2 AND workspace_id=$4 AND source_context_id IS NULL AND ((comment_id=$1 AND NOT id=ANY($3::uuid[])) OR (comment_id IS NULL AND id=ANY($3::uuid[])))",&[&id,&issue,&attachments,&ws]).await.map_err(|_|fail())?;
        if changed > 0 && !text_changed {
            updated=tx.query_one("UPDATE comment SET revision=revision+1,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",&[&id,&ws]).await.map_err(|_|fail())?;
        }
    }
    tx.commit().await.map_err(|_| fail())?;
    let mut result = response(&db, &updated, ws).await;
    drop(db);
    if issue_revision > 0 {
        result["issue_revision"] = json!(issue_revision);
    }
    let mut payload = json!({"comment":result});
    if issue_revision > 0 {
        payload["issue_revision"] = json!(issue_revision);
    }
    sink.publish_comment_changed(IssueCommentChangedEvent {
        event_type: "comment:updated",
        workspace_id: ws,
        actor_type: kind,
        actor_id,
        payload,
    });
    replay(state, batch, Some(id)).await;
    if text_changed {
        let feedback = runtime
            .changed(CommentChangedAction {
                comment: action(&updated, ws, (kind, actor_id, user)),
                deleted: false,
                cancelled_task_ids: cancelled,
                deleted_attachment_urls: vec![],
            })
            .await;
        if !feedback.trigger_outcomes.is_empty() {
            result["trigger_outcomes"] = json!(feedback.trigger_outcomes);
        }
    }
    Ok((StatusCode::OK, GoJson(result)))
}

async fn delete(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
) -> Result<StatusCode, WriteError> {
    require_runtime(&state)?;
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid comment id"))?;
    let batch = prepare_legacy(&state, ws, user, actor.clone(), id, None).await?;
    let result = delete_inner(&state, ws, user, actor, id, batch.as_ref()).await;
    if result.is_err() {
        replay(&state, batch.as_ref(), None).await;
    }
    result
}

async fn delete_inner(
    state: &ApiState,
    ws: Uuid,
    user: Uuid,
    actor: Actor,
    id: Uuid,
    batch: Option<&CommentCancelledBatch>,
) -> Result<StatusCode, WriteError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let runtime = state.issue_comment_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment runtime unavailable",
        )
    })?;
    let (kind, actor_id, task_id) = write_actor(actor, user)?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to delete comment",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let tx = db.transaction().await.map_err(|_| fail())?;
    stamp(&tx, kind, actor_id, &task_id).await?;
    let current = lock_target(&tx, id, ws).await?;
    authorize(&tx, &current, ws, user, kind, actor_id, "delete").await?;
    // A batch arriving after preflight is not silently cancelled in the delete
    // transaction: without the pre-commit runtime, preserve the fail-closed gate.
    if batch.is_none() && has_active_batches(&tx, id, ws).await? {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment deletion cancellation runtime unavailable",
        )
        .into());
    }
    let receipts = tx
        .query(
            "SELECT status FROM task_supplement WHERE comment_id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?;
    if receipts.iter().any(|r| {
        matches!(
            r.get::<_, String>("status").as_str(),
            "pending" | "delivering"
        )
    }) {
        return Err(error(
            StatusCode::CONFLICT,
            "additional message is still being delivered",
        )
        .into());
    }
    let cancelled = if batch.is_none() {
        cancel_batches(&tx, id, ws).await?
    } else {
        vec![]
    };
    tx.execute(
        "DELETE FROM task_supplement WHERE comment_id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail())?;
    let has_replies: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM comment WHERE parent_id=$1 AND workspace_id=$2)",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    let urls: Vec<String> = tx
        .query(
            "DELETE FROM attachment WHERE comment_id=$1 AND workspace_id=$2 RETURNING url",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .iter()
        .map(|r| r.get(0))
        .collect();
    tx.execute(
        "DELETE FROM comment_reaction WHERE comment_id=$1 AND workspace_id=$2",
        &[&id, &ws],
    )
    .await
    .map_err(|_| fail())?;
    let mut tombstone = None;
    let mut removed = vec![];
    if has_replies {
        let row=tx.query_one("UPDATE comment SET content='',deleted_at=now(),resolved_at=NULL,resolved_by_type=NULL,resolved_by_id=NULL,revision=revision+1,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *",&[&id,&ws]).await.map_err(|_|fail())?;
        tx.execute("UPDATE comment SET recovery_settled_at=now() WHERE id=$1 AND author_type='system' AND type='progress_update' AND source_task_id IS NOT NULL AND recovery_settled_at IS NULL",&[&id]).await.map_err(|_|fail())?;
        tombstone = Some(super::issue_write_resolution::base_response(&row));
    } else {
        let row=tx.query_one("DELETE FROM comment WHERE id=$1 AND workspace_id=$2 AND NOT EXISTS(SELECT 1 FROM comment c WHERE c.parent_id=$1) RETURNING parent_id",&[&id,&ws]).await.map_err(|_|fail())?;
        removed.push(id);
        let mut parent: Option<Uuid> = row.get(0);
        for _ in 0..256 {
            let Some(p) = parent else { break };
            let pruned=tx.query_opt("DELETE FROM comment WHERE id=$1 AND workspace_id=$2 AND deleted_at IS NOT NULL AND NOT EXISTS(SELECT 1 FROM comment child WHERE child.parent_id=$1) RETURNING parent_id",&[&p,&ws]).await.map_err(|_|fail())?;
            let Some(row) = pruned else { break };
            removed.push(p);
            parent = row.get(0);
        }
    }
    let issue: Uuid = current.get("issue_id");
    let issue_revision=tx.query_opt("UPDATE issue SET revision=revision+1,last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now()) WHERE id=$1 AND workspace_id=$2 RETURNING revision",&[&issue,&ws]).await.map_err(|_|fail())?.map_or(0i64,|r|r.get(0));
    tx.commit().await.map_err(|_| fail())?;
    if let Some(mut comment) = tombstone {
        if issue_revision > 0 {
            comment["issue_revision"] = json!(issue_revision);
        }
        let mut payload = json!({"comment":comment});
        if issue_revision > 0 {
            payload["issue_revision"] = json!(issue_revision);
        }
        sink.publish_comment_changed(IssueCommentChangedEvent {
            event_type: "comment:updated",
            workspace_id: ws,
            actor_type: kind,
            actor_id,
            payload,
        });
    }
    for comment in removed {
        let mut payload = json!({"comment_id":comment,"issue_id":issue});
        if issue_revision > 0 {
            payload["issue_revision"] = json!(issue_revision);
        }
        sink.publish_comment_changed(IssueCommentChangedEvent {
            event_type: "comment:deleted",
            workspace_id: ws,
            actor_type: kind,
            actor_id,
            payload,
        });
    }
    replay(state, batch, Some(id)).await;
    runtime
        .changed(CommentChangedAction {
            comment: action(&current, ws, (kind, actor_id, user)),
            deleted: true,
            cancelled_task_ids: cancelled,
            deleted_attachment_urls: urls,
        })
        .await;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edit_validation() {
        for raw in [
            br#"{"content":"x","expected_revision":0}"#.as_slice(),
            br#"{"content":"x","expected_revision":-1}"#,
        ] {
            assert_eq!(
                parse_edit(raw).err().unwrap().1,
                "expected_revision must be a positive integer"
            );
        }
        assert_eq!(parse_edit(b"[]").err().unwrap().1, "invalid request body");
        let req =
            parse_edit(br#"{"content":"x\u0000","content_base":"y\u0000","attachment_ids":null}"#)
                .unwrap();
        assert_eq!(req.content, "x");
        assert_eq!(req.content_base.as_deref(), Some("y"));
        assert!(req.attachment_ids.is_none());
    }
}
