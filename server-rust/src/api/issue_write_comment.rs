//! Comment creation DB boundary. Runtime invocation/notification wiring is mandatory.
#[path = "issue_write_comment_guard.rs"]
mod guard;
#[path = "issue_write_comment_reopen.rs"]
mod reopen;
use super::{
    ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{WriteError, write_actor},
};
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

/// Real runtime hook, not a second event bus. Called only after the comment commits.
/// Must implement Go trigger routing, invocation admission, coalescing, steering,
/// survivor reconciliation, and private feedback. The event sink owns notifications.
#[async_trait]
pub trait IssueCommentRuntime: Send + Sync {
    async fn created(&self, action: CommentCreatedAction) -> CommentFeedback;
    /// Broadcast cancelled tasks; retrigger surviving batch inputs; route new text.
    /// Delete actions also clean up the committed attachment URLs from storage.
    async fn changed(&self, action: CommentChangedAction) -> CommentFeedback;
    /// Required for legacy edits/deletes of active batches. No silent no-op adapter.
    fn legacy_batches(&self) -> Option<&dyn IssueCommentLegacyRuntime> {
        None
    }
}

/// Task-service integration for cancellation committed BEFORE a legacy comment
/// mutation. `cancelled` must capture analytics, broadcast task:cancelled, reconcile
/// distinct agents and notify finished tasks. Rust selects live, tenant-scoped
/// survivors from the cancelled-task snapshot, ordered by created_at/id; `replay`
/// must recompute authority from each input's stored source task, intersect
/// triggers with `target_agent_ids`, and route delegated-failure recovery through
/// its durable recovery path. Never borrow the editor's authority.
/// None exclusion restores the original batch after a failed comment mutation;
/// Some(comment_id) replays only OTHER inputs after a successful edit/delete.
/// Calls happen outside all DB transactions; post-commit delivery/retry is owned
/// by the real runtime. This interface alone is not production runtime parity.
#[async_trait]
pub trait IssueCommentLegacyRuntime: Send + Sync {
    async fn cancelled(&self, batch: &CommentCancelledBatch);
    async fn replay(
        &self,
        batch: &CommentCancelledBatch,
        excluded_comment_id: Option<Uuid>,
        inputs: Vec<CommentReplayInput>,
    );
}

#[derive(Clone, Debug)]
pub struct CommentCancelledBatch {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub comment_id: Uuid,
    pub task_ids: Vec<Uuid>,
    pub tasks: Vec<CommentCancelledTask>,
}

#[derive(Clone, Debug)]
pub struct CommentCancelledTask {
    pub id: Uuid,
    pub agent_id: Option<Uuid>,
    pub trigger_comment_id: Option<Uuid>,
    pub coalesced_comment_ids: Vec<Uuid>,
}

/// Live, ordered, tenant-scoped inputs selected by Rust from the immutable
/// cancelled-task snapshot. Runtime must still recheck invocation permissions.
#[derive(Clone, Debug)]
pub struct CommentReplayInput {
    pub comment_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub author_type: String,
    pub author_id: Uuid,
    pub source_task_id: Option<Uuid>,
    pub content: String,
    pub suppressed_agent_ids: Vec<Uuid>,
    pub target_agent_ids: Vec<Uuid>,
    pub delegated_failure_recovery: bool,
}
pub type SharedIssueCommentRuntime = Arc<dyn IssueCommentRuntime>;

#[derive(Clone, Debug)]
pub struct CommentCreatedAction {
    pub workspace_id: Uuid,
    pub issue_id: Uuid,
    pub comment_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    /// Only human/cloud authors provide this. Task authors MUST resolve authority
    /// from source_task_id; never borrow the issue creator or runtime owner.
    pub member_originator: Option<Uuid>,
    pub source_task_id: Option<Uuid>,
    pub suppressed_agent_ids: Vec<Uuid>,
    pub steer_task_ids: Vec<Uuid>,
    pub steer_all: bool,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct CommentChangedAction {
    pub comment: CommentCreatedAction,
    pub deleted: bool,
    pub cancelled_task_ids: Vec<Uuid>,
    pub deleted_attachment_urls: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct IssueCommentChangedEvent {
    pub event_type: &'static str,
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct CommentTriggerOutcome {
    pub target_type: String,
    /// The id exactly as the author wrote it. A malformed mention (for example
    /// `mention://agent/-`) still gets an outcome, and rewriting it to a parsed
    /// UUID — or to the nil UUID — would lose which target the client named.
    pub target_id: String,
    pub status: String,
    pub reason_code: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct CommentSupplement {
    pub task_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<Uuid>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct CommentFeedback {
    pub trigger_outcomes: Vec<CommentTriggerOutcome>,
    pub supplements: Vec<CommentSupplement>,
}

#[derive(Clone, Debug)]
pub struct IssueCommentCreatedEvent {
    pub workspace_id: Uuid,
    pub actor_type: &'static str,
    pub actor_id: Uuid,
    /// Shared event has no trigger_outcomes or steering receipts.
    pub payload: Value,
}

struct Validated {
    content: String,
    kind: String,
    parent: Option<Uuid>,
    attachments: Vec<Uuid>,
    suppressed: Vec<Uuid>,
    steer_tasks: Vec<Uuid>,
    steer: bool,
}

fn uuid_list(values: Option<Vec<String>>, name: &str) -> Result<Vec<Uuid>, WriteError> {
    values
        .unwrap_or_default()
        .into_iter()
        .map(|raw| {
            Uuid::parse_str(&raw)
                .map_err(|_| WriteError(StatusCode::BAD_REQUEST, format!("invalid {name}")))
        })
        .collect()
}

fn validate(body: &[u8]) -> Result<Validated, WriteError> {
    let req = super::issue_write_comment_decode::decode::<
        super::issue_write_comment_decode::CreateRequest,
    >(body)
    .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    let content = req.content.replace('\0', "");
    if content.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "content is required").into());
    }
    let mut kind = req.kind;
    if kind.is_empty() {
        kind = "comment".into();
    }
    if !matches!(kind.as_str(), "comment" | "progress_update") {
        return Err(error(StatusCode::BAD_REQUEST, "invalid comment type").into());
    }
    let parent = req
        .parent_id
        .map(|s| {
            Uuid::parse_str(&s).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid parent_id"))
        })
        .transpose()?;
    let attachments = uuid_list(req.attachment_ids, "attachment_ids")?;
    let suppressed = uuid_list(req.suppress_agent_ids, "suppress_agent_ids")?;
    let steer_tasks = uuid_list(req.steer_task_ids, "steer_task_ids")?;
    // Files cannot be delivered as a text supplement to a running turn.
    let (steer_tasks, steer) = if attachments.is_empty() {
        (steer_tasks, req.steer)
    } else {
        (Vec::new(), false)
    };
    Ok(Validated {
        content,
        kind,
        parent,
        attachments,
        suppressed,
        steer_tasks,
        steer,
    })
}

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/comments", post(create))
}

async fn create(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: axum::body::Bytes,
) -> Result<(StatusCode, GoJson<Value>), WriteError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    // Missing runtime must fail BEFORE any mutation, including /note and unassigned
    // issues: notifications, reply ownership and future reconciliation still matter.
    let runtime = state.issue_comment_runtime.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment runtime unavailable",
        )
    })?;
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to create comment",
        )
    };
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let issue = load_issue(&db, ws, &raw).await?;
    let issue_id: Uuid = issue.get("id");
    let req = validate(&body)?;
    // Go snapshots the root before inserting the reply. Reopening is a separate,
    // best-effort write after commit, never part of the reply transaction.
    let resolved_root = reopen::snapshot(&db, ws, issue_id, req.parent).await;
    let member_originator = if matches!(actor, Actor::Human | Actor::Cloud) {
        Some(user)
    } else {
        None
    };
    let (actor_type, actor_id, task_id) = write_actor(actor.clone(), user)?;
    let source_task_id = if let Actor::Task(task) = actor {
        Some(task.task)
    } else {
        None
    };
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task_id]).await.map_err(|_| fail())?;
    // This owner lock freezes reply insertion and teardown before any child lock.
    let issue = tx
        .query_opt(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&issue_id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    if let Some(parent) = req.parent
        && tx
            .query_opt(
                "SELECT id FROM comment WHERE id=$1 AND issue_id=$2 AND workspace_id=$3",
                &[&parent, &issue_id, &ws],
            )
            .await
            .map_err(|_| fail())?
            .is_none()
    {
        return Err(error(StatusCode::BAD_REQUEST, "invalid parent comment").into());
    }
    if let Some(task_id) = source_task_id {
        let task = tx.query_opt("SELECT issue_id,trigger_comment_id,coalesced_comment_ids FROM agent_task_queue WHERE id=$1 AND agent_id=$2", &[&task_id,&actor_id]).await.map_err(|_| fail())?
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid token"))?;
        if task.get::<_, Option<Uuid>>("issue_id") == Some(issue_id) {
            if let Some(trigger) = task.get::<_, Option<Uuid>>("trigger_comment_id") {
                let coalesced: Vec<Uuid> = task.get("coalesced_comment_ids");
                if req
                    .parent
                    .is_none_or(|p| p != trigger && !coalesced.contains(&p))
                {
                    let fix =
                        format!("set parent_id (--parent) to {trigger} or a coalesced comment id");
                    let message = match req.parent {
                        None => format!(
                            "comment-triggered tasks cannot create top-level comments; {fix}"
                        ),
                        Some(parent) => format!(
                            "parent_id {parent} is not a comment this task may reply under; {fix}"
                        ),
                    };
                    return Err(WriteError(StatusCode::CONFLICT, message));
                }
            }
            if guard::has_no_action(&tx, issue_id, actor_id, task_id).await? {
                return Err(error(
                    StatusCode::CONFLICT,
                    "squad leader recorded no_action; comments are not allowed for this task",
                )
                .into());
            }
        }
    }
    let id = Uuid::now_v7();
    let created = tx
        .query_one(
            include_str!("issue_write_comment_create.sql"),
            &[
                &issue_id,
                &ws,
                &id,
                &actor_type,
                &actor_id,
                &req.content,
                &req.kind,
                &req.parent,
                &source_task_id,
                &req.suppressed,
            ],
        )
        .await
        .map_err(|_| fail())?;
    if !req.attachments.is_empty() {
        let available = tx.query("SELECT id FROM attachment WHERE workspace_id=$1 AND issue_id=$2 AND comment_id IS NULL AND source_context_id IS NULL AND id=ANY($3::uuid[]) ORDER BY id FOR UPDATE", &[&ws,&issue_id,&req.attachments]).await.map_err(|_| fail())?;
        for requested in &req.attachments {
            if !available
                .iter()
                .any(|r| r.get::<_, Uuid>("id") == *requested)
            {
                return Err(WriteError(
                    StatusCode::CONFLICT,
                    format!("attachment {requested} is no longer available"),
                ));
            }
        }
        tx.execute("UPDATE attachment SET comment_id=$1 WHERE workspace_id=$2 AND issue_id=$3 AND comment_id IS NULL AND source_context_id IS NULL AND id=ANY($4::uuid[])", &[&id,&ws,&issue_id,&req.attachments]).await.map_err(|_| fail())?;
    }
    tx.commit().await.map_err(|_| fail())?;
    let mut response = super::issue_write_resolution::with_attachments(&db, &created, ws).await;
    // Live Go comments omit absent optional identity fields.
    if source_task_id.is_none() {
        response.as_object_mut().unwrap().remove("source_task_id");
    }
    response["issue_revision"] = json!(created.get::<_, i64>("issue_revision"));
    drop(db);
    // No private dispatch feedback in shared timeline events.
    sink.publish_comment_created(IssueCommentCreatedEvent { workspace_id: ws,actor_type,actor_id,payload: json!({
        "comment":response,"issue_title":issue.get::<_,String>("title"),
        "issue_assignee_type":issue.get::<_,Option<String>>("assignee_type"),
        "issue_assignee_id":issue.get::<_,Option<Uuid>>("assignee_id"),
        "issue_status":issue.get::<_,String>("status"),"issue_revision":created.get::<_,i64>("issue_revision")
    }) });
    if let Some(root) = resolved_root {
        reopen::after_commit(&state, sink, ws, root, actor_type, actor_id, &task_id).await;
    }
    let feedback = runtime
        .created(CommentCreatedAction {
            workspace_id: ws,
            issue_id,
            comment_id: id,
            parent_id: req.parent,
            actor_type,
            actor_id,
            member_originator,
            source_task_id,
            suppressed_agent_ids: req.suppressed,
            steer_task_ids: req.steer_tasks,
            steer_all: req.steer,
            content: req.content,
        })
        .await;
    if !feedback.trigger_outcomes.is_empty() {
        response["trigger_outcomes"] = json!(feedback.trigger_outcomes);
    }
    if let Some(first) = feedback.supplements.first() {
        response["supplement_task_id"] = json!(first.task_id);
        response["supplement_status"] = json!(first.status);
        if let Some(reason) = &first.failure_reason {
            response["supplement_failure_reason"] = json!(reason);
        }
        if let Some(at) = &first.delivered_at {
            response["supplement_delivered_at"] = json!(at);
        }
        response["supplements"] = json!(feedback.supplements);
    }
    Ok((StatusCode::CREATED, GoJson(response)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn body_validation_and_machine_type_refusal() {
        for raw in [
            br#"{}"#.as_slice(),
            b"null",
            br#"{"content":null}"#,
            br#"{"content":"\u0000"}"#,
        ] {
            assert_eq!(validate(raw).err().unwrap().1, "content is required");
        }
        for raw in [
            br#"{"content":true}"#.as_slice(),
            br#"{"content":"x","steer":[]}"#,
            b"[]",
        ] {
            assert_eq!(validate(raw).err().unwrap().1, "invalid request body");
        }
        for kind in ["system", "status_change", "invalid"] {
            assert_eq!(
                validate(json!({"content":"x","type":kind}).to_string().as_bytes())
                    .err()
                    .unwrap()
                    .1,
                "invalid comment type"
            );
        }
        let req = validate(br#"{"content":"a\u0000<&>\n**b**","type":null,"unknown":1}"#).unwrap();
        assert_eq!(req.content, "a<&>\n**b**");
        assert_eq!(req.kind, "comment");
        assert!(validate(br#"{"content":" "}"#).is_ok());
    }
    #[test]
    fn ids_and_steering_validation() {
        for (field, expected) in [
            ("parent_id", "invalid parent_id"),
            ("attachment_ids", "invalid attachment_ids"),
            ("suppress_agent_ids", "invalid suppress_agent_ids"),
            ("steer_task_ids", "invalid steer_task_ids"),
        ] {
            let mut body = json!({"content":"x"});
            body[field] = if field == "parent_id" {
                json!("bad")
            } else {
                json!(["bad"])
            };
            assert_eq!(
                validate(body.to_string().as_bytes()).err().unwrap().1,
                expected
            );
        }
        let id = Uuid::new_v4();
        let req = validate(
            json!({"content":"x","attachment_ids":[id],"steer":true,"steer_task_ids":[id]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
        assert!(!req.steer);
        assert!(req.steer_tasks.is_empty());
    }
}
