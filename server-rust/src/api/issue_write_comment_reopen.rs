//! Reopening a resolved thread is a follow-up to a committed reply, not part of it.
use super::super::{
    ApiState,
    issue_write::{IssueCommentResolutionEvent, SharedIssueEventSink},
    timestamp,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

pub(super) async fn snapshot(
    db: &Client,
    workspace: Uuid,
    issue: Uuid,
    parent: Option<Uuid>,
) -> Option<Uuid> {
    let parent = parent?;
    // Mirror GetThreadRoot. Missing roots and lookup failures never block a reply;
    // parent validation still occurs under the issue lock in the create path.
    match db.query_opt(
        "WITH RECURSIVE root_of AS (SELECT id,parent_id FROM comment WHERE id=$1 AND workspace_id=$2 AND issue_id=$3 UNION SELECT p.id,p.parent_id FROM comment p JOIN root_of r ON p.id=r.parent_id WHERE p.workspace_id=$2 AND p.issue_id=$3) SELECT id FROM comment WHERE id=(SELECT id FROM root_of WHERE parent_id IS NULL LIMIT 1) AND workspace_id=$2 AND issue_id=$3 AND resolved_at IS NOT NULL",
        &[&parent, &workspace, &issue],
    ).await {
        Ok(row) => row.map(|row| row.get(0)),
        Err(err) => {
            eprintln!("comment root lookup failed for {parent}: {err}");
            None
        }
    }
}

pub(super) async fn after_commit(
    state: &ApiState,
    sink: &SharedIssueEventSink,
    workspace: Uuid,
    root: Uuid,
    actor_type: &'static str,
    actor_id: Uuid,
    source_task_id: &str,
) {
    if let Err(err) = reopen(
        state,
        sink,
        workspace,
        root,
        actor_type,
        actor_id,
        source_task_id,
    )
    .await
    {
        eprintln!("auto-unresolve on reply failed for {root}: {err}");
    }
}

async fn reopen(
    state: &ApiState,
    sink: &SharedIssueEventSink,
    workspace: Uuid,
    root: Uuid,
    actor_type: &'static str,
    actor_id: Uuid,
    source_task_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut db = state.pool.get().await?;
    let tx = db.transaction().await?;
    tx.execute(
        "SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)",
        &[&actor_type, &actor_id.to_string(), &source_task_id],
    ).await?;
    // Go's UnresolveComment is idempotent; even a concurrent unresolve still
    // publishes from the resolved root captured when the reply began.
    let row = tx.query_one(
        "UPDATE comment SET resolved_at=NULL,resolved_by_type=NULL,resolved_by_id=NULL,revision=revision+CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END,updated_at=CASE WHEN resolved_at IS NOT NULL THEN now() ELSE updated_at END WHERE id=$1 AND workspace_id=$2 RETURNING id,issue_id,author_type,author_id,content,type,parent_id,created_at,updated_at,resolved_at,resolved_by_type,resolved_by_id,revision",
        &[&root, &workspace],
    ).await?;
    let comment: Value = json!({
        "id":row.get::<_, Uuid>("id"),
        "issue_id":row.get::<_, Uuid>("issue_id"),
        "author_type":row.get::<_, String>("author_type"),
        "author_id":row.get::<_, Uuid>("author_id"),
        "content":row.get::<_, String>("content"),
        "type":row.get::<_, String>("type"),
        "parent_id":row.get::<_, Option<Uuid>>("parent_id"),
        "created_at":timestamp(row.get::<_, DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_, DateTime<Utc>>("updated_at")),
        "resolved_at":row.get::<_, Option<DateTime<Utc>>>("resolved_at").map(timestamp),
        "resolved_by_type":row.get::<_, Option<String>>("resolved_by_type"),
        "resolved_by_id":row.get::<_, Option<Uuid>>("resolved_by_id"),
        "revision":row.get::<_, i64>("revision"),
    });
    tx.commit().await?;
    sink.publish_comment_resolution(IssueCommentResolutionEvent {
        event_type: "comment:unresolved",
        workspace_id: workspace,
        actor_type,
        actor_id,
        payload: json!({"comment":comment}),
    });
    Ok(())
}
