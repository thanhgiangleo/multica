//! Transactional workflow status remap. Callers hold catalog and project locks;
//! integration must publish snapshots and cancelled tasks only after commit.
use deadpool_postgres::Transaction;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

pub struct RemapEffects {
    pub changed: Vec<Row>,
    pub cancelled_wakeups: Vec<Row>,
}

pub async fn remap_issues(
    tx: &Transaction<'_>,
    workspace: Uuid,
    projects: &[Uuid],
    mapping: &[(String, String)],
) -> Result<RemapEffects, Error> {
    let mut effects = RemapEffects {
        changed: Vec::new(),
        cancelled_wakeups: Vec::new(),
    };
    for (from, to) in mapping {
        let rows = tx
            .query(
                "UPDATE issue AS i SET status=$4, duplicate_of_issue_id=NULL, \
                 position=(SELECT COALESCE(MIN(target.position),0)-1 FROM issue AS target \
                   WHERE target.workspace_id=i.workspace_id AND target.status=$4), \
                 revision=i.revision+1, \
                 last_activity_at=GREATEST(COALESCE(i.last_activity_at,i.updated_at),now()), \
                 updated_at=now() \
                 WHERE i.workspace_id=$1 AND i.project_id=ANY($2::uuid[]) \
                   AND i.status=$3 AND i.status<>$4 RETURNING i.*",
                &[&workspace, &projects, from, to],
            )
            .await?;
        for issue in &rows {
            let id: Uuid = issue.get("id");
            let status: String = issue.get("status");
            let closed = if matches!(status.as_str(), "done" | "cancelled") {
                true
            } else if matches!(
                status.as_str(),
                "backlog" | "todo" | "in_progress" | "in_review" | "blocked"
            ) {
                false
            } else {
                let category: String = tx
                    .query_one(
                        "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
                        &[&workspace, &status],
                    )
                    .await?
                    .get(0);
                matches!(category.as_str(), "done" | "closed")
            };
            if closed {
                tx.execute(
                    "UPDATE issue_wakeup SET enabled=false,disabled_at=clock_timestamp(), \
                     updated_at=clock_timestamp() \
                     WHERE issue_id=$1 AND disabled_at IS NULL AND system_rule IS NULL",
                    &[&id],
                )
                .await?;
                effects.cancelled_wakeups.extend(
                    tx.query(
                        "UPDATE agent_task_queue SET status='cancelled',completed_at=now(), \
                         error='Issue closed; wakeup disabled' \
                         WHERE issue_id=$1 AND context->>'wakeup_id' IS NOT NULL \
                           AND status IN ('queued','deferred') AND started_at IS NULL RETURNING *",
                        &[&id],
                    )
                    .await?,
                );
            }
        }
        effects.changed.extend(rows);
    }
    Ok(effects)
}
