//! Subscriber notification fanout for trusted channel events; no workspace-wide inbox broadcast.
use deadpool_postgres::Pool;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use tokio_postgres::{Error, Transaction};
use uuid::Uuid;

pub struct Notification {
    pub event_id: Uuid,
    pub workspace: Uuid,
    pub issue: Uuid,
    pub actor: Option<Uuid>,
    pub actor_type: String,
    pub excluded: HashSet<Uuid>,
    pub kind: String,
    pub severity: String,
    pub title: String,
    pub body: String,
    pub details: Value,
}

/// A recipient-scoped row. Publish member rows only to that member's personal
/// stream after commit. Agent rows must never enter a member stream; an agent
/// inbox reader may consume them separately. Never publish to a workspace feed.
pub struct InboxDelivery {
    pub id: Uuid,
    pub workspace: Uuid,
    pub recipient: Uuid,
    pub recipient_type: &'static str,
    pub issue: Uuid,
    pub issue_status: String,
}

pub(super) fn preference_group(kind: &str) -> Option<&'static str> {
    match kind {
        "issue_assigned" | "unassigned" | "assignee_changed" => Some("assignments"),
        "status_changed" => Some("status_changes"),
        "new_comment" => Some("comments"),
        "mentioned" => Some("mentions"),
        "priority_changed" | "start_date_changed" | "due_date_changed" => Some("updates"),
        "task_completed" | "task_failed" | "agent_blocked" | "agent_completed" => {
            Some("agent_activity")
        }
        _ => None,
    }
}

fn handoff(status: &str, category: Option<&str>) -> bool {
    match status {
        "done" | "cancelled" | "in_review" | "blocked" => true,
        "in_progress" | "backlog" | "todo" => false,
        _ => matches!(category, Some("done" | "closed" | "started")),
    }
}

fn delegated_allowed(kind: &str, is_handoff: bool) -> bool {
    matches!(kind, "mentioned" | "task_failed" | "agent_blocked")
        || (kind == "status_changed" && is_handoff)
}

pub(super) fn notification_id(event: Uuid, issue: Uuid, member: Uuid, kind: &str) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"multica:channel-notification:v1");
    h.update(event.as_bytes());
    h.update(issue.as_bytes());
    h.update(member.as_bytes());
    h.update(kind.as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[derive(Debug)]
pub enum FanoutError {
    Pool(deadpool_postgres::PoolError),
    Database(Error),
    InvalidKind,
    UnknownStatus,
    InvalidStatusCategory,
    InvalidEvent,
}
impl From<Error> for FanoutError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}

/// Records a trusted event's member-only inbox rows. Event ID must remain stable
/// across retries. Transaction holds issue and membership locks until rows commit.
/// Recipient-scoped deliveries are returned only for newly inserted rows.
pub async fn fanout(pool: &Arc<Pool>, n: &Notification) -> Result<Vec<InboxDelivery>, FanoutError> {
    let mut db = pool.get().await.map_err(FanoutError::Pool)?;
    let tx = db.transaction().await?;
    let delivered = fanout_in_transaction(&tx, n).await?;
    tx.commit().await?;
    Ok(delivered)
}

pub(super) async fn fanout_in_transaction(
    tx: &Transaction<'_>,
    n: &Notification,
) -> Result<Vec<InboxDelivery>, FanoutError> {
    let issue = tx
        .query_opt(
            "SELECT status,parent_issue_id FROM issue WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[&n.issue, &n.workspace],
        )
        .await?;
    let Some(issue) = issue else {
        return Ok(Vec::new());
    };
    let status: String = issue.get("status");
    let parent: Option<Uuid> = issue.get("parent_issue_id");
    let is_handoff = status_handoff_in_transaction(tx, n.workspace, &status).await?;
    let mut already = n.excluded.clone();
    if let Some(actor) = n.actor {
        already.insert(actor);
    }
    let mut suppressed = HashSet::new();
    let mut delivered = Vec::new();
    fanout_issue(
        tx,
        n,
        n.issue,
        &status,
        is_handoff,
        &mut already,
        &mut suppressed,
        &mut delivered,
    )
    .await?;
    if n.kind == "status_changed"
        && let Some(parent) = parent
    {
        // A parent subscription can only inherit a notification from a child
        // inside the same workspace. Suppressed delegated recipients cannot
        // return through their parent subscription.
        let same_workspace = tx
            .query_opt(
                "SELECT 1 FROM issue WHERE id=$1 AND workspace_id=$2 FOR SHARE",
                &[&parent, &n.workspace],
            )
            .await?
            .is_some();
        if same_workspace {
            already.extend(suppressed);
            fanout_issue(
                tx,
                n,
                parent,
                &status,
                is_handoff,
                &mut already,
                &mut HashSet::new(),
                &mut delivered,
            )
            .await?;
        }
    }
    Ok(delivered)
}

pub(super) async fn status_handoff_in_transaction(
    tx: &Transaction<'_>,
    workspace: Uuid,
    status: &str,
) -> Result<bool, FanoutError> {
    let category = if matches!(
        status,
        "done" | "cancelled" | "in_review" | "blocked" | "in_progress" | "backlog" | "todo"
    ) {
        None
    } else {
        let row = tx
            .query_opt(
                "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2 FOR SHARE",
                &[&workspace, &status],
            )
            .await?
            .ok_or(FanoutError::UnknownStatus)?;
        let category: String = row.get(0);
        if !matches!(
            category.as_str(),
            "unstarted" | "started" | "done" | "closed"
        ) {
            return Err(FanoutError::InvalidStatusCategory);
        }
        Some(category)
    };
    Ok(handoff(status, category.as_deref()))
}

#[allow(clippy::too_many_arguments)]
async fn fanout_issue(
    tx: &Transaction<'_>,
    n: &Notification,
    subscriber_issue: Uuid,
    status: &str,
    is_handoff: bool,
    already: &mut HashSet<Uuid>,
    suppressed: &mut HashSet<Uuid>,
    delivered: &mut Vec<InboxDelivery>,
) -> Result<(), Error> {
    let rows = tx.query("SELECT s.user_id,s.reason,COALESCE(p.preferences,'{}'::jsonb) AS preferences FROM issue_subscriber s JOIN member m ON m.user_id=s.user_id AND m.workspace_id=$2 JOIN \"user\" u ON u.id=s.user_id AND NOT u.zbs_is_shadow LEFT JOIN notification_preference p ON p.user_id=s.user_id AND p.workspace_id=$2 WHERE s.issue_id=$1 AND s.user_type='member' AND s.unsubscribed_at IS NULL FOR SHARE OF s,m,u", &[&subscriber_issue, &n.workspace]).await?;
    let group = preference_group(&n.kind);
    for row in rows {
        let member: Uuid = row.get("user_id");
        if already.contains(&member) {
            continue;
        }
        let prefs: Value = row.get("preferences");
        if group.is_some_and(|g| prefs.get(g).and_then(Value::as_str) == Some("muted")) {
            continue;
        }
        let reason: String = row.get("reason");
        if reason == "delegated" && !delegated_allowed(&n.kind, is_handoff) {
            suppressed.insert(member);
            continue;
        }
        let id = notification_id(n.event_id, n.issue, member, &n.kind);
        // Recheck eligibility at INSERT, so a stale subscriber cannot populate a
        // private inbox after their membership is removed.
        let inserted = tx.query_opt("INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title,body,actor_type,actor_id,details) SELECT $1,$2,'member',$3,$4,$5,$6,$7,$8,$11,$9,$10 WHERE EXISTS (SELECT 1 FROM member WHERE workspace_id=$2 AND user_id=$3) ON CONFLICT (id) DO NOTHING RETURNING id", &[&id,&n.workspace,&member,&n.kind,&n.severity,&n.issue,&n.title,&n.body,&n.actor,&n.details,&n.actor_type]).await?.is_some();
        already.insert(member);
        if inserted {
            delivered.push(InboxDelivery {
                id,
                workspace: n.workspace,
                recipient: member,
                recipient_type: "member",
                issue: n.issue,
                issue_status: status.to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channel_notification_policy_matches_go_oracle() {
        assert!(delegated_allowed(
            "status_changed",
            handoff("in_review", None)
        ));
        assert!(!delegated_allowed(
            "status_changed",
            handoff("in_progress", None)
        ));
        assert!(handoff("custom_review", Some("started")));
        assert!(!handoff("custom_queue", Some("unstarted")));
        assert!(!handoff("unknown", None));
        assert!(!delegated_allowed("new_comment", true));
        assert!(delegated_allowed("mentioned", false));
        assert_eq!(preference_group("task_failed"), Some("agent_activity"));
        assert_eq!(preference_group("other"), None);
        assert_eq!(
            notification_id(Uuid::nil(), Uuid::nil(), Uuid::nil(), "mentioned"),
            notification_id(Uuid::nil(), Uuid::nil(), Uuid::nil(), "mentioned")
        );
    }
}
