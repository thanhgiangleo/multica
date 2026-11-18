//! Trusted direct and parsed-mention notification targets; no provider delivery.
use super::channels_sprint_notify::{
    FanoutError, InboxDelivery, Notification, notification_id, preference_group,
};
use deadpool_postgres::Pool;
use std::{collections::BTreeSet, sync::Arc};
use tokio_postgres::{Error, Transaction};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectRecipient {
    Member(Uuid),
    Agent(Uuid),
}

/// Input must come from the trusted CommonMark mention parser after provider
/// authentication. This is NOT a parser for user-controlled raw markdown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParsedMention {
    Member(Uuid),
    Squad(Uuid),
    All,
}

/// Notify the exact assignee or previous human assignee. Unlike subscribers,
/// agents can own direct inbox items; squads cannot. Missing/foreign recipients
/// and shadow users are quiet skips, with no workspace-wide publication.
pub async fn notify_direct(
    pool: &Arc<Pool>,
    n: &Notification,
    recipient: DirectRecipient,
) -> Result<Vec<InboxDelivery>, FanoutError> {
    let mut db = pool.get().await.map_err(FanoutError::Pool)?;
    let tx = db.transaction().await?;
    let delivered = notify_direct_in_transaction(&tx, n, recipient).await?;
    tx.commit().await?;
    Ok(delivered)
}

pub(super) async fn notify_direct_in_transaction(
    tx: &Transaction<'_>,
    n: &Notification,
    recipient: DirectRecipient,
) -> Result<Vec<InboxDelivery>, Error> {
    let Some(status) = issue_status(tx, n).await? else {
        return Ok(Vec::new());
    };
    Ok(insert_eligible(tx, n, &status, recipient, &n.kind)
        .await?
        .into_iter()
        .collect())
}

/// Expand already-parsed @member/@squad/@all into workspace-local human
/// members. Other mention types (agent, issue) intentionally have no inbox
/// targets. Duplicate/actor/excluded/muted recipients never insert rows.
/// Call separately from subscriber `fanout` with a stable event UUID.
pub async fn notify_mentions(
    pool: &Arc<Pool>,
    n: &Notification,
    mentions: &[ParsedMention],
) -> Result<Vec<InboxDelivery>, FanoutError> {
    if n.kind != "mentioned" {
        return Err(FanoutError::InvalidKind);
    }
    let mut db = pool.get().await.map_err(FanoutError::Pool)?;
    let tx = db.transaction().await?;
    let delivered = notify_mentions_in_transaction(&tx, n, mentions).await?;
    tx.commit().await?;
    Ok(delivered)
}

pub(super) async fn notify_mentions_in_transaction(
    tx: &Transaction<'_>,
    n: &Notification,
    mentions: &[ParsedMention],
) -> Result<Vec<InboxDelivery>, Error> {
    let Some(status) = issue_status(tx, n).await? else {
        return Ok(Vec::new());
    };
    let mut recipients = BTreeSet::new();
    let mut squads = BTreeSet::new();
    let mut all = false;
    for mention in mentions {
        match mention {
            ParsedMention::Member(id) => {
                recipients.insert(*id);
            }
            ParsedMention::Squad(id) => {
                squads.insert(*id);
            }
            ParsedMention::All => all = true,
        }
    }
    if all {
        let rows = tx.query(
            "SELECT m.user_id FROM member m JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE m.workspace_id=$1 FOR SHARE OF m,u",
            &[&n.workspace],
        ).await?;
        recipients.extend(rows.into_iter().map(|r| r.get::<_, Uuid>(0)));
    }
    if !squads.is_empty() {
        let squad_ids: Vec<Uuid> = squads.into_iter().collect();
        let rows = tx.query(
            "SELECT m.user_id FROM squad s JOIN squad_member sm ON sm.squad_id=s.id AND sm.member_type='member' JOIN member m ON m.user_id=sm.member_id AND m.workspace_id=s.workspace_id JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE s.workspace_id=$1 AND s.id=ANY($2) FOR SHARE OF s,sm,m,u",
            &[&n.workspace, &squad_ids],
        ).await?;
        recipients.extend(rows.into_iter().map(|r| r.get::<_, Uuid>(0)));
    }
    let mut delivered = Vec::new();
    for member in recipients {
        if let Some(item) =
            insert_eligible(tx, n, &status, DirectRecipient::Member(member), "mentioned").await?
        {
            delivered.push(item);
        }
    }
    Ok(delivered)
}

async fn issue_status(tx: &Transaction<'_>, n: &Notification) -> Result<Option<String>, Error> {
    Ok(tx
        .query_opt(
            "SELECT status FROM issue WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[&n.issue, &n.workspace],
        )
        .await?
        .map(|r| r.get(0)))
}

async fn insert_eligible(
    tx: &Transaction<'_>,
    n: &Notification,
    status: &str,
    recipient: DirectRecipient,
    kind: &str,
) -> Result<Option<InboxDelivery>, Error> {
    let (id, recipient_type, id_domain) = match recipient {
        DirectRecipient::Member(id) => (id, "member", kind.to_owned()),
        DirectRecipient::Agent(id) => (id, "agent", format!("agent:{kind}")),
    };
    if n.actor == Some(id) || n.excluded.contains(&id) {
        return Ok(None);
    }
    let eligible = match recipient {
        DirectRecipient::Member(_) => {
            let row = tx.query_opt(
                "SELECT 1 FROM member m JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE m.workspace_id=$1 AND m.user_id=$2 FOR SHARE OF m,u",
                &[&n.workspace, &id],
            ).await?;
            if row.is_none() {
                return Ok(None);
            }
            if let Some(group) = preference_group(kind) {
                let muted = tx.query_opt(
                    "SELECT preferences->>$3 AS setting FROM notification_preference WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
                    &[&n.workspace, &id, &group],
                ).await?.is_some_and(|r| r.get::<_, Option<String>>(0).as_deref() == Some("muted"));
                if muted {
                    return Ok(None);
                }
            }
            "EXISTS (SELECT 1 FROM member m JOIN \"user\" u ON u.id=m.user_id AND NOT u.zbs_is_shadow WHERE m.workspace_id=$2 AND m.user_id=$3)"
        }
        DirectRecipient::Agent(_) => {
            if tx
                .query_opt(
                    "SELECT 1 FROM agent WHERE id=$1 AND workspace_id=$2 FOR SHARE",
                    &[&id, &n.workspace],
                )
                .await?
                .is_none()
            {
                return Ok(None);
            }
            "EXISTS (SELECT 1 FROM agent WHERE workspace_id=$2 AND id=$3)"
        }
    };
    let item_id = notification_id(n.event_id, n.issue, id, &id_domain);
    // `eligible` comes from two fixed literals, never from request data.
    let sql = format!(
        "INSERT INTO inbox_item(id,workspace_id,recipient_type,recipient_id,type,severity,issue_id,title,body,actor_type,actor_id,details) SELECT $1,$2,$4,$3,$5,$6,$7,$8,$9,$10,$11,$12 WHERE {eligible} ON CONFLICT (id) DO NOTHING RETURNING id"
    );
    let inserted = tx
        .query_opt(
            &sql,
            &[
                &item_id,
                &n.workspace,
                &id,
                &recipient_type,
                &kind,
                &n.severity,
                &n.issue,
                &n.title,
                &n.body,
                &n.actor_type,
                &n.actor,
                &n.details,
            ],
        )
        .await?
        .is_some();
    Ok(inserted.then_some(InboxDelivery {
        id: item_id,
        workspace: n.workspace,
        recipient: id,
        recipient_type,
        issue: n.issue,
        issue_status: status.to_owned(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_and_mention_targets_are_typed() {
        assert_ne!(
            notification_id(Uuid::nil(), Uuid::nil(), Uuid::nil(), "mentioned"),
            notification_id(Uuid::nil(), Uuid::nil(), Uuid::nil(), "agent:mentioned"),
        );
        assert_eq!(preference_group("mentioned"), Some("mentions"));
        assert_eq!(preference_group("issue_assigned"), Some("assignments"));
    }
}
