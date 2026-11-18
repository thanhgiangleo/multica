//! Transactional mapping of trusted issue/comment events into member/agent inbox rows.
//! Integrator supplies parsed mentions and publishes only returned personal deliveries.
use super::{
    channels_sprint_notify::{
        FanoutError, InboxDelivery, Notification, fanout_in_transaction,
        status_handoff_in_transaction,
    },
    channels_sprint_notify_targets::{
        DirectRecipient, ParsedMention, notify_direct_in_transaction,
        notify_mentions_in_transaction,
    },
};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

/// Publish only to this member's personal stream after commit. Never publish
/// archive counts to workspace feeds or route agent recipients to member feeds.
#[derive(Debug, PartialEq, Eq)]
pub struct InboxArchive {
    pub workspace: Uuid,
    pub issue: Uuid,
    pub recipient: Uuid,
    pub count: i64,
    pub reason: &'static str,
}

#[derive(Default)]
pub struct EventOutcome {
    pub deliveries: Vec<InboxDelivery>,
    pub archived: Vec<InboxArchive>,
}
use uuid::Uuid;

pub enum ChangeField {
    Status,
    Priority,
    StartDate,
    DueDate,
}

#[derive(Clone, Copy)]
pub enum Assignee {
    Member(Uuid),
    Agent(Uuid),
    Squad(Uuid),
}

pub enum TrustedEventKind {
    IssueCreated {
        mentions: Vec<ParsedMention>,
    },
    AssigneeChanged {
        previous: Option<Assignee>,
    },
    DescriptionMentionsAdded {
        mentions: Vec<ParsedMention>,
    },
    CommentCreated {
        comment: Uuid,
        mentions: Vec<ParsedMention>,
    },
    IssueReactionAdded {
        reaction: Uuid,
    },
    CommentReactionAdded {
        reaction: Uuid,
    },
    TaskFailed {
        task: Uuid,
    },
    TaskCompleted {
        task: Uuid,
    },
    FieldChanged {
        field: ChangeField,
        from: String,
        to: String,
    },
}

/// This input is exclusively for the authenticated event listener, not an HTTP
/// endpoint or a channel webhook. ID must be stable on replay. Comment contents
/// and author type are read from the database, not supplied by the event caller.
pub struct TrustedEvent {
    pub id: Uuid,
    pub workspace: Uuid,
    pub issue: Uuid,
    pub actor: Option<Uuid>,
    pub actor_type: String,
    pub kind: TrustedEventKind,
}

fn notification(
    e: &TrustedEvent,
    title: &str,
    kind: &str,
    severity: &str,
    body: &str,
    details: Value,
    excluded: HashSet<Uuid>,
) -> Notification {
    Notification {
        event_id: e.id,
        workspace: e.workspace,
        issue: e.issue,
        actor: e.actor,
        actor_type: e.actor_type.clone(),
        excluded,
        kind: kind.to_owned(),
        severity: severity.to_owned(),
        title: title.to_owned(),
        body: body.to_owned(),
        details,
    }
}

/// All inbox writes for one event commit together. An error rolls back direct,
/// subscriber, mention and archive changes. Result contains only newly inserted
/// rows and newly archived member counts, all recipient-scoped. Caller must never
/// publish them to a workspace-wide feed.
pub async fn apply_trusted_event(
    pool: &Arc<Pool>,
    e: &TrustedEvent,
) -> Result<EventOutcome, FanoutError> {
    let mut db = pool.get().await.map_err(FanoutError::Pool)?;
    let tx = db.transaction().await?;
    let Some(issue) = tx.query_opt(
        "SELECT title,status,assignee_type,assignee_id FROM issue WHERE id=$1 AND workspace_id=$2 FOR SHARE",
        &[&e.issue, &e.workspace],
    ).await? else {
        return Ok(EventOutcome::default());
    };
    let title: String = issue.get("title");
    let status: String = issue.get("status");
    let assignee_kind: Option<String> = issue.get("assignee_type");
    let assignee_id: Option<Uuid> = issue.get("assignee_id");
    let assignee = match (assignee_kind.as_deref(), assignee_id) {
        (Some("member"), Some(id)) => Some(Assignee::Member(id)),
        (Some("agent"), Some(id)) => Some(Assignee::Agent(id)),
        (Some("squad"), Some(id)) => Some(Assignee::Squad(id)),
        _ => None,
    };
    let mut delivered = Vec::new();
    let mut archived = Vec::new();
    match &e.kind {
        TrustedEventKind::IssueCreated { mentions } => {
            let mut excluded = HashSet::new();
            if let Some(recipient) = assignee.filter(|a| direct_recipient(*a).is_some()) {
                let n = notification(
                    e,
                    &title,
                    "issue_assigned",
                    "action_required",
                    "",
                    json!({}),
                    HashSet::new(),
                );
                if let Some(direct) = direct_recipient(recipient) {
                    delivered.extend(notify_direct_in_transaction(&tx, &n, direct).await?);
                }
                excluded.insert(recipient_id(recipient));
            }
            if !mentions.is_empty() {
                let n = notification(e, &title, "mentioned", "info", "", json!({}), excluded);
                delivered.extend(notify_mentions_in_transaction(&tx, &n, mentions).await?);
            }
        }
        TrustedEventKind::AssigneeChanged { previous } => {
            let mut details = serde_json::Map::new();
            if let Some(old) = previous {
                details.insert("prev_assignee_type".into(), json!(recipient_type(*old)));
                details.insert(
                    "prev_assignee_id".into(),
                    json!(recipient_id(*old).to_string()),
                );
            }
            if let Some(new) = assignee {
                details.insert("new_assignee_type".into(), json!(recipient_type(new)));
                details.insert(
                    "new_assignee_id".into(),
                    json!(recipient_id(new).to_string()),
                );
            }
            let details = Value::Object(details);
            let mut excluded = HashSet::new();
            if let Some(new) = assignee {
                excluded.insert(recipient_id(new));
                let n = notification(
                    e,
                    &title,
                    "issue_assigned",
                    "action_required",
                    "",
                    details.clone(),
                    HashSet::new(),
                );
                if let Some(direct) = direct_recipient(new) {
                    delivered.extend(notify_direct_in_transaction(&tx, &n, direct).await?);
                }
            }
            if let Some(old) = previous {
                excluded.insert(recipient_id(*old));
                if let Assignee::Member(id) = old {
                    let n = notification(
                        e,
                        &title,
                        "unassigned",
                        "info",
                        "",
                        details.clone(),
                        HashSet::new(),
                    );
                    delivered.extend(
                        notify_direct_in_transaction(&tx, &n, DirectRecipient::Member(*id)).await?,
                    );
                }
            }
            let n = notification(e, &title, "assignee_changed", "info", "", details, excluded);
            delivered.extend(fanout_in_transaction(&tx, &n).await?);
        }
        TrustedEventKind::DescriptionMentionsAdded { mentions } => {
            if !mentions.is_empty() {
                let n = notification(
                    e,
                    &title,
                    "mentioned",
                    "info",
                    "",
                    json!({}),
                    HashSet::new(),
                );
                delivered.extend(notify_mentions_in_transaction(&tx, &n, mentions).await?);
            }
        }
        TrustedEventKind::CommentCreated { comment, mentions } => {
            let Some(row) = tx
                .query_opt(
                    "SELECT content,author_type FROM comment WHERE id=$1 AND issue_id=$2 AND workspace_id=$3 FOR SHARE",
                    &[comment, &e.issue, &e.workspace],
                )
                .await?
            else {
                return Ok(EventOutcome::default());
            };
            let author_type: String = row.get("author_type");
            if author_type == "system" {
                return Ok(EventOutcome::default());
            }
            let content: String = row.get("content");
            let details = json!({"comment_id":comment.to_string()});
            let n = notification(
                e,
                &title,
                "new_comment",
                "info",
                &content,
                details.clone(),
                HashSet::new(),
            );
            delivered.extend(fanout_in_transaction(&tx, &n).await?);
            if !mentions.is_empty() {
                let n = notification(e, &title, "mentioned", "info", "", details, HashSet::new());
                delivered.extend(notify_mentions_in_transaction(&tx, &n, mentions).await?);
            }
        }
        TrustedEventKind::IssueReactionAdded { reaction } => {
            let Some(row) = tx.query_opt(
                "SELECT r.emoji,r.actor_type,r.actor_id,i.creator_type,i.creator_id FROM issue_reaction r JOIN issue i ON i.id=r.issue_id AND i.workspace_id=r.workspace_id WHERE r.id=$1 AND r.issue_id=$2 AND r.workspace_id=$3 FOR SHARE OF r,i",
                &[reaction, &e.issue, &e.workspace],
            ).await? else {
                return Ok(EventOutcome::default());
            };
            if row.get::<_, String>("actor_type") != e.actor_type
                || Some(row.get::<_, Uuid>("actor_id")) != e.actor
            {
                return Err(FanoutError::InvalidEvent);
            }
            if let Some(recipient) =
                reaction_recipient(&row.get::<_, String>("creator_type"), row.get("creator_id"))
            {
                let n = notification(
                    e,
                    &title,
                    "reaction_added",
                    "info",
                    "",
                    json!({"emoji":row.get::<_, String>("emoji")}),
                    HashSet::new(),
                );
                delivered.extend(notify_direct_in_transaction(&tx, &n, recipient).await?);
            }
        }
        TrustedEventKind::CommentReactionAdded { reaction } => {
            let Some(row) = tx.query_opt(
                "SELECT r.emoji,r.actor_type,r.actor_id,c.id AS comment_id,c.author_type,c.author_id FROM comment_reaction r JOIN comment c ON c.id=r.comment_id AND c.workspace_id=r.workspace_id WHERE r.id=$1 AND c.issue_id=$2 AND r.workspace_id=$3 FOR SHARE OF r,c",
                &[reaction, &e.issue, &e.workspace],
            ).await? else {
                return Ok(EventOutcome::default());
            };
            if row.get::<_, String>("actor_type") != e.actor_type
                || Some(row.get::<_, Uuid>("actor_id")) != e.actor
            {
                return Err(FanoutError::InvalidEvent);
            }
            if let Some(recipient) =
                reaction_recipient(&row.get::<_, String>("author_type"), row.get("author_id"))
            {
                let n = notification(
                    e,
                    &title,
                    "reaction_added",
                    "info",
                    "",
                    json!({"emoji":row.get::<_, String>("emoji"),"comment_id":row.get::<_, Uuid>("comment_id").to_string()}),
                    HashSet::new(),
                );
                delivered.extend(notify_direct_in_transaction(&tx, &n, recipient).await?);
            }
        }
        TrustedEventKind::TaskFailed { task } | TrustedEventKind::TaskCompleted { task } => {
            let Some(row) = tx.query_opt(
                "SELECT t.agent_id,t.status FROM agent_task_queue t JOIN agent a ON a.id=t.agent_id AND a.workspace_id=$3 WHERE t.id=$1 AND t.issue_id=$2 FOR SHARE OF t,a",
                &[task, &e.issue, &e.workspace],
            ).await? else {
                return Ok(EventOutcome::default());
            };
            if e.actor_type != "agent" || Some(row.get::<_, Uuid>("agent_id")) != e.actor {
                return Err(FanoutError::InvalidEvent);
            }
            let expected = if matches!(e.kind, TrustedEventKind::TaskFailed { .. }) {
                "failed"
            } else {
                "completed"
            };
            if row.get::<_, String>("status") != expected {
                return Err(FanoutError::InvalidEvent);
            }
            // Go does not create a task_completed inbox row; status changes
            // carry completion. Failure reaches subscribers, including delegates.
            if expected == "failed" {
                let n = notification(
                    e,
                    &title,
                    "task_failed",
                    "action_required",
                    "",
                    json!({}),
                    HashSet::new(),
                );
                delivered.extend(fanout_in_transaction(&tx, &n).await?);
            }
        }
        TrustedEventKind::FieldChanged { field, from, to } => {
            let kind = match field {
                ChangeField::Status => "status_changed",
                ChangeField::Priority => "priority_changed",
                ChangeField::StartDate => "start_date_changed",
                ChangeField::DueDate => "due_date_changed",
            };
            let n = notification(
                e,
                &title,
                kind,
                "info",
                "",
                json!({"from":from,"to":to}),
                HashSet::new(),
            );
            delivered.extend(fanout_in_transaction(&tx, &n).await?);
            if matches!(field, ChangeField::Status)
                && status != "blocked"
                && status_handoff_in_transaction(&tx, e.workspace, &status).await?
            {
                // Preserve failure history while work is stalled. Archive only
                // this issue's active failure notices, never parent/sibling rows.
                let rows = tx.query(
                    "UPDATE inbox_item SET archived=true WHERE workspace_id=$1 AND issue_id=$2 AND type='task_failed' AND NOT archived RETURNING recipient_type,recipient_id",
                    &[&e.workspace, &e.issue],
                ).await?;
                let mut counts = BTreeMap::<Uuid, i64>::new();
                for row in rows {
                    if row.get::<_, String>("recipient_type") == "member" {
                        *counts.entry(row.get("recipient_id")).or_default() += 1;
                    }
                }
                archived.extend(counts.into_iter().map(|(recipient, count)| InboxArchive {
                    workspace: e.workspace,
                    issue: e.issue,
                    recipient,
                    count,
                    reason: "issue_status_terminal",
                }));
            }
        }
    }
    tx.commit().await?;
    Ok(EventOutcome {
        deliveries: delivered,
        archived,
    })
}

fn reaction_recipient(kind: &str, id: Uuid) -> Option<DirectRecipient> {
    match kind {
        "member" => Some(DirectRecipient::Member(id)),
        "agent" => Some(DirectRecipient::Agent(id)),
        _ => None,
    }
}

fn direct_recipient(assignee: Assignee) -> Option<DirectRecipient> {
    match assignee {
        Assignee::Member(id) => Some(DirectRecipient::Member(id)),
        Assignee::Agent(id) => Some(DirectRecipient::Agent(id)),
        Assignee::Squad(_) => None,
    }
}

fn recipient_id(assignee: Assignee) -> Uuid {
    match assignee {
        Assignee::Member(id) | Assignee::Agent(id) | Assignee::Squad(id) => id,
    }
}

fn recipient_type(assignee: Assignee) -> &'static str {
    match assignee {
        Assignee::Member(_) => "member",
        Assignee::Agent(_) => "agent",
        Assignee::Squad(_) => "squad",
    }
}
