//! Source-context read projection for bounded captured threads.
//! Attachment references are projected to stable identities before comparison.
use super::{ApiError, error, timestamp};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

fn unavailable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "issue source context rendering unavailable",
    )
}
fn is_false(value: &bool) -> bool {
    !value
}
#[path = "queries_sprint_source_context_markdown.rs"]
mod markdown;
use markdown::{
    description_attachment_changes, description_projection, markdown_content_projection,
};
fn failed() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to load issue source context",
    )
}

#[derive(Clone, Deserialize, Serialize)]
struct SourceAttachment {
    id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_attachment_id: Option<Uuid>,
    owner_type: String,
    owner_id: Uuid,
    filename: String,
    content_type: String,
    size_bytes: i64,
    created_at: String,
}

impl SourceAttachment {
    fn comparable(&self) -> (Uuid, &str, &str, i64) {
        (
            self.source_attachment_id.unwrap_or(self.id),
            &self.filename,
            &self.content_type,
            self.size_bytes,
        )
    }
    fn from_row(row: &Row, owner_type: &str, owner_id: Uuid) -> Self {
        Self {
            id: row.get("id"),
            source_attachment_id: None,
            owner_type: owner_type.to_owned(),
            owner_id,
            filename: row.get("filename"),
            content_type: row.get("content_type"),
            size_bytes: row.get("size_bytes"),
            created_at: timestamp(row.get("created_at")),
        }
    }
}

fn valid_attachments(items: &[SourceAttachment], owner_type: &str, owner_id: Uuid) -> bool {
    let mut ids = HashSet::new();
    items.iter().all(|item| {
        item.owner_type == owner_type
            && item.owner_id == owner_id
            && item.size_bytes >= 0
            && ids.insert(item.id)
    })
}

fn comparable_attachments(items: &[SourceAttachment]) -> Vec<(Uuid, &str, &str, i64)> {
    items.iter().map(SourceAttachment::comparable).collect()
}

#[derive(Deserialize, Serialize)]
struct SourceIssue {
    id: Uuid,
    identifier: String,
    number: i32,
    title: String,
    description: Option<String>,
    created_at: String,
    updated_at: String,
    revision: i64,
    attachments: Vec<SourceAttachment>,
}
#[derive(Clone, Deserialize, Serialize)]
struct Author {
    #[serde(rename = "type")]
    kind: String,
    id: Uuid,
    name: String,
}
#[derive(Clone, Deserialize, Serialize)]
struct SourceComment {
    id: Uuid,
    parent_id: Option<Uuid>,
    #[serde(rename = "type")]
    kind: String,
    content: String,
    author: Author,
    created_at: String,
    updated_at: String,
    revision: i64,
    attachments: Vec<SourceAttachment>,
    #[serde(default, skip_serializing_if = "is_false")]
    deleted: bool,
}
#[derive(Deserialize, Serialize)]
struct Snapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<i16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    captured_by_user_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    captured_at: Option<String>,
    source_issue: SourceIssue,
    comment_thread: Vec<SourceComment>,
    anchor_comment_id: Uuid,
}

fn current_thread_ids(rows: &[Row]) -> Vec<Uuid> {
    rows.iter().map(|row| row.get("id")).collect()
}

async fn source_author_states(
    tx: &Transaction<'_>,
    ws: Uuid,
    snapshot: &Snapshot,
) -> Result<Vec<Value>, ApiError> {
    let mut authors = Vec::new();
    let mut seen_authors = HashSet::new();
    // Fetch author state in two workspace-scoped batches rather than up to 256 round trips.
    let member_ids: Vec<Uuid> = snapshot
        .comment_thread
        .iter()
        .filter(|comment| comment.author.kind == "member")
        .map(|comment| comment.author.id)
        .collect();
    let agent_ids: Vec<Uuid> = snapshot
        .comment_thread
        .iter()
        .filter(|comment| comment.author.kind == "agent")
        .map(|comment| comment.author.id)
        .collect();
    let member_names: HashMap<Uuid, String> = tx
        .query(
            "SELECT m.user_id,u.name FROM member m JOIN \"user\" u ON u.id=m.user_id \
         WHERE m.workspace_id=$1 AND m.user_id=ANY($2::uuid[])",
            &[&ws, &member_ids],
        )
        .await
        .map_err(|_| failed())?
        .into_iter()
        .map(|row| (row.get("user_id"), row.get("name")))
        .collect();
    let agent_names: HashMap<Uuid, (String, bool)> = tx
        .query(
            "SELECT id,name,archived_at IS NOT NULL AS archived FROM agent \
         WHERE workspace_id=$1 AND id=ANY($2::uuid[])",
            &[&ws, &agent_ids],
        )
        .await
        .map_err(|_| failed())?
        .into_iter()
        .map(|row| (row.get("id"), (row.get("name"), row.get("archived"))))
        .collect();
    for comment in &snapshot.comment_thread {
        let key = (comment.author.kind.as_str(), comment.author.id);
        if !seen_authors.insert(key) {
            continue;
        }
        let current = if comment.author.kind == "member" {
            member_names
                .get(&comment.author.id)
                .map(|name| (name, false))
        } else {
            agent_names
                .get(&comment.author.id)
                .map(|(name, archived)| (name, *archived))
        };
        let state = match current {
            None if comment.author.kind == "member" => "no_longer_in_workspace",
            None => "deleted_agent",
            Some((_, true)) => "archived",
            Some((name, _)) if name != &comment.author.name => "renamed",
            _ => "unchanged",
        };
        let mut value = json!({"type":comment.author.kind,"id":comment.author.id,
            "captured_name":comment.author.name,"state":state});
        if let Some((name, _)) = current {
            value["current_name"] = json!(name);
        }
        authors.push(value);
    }
    Ok(authors)
}

async fn unavailable_current(
    tx: &Transaction<'_>,
    ws: Uuid,
    row: &Row,
    version: i16,
    snapshot: Snapshot,
) -> Result<Option<Value>, ApiError> {
    let captured_at: chrono::DateTime<chrono::Utc> = row.get("captured_at");
    let authors = source_author_states(tx, ws, &snapshot).await?;
    Ok(Some(json!({
        "id": row.get::<_, Uuid>("id"), "version": version,
        "usage": "read_only_historical_background", "captured_at": timestamp(captured_at),
        "display_state": "unavailable", "source_issue_state": "unavailable",
        "comment_thread_state": "unavailable", "anchor_comment_state": "unavailable",
        "can_open_current_source": false, "change_reasons": [],
        "change_details": {"changed_comment_ids": [], "description_attachment_changes": []},
        "snapshot": snapshot, "source_author_state": authors,
    })))
}

pub(super) async fn for_issue(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
) -> Result<Option<Value>, ApiError> {
    let rows = tx
        .query(
            "SELECT id,source_issue_id,anchor_comment_id,snapshot_version,snapshot,captured_at \
         FROM issue_source_context WHERE workspace_id=$1 AND issue_id=$2 AND state='attached' LIMIT 2",
            &[&ws, &issue],
        )
        .await
        .map_err(|_| failed())?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if rows.len() != 1 {
        return Err(unavailable());
    }
    let source_id: Uuid = row.get("source_issue_id");
    let anchor_id: Uuid = row.get("anchor_comment_id");
    let version: i16 = row.get("snapshot_version");
    let raw_snapshot: Value = row.get("snapshot");
    if serde_json::to_vec(&raw_snapshot)
        .map_err(|_| unavailable())?
        .len()
        > 1 << 20
    {
        return Err(unavailable());
    }
    let snapshot: Snapshot = serde_json::from_value(raw_snapshot).map_err(|_| unavailable())?;
    let attachment_bytes = snapshot
        .source_issue
        .attachments
        .iter()
        .chain(
            snapshot
                .comment_thread
                .iter()
                .flat_map(|comment| &comment.attachments),
        )
        .try_fold(0_i64, |sum, attachment| {
            sum.checked_add(attachment.size_bytes)
        })
        .ok_or_else(unavailable)?;
    if version != 1
        || attachment_bytes > 500 << 20
        || snapshot.version != Some(version)
        || snapshot.source_issue.id != source_id
        || snapshot.anchor_comment_id != anchor_id
        || snapshot.comment_thread.is_empty()
        || snapshot.comment_thread.len() > 256
        || snapshot.source_issue.attachments.len()
            + snapshot
                .comment_thread
                .iter()
                .map(|comment| comment.attachments.len())
                .sum::<usize>()
            > 100
        || !valid_attachments(&snapshot.source_issue.attachments, "issue", source_id)
    {
        return Err(unavailable());
    }
    if description_projection(
        snapshot.source_issue.description.as_deref().unwrap_or(""),
        &snapshot.source_issue.attachments,
        &[],
    )
    .is_none()
    {
        return Err(unavailable());
    }
    let mut seen = HashSet::new();
    let mut previous = None;
    for (index, comment) in snapshot.comment_thread.iter().enumerate() {
        if !seen.insert(comment.id)
            || !matches!(comment.author.kind.as_str(), "member" | "agent")
            || !valid_attachments(&comment.attachments, "comment", comment.id)
            || markdown_content_projection(
                &comment.content,
                &comment.attachments,
                &comment.attachments,
            )
            .contains("/api/attachments/")
            || (index == 0) != comment.parent_id.is_none()
            || comment
                .parent_id
                .is_some_and(|parent| !seen.contains(&parent))
        {
            return Err(unavailable());
        }
        let created_at =
            chrono::DateTime::parse_from_rfc3339(&comment.created_at).map_err(|_| unavailable())?;
        if previous.is_some_and(|(time, id)| (created_at, comment.id) <= (time, id))
            || (comment.id == anchor_id && index != snapshot.comment_thread.len() - 1)
        {
            return Err(unavailable());
        }
        previous = Some((created_at, comment.id));
    }
    let root_id = snapshot.comment_thread[0].id;
    let captured_anchor = snapshot.comment_thread.last().ok_or_else(unavailable)?;
    if captured_anchor.id != anchor_id
        || captured_anchor.deleted
        || captured_anchor.kind != "comment"
    {
        return Err(unavailable());
    }
    let anchor_time = chrono::DateTime::parse_from_rfc3339(&captured_anchor.created_at)
        .map_err(|_| unavailable())?
        .with_timezone(&chrono::Utc);
    let source = tx
        .query_opt(
            "SELECT i.number,i.title,i.description,w.issue_prefix \
         FROM issue i JOIN workspace w ON w.id=i.workspace_id \
         WHERE i.workspace_id=$1 AND i.id=$2",
            &[&ws, &source_id],
        )
        .await
        .map_err(|_| failed())?;
    let mut reasons = Vec::<&str>::new();
    let mut description_changes = Vec::new();
    let mut current_identifier = None;
    let mut current_attachment_rows_len = 0;
    let source_state =
        if let Some(source) = &source {
            let number: i32 = source.get("number");
            let identifier = format!("{}-{number}", source.get::<_, String>("issue_prefix"));
            current_identifier = Some(identifier);
            // Renumbering or changing the workspace prefix changes the link label,
            // not the captured source content or its workspace-scoped identity.
            if source.get::<_, String>("title") != snapshot.source_issue.title {
                reasons.push("issue_title");
            }
            let current_description: Option<String> = source.get("description");
            // Reading metadata is safe beyond the capture budget: Go still compares
            // oversized current inventories to the historical snapshot. Bound a
            // pathological live inventory independently of that capture limit.
            let current_attachment_rows = tx.query(
            "SELECT a.id,a.filename,a.content_type,a.size_bytes,a.created_at FROM attachment a \
             WHERE a.workspace_id=$1 AND a.issue_id=$2 AND a.comment_id IS NULL \
             AND a.source_context_id IS NULL ORDER BY a.id LIMIT 1025",
            &[&ws, &source_id],
        ).await.map_err(|_| failed())?;
            if current_attachment_rows.len() > 1024
                || current_attachment_rows
                    .iter()
                    .any(|row| row.get::<_, i64>("size_bytes") < 0)
            {
                return unavailable_current(tx, ws, row, version, snapshot).await;
            }
            current_attachment_rows_len = current_attachment_rows.len();
            let current_attachments: Vec<_> = current_attachment_rows
                .iter()
                .map(|row| SourceAttachment::from_row(row, "issue", source_id))
                .collect();
            let (captured_body, captured_refs) = description_projection(
                snapshot.source_issue.description.as_deref().unwrap_or(""),
                &snapshot.source_issue.attachments,
                &current_attachments,
            )
            .ok_or_else(unavailable)?;
            let (current_body, current_refs) = description_projection(
                current_description.as_deref().unwrap_or(""),
                &snapshot.source_issue.attachments,
                &current_attachments,
            )
            .ok_or_else(unavailable)?;
            if captured_body != current_body {
                reasons.push("issue_description");
            }
            description_changes = description_attachment_changes(&captured_refs, &current_refs);
            if !description_changes.is_empty() {
                reasons.push("issue_description_attachments");
            }
            if reasons.is_empty() {
                "unchanged"
            } else {
                "changed"
            }
        } else {
            "deleted"
        };
    // Capture is an immutable chronological prefix of the root's thread. A
    // reply created after the anchor is not part of the historical comparison.
    // The visited path and explicit limit bound corrupted/cyclic DB data.
    let rows = if source_state == "deleted" {
        Vec::new()
    } else {
        tx.query(
        r#"WITH RECURSIVE thread AS (
          SELECT c.id,c.parent_id,c.type,c.content,c.author_type,c.author_id,c.created_at,
                 c.updated_at,c.revision,c.deleted_at,ARRAY[c.id]::uuid[] AS path
          FROM comment c WHERE c.workspace_id=$1 AND c.issue_id=$2 AND c.id=$3
            AND c.parent_id IS NULL AND (c.created_at,c.id)<=($4,$5)
          UNION ALL
          SELECT c.id,c.parent_id,c.type,c.content,c.author_type,c.author_id,c.created_at,
                 c.updated_at,c.revision,c.deleted_at,t.path || c.id
          FROM comment c JOIN thread t ON c.parent_id=t.id
          WHERE c.workspace_id=$1 AND c.issue_id=$2 AND (c.created_at,c.id)<=($4,$5)
            AND cardinality(t.path)<257 AND NOT c.id=ANY(t.path))
         SELECT id,parent_id,type,content,author_type,author_id,created_at,updated_at,revision,deleted_at
         FROM thread ORDER BY created_at,id LIMIT 257"#,
        &[&ws,&source_id,&root_id,&anchor_time,&anchor_id],
    ).await.map_err(|_| failed())?
    };
    // A physically removed root cannot be traversed, but the captured snapshot
    // still describes the historical thread. Report its members as removed.
    if rows.len() > 256 || (!rows.is_empty() && rows[0].get::<_, Uuid>("id") != root_id) {
        return Err(unavailable());
    }
    let attachment_rows = tx
        .query(
            "SELECT a.id,a.comment_id,a.filename,a.content_type,a.size_bytes,a.created_at \
         FROM attachment a WHERE a.workspace_id=$1 AND a.issue_id=$2 \
         AND a.source_context_id IS NULL AND a.comment_id=ANY($3::uuid[]) \
         ORDER BY a.id LIMIT 1025",
            &[&ws, &source_id, &current_thread_ids(&rows)],
        )
        .await
        .map_err(|_| failed())?;
    if attachment_rows.len() + current_attachment_rows_len > 1024
        || attachment_rows
            .iter()
            .any(|row| row.get::<_, i64>("size_bytes") < 0)
    {
        return unavailable_current(tx, ws, row, version, snapshot).await;
    }
    let mut comment_attachments: HashMap<Uuid, Vec<SourceAttachment>> = HashMap::new();
    for row in &attachment_rows {
        let owner: Option<Uuid> = row.get("comment_id");
        let id = owner.ok_or_else(unavailable)?;
        comment_attachments
            .entry(id)
            .or_default()
            .push(SourceAttachment::from_row(row, "comment", id));
    }
    // Preserve Go's identity-sorted metadata inventory; never expose storage URLs.
    let captured_ids: HashSet<Uuid> = snapshot
        .comment_thread
        .iter()
        .map(|comment| comment.id)
        .collect();
    let added_member_ids: Vec<Uuid> = rows
        .iter()
        .filter(|row| {
            row.get::<_, String>("author_type") == "member"
                && !captured_ids.contains(&row.get::<_, Uuid>("id"))
        })
        .map(|row| row.get("author_id"))
        .collect();
    let added_agent_ids: Vec<Uuid> = rows
        .iter()
        .filter(|row| {
            row.get::<_, String>("author_type") == "agent"
                && !captured_ids.contains(&row.get::<_, Uuid>("id"))
        })
        .map(|row| row.get("author_id"))
        .collect();
    // An author may have left the workspace after writing a comment. Resolve
    // their historical display name without using member rows for this read.
    let added_member_names: HashMap<Uuid, String> = tx
        .query(
            "SELECT id,name FROM \"user\" WHERE id=ANY($1::uuid[])",
            &[&added_member_ids],
        )
        .await
        .map_err(|_| failed())?
        .into_iter()
        .map(|row| (row.get("id"), row.get("name")))
        .collect();
    let added_agent_names: HashMap<Uuid, String> = tx
        .query(
            "SELECT id,name FROM agent WHERE workspace_id=$1 AND id=ANY($2::uuid[])",
            &[&ws, &added_agent_ids],
        )
        .await
        .map_err(|_| failed())?
        .into_iter()
        .map(|row| (row.get("id"), row.get("name")))
        .collect();
    let mut current_thread = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: Uuid = row.get("id");
        let author_id: Uuid = row.get("author_id");
        let kind: String = row.get("author_type");
        if !matches!(kind.as_str(), "member" | "agent") {
            return Err(unavailable());
        }
        let captured = snapshot
            .comment_thread
            .iter()
            .find(|comment| comment.id == id);
        let name = if let Some(captured) = captured {
            if captured.author.id != author_id || captured.author.kind != kind {
                return Err(unavailable());
            }
            captured.author.name.clone()
        } else if kind == "member" {
            added_member_names
                .get(&author_id)
                .ok_or_else(unavailable)?
                .clone()
        } else {
            added_agent_names
                .get(&author_id)
                .ok_or_else(unavailable)?
                .clone()
        };
        let deleted = row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("deleted_at")
            .is_some();
        let content: String = row.get("content");
        let captured_attachments = captured
            .map(|item| item.attachments.as_slice())
            .unwrap_or(&[]);
        let live_attachments = comment_attachments
            .get(&id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if markdown_content_projection(&content, captured_attachments, live_attachments)
            .contains("/api/attachments/")
        {
            return Err(unavailable());
        }
        current_thread.push(SourceComment {
            id,
            parent_id: row.get("parent_id"),
            kind: row.get("type"),
            content: if deleted { String::new() } else { content },
            author: Author {
                kind,
                id: author_id,
                name,
            },
            created_at: timestamp(row.get("created_at")),
            updated_at: timestamp(row.get("updated_at")),
            revision: row.get("revision"),
            attachments: comment_attachments.remove(&id).unwrap_or_default(),
            deleted,
        });
    }
    let anchor = current_thread.iter().find(|c| c.id == anchor_id);
    let anchor_deleted = anchor.is_none_or(|c| c.deleted || c.kind != "comment");
    let captured_live: Vec<_> = snapshot
        .comment_thread
        .iter()
        .filter(|c| !c.deleted)
        .collect();
    let current_live: Vec<_> = current_thread.iter().filter(|c| !c.deleted).collect();
    let captured_by_id: HashMap<_, _> = captured_live.iter().map(|c| (c.id, *c)).collect();
    let current_by_id: HashMap<_, _> = current_live.iter().map(|c| (c.id, *c)).collect();
    let changed_comment_ids: Vec<_> = captured_live
        .iter()
        .filter_map(|comment| {
            current_by_id
                .get(&comment.id)
                .filter(|current| {
                    comment.parent_id != current.parent_id
                        || comment.kind != current.kind
                        || markdown_content_projection(
                            &comment.content,
                            &comment.attachments,
                            &current.attachments,
                        ) != markdown_content_projection(
                            &current.content,
                            &comment.attachments,
                            &current.attachments,
                        )
                        || comparable_attachments(&comment.attachments)
                            != comparable_attachments(&current.attachments)
                })
                .map(|_| comment.id)
        })
        .collect();
    let added_comments: Vec<_> = current_live
        .iter()
        .filter(|c| !captured_by_id.contains_key(&c.id))
        .map(|c| (*c).clone())
        .collect();
    let removed_comment_ids: Vec<_> = captured_live
        .iter()
        .filter(|c| !current_by_id.contains_key(&c.id))
        .map(|c| c.id)
        .collect();
    let structure_changed = captured_live
        .iter()
        .map(|c| (c.id, c.parent_id, &c.kind))
        .collect::<Vec<_>>()
        != current_live
            .iter()
            .map(|c| (c.id, c.parent_id, &c.kind))
            .collect::<Vec<_>>();
    let thread_state = if source_state == "deleted" {
        "unavailable"
    } else if structure_changed || !changed_comment_ids.is_empty() {
        "changed"
    } else {
        "unchanged"
    };
    if thread_state == "changed" {
        reasons.push("comment_thread");
    }
    let authors = source_author_states(tx, ws, &snapshot).await?;
    let display_state = if source_state == "deleted" {
        "deleted"
    } else if source_state == "changed" || thread_state == "changed" {
        "changed"
    } else {
        "unchanged"
    };
    let captured_at: chrono::DateTime<chrono::Utc> = row.get("captured_at");
    let mut details = json!({"changed_comment_ids":if source_state == "deleted" { Vec::new() } else { changed_comment_ids },
        "description_attachment_changes":description_changes});
    if source_state != "deleted" {
        if !added_comments.is_empty() {
            details["added_comments"] = json!(added_comments);
        }
        if !removed_comment_ids.is_empty() {
            details["removed_comment_ids"] = json!(removed_comment_ids);
        }
    }
    let mut result = json!({
        "id":row.get::<_,Uuid>("id"),"version":version,
        "usage":"read_only_historical_background","captured_at":timestamp(captured_at),
        "display_state":display_state,"source_issue_state":source_state,
        "comment_thread_state":thread_state,
        "anchor_comment_state":if source_state == "deleted" || anchor_deleted {"deleted"} else {"available"},
        "can_open_current_source":!anchor_deleted && source_state != "deleted",
        "change_reasons":reasons,"change_details":details,
        "snapshot":snapshot,
        "source_author_state":authors,
    });
    if !anchor_deleted && source_state != "deleted" {
        result["current_source"] = json!({"issue_id":source_id,"anchor_comment_id":anchor_id,
            "identifier":current_identifier.ok_or_else(unavailable)?});
    }
    Ok(Some(result))
}
