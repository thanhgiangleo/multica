//! Select surviving inputs from the pre-mutation task snapshot, not current queue state.
use super::super::{
    ApiError, ApiState, error,
    issue_write::{CommentCancelledBatch, CommentReplayInput},
};
use axum::http::StatusCode;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn is_note(content: &str) -> bool {
    content
        .trim_start_matches([' ', '\t', '\r', '\n'])
        .split(char::is_whitespace)
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("/note"))
}

pub(super) async fn inputs(
    state: &ApiState,
    batch: &CommentCancelledBatch,
    excluded: Option<Uuid>,
) -> Result<Vec<CommentReplayInput>, ApiError> {
    let mut targets: BTreeMap<Uuid, BTreeSet<Uuid>> = BTreeMap::new();
    for task in &batch.tasks {
        let Some(agent_id) = task.agent_id else {
            continue;
        };
        for comment_id in task
            .coalesced_comment_ids
            .iter()
            .copied()
            .chain(task.trigger_comment_id)
        {
            if Some(comment_id) != excluded {
                targets.entry(comment_id).or_default().insert(agent_id);
            }
        }
    }
    if targets.is_empty() {
        return Ok(vec![]);
    }
    let ids: Vec<Uuid> = targets.keys().copied().collect();
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to select cancelled batch survivors",
        )
    })?;
    let rows = db
        .query(
            "SELECT id,parent_id,author_type,author_id,source_task_id,content,suppressed_agent_ids,type FROM comment WHERE workspace_id=$1 AND issue_id=$2 AND id=ANY($3::uuid[]) AND deleted_at IS NULL ORDER BY created_at,id",
            &[&batch.workspace_id, &batch.issue_id, &ids],
        )
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to select cancelled batch survivors"))?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let content: String = row.get("content");
            if is_note(&content) {
                return None;
            }
            let comment_id: Uuid = row.get("id");
            let author_type: String = row.get("author_type");
            let source_task_id: Option<Uuid> = row.get("source_task_id");
            let kind: String = row.get("type");
            Some(CommentReplayInput {
                comment_id,
                parent_id: row.get("parent_id"),
                author_id: row.get("author_id"),
                delegated_failure_recovery: author_type == "system"
                    && kind == "progress_update"
                    && source_task_id.is_some(),
                author_type,
                source_task_id,
                content,
                suppressed_agent_ids: row
                    .get::<_, Option<Vec<Uuid>>>("suppressed_agent_ids")
                    .unwrap_or_default(),
                target_agent_ids: targets.get(&comment_id)?.iter().copied().collect(),
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::is_note;
    #[test]
    fn note_prefix_is_first_token_only() {
        for note in ["/note", "  /NOTE details", "/note\nnext"] {
            assert!(is_note(note));
        }
        for regular in ["/notes", "/ note", "see /note", "foo/note", "\u{a0}/note"] {
            assert!(!is_note(regular));
        }
    }
}
