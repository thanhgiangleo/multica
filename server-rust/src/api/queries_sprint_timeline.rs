//! Issue timeline read model; storage-backed avatar URLs require the integrator's policy.
use super::{
    ApiError, ApiState, GoJson,
    catalog::{WorkspaceId, parse_params},
    error,
    issue_read::load_issue,
    queries_sprint_comments::{complete_threads, project_comments},
    timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio_postgres::{IsolationLevel, Row};
use uuid::Uuid;

const CAP: usize = 2000;

/// Resolve only stored avatar references belonging to actors already present in
/// an authorized timeline. Signing/storage configuration stays outside this module.
/// None means unavailable; never echo a private object reference as a fallback.
pub trait TimelineAvatarUrlPolicy: Send + Sync {
    fn resolve(&self, stored: &str) -> Option<String>;
}
pub type SharedTimelineAvatarUrlPolicy = Arc<dyn TimelineAvatarUrlPolicy>;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/timeline", get(list))
}

fn db_error(err: tokio_postgres::Error) -> ApiError {
    if err.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) {
        error(StatusCode::GATEWAY_TIMEOUT, "timeline query timed out")
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list timeline")
    }
}

fn comment_entry(mut comment: Value) -> Value {
    let mut entry = json!({
        "type": "comment", "id": comment["id"],
        "actor_type": comment["author_type"], "actor_id": comment["author_id"],
        "created_at": comment["created_at"], "content": comment["content"],
        "updated_at": comment["updated_at"], "comment_type": comment["type"],
    });
    if comment["revision"] != 0 {
        entry["revision"] = comment["revision"].clone();
    }
    for key in [
        "parent_id",
        "quick_action_id",
        "resolved_at",
        "resolved_by_type",
        "resolved_by_id",
        "source_task_id",
        "deleted_at",
        "supplements",
        "supplement_task_id",
        "supplement_status",
        "supplement_failure_reason",
        "supplement_delivered_at",
        "reactions",
        "attachments",
    ] {
        if let Some(value) = comment.as_object_mut().unwrap().remove(key)
            && !value.is_null()
            && !value.as_array().is_some_and(Vec::is_empty)
        {
            entry[key] = value;
        }
    }
    entry
}

fn activity_entry(row: &Row) -> Value {
    json!({
        "type": "activity", "id": row.get::<_, Uuid>("id"),
        "actor_type": row.get::<_, Option<String>>("actor_type").unwrap_or_default(),
        "actor_id": row.get::<_, Option<Uuid>>("actor_id").map(|id| id.to_string()).unwrap_or_default(),
        "action": row.get::<_, String>("action"), "details": row.get::<_, Value>("details"),
        "created_at": timestamp(row.get::<_, DateTime<Utc>>("created_at")),
    })
}

fn sort_entries(entries: &mut [Value], wrapped: bool) {
    // Frozen Go merges on its serialized second-precision timestamp, then ID.
    entries.sort_by(|a, b| {
        let order = a["created_at"]
            .as_str()
            .cmp(&b["created_at"].as_str())
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()));
        if wrapped { order.reverse() } else { order }
    });
}

async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<(HeaderMap, GoJson<Value>), ApiError> {
    let mut db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list timeline"))?;
    if Uuid::parse_str(&raw).is_err()
        && !raw.rsplit_once('-').is_some_and(|(prefix, number)| {
            !prefix.is_empty()
                && !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && number.parse::<i32>().is_ok_and(|number| number > 0)
        })
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let issue: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(db_error)?;
    tx.batch_execute("SET LOCAL statement_timeout = 8000")
        .await
        .map_err(db_error)?;
    if tx
        .query_opt(
            "SELECT id FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&issue, &ws],
        )
        .await
        .map_err(db_error)?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let mut comments = tx.query(
        "SELECT * FROM (SELECT * FROM comment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT $3::int4) recent ORDER BY created_at,id",
        &[&issue, &ws, &((CAP + 1) as i32)],
    ).await.map_err(db_error)?;
    let mut activities = tx.query(
        "SELECT * FROM (SELECT * FROM activity_log WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT $3::int4) recent ORDER BY created_at,id",
        &[&issue, &ws, &((CAP + 1) as i32)],
    ).await.map_err(db_error)?;
    let comments_truncated = comments.len() > CAP;
    let activities_truncated = activities.len() > CAP;
    if comments_truncated {
        comments.remove(0);
        comments = complete_threads(&tx, issue, ws, comments).await?;
    }
    if activities_truncated {
        activities.remove(0);
    }
    let mut entries: Vec<Value> = project_comments(&tx, ws, issue, &comments, false)
        .await?
        .into_iter()
        .map(comment_entry)
        .collect();
    entries.extend(activities.iter().map(activity_entry));
    let params = parse_params(query.as_deref());
    let wrapped = ["limit", "before", "after", "around"]
        .iter()
        .any(|key| params.get(*key).is_some_and(|value| !value.is_empty()));
    sort_entries(&mut entries, wrapped);

    // Departed members keep display attribution, but unrelated users are never read.
    let ids: Vec<Uuid> = entries
        .iter()
        .filter(|entry| entry["actor_type"] == "member")
        .filter_map(|entry| {
            entry["actor_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if !ids.is_empty()
        && let Ok(users) = tx
            .query(
                "SELECT id,name,avatar_url FROM \"user\" WHERE id=ANY($1::uuid[])",
                &[&ids],
            )
            .await
    {
        let users: HashMap<Uuid, Row> = users.into_iter().map(|row| (row.get("id"), row)).collect();
        for entry in &mut entries {
            if entry["actor_type"] != "member" {
                continue;
            }
            let Some(user) = entry["actor_id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .and_then(|id| users.get(&id))
            else {
                continue;
            };
            let name: String = user.get("name");
            if !name.is_empty() {
                entry["actor_name"] = json!(name);
            }
            if let Some(raw) = user
                .get::<_, Option<String>>("avatar_url")
                .filter(|raw| !raw.is_empty())
            {
                let url = state
                    .timeline_avatar_urls
                    .as_ref()
                    .and_then(|policy| policy.resolve(&raw))
                    .ok_or_else(|| {
                        error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "timeline avatar URL policy unavailable",
                        )
                    })?;
                if !url.is_empty() {
                    entry["actor_avatar_url"] = json!(url);
                }
            }
        }
    }
    let mut headers = HeaderMap::new();
    let kinds = match (comments_truncated, activities_truncated) {
        (true, true) => "activity,comment",
        (true, false) => "comment",
        (false, true) => "activity",
        _ => "",
    };
    if !kinds.is_empty() {
        headers.insert("X-Timeline-Truncated", kinds.parse().unwrap());
    }
    let body = if wrapped {
        let anchor = params.get("around");
        let index = anchor.and_then(|id| {
            entries
                .iter()
                .position(|entry| entry["id"].as_str() == Some(id.as_str()))
        });
        let mut body = json!({"entries": entries, "next_cursor": null, "prev_cursor": null,
            "has_more_before": comments_truncated || activities_truncated, "has_more_after": false});
        if let Some(index) = index {
            body["target_index"] = json!(index);
        }
        body
    } else {
        json!(entries)
    };
    Ok((headers, GoJson(body)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queries_sprint_timeline_comment_shape_and_frozen_go_sort() {
        let c = json!({"id":"b", "author_type":"member", "author_id":"actor", "created_at":"2026-01-01T00:00:00Z",
            "updated_at":"2026-01-01T00:00:00Z", "type":"comment", "content":"", "revision":0,
            "parent_id":null,"resolved_at":null,"reactions":[],"attachments":[]});
        let entry = comment_entry(c);
        assert_eq!(
            entry,
            json!({"id":"b","actor_type":"member","actor_id":"actor","type":"comment",
            "created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z","comment_type":"comment","content":""})
        );
        let mut values = vec![entry, json!({"id":"a","created_at":"2026-01-01T00:00:00Z"})];
        sort_entries(&mut values, false);
        assert_eq!(values[0]["id"], "a");
        sort_entries(&mut values, true);
        assert_eq!(values[0]["id"], "b");
    }
}
