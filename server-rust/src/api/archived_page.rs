//! Archived inbox page from the frozen Go group and cursor contract.
use super::{
    ApiError, ApiState, GoJson, Identity,
    catalog::{WorkspaceId, parse_params},
    error,
    inbox::{InboxItem, list_body},
    timestamp,
};
use axum::{
    extract::{Extension, RawQuery, State},
    http::StatusCode,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Serialize)]
struct ScopeFilters<'a> {
    workspace_id: Uuid,
    recipient_id: Uuid,
    statuses: &'a [String],
    priorities: &'a [String],
    actors: &'a [String],
    unread_only: bool,
}

#[derive(Serialize)]
struct Scope<'a> {
    #[serde(rename = "Filters")]
    filters: ScopeFilters<'a>,
    #[serde(rename = "Group")]
    group: Option<Uuid>,
}

#[derive(Deserialize, Serialize)]
struct Cursor {
    time: String,
    id: String,
    scope: String,
}

#[derive(Serialize)]
pub(super) struct Page {
    items: Vec<InboxItem>,
    next_cursor: Option<String>,
    has_more: bool,
}

fn scope(
    ws: Uuid,
    user: Uuid,
    group: Option<Uuid>,
    filters: &super::archived_facets::Filters,
) -> String {
    let bytes = serde_json::to_vec(&Scope {
        filters: ScopeFilters {
            workspace_id: ws,
            recipient_id: user,
            statuses: &filters.statuses,
            priorities: &filters.priorities,
            actors: &filters.actors,
            unread_only: filters.unread_only,
        },
        group,
    })
    .expect("scope serializes");
    // encoding/json escapes HTML-sensitive characters and JS separators by default.
    // A cursor minted by Go must hash the same filter bytes as a Rust cursor.
    let encoded = String::from_utf8(bytes).expect("JSON is UTF-8");
    let encoded = encoded
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    hex::encode(Sha256::digest(encoded.as_bytes()))
}

fn parse_cursor(raw: &str, expected_scope: &str) -> Result<(DateTime<Utc>, Uuid), ApiError> {
    let invalid = || error(StatusCode::BAD_REQUEST, "invalid archive cursor");
    if raw.len() > 2048 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid())?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.scope != expected_scope {
        return Err(invalid());
    }
    let time = DateTime::parse_from_rfc3339(&cursor.time)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid archive cursor time"))?
        .with_timezone(&Utc);
    let id = Uuid::parse_str(&cursor.id)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid cursor id"))?;
    Ok((time, id))
}

pub(super) async fn get(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Page>, ApiError> {
    let filters = super::archived_facets::filters(raw.as_deref())?;
    let params = parse_params(raw.as_deref());
    let limit: i32 = match params.get("limit").filter(|s| !s.is_empty()) {
        Some(value) => value
            .parse()
            .ok()
            .filter(|n| (1..=100).contains(n))
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "limit must be between 1 and 100"))?,
        None => 50,
    };
    let group = params
        .get("group_id")
        .filter(|s| !s.is_empty())
        .map(|value| {
            Uuid::parse_str(value).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid group_id"))
        })
        .transpose()?;
    let scope = scope(ws, user, group, &filters);
    let (before_time, before_id) = params
        .get("cursor")
        .filter(|s| !s.is_empty())
        .map(|value| parse_cursor(value, &scope))
        .transpose()?
        .map_or((None, None), |(time, id)| (Some(time), Some(id)));
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to load archived inbox page",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db
        .query(
            include_str!("archived_page.sql"),
            &[
                &ws,
                &user,
                &group,
                &filters.statuses,
                &filters.priorities,
                &filters.actors,
                &filters.unread_only,
                &before_time,
                &before_id,
                &(limit + 1),
            ],
        )
        .await
        .map_err(|_| fail())?;
    let has_more = rows.len() > limit as usize;
    let mut items = Vec::with_capacity(rows.len().min(limit as usize));
    let mut last = None;
    for row in rows.iter().take(limit as usize) {
        let id: Uuid = row.get("id");
        let created_at: DateTime<Utc> = row.get("created_at");
        let kind: String = row.get("type");
        let issue_id: Option<Uuid> = row.get("issue_id");
        let mut details = row
            .get::<_, Option<Value>>("details")
            .unwrap_or(Value::Null);
        let comment_id: String = row.get("comment_id");
        if !comment_id.is_empty() {
            match details {
                Value::Null => details = Value::Object(Default::default()),
                Value::Object(_) => {}
                _ => {
                    return Err(error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "invalid archived notification details",
                    ));
                }
            }
            details
                .as_object_mut()
                .expect("checked object")
                .insert("comment_id".to_owned(), Value::String(comment_id));
        }
        last = Some((created_at, id));
        items.push(InboxItem {
            id,
            workspace_id: row.get("workspace_id"),
            recipient_type: row.get("recipient_type"),
            recipient_id: row.get("recipient_id"),
            kind: kind.clone(),
            severity: row.get("severity"),
            issue_id,
            title: row.get("title"),
            body: list_body(&kind, issue_id, row.get("body")),
            read: row.get("read"),
            archived: row.get("archived"),
            created_at: timestamp(created_at),
            issue_status: row.get("issue_status"),
            issue_priority: row.get("issue_priority"),
            actor_type: row.get("actor_type"),
            actor_id: row.get("actor_id"),
            details,
        });
    }
    let next_cursor = if has_more {
        last.map(|(time, id)| {
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&Cursor {
                    time: time.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                    id: id.to_string(),
                    scope,
                })
                .expect("cursor serializes"),
            )
        })
    } else {
        None
    };
    Ok(GoJson(Page {
        items,
        next_cursor,
        has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scope_uses_go_struct_order_and_html_escaping() {
        let ws = Uuid::from_u128(1);
        let user = Uuid::from_u128(2);
        let group = Uuid::from_u128(3);
        let filters = super::super::archived_facets::filters(Some(
            "statuses=done&actors=system,member:%3C%26%3E&unread_only=true",
        ))
        .unwrap();
        let go_json = r#"{"Filters":{"workspace_id":"00000000-0000-0000-0000-000000000001","recipient_id":"00000000-0000-0000-0000-000000000002","statuses":["done"],"priorities":[],"actors":["member:\u003c\u0026\u003e","system"],"unread_only":true},"Group":"00000000-0000-0000-0000-000000000003"}"#;
        assert_eq!(
            scope(ws, user, Some(group), &filters),
            hex::encode(Sha256::digest(go_json))
        );
    }

    #[test]
    fn cursor_rejects_wrong_scope_and_bad_time() {
        let filters = super::super::archived_facets::filters(None).unwrap();
        let scope = scope(Uuid::nil(), Uuid::nil(), None, &filters);
        let encoded = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&Cursor {
                time: "not-time".into(),
                id: Uuid::nil().to_string(),
                scope: scope.clone(),
            })
            .unwrap(),
        );
        assert!(parse_cursor(&encoded, &scope).is_err());
        assert!(parse_cursor(&encoded, "different").is_err());
    }
}
