//! Human-only local search sync. All snapshots and rows come from the primary pool.
use super::{
    ApiError, ApiState, GoJson,
    actor::Actor,
    catalog::{WorkspaceId, parse_params},
    queries_sprint_search_shapes as shape,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashSet, time::Duration};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_postgres::{IsolationLevel, Row, error::SqlState};
use uuid::Uuid;

const MAX_CURSOR_BYTES: usize = 256 << 10;
static BOOTSTRAP_SLOTS: Semaphore = Semaphore::const_new(4);

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/search-index/manifest", get(manifest))
        .route("/api/search-index/snapshot", get(snapshot))
        .route("/api/search-index/changes", post(changes))
}

struct SearchError(StatusCode, String, bool);
impl IntoResponse for SearchError {
    fn into_response(self) -> Response {
        let mut response = GoJson(json!({"error":self.1})).into_response();
        *response.status_mut() = self.0;
        if self.2 {
            response
                .headers_mut()
                .insert("retry-after", "5".parse().unwrap());
        }
        response
    }
}
impl From<ApiError> for SearchError {
    fn from(error: ApiError) -> Self {
        Self(error.0, error.1, false)
    }
}
fn bad(message: impl Into<String>) -> SearchError {
    SearchError(StatusCode::BAD_REQUEST, message.into(), false)
}
fn failed() -> SearchError {
    SearchError(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to read search index".into(),
        false,
    )
}
fn db_error(error: tokio_postgres::Error) -> SearchError {
    match error.code() {
        Some(&SqlState::QUERY_CANCELED) => SearchError(
            StatusCode::SERVICE_UNAVAILABLE,
            "search index read timed out; please retry".into(),
            false,
        ),
        Some(&SqlState::INVALID_TEXT_REPRESENTATION) => bad("invalid cursor"),
        _ => failed(),
    }
}
fn require_enabled(state: &ApiState, actor: &Actor) -> Result<(), SearchError> {
    actor.require_human()?;
    if !state.local_search_index {
        return Err(SearchError(
            StatusCode::NOT_FOUND,
            "local search index is disabled".into(),
            false,
        ));
    }
    Ok(())
}
async fn bootstrap() -> Result<SemaphorePermit<'static>, SearchError> {
    tokio::time::timeout(Duration::from_secs(2), BOOTSTRAP_SLOTS.acquire())
        .await
        .ok()
        .and_then(Result::ok)
        .ok_or(SearchError(
            StatusCode::SERVICE_UNAVAILABLE,
            "search index is busy; retry later".into(),
            true,
        ))
}

#[derive(Default, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Cursor {
    #[serde(default)]
    v: u32,
    #[serde(default)]
    s: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    t: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    x: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    e: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    i: String,
}
impl Cursor {
    fn since(snapshot: String) -> Self {
        Self {
            v: 1,
            s: snapshot,
            ..Self::default()
        }
    }
    fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap())
    }
    fn decode(raw: &str) -> Result<Self, SearchError> {
        let invalid = || bad("invalid cursor");
        if raw.is_empty() || raw.len() > MAX_CURSOR_BYTES {
            return Err(invalid());
        }
        let bytes = URL_SAFE_NO_PAD.decode(raw).map_err(|_| invalid())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.v != 1 || !valid_snapshot(&cursor.s) {
            return Err(invalid());
        }
        if cursor.t.is_empty() {
            if !cursor.x.is_empty() || !cursor.e.is_empty() || !cursor.i.is_empty() {
                return Err(invalid());
            }
        } else if !valid_snapshot(&cursor.t)
            || cursor.x.parse::<u64>().is_err()
            || !matches!(cursor.e.as_str(), "issue" | "comment" | "project")
            || Uuid::parse_str(&cursor.i).is_err()
        {
            return Err(invalid());
        }
        Ok(cursor)
    }
}
fn valid_snapshot(raw: &str) -> bool {
    let parts: Vec<_> = raw.split(':').collect();
    if parts.len() != 3 {
        return false;
    }
    fn number(raw: &str) -> Option<u64> {
        if raw.is_empty() || raw.len() > 20 || !raw.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        raw.parse().ok()
    }
    let (Some(xmin), Some(xmax)) = (number(parts[0]), number(parts[1])) else {
        return false;
    };
    if xmin == 0 || xmin > xmax {
        return false;
    }
    if parts[2].is_empty() {
        return true;
    }
    let mut last = xmin;
    for raw in parts[2].split(',') {
        let Some(xip) = number(raw) else {
            return false;
        };
        if xip < last || xip >= xmax {
            return false;
        }
        last = xip;
    }
    true
}

const MANIFEST_SQL: &str = "WITH ws AS (SELECT $1::uuid AS id) SELECT \
    pg_current_snapshot()::text AS snapshot, \
    (SELECT count(*) FROM issue i,ws WHERE i.workspace_id=ws.id)::bigint AS issue_count, \
    (SELECT COALESCE(sum(octet_length(i.title)+COALESCE(octet_length(i.description),0)),0) FROM issue i,ws WHERE i.workspace_id=ws.id)::bigint AS issue_bytes, \
    (SELECT count(*) FROM comment c,ws WHERE c.workspace_id=ws.id AND c.deleted_at IS NULL)::bigint AS comment_count, \
    (SELECT COALESCE(sum(octet_length(c.content)),0) FROM comment c,ws WHERE c.workspace_id=ws.id AND c.deleted_at IS NULL)::bigint AS comment_bytes, \
    (SELECT count(*) FROM project p,ws WHERE p.workspace_id=ws.id)::bigint AS project_count, \
    (SELECT COALESCE(sum(octet_length(p.title)+COALESCE(octet_length(p.description),0)),0) FROM project p,ws WHERE p.workspace_id=ws.id)::bigint AS project_bytes";
const CHANGES_SQL: &str = "SELECT entity_type,entity_id,change_xid::text AS change_xid \
    FROM search_index_change WHERE workspace_id=$1 \
    AND change_xid>=pg_snapshot_xmin(($2::text)::pg_snapshot) \
    AND change_xid<pg_snapshot_xmax(($3::text)::pg_snapshot) \
    AND NOT pg_visible_in_snapshot(change_xid,($2::text)::pg_snapshot) \
    AND pg_visible_in_snapshot(change_xid,($3::text)::pg_snapshot) \
    AND (change_xid,entity_type,entity_id)>(($4::text)::xid8,$5::text,$6::uuid) \
    ORDER BY change_xid,entity_type,entity_id LIMIT $7::int4";

async fn manifest(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Value>, SearchError> {
    require_enabled(&state, &actor)?;
    let _permit = bootstrap().await?;
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(db_error)?;
    tx.batch_execute("SET LOCAL statement_timeout = 15000")
        .await
        .map_err(db_error)?;
    let row = tx.query_one(MANIFEST_SQL, &[&ws]).await.map_err(db_error)?;
    let result = json!({"cursor":Cursor::since(row.get("snapshot")).encode(),
        "issue_count":row.get::<_,i64>("issue_count"),"comment_count":row.get::<_,i64>("comment_count"),
        "project_count":row.get::<_,i64>("project_count"),
        "text_bytes":row.get::<_,i64>("issue_bytes")+row.get::<_,i64>("comment_bytes")+row.get::<_,i64>("project_bytes")});
    tx.commit().await.map_err(db_error)?;
    Ok(GoJson(result))
}

async fn snapshot(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Value>, SearchError> {
    require_enabled(&state, &actor)?;
    let params = parse_params(raw.as_deref());
    let after = match params.get("after_number").filter(|s| !s.is_empty()) {
        None => 0,
        Some(raw) => raw
            .parse::<i32>()
            .ok()
            .filter(|n| *n >= 0)
            .ok_or_else(|| bad("invalid after_number"))?,
    };
    let limit = match params.get("limit").filter(|s| !s.is_empty()) {
        None => 200,
        Some(raw) => raw
            .parse::<i64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| bad("invalid limit"))?
            .min(500) as i32,
    };
    let _permit = bootstrap().await?;
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(db_error)?;
    tx.batch_execute("SET LOCAL statement_timeout = 15000")
        .await
        .map_err(db_error)?;
    let sql = format!(
        "{} WHERE i.workspace_id=$1 AND i.number>$2 ORDER BY i.number LIMIT $3::int4",
        shape::ISSUE_SELECT
    );
    let rows = tx
        .query(&sql, &[&ws, &after, &limit])
        .await
        .map_err(db_error)?;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let comments=tx.query("SELECT id,issue_id,content,created_at FROM comment WHERE workspace_id=$1 AND issue_id=ANY($2::uuid[]) AND deleted_at IS NULL",&[&ws,&ids]).await.map_err(db_error)?;
    let projects = if after == 0 {
        tx.query(
            "SELECT * FROM project WHERE workspace_id=$1 ORDER BY id",
            &[&ws],
        )
        .await
        .map_err(db_error)?
    } else {
        vec![]
    };
    let result = json!({"issues":rows.iter().map(shape::issue).collect::<Vec<_>>(),
        "comments":comments.iter().map(shape::comment).collect::<Vec<_>>(),
        "projects":projects.iter().map(shape::project).collect::<Vec<_>>(),
        "next_after_number":rows.last().map(|r|r.get::<_,i32>("number")).unwrap_or(after),
        "done":rows.len()<(limit as usize)});
    tx.commit().await.map_err(db_error)?;
    Ok(GoJson(result))
}

#[derive(Deserialize)]
struct ChangesRequest {
    cursor: Option<String>,
    limit: Option<i64>,
}
async fn changes(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<GoJson<Value>, SearchError> {
    require_enabled(&state, &actor)?;
    if body.len() > MAX_CURSOR_BYTES + 1024 {
        return Err(bad("invalid request body"));
    }
    // Go's JSON decoder reads one object; trailing data is not another request.
    let request = serde_json::Deserializer::from_slice(&body)
        .into_iter::<ChangesRequest>()
        .next()
        .and_then(Result::ok)
        .ok_or_else(|| bad("invalid request body"))?;
    let cursor = Cursor::decode(request.cursor.as_deref().unwrap_or(""))?;
    let limit = request.limit.filter(|n| *n > 0).unwrap_or(500).min(1000) as i32;
    let mut db = state.pool.get().await.map_err(|_| failed())?;
    let tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await
        .map_err(db_error)?;
    tx.batch_execute("SET LOCAL statement_timeout = 15000")
        .await
        .map_err(db_error)?;
    let pruned:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM search_index_prune_mark WHERE singleton AND pruned_through_xid>=pg_snapshot_xmin(($1::text)::pg_snapshot))",&[&cursor.s]).await.map_err(db_error)?.get(0);
    if pruned {
        return Err(SearchError(
            StatusCode::GONE,
            "search index cursor expired; rebuild the local index".into(),
            false,
        ));
    }
    let target = if cursor.t.is_empty() {
        tx.query_one("SELECT pg_current_snapshot()::text", &[])
            .await
            .map_err(db_error)?
            .get::<_, String>(0)
    } else {
        cursor.t.clone()
    };
    let (after_xid, after_type, after_id) = if cursor.t.is_empty() {
        ("0", "", Uuid::nil())
    } else {
        (
            cursor.x.as_str(),
            cursor.e.as_str(),
            Uuid::parse_str(&cursor.i).unwrap(),
        )
    };
    let mut changed = tx
        .query(
            CHANGES_SQL,
            &[
                &ws,
                &cursor.s,
                &target,
                &after_xid,
                &after_type,
                &after_id,
                &(limit + 1),
            ],
        )
        .await
        .map_err(db_error)?;
    changed.truncate(limit as usize);
    let mut issue_ids = vec![];
    let mut comment_ids = vec![];
    let mut project_ids = vec![];
    for row in &changed {
        match row.get::<_, &str>("entity_type") {
            "issue" => issue_ids.push(row.get::<_, Uuid>("entity_id")),
            "comment" => comment_ids.push(row.get::<_, Uuid>("entity_id")),
            "project" => project_ids.push(row.get::<_, Uuid>("entity_id")),
            _ => {}
        }
    }
    let issues = tx
        .query(
            &format!(
                "{} WHERE i.workspace_id=$1 AND i.id=ANY($2::uuid[])",
                shape::ISSUE_SELECT
            ),
            &[&ws, &issue_ids],
        )
        .await
        .map_err(db_error)?;
    let comments=tx.query("SELECT id,issue_id,content,created_at FROM comment WHERE workspace_id=$1 AND id=ANY($2::uuid[]) AND deleted_at IS NULL",&[&ws,&comment_ids]).await.map_err(db_error)?;
    let projects = tx
        .query(
            "SELECT * FROM project WHERE workspace_id=$1 AND id=ANY($2::uuid[])",
            &[&ws, &project_ids],
        )
        .await
        .map_err(db_error)?;
    fn ids(rows: &[Row]) -> HashSet<Uuid> {
        rows.iter().map(|r| r.get("id")).collect()
    }
    let live = (ids(&issues), ids(&comments), ids(&projects));
    let mut deleted = json!({"issues":[],"comments":[],"projects":[]});
    for row in &changed {
        let id = row.get::<_, Uuid>("entity_id");
        let (key, live) = match row.get::<_, &str>("entity_type") {
            "issue" => ("issues", &live.0),
            "comment" => ("comments", &live.1),
            "project" => ("projects", &live.2),
            _ => continue,
        };
        if !live.contains(&id) {
            deleted[key].as_array_mut().unwrap().push(json!(id));
        }
    }
    let has_more = changed.len() == limit as usize;
    let next = if has_more {
        let last = changed.last().unwrap();
        Cursor {
            v: 1,
            s: cursor.s,
            t: target,
            x: last.get("change_xid"),
            e: last.get("entity_type"),
            i: last.get::<_, Uuid>("entity_id").to_string(),
        }
    } else {
        Cursor::since(target)
    };
    let result = json!({"issues":issues.iter().map(shape::issue).collect::<Vec<_>>(),
        "comments":comments.iter().map(shape::comment).collect::<Vec<_>>(),
        "projects":projects.iter().map(shape::project).collect::<Vec<_>>(),
        "deleted":deleted,"cursor":next.encode(),"has_more":has_more});
    tx.commit().await.map_err(db_error)?;
    Ok(GoJson(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_snapshot_and_cursor_before_empty_db_range() {
        for raw in [
            "1:1:",
            "10:20:10,11,19",
            "18446744073709551615:18446744073709551615:",
        ] {
            assert!(valid_snapshot(raw), "{raw}");
        }
        for raw in [
            "",
            "0:1:",
            "2:1:",
            "1:2:2",
            "1:5:3,2",
            "1:2:0",
            "1:2:1,",
            "+1:2:",
            "1:2: 1",
            "1:18446744073709551616:",
            "1:2::",
        ] {
            assert!(!valid_snapshot(raw), "{raw}");
        }
        let valid = Cursor::since("10:20:10,11".into());
        assert_eq!(Cursor::decode(&valid.encode()).ok(), Some(valid.clone()));
        for field in ["x", "e", "i"] {
            let mut value = serde_json::to_value(&valid).unwrap();
            value[field] = json!("orphan");
            assert!(
                Cursor::decode(&URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).unwrap()))
                    .is_err()
            );
        }
        let page = Cursor {
            v: 1,
            s: "10:20:10".into(),
            t: "20:25:20".into(),
            x: "22".into(),
            e: "issue".into(),
            i: Uuid::new_v4().to_string(),
        };
        assert_eq!(Cursor::decode(&page.encode()).ok(), Some(page));
        assert!(Cursor::decode(&"x".repeat(MAX_CURSOR_BYTES + 1)).is_err());
        assert!(Cursor::decode("!invalid!").is_err());
    }
}
