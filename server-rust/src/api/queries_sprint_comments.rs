//! Bounded comment read projections with whole-thread completion for default lists.
use super::{
    ApiError, ApiState, GoJson, catalog::WorkspaceId, error, issue_read::load_issue,
    queries_sprint_search_shapes::nano, timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::{HeaderMap, StatusCode},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::{IsolationLevel, Row};
use uuid::Uuid;

const CAP: usize = 2000;
const THREAD_CONTEXT_BUDGET: usize = 2000;
const THREAD_MAX_DEPTH: usize = 64;
const ROOTS_SQL: &str = include_str!("queries_sprint_comments_roots.sql");
const THREAD_SQL: &str = include_str!("queries_sprint_comments_thread.sql");
const RECENT_SQL: &str = include_str!("queries_sprint_comments_recent.sql");

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/{id}/comments", get(list))
}

#[derive(Debug, Default)]
struct Options {
    since: Option<DateTime<Utc>>,
    roots: bool,
    summary: bool,
    fold: bool,
    thread: Option<Uuid>,
    tail: Option<usize>,
    recent: Option<usize>,
    before: Option<DateTime<Utc>>,
    before_id: Option<Uuid>,
}

fn bad(message: &'static str) -> ApiError {
    error(StatusCode::BAD_REQUEST, message)
}

fn parse(raw: &str) -> Result<Options, ApiError> {
    // net/url.Values.Get uses the first value, including an explicitly empty one.
    let mut params = HashMap::new();
    for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
        params.entry(key.into_owned()).or_insert(value.into_owned());
    }
    let get = |key: &str| params.get(key).map(String::as_str).unwrap_or("");
    let boolean = |key, message| match get(key) {
        "" | "false" => Ok(false),
        "true" => Ok(true),
        _ => Err(bad(message)),
    };
    let date = |value: &str, message| {
        if value.is_empty() {
            Ok(None)
        } else {
            DateTime::parse_from_rfc3339(value)
                .map(|date| Some(date.with_timezone(&Utc)))
                .map_err(|_| bad(message))
        }
    };
    let since = date(
        get("since"),
        "invalid since parameter; expected RFC3339 format",
    )?;
    let roots_key = if get("roots_only").is_empty() {
        "roots-only"
    } else {
        "roots_only"
    };
    let roots = boolean(roots_key, "invalid roots_only parameter; expected boolean")?;
    let summary = boolean("summary", "invalid summary parameter; expected boolean")?;
    let fold = boolean("fold", "invalid fold parameter; expected boolean")?;
    let thread = get("thread");
    let tail = get("tail");
    let recent = get("recent");
    let before = get("before");
    let before_id = if get("before_id").is_empty() {
        get("before-id")
    } else {
        get("before_id")
    };
    if fold && since.is_some() {
        return Err(bad(
            "fold and since are mutually exclusive: since returns a partial thread, and a fold over a partial thread could hide a resolution that was not fetched",
        ));
    }
    if fold && !tail.is_empty() {
        return Err(bad(
            "fold and tail are mutually exclusive: tail returns a partial thread, which cannot be folded safely",
        ));
    }
    if fold && roots {
        return Err(bad(
            "fold and roots_only are mutually exclusive: roots_only returns no replies to fold",
        ));
    }
    for (conflict, message) in [
        (
            !thread.is_empty(),
            "roots_only and thread are mutually exclusive",
        ),
        (
            !recent.is_empty(),
            "roots_only and recent are mutually exclusive",
        ),
        (
            !tail.is_empty(),
            "roots_only and tail are mutually exclusive",
        ),
        (
            !before.is_empty() || !before_id.is_empty(),
            "roots_only does not support before / before_id",
        ),
    ] {
        if roots && conflict {
            return Err(bad(message));
        }
    }
    if !thread.is_empty() && !recent.is_empty() {
        return Err(bad("thread and recent are mutually exclusive"));
    }
    if !tail.is_empty() && thread.is_empty() {
        return Err(bad("tail requires thread (it is a thread-scoped limit)"));
    }
    if before.is_empty() != before_id.is_empty() {
        return Err(bad(
            "before and before_id must be set together (composite cursor)",
        ));
    }
    if !before.is_empty() && recent.is_empty() && (thread.is_empty() || tail.is_empty()) {
        return Err(bad(
            "before / before_id require recent (thread cursor) or thread + tail (reply cursor)",
        ));
    }
    let before = date(before, "invalid before parameter; expected RFC3339 format")?;
    let before_id = if before_id.is_empty() {
        None
    } else {
        Some(
            Uuid::parse_str(before_id)
                .map_err(|_| bad("invalid before_id parameter; expected UUID"))?,
        )
    };
    let number = |value: &str, minimum: i64, message| {
        if value.is_empty() {
            return Ok(None);
        }
        let n = value.parse::<i64>().map_err(|_| bad(message))?;
        if n < minimum {
            return Err(bad(message));
        }
        Ok(Some(n.min(CAP as i64) as usize))
    };
    let recent = number(
        recent,
        1,
        "invalid recent parameter; expected positive integer",
    )?;
    let tail = number(
        tail,
        0,
        "invalid tail parameter; expected non-negative integer",
    )?;
    let thread = if thread.is_empty() {
        None
    } else {
        Some(Uuid::parse_str(thread).map_err(|_| bad("invalid thread parameter; expected UUID"))?)
    };
    Ok(Options {
        since,
        roots,
        summary,
        fold,
        thread,
        tail,
        recent,
        before,
        before_id,
    })
}

fn db_error(err: tokio_postgres::Error) -> ApiError {
    if err.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) {
        error(StatusCode::GATEWAY_TIMEOUT, "comment query timed out")
    } else {
        error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list comments")
    }
}

fn fold(rows: Vec<Row>) -> (Vec<Row>, HashMap<Uuid, usize>) {
    let by_id: HashMap<Uuid, usize> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (row.get("id"), i))
        .collect();
    let mut threads: HashMap<Uuid, Vec<usize>> = HashMap::new();
    for (i, _) in rows.iter().enumerate() {
        let mut root = i;
        for _ in 0..rows.len() {
            let Some(parent) = rows[root].get::<_, Option<Uuid>>("parent_id") else {
                break;
            };
            let Some(index) = by_id.get(&parent) else {
                break;
            };
            root = *index;
        }
        threads.entry(rows[root].get("id")).or_default().push(i);
    }
    let mut keep = HashSet::new();
    let mut stats = HashMap::new();
    for (id, indices) in threads {
        let root = by_id[&id];
        keep.insert(root);
        if rows[root]
            .get::<_, Option<DateTime<Utc>>>("resolved_at")
            .is_some()
        {
            stats.insert(id, indices.iter().filter(|i| **i != root).count());
            continue;
        }
        // Equal resolution timestamps retain the first chronological reply, like Go.
        let mut resolution: Option<(usize, DateTime<Utc>)> = None;
        for &index in &indices {
            if index == root {
                continue;
            }
            if let Some(at) = rows[index].get::<_, Option<DateTime<Utc>>>("resolved_at")
                && resolution.is_none_or(|(_, previous)| at > previous)
            {
                resolution = Some((index, at));
            }
        }
        if let Some((index, _)) = resolution {
            keep.insert(index);
            stats.insert(id, indices.len().saturating_sub(2));
        } else {
            keep.extend(indices);
        }
    }
    (
        rows.into_iter()
            .enumerate()
            .filter_map(|(i, row)| keep.contains(&i).then_some(row))
            .collect(),
        stats,
    )
}

fn thread_root(id: Uuid, rows: &HashMap<Uuid, Row>) -> Option<Uuid> {
    let mut seen = HashSet::new();
    let mut current = id;
    loop {
        if !seen.insert(current) {
            return None;
        }
        match rows.get(&current)?.get::<_, Option<Uuid>>("parent_id") {
            Some(parent) => current = parent,
            None => return Some(current),
        }
    }
}

pub(super) async fn complete_threads(
    tx: &tokio_postgres::Transaction<'_>,
    issue: Uuid,
    ws: Uuid,
    window: Vec<Row>,
) -> Result<Vec<Row>, ApiError> {
    if window.is_empty() {
        return Ok(window);
    }
    let initial: HashSet<Uuid> = window.iter().map(|row| row.get("id")).collect();
    let through = window
        .iter()
        .map(|row| {
            (
                row.get::<_, DateTime<Utc>>("created_at"),
                row.get::<_, Uuid>("id"),
            )
        })
        .max()
        .unwrap();
    let mut rows: HashMap<Uuid, Row> = window.into_iter().map(|row| (row.get("id"), row)).collect();
    let mut added = 0;

    // Walk missing ancestors one level at a time. A foreign or cyclic chain
    // is removed rather than returning a reply without its root.
    for _ in 0..THREAD_MAX_DEPTH {
        let missing: HashSet<Uuid> = rows
            .values()
            .filter_map(|row| row.get::<_, Option<Uuid>>("parent_id"))
            .filter(|parent| !rows.contains_key(parent))
            .collect();
        if missing.is_empty() || added + missing.len() > THREAD_CONTEXT_BUDGET {
            break;
        }
        let ids: Vec<Uuid> = missing.into_iter().collect();
        let parents = tx.query(
            "SELECT * FROM comment WHERE id=ANY($1::uuid[]) AND issue_id=$2 AND workspace_id=$3 ORDER BY created_at,id",
            &[&ids, &issue, &ws],
        ).await.map_err(db_error)?;
        if parents.is_empty() {
            break;
        }
        for row in parents {
            if rows.insert(row.get("id"), row).is_none() {
                added += 1;
            }
        }
    }
    let connected: HashSet<Uuid> = rows
        .keys()
        .copied()
        .filter(|id| thread_root(*id, &rows).is_some())
        .collect();
    rows.retain(|id, _| connected.contains(id));

    // Threads whose roots were outside the newest window also need all older
    // siblings and descendants. Otherwise a partial fold invents a resolution.
    let affected: HashSet<Uuid> = rows
        .iter()
        .filter(|(id, row)| {
            row.get::<_, Option<Uuid>>("parent_id").is_none() && !initial.contains(id)
        })
        .map(|(id, _)| *id)
        .collect();
    if !affected.is_empty() {
        let mut frontier: HashMap<Uuid, Uuid> = affected.iter().map(|id| (*id, *id)).collect();
        let mut scan_budget = rows.len() + THREAD_CONTEXT_BUDGET - added;
        let mut complete = false;
        'walk: for depth in 0..=THREAD_MAX_DEPTH {
            let parents: Vec<Uuid> = frontier.keys().copied().collect();
            let children = tx.query(
                "SELECT * FROM comment WHERE parent_id=ANY($1::uuid[]) AND issue_id=$2 AND workspace_id=$3 AND (created_at,id)<=($4::timestamptz,$5::uuid) ORDER BY parent_id,created_at,id LIMIT $6::int4",
                &[&parents, &issue, &ws, &through.0, &through.1, &((scan_budget + 1) as i32)],
            ).await.map_err(db_error)?;
            if children.is_empty() {
                complete = true;
                break;
            }
            if children.len() > scan_budget || depth == THREAD_MAX_DEPTH {
                break;
            }
            scan_budget -= children.len();
            let mut next = HashMap::new();
            for row in children {
                let id: Uuid = row.get("id");
                let parent: Uuid = row.get("parent_id");
                let Some(root) = frontier.get(&parent) else {
                    break 'walk;
                };
                next.insert(id, *root);
                if let std::collections::hash_map::Entry::Vacant(entry) = rows.entry(id) {
                    if added >= THREAD_CONTEXT_BUDGET {
                        break 'walk;
                    }
                    entry.insert(row);
                    added += 1;
                }
            }
            frontier = next;
        }
        if !complete {
            let keep: HashSet<Uuid> = rows
                .keys()
                .copied()
                .filter(|id| thread_root(*id, &rows).is_some_and(|root| !affected.contains(&root)))
                .collect();
            rows.retain(|id, _| keep.contains(id));
        }
    }
    let mut result: Vec<Row> = rows.into_values().collect();
    result.sort_by_key(|row| {
        (
            row.get::<_, DateTime<Utc>>("created_at"),
            row.get::<_, Uuid>("id"),
        )
    });
    Ok(result)
}

fn project(row: &Row, summary: bool) -> Value {
    let content: String = row.get("content");
    let mut value = json!({
        "id":row.get::<_,Uuid>("id"), "issue_id":row.get::<_,Uuid>("issue_id"),
        "author_type":row.get::<_,String>("author_type"), "author_id":row.get::<_,Uuid>("author_id"),
        "content":content, "type":row.get::<_,String>("type"), "parent_id":row.get::<_,Option<Uuid>>("parent_id"),
        "created_at":timestamp(row.get("created_at")), "updated_at":timestamp(row.get("updated_at")),
        "revision":row.get::<_,i64>("revision"),
        "resolved_at":row.get::<_,Option<DateTime<Utc>>>("resolved_at").map(timestamp),
        "resolved_by_type":row.get::<_,Option<String>>("resolved_by_type"),
        "resolved_by_id":row.get::<_,Option<Uuid>>("resolved_by_id"),
        "reactions":[], "attachments":[],
    });
    for key in ["source_task_id", "quick_action_id"] {
        if let Some(id) = row.get::<_, Option<Uuid>>(key) {
            value[key] = json!(id);
        }
    }
    if let Some(deleted) = row.get::<_, Option<DateTime<Utc>>>("deleted_at") {
        value["deleted_at"] = json!(timestamp(deleted));
    }
    if summary {
        let clipped: String = content.chars().take(200).collect();
        let truncated = content.chars().nth(200).is_some();
        value["content"] = json!(if truncated { clipped + "…" } else { clipped });
        value["content_truncated"] = json!(truncated);
    }
    value
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
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to list comments"))?;
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
    let id: Uuid = load_issue(&db, ws, &raw).await?.get("id");
    let options = parse(query.as_deref().unwrap_or(""))?;
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
            "SELECT id FROM issue WHERE workspace_id=$1 AND id=$2",
            &[&ws, &id],
        )
        .await
        .map_err(db_error)?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "issue not found"));
    }
    let mut headers = HeaderMap::new();
    let mut rows;
    if options.roots {
        rows = tx
            .query(ROOTS_SQL, &[&id, &ws, &options.since, &((CAP + 1) as i32)])
            .await
            .map_err(db_error)?;
        if rows.len() > CAP {
            if options.since.is_some() {
                rows.pop();
            } else {
                rows.remove(0);
            }
            headers.insert("X-Comments-Truncated", "true".parse().unwrap());
        }
    } else if let Some(anchor) = options.thread {
        let limit = options.tail.map_or(CAP, |tail| tail + 1) as i32;
        rows = tx
            .query(
                THREAD_SQL,
                &[
                    &anchor,
                    &id,
                    &ws,
                    &options.before,
                    &options.before_id,
                    &limit,
                ],
            )
            .await
            .map_err(db_error)?;
        if rows.is_empty() {
            return Err(error(
                StatusCode::NOT_FOUND,
                "thread anchor not found in this issue",
            ));
        }
        let root_index = rows
            .iter()
            .position(|row| row.get::<_, Option<Uuid>>("parent_id").is_none())
            .unwrap();
        let root = rows.remove(root_index);
        let overflow = rows.len()
            >= if let Some(tail) = options.tail {
                tail + 1
            } else {
                CAP
            };
        if overflow {
            rows.remove(0);
        }
        if overflow && options.tail.is_none() {
            headers.insert("X-Comments-Truncated", "true".parse().unwrap());
        }
        if options.tail.is_some()
            && overflow
            && let Some(oldest) = rows.first()
        {
            let at: DateTime<Utc> = oldest.get("created_at");
            if options.since.is_none_or(|since| at > since) {
                headers.insert("X-Multica-Next-Before", nano(at).parse().unwrap());
                headers.insert(
                    "X-Multica-Next-Before-Id",
                    oldest.get::<_, Uuid>("id").to_string().parse().unwrap(),
                );
            }
        }
        rows.retain(|row| {
            options
                .since
                .is_none_or(|since| row.get::<_, DateTime<Utc>>("created_at") > since)
        });
        if options.tail.is_some()
            || options
                .since
                .is_none_or(|since| root.get::<_, DateTime<Utc>>("created_at") > since)
        {
            rows.insert(0, root);
        }
        // Never fold a defensive, partial untailed thread.
        if overflow && options.tail.is_none() && options.fold {
            return project_response(&tx, ws, id, rows, &options, headers, HashMap::new()).await;
        }
    } else if let Some(recent) = options.recent {
        // Select the active roots and materialize their membership once. Re-running
        // the recursive traversal for every 512-row page is quadratic for a
        // single large thread and can exceed the request's statement timeout.
        // The response must include every comment in each selected thread.
        rows = tx
            .query(
                RECENT_SQL,
                &[
                    &id,
                    &ws,
                    &options.before.is_some(),
                    &options.before,
                    &options.before_id,
                    &(recent as i32),
                ],
            )
            .await
            .map_err(db_error)?;
        let roots: HashSet<Uuid> = rows.iter().map(|row| row.get("thread_root_id")).collect();
        if roots.len() >= recent
            && let Some(head) = rows.first()
        {
            let at: DateTime<Utc> = head.get("thread_last_activity_at");
            if options.since.is_none_or(|since| at > since) {
                headers.insert("X-Multica-Next-Before", nano(at).parse().unwrap());
                headers.insert(
                    "X-Multica-Next-Before-Id",
                    head.get::<_, Uuid>("thread_root_id")
                        .to_string()
                        .parse()
                        .unwrap(),
                );
            }
        }
        rows.retain(|row| {
            options
                .since
                .is_none_or(|since| row.get::<_, DateTime<Utc>>("created_at") > since)
        });
    } else if options.since.is_some() {
        rows = tx.query("SELECT * FROM comment WHERE issue_id=$1 AND workspace_id=$2 AND created_at>$3 ORDER BY created_at,id LIMIT $4::int4", &[&id,&ws,&options.since,&((CAP+1) as i32)]).await.map_err(db_error)?;
        if rows.len() > CAP {
            rows.pop();
            headers.insert("X-Comments-Truncated", "true".parse().unwrap());
        }
    } else {
        rows = tx.query("SELECT * FROM (SELECT * FROM comment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at DESC,id DESC LIMIT $3::int4) recent ORDER BY created_at,id", &[&id,&ws,&((CAP+1) as i32)]).await.map_err(db_error)?;
        if rows.len() > CAP {
            rows.remove(0);
            rows = complete_threads(&tx, id, ws, rows).await?;
            headers.insert("X-Comments-Truncated", "true".parse().unwrap());
        }
    }
    let (rows, stats) = if options.fold {
        fold(rows)
    } else {
        (rows, HashMap::new())
    };
    project_response(&tx, ws, id, rows, &options, headers, stats).await
}

async fn project_response(
    tx: &tokio_postgres::Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    rows: Vec<Row>,
    options: &Options,
    headers: HeaderMap,
    fold_stats: HashMap<Uuid, usize>,
) -> Result<(HeaderMap, GoJson<Value>), ApiError> {
    let mut values = project_comments(tx, ws, issue, &rows, options.summary).await?;
    for (row, value) in rows.iter().zip(&mut values) {
        let id: Uuid = row.get("id");
        if options.roots {
            value["reply_count"] = json!(row.get::<_, i32>("reply_count"));
            value["last_activity_at"] = json!(timestamp(row.get("last_activity_at")));
        }
        if let Some(count) = fold_stats.get(&id) {
            value["thread_resolved"] = json!(true);
            value["folded_count"] = json!(count);
        }
    }
    Ok((headers, GoJson(json!(values))))
}

pub(super) async fn project_comments(
    tx: &tokio_postgres::Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    rows: &[Row],
    summary: bool,
) -> Result<Vec<Value>, ApiError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    let attachments: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM attachment WHERE workspace_id=$1 AND comment_id=ANY($2::uuid[]))", &[&ws,&ids]).await.map_err(db_error)?.get(0);
    if attachments {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "comment attachment URL policy unavailable",
        ));
    }
    let reactions = tx.query("SELECT r.* FROM comment_reaction r JOIN comment c ON c.id=r.comment_id AND c.workspace_id=r.workspace_id WHERE r.workspace_id=$1 AND c.issue_id=$2 AND r.comment_id=ANY($3::uuid[]) ORDER BY r.created_at ASC", &[&ws,&issue,&ids]).await.map_err(db_error)?;
    let mut grouped: HashMap<Uuid, Vec<Value>> = HashMap::new();
    for row in reactions {
        grouped.entry(row.get("comment_id")).or_default().push(json!({
            "id":row.get::<_,Uuid>("id"), "comment_id":row.get::<_,Uuid>("comment_id"),
            "actor_type":row.get::<_,String>("actor_type"), "actor_id":row.get::<_,Uuid>("actor_id"),
            "emoji":row.get::<_,String>("emoji"), "created_at":timestamp(row.get("created_at")),
        }));
    }
    let mut supplements: HashMap<Uuid, Vec<Value>> = HashMap::new();
    let mut cursor: Option<(DateTime<Utc>, Uuid)> = None;
    loop {
        // Keyset paging keeps a receipt on the same page even when many turns
        // are steered by one comment; the enclosing repeatable-read transaction
        // holds the entire read at one snapshot.
        let receipts = tx.query(
            "SELECT s.comment_id,s.task_id,s.created_at,t.agent_id,s.status,s.failure_reason,s.delivered_at FROM task_supplement s JOIN comment c ON c.id=s.comment_id AND c.workspace_id=s.workspace_id AND c.issue_id=s.issue_id LEFT JOIN agent_task_queue t ON t.id=s.task_id AND t.issue_id=s.issue_id AND EXISTS(SELECT 1 FROM agent a WHERE a.id=t.agent_id AND a.workspace_id=s.workspace_id) WHERE s.workspace_id=$1 AND s.issue_id=$2 AND s.comment_id=ANY($3::uuid[]) AND ($4::timestamptz IS NULL OR (s.created_at,s.task_id)>($4,$5::uuid)) ORDER BY s.created_at,s.task_id LIMIT 1024",
            &[&ws, &issue, &ids, &cursor.as_ref().map(|c| c.0), &cursor.as_ref().map(|c| c.1)],
        ).await.map_err(db_error)?;
        let count = receipts.len();
        for receipt in receipts {
            cursor = Some((receipt.get("created_at"), receipt.get("task_id")));
            let mut value = json!({
                "task_id": receipt.get::<_, Uuid>("task_id"),
                "status": receipt.get::<_, String>("status"),
            });
            if let Some(agent) = receipt.get::<_, Option<Uuid>>("agent_id") {
                value["agent_id"] = json!(agent);
            }
            if let Some(reason) = receipt.get::<_, Option<String>>("failure_reason") {
                value["failure_reason"] = json!(reason);
            }
            if let Some(at) = receipt.get::<_, Option<DateTime<Utc>>>("delivered_at") {
                value["delivered_at"] = json!(timestamp(at));
            }
            supplements
                .entry(receipt.get("comment_id"))
                .or_default()
                .push(value);
        }
        if count < 1024 {
            break;
        }
    }
    let values = rows
        .iter()
        .map(|row| {
            let id: Uuid = row.get("id");
            let mut value = project(row, summary);
            if let Some(reactions) = grouped.remove(&id) {
                value["reactions"] = json!(reactions);
            }
            if let Some(receipts) = supplements.remove(&id) {
                value["supplement_task_id"] = receipts[0]["task_id"].clone();
                value["supplement_status"] = receipts[0]["status"].clone();
                if let Some(reason) = receipts[0].get("failure_reason") {
                    value["supplement_failure_reason"] = reason.clone();
                }
                if let Some(at) = receipts[0].get("delivered_at") {
                    value["supplement_delivered_at"] = at.clone();
                }
                value["supplements"] = json!(receipts);
            }
            value
        })
        .collect();
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_sprint_comments_options_and_conflicts() {
        let options = parse("roots-only=true&summary=true&since=2026-11-01T00%3A00%3A00Z").unwrap();
        assert!(options.roots && options.summary && options.since.is_some());
        assert!(!parse("summary=false&summary=true").unwrap().summary);
        assert_eq!(
            parse("thread=00000000-0000-0000-0000-000000000001&tail=%2B9999")
                .unwrap()
                .tail,
            Some(CAP)
        );
        for query in [
            "summary=1",
            "fold=true&since=2026-11-01T00:00:00Z",
            "fold=true&tail=1",
            "fold=true&roots_only=true",
            "roots-only=true&thread=x",
            "roots_only=true&recent=1",
            "roots_only=true&tail=0",
            "roots_only=true&before=x",
            "tail=1",
            "thread=x&recent=1",
            "before=x",
            "before=x&before_id=x",
            "recent=0",
            "recent=9223372036854775808",
            "thread=x",
            "thread=00000000-0000-0000-0000-000000000001&tail=-1",
            "since=yesterday",
        ] {
            assert!(parse(query).is_err(), "{query}");
        }
    }
}
