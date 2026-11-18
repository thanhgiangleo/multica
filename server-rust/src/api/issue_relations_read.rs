//! Read-only children and duplicate relations from the frozen Go issue contract.
use super::{
    ApiError, ApiState, GoJson,
    catalog::{WorkspaceId, parse_params},
    error,
    issue_read::load_issue,
    squad::legacy_prefix,
    timestamp,
};
use axum::{
    Router,
    extract::{Extension, Path, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Local, NaiveDate, SecondsFormat, Utc};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::Row;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/children", get(children_by_parents))
        .route("/api/issues/{id}/children", get(children))
        .route("/api/issues/{id}/duplicates", get(duplicates))
}

fn builtin(status: &str) -> bool {
    matches!(
        status,
        "backlog" | "todo" | "in_progress" | "in_review" | "blocked" | "done" | "cancelled"
    )
}

fn wire_category(category: &str) -> &str {
    match category {
        "unstarted" => "todo",
        "started" => "in_progress",
        "done" => "done",
        "closed" => "cancelled",
        _ => "",
    }
}

// The Go resolver caches even failed catalog reads for this response. Archived
// statuses still describe old issues; a catalog failure must not fail the list.
async fn statuses(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    rows: &[Row],
) -> HashMap<String, (String, String)> {
    if rows.iter().all(|r| builtin(r.get("status"))) {
        return HashMap::new();
    }
    db.query(
        "SELECT key, category, name FROM issue_status WHERE workspace_id=$1",
        &[&ws],
    )
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|r| (r.get("key"), (r.get("category"), r.get("name"))))
    .collect()
}

async fn prefix(db: &deadpool_postgres::Client, ws: Uuid) -> String {
    let Ok(Some(row)) = db
        .query_opt(
            "SELECT name, issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
    else {
        return String::new();
    };
    let stored: String = row.get("issue_prefix");
    if stored.is_empty() {
        legacy_prefix(row.get("name"))
    } else {
        stored
    }
}

fn object_or_empty(value: Value) -> Value {
    if value.is_object() { value } else { json!({}) }
}

// Coordinator-owned read mapper. Mutation mappers stay within child ownership.
fn issue_response(
    row: &Row,
    prefix: &str,
    catalog: &HashMap<String, (String, String)>,
    names: bool,
) -> Value {
    let status: &str = row.get("status");
    let custom = catalog.get(status);
    let category = if builtin(status) {
        status
    } else {
        custom.map(|c| wire_category(&c.0)).unwrap_or("")
    };
    let mut response = json!({
        "id":row.get::<_,Uuid>("id"), "workspace_id":row.get::<_,Uuid>("workspace_id"),
        "number":row.get::<_,i32>("number"), "identifier":format!("{prefix}-{}",row.get::<_,i32>("number")),
        "title":row.get::<_,String>("title"), "description":row.get::<_,Option<String>>("description"),
        "status":status, "status_name":if names && !builtin(status) { custom.map(|c| c.1.as_str()).unwrap_or("") } else { "" },
        "priority":row.get::<_,String>("priority"), "assignee_type":row.get::<_,Option<String>>("assignee_type"),
        "assignee_id":row.get::<_,Option<Uuid>>("assignee_id"), "creator_type":row.get::<_,String>("creator_type"),
        "creator_id":row.get::<_,Uuid>("creator_id"), "parent_issue_id":row.get::<_,Option<Uuid>>("parent_issue_id"),
        "duplicate_of":null, "project_id":row.get::<_,Option<Uuid>>("project_id"), "position":row.get::<_,f64>("position"),
        "stage":row.get::<_,Option<i32>>("stage"),
        "start_date":row.get::<_,Option<NaiveDate>>("start_date").map(|d|d.to_string()),
        "due_date":row.get::<_,Option<NaiveDate>>("due_date").map(|d|d.to_string()),
        "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
        "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")), "revision":row.get::<_,i64>("revision"),
        "last_activity_at":row.get::<_,Option<DateTime<Utc>>>("last_activity_at").map(|t|t.with_timezone(&Local).to_rfc3339_opts(SecondsFormat::AutoSi,true)),
        "metadata":object_or_empty(row.get("metadata")), "properties":object_or_empty(row.get("properties")),
    });
    if !category.is_empty() {
        response["status_category"] = json!(category);
    }
    response
}

async fn children_response(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    rows: &[Row],
) -> GoJson<Value> {
    let prefix = prefix(db, ws).await;
    let catalog = statuses(db, ws, rows).await;
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let mut labels: HashMap<Uuid, Vec<Value>> = HashMap::new();
    if !ids.is_empty() {
        // Label rendering is deliberately non-critical, matching labelsByIssue.
        for row in db.query("SELECT il.issue_id,l.* FROM issue_label l JOIN issue_to_label il ON il.label_id=l.id \
            WHERE il.issue_id=ANY($1::uuid[]) AND l.workspace_id=$2 AND l.resource_type='issue' \
            ORDER BY il.issue_id,lower(l.name) ASC", &[&ids,&ws]).await.unwrap_or_default() {
            labels.entry(row.get("issue_id")).or_default().push(json!({
                "id":row.get::<_,Uuid>("id"), "workspace_id":ws, "resource_type":row.get::<_,String>("resource_type"),
                "name":row.get::<_,String>("name"), "description":row.get::<_,String>("description"),
                "color":row.get::<_,String>("color"), "usage_count":0,
                "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),
                "updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
            }));
        }
    }
    let issues: Vec<_> = rows
        .iter()
        .map(|row| {
            // Children only resolve the category, not custom names or duplicate refs.
            let mut response = issue_response(row, &prefix, &catalog, false);
            response["labels"] =
                json!(labels.remove(&row.get::<_, Uuid>("id")).unwrap_or_default());
            response
        })
        .collect();
    GoJson(json!({"issues":issues}))
}

async fn children(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let parent = load_issue(&db, ws, &raw).await?;
    // The frozen single-parent query lacks workspace_id. Constrain malformed
    // cross-tenant parent pointers as required by repository isolation rules.
    let rows = db
        .query(
            "SELECT * FROM issue WHERE parent_issue_id=$1 AND workspace_id=$2 ORDER BY number ASC",
            &[&parent.get::<_, Uuid>("id"), &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list child issues",
            )
        })?;
    Ok(children_response(&db, ws, &rows).await)
}

fn parent_ids(raw: &str) -> Result<Vec<Uuid>, ApiError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let parts: Vec<_> = raw.split(',').collect();
    if parts.len() > 200 {
        return Err(error(StatusCode::BAD_REQUEST, "too many parent_ids"));
    }
    parts
        .into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            Uuid::parse_str(s).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid parent_ids"))
        })
        .collect()
}

async fn children_by_parents(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    RawQuery(query): RawQuery,
) -> Result<GoJson<Value>, ApiError> {
    let params = parse_params(query.as_deref());
    let parents = parent_ids(params.get("parent_ids").map(String::as_str).unwrap_or(""))?;
    if parents.is_empty() {
        return Ok(GoJson(json!({"issues":[]})));
    }
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list child issues",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    let rows = db.query("SELECT * FROM issue WHERE workspace_id=$1 AND parent_issue_id=ANY($2::uuid[]) ORDER BY parent_issue_id,number ASC",
        &[&ws,&parents]).await.map_err(|_| fail())?;
    Ok(children_response(&db, ws, &rows).await)
}

async fn duplicates(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Value>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let current = db
        .query_opt(
            "SELECT status,duplicate_of_issue_id FROM issue WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to load duplicate original",
            )
        })?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let mut rows = Vec::new();
    if current.get::<_, String>("status") == "cancelled"
        && let Some(original) = current.get::<_, Option<Uuid>>("duplicate_of_issue_id")
        && let Some(row) = db
            .query_opt(
                "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&original, &ws],
            )
            .await
            .map_err(|_| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to load duplicate original",
                )
            })?
    {
        rows.push(row);
    }
    let has_original = !rows.is_empty();
    rows.extend(db.query("SELECT * FROM issue WHERE workspace_id=$1 AND duplicate_of_issue_id=$2 AND status='cancelled' ORDER BY created_at ASC,id ASC",
        &[&ws,&id]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to list duplicates"))?);
    let prefix = prefix(&db, ws).await;
    let catalog = statuses(&db, ws, &rows).await;
    let original_ids: Vec<Uuid> = rows
        .iter()
        .filter(|r| r.get::<_, String>("status") == "cancelled")
        .filter_map(|r| r.get("duplicate_of_issue_id"))
        .collect();
    let mut refs = HashMap::new();
    if !original_ids.is_empty() {
        for r in db.query("SELECT id,number,title,status FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[])", &[&ws,&original_ids]).await.unwrap_or_default() {
            refs.insert(r.get::<_,Uuid>("id"),json!({"id":r.get::<_,Uuid>("id"),"identifier":format!("{prefix}-{}",r.get::<_,i32>("number")),
                "title":r.get::<_,String>("title"),"status":r.get::<_,String>("status")}));
        }
    }
    let mut values: Vec<_> = rows
        .iter()
        .map(|r| {
            let mut response = issue_response(r, &prefix, &catalog, true);
            if r.get::<_, String>("status") == "cancelled"
                && let Some(original) = r.get::<_, Option<Uuid>>("duplicate_of_issue_id")
                && let Some(reference) = refs.get(&original)
            {
                response["duplicate_of"] = reference.clone();
            }
            response
        })
        .collect();
    let original = if has_original {
        values.remove(0)
    } else {
        Value::Null
    };
    Ok(GoJson(json!({"duplicate_of":original,"duplicates":values})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parent_input_cap_counts_empty_parts_and_preserves_duplicates() {
        let id = Uuid::new_v4();
        assert_eq!(parent_ids(&format!(" {id},,{id}, ")).unwrap(), vec![id, id]);
        assert!(parent_ids(&",".repeat(200)).is_err());
        assert!(parent_ids("ABC-1").is_err());
        assert!(parent_ids("").unwrap().is_empty());
    }
    #[test]
    fn invalid_custom_categories_are_not_guessed() {
        assert_eq!(wire_category("unstarted"), "todo");
        assert_eq!(wire_category("started"), "in_progress");
        assert_eq!(wire_category("closed"), "cancelled");
        assert_eq!(wire_category("bogus"), "");
        assert!(!builtin("custom_done"));
    }
}
