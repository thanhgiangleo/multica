//! Frozen project-workflow reads, including the lazy Default snapshot write.
use super::{ApiError, ApiState, GoJson, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::get,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issue-workflows", get(list))
        .route("/api/issue-workflows/", get(list))
        .route("/api/issue-workflows/{id}", get(detail))
        .route("/api/issue-workflows/{id}/", get(detail))
}

pub(super) const BUILT_INS: [&str; 7] = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "cancelled",
];
const LIST: &str = "SELECT * FROM issue_workflow WHERE workspace_id=$1 ORDER BY is_default DESC, lower(name), created_at, id";

#[derive(Serialize, Debug, PartialEq, Clone, Default)]
pub(super) struct Handler {
    pub(super) r#type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(super) id: String,
}
#[derive(Serialize, Debug, PartialEq, Clone)]
pub(super) struct Step {
    pub(super) status_key: String,
    pub(super) handler: Handler,
    pub(super) instructions: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(super) next_status_key: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub(super) back_status_key: String,
}
#[derive(Serialize)]
struct Workflow {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    description: String,
    is_default: bool,
    owner_id: Option<Uuid>,
    steps: Vec<Step>,
    project_ids: Vec<Uuid>,
    created_at: String,
    updated_at: String,
}

// Match Go's case-insensitive tags in PostgreSQL JSONB wire order. Null
// leaves scalar strings intact, but resets a pointer to the nested handler.
fn fields<'a>(v: &'a Value, name: &str) -> Vec<&'a Value> {
    let mut pairs: Vec<_> = v
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(k, _)| k.eq_ignore_ascii_case(name))
        .collect();
    pairs.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
    pairs.into_iter().map(|(_, v)| v).collect()
}
fn update_text(v: &Value, name: &str, out: &mut String) -> Result<(), ()> {
    for value in fields(v, name) {
        match value {
            Value::Null => {}
            Value::String(s) => *out = s.clone(),
            _ => return Err(()),
        }
    }
    Ok(())
}
fn text(v: &Value, name: &str) -> Result<String, ()> {
    let mut out = String::new();
    update_text(v, name, &mut out)?;
    Ok(out)
}
pub(super) fn decode(raw: Value) -> Result<Vec<Step>, ()> {
    if raw.is_null() {
        return Ok(vec![]);
    }
    raw.as_array()
        .ok_or(())?
        .iter()
        .map(|s| {
            if !s.is_object() && !s.is_null() {
                return Err(());
            }
            let mut handler: Option<Handler> = None;
            for h in fields(s, "handler") {
                if h.is_null() {
                    handler = None;
                    continue;
                }
                if !h.is_object() {
                    return Err(());
                }
                let mut value = handler.take().unwrap_or(Handler {
                    r#type: String::new(),
                    id: String::new(),
                });
                update_text(h, "type", &mut value.r#type)?;
                update_text(h, "id", &mut value.id)?;
                handler = Some(value);
            }
            let handler = handler.filter(|h| !h.r#type.is_empty()).unwrap_or(Handler {
                r#type: "none".into(),
                id: String::new(),
            });
            Ok(Step {
                status_key: text(s, "status_key")?,
                handler,
                instructions: text(s, "instructions")?,
                next_status_key: text(s, "next_status_key")?,
                back_status_key: text(s, "back_status_key")?,
            })
        })
        .collect()
}

pub(super) fn decode_step_values(raw: Value) -> Result<Vec<Value>, ()> {
    decode(raw)?
        .into_iter()
        .map(|step| serde_json::to_value(step).map_err(|_| ()))
        .collect()
}

pub(super) async fn ensure_default(
    db: &impl GenericClient,
    ws: Uuid,
) -> Result<(), tokio_postgres::Error> {
    if db
        .query_opt(
            "SELECT id FROM issue_workflow WHERE workspace_id=$1 AND is_default",
            &[&ws],
        )
        .await?
        .is_some()
    {
        return Ok(());
    }
    // ActiveKeys does not seed the catalog. It appends absent built-ins before ArrangeKeys.
    let rows = db.query("SELECT key FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END, position, CASE WHEN is_system THEN 0 ELSE 1 END, CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END, key", &[&ws]).await?;
    let mut active: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    for key in BUILT_INS {
        if !active.iter().any(|s| s == key) {
            active.push(key.into());
        }
    }
    let named = db
        .query(
            "SELECT steps FROM issue_workflow WHERE workspace_id=$1 AND NOT is_default",
            &[&ws],
        )
        .await?;
    let claimed: HashSet<String> = named
        .iter()
        .filter_map(|r| decode(r.get(0)).ok())
        .flatten()
        .map(|s| s.status_key)
        .collect();
    let fixed = ["backlog", "todo", "blocked", "done", "cancelled"];
    let middle = active.into_iter().filter(|s| {
        !fixed.contains(&s.as_str()) && (!claimed.contains(s) || BUILT_INS.contains(&s.as_str()))
    });
    let keys: Vec<_> = ["backlog".to_string(), "todo".to_string()]
        .into_iter()
        .chain(middle)
        .chain(["blocked".into(), "done".into(), "cancelled".into()])
        .collect();
    let steps = Value::Array(
        keys.into_iter()
            .map(|key| json!({"status_key":key}))
            .collect(),
    );
    db.execute("INSERT INTO issue_workflow (workspace_id,name,description,initial_status_key,steps,is_default) VALUES ($1,'Default','','todo',$2,true) ON CONFLICT (workspace_id) WHERE is_default DO NOTHING", &[&ws,&steps]).await?;
    Ok(())
}

async fn project_ids(
    db: &impl GenericClient,
    ws: Uuid,
    rows: &[Row],
) -> Result<HashMap<Uuid, Vec<Uuid>>, tokio_postgres::Error> {
    let known: HashSet<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let default = rows
        .iter()
        .find(|r| r.get::<_, bool>("is_default"))
        .map(|r| r.get::<_, Uuid>("id"));
    let mut out: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for p in db
        .query(
            "SELECT id,workflow_id FROM project WHERE workspace_id=$1 ORDER BY created_at DESC",
            &[&ws],
        )
        .await?
    {
        let key = p
            .get::<_, Option<Uuid>>("workflow_id")
            .filter(|id| known.contains(id))
            .or(default);
        if let Some(key) = key {
            out.entry(key).or_default().push(p.get("id"));
        }
    }
    Ok(out)
}
fn response(row: &Row, steps: Vec<Step>, projects: &HashMap<Uuid, Vec<Uuid>>) -> Workflow {
    let id = row.get("id");
    Workflow {
        id,
        workspace_id: row.get("workspace_id"),
        name: row.get("name"),
        description: row.get("description"),
        is_default: row.get("is_default"),
        owner_id: row.get("owner_id"),
        steps,
        project_ids: projects.get(&id).cloned().unwrap_or_default(),
        created_at: timestamp(row.get("created_at")),
        updated_at: timestamp(row.get("updated_at")),
    }
}
async fn list(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
) -> Result<GoJson<Value>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list workflows",
        )
    };
    let db = state.pool.get().await.map_err(|_| fail())?;
    ensure_default(&**db, ws).await.map_err(|_| fail())?;
    let rows = db.query(LIST, &[&ws]).await.map_err(|_| fail())?;
    let projects = project_ids(&**db, ws, &rows).await.map_err(|_| fail())?;
    let workflows: Vec<_> = rows
        .iter()
        .filter_map(|r| {
            decode(r.get("steps"))
                .ok()
                .map(|s| response(r, s, &projects))
        })
        .collect();
    Ok(GoJson(
        json!({"total":workflows.len(),"workflows":workflows}),
    ))
}
async fn detail(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Path(raw): Path<String>,
) -> Result<GoJson<Workflow>, ApiError> {
    let id =
        Uuid::parse_str(&raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workflow id"))?;
    let fail = || error(StatusCode::NOT_FOUND, "workflow not found");
    let db = state.pool.get().await.map_err(|_| fail())?;
    let row = db
        .query_opt(
            "SELECT * FROM issue_workflow WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(fail)?;
    let rows = db.query(LIST, &[&ws]).await.unwrap_or_default();
    let known = if rows.is_empty() {
        std::slice::from_ref(&row)
    } else {
        &rows
    };
    let projects = project_ids(&**db, ws, known).await.unwrap_or_default();
    Ok(GoJson(response(
        &row,
        decode(row.get("steps")).unwrap_or_default(),
        &projects,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_steps_go_null_types_and_omissions() {
        let steps = decode(json!([null, {"status_key":null,"handler":{"type":"","id":"discard"}}, {"STATUS_KEY":"doing","HANDLER":{"TYPE":"agent","ID":"fake"},"instructions":"<&>","next_status_key":"done"}])).unwrap();
        assert_eq!(steps[0].handler.r#type, "none");
        assert_eq!(steps[1].handler.id, "");
        let value = serde_json::to_value(&steps).unwrap();
        assert_eq!(value[2]["handler"], json!({"type":"agent","id":"fake"}));
        assert!(value[0].get("back_status_key").is_none());
        assert_eq!(value[2]["instructions"], "<&>");
        for bad in [
            json!([false]),
            json!([{"handler":[]}]),
            json!([{"status_key":9}]),
            json!([{"handler":{"id":7}}]),
        ] {
            assert!(decode(bad).is_err());
        }
    }
}
