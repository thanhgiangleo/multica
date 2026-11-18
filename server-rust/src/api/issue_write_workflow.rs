//! Project workflow admission and member handoffs for issue writes.
//! Agent/squad handoffs remain unavailable until dispatch is integrated.
use super::super::squad::legacy_prefix;
use super::{ApiError, GoJson, error};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

struct Step {
    status_key: String,
    handler: Option<Handler>,
}

#[derive(Default)]
struct Handler {
    kind: String,
    id: String,
}

// Stored JSONB follows Go's folded field tags and scalar-null semantics.
// Never ignore a folded HANDLER/TYPE and accidentally admit a runtime handoff.
fn fields<'a>(value: &'a Value, key: &str) -> Vec<&'a Value> {
    let mut fields: Vec<_> = value
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| name.eq_ignore_ascii_case(key))
        .collect();
    fields.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then(a.cmp(b)));
    fields.into_iter().map(|(_, value)| value).collect()
}

fn text(value: &Value, key: &str, target: &mut String) -> Result<(), ApiError> {
    for value in fields(value, key) {
        match value {
            Value::Null => {}
            Value::String(value) => *target = value.clone(),
            _ => return Err(fail()),
        }
    }
    Ok(())
}

fn decode(raw: Value) -> Result<Vec<Step>, ApiError> {
    if raw.is_null() {
        return Ok(Vec::new());
    }
    raw.as_array()
        .ok_or_else(fail)?
        .iter()
        .map(|value| {
            if !value.is_object() && !value.is_null() {
                return Err(fail());
            }
            let mut status_key = String::new();
            text(value, "status_key", &mut status_key)?;
            for key in ["instructions", "next_status_key", "back_status_key"] {
                text(value, key, &mut String::new())?;
            }
            let mut handler: Option<Handler> = None;
            for value in fields(value, "handler") {
                if value.is_null() {
                    handler = None;
                    continue;
                }
                if !value.is_object() {
                    return Err(fail());
                }
                let mut next = handler.take().unwrap_or_default();
                text(value, "type", &mut next.kind)?;
                text(value, "id", &mut next.id)?;
                handler = Some(next);
            }
            Ok(Step {
                status_key,
                handler,
            })
        })
        .collect()
}

struct Workflow {
    name: String,
    default: bool,
    lead: Option<(String, Uuid)>,
    steps: Vec<Step>,
}

fn fail() -> ApiError {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "failed to resolve project workflow",
    )
}

fn unavailable() -> ApiError {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "project workflow mutation runtime unavailable",
    )
}

async fn load(
    tx: &Transaction<'_>,
    ws: Uuid,
    project: Option<Uuid>,
    require_project: bool,
) -> Result<Option<Workflow>, ApiError> {
    let Some(project) = project else {
        return Ok(None);
    };
    let project = tx
        .query_opt(
            "SELECT workflow_id,lead_type,lead_id FROM project WHERE id=$1 AND workspace_id=$2 FOR SHARE",
            &[&project, &ws],
        )
        .await
        .map_err(|_| fail())?;
    let Some(project) = project else {
        if require_project {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "project not found in this workspace",
            ));
        }
        // ForProject treats a legacy dangling project like no project.
        return Ok(None);
    };
    let lead = project
        .get::<_, Option<String>>("lead_type")
        .zip(project.get::<_, Option<Uuid>>("lead_id"));
    let named: Option<Uuid> = project.get("workflow_id");
    let row = if let Some(id) = named {
        tx.query_opt("SELECT name,is_default,steps FROM issue_workflow WHERE id=$1 AND workspace_id=$2 FOR SHARE", &[&id, &ws]).await.map_err(|_| fail())?
    } else {
        None
    };
    let row = match row {
        Some(row) => Some(row),
        None => tx.query_opt("SELECT name,is_default,steps FROM issue_workflow WHERE workspace_id=$1 AND is_default=true FOR SHARE", &[&ws]).await.map_err(|_| fail())?,
    };
    // No stored Default: the built-in workflow accepts every status and never hands off.
    row.map(|row| {
        let raw: Value = row.get("steps");
        let steps = decode(raw)?;
        Ok(Workflow {
            name: row.get("name"),
            default: row.get("is_default"),
            lead,
            steps,
        })
    })
    .transpose()
}

fn check_status(def: &Workflow, status: &str) -> Result<(), Box<Response>> {
    if def.default || def.steps.iter().any(|s| s.status_key == status) {
        return Ok(());
    }
    let keys: Vec<_> = def.steps.iter().map(|s| s.status_key.as_str()).collect();
    Err(Box::new((StatusCode::BAD_REQUEST, GoJson(json!({
        "error":format!("status {status:?} is not part of this project's workflow {:?}; use one of: {}",def.name,keys.join(", ")),
        "code":"status_not_in_workflow", "workflow_name":def.name, "allowed_statuses":keys
    }))).into_response()))
}

#[derive(Clone)]
pub(in crate::api) struct Handoff {
    pub kind: String,
    pub id: Uuid,
    pub workflow: String,
}

impl Handoff {
    pub fn payload(&self) -> Value {
        json!({"workflow":self.workflow,"to_type":self.kind,"to_id":self.id,"run":"false"})
    }
}

// A step may name a handler without having anyone to assign, e.g. a project
// without a lead. Match ResolveHandler before applying the runtime fence.
fn resolve_handler(def: &Workflow, status: &str, creator: (&str, Uuid)) -> Option<Handoff> {
    let step = def.steps.iter().find(|step| step.status_key == status)?;
    let handler = step.handler.as_ref()?;
    let (kind, id) = match handler.kind.as_str() {
        "agent" | "squad" | "member" => (handler.kind.clone(), Uuid::parse_str(&handler.id).ok()?),
        "project_lead" => def.lead.clone()?,
        "creator" if !creator.0.is_empty() => (creator.0.to_owned(), creator.1),
        _ => return None,
    };
    Some(Handoff {
        kind,
        id,
        workflow: def.name.clone(),
    })
}

async fn member_handoff(
    tx: &Transaction<'_>,
    ws: Uuid,
    def: &Workflow,
    status: &str,
    creator: (&str, Uuid),
) -> Result<Option<Handoff>, ApiError> {
    let Some(handoff) = resolve_handler(def, status, creator) else {
        return Ok(None);
    };
    // Agent/squad assignment must invoke the run adapter and permission gate.
    if handoff.kind != "member" {
        return Err(unavailable());
    }
    let member = tx
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id=$1 AND user_id=$2 FOR SHARE",
            &[&ws, &handoff.id],
        )
        .await
        .map_err(|_| fail())?;
    if member.is_none() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "assignee_id does not refer to a member of this workspace",
        ));
    }
    Ok(Some(handoff))
}

/// Admit a new issue and assign its workflow handler unless the caller chose
/// an assignee. Only member handlers work without the agent dispatch adapter.
pub(in crate::api) async fn create(
    tx: &Transaction<'_>,
    ws: Uuid,
    project: Option<Uuid>,
    status: &str,
    assignee_explicit: bool,
    creator: (&str, Uuid),
) -> Result<Result<Option<Handoff>, Response>, ApiError> {
    let Some(def) = load(tx, ws, project, true).await? else {
        return Ok(Ok(None));
    };
    if let Err(response) = check_status(&def, status) {
        return Ok(Err(*response));
    }
    if assignee_explicit {
        return Ok(Ok(None));
    }
    Ok(Ok(member_handoff(tx, ws, &def, status, creator).await?))
}

fn category<'a>(key: &str, custom: &'a [(String, String)]) -> &'a str {
    match key {
        "backlog" | "todo" => "unstarted",
        "in_progress" | "in_review" | "blocked" => "started",
        "done" => "done",
        "cancelled" => "closed",
        _ => custom
            .iter()
            .find(|(name, category)| {
                name == key
                    && matches!(
                        category.as_str(),
                        "unstarted" | "started" | "done" | "closed"
                    )
            })
            .map_or("", |(_, category)| category.as_str()),
    }
}

fn mapped_status(def: &Workflow, previous: &str, custom: &[(String, String)]) -> Option<String> {
    if def.steps.iter().any(|step| step.status_key == previous) {
        return None;
    }
    let previous_category = category(previous, custom);
    let target = def
        .steps
        .iter()
        .find(|step| {
            !previous_category.is_empty() && category(&step.status_key, custom) == previous_category
        })
        .map_or("todo", |step| step.status_key.as_str());
    Some(target.to_owned())
}

async fn move_target(
    tx: &Transaction<'_>,
    ws: Uuid,
    def: &Workflow,
    previous: &str,
) -> Result<Option<String>, ApiError> {
    if def.steps.iter().any(|step| step.status_key == previous) {
        return Ok(None);
    }
    let keys: Vec<_> = std::iter::once(previous.to_owned())
        .chain(def.steps.iter().map(|step| step.status_key.clone()))
        .collect();
    let rows = tx
        .query(
            "SELECT key,category FROM issue_status WHERE workspace_id=$1 AND key=ANY($2::text[])",
            &[&ws, &keys],
        )
        .await
        .map_err(|_| fail())?;
    let custom: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row.get("key"), row.get("category")))
        .collect();
    Ok(mapped_status(def, previous, &custom))
}

// A task token may be used on an unrelated issue. Only the run bound to this
// issue owns its workflow step; a foreign run must not gate its status writes.
pub(in crate::api) async fn caller_step(
    tx: &Transaction<'_>,
    issue: Uuid,
    task: &str,
) -> Result<Option<String>, ApiError> {
    let Ok(task) = Uuid::parse_str(task) else {
        return Ok(None);
    };
    tx.query_opt(
        "SELECT workflow_step FROM agent_task_queue WHERE id=$1 AND issue_id=$2",
        &[&task, &issue],
    )
    .await
    .map(|row| row.and_then(|row| row.get("workflow_step")))
    .map_err(|_| fail())
}

// Advance after committing the issue, like Go: the task owner may lock a task
// before the issue, so holding an issue lock while updating a task deadlocks.
// Compare the old step so a concurrent writer cannot rewind it.
pub(in crate::api) async fn advance_step(
    db: &impl GenericClient,
    issue: Uuid,
    task: &str,
    previous: &str,
    next: &str,
) -> Result<(), ApiError> {
    if previous == next {
        return Ok(());
    }
    let task = Uuid::parse_str(task).map_err(|_| fail())?;
    let updated = db.execute(
        "UPDATE agent_task_queue SET workflow_step=$1 WHERE id=$2 AND issue_id=$3 AND workflow_step=$4",
        &[&next, &task, &issue, &previous],
    )
    .await
    .map_err(|_| fail())?;
    if updated != 1 {
        return Err(unavailable());
    }
    Ok(())
}

fn status_label(name: Option<&str>, key: &str) -> String {
    match name {
        Some(name) if !name.is_empty() && name != key => format!("{name:?} ({key})"),
        _ => format!("{key:?}"),
    }
}

async fn step_moved(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    number: i32,
    step: &str,
    current: &str,
) -> Response {
    let prefix = tx
        .query_opt(
            "SELECT name,issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .ok()
        .flatten()
        .map(|row| {
            let stored: String = row.get("issue_prefix");
            if stored.is_empty() {
                legacy_prefix(row.get("name"))
            } else {
                stored
            }
        })
        .unwrap_or_default();
    let names = tx
        .query(
            "SELECT key,name FROM issue_status WHERE workspace_id=$1 AND key=ANY($2::text[])",
            &[&ws, &vec![step, current]],
        )
        .await
        .unwrap_or_default();
    let name = |key: &str| {
        if matches!(
            key,
            "backlog" | "todo" | "in_progress" | "in_review" | "done" | "blocked" | "cancelled"
        ) {
            return None;
        }
        names
            .iter()
            .find(|row| row.get::<_, String>("key") == key)
            .map(|row| row.get::<_, String>("name"))
    };
    let moved_by = match tx
        .query_opt(
            "SELECT actor_type,actor_id FROM activity_log WHERE issue_id=$1 AND action='status_changed' ORDER BY created_at DESC,id DESC LIMIT 1",
            &[&issue],
        )
        .await
    {
        Ok(Some(row)) => {
            let kind: Option<String> = row.get("actor_type");
            let id: Option<Uuid> = row.get("actor_id");
            if let (Some(kind), Some(id)) = (kind, id) {
                let query = match kind.as_str() {
                    "agent" => Some("SELECT name FROM agent WHERE id=$1 AND workspace_id=$2"),
                    "squad" => Some("SELECT name FROM squad WHERE id=$1 AND workspace_id=$2"),
                    "member" => Some("SELECT name FROM \"user\" WHERE id=$1"),
                    _ => None,
                };
                if let Some(query) = query {
                    let row = if kind == "member" {
                        tx.query_opt(query, &[&id]).await
                    } else {
                        tx.query_opt(query, &[&id, &ws]).await
                    };
                    row.ok().flatten().map(|row| row.get("name")).unwrap_or(kind)
                } else {
                    kind
                }
            } else {
                "someone else".to_owned()
            }
        }
        _ => "someone else".to_owned(),
    };
    let message = format!(
        "{prefix}-{number} is no longer at {}: {moved_by} moved it to {} while this run was working, so this status change was not applied. Do not change the status again in this run; leave a comment if something still needs attention.",
        status_label(name(step).as_deref(), step),
        status_label(name(current).as_deref(), current),
    );
    (
        StatusCode::CONFLICT,
        GoJson(json!({
            "code":"workflow_step_moved", "step":step,
            "current_status":current, "error":message
        })),
    )
        .into_response()
}

/// Enforce a project's statuses, remap moves by lifecycle category, and
/// resolve member handoffs. Agent/squad handoffs need the dispatch runtime.
#[allow(clippy::too_many_arguments)]
pub(in crate::api) async fn update(
    tx: &Transaction<'_>,
    ws: Uuid,
    issue: Uuid,
    number: i32,
    previous_project: Option<Uuid>,
    target_project: Option<Uuid>,
    previous_status: &str,
    requested_status: &mut Option<String>,
    assignee_explicit: bool,
    entering_on_move: bool,
    triage: bool,
    caller_step: Option<&str>,
    creator: (&str, Uuid),
) -> Result<Result<Option<Handoff>, Response>, ApiError> {
    let Some(def) = load(tx, ws, target_project, false).await? else {
        return Ok(Ok(None));
    };
    let moving = previous_project != target_project;
    let status_explicit = requested_status.is_some();
    if moving && requested_status.is_none() && !def.default {
        *requested_status = move_target(tx, ws, &def, previous_status).await?;
    }
    let status = requested_status.as_deref().unwrap_or(previous_status);
    if let Some(step) = caller_step.filter(|step| *step != previous_status)
        && (!def.default
            || def.steps.iter().any(|step| {
                step.handler.as_ref().is_some_and(|handler| {
                    matches!(
                        handler.kind.as_str(),
                        "agent" | "squad" | "member" | "project_lead" | "creator"
                    )
                })
            }))
        && status_explicit
        && status != previous_status
    {
        // A bound run cannot undo a move another writer made while it worked.
        return Ok(Err(step_moved(
            tx,
            ws,
            issue,
            number,
            step,
            previous_status,
        )
        .await));
    }
    if requested_status.is_some()
        && let Err(response) = check_status(&def, status)
    {
        return Ok(Err(*response));
    }
    let entering =
        (moving && entering_on_move) || (!moving && status_explicit && status != previous_status);
    if entering && !assignee_explicit && !triage {
        return Ok(Ok(member_handoff(tx, ws, &def, status, creator).await?));
    }
    Ok(Ok(None))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_steps_preserve_folded_handoffs_and_nulls() {
        let def = Workflow {
            name: "Folded".into(),
            default: false,
            lead: None,
            steps: decode(json!([
                null,
                {"STATUS_KEY":"todo", "HANDLER":{"TYPE":"agent","ID":"synthetic"}},
                {"status_key":"done", "handler":null},
                {"status_key":"blocked", "handler":{"type":null}}
            ]))
            .unwrap(),
        };
        assert_eq!(def.steps[1].handler.as_ref().unwrap().id, "synthetic");
        let creator = ("member", Uuid::nil());
        assert!(resolve_handler(&def, "todo", creator).is_none());
        assert!(resolve_handler(&def, "done", creator).is_none());
        assert!(resolve_handler(&def, "blocked", creator).is_none());
        assert_eq!(def.steps[0].status_key, "");
        for raw in [
            json!([false]),
            json!([{"handler":false}]),
            json!([{"status_key":1}]),
            json!([{"handler":{"id":1}}]),
            json!([{"instructions":1}]),
        ] {
            assert!(decode(raw).is_err());
        }
    }

    #[test]
    fn concrete_and_relative_handlers_resolve_without_inventing_targets() {
        let member = Uuid::new_v4();
        let creator = Uuid::new_v4();
        let mut def = Workflow {
            name: "Handlers".into(),
            default: false,
            lead: Some(("member".into(), member)),
            steps: decode(json!([
                {"status_key":"todo","handler":{"type":"member","id":member}},
                {"status_key":"blocked","handler":{"type":"project_lead"}},
                {"status_key":"in_review","handler":{"type":"creator"}},
                {"status_key":"done","handler":{"type":"unknown","id":member}}
            ]))
            .unwrap(),
        };
        for status in ["todo", "blocked"] {
            let resolved = resolve_handler(&def, status, ("member", creator)).unwrap();
            assert_eq!(resolved.id, member);
            assert_eq!(
                resolved.payload(),
                json!({"workflow":"Handlers","to_type":"member","to_id":member,"run":"false"})
            );
        }
        let resolved = resolve_handler(&def, "in_review", ("agent", creator)).unwrap();
        assert_eq!((resolved.kind.as_str(), resolved.id), ("agent", creator));
        assert!(resolve_handler(&def, "done", ("member", creator)).is_none());
        assert!(resolve_handler(&def, "in_review", ("", creator)).is_none());
        def.lead = None;
        assert!(resolve_handler(&def, "blocked", ("member", creator)).is_none());
    }

    #[test]
    fn project_move_uses_first_matching_lifecycle_category() {
        let def = Workflow {
            name: "Target".into(),
            default: false,
            lead: None,
            steps: ["review", "cancelled", "todo", "in_progress", "done"]
                .map(|status_key| Step {
                    status_key: status_key.into(),
                    handler: None,
                })
                .into(),
        };
        let custom = vec![
            ("review".into(), "started".into()),
            ("old".into(), "started".into()),
        ];
        for (previous, expected) in [
            ("in_review", Some("review")),
            ("blocked", Some("review")),
            ("old", Some("review")),
            ("backlog", Some("todo")),
            ("done", None),
            ("cancelled", None),
            ("unknown", Some("todo")),
        ] {
            assert_eq!(mapped_status(&def, previous, &custom).as_deref(), expected);
        }
        assert_eq!(
            category("in_review", &[("in_review".into(), "closed".into())]),
            "started"
        );
        assert_eq!(category("bad", &[("bad".into(), "invalid".into())]), "");
    }

    #[test]
    fn named_status_error_and_handoff() {
        let def = Workflow {
            name: "Review".into(),
            default: false,
            lead: None,
            steps: vec![
                Step {
                    status_key: "todo".into(),
                    handler: None,
                },
                Step {
                    status_key: "in_review".into(),
                    handler: Some(Handler {
                        kind: "agent".into(),
                        id: Uuid::nil().to_string(),
                    }),
                },
            ],
        };
        assert!(check_status(&def, "todo").is_ok());
        assert_eq!(
            resolve_handler(&def, "in_review", ("member", Uuid::nil()))
                .unwrap()
                .kind,
            "agent"
        );
        assert!(resolve_handler(&def, "todo", ("member", Uuid::nil())).is_none());
        assert!(check_status(&def, "done").is_err());
        let default = Workflow {
            default: true,
            ..def
        };
        assert!(check_status(&default, "done").is_ok());
    }
}
