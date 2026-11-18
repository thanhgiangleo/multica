//! Frozen BatchUpdateIssues: member/unassigned writes and workflow admission.
//! Per-item commit/skip, not an atomic batch or a count of distinct changed rows.
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_write::{IssueUpdatedEvent, write_actor},
    issue_write_structure,
    issue_write_update::{self, issue_decode, workflow},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{patch, post},
};
use chrono::NaiveDate;
use serde_json::{Value, json};
use tokio_postgres::{Client, Transaction};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/batch-update", post(batch))
        .route("/api/issues/batch", patch(batch))
}

type Updates = issue_decode::UpdateRequest;
struct Parsed {
    ids: Vec<String>,
    updates: Updates,
    raw: Value,
}
fn parse(body: &[u8]) -> Result<Parsed, ApiError> {
    let invalid = || error(StatusCode::BAD_REQUEST, "invalid request body");
    let (req, top) =
        issue_decode::decode::<issue_decode::BatchRequest>(body).map_err(|_| invalid())?;
    let ids = req.ids.unwrap_or_default();
    let updates = req.updates;
    let raw = top.get("updates").cloned().unwrap_or(Value::Null);
    if ids.is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "issue_ids is required"));
    }
    if raw.get("duplicate_of_issue_id").is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "duplicate_of_issue_id is not supported in batch updates",
        ));
    }
    Ok(Parsed { ids, updates, raw })
}
impl Parsed {
    fn has_mutation(&self) -> bool {
        let u = &self.updates;
        u.title.is_some()
            || u.description.is_some()
            || u.status.is_some()
            || u.priority.is_some()
            || u.position.is_some()
            || [
                "assignee_type",
                "assignee_id",
                "start_date",
                "due_date",
                "parent_issue_id",
                "project_id",
                "stage",
            ]
            .iter()
            .any(|key| self.raw.get(key).is_some())
    }
    fn structural(&self) -> bool {
        issue_write_structure::touched(&self.raw)
    }
}
fn date(raw: &Option<String>) -> Result<Option<NaiveDate>, ()> {
    let Some(raw) = raw.as_ref().filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    super::issue_write::calendar::parse(raw).map(Some).ok_or(())
}

async fn batch(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let parsed = parse(&body)?;
    let (actor_type, actor_id, task) = write_actor(actor, user)?;
    if !parsed.has_mutation() {
        return Ok(GoJson(json!({"updated":0})).into_response());
    }
    if parsed
        .updates
        .priority
        .as_ref()
        .is_some_and(|v| !["urgent", "high", "medium", "low", "none"].contains(&v.as_str()))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "priority must be one of: urgent, high, medium, low, none",
        ));
    }
    let child_runtime =
        if issue_write_structure::child_touched(&parsed.raw) || parsed.updates.status.is_some() {
            Some(state.issue_child_runtime.as_ref().ok_or_else(|| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "issue structural mutation runtime unavailable",
                )
            })?)
        } else {
            state.issue_child_runtime.as_ref()
        };
    // Triage and target-project scope are whole-request checks in frozen Go,
    // unlike malformed parents/assignees/stages, which skip individual items.
    let ids: Vec<Uuid> = parsed
        .ids
        .iter()
        .filter_map(|s| Uuid::parse_str(s).ok())
        .collect();
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to validate issues",
        )
    })?;
    if let Err(response) =
        issue_write_update::status::preflight(&db, ws, parsed.updates.status.as_deref()).await
    {
        return Ok(response);
    }
    if parsed.updates.status.is_some() {
        let unsupported: bool = db.query_one("SELECT EXISTS(SELECT 1 FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[]) AND assignee_type IN ('agent','squad'))", &[&ws,&ids]).await.map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR,"failed to validate issues"))?.get(0);
        if unsupported {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "issue status dispatch runtime unavailable",
            ));
        }
    }
    if parsed.raw.get("parent_issue_id").is_some() {
        let count: i64 = db.query_one("SELECT count(*) FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[]) AND triage_state IS NOT NULL", &[&ws,&ids]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to validate issues"))?.get(0);
        if count > 0 {
            return Ok((StatusCode::BAD_REQUEST,GoJson(json!({"error":"the issue is in Triage, so its parent_issue_id cannot be set; accept it out of Triage first","code":"issue_in_triage"}))).into_response());
        }
    }
    if parsed.raw.get("project_id").is_some()
        && let Some(raw) = &parsed.updates.structure.project_id
    {
        let id = Uuid::parse_str(raw)
            .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid project_id"))?;
        db.query_opt(
            "SELECT id FROM project WHERE id=$1 AND workspace_id=$2",
            &[&id, &ws],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to validate project",
            )
        })?
        .ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "project not found in this workspace",
            )
        })?;
    }
    if parsed.raw.get("assignee_type").is_some() || parsed.raw.get("assignee_id").is_some() {
        let kind = parsed.updates.structure.assignee_type.as_deref();
        let unsupported = matches!(kind,Some("agent"|"squad")) || (parsed.raw.get("assignee_type").is_none()
            && db.query_one("SELECT EXISTS(SELECT 1 FROM issue WHERE workspace_id=$1 AND id=ANY($2::uuid[]) AND assignee_type IN ('agent','squad'))", &[&ws,&ids]).await.map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to validate issues"))?.get::<_,bool>(0));
        if unsupported {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "agent assignment runtime unavailable",
            ));
        }
    }
    drop(db);
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    // Go treats malformed dates as per-item skips, not request errors. Other
    // edits in that same item must not leak through when either date is invalid.
    let (Ok(start), Ok(due)) = (
        date(&parsed.updates.start_date),
        date(&parsed.updates.due_date),
    ) else {
        return Ok(GoJson(json!({"updated":0})).into_response());
    };
    let mut updated = 0;
    let mut changed_parents = Vec::new();
    for raw in &parsed.ids {
        // Unlike single issue writes, the batch accepts UUIDs only, not keys.
        let Ok(id) = Uuid::parse_str(raw) else {
            continue;
        };
        let Ok(mut db) = state.pool.get().await else {
            continue;
        };
        match item(
            &mut db,
            ws,
            id,
            &parsed,
            start,
            due,
            actor_type,
            actor_id,
            &task,
            child_runtime.is_some(),
        )
        .await
        {
            Ok(Some((payload, parents, cancelled))) => {
                for event in cancelled {
                    sink.publish_task_cancelled(event);
                }
                sink.publish_issue_updated(IssueUpdatedEvent {
                    workspace_id: ws,
                    actor_type,
                    actor_id,
                    payload,
                });
                changed_parents.extend(parents);
                updated += 1;
            }
            Ok(None) => {}
            // Archive races on the batch's shared status are request-level
            // conflicts in Go, even though earlier items may have committed.
            Err(err) if err.0 == StatusCode::CONFLICT => return Err(err),
            // Other per-item validation/write failures are skipped. A
            // concurrent workflow change may reject only one item.
            Err(_) => {}
        }
    }
    if let Some(runtime) = child_runtime
        && !changed_parents.is_empty()
    {
        changed_parents.sort_unstable();
        changed_parents.dedup();
        runtime.process_child_events(ws, changed_parents);
    }
    Ok(GoJson(json!({"updated":updated})).into_response())
}

#[allow(clippy::too_many_arguments)]
async fn item(
    db: &mut Client,
    ws: Uuid,
    id: Uuid,
    parsed: &Parsed,
    start: Option<NaiveDate>,
    due: Option<NaiveDate>,
    actor_type: &str,
    actor_id: Uuid,
    task: &str,
    child_runtime_present: bool,
) -> Result<
    Option<(
        Value,
        Vec<Uuid>,
        Vec<super::issue_write::IssueTaskCancelledEvent>,
    )>,
    ApiError,
> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue");
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task]).await.map_err(|_|fail())?;
    if parsed.structural() {
        issue_write_structure::lock_graph(&tx, ws).await?;
    }
    if parsed.raw.get("project_id").is_some() {
        issue_write_update::status::lock_namespace(&tx, ws).await?;
    }
    issue_write_update::status::lock_catalog(&tx, ws, parsed.updates.status.as_deref()).await?;
    let Some(current) = tx
        .query_opt(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
    else {
        return Ok(None);
    };
    if current.get::<_, Option<String>>("triage_state").is_some()
        && parsed.raw.get("parent_issue_id").is_some()
    {
        return Ok(None);
    }
    let plan = if parsed.structural() {
        Some(
            issue_write_structure::plan(
                &tx,
                ws,
                id,
                &current,
                &parsed.updates.structure,
                &parsed.raw,
            )
            .await?,
        )
    } else {
        None
    };
    let parent = plan.as_ref().and_then(|p| p.parent);
    let project = plan.as_ref().and_then(|p| p.project);
    let stage = plan.as_ref().and_then(|p| p.stage);
    let u = &parsed.updates;
    let mut status = u.status.clone();
    let caller_step = if actor_type == "agent" {
        workflow::caller_step(&tx, id, task).await?
    } else {
        None
    };
    let handoff = if status.is_some() || parsed.raw.get("project_id").is_some() {
        match workflow::update(
            &tx,
            ws,
            id,
            current.get("number"),
            current.get("project_id"),
            plan.as_ref()
                .map_or_else(|| current.get("project_id"), |p| p.project),
            current.get::<_, String>("status").as_str(),
            &mut status,
            parsed.raw.get("assignee_type").is_some() || parsed.raw.get("assignee_id").is_some(),
            false,
            current.get::<_, Option<String>>("triage_state").is_some(),
            caller_step.as_deref(),
            (
                current.get::<_, String>("creator_type").as_str(),
                current.get("creator_id"),
            ),
        )
        .await
        {
            Ok(Ok(handoff)) => handoff,
            _ => return Ok(None),
        }
    } else {
        None
    };
    let assignee_type = handoff.as_ref().map_or_else(
        || plan.as_ref().and_then(|p| p.assignee_type.clone()),
        |h| Some(h.kind.clone()),
    );
    let assignee_id = handoff
        .as_ref()
        .map_or_else(|| plan.as_ref().and_then(|p| p.assignee_id), |h| Some(h.id));
    if status.is_some() {
        issue_write_update::status::lock_catalog(&tx, ws, status.as_deref()).await?;
        if !child_runtime_present {
            return Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "issue structural mutation runtime unavailable",
            ));
        }
        issue_write_update::status::dispatch_supported(&current, assignee_type.as_deref())?;
    }
    let description = if let Some(incoming) = &u.description {
        let attachments = media(&tx, ws, id).await?;
        Some(issue_write_update::merge_media(
            current
                .get::<_, Option<String>>("description")
                .as_deref()
                .unwrap_or_default(),
            incoming,
            None,
            &attachments,
        ))
    } else {
        None
    };
    let updated = tx
        .query_one(
            include_str!("issue_write_update.sql"),
            &[
                &id,
                &ws,
                &u.title,
                &description,
                &u.priority,
                &u.position,
                &parsed.raw.get("start_date").is_some(),
                &start,
                &parsed.raw.get("due_date").is_some(),
                &due,
                &parsed.raw.get("parent_issue_id").is_some(),
                &parent,
                &parsed.raw.get("project_id").is_some(),
                &project,
                &parsed.raw.get("stage").is_some(),
                &stage,
                &(parsed.raw.get("assignee_type").is_some()
                    || parsed.raw.get("assignee_id").is_some()
                    || handoff.is_some()),
                &assignee_type,
                &assignee_id,
                &status,
                &None::<Uuid>,
            ],
        )
        .await
        .map_err(|_| fail())?;
    let cancelled = issue_write_update::status::stop_closed(&tx, ws, id, status.as_deref()).await?;
    let issue = issue_write_update::response(&tx, &updated, ws).await?;
    let mark = |row: &tokio_postgres::Row| {
        if row.get::<_, String>("status") == "cancelled" {
            row.get::<_, Option<Uuid>>("duplicate_of_issue_id")
        } else {
            None
        }
    };
    // Batch has a narrower activity payload than UpdateIssue, even when title
    // or dates changed. Preserve that difference and emit on a no-op, too.
    let assignee_changed = (u.structure.assignee_type.is_some()
        || u.structure.assignee_id.is_some()
        || handoff.is_some())
        && (current.get::<_, Option<String>>("assignee_type")
            != updated.get::<_, Option<String>>("assignee_type")
            || current.get::<_, Option<Uuid>>("assignee_id")
                != updated.get::<_, Option<Uuid>>("assignee_id"));
    let project_changed = u.structure.project_id.is_some()
        && current.get::<_, Option<Uuid>>("project_id")
            != updated.get::<_, Option<Uuid>>("project_id");
    let status_changed = status.is_some()
        && current.get::<_, String>("status") != updated.get::<_, String>("status");
    let parents = if status_changed
        || current.get::<_, Option<Uuid>>("parent_issue_id")
            != updated.get::<_, Option<Uuid>>("parent_issue_id")
        || current.get::<_, Option<i32>>("stage") != updated.get::<_, Option<i32>>("stage")
    {
        [
            updated.get::<_, Option<Uuid>>("parent_issue_id"),
            current.get::<_, Option<Uuid>>("parent_issue_id"),
        ]
        .into_iter()
        .flatten()
        .collect()
    } else {
        Vec::new()
    };
    let mut payload = json!({"issue":issue,"assignee_changed":assignee_changed,"status_changed":status_changed,"priority_changed":u.priority.is_some() && current.get::<_,String>("priority") != updated.get::<_,String>("priority"),"project_changed":project_changed,"prev_status":current.get::<_,String>("status"),"duplicate_of_issue_id":mark(&updated),"prev_duplicate_of_issue_id":mark(&current)});
    if let Some(handoff) = &handoff {
        payload["workflow_handoff"] = handoff.payload();
    }
    tx.commit().await.map_err(|_| fail())?;
    if let Some(step) = caller_step.as_deref()
        && let Err(err) =
            workflow::advance_step(&*db, id, task, step, &updated.get::<_, String>("status")).await
    {
        eprintln!("issue workflow batch step advancement failed for {id}: {err:?}");
    }
    Ok(Some((payload, parents, cancelled)))
}
async fn media(
    tx: &Transaction<'_>,
    ws: Uuid,
    id: Uuid,
) -> Result<Vec<(Uuid, String, String)>, ApiError> {
    tx.query("SELECT id,filename,content_type FROM attachment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at,id", &[&id,&ws]).await.map(|rows| rows.iter().map(|r| (r.get("id"),r.get("filename"),r.get("content_type"))).collect()).map_err(|_|error(StatusCode::INTERNAL_SERVER_ERROR,"failed to update issue"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn go_batch_presence_and_ignored_fields() {
        for body in [
            json!({"updates":{"title":"x"}}),
            json!({"issue_ids":null}),
            json!({"issue_ids":[1]}),
            json!({"issue_ids":["x"],"updates":[]}),
            json!({"issue_ids":["x"],"updates":{"stage":1.5}}),
        ] {
            assert!(parse(body.to_string().as_bytes()).is_err(), "{body}");
        }
        for u in [
            json!(null),
            json!({}),
            json!({"status":null,"title":null}),
            json!({"expected_revision":-1,"attachment_ids":["bad"],"enter_workflow_step":true}),
        ] {
            assert!(
                !parse(
                    json!({"issue_ids":["bad"],"updates":u})
                        .to_string()
                        .as_bytes()
                )
                .unwrap()
                .has_mutation()
            );
        }
        for u in [
            json!({"start_date":null}),
            json!({"due_date":""}),
            json!({"title":""}),
            json!({"position":0}),
        ] {
            let p = parse(
                json!({"issue_ids":["bad"],"updates":u})
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
            assert!(p.has_mutation() && !p.structural());
        }
        assert_eq!(
            parse(br#"{"issue_ids":[null,"x"],"updates":{"attachment_ids":[null]}}"#)
                .unwrap()
                .ids,
            vec!["", "x"]
        );
        assert!(parse(br#"{"issue_ids":["x"],"updates":{"duplicate_of_issue_id":null}}"#).is_err());
        assert!(date(&Some("2026-02-29".into())).is_err());
        assert!(date(&Some("2026-2-01".into())).is_err());
        assert_eq!(date(&Some(String::new())), Ok(None));
    }
}
