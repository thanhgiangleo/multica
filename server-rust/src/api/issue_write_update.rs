//! Atomic issue edits and member workflow entry. Task effects fail closed.
#[path = "issue_write_issue_decode.rs"]
pub(super) mod issue_decode;
#[path = "issue_write_status.rs"]
pub(super) mod status;
#[path = "issue_write_workflow.rs"]
pub(super) mod workflow;
use super::{
    ApiError, ApiState, GoJson, Identity,
    actor::Actor,
    catalog::WorkspaceId,
    error,
    issue_read::load_issue,
    issue_write::{IssueUpdatedEvent, write_actor},
    issue_write_structure::{self},
    squad::legacy_prefix,
    timestamp,
};
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::put,
};
use chrono::{DateTime, Local, NaiveDate, SecondsFormat, Utc};
use issue_decode::UpdateRequest as Request;
use serde_json::{Value, json};
use std::collections::HashSet;
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}", put(update))
        .route("/api/issues/{id}/", put(update))
}

struct Parsed {
    req: Request,
    start_touched: bool,
    start: Option<NaiveDate>,
    due_touched: bool,
    due: Option<NaiveDate>,
    attachments: Vec<Uuid>,
    raw: Value,
}
fn parse(body: &[u8]) -> Result<Parsed, ApiError> {
    let invalid = || error(StatusCode::BAD_REQUEST, "invalid request body");
    let (req, raw) = issue_decode::decode::<Request>(body).map_err(|_| invalid())?;
    if req.expected_revision.is_some_and(|n| n < 1) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "expected_revision must be a positive integer",
        ));
    }
    if let Some(priority) = &req.priority
        && !["urgent", "high", "medium", "low", "none"].contains(&priority.as_str())
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "priority must be one of: urgent, high, medium, low, none",
        ));
    }
    fn date(value: &Option<String>, field: &'static str) -> Result<Option<NaiveDate>, ApiError> {
        let Some(s) = value.as_ref().filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        super::issue_write::calendar::parse(s)
            .map(Some)
            .ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    if field == "start_date" {
                        "invalid start_date format, expected YYYY-MM-DD"
                    } else {
                        "invalid due_date format, expected YYYY-MM-DD"
                    },
                )
            })
    }
    let start = date(&req.start_date, "start_date")?;
    let due = date(&req.due_date, "due_date")?;
    let attachments = req
        .attachment_ids
        .as_ref()
        .into_iter()
        .flatten()
        .map(|s| {
            Uuid::parse_str(s).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid attachment_ids"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Parsed {
        start_touched: raw.get("start_date").is_some(),
        due_touched: raw.get("due_date").is_some(),
        start,
        due,
        attachments,
        req,
        raw,
    })
}

fn conflict(id: Uuid, expected: Option<i64>, actual: i64) -> Response {
    let body = if let Some(expected) = expected {
        json!({"error":"resource changed since it was loaded","code":"revision_conflict","resource_type":"issue","resource_id":id,"expected_revision":expected,"actual_revision":actual})
    } else {
        json!({"error":"resource field changed since it was loaded","code":"revision_conflict","resource_type":"issue","resource_id":id})
    };
    (StatusCode::CONFLICT, GoJson(body)).into_response()
}

fn marked_ids(text: &str) -> Vec<Uuid> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for tail in text.split("<!-- multica:channel-media:").skip(1) {
        let Some(value) = tail.get(..36) else {
            continue;
        };
        if tail.get(36..40) != Some(" -->")
            || !value.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
        {
            continue;
        }
        if let Ok(id) = Uuid::parse_str(value)
            && seen.insert(id)
        {
            ids.push(id);
        }
    }
    ids
}
fn append(text: &mut String, block: &str) {
    if !text.is_empty() {
        text.push_str("\n\n");
    }
    text.push_str(block);
}
pub(super) fn merge_media(
    current: &str,
    incoming: &str,
    base: Option<&str>,
    attachments: &[(Uuid, String, String)],
) -> String {
    let known: HashSet<_> = base
        .map(marked_ids)
        .unwrap_or_default()
        .into_iter()
        .collect();
    let mut merged = incoming.to_owned();
    for id in marked_ids(current) {
        let Some((_, name, kind)) = attachments.iter().find(|a| a.0 == id) else {
            continue;
        };
        let path = format!("/api/attachments/{id}/download");
        let marker = format!("<!-- multica:channel-media:{id} -->");
        let has_link = merged.contains(&path);
        if base.is_some() && known.contains(&id) && !has_link {
            continue;
        }
        if !has_link {
            let block = if kind.starts_with("image/") {
                format!("![]({path})\n\n{marker}")
            } else {
                let label = name
                    .replace('\\', "\\\\")
                    .replace('[', "\\[")
                    .replace(']', "\\]")
                    .replace(['\r', '\n'], " ");
                format!(
                    "[{}]({path})\n\n{marker}",
                    if label.is_empty() {
                        "attachment"
                    } else {
                        &label
                    }
                )
            };
            append(&mut merged, &block);
        } else if !merged.contains(&marker) {
            append(&mut merged, &marker);
        }
    }
    merged
}

pub(super) async fn response(tx: &Transaction<'_>, row: &Row, ws: Uuid) -> Result<Value, ApiError> {
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue");
    let workspace = tx
        .query_one(
            "SELECT name,issue_prefix FROM workspace WHERE id=$1",
            &[&ws],
        )
        .await
        .map_err(|_| fail())?;
    let stored: String = workspace.get("issue_prefix");
    let prefix = if stored.is_empty() {
        legacy_prefix(workspace.get("name"))
    } else {
        stored
    };
    let status: String = row.get("status");
    let category = tx
        .query_opt(
            "SELECT category,name,is_system FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&ws, &status],
        )
        .await
        .map_err(|_| fail())?;
    let mut value = json!({
      "id":row.get::<_,Uuid>("id"),"workspace_id":ws,"number":row.get::<_,i32>("number"),
      "identifier":format!("{prefix}-{}",row.get::<_,i32>("number")),"title":row.get::<_,String>("title"),
      "description":row.get::<_,Option<String>>("description"),"status":status,"status_name":"",
      "priority":row.get::<_,String>("priority"),"assignee_type":row.get::<_,Option<String>>("assignee_type"),
      "assignee_id":row.get::<_,Option<Uuid>>("assignee_id"),"creator_type":row.get::<_,String>("creator_type"),
      "creator_id":row.get::<_,Uuid>("creator_id"),"parent_issue_id":row.get::<_,Option<Uuid>>("parent_issue_id"),
      "duplicate_of":null,"project_id":row.get::<_,Option<Uuid>>("project_id"),"position":row.get::<_,f64>("position"),
      "stage":row.get::<_,Option<i32>>("stage"),"start_date":row.get::<_,Option<NaiveDate>>("start_date").map(|d|d.to_string()),
      "due_date":row.get::<_,Option<NaiveDate>>("due_date").map(|d|d.to_string()),
      "created_at":timestamp(row.get::<_,DateTime<Utc>>("created_at")),"updated_at":timestamp(row.get::<_,DateTime<Utc>>("updated_at")),
      "revision":row.get::<_,i64>("revision"),
      "last_activity_at":row.get::<_,Option<DateTime<Utc>>>("last_activity_at").map(|t|t.with_timezone(&Local).to_rfc3339_opts(SecondsFormat::AutoSi,true)),
      "metadata":row.get::<_,Value>("metadata"),"properties":row.get::<_,Value>("properties")
    });
    if [
        "backlog",
        "todo",
        "in_progress",
        "in_review",
        "blocked",
        "done",
        "cancelled",
    ]
    .contains(&status.as_str())
    {
        value["status_category"] = json!(status);
    }
    if value.get("status_category").is_none()
        && let Some(category) = category
    {
        let stored: String = category.get("category");
        value["status_category"] = json!(match stored.as_str() {
            "unstarted" => "todo",
            "started" => "in_progress",
            "closed" => "cancelled",
            other => other,
        });
        value["status_name"] = json!(category.get::<_, String>("name"));
    }
    if status == "cancelled"
        && let Some(original) = row.get::<_, Option<Uuid>>("duplicate_of_issue_id")
        && let Some(original) = tx
            .query_opt(
                "SELECT id,number,title,status FROM issue WHERE id=$1 AND workspace_id=$2",
                &[&original, &ws],
            )
            .await
            .map_err(|_| fail())?
    {
        value["duplicate_of"] = json!({"id":original.get::<_,Uuid>("id"),"identifier":format!("{prefix}-{}",original.get::<_,i32>("number")),"title":original.get::<_,String>("title"),"status":original.get::<_,String>("status")});
    }
    Ok(value)
}

pub(super) async fn update(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let sink = state.issue_events.as_ref().ok_or_else(|| {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue event bus unavailable",
        )
    })?;
    let (actor_type, actor_id, task) = write_actor(actor, user)?;
    let fail = || error(StatusCode::INTERNAL_SERVER_ERROR, "failed to update issue");
    let mut db = state.pool.get().await.map_err(|_| fail())?;
    let issue = load_issue(&db, ws, &raw).await?;
    let id: Uuid = issue.get("id");
    let mut parsed = parse(&body)?;
    let duplicate = status::duplicate(&parsed.raw, &mut parsed.req, id)?;
    if let Err(response) = status::preflight(&db, ws, parsed.req.status.as_deref()).await {
        return Ok(response);
    }
    // Retained wire fields; none can cause a run on this supported slice.
    let _ = (&parsed.req.suppress_run, &parsed.req.handoff_note);
    let structural = issue_write_structure::touched(&parsed.raw);
    let mut child_runtime =
        if issue_write_structure::child_touched(&parsed.raw) || parsed.req.status.is_some() {
            Some(state.issue_child_runtime.as_ref().ok_or_else(|| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "issue structural mutation runtime unavailable",
                )
            })?)
        } else {
            None
        };
    let tx = db.transaction().await.map_err(|_| fail())?;
    tx.execute("SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true),set_config('multica.source_task_id',$3,true)", &[&actor_type,&actor_id.to_string(),&task]).await.map_err(|_| fail())?;
    if structural {
        issue_write_structure::lock_graph(&tx, ws).await?;
    }
    if parsed.raw.get("project_id").is_some() {
        status::lock_namespace(&tx, ws).await?;
    }
    status::lock_catalog(&tx, ws, parsed.req.status.as_deref()).await?;
    if let Some(target) = duplicate
        && let Err(response) = status::check_duplicate(&tx, ws, id, target).await
    {
        return Ok(response);
    }
    if !parsed.attachments.is_empty() {
        tx.query("SELECT id FROM attachment WHERE workspace_id=$1 AND issue_id IS NULL AND source_context_id IS NULL AND id=ANY($2::uuid[]) ORDER BY id FOR UPDATE", &[&ws,&parsed.attachments]).await.map_err(|_| fail())?;
    }
    let current = tx
        .query_opt(
            "SELECT * FROM issue WHERE id=$1 AND workspace_id=$2 FOR UPDATE",
            &[&id, &ws],
        )
        .await
        .map_err(|_| fail())?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "issue not found"))?;
    if current.get::<_, Option<String>>("triage_state").is_some()
        && parsed.raw.get("parent_issue_id").is_some()
    {
        return Ok((StatusCode::BAD_REQUEST,GoJson(json!({"error":"the issue is in Triage, so its parent_issue_id cannot be set; accept it out of Triage first","code":"issue_in_triage"}))).into_response());
    }
    let revision: i64 = current.get("revision");
    if parsed.req.expected_revision.is_some_and(|r| r != revision) {
        return Ok(conflict(id, parsed.req.expected_revision, revision));
    }
    let prev_title: String = current.get("title");
    if let (Some(title), Some(base)) = (&parsed.req.title, &parsed.req.title_base)
        && prev_title != *base
        && prev_title != *title
    {
        return Ok(conflict(id, None, revision));
    }
    let plan = if structural {
        Some(
            issue_write_structure::plan(&tx, ws, id, &current, &parsed.req.structure, &parsed.raw)
                .await?,
        )
    } else {
        None
    };
    let parent = plan.as_ref().and_then(|p| p.parent);
    let project = plan.as_ref().and_then(|p| p.project);
    let stage = plan.as_ref().and_then(|p| p.stage);
    let caller_step = if actor_type == "agent" {
        workflow::caller_step(&tx, id, &task).await?
    } else {
        None
    };
    let handoff = if parsed.req.status.is_some() || parsed.raw.get("project_id").is_some() {
        match workflow::update(
            &tx,
            ws,
            id,
            current.get("number"),
            current.get("project_id"),
            plan.as_ref()
                .map_or_else(|| current.get("project_id"), |p| p.project),
            current.get::<_, String>("status").as_str(),
            &mut parsed.req.status,
            parsed.raw.get("assignee_type").is_some() || parsed.raw.get("assignee_id").is_some(),
            parsed.req.enter_workflow_step == Some(true),
            current.get::<_, Option<String>>("triage_state").is_some(),
            caller_step.as_deref(),
            (
                current.get::<_, String>("creator_type").as_str(),
                current.get("creator_id"),
            ),
        )
        .await?
        {
            Ok(handoff) => handoff,
            Err(response) => return Ok(response),
        }
    } else {
        None
    };
    // Frozen Go only stops prior runs for a workflow handoff. Member handlers
    // have no runs to stop; unrelated edits must not require a task adapter.
    // All admitted handoffs here target members, so an old agent/squad is
    // necessarily a different handler and still needs real cancellation.
    if parsed.req.stop_previous_assignee_runs == Some(true)
        && handoff.is_some()
        && current.get::<_, Option<Uuid>>("assignee_id").is_some()
        && matches!(
            current.get::<_, Option<String>>("assignee_type").as_deref(),
            Some("agent" | "squad")
        )
    {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "issue structural mutation runtime unavailable",
        ));
    }
    let assignee_type = handoff.as_ref().map_or_else(
        || plan.as_ref().and_then(|p| p.assignee_type.clone()),
        |h| Some(h.kind.clone()),
    );
    let assignee_id = handoff
        .as_ref()
        .map_or_else(|| plan.as_ref().and_then(|p| p.assignee_id), |h| Some(h.id));
    if parsed.req.status.is_some() {
        status::lock_catalog(&tx, ws, parsed.req.status.as_deref()).await?;
        if child_runtime.is_none() {
            child_runtime = Some(state.issue_child_runtime.as_ref().ok_or_else(|| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "issue structural mutation runtime unavailable",
                )
            })?);
        }
        status::dispatch_supported(&current, assignee_type.as_deref())?;
    }
    let description_touched = parsed.req.description.is_some();
    let prev_description: Option<String> = current.get("description");
    if let Some(incoming) = &parsed.req.description {
        let attachments=tx.query("SELECT id,filename,content_type FROM attachment WHERE issue_id=$1 AND workspace_id=$2 ORDER BY created_at,id", &[&id,&ws]).await.map_err(|_| fail())?;
        let attachments: Vec<_> = attachments
            .iter()
            .map(|a| (a.get("id"), a.get("filename"), a.get("content_type")))
            .collect();
        parsed.req.description = Some(merge_media(
            prev_description.as_deref().unwrap_or_default(),
            incoming,
            parsed.req.description_base.as_deref(),
            &attachments,
        ));
    }
    let mut updated = tx
        .query_one(
            include_str!("issue_write_update.sql"),
            &[
                &id,
                &ws,
                &parsed.req.title,
                &parsed.req.description,
                &parsed.req.priority,
                &parsed.req.position,
                &parsed.start_touched,
                &parsed.start,
                &parsed.due_touched,
                &parsed.due,
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
                &parsed.req.status,
                &duplicate,
            ],
        )
        .await
        .map_err(|_| fail())?;
    let linked = if parsed.attachments.is_empty() {
        false
    } else {
        let count=tx.execute("UPDATE attachment SET issue_id=$1 WHERE workspace_id=$2 AND issue_id IS NULL AND source_context_id IS NULL AND id=ANY($3::uuid[])", &[&id,&ws,&parsed.attachments]).await.map_err(|_| fail())?;
        if count > 0 && updated.get::<_, i64>("revision") == revision {
            updated=tx.query_one("UPDATE issue SET revision=revision+1,updated_at=now() WHERE id=$1 AND workspace_id=$2 RETURNING *", &[&id,&ws]).await.map_err(|_| fail())?;
        }
        count > 0
    };
    let cancelled = status::stop_closed(&tx, ws, id, parsed.req.status.as_deref()).await?;
    let value = response(&tx, &updated, ws).await?;
    let prev_start = current
        .get::<_, Option<NaiveDate>>("start_date")
        .map(|d| d.to_string());
    let prev_due = current
        .get::<_, Option<NaiveDate>>("due_date")
        .map(|d| d.to_string());
    let mark = |row: &Row| {
        if row.get::<_, String>("status") == "cancelled" {
            row.get::<_, Option<Uuid>>("duplicate_of_issue_id")
        } else {
            None
        }
    };
    let assignee_changed = (parsed.req.structure.assignee_type.is_some()
        || parsed.req.structure.assignee_id.is_some()
        || handoff.is_some())
        && (current.get::<_, Option<String>>("assignee_type")
            != updated.get::<_, Option<String>>("assignee_type")
            || current.get::<_, Option<Uuid>>("assignee_id")
                != updated.get::<_, Option<Uuid>>("assignee_id"));
    let project_changed = parsed.req.structure.project_id.is_some()
        && current.get::<_, Option<Uuid>>("project_id")
            != updated.get::<_, Option<Uuid>>("project_id");
    let status_changed = parsed.req.status.is_some()
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
        .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut payload = json!({"issue":value,"assignee_changed":assignee_changed,"status_changed":status_changed,"priority_changed":parsed.req.priority.is_some() && current.get::<_,String>("priority")!=updated.get::<_,String>("priority"),"project_changed":project_changed,
      "start_date_changed":current.get::<_,Option<NaiveDate>>("start_date")!=updated.get::<_,Option<NaiveDate>>("start_date"),
      "due_date_changed":current.get::<_,Option<NaiveDate>>("due_date")!=updated.get::<_,Option<NaiveDate>>("due_date"),
      "description_changed":description_touched,"title_changed":parsed.req.title.is_some() && prev_title!=updated.get::<_,String>("title"),
      "prev_title":prev_title,"prev_assignee_type":current.get::<_,Option<String>>("assignee_type"),"prev_assignee_id":current.get::<_,Option<Uuid>>("assignee_id"),
      "prev_status":current.get::<_,String>("status"),"prev_priority":current.get::<_,String>("priority"),"prev_start_date":prev_start,"prev_due_date":prev_due,"prev_description":prev_description,
      "creator_type":current.get::<_,String>("creator_type"),"creator_id":current.get::<_,Uuid>("creator_id"),"duplicate_of_issue_id":mark(&updated),"prev_duplicate_of_issue_id":mark(&current)});
    if let Some(handoff) = &handoff {
        payload["workflow_handoff"] = handoff.payload();
    }
    tx.commit().await.map_err(|_| fail())?;
    if let Some(step) = caller_step.as_deref()
        && let Err(err) =
            workflow::advance_step(&**db, id, &task, step, &updated.get::<_, String>("status"))
                .await
    {
        eprintln!("issue workflow step advancement failed for {id}: {err:?}");
    }
    for event in cancelled {
        sink.publish_task_cancelled(event);
    }
    // The owner snapshot precedes the attachment revision event, even on no-op.
    sink.publish_issue_updated(IssueUpdatedEvent {
        workspace_id: ws,
        actor_type,
        actor_id,
        payload,
    });
    if linked {
        sink.publish_assignment_changed(super::issue_write::IssueAssignmentEvent {
            event_type: "issue_attachments:changed",
            workspace_id: ws,
            actor_type,
            actor_id,
            payload: json!({"issue_id":id,"issue_revision":value["revision"]}),
        });
    }
    if let Some(runtime) = child_runtime
        && !parents.is_empty()
    {
        let mut parents = parents;
        parents.sort_unstable();
        parents.dedup();
        runtime.process_child_events(ws, parents);
    }
    Ok(GoJson(value).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn presence_and_validation() {
        let p = parse(br#"{"title":null,"start_date":null,"due_date":""}"#).unwrap();
        assert!(
            p.req.title.is_none()
                && p.start_touched
                && p.start.is_none()
                && p.due_touched
                && p.due.is_none()
        );
        for body in [
            json!({"expected_revision":0}),
            json!({"expected_revision":1.2}),
            json!({"priority":"urgent!"}),
            json!({"due_date":"2026-02-29"}),
            json!({"start_date":"2026-2-01"}),
            json!({"attachment_ids":["invalid"]}),
            json!([]),
        ] {
            assert!(parse(body.to_string().as_bytes()).is_err(), "{body}");
        }
        assert!(parse(b"null").is_ok());
    }
    #[test]
    fn media_merge_matches_go_provenance_rules() {
        let id = Uuid::new_v4();
        let absent = Uuid::new_v4();
        let marker = format!("<!-- multica:channel-media:{id} -->");
        let path = format!("/api/attachments/{id}/download");
        let current = format!("old\n\n{marker}\n\n<!-- multica:channel-media:{absent} -->");
        let atts = vec![(id, "[report]\\\n.txt".into(), "text/plain".into())];
        let preserved = merge_media(&current, "new", None, &atts);
        assert_eq!(
            preserved,
            format!("new\n\n[\\[report\\]\\\\ .txt]({path})\n\n{marker}")
        );
        assert_eq!(merge_media(&current, "new", Some(&current), &atts), "new");
        assert_eq!(
            merge_media(&current, &path, None, &atts),
            format!("{path}\n\n{marker}")
        );
        assert!(!preserved.contains(&absent.to_string()));
        assert_eq!(marked_ids(&format!("{marker}{marker}")), vec![id]);
    }
}
