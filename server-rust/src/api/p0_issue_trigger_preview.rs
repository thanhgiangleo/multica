//! Read-only issue-run preview, scoped by the catalog workspace-member guard.
//! Mirrors Go's PreviewIssueTrigger / WillEnqueueRun without dispatching work.
use super::super::{
    Actor, ApiError, ApiState, GoJson, Identity, agent_invoke, agent_ready, error, issue_read,
    issue_workflow,
};
use super::WorkspaceId;
use axum::{
    Router,
    body::Bytes,
    extract::{Extension, State},
    http::StatusCode,
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{GenericClient, Row};
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new().route("/api/issues/preview-trigger", post(preview))
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Request {
    issue_ids: Option<Vec<String>>,
    is_create: bool,
    assignee_type: Option<String>,
    assignee_id: Option<String>,
    status: Option<String>,
    project_id: Option<String>,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
struct Trigger {
    issue_id: String,
    agent_id: Uuid,
    source: &'static str,
}

#[derive(Serialize)]
struct Reply {
    triggers: Vec<Trigger>,
    total_count: usize,
}

#[derive(Clone)]
struct Candidate {
    id: Uuid,
    workspace: Uuid,
    status: String,
    assignee_type: Option<String>,
    assignee_id: Option<Uuid>,
    triage: bool,
}

impl Candidate {
    // Every selected column is known, but try_get prevents a mismatched DB
    // schema or unexpected NULL from panicking in a request handler.
    fn from_row(row: &Row) -> Option<Self> {
        Some(Self {
            id: row.try_get("id").ok()?,
            workspace: row.try_get("workspace_id").ok()?,
            status: row.try_get("status").ok()?,
            assignee_type: row.try_get("assignee_type").ok()?,
            assignee_id: row.try_get("assignee_id").ok()?,
            triage: row
                .try_get::<_, Option<String>>("triage_state")
                .ok()?
                .is_some(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    Assign,
    Status,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Self::Assign => "assign",
            Self::Status => "status",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Change {
    create: bool,
    assignee_changed: bool,
    status_changed: bool,
    entered_handoff: bool,
    self_loop: bool,
}

// Status resolution has already projected custom terminal categories. Only
// the fixed backlog key parks work; an unstarted custom status does not.
fn source(current: &str, previous: &str, change: Change) -> Option<Source> {
    if change.create || change.assignee_changed {
        return (current != "backlog").then_some(Source::Assign);
    }
    (change.status_changed
        && previous == "backlog"
        && !matches!(current, "backlog" | "done" | "cancelled")
        && !change.self_loop)
        .then_some(Source::Status)
}

#[derive(Default)]
struct Gates {
    runnable: bool,
    accessible: bool,
    suppress_active_self_assignment: bool,
    pending: bool,
}

fn decision(
    candidate: &Candidate,
    previous: &str,
    change: Change,
    gates: Gates,
    agent_id: Uuid,
) -> Option<Trigger> {
    if candidate.triage || candidate.assignee_type.is_none() || candidate.assignee_id.is_none() {
        return None;
    }
    let source = source(&candidate.status, previous, change)?;
    if !gates.runnable || !gates.accessible {
        return None;
    }
    if candidate.assignee_type.as_deref() == Some("agent")
        && source == Source::Assign
        && !change.create
        && !change.entered_handoff
        && gates.suppress_active_self_assignment
    {
        return None;
    }
    let dedup = source == Source::Status
        || (candidate.assignee_type.as_deref() == Some("agent") && change.entered_handoff);
    if dedup && gates.pending {
        return None;
    }
    Some(Trigger {
        issue_id: if candidate.id.is_nil() {
            String::new()
        } else {
            candidate.id.to_string()
        },
        agent_id,
        source: source.name(),
    })
}

// Go's Effective returns the original key on catalog lookup failure, and
// never treats custom unstarted/started keys as the built-in backlog key.
async fn effective(db: &impl GenericClient, workspace: Uuid, status: &str) -> String {
    if super::is_builtin_status(status) {
        return status.to_owned();
    }
    match db
        .query_opt(
            "SELECT category FROM issue_status WHERE workspace_id=$1 AND key=$2",
            &[&workspace, &status],
        )
        .await
    {
        Ok(Some(row)) => match row.try_get::<_, String>("category").ok().as_deref() {
            Some("done") => "done".into(),
            Some("closed") => "cancelled".into(),
            _ => status.into(),
        },
        _ => status.into(),
    }
}

async fn actor_context(
    db: &impl GenericClient,
    user: Uuid,
    actor: &Actor,
) -> (String, Uuid, Option<Uuid>, Option<Uuid>) {
    let Actor::Task(task) = actor else {
        return ("member".into(), user, Some(user), None);
    };
    // A terminal task cannot lend its originator's invoke permissions.
    let row = db
        .query_opt(
            "SELECT status,originator_user_id FROM agent_task_queue WHERE id=$1 AND agent_id=$2",
            &[&task.task, &task.agent],
        )
        .await
        .ok()
        .flatten();
    let originator = row.and_then(|r| {
        let status: String = r.try_get("status").ok()?;
        (!matches!(status.as_str(), "completed" | "failed" | "cancelled"))
            .then(|| {
                r.try_get::<_, Option<Uuid>>("originator_user_id")
                    .ok()
                    .flatten()
            })
            .flatten()
    });
    ("agent".into(), task.agent, originator, Some(task.task))
}

/// Go's `canAccessAgent` takes the same eight values; the actor pair and the
/// workspace are part of its signature, not incidental arguments.
#[allow(clippy::too_many_arguments)]
async fn can_access_agent(
    db: &impl GenericClient,
    agent: Uuid,
    owner: Option<Uuid>,
    permission_mode: &str,
    actor_type: &str,
    actor_id: Uuid,
    originator: Option<Uuid>,
    workspace: Uuid,
) -> bool {
    // The one invoke gate every Rust admission path asks; a failed query denies.
    agent_invoke::can_invoke(
        db,
        workspace,
        agent_invoke::InvokeTarget {
            id: agent,
            owner_id: owner,
            permission_mode,
        },
        agent_invoke::Invoker {
            actor_type,
            actor_id,
            originator,
        },
    )
    .await
    .unwrap_or(false)
}

// Unknown lookups fail closed, like the Go advisory probes.
async fn active(db: &impl GenericClient, issue: Uuid, agent: Uuid) -> bool {
    db.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND (status IN ('queued','dispatched','running','waiting_local_directory') OR (status='deferred' AND context->>'channel_issue_media_pending'='true')))",
        &[&issue, &agent],
    ).await.ok().and_then(|row| row.try_get::<_, bool>(0).ok()).unwrap_or(true)
}

async fn pending(db: &impl GenericClient, issue: Uuid, agent: Uuid) -> bool {
    // ResolveIssueReviewSHA chooses the newest live PR before a stale merged
    // sibling, on either VCS. A failed lookup falls back to no SHA, as in Go.
    let head: Option<String> = db.query_opt(
        "SELECT head_sha FROM (SELECT pr.head_sha,pr.state,pr.pr_updated_at FROM github_pull_request pr JOIN issue_pull_request ipr ON ipr.pull_request_id=pr.id WHERE ipr.issue_id=$1 AND pr.head_sha<>'' UNION ALL SELECT pr.head_sha,pr.state,pr.pr_updated_at FROM vcs_pull_request pr JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id=pr.id WHERE ipr.issue_id=$1 AND pr.head_sha<>'') combined ORDER BY (state IN ('open','draft')) DESC,pr_updated_at DESC LIMIT 1",
        &[&issue],
    ).await.ok().flatten().and_then(|row| row.try_get("head_sha").ok());
    db.query_one(
        "SELECT EXISTS(SELECT 1 FROM agent_task_queue WHERE context->>'wakeup_id' IS NULL AND issue_id=$1 AND agent_id=$2 AND (status IN ('queued','dispatched') OR (status='deferred' AND context->>'channel_issue_media_pending'='true')) AND (COALESCE($3::text,'')='' OR context->>'head_sha'=$3))",
        &[&issue, &agent, &head.filter(|s| !s.is_empty())],
    ).await.ok().and_then(|row| row.try_get::<_, bool>(0).ok()).unwrap_or(true)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate(
    db: &impl GenericClient,
    candidate: &Candidate,
    previous: &str,
    change: Change,
    actor_type: &str,
    actor_id: Uuid,
    originator: Option<Uuid>,
) -> Option<Trigger> {
    let assignee = candidate.assignee_id?;
    let current = effective(db, candidate.workspace, &candidate.status).await;
    let prev = effective(db, candidate.workspace, previous).await;
    let chosen = source(&current, &prev, change)?;
    if candidate.triage {
        return None;
    }
    let (agent_id, runnable, owner, permission_mode) =
        match candidate.assignee_type.as_deref()? {
            "agent" => {
                let row = db.query_opt(
                "SELECT id,owner_id,permission_mode,runtime_id,archived_at FROM agent WHERE id=$1",
                &[&assignee],
            ).await.ok().flatten()?;
                let runtime: Option<Uuid> = row.try_get("runtime_id").ok()?;
                let archived: Option<chrono::DateTime<chrono::Utc>> =
                    row.try_get("archived_at").ok()?;
                (
                    assignee,
                    runtime.is_some() && archived.is_none(),
                    row.try_get::<_, Option<Uuid>>("owner_id").ok()?,
                    row.try_get::<_, String>("permission_mode").ok()?,
                )
            }
            "squad" => {
                let squad = db
                    .query_opt(
                        "SELECT leader_id FROM squad WHERE id=$1 AND workspace_id=$2",
                        &[&assignee, &candidate.workspace],
                    )
                    .await
                    .ok()
                    .flatten()?;
                let leader: Uuid = squad.try_get("leader_id").ok()?;
                let row = db.query_opt(
                "SELECT owner_id,permission_mode,runtime_id,archived_at FROM agent WHERE id=$1",
                &[&leader],
            ).await.ok().flatten()?;
                let owner: Option<Uuid> = row.try_get("owner_id").ok()?;
                let runtime_id: Option<Uuid> = row.try_get("runtime_id").ok()?;
                let archived = row
                    .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
                    .ok()?
                    .is_some();
                let verdict = agent_ready::agent_readiness(
                    db,
                    agent_ready::ReadinessInput {
                        archived,
                        runtime_id,
                        owner_id: owner,
                    },
                )
                .await
                .ok()?;
                (
                    leader,
                    verdict.availability == agent_ready::AgentAvailability::Available,
                    owner,
                    row.try_get::<_, String>("permission_mode").ok()?,
                )
            }
            _ => return None,
        };
    if !runnable {
        return None;
    }
    let accessible = can_access_agent(
        db,
        agent_id,
        owner,
        &permission_mode,
        actor_type,
        actor_id,
        originator,
        candidate.workspace,
    )
    .await;
    if !accessible {
        return None;
    }
    let suppress = chosen == Source::Assign
        && !change.create
        && !change.entered_handoff
        && candidate.assignee_type.as_deref() == Some("agent")
        && actor_type == "agent"
        && actor_id == agent_id
        && active(db, candidate.id, agent_id).await;
    let needs_dedup = chosen == Source::Status
        || (candidate.assignee_type.as_deref() == Some("agent") && change.entered_handoff);
    let has_pending = needs_dedup && pending(db, candidate.id, agent_id).await;
    let mut projected = candidate.clone();
    projected.status = current;
    decision(
        &projected,
        &prev,
        change,
        Gates {
            runnable,
            accessible,
            suppress_active_self_assignment: suppress,
            pending: has_pending,
        },
        agent_id,
    )
}

async fn handoff(
    db: &deadpool_postgres::Client,
    ws: Uuid,
    project: Option<Uuid>,
    status: &str,
    creator_type: Option<&str>,
    creator_id: Option<Uuid>,
) -> Option<(String, Uuid)> {
    let (definition, project) = issue_workflow::for_project(db, ws, project).await.ok()??;
    let step = definition.step(status).filter(|step| step.hands_off())?;
    issue_workflow::resolve_handler(step, &project, creator_type, creator_id)
}

async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(workspace)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Result<GoJson<Reply>, ApiError> {
    let req: Request = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| {
            if v.is_null() {
                Some(Request::default())
            } else {
                serde_json::from_value(v).ok()
            }
        })
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if req.issue_ids.as_ref().is_some_and(|ids| ids.len() > 500) {
        return Err(error(StatusCode::BAD_REQUEST, "too many issue_ids"));
    }
    let new_assignee = req
        .assignee_type
        .as_deref()
        .filter(|v| !v.is_empty())
        .zip(req.assignee_id.as_deref().filter(|v| !v.is_empty()))
        .map(|(kind, id)| Uuid::parse_str(id).map(|id| (kind.to_owned(), id)))
        .transpose()
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid assignee_id"))?;
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to preview issue trigger",
        )
    })?;
    let (actor_type, actor_id, originator, task) = actor_context(&**db, user, &actor).await;
    let mut triggers = Vec::new();
    if req.is_create {
        // A not-yet-persisted issue has no row, so the returned id is empty
        // (Go uuidToString of the zero UUID).
        let mut candidate = Candidate {
            id: Uuid::nil(),
            workspace,
            status: req
                .status
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("todo")
                .into(),
            assignee_type: new_assignee.as_ref().map(|(kind, _)| kind.clone()),
            assignee_id: new_assignee.as_ref().map(|(_, id)| *id),
            triage: false,
        };
        if new_assignee.is_none()
            && let Some(project) = req
                .project_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .and_then(|id| Uuid::parse_str(id).ok())
            && let Some((kind, id)) = handoff(
                &db,
                workspace,
                Some(project),
                &candidate.status,
                Some(&actor_type),
                Some(actor_id),
            )
            .await
        {
            candidate.assignee_type = Some(kind);
            candidate.assignee_id = Some(id);
        }
        if let Some(trigger) = evaluate(
            &**db,
            &candidate,
            "",
            Change {
                create: true,
                ..Change::default()
            },
            &actor_type,
            actor_id,
            originator,
        )
        .await
        {
            triggers.push(trigger);
        }
    } else {
        for raw in req.issue_ids.as_deref().unwrap_or(&[]) {
            // GetIssueInWorkspace accepts UUID only, not a human-readable key.
            if Uuid::parse_str(raw).is_err() {
                continue;
            }
            let Ok(base) = issue_read::load_issue(&db, workspace, raw).await else {
                continue;
            };
            let Ok(issue_id) = base.try_get::<_, Uuid>("id") else {
                continue;
            };
            let Ok(Some(row)) = db.query_opt("SELECT id,workspace_id,status,assignee_type,assignee_id,triage_state,project_id,creator_type,creator_id FROM issue WHERE id=$1 AND workspace_id=$2", &[&issue_id, &workspace]).await else { continue; };
            let Some(mut candidate) = Candidate::from_row(&row) else {
                continue;
            };
            let previous = candidate.status.clone();
            let mut change = Change::default();
            if let Some((kind, id)) = &new_assignee {
                change.assignee_changed = candidate.assignee_type.as_deref() != Some(kind)
                    || candidate.assignee_id != Some(*id);
                candidate.assignee_type = Some(kind.clone());
                candidate.assignee_id = Some(*id);
            }
            if let Some(status) = req.status.as_deref().filter(|s| !s.is_empty()) {
                change.status_changed = previous != status;
                candidate.status = status.into();
                if new_assignee.is_none() && change.status_changed && !candidate.triage {
                    let project: Option<Uuid> = row.try_get("project_id").ok().flatten();
                    let creator_type: Option<String> = row.try_get("creator_type").ok();
                    let creator_id: Option<Uuid> = row.try_get("creator_id").ok();
                    if let Some((kind, id)) = handoff(
                        &db,
                        workspace,
                        project,
                        status,
                        creator_type.as_deref(),
                        creator_id,
                    )
                    .await
                    {
                        candidate.assignee_type = Some(kind);
                        candidate.assignee_id = Some(id);
                        change.assignee_changed = true;
                        change.entered_handoff = true;
                    }
                }
            }
            if change.status_changed && !change.assignee_changed {
                change.self_loop = if let Some(task_id) = task {
                    db.query_opt(
                        "SELECT issue_id FROM agent_task_queue WHERE id=$1",
                        &[&task_id],
                    )
                    .await
                    .ok()
                    .flatten()
                    .and_then(|r| r.try_get::<_, Option<Uuid>>("issue_id").ok().flatten())
                        == Some(candidate.id)
                } else {
                    false
                };
            }
            if let Some(trigger) = evaluate(
                &**db,
                &candidate,
                &previous,
                change,
                &actor_type,
                actor_id,
                originator,
            )
            .await
            {
                triggers.push(trigger);
            }
        }
    }
    Ok(GoJson(Reply {
        total_count: triggers.len(),
        triggers,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candidate(status: &str, kind: &str) -> Candidate {
        Candidate {
            id: Uuid::new_v4(),
            workspace: Uuid::new_v4(),
            status: status.into(),
            assignee_type: Some(kind.into()),
            assignee_id: Some(Uuid::new_v4()),
            triage: false,
        }
    }
    fn gates() -> Gates {
        Gates {
            runnable: true,
            accessible: true,
            ..Gates::default()
        }
    }
    #[test]
    fn pure_decision_matrix() {
        let agent = Uuid::new_v4();
        let assigned = Change {
            create: true,
            ..Change::default()
        };
        assert_eq!(
            decision(&candidate("todo", "agent"), "", assigned, gates(), agent)
                .unwrap()
                .source,
            "assign"
        );
        assert!(decision(&candidate("backlog", "agent"), "", assigned, gates(), agent).is_none());
        let mut triage = candidate("todo", "agent");
        triage.triage = true;
        assert!(decision(&triage, "", assigned, gates(), agent).is_none());
        let status = Change {
            status_changed: true,
            ..Change::default()
        };
        assert_eq!(
            decision(
                &candidate("in_progress", "squad"),
                "backlog",
                status,
                gates(),
                agent
            )
            .unwrap()
            .source,
            "status"
        );
        for target in ["backlog", "done", "cancelled"] {
            assert!(
                decision(
                    &candidate(target, "agent"),
                    "backlog",
                    status,
                    gates(),
                    agent
                )
                .is_none()
            );
        }
        assert!(decision(&candidate("todo", "agent"), "todo", status, gates(), agent).is_none());
        assert!(
            decision(
                &candidate("todo", "agent"),
                "backlog",
                Change {
                    self_loop: true,
                    ..status
                },
                gates(),
                agent
            )
            .is_none()
        );
        assert!(
            decision(
                &candidate("todo", "agent"),
                "backlog",
                status,
                Gates {
                    pending: true,
                    ..gates()
                },
                agent
            )
            .is_none()
        );
        assert!(
            decision(
                &candidate("todo", "agent"),
                "",
                assigned,
                Gates {
                    runnable: false,
                    ..gates()
                },
                agent
            )
            .is_none()
        );
        assert!(
            decision(
                &candidate("todo", "agent"),
                "",
                assigned,
                Gates {
                    accessible: false,
                    ..gates()
                },
                agent
            )
            .is_none()
        );
        let changed = Change {
            assignee_changed: true,
            ..Change::default()
        };
        assert!(
            decision(
                &candidate("todo", "agent"),
                "todo",
                changed,
                Gates {
                    suppress_active_self_assignment: true,
                    ..gates()
                },
                agent
            )
            .is_none()
        );
        assert!(
            decision(
                &candidate("todo", "squad"),
                "todo",
                changed,
                Gates {
                    suppress_active_self_assignment: true,
                    ..gates()
                },
                agent
            )
            .is_some()
        );
        let handoff = Change {
            entered_handoff: true,
            ..changed
        };
        assert!(
            decision(
                &candidate("todo", "agent"),
                "todo",
                handoff,
                Gates {
                    pending: true,
                    ..gates()
                },
                agent
            )
            .is_none()
        );
        assert!(
            decision(
                &candidate("todo", "agent"),
                "todo",
                handoff,
                Gates {
                    suppress_active_self_assignment: true,
                    ..gates()
                },
                agent
            )
            .is_some()
        );
        assert!(
            decision(
                &candidate("todo", "agent"),
                "todo",
                changed,
                Gates {
                    pending: true,
                    ..gates()
                },
                agent
            )
            .is_some()
        );
    }
}
