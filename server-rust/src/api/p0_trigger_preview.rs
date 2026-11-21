//! Read-only comment trigger preview, matching Go's composer response.
use super::{
    ApiError, ApiState, GoJson, Identity, WorkspaceSelectors,
    actor::Actor,
    catalog::{WorkspaceId, parse_params},
    comment_dispatch::DispatchStatus,
    comment_routing::{
        self, CommentAgentTrigger, CommentMentionTarget, IssueFacts, RoutingOptions, TriggerSource,
    },
    comment_routing_db::DbRouting,
    error,
    issue_read::load_issue,
    issue_write::write_actor,
};
use axum::{
    Router,
    extract::{Extension, Path, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::post,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub(super) fn routes(state: ApiState) -> Router<ApiState> {
    Router::new()
        .route("/api/issues/{id}/comments/trigger-preview", post(preview))
        .route_layer(middleware::from_fn_with_state(state, workspace_guard))
}

// The existing catalog workspace guard is private to that module. Apply its
// selector/membership rules to this one independently registered route.
async fn workspace_guard(
    State(state): State<ApiState>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Extension(selectors): Extension<WorkspaceSelectors>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let params = if actor.workspace().is_some() {
        Default::default()
    } else {
        parse_params(request.uri().query())
    };
    let slug = params
        .get("workspace_slug")
        .filter(|s| !s.is_empty())
        .map(String::as_str)
        .or_else(|| selectors.slug.as_deref().filter(|s| !s.is_empty()));
    let id = params
        .get("workspace_id")
        .filter(|s| !s.is_empty())
        .map(String::as_str)
        .or_else(|| selectors.id.as_deref().filter(|s| !s.is_empty()));
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?;
    let workspace = if let Some(slug) = slug {
        db.query_opt("SELECT id FROM workspace WHERE slug = $1", &[&slug])
            .await
            .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
            .map(|row| row.get::<_, Uuid>(0))
            .ok_or_else(|| error(StatusCode::NOT_FOUND, "workspace not found"))?
    } else {
        let raw = id.ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "workspace_id or workspace_slug is required",
            )
        })?;
        Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid workspace_id"))?
    };
    if db
        .query_opt(
            "SELECT 1 FROM member WHERE workspace_id = $1 AND user_id = $2",
            &[&workspace, &user],
        )
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "workspace not found"))?
        .is_none()
    {
        return Err(error(StatusCode::NOT_FOUND, "workspace not found"));
    }
    drop(db);
    request.extensions_mut().insert(WorkspaceId(workspace));
    Ok(next.run(request).await)
}

#[derive(Deserialize)]
struct PreviewRequest {
    #[serde(default)]
    content: Option<String>,
    parent_id: Option<String>,
    editing_comment_id: Option<String>,
}

#[derive(Serialize)]
struct PreviewAgent {
    id: Uuid,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    avatar_url: Option<String>,
    source: &'static str,
    reason: &'static str,
}

#[derive(Serialize)]
struct PreviewResponse {
    agents: Vec<PreviewAgent>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    blocked: Vec<super::issue_write_comment::CommentTriggerOutcome>,
}

fn parse_request(body: &[u8]) -> Result<PreviewRequest, ApiError> {
    let mut req: PreviewRequest = serde_json::from_slice(body)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "invalid request body"))?;
    if let Some(content) = req.content.as_mut() {
        content.retain(|ch| ch != '\0');
    }
    Ok(req)
}

/// Go decodes a missing or null `content` into the empty string.
fn content(req: &PreviewRequest) -> &str {
    req.content.as_deref().unwrap_or_default()
}

fn parsed_id(raw: &str, field: &'static str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| error(StatusCode::BAD_REQUEST, format!("invalid {field}")))
}

fn reason(source: TriggerSource) -> &'static str {
    match source {
        TriggerSource::IssueAssignee => "Current issue assignment will trigger this agent.",
        TriggerSource::MentionAgent => "This agent was mentioned in the comment.",
        TriggerSource::MentionSquadLeader => "A mentioned squad will trigger its leader.",
        TriggerSource::ThreadParent => "This reply will trigger the parent comment's author.",
        TriggerSource::ConversationContinuation => {
            "This follow-up will continue the recent agent conversation."
        }
    }
}

fn blocked(
    targets: &[CommentMentionTarget],
) -> Vec<super::issue_write_comment::CommentTriggerOutcome> {
    targets
        .iter()
        .filter(|target| target.status == Some(DispatchStatus::Blocked))
        .map(|target| super::issue_write_comment::CommentTriggerOutcome {
            target_type: target.target_type.into(),
            target_id: target.target_id.clone(),
            status: "blocked".into(),
            reason_code: target
                .reason
                .map(|r| r.as_str())
                .unwrap_or("internal_error")
                .into(),
        })
        .collect()
}

async fn preview(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    Path(raw): Path<String>,
    body: axum::body::Bytes,
) -> Result<GoJson<PreviewResponse>, ApiError> {
    let db = state
        .pool
        .get()
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let issue = load_issue(&db, ws, &raw).await?;
    let issue_id: Uuid = issue.get("id");
    let req = parse_request(&body)?;
    let editing = if let Some(raw) = req.editing_comment_id.as_deref() {
        let id = parsed_id(raw, "editing_comment_id")?;
        Some(
            db.query_opt(
                "SELECT id, issue_id, parent_id FROM comment WHERE id=$1 AND workspace_id=$2",
                &[&id, &ws],
            )
            .await
            .ok()
            .flatten()
            .filter(|row| row.get::<_, Uuid>("issue_id") == issue_id)
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid editing comment"))?,
        )
    } else {
        None
    };
    let parent_id = if let Some(raw) = req.parent_id.as_deref() {
        let parent = parsed_id(raw, "parent_id")?;
        if editing
            .as_ref()
            .is_some_and(|row| row.get::<_, Option<Uuid>>("parent_id") != Some(parent))
        {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "parent_id does not match editing comment",
            ));
        }
        Some(parent)
    } else {
        editing.as_ref().and_then(|row| row.get("parent_id"))
    };
    let parent = if let Some(id) = parent_id {
        Some(db.query_opt("SELECT id,issue_id,author_type,author_id,deleted_at,source_task_id,content FROM comment WHERE id=$1 AND workspace_id=$2", &[&id, &ws])
            .await.ok().flatten().filter(|row| row.get::<_, Uuid>("issue_id") == issue_id)
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "invalid parent comment"))?)
    } else {
        None
    };
    let mut response = PreviewResponse {
        agents: Vec::new(),
        blocked: Vec::new(),
    };
    let content = content(&req);
    if content.is_empty() {
        return Ok(GoJson(response));
    }
    let (actor_type, actor_id, _) = write_actor(actor.clone(), user)?;
    let task_id = match actor {
        Actor::Task(task) => Some(task.task),
        _ => None,
    };
    let originator = if actor_type == "member" {
        Some(actor_id)
    } else if let Some(task) = task_id {
        db.query_opt(
            "SELECT status, originator_user_id FROM agent_task_queue WHERE id=$1",
            &[&task],
        )
        .await
        .ok()
        .flatten()
        .and_then(|row| {
            (!matches!(
                row.get::<_, String>("status").as_str(),
                "completed" | "failed" | "cancelled"
            ))
            .then(|| row.get::<_, Option<Uuid>>("originator_user_id"))
            .flatten()
        })
    } else {
        None
    };
    let row = db.query_one("SELECT id,workspace_id,assignee_type,assignee_id,triage_state FROM issue WHERE id=$1 AND workspace_id=$2", &[&issue_id, &ws])
        .await.map_err(|_| error(StatusCode::NOT_FOUND, "issue not found"))?;
    let facts = IssueFacts {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        assignee_type: row.get("assignee_type"),
        assignee_id: row.get("assignee_id"),
        triage_state: row.get("triage_state"),
    };
    let parent = parent.as_ref().map(|row| comment_routing::CommentFacts {
        id: row.get("id"),
        author_type: row.get("author_type"),
        author_id: row.get("author_id"),
        deleted: row
            .get::<_, Option<chrono::DateTime<chrono::Utc>>>("deleted_at")
            .is_some(),
        source_task_id: row.get("source_task_id"),
        content: row.get("content"),
    });
    let excluded = editing.as_ref().map(|row| row.get::<_, Uuid>("id"));
    drop(db);
    let plan = comment_routing::compute_comment_agent_triggers(
        &DbRouting::new(
            Arc::clone(&state.pool),
            ws,
            issue_id,
            excluded.or(parent_id),
            excluded,
        ),
        &facts,
        content,
        parent.as_ref(),
        actor_type,
        actor_id,
        RoutingOptions {
            thread_comment_id: excluded.or(parent_id),
            exclude_trigger_comment_id: excluded,
            authoring_task_id: task_id,
            originator_user_id: originator,
        },
    )
    .await;
    response.blocked = blocked(&plan.targets);
    for CommentAgentTrigger {
        agent_id, source, ..
    } in plan.triggers
    {
        // A resolved trigger is already authorized; the lookup here is only for
        // its display fields. A vanished row is omitted, as in Go's lookup.
        let db = state
            .pool
            .get()
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load agent"))?;
        let row = db
            .query_opt(
                "SELECT name, avatar_url FROM agent WHERE id=$1 AND workspace_id=$2",
                &[&agent_id, &ws],
            )
            .await
            .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "failed to load agent"))?;
        if let Some(row) = row {
            let avatar: Option<String> = row.get("avatar_url");
            response.agents.push(PreviewAgent {
                id: agent_id,
                name: row.get("name"),
                avatar_url: avatar.filter(|s| !s.is_empty()).and_then(|s| {
                    state
                        .timeline_avatar_urls
                        .as_ref()
                        .and_then(|policy| policy.resolve(&s))
                        .or(Some(s))
                }),
                source: source.as_str(),
                reason: reason(source),
            });
        }
    }
    Ok(GoJson(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn payload_sanitization_and_go_shape() {
        assert_eq!(
            content(&parse_request(br#"{"content":"a\u0000b"}"#).unwrap()),
            "ab"
        );
        assert_eq!(content(&parse_request(b"{}").unwrap()), "");
        assert_eq!(content(&parse_request(br#"{"content":null}"#).unwrap()), "");
        assert_eq!(
            parse_request(br#"{"content":true}"#).err().unwrap().0,
            StatusCode::BAD_REQUEST
        );
        let empty = PreviewResponse {
            agents: vec![],
            blocked: vec![],
        };
        assert_eq!(json!(empty), json!({"agents":[]}));
        for source in [
            TriggerSource::IssueAssignee,
            TriggerSource::MentionAgent,
            TriggerSource::MentionSquadLeader,
            TriggerSource::ThreadParent,
            TriggerSource::ConversationContinuation,
        ] {
            assert!(!reason(source).is_empty());
        }
    }
    #[test]
    fn uuid_errors_match_go() {
        assert_eq!(
            parsed_id("not-uuid", "parent_id").unwrap_err().1,
            "invalid parent_id"
        );
        assert_eq!(
            parsed_id("not-uuid", "editing_comment_id").unwrap_err().1,
            "invalid editing_comment_id"
        );
    }
}
