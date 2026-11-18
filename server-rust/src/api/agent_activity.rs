//! Workspace agent aggregates from the frozen 30-day task queries.
use super::{Actor, ApiError, ApiState, GoJson, Identity, catalog::WorkspaceId, error, timestamp};
use axum::{
    Router,
    extract::{Extension, RawQuery, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashSet;
use tokio_postgres::Client;
use uuid::Uuid;

pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/api/agent-run-counts", get(run_counts))
        .route("/api/agent-activity-30d", get(activity))
        .route("/api/working-agents", get(working_agents))
}

#[derive(Serialize)]
struct RunCount {
    agent_id: Uuid,
    run_count: i32,
}

#[derive(Serialize)]
struct Bucket {
    agent_id: Uuid,
    bucket_at: String,
    task_count: i32,
    failed_count: i32,
    completed_count: i32,
    cancelled_count: i32,
    duration_ms: f64,
    duration_count: i32,
}

// ListAllAgents contains only user-kind agents, including archived agents.
// A task actor is an agent principal in Go and may see all workspace agents;
// human/cloud callers are subject to the member visibility predicate.
pub(super) async fn allowed_agents(
    db: &Client,
    ws: Uuid,
    user: Uuid,
    actor: &Actor,
) -> Result<Vec<Uuid>, ApiError> {
    let fail = || {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to resolve agent access",
        )
    };
    let role: String = db
        .query_one(
            "SELECT role FROM member WHERE workspace_id=$1 AND user_id=$2",
            &[&ws, &user],
        )
        .await
        .map_err(|_| fail())?
        .get(0);
    let unrestricted = matches!(actor, Actor::Task(_)) || role == "owner" || role == "admin";
    let rows = db
        .query(
            "SELECT a.id FROM agent a WHERE a.workspace_id=$1 AND a.kind='user' AND (\
         $3::bool OR a.owner_id=$2 OR (a.permission_mode='public_to' AND EXISTS (\
         SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id AND (\
         t.target_type='workspace' OR (t.target_type='member' AND t.target_id=$2)))))",
            &[&ws, &user, &unrestricted],
        )
        .await
        .map_err(|_| fail())?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

const RUN_COUNTS: &str = "SELECT atq.agent_id, COUNT(*)::int AS run_count \
FROM agent_task_queue atq JOIN agent a ON a.id=atq.agent_id \
WHERE a.workspace_id=$1 AND atq.created_at > now() - INTERVAL '30 days' GROUP BY atq.agent_id";

const ACTIVITY: &str = "SELECT atq.agent_id, DATE_TRUNC('day', atq.completed_at)::timestamptz AS bucket, \
COUNT(*)::int AS task_count, COUNT(*) FILTER (WHERE atq.status='failed')::int AS failed_count, \
COUNT(*) FILTER (WHERE atq.status='completed')::int AS completed_count, \
COUNT(*) FILTER (WHERE atq.status='cancelled')::int AS cancelled_count, \
COALESCE(SUM(EXTRACT(EPOCH FROM (atq.completed_at-atq.started_at))*1000) \
FILTER (WHERE atq.completed_at > atq.started_at),0)::float8 AS duration_ms, \
COUNT(*) FILTER (WHERE atq.completed_at > atq.started_at)::int AS duration_count \
FROM agent_task_queue atq JOIN agent a ON a.id=atq.agent_id \
WHERE a.workspace_id=$1 AND atq.completed_at IS NOT NULL \
AND atq.completed_at > now() - INTERVAL '30 days' GROUP BY atq.agent_id,bucket ORDER BY atq.agent_id,bucket";

async fn run_counts(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Vec<RunCount>>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get agent run counts",
        )
    })?;
    let rows = db.query(RUN_COUNTS, &[&ws]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get agent run counts",
        )
    })?;
    let allowed = allowed_agents(&db, ws, user, &actor).await?;
    Ok(GoJson(
        rows.iter()
            .filter(|r| allowed.contains(&r.get::<_, Uuid>("agent_id")))
            .map(|r| RunCount {
                agent_id: r.get("agent_id"),
                run_count: r.get("run_count"),
            })
            .collect(),
    ))
}

async fn activity(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
) -> Result<GoJson<Vec<Bucket>>, ApiError> {
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get agent activity",
        )
    })?;
    let rows = db.query(ACTIVITY, &[&ws]).await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get agent activity",
        )
    })?;
    let allowed = allowed_agents(&db, ws, user, &actor).await?;
    Ok(GoJson(
        rows.iter()
            .filter(|r| allowed.contains(&r.get::<_, Uuid>("agent_id")))
            .map(|r| Bucket {
                agent_id: r.get("agent_id"),
                bucket_at: timestamp(r.get::<_, DateTime<Utc>>("bucket")),
                task_count: r.get("task_count"),
                failed_count: r.get("failed_count"),
                completed_count: r.get("completed_count"),
                cancelled_count: r.get("cancelled_count"),
                duration_ms: r.get("duration_ms"),
                duration_count: r.get("duration_count"),
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct WorkingAgent {
    id: Uuid,
    name: String,
    avatar_url: Option<String>,
    running_task_count: i32,
    issue_ids: Vec<Uuid>,
}

// Frozen agent.sql ListWorkspaceWorkingAgents, with sqlc names replaced by
// positional parameters in work_type, mine_relation, member_id, parent_issue_id order.
const WORKING_AGENTS: &str = r#"SELECT
  a.id,
  a.name,
  a.avatar_url,
  COUNT(*)::int AS running_task_count,
  COALESCE(
    ARRAY_AGG(DISTINCT atq.issue_id ORDER BY atq.issue_id)
      FILTER (WHERE atq.issue_id IS NOT NULL),
    ARRAY[]::uuid[]
  )::uuid[] AS issue_ids
FROM agent a
JOIN agent_task_queue atq ON atq.agent_id = a.id
WHERE a.workspace_id = $1
  AND a.kind = 'user'
  AND a.archived_at IS NULL
  AND atq.status = 'running'
  AND (
    $2::text = ''
    OR ($2::text = 'chat' AND atq.chat_session_id IS NOT NULL)
    OR (
      $2::text = 'autopilot'
      AND atq.chat_session_id IS NULL
      AND atq.autopilot_run_id IS NOT NULL
    )
    OR (
      $2::text = 'issue'
      AND atq.chat_session_id IS NULL
      AND atq.autopilot_run_id IS NULL
      AND atq.issue_id IS NOT NULL
    )
  )
  AND (
    $3::text = ''
    OR EXISTS (
      SELECT 1
      FROM issue i
      WHERE i.id = atq.issue_id
        AND i.workspace_id = a.workspace_id
        AND (
          (
            $3::text IN ('assigned', 'any')
            AND i.assignee_type = 'member'
            AND i.assignee_id = $4::uuid
          )
          OR (
            $3::text IN ('created', 'any')
            AND i.creator_type = 'member'
            AND i.creator_id = $4::uuid
          )
          OR (
            $3::text IN ('involved', 'any')
            AND (
              (
                i.assignee_type = 'agent'
                AND EXISTS (
                  SELECT 1
                  FROM agent owned_agent
                  WHERE owned_agent.id = i.assignee_id
                    AND owned_agent.workspace_id = a.workspace_id
                    AND owned_agent.owner_id = $4::uuid
                )
              )
              OR (
                i.assignee_type = 'squad'
                AND EXISTS (
                  SELECT 1
                  FROM squad s
                  WHERE s.id = i.assignee_id
                    AND s.workspace_id = a.workspace_id
                    AND (
                      EXISTS (
                        SELECT 1
                        FROM squad_member sm
                        WHERE sm.squad_id = s.id
                          AND sm.member_type = 'member'
                          AND sm.member_id = $4::uuid
                      )
                      OR EXISTS (
                        SELECT 1
                        FROM agent leader
                        WHERE leader.id = s.leader_id
                          AND leader.workspace_id = a.workspace_id
                          AND leader.owner_id = $4::uuid
                      )
                      OR EXISTS (
                        SELECT 1
                        FROM squad_member sm
                        JOIN agent owned_member ON owned_member.id = sm.member_id
                        WHERE sm.squad_id = s.id
                          AND sm.member_type = 'agent'
                          AND owned_member.workspace_id = a.workspace_id
                          AND owned_member.owner_id = $4::uuid
                      )
                    )
                )
              )
            )
          )
        )
    )
  )
  AND (
    $5::uuid IS NULL
    OR EXISTS (
      SELECT 1
      FROM issue child
      WHERE child.id = atq.issue_id
        AND child.workspace_id = a.workspace_id
        AND child.parent_issue_id = $5::uuid
    )
  )
GROUP BY a.id, a.name, a.avatar_url, a.created_at
ORDER BY a.created_at ASC"#;

async fn working_agents(
    State(state): State<ApiState>,
    Extension(WorkspaceId(ws)): Extension<WorkspaceId>,
    Extension(Identity(user)): Extension<Identity>,
    Extension(actor): Extension<Actor>,
    RawQuery(raw): RawQuery,
) -> Result<GoJson<Vec<WorkingAgent>>, ApiError> {
    let params = super::catalog::parse_params(raw.as_deref());
    let work_type = params.get("type").map_or("", |s| s.trim());
    if !matches!(work_type, "" | "issue" | "autopilot" | "chat") {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid type: must be issue, autopilot, or chat",
        ));
    }
    let scope = params.get("scope").map_or("", |s| s.trim());
    let mut relation = params.get("relation").map_or("", |s| s.trim());
    match scope {
        "" if !relation.is_empty() => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "relation requires scope=mine",
            ));
        }
        "" => (),
        "mine" if work_type != "issue" => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "scope=mine requires type=issue",
            ));
        }
        "mine" => {
            if relation.is_empty() {
                relation = "any";
            }
            if !matches!(relation, "assigned" | "created" | "involved" | "any") {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "invalid relation: must be assigned, created, involved, or any",
                ));
            }
        }
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "invalid scope: must be mine",
            ));
        }
    }
    let parent = params
        .get("parent")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    if parent.is_some() && work_type != "issue" {
        return Err(error(StatusCode::BAD_REQUEST, "parent requires type=issue"));
    }
    if parent.is_some() && !scope.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "parent cannot be combined with scope",
        ));
    }
    let parent = parent
        .map(|p| Uuid::parse_str(p).map_err(|_| error(StatusCode::BAD_REQUEST, "invalid parent")))
        .transpose()?;
    let member: Option<Uuid> = (scope == "mine").then_some(user);
    let db = state.pool.get().await.map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to list workspace working agents",
        )
    })?;
    let rows = db
        .query(
            WORKING_AGENTS,
            &[&ws, &work_type, &relation, &member, &parent],
        )
        .await
        .map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to list workspace working agents",
            )
        })?;
    let allowed: HashSet<_> = allowed_agents(&db, ws, user, &actor)
        .await?
        .into_iter()
        .collect();
    Ok(GoJson(
        rows.into_iter()
            .filter(|r| allowed.contains(&r.get::<_, Uuid>("id")))
            .map(|r| WorkingAgent {
                id: r.get("id"),
                name: r.get("name"),
                avatar_url: r.get("avatar_url"),
                running_task_count: r.get("running_task_count"),
                issue_ids: r.get("issue_ids"),
            })
            .collect(),
    ))
}
