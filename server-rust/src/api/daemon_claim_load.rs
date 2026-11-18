//! Read-only loaders used to build a daemon claim payload.
//!
//! Every body here is a verbatim port of the Go sqlc query the claim path calls,
//! so a payload can never be assembled from a query with different semantics
//! than the server it replaces. Reads that Go treats as best-effort (workspace
//! catalog, owner profile, joined wakeups) return `Option`/empty instead of an
//! error, because Go logs those and keeps the claim.
use super::daemon_claim_pure::{
    ClaimComment, CommentEditFacts, IssueStateSnapshot, decode_issue_state_snapshot,
};
use chrono::{DateTime, Utc};
use deadpool_postgres::GenericClient;
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

/// `GetIssue` is read through this so the caller can distinguish "no row"
/// (which cancels the task) from a transient failure (which only fails it).
pub(super) struct IssueClaimRow {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub title: String,
    pub description: String,
    pub status: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<Uuid>,
    /// The workflow brief resolves a step handler against the issue's creator,
    /// so the claim needs the same pair the editor reads.
    pub creator_type: Option<String>,
    pub creator_id: Option<Uuid>,
    pub number: i64,
    pub project_id: Option<Uuid>,
}

pub(super) async fn issue(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<IssueClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,workspace_id,title,description,status,assignee_type,assignee_id,creator_type,creator_id,number,project_id FROM issue WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load issue".to_owned())?;
    Ok(row.map(|row| IssueClaimRow {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        title: row.get("title"),
        description: row
            .get::<_, Option<String>>("description")
            .unwrap_or_default(),
        status: row.get("status"),
        assignee_type: row.get("assignee_type"),
        assignee_id: row.get("assignee_id"),
        creator_type: row.get("creator_type"),
        creator_id: row.get("creator_id"),
        // issue.number is int4 (migrations/020_issue_number), so decode it as
        // i32 and widen; reading an INT column straight into i64 panics.
        number: row.get::<_, i32>("number") as i64,
        project_id: row.get("project_id"),
    }))
}

pub(super) struct ChatClaimRow {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub title: String,
    pub session_id: Option<String>,
    pub work_dir: Option<String>,
    pub runtime_id: Option<Uuid>,
    pub is_agent_intro: bool,
    pub creator_id: Option<Uuid>,
    pub project_id: Option<Uuid>,
}

pub(super) async fn chat_session(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<ChatClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,workspace_id,title,session_id,work_dir,runtime_id,is_agent_intro,creator_id,project_id FROM chat_session WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load chat session".to_owned())?;
    Ok(row.map(|row| ChatClaimRow {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        title: row.get("title"),
        session_id: row.get("session_id"),
        work_dir: row.get("work_dir"),
        runtime_id: row.get("runtime_id"),
        is_agent_intro: row.get("is_agent_intro"),
        creator_id: row.get("creator_id"),
        project_id: row.get("project_id"),
    }))
}

pub(super) struct AutopilotRunClaimRow {
    pub id: Uuid,
    pub autopilot_id: Uuid,
    pub trigger_payload: Option<Value>,
}

pub(super) async fn autopilot_run(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<AutopilotRunClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,autopilot_id,trigger_payload FROM autopilot_run WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load autopilot run".to_owned())?;
    Ok(row.map(|row| AutopilotRunClaimRow {
        id: row.get("id"),
        autopilot_id: row.get("autopilot_id"),
        trigger_payload: row.get("trigger_payload"),
    }))
}

pub(super) struct AutopilotClaimRow {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub title: String,
    pub description: String,
    pub source: String,
    pub project_id: Option<Uuid>,
}

pub(super) async fn autopilot(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<AutopilotClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,workspace_id,title,description,source,project_id FROM autopilot WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load autopilot".to_owned())?;
    Ok(row.map(|row| AutopilotClaimRow {
        id: row.get("id"),
        workspace_id: row.get("workspace_id"),
        title: row.get("title"),
        description: row.get("description"),
        source: row.get("source"),
        project_id: row.get("project_id"),
    }))
}

/// `GetWorkspace`: the slug, the issue prefix, the free-text context briefed to
/// the agent, and the workspace-wide repo list that a project without its own
/// repos falls back to. `name` rides along because a workspace whose stored
/// prefix is empty derives its identifier from the name.
pub(super) struct WorkspaceClaimRow {
    pub slug: String,
    pub name: String,
    pub issue_prefix: String,
    pub context: String,
    pub repos: Vec<Value>,
    /// The stored `repos` was present but not a JSON array. Go logs this and
    /// degrades to no repos rather than failing the claim.
    pub repos_corrupt: bool,
}

pub(super) async fn workspace(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<WorkspaceClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT slug,name,issue_prefix,context,repos FROM workspace WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load workspace".to_owned())?;
    Ok(row.map(|row| {
        let raw: Option<Value> = row.get("repos");
        let repos_corrupt = raw
            .as_ref()
            .is_some_and(|value| !value.is_null() && !value.is_array());
        WorkspaceClaimRow {
            slug: row.get("slug"),
            name: row.get("name"),
            issue_prefix: row
                .get::<_, Option<String>>("issue_prefix")
                .unwrap_or_default(),
            context: row.get::<_, Option<String>>("context").unwrap_or_default(),
            repos: decode_repos(raw),
            repos_corrupt,
        }
    }))
}

/// `ws.repos` is stored as JSONB; Go unmarshals it into `[]RepoData` and treats
/// an absent, null or non-array value as no repos.
fn decode_repos(raw: Option<Value>) -> Vec<Value> {
    raw.and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| entry["url"].as_str().is_some_and(|url| !url.is_empty()))
        .map(|entry| {
            let mut out = serde_json::json!({ "url": entry["url"] });
            if let Some(description) = entry["description"].as_str().filter(|v| !v.is_empty()) {
                out["description"] = serde_json::json!(description);
            }
            if let Some(reference) = entry["ref"].as_str().filter(|v| !v.is_empty()) {
                out["ref"] = serde_json::json!(reference);
            }
            out
        })
        .collect()
}

/// `projectResourcesForClaim`: the resource list for the wire plus the repos a
/// `github_repo` resource contributes, so the agent also gets a checkout target.
pub(super) struct ProjectClaimResources {
    pub resources: Vec<Value>,
    pub repos: Vec<Value>,
}

/// `ListProjectResourcesInWorkspace` rows in both claim shapes.
pub(super) async fn project_resources(
    db: &impl GenericClient,
    project: Uuid,
    workspace: Uuid,
) -> Result<ProjectClaimResources, String> {
    let rows = db
        .query(
            "SELECT id,resource_type,resource_ref,label FROM project_resource WHERE project_id=$1 AND workspace_id=$2 ORDER BY position,created_at",
            &[&project, &workspace],
        )
        .await
        .map_err(|_| "failed to load project resources".to_owned())?;
    let mut resources = Vec::with_capacity(rows.len());
    let mut repos = Vec::new();
    for row in rows {
        let resource_type: String = row.get("resource_type");
        let reference: Option<Value> = row.get("resource_ref");
        let label: Option<String> = row.get("label");
        let mut out = serde_json::json!({
            "id": row.get::<_, Uuid>("id"),
            "resource_type": resource_type,
            "resource_ref": reference.clone().unwrap_or_else(|| serde_json::json!({})),
        });
        if let Some(label) = label.filter(|label| !label.is_empty()) {
            out["label"] = serde_json::json!(label);
        }
        resources.push(out);
        if resource_type == "github_repo"
            && let Some(reference) = reference
            && let Some(url) = reference["url"].as_str()
            && !url.is_empty()
        {
            let mut repo = serde_json::json!({ "url": url });
            let reference_name = reference["ref"].as_str().unwrap_or("").trim();
            if !reference_name.is_empty() {
                repo["ref"] = serde_json::json!(reference_name);
            }
            repos.push(repo);
        }
    }
    Ok(ProjectClaimResources { resources, repos })
}

/// `ListEnabledAgentMcpServers` — only bound AND enabled servers reach a claim,
/// so a library entry nobody added stays inert.
pub(super) async fn enabled_agent_mcp_servers(
    db: &impl GenericClient,
    agent: Uuid,
) -> Result<Vec<(String, Value)>, String> {
    let rows = db
        .query(
            "SELECT s.name,s.config FROM workspace_mcp_server s JOIN agent_mcp_server b ON b.server_id=s.id WHERE b.agent_id=$1 AND b.enabled AND s.enabled ORDER BY s.name",
            &[&agent],
        )
        .await
        .map_err(|_| "failed to load agent mcp servers".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("name"), row.get("config")))
        .collect())
}

/// `ListIssueStatusEntries` with `include_archived = false`, in the catalog's
/// canonical order. Returns key/name/wire-category/description tuples.
pub(super) async fn issue_statuses(
    db: &impl GenericClient,
    workspace: Uuid,
) -> Result<Vec<(String, String, String, String, bool)>, String> {
    let rows = db
        .query(
            "SELECT key,name,category,description,is_system FROM issue_status WHERE workspace_id=$1 AND archived_at IS NULL ORDER BY CASE category WHEN 'unstarted' THEN 0 WHEN 'started' THEN 1 WHEN 'done' THEN 2 WHEN 'closed' THEN 3 ELSE 4 END, position, CASE WHEN is_system THEN 0 ELSE 1 END, CASE key WHEN 'backlog' THEN 0 WHEN 'todo' THEN 1 WHEN 'in_progress' THEN 2 WHEN 'in_review' THEN 3 WHEN 'blocked' THEN 4 WHEN 'done' THEN 5 WHEN 'cancelled' THEN 6 ELSE 7 END, key",
            &[&workspace],
        )
        .await
        .map_err(|_| "failed to load issue statuses".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>("key"),
                row.get::<_, String>("name"),
                row.get::<_, String>("category"),
                row.get::<_, String>("description"),
                row.get::<_, bool>("is_system"),
            )
        })
        .collect())
}

/// `ListAgentSkills` joined through `agent_skill`, enabled only, by skill name.
pub(super) struct AgentSkillRow {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub content: String,
    pub config: Option<Value>,
}

pub(super) async fn agent_skills(
    db: &impl GenericClient,
    agent: Uuid,
) -> Result<Vec<AgentSkillRow>, String> {
    let rows = db
        .query(
            "SELECT s.id,s.name,s.description,s.content,s.config FROM skill s JOIN agent_skill ask ON ask.skill_id=s.id WHERE ask.agent_id=$1 AND ask.enabled = TRUE ORDER BY s.name ASC",
            &[&agent],
        )
        .await
        .map_err(|_| "failed to load agent skills".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| AgentSkillRow {
            id: row.get("id"),
            name: row.get("name"),
            description: row.get("description"),
            content: row.get("content"),
            config: row.get("config"),
        })
        .collect())
}

pub(super) struct SkillFileRow {
    pub skill_id: Uuid,
    pub path: String,
    pub content: String,
}

/// `ListSkillFilesBySkillIDs` — one batched read, ordered by skill then path so
/// a skill's files reach the daemon in the manifest's own order.
pub(super) async fn skill_files(
    db: &impl GenericClient,
    skill_ids: &[Uuid],
) -> Result<Vec<SkillFileRow>, String> {
    if skill_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = db
        .query(
            "SELECT skill_id,path,content FROM skill_file WHERE skill_id = ANY($1::uuid[]) ORDER BY skill_id, path ASC",
            &[&skill_ids],
        )
        .await
        .map_err(|_| "failed to load skill files".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| SkillFileRow {
            skill_id: row.get("skill_id"),
            path: row.get("path"),
            content: row.get("content"),
        })
        .collect())
}

/// `GetCommentInWorkspace`, plus the edit facts the claim needs for its notices.
pub(super) struct CommentClaimRow {
    pub comment: ClaimComment,
    pub parent_id: Option<Uuid>,
    pub deleted: bool,
    pub edit: CommentEditFacts,
}

pub(super) async fn comment(
    db: &impl GenericClient,
    id: Uuid,
    workspace: Uuid,
) -> Result<Option<CommentClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,content,parent_id,author_type,author_id,created_at,updated_at,revision,resolved_at,deleted_at FROM comment WHERE id=$1 AND workspace_id=$2",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| "failed to load comment".to_owned())?;
    let Some(row) = row else {
        return Ok(None);
    };
    let author_type: String = row.get("author_type");
    let author_id: Option<Uuid> = row.get("author_id");
    let author_name = author_name(db, &author_type, author_id).await;
    let created_at: DateTime<Utc> = row.get("created_at");
    let thread_id: Uuid = match row.get::<_, Option<Uuid>>("parent_id") {
        Some(_) => match thread_root(db, id).await {
            Some(root) => root,
            None => return Ok(None),
        },
        None => id,
    };
    let edit = CommentEditFacts {
        created_at: Some(created_at),
        updated_at: row.get("updated_at"),
        revision: row.get::<_, i64>("revision"),
        resolved_at: row.get("resolved_at"),
    };
    Ok(Some(CommentClaimRow {
        comment: ClaimComment {
            id,
            thread_id,
            content: row.get("content"),
            author_type,
            author_name,
            // `CoalescedCommentData.CreatedAt` is `timestampToString`, i.e.
            // Go's `time.RFC3339`, which never carries fractional seconds.
            created_at: super::daemon_claim_pure::rfc3339_secs(created_at),
            created_at_value: Some(created_at),
            revision: edit.revision,
            updated_at: edit.updated_at,
            resolved_at: edit.resolved_at,
        },
        parent_id: row.get("parent_id"),
        deleted: row.get::<_, Option<DateTime<Utc>>>("deleted_at").is_some(),
        edit,
    }))
}

/// `GetCommentThreadRootID`, used only when a parent exists.
pub(super) async fn thread_root(db: &impl GenericClient, comment: Uuid) -> Option<Uuid> {
    db.query_opt(
        "SELECT comment_thread_root_id($1::uuid)::uuid AS id",
        &[&comment],
    )
    .await
    .ok()
    .flatten()
    .map(|row| row.get("id"))
}

/// Author display name by actor type. A lookup failure leaves the name empty
/// rather than failing the claim — the daemon prints an unnamed author.
async fn author_name(
    db: &impl GenericClient,
    author_type: &str,
    author_id: Option<Uuid>,
) -> String {
    let Some(author_id) = author_id else {
        return String::new();
    };
    let (sql, column) = match author_type {
        "agent" => ("SELECT name FROM agent WHERE id=$1", "name"),
        "member" => ("SELECT name FROM \"user\" WHERE id=$1", "name"),
        _ => return String::new(),
    };
    db.query_opt(sql, &[&author_id])
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<_, String>(column))
        .unwrap_or_default()
}

/// `CountNewCommentsSince`: comments created after the resumed run, excluding
/// the anchor itself and the agent's own comments.
pub(super) async fn count_new_comments_since(
    db: &impl GenericClient,
    issue: Uuid,
    workspace: Uuid,
    since: DateTime<Utc>,
    anchor: Uuid,
    agent: Uuid,
) -> Result<i64, String> {
    let row = db
        .query_one(
            "SELECT count(*) FROM comment WHERE issue_id=$1 AND workspace_id=$2 AND created_at > $3 AND id <> $4 AND deleted_at IS NULL AND NOT (author_type='agent' AND author_id=$5)",
            &[&issue, &workspace, &since, &anchor, &agent],
        )
        .await
        .map_err(|_| "failed to count new comments".to_owned())?;
    Ok(row.get(0))
}

/// The most recent terminal task for (agent, issue) plus what the two deltas
/// need from it. Kept byte-for-byte in step with the Go resume query, including
/// the poison text guards, so a resource cannot be resumed here but not there.
pub(super) struct LastTaskSession {
    pub session_id: String,
    pub work_dir: Option<String>,
    pub runtime_id: Option<Uuid>,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub snapshot: Option<IssueStateSnapshot>,
}

const LAST_TASK_SESSION: &str = r#"WITH retired_sessions AS (
    SELECT DISTINCT r.retired_session_id AS session_id
    FROM agent_task_queue r
    WHERE r.agent_id = $1 AND r.issue_id = $2
      AND COALESCE(r.context->>'type', '') <> 'triage'
      AND r.retired_session_id IS NOT NULL
), resume_overflow_at AS (
    SELECT MAX(COALESCE(t.completed_at, t.started_at, t.dispatched_at, t.created_at)) AS at
    FROM agent_task_queue t
    WHERE t.agent_id = $1 AND t.issue_id = $2
      AND COALESCE(t.context->>'type', '') <> 'triage'
      AND t.status = 'failed'
      AND (
        COALESCE(t.failure_reason, '') = 'codex_resume_oversized'
        OR (COALESCE(t.error, '') ILIKE '%thread/resume failed%' AND COALESCE(t.error, '') ILIKE '%token too long%')
      )
), latest_per_session AS (
    SELECT DISTINCT ON (t.session_id)
        t.session_id, t.work_dir, t.runtime_id, t.status, t.failure_reason, t.error,
        t.started_at, t.issue_snapshot,
        COALESCE(t.completed_at, t.started_at, t.dispatched_at, t.created_at) AS terminal_at
    FROM agent_task_queue t
    WHERE t.agent_id = $1 AND t.issue_id = $2
      AND COALESCE(t.context->>'type', '') <> 'triage'
      AND t.session_id IS NOT NULL
      AND t.status IN ('completed', 'failed', 'cancelled')
    ORDER BY t.session_id, COALESCE(t.completed_at, t.started_at, t.dispatched_at, t.created_at) DESC
)
SELECT session_id, work_dir, runtime_id, status, started_at, issue_snapshot FROM latest_per_session
WHERE session_id NOT IN (SELECT session_id FROM retired_sessions)
  AND (
    status IN ('completed', 'cancelled')
    OR (
      status = 'failed'
      AND COALESCE(failure_reason, '') NOT IN ('iteration_limit', 'agent_fallback_message', 'api_invalid_request', 'codex_semantic_inactivity', 'agent_error.context_overflow', 'codex_resume_oversized', 'opencode_clock_skew')
      AND NOT (COALESCE(error, '') ILIKE '%400%' AND COALESCE(error, '') ILIKE '%invalid_request_error%')
      AND NOT (COALESCE(error, '') ILIKE '%image dimensions exceed max allowed size%' AND COALESCE(error, '') ILIKE '%image.source.base64.data%')
      AND NOT (COALESCE(error, '') ILIKE '%could not resolve authentication method%')
      AND NOT (COALESCE(error, '') ~* 'must not be empty|must be non-?empty|must have non-?empty|non-?empty content|cannot be empty|should not be empty'
               AND COALESCE(error, '') ~* 'role[^a-z0-9]{0,2}assistant|assistant message|message at position|messages\.[0-9]|messages\[[0-9]')
      AND NOT (COALESCE(error, '') ILIKE '%invalid conversation history%')
    )
  )
  AND (
    (SELECT at FROM resume_overflow_at) IS NULL
    OR terminal_at > (SELECT at FROM resume_overflow_at)
  )
ORDER BY terminal_at DESC
LIMIT 1"#;

pub(super) async fn last_task_session(
    db: &impl GenericClient,
    agent: Uuid,
    issue: Uuid,
) -> Result<Option<LastTaskSession>, String> {
    let row = db
        .query_opt(LAST_TASK_SESSION, &[&agent, &issue])
        .await
        .map_err(|_| "failed to load prior session".to_owned())?;
    Ok(row.map(|row| LastTaskSession {
        session_id: row.get("session_id"),
        work_dir: row.get("work_dir"),
        runtime_id: row.get("runtime_id"),
        status: row.get("status"),
        started_at: row.get("started_at"),
        snapshot: decode_issue_state_snapshot(
            row.get::<_, Option<Vec<u8>>>("issue_snapshot").as_deref(),
        ),
    }))
}

/// `GetLatestTaskRolloutMissing`: whether the newest terminal run withheld its
/// session, which is what makes a fallback to an older session a continuity gap
/// the agent must be told about.
pub(super) async fn latest_task_rollout_missing(
    db: &impl GenericClient,
    agent: Uuid,
    issue: Uuid,
) -> Result<bool, String> {
    let row = db
        .query_opt(
            "SELECT COALESCE(session_rollout_missing,FALSE) FROM agent_task_queue WHERE agent_id=$1 AND issue_id=$2 AND COALESCE(context->>'type','') <> 'triage' AND status IN ('completed','failed') AND started_at IS NOT NULL ORDER BY COALESCE(completed_at,started_at,dispatched_at,created_at) DESC LIMIT 1",
            &[&agent, &issue],
        )
        .await
        .map_err(|_| "failed to load rollout state".to_owned())?;
    Ok(row.map(|row| row.get::<_, bool>(0)).unwrap_or(false))
}

/// Chat counterpart of the resume query. Selection is per-session and scoped by
/// `channel_context_revision`, so a channel turn resumes only the generation it
/// was enqueued against. `cancelled` is resumable: a session id on a cancelled
/// row means the provider really did emit that transcript.
pub(super) struct LastChatTaskSession {
    pub session_id: String,
    pub work_dir: Option<String>,
    pub runtime_id: Option<Uuid>,
}

const LAST_CHAT_TASK_SESSION: &str = r#"WITH retired_sessions AS (
    SELECT DISTINCT r.retired_session_id AS session_id
    FROM agent_task_queue r
    WHERE r.chat_session_id = $1
      AND ($2::bigint IS NULL OR COALESCE(r.channel_context_revision, 1) = $2::bigint)
      AND r.retired_session_id IS NOT NULL
), resume_overflow_at AS (
    SELECT MAX(t.completed_at) AS at
    FROM agent_task_queue t
    WHERE t.chat_session_id = $1
      AND ($2::bigint IS NULL OR COALESCE(t.channel_context_revision, 1) = $2::bigint)
      AND t.status = 'failed'
      AND (
        COALESCE(t.failure_reason, '') = 'codex_resume_oversized'
        OR (COALESCE(t.error, '') ILIKE '%thread/resume failed%' AND COALESCE(t.error, '') ILIKE '%token too long%')
      )
), latest_per_session AS (
    SELECT DISTINCT ON (t.session_id)
        t.session_id, t.work_dir, t.runtime_id, t.status, t.failure_reason, t.error, t.completed_at
    FROM agent_task_queue t
    WHERE t.chat_session_id = $1
      AND ($2::bigint IS NULL OR COALESCE(t.channel_context_revision, 1) = $2::bigint)
      AND t.session_id IS NOT NULL
      AND t.status IN ('completed', 'failed', 'cancelled')
    ORDER BY t.session_id, t.completed_at DESC
)
SELECT session_id, work_dir, runtime_id FROM latest_per_session
WHERE session_id NOT IN (SELECT session_id FROM retired_sessions)
  AND (
    status IN ('completed', 'cancelled')
    OR (
      status = 'failed'
      AND COALESCE(failure_reason, '') NOT IN ('iteration_limit', 'agent_fallback_message', 'api_invalid_request', 'codex_semantic_inactivity', 'agent_error.context_overflow', 'codex_resume_oversized', 'opencode_clock_skew')
      AND NOT (COALESCE(error, '') ILIKE '%400%' AND COALESCE(error, '') ILIKE '%invalid_request_error%')
      AND NOT (COALESCE(error, '') ILIKE '%image dimensions exceed max allowed size%' AND COALESCE(error, '') ILIKE '%image.source.base64.data%')
      AND NOT (COALESCE(error, '') ILIKE '%could not resolve authentication method%')
      AND NOT (COALESCE(error, '') ~* 'must not be empty|must be non-?empty|must have non-?empty|non-?empty content|cannot be empty|should not be empty'
               AND COALESCE(error, '') ~* 'role[^a-z0-9]{0,2}assistant|assistant message|message at position|messages\.[0-9]|messages\[[0-9]')
      AND NOT (COALESCE(error, '') ILIKE '%invalid conversation history%')
    )
  )
  AND (
    (SELECT at FROM resume_overflow_at) IS NULL
    OR completed_at > (SELECT at FROM resume_overflow_at)
  )
ORDER BY completed_at DESC
LIMIT 1"#;

pub(super) async fn last_chat_task_session(
    db: &impl GenericClient,
    chat: Uuid,
    channel_context_revision: Option<i64>,
) -> Result<Option<LastChatTaskSession>, String> {
    let row = db
        .query_opt(LAST_CHAT_TASK_SESSION, &[&chat, &channel_context_revision])
        .await
        .map_err(|_| "failed to load prior chat session".to_owned())?;
    Ok(row.map(|row| LastChatTaskSession {
        session_id: row.get("session_id"),
        work_dir: row.get("work_dir"),
        runtime_id: row.get("runtime_id"),
    }))
}

pub(super) async fn latest_chat_rollout_missing(
    db: &impl GenericClient,
    chat: Uuid,
    channel_context_revision: Option<i64>,
) -> Result<bool, String> {
    let row = db
        .query_opt(
            "SELECT COALESCE(session_rollout_missing,FALSE) FROM agent_task_queue WHERE chat_session_id=$1 AND ($2::bigint IS NULL OR COALESCE(channel_context_revision,1)=$2::bigint) AND status IN ('completed','failed') AND started_at IS NOT NULL ORDER BY COALESCE(completed_at,started_at,dispatched_at,created_at) DESC LIMIT 1",
            &[&chat, &channel_context_revision],
        )
        .await
        .map_err(|_| "failed to load chat rollout state".to_owned())?;
    Ok(row.map(|row| row.get::<_, bool>(0)).unwrap_or(false))
}

/// `IsTaskSessionRetired`: the exclusion the exact-source rerun path must apply
/// itself, because it bypasses the (agent, issue) resume lookup.
pub(super) async fn session_retired(
    db: &impl GenericClient,
    agent: Uuid,
    session_id: &str,
) -> Result<bool, String> {
    let row = db
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM agent_task_queue r WHERE r.agent_id=$1 AND r.retired_session_id=$2::text) AS retired",
            &[&agent, &session_id],
        )
        .await
        .map_err(|_| "failed to load retired session".to_owned())?;
    Ok(row.get("retired"))
}

pub(super) async fn chat_session_has_user_message(
    db: &impl GenericClient,
    chat: Uuid,
) -> Result<bool, String> {
    let row = db
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM chat_message WHERE chat_session_id=$1 AND role='user') AS has_user_message",
            &[&chat],
        )
        .await
        .map_err(|_| "failed to load chat messages".to_owned())?;
    Ok(row.get("has_user_message"))
}

pub(super) struct ChannelDelivery {
    pub channel_type: String,
    pub chat_type: String,
    pub channel_thread_id: Option<String>,
    pub channel_message_id: Option<String>,
    pub delivers_files: bool,
}

/// `GetChannelTaskDelivery`, projected to the fields the claim exposes.
///
/// `file_delivery` is the deployment capability, decided once at wiring time:
/// Go declares it per adapter only where object storage exists, and one `if`
/// decides both the outbound hop and the promise made to the agent. A channel
/// that does not carry files is therefore the default, so a false negative
/// costs the agent a described file while a false positive has it claim an
/// attachment nobody can open.
pub(super) async fn channel_task_delivery(
    db: &impl GenericClient,
    task: Uuid,
    file_delivery: &(dyn Fn(&str) -> bool + Sync),
) -> Result<Option<ChannelDelivery>, String> {
    let row = db
        .query_opt(
            "SELECT channel_type,chat_type,channel_thread_id,channel_message_id FROM channel_task_delivery WHERE task_id=$1",
            &[&task],
        )
        .await
        .map_err(|_| "failed to load channel task delivery".to_owned())?;
    Ok(row.map(|row| {
        let channel_type: String = row.get("channel_type");
        ChannelDelivery {
            delivers_files: file_delivery(&channel_type),
            channel_type,
            chat_type: row.get("chat_type"),
            channel_thread_id: row.get("channel_thread_id"),
            channel_message_id: row.get("channel_message_id"),
        }
    }))
}

/// One user message of the task's sealed input batch.
pub(super) struct InputMessage {
    pub id: Uuid,
    pub content: String,
    pub task_id: Option<Uuid>,
    pub role: String,
    pub channel_media_pending_until: Option<DateTime<Utc>>,
}

pub(super) fn trailing_input_messages(
    messages: Vec<InputMessage>,
    now: DateTime<Utc>,
) -> Vec<InputMessage> {
    let start = messages
        .iter()
        .rposition(|m| m.role != "user")
        .map_or(0, |i| i + 1);
    let mut tail = messages.into_iter().skip(start).collect::<Vec<_>>();
    if let Some(cut) = tail.iter().position(|m| {
        m.channel_media_pending_until
            .is_some_and(|until| until > now)
    }) {
        tail.truncate(cut);
    }
    tail
}

/// `ListChatInputMessages` for a task-owned batch. Never absorbs a message that
/// arrived after the batch was sealed.
pub(super) async fn chat_input_messages(
    db: &impl GenericClient,
    input_task: Uuid,
) -> Result<Vec<InputMessage>, String> {
    let rows = db
        .query(
            "SELECT id,content,task_id,role,channel_media_pending_until FROM chat_message WHERE task_id=$1 AND role='user' ORDER BY created_at ASC, id ASC",
            &[&input_task],
        )
        .await
        .map_err(|_| "failed to load chat input".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| InputMessage {
            id: row.get("id"),
            content: row.get("content"),
            task_id: row.get("task_id"),
            role: row.get("role"),
            channel_media_pending_until: row.get("channel_media_pending_until"),
        })
        .collect())
}

/// `ListChatMessagesForLegacyTask`: trailing history for a legacy or reclaimed
/// task, but never a newer queued successor bound to its own user message.
pub(super) async fn legacy_chat_messages(
    db: &impl GenericClient,
    chat: Uuid,
) -> Result<Vec<InputMessage>, String> {
    let rows = db
        .query(
            r#"SELECT message.id,message.content,message.task_id,message.role,message.channel_media_pending_until FROM chat_message AS message
WHERE message.chat_session_id = $1
  AND NOT (
    message.role = 'user'
    AND EXISTS (
      SELECT 1
      FROM agent_task_queue AS task
      WHERE task.chat_session_id = message.chat_session_id
        AND task.status = 'queued'
        AND task.id = message.task_id
        AND task.id <> (
          SELECT head.id
          FROM agent_task_queue AS head
          WHERE head.chat_session_id = $1
            AND head.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
            AND head.regenerate_quick_actions_for IS NULL
          ORDER BY
            CASE
              WHEN head.status IN ('dispatched', 'running', 'waiting_local_directory') THEN 0
              WHEN head.status = 'deferred' THEN 1
              ELSE 2
            END,
            head.priority DESC,
            head.created_at ASC,
            head.id ASC
          LIMIT 1
        )
    )
  )
ORDER BY message.created_at ASC, message.id ASC"#,
            &[&chat],
        )
        .await
        .map_err(|_| "failed to load chat messages".to_owned())?;
    Ok(rows
        .into_iter()
        .map(|row| InputMessage {
            id: row.get("id"),
            content: row.get("content"),
            task_id: row.get("task_id"),
            role: row.get("role"),
            channel_media_pending_until: row.get("channel_media_pending_until"),
        })
        .collect())
}

/// `ListAttachmentsByChatMessageIDs`, ordered by creation so the daemon receives
/// attachments in the same order as the messages that carried them.
pub(super) async fn chat_attachments(
    db: &impl GenericClient,
    message_ids: &[Uuid],
    workspace: Uuid,
) -> Result<Vec<(Uuid, Value)>, String> {
    if message_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = db
        .query(
            "SELECT id,filename,content_type,chat_message_id FROM attachment WHERE chat_message_id = ANY($1::uuid[]) AND workspace_id = $2 ORDER BY created_at ASC",
            &[&message_ids, &workspace],
        )
        .await
        .map_err(|_| "failed to load chat attachments".to_owned())?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get("chat_message_id"),
                attachment_wire(row.get("id"), row.get("filename"), row.get("content_type")),
            )
        })
        .collect())
}

/// `ChatAttachmentMeta`: exactly the three fields the daemon prints in the
/// prompt. Size and storage keys never reach the wire.
pub(super) fn attachment_wire(id: Uuid, filename: String, content_type: String) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("id".into(), serde_json::json!(id));
    out.insert("filename".into(), serde_json::json!(filename));
    if !content_type.is_empty() {
        out.insert("content_type".into(), serde_json::json!(content_type));
    }
    Value::Object(out)
}

/// `GetIssueSourceContextByID` for a quick-create task's captured branch point.
pub(super) struct SourceContext {
    pub state: String,
    pub origin_task_id: Uuid,
    pub snapshot: Value,
}

pub(super) async fn issue_source_context(
    db: &impl GenericClient,
    workspace: Uuid,
    id: Uuid,
) -> Result<Option<SourceContext>, String> {
    let row = db
        .query_opt(
            "SELECT state,origin_task_id,snapshot FROM issue_source_context WHERE workspace_id=$1 AND id=$2",
            &[&workspace, &id],
        )
        .await
        .map_err(|_| "failed to load captured source context".to_owned())?;
    Ok(row.map(|row| SourceContext {
        state: row.get("state"),
        origin_task_id: row.get("origin_task_id"),
        snapshot: row.get("snapshot"),
    }))
}

/// `GetSquadInWorkspace` plus its leader, for the leader briefing and the
/// squad name on a quick-create task.
pub(super) struct SquadClaimRow {
    pub id: Uuid,
    pub name: String,
    pub leader_id: Option<Uuid>,
    pub instructions: String,
}

pub(super) async fn squad(
    db: &impl GenericClient,
    id: Uuid,
    workspace: Uuid,
) -> Result<Option<SquadClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,name,leader_id,instructions FROM squad WHERE id=$1 AND workspace_id=$2",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| "failed to load squad".to_owned())?;
    Ok(row.map(|row| SquadClaimRow {
        id: row.get("id"),
        name: row.get("name"),
        leader_id: row.get("leader_id"),
        instructions: row.get("instructions"),
    }))
}

/// The runtime owner's display profile, for the "## Requesting User" section.
pub(super) async fn user_profile(
    db: &impl GenericClient,
    user: Uuid,
) -> Result<Option<(String, String)>, String> {
    let row = db
        .query_opt(
            "SELECT name,profile_description FROM \"user\" WHERE id=$1",
            &[&user],
        )
        .await
        .map_err(|_| "failed to load user".to_owned())?;
    Ok(row.map(|row| (row.get("name"), row.get("profile_description"))))
}

/// Metadata of an existing task row, for the exact-source rerun path.
pub(super) struct PriorTaskRow {
    pub agent_id: Uuid,
    pub issue_id: Option<Uuid>,
    pub chat_session_id: Option<Uuid>,
    pub session_id: Option<String>,
    pub work_dir: Option<String>,
    pub runtime_id: Option<Uuid>,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub failure_reason: Option<String>,
    pub error: Option<String>,
    pub issue_snapshot: Option<Vec<u8>>,
    pub session_rollout_missing: bool,
    pub context: Option<Value>,
}

pub(super) async fn prior_task(
    db: &impl GenericClient,
    id: Uuid,
) -> Result<Option<PriorTaskRow>, String> {
    let row = db
        .query_opt(
            "SELECT agent_id,issue_id,chat_session_id,session_id,work_dir,runtime_id,status,started_at,failure_reason,error,issue_snapshot,session_rollout_missing,context FROM agent_task_queue WHERE id=$1",
            &[&id],
        )
        .await
        .map_err(|_| "failed to load prior task".to_owned())?;
    Ok(row.map(|row| PriorTaskRow {
        agent_id: row.get("agent_id"),
        issue_id: row.get("issue_id"),
        chat_session_id: row.get("chat_session_id"),
        session_id: row.get("session_id"),
        work_dir: row.get("work_dir"),
        runtime_id: row.get("runtime_id"),
        status: row.get("status"),
        started_at: row.get("started_at"),
        failure_reason: row.get("failure_reason"),
        error: row.get("error"),
        issue_snapshot: row.get("issue_snapshot"),
        session_rollout_missing: row.get("session_rollout_missing"),
        context: row.get("context"),
    }))
}

/// `GetProjectInWorkspace`: the soft project reference a claim resolves, plus
/// the two fields the payload copies onto the response.
pub(super) struct ProjectClaimRow {
    pub id: Uuid,
    pub title: String,
    pub description: String,
}

pub(super) async fn project(
    db: &impl GenericClient,
    id: Uuid,
    workspace: Uuid,
) -> Result<Option<ProjectClaimRow>, String> {
    let row = db
        .query_opt(
            "SELECT id,title,description FROM project WHERE id=$1 AND workspace_id=$2",
            &[&id, &workspace],
        )
        .await
        .map_err(|_| "failed to load project".to_owned())?;
    Ok(row.map(|row| ProjectClaimRow {
        id: row.get("id"),
        title: row.get("title"),
        description: row
            .get::<_, Option<String>>("description")
            .unwrap_or_default(),
    }))
}

/// Go `claimProjectContext`: what a claim says about the issue's project.
/// Assigned wholesale so a claim can never carry a project's title without its
/// resources, and `resources` stays `None` when the project has none — Go only
/// overwrites the response field when the slice is non-empty.
#[derive(Default)]
pub(super) struct ClaimProjectContext {
    pub project_id: String,
    pub title: String,
    pub description: String,
    pub resources: Vec<Value>,
    pub repos: Vec<Value>,
}

/// Go `resolveClaimProjectContext`.
///
/// Both reads are workspace-scoped, because `project_resource` carries its own
/// workspace id and a corrupt project reference must not lift another tenant's
/// repository URLs into a prompt.
///
/// A read failure is NOT "no project": the caller turns it into a 500 so the
/// task is preserved for redelivery. Collapsing a transient error into the
/// workspace-repo fallback is what would let one blip run an agent against the
/// wrong repository. A project that resolves to no row IS "no project" — the
/// reference is stale, deleted, or foreign, and the claim degrades to workspace
/// context.
///
/// Repo precedence: a project's own `github_repo` resources override workspace
/// repos. Mixing both would just confuse the agent.
pub(super) async fn resolve_claim_project_context(
    db: &impl GenericClient,
    project_id: Option<Uuid>,
    workspace_id: Uuid,
) -> Result<ClaimProjectContext, String> {
    let mut out = ClaimProjectContext::default();
    if let Some(project_id) = project_id {
        match project(db, project_id, workspace_id).await {
            Ok(Some(project)) => {
                out.project_id = project.id.to_string();
                out.title = project.title;
                out.description = project.description;
                let rows = project_resources(db, project.id, workspace_id).await?;
                out.resources = rows.resources;
                out.repos = rows.repos;
            }
            Ok(None) => {}
            Err(_) => return Err("get project".into()),
        }
    }
    if !out.repos.is_empty() {
        return Ok(out);
    }
    let Some(workspace) = workspace(db, workspace_id)
        .await
        .map_err(|_| "get workspace".to_owned())?
    else {
        return Err("get workspace".into());
    };
    // Corrupt stored JSON is not transient: failing the claim would wedge every
    // claim in this workspace until somebody repairs the row. Degrade to no
    // repos and leave a trail instead.
    if workspace.repos_corrupt {
        eprintln!(
            "claim project context: workspace {workspace_id} repos are not valid JSON; claiming without repos"
        );
    }
    out.repos = workspace.repos;
    Ok(out)
}

/// Go `repairStaleCommentPlanIfNeeded`: a comment-backed task whose plan was
/// built from comments that have since been removed cannot be dispatched — the
/// run would be asked to answer text that no longer exists. Repairing by
/// cancelling also retriggers the survivors that were queued behind it, so the
/// obligation is carried on rather than dropped.
///
/// Returns `Some(failure)` when the task must NOT be dispatched: a clean repair
/// also returns `Some` with `settled = true`, because the caller answers the
/// daemon with `null` either way.
pub(super) async fn repair_stale_comment_plan_if_needed(
    state: &super::ApiState,
    task: &Row,
    runtime_workspace_id: Uuid,
) -> Option<super::daemon_claim_build::ClaimBuildFailure> {
    let planned: Vec<Uuid> = task
        .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
        .unwrap_or_default();
    if planned.is_empty() {
        return None;
    }
    let trigger: Option<Uuid> = task.get("trigger_comment_id");
    let db = match state.pool.get().await {
        Ok(db) => db,
        Err(_) => return Some(stale_plan_failure("failed to repair stale comment task")),
    };
    if let Some(trigger) = trigger {
        // A trigger deleted while it had replies stays as a tombstone instead of
        // clearing trigger_comment_id; repair it like a removed one.
        match comment(&db, trigger, runtime_workspace_id).await {
            // A tombstone (or a row this workspace cannot see) is treated as
            // removed; a live comment means the plan is still valid.
            Ok(Some(row)) if !row.deleted => return None,
            Ok(_) => {}
            // A read failure is not "removed": leave the plan alone rather than
            // cancel a task on a transient error.
            Err(_) => return None,
        }
    }
    let Some(issue_id) = task.get::<_, Option<Uuid>>("issue_id") else {
        return Some(stale_plan_failure("comment task has no issue"));
    };
    let issue = match issue(&db, issue_id).await {
        Ok(Some(issue)) => issue,
        Ok(None) | Err(_) => {
            return Some(stale_plan_failure("failed to repair stale comment task"));
        }
    };
    let task_id: Uuid = task.get("id");
    if issue.workspace_id != runtime_workspace_id {
        if super::daemon_claim_service::cancel_task(state, task_id, None)
            .await
            .is_err()
        {
            eprintln!("task claim: cancel stale cross-workspace task {task_id} failed");
        }
        return Some(stale_plan_failure("task workspace isolation check failed"));
    }
    let Ok(cancelled) = super::daemon_claim_service::cancel_task(state, task_id, None).await else {
        return Some(stale_plan_failure("failed to repair stale comment task"));
    };
    if let Some(cancelled) = cancelled {
        // The claim path passes an invalid excluded id: it cancels for a reason
        // unrelated to one specific input.
        let carried = super::issue_write_comment::CommentCancelledTask {
            id: cancelled.get("id"),
            agent_id: cancelled.get("agent_id"),
            trigger_comment_id: cancelled.get("trigger_comment_id"),
            coalesced_comment_ids: cancelled
                .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
                .unwrap_or_default(),
        };
        super::issue_comment_runtime::PgCommentRuntime::new(state.clone())
            .retrigger_cancelled_task_survivors(issue.workspace_id, issue_id, &[carried], None)
            .await;
    }
    // A clean repair has no failure to report: the caller answers `null`.
    Some(super::daemon_claim_build::ClaimBuildFailure {
        outcome: "stale_comment_plan",
        status: axum::http::StatusCode::OK,
        message: String::new(),
        settled: true,
    })
}

fn stale_plan_failure(message: &'static str) -> super::daemon_claim_build::ClaimBuildFailure {
    super::daemon_claim_build::ClaimBuildFailure::unsettled(
        "error_stale_comment_plan",
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        message,
    )
}

/// `maxClaimCommentPayloadBytes`: 512 KiB of comment input per claim.
pub(super) const MAX_CLAIM_COMMENT_PAYLOAD_BYTES: usize = 512 << 10;

/// Go `buildCoalescedCommentData`: the comments a run must answer, in the order
/// the daemon should read them.
///
/// Every read is workspace-scoped so a foreign comment UUID resolves to
/// "missing" (skipped) rather than leaking another tenant's text into a prompt.
/// A tombstone is as missing as a removed row: there is no body left to
/// deliver. A thread-root lookup failure skips the entry rather than sending a
/// reply the daemon cannot thread.
pub(super) async fn build_coalesced_comment_data(
    db: &impl GenericClient,
    workspace: Uuid,
    ids: &[Uuid],
) -> Vec<ClaimComment> {
    if ids.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<ClaimComment> = Vec::with_capacity(ids.len());
    let mut seen: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
    for id in ids {
        if !seen.insert(*id) {
            continue;
        }
        let Ok(Some(row)) = comment(db, *id, workspace).await else {
            continue;
        };
        if row.deleted {
            continue;
        }
        out.push(row.comment);
    }
    if out.is_empty() {
        return Vec::new();
    }
    out.sort_by(
        |left, right| match (left.created_at_value, right.created_at_value) {
            (Some(left_at), Some(right_at)) if left_at != right_at => left_at.cmp(&right_at),
            _ => left
                .created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id)),
        },
    );
    out
}

/// Go `commentDataIDs`: the ids of the comments actually delivered.
pub(super) fn comment_data_ids(comments: &[ClaimComment]) -> Vec<Uuid> {
    comments.iter().map(|comment| comment.id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The search-path builders are the only pure part of this module; their SQL
    // text is pinned in the module constants and verified by the disposable-DB
    // claim test. These assertions guard the two wire projections instead.
    #[test]
    fn legacy_chat_uses_only_unanswered_visible_user_messages() {
        let now = Utc::now();
        let make = |role: &str, pending| InputMessage {
            id: Uuid::now_v7(),
            content: role.into(),
            task_id: None,
            role: role.into(),
            channel_media_pending_until: pending,
        };
        let messages = vec![
            make("user", None),
            make("assistant", None),
            make("user", None),
            make("user", Some(now + chrono::Duration::minutes(5))),
            make("user", None),
        ];
        let selected = trailing_input_messages(messages, now);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].role, "user");
        assert!(trailing_input_messages(vec![make("assistant", None)], now).is_empty());
    }

    #[test]
    fn attachment_wire_keeps_only_go_meta_fields() {
        // Built through the projection the loader uses, so a stray key cannot
        // appear on the daemon wire without failing here.
        let value = attachment_wire(Uuid::nil(), "a.txt".into(), "text/plain".into());
        assert_eq!(
            value,
            serde_json::json!({
                "id": Uuid::nil(),
                "filename": "a.txt",
                "content_type": "text/plain",
            })
        );
        // `content_type` is omitempty in Go, and an attachment row is free to
        // carry an empty string.
        let bare = attachment_wire(Uuid::nil(), "b.bin".into(), String::new());
        assert_eq!(bare.get("content_type"), None);
        assert_eq!(bare.get("size_bytes"), None);
        assert_eq!(bare.get("url"), None);
    }
}
