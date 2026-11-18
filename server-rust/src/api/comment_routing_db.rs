//! PostgreSQL-backed [`CommentRoutingLookup`].
//!
//! This is the read half of Go's `computeCommentAgentTriggers`: it supplies the
//! facts the pure decision tree branches on, with the same predicates Go uses.
//! Every gap is deliberate and fail-closed — a failed read answers `None` (the
//! target does not exist) or `Err(())` (the caller must not report success),
//! never a permissive default.
//!
//! The lookup is bound to one issue and one comment thread, because the dedup
//! queries are thread- and head-scoped: the same agent may hold a pending run in
//! another thread of the same issue without satisfying this comment.

use super::agent_invoke::{self, InvokeTarget, Invoker};
use super::agent_ready::{self, AgentVerdict, ReadinessInput};
use super::comment_routing::{
    AgentFacts, CommentFacts, CommentRoutingLookup, RootOwner, SquadFacts, TaskFacts, TaskRole,
};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use std::sync::Arc;
use tokio::sync::OnceCell;
use uuid::Uuid;

/// Facts are read on the pool: routing runs after the comment transaction has
/// committed, so it must observe the committed row and nothing of an aborted
/// attempt.
pub struct DbRouting {
    pool: Arc<Pool>,
    workspace_id: Uuid,
    issue_id: Uuid,
    /// The thread a new comment belongs to. `None` means the comment has no
    /// thread yet, and Go answers "nothing is pending" without querying.
    thread_comment_id: Option<Uuid>,
    /// Set on an edit preview: the comment whose old run this save replaces is
    /// not evidence that the target is already covered.
    exclude_trigger_comment_id: Option<Uuid>,
    head_sha: OnceCell<Option<String>>,
}

impl DbRouting {
    pub fn new(
        pool: Arc<Pool>,
        workspace_id: Uuid,
        issue_id: Uuid,
        thread_comment_id: Option<Uuid>,
        exclude_trigger_comment_id: Option<Uuid>,
    ) -> Self {
        Self {
            pool,
            workspace_id,
            issue_id,
            thread_comment_id,
            exclude_trigger_comment_id,
            head_sha: OnceCell::new(),
        }
    }

    /// The reviewed HEAD, resolved at most once. It keys both the dedup and the
    /// merge so a PR pushed mid-review invalidates both together; an issue with
    /// no linked PR has no key and dedups on (issue, agent, thread) alone.
    async fn head_sha(&self) -> Option<String> {
        if let Some(cached) = self.head_sha.get() {
            return cached.clone();
        }
        let loaded = self.load_head_sha().await;
        // A concurrent load may win the set; both ran the same query.
        let _ = self.head_sha.set(loaded.clone());
        loaded
    }

    async fn load_head_sha(&self) -> Option<String> {
        let client = self.pool.get().await.ok()?;
        client
            .query_opt(HEAD_SHA_SQL, &[&self.issue_id])
            .await
            .ok()
            .flatten()
            .map(|row| row.get::<_, String>(0))
            .filter(|sha| !sha.is_empty())
    }
}

/// Same union Go reads: GitHub and self-hosted VCS PRs both move the key, and an
/// in-flight PR is preferred over a merged sibling that would shadow it.
const HEAD_SHA_SQL: &str = "\
SELECT head_sha FROM (
    SELECT pr.head_sha AS head_sha, pr.state AS state, pr.pr_updated_at AS pr_updated_at
    FROM github_pull_request pr
    JOIN issue_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1 AND pr.head_sha <> ''
    UNION ALL
    SELECT pr.head_sha AS head_sha, pr.state AS state, pr.pr_updated_at AS pr_updated_at
    FROM vcs_pull_request pr
    JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1 AND pr.head_sha <> ''
) combined
ORDER BY (state IN ('open', 'draft')) DESC, pr_updated_at DESC
LIMIT 1";

/// Pending (pre-claim) work in this thread. `dispatched` counts because a task
/// that has been handed to a daemon still holds the pending slot; a
/// channel-media `deferred` row counts because it is the issue's sole assigned
/// task and has not been claimable yet.
const HAS_PENDING_SQL: &str = "\
SELECT count(*) > 0 FROM agent_task_queue
WHERE context->>'wakeup_id' IS NULL AND issue_id = $1 AND agent_id = $2
  AND (
    status IN ('queued', 'dispatched')
    OR (status = 'deferred' AND context->>'channel_issue_media_pending' = 'true')
  )
  AND (COALESCE($3::text, '') = '' OR context->>'head_sha' = $3::text)
  AND comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($4::uuid)";

/// Same, ignoring the comment the caller is editing.
const HAS_PENDING_EXCLUDING_SQL: &str = "\
SELECT count(*) > 0 FROM agent_task_queue
WHERE context->>'wakeup_id' IS NULL AND issue_id = $1 AND agent_id = $2
  AND (
    status IN ('queued', 'dispatched')
    OR (status = 'deferred' AND context->>'channel_issue_media_pending' = 'true')
  )
  AND trigger_comment_id IS DISTINCT FROM $5::uuid
  AND (COALESCE($3::text, '') = '' OR context->>'head_sha' = $3::text)
  AND comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($4::uuid)";

/// Non-terminal work, used by the self-trigger-suppressed leader decision and by
/// the enqueue ladder's post-merge-miss branch. No head filter: the question is
/// whether a run exists at all, not whether it targets this review round.
pub(super) const HAS_ACTIVE_SQL: &str = "\
SELECT count(*) > 0 FROM agent_task_queue
WHERE issue_id = $1 AND agent_id = $2
  AND (
    status IN ('queued', 'dispatched', 'running', 'waiting_local_directory')
    OR (status = 'deferred' AND context->>'channel_issue_media_pending' = 'true')
  )
  AND comment_thread_id IS NOT DISTINCT FROM comment_thread_root_id($3::uuid)";

const LATEST_ROLE_SQL: &str = "\
SELECT is_leader_task, squad_id FROM agent_task_queue
WHERE issue_id = $1 AND agent_id = $2
ORDER BY created_at DESC
LIMIT 1";

/// Walk `parent_id` to the row whose parent is NULL. Scoped to the workspace on
/// the starting row so a cross-workspace id resolves to nothing.
const THREAD_ROOT_SQL: &str = "\
WITH RECURSIVE root_of AS (
    SELECT c.id, c.parent_id
    FROM comment c
    WHERE c.id = $1 AND c.workspace_id = $2
    UNION ALL
    SELECT p.id, p.parent_id
    FROM comment p
    JOIN root_of r ON p.id = r.parent_id
)
SELECT c.id, c.author_type, c.author_id, c.deleted_at, c.source_task_id, c.content
FROM comment c
WHERE c.id = (SELECT id FROM root_of WHERE parent_id IS NULL LIMIT 1)";

/// Every agent that already owns a run covering this exact root comment,
/// directly or as a folded input. Leader runs sort first for the same agent so a
/// conversation continues in the role that spoke last.
const ROOT_OWNERS_SQL: &str = "\
SELECT DISTINCT ON (agent_id) agent_id, squad_id FROM agent_task_queue t
WHERE t.issue_id = $1 AND t.agent_id IS NOT NULL
  AND (
    t.trigger_comment_id = $2
    OR $2 = ANY(t.coalesced_comment_ids)
    OR EXISTS (
        SELECT 1 FROM task_supplement s
        WHERE s.task_id = t.id AND s.issue_id = t.issue_id
          AND s.comment_id = $2 AND s.status = 'delivered'
    )
  )
ORDER BY agent_id, (squad_id IS NOT NULL) DESC, created_at DESC, id DESC";

#[async_trait]
impl CommentRoutingLookup for DbRouting {
    async fn agent(&self, workspace_id: Uuid, agent_id: Uuid) -> Option<AgentFacts> {
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(
                "SELECT id, name, runtime_id, archived_at, owner_id, permission_mode FROM agent \
                 WHERE id=$1 AND workspace_id=$2 AND kind='user'",
                &[&agent_id, &workspace_id],
            )
            .await
            .ok()??;
        Some(AgentFacts {
            id: row.get("id"),
            name: row.get("name"),
            runtime_id: row.get("runtime_id"),
            archived: row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
                .is_some(),
            owner_id: row.get("owner_id"),
            permission_mode: row.get("permission_mode"),
        })
    }

    /// A squad is loaded without an archived filter; the caller decides whether
    /// an archived squad still deserves an answer.
    async fn squad(&self, workspace_id: Uuid, squad_id: Uuid) -> Option<SquadFacts> {
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(
                "SELECT id, leader_id, archived_at FROM squad WHERE id=$1 AND workspace_id=$2",
                &[&squad_id, &workspace_id],
            )
            .await
            .ok()??;
        Some(SquadFacts {
            id: row.get("id"),
            leader_id: row.get("leader_id"),
            archived: row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("archived_at")
                .is_some(),
        })
    }

    /// Deny on any read failure: a transient error must never widen access.
    async fn can_invoke(
        &self,
        agent: &AgentFacts,
        actor_type: &str,
        actor_id: Uuid,
        originator: Option<Uuid>,
    ) -> bool {
        let Ok(client) = self.pool.get().await else {
            return false;
        };
        agent_invoke::can_invoke(
            &**client,
            self.workspace_id,
            InvokeTarget {
                id: agent.id,
                owner_id: agent.owner_id,
                permission_mode: &agent.permission_mode,
            },
            Invoker {
                actor_type,
                actor_id,
                originator,
            },
        )
        .await
        .unwrap_or(false)
    }

    /// Go returns an error when the runtime row cannot be read, and every caller
    /// then treats the target as *not* blocked rather than inventing a refusal.
    async fn readiness_block(&self, agent: &AgentFacts) -> Option<AgentVerdict> {
        let client = self.pool.get().await.ok()?;
        // Go returns an error when the runtime row cannot be read, and every
        // caller then treats the target as *not* blocked rather than inventing
        // a refusal.
        agent_ready::agent_readiness(
            &**client,
            ReadinessInput {
                archived: agent.archived,
                runtime_id: agent.runtime_id,
                owner_id: agent.owner_id,
            },
        )
        .await
        .ok()
        .filter(|verdict| verdict.blocked_now())
    }

    async fn has_pending(&self, agent_id: Uuid) -> Result<bool, ()> {
        let Some(thread) = self.thread_comment_id else {
            return Ok(false);
        };
        let client = self.pool.get().await.map_err(|_| ())?;
        let head = self.head_sha().await;
        let row = match self.exclude_trigger_comment_id {
            Some(exclude) => {
                client
                    .query_one(
                        HAS_PENDING_EXCLUDING_SQL,
                        &[&self.issue_id, &agent_id, &head, &thread, &exclude],
                    )
                    .await
            }
            None => {
                client
                    .query_one(
                        HAS_PENDING_SQL,
                        &[&self.issue_id, &agent_id, &head, &thread],
                    )
                    .await
            }
        }
        .map_err(|_| ())?;
        Ok(row.get(0))
    }

    async fn has_active(
        &self,
        agent_id: Uuid,
        thread_comment_id: Option<Uuid>,
    ) -> Result<bool, ()> {
        let client = self.pool.get().await.map_err(|_| ())?;
        let row = client
            .query_one(
                HAS_ACTIVE_SQL,
                &[&self.issue_id, &agent_id, &thread_comment_id],
            )
            .await
            .map_err(|_| ())?;
        Ok(row.get(0))
    }

    async fn latest_task_role(&self, agent_id: Uuid) -> Result<TaskRole, ()> {
        let client = self.pool.get().await.map_err(|_| ())?;
        let row = client
            .query_opt(LATEST_ROLE_SQL, &[&self.issue_id, &agent_id])
            .await
            .map_err(|_| ())?
            .ok_or(())?;
        Ok(TaskRole {
            is_leader_task: row.get("is_leader_task"),
            squad_id: row.get("squad_id"),
        })
    }

    async fn task(&self, task_id: Uuid) -> Option<TaskFacts> {
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(
                "SELECT agent_id, issue_id, is_leader_task, squad_id, trigger_comment_id, coalesced_comment_ids \
                 FROM agent_task_queue WHERE id=$1",
                &[&task_id],
            )
            .await
            .ok()??;
        Some(TaskFacts {
            agent_id: row.get("agent_id"),
            issue_id: row.get("issue_id"),
            is_leader_task: row.get("is_leader_task"),
            squad_id: row.get("squad_id"),
            trigger_comment_id: row.get("trigger_comment_id"),
            coalesced_comment_ids: row
                .get::<_, Option<Vec<Uuid>>>("coalesced_comment_ids")
                .unwrap_or_default(),
        })
    }

    async fn thread_root(&self, comment_id: Uuid) -> Option<CommentFacts> {
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(THREAD_ROOT_SQL, &[&comment_id, &self.workspace_id])
            .await
            .ok()??;
        Some(CommentFacts {
            id: row.get("id"),
            author_type: row.get("author_type"),
            author_id: row.get("author_id"),
            deleted: row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>("deleted_at")
                .is_some(),
            source_task_id: row.get("source_task_id"),
            content: row.get("content"),
        })
    }

    async fn conversation_root_owners(&self, root_id: Uuid) -> Result<Vec<RootOwner>, ()> {
        let client = self.pool.get().await.map_err(|_| ())?;
        let rows = client
            .query(ROOT_OWNERS_SQL, &[&self.issue_id, &root_id])
            .await
            .map_err(|_| ())?;
        Ok(rows
            .iter()
            .map(|row| RootOwner {
                agent_id: row.get("agent_id"),
                squad_id: row.get("squad_id"),
            })
            .collect())
    }
}
