-- CreateManualQuickCreateRetryTask (pkg/db/queries/agent.sql). A human retry of
-- an issue-less quick-create is a new direct_human run that keeps the original
-- context JSON (including source_context_id), starts a fresh session and records
-- rerun_of_task_id. The caller moves the pending issue_source_context to the new
-- row in this same transaction, so a double submit cannot mint two live attempts.
INSERT INTO agent_task_queue (
    agent_id, runtime_id, status, priority, context,
    force_fresh_session, is_leader_task, squad_id,
    originator_user_id, accountable_user_id,
    runtime_mcp_overlay, runtime_connected_apps,
    originator_source, rerun_of_task_id, id
)
SELECT
    p.agent_id, p.runtime_id, 'queued', p.priority, p.context,
    TRUE, p.is_leader_task, p.squad_id,
    $1, $1,
    $3::jsonb, $4::jsonb,
    'direct_human', p.id, $2
FROM agent_task_queue p
WHERE p.id = $5
  AND p.status = 'failed'
  AND p.issue_id IS NULL
  AND p.chat_session_id IS NULL
  AND p.autopilot_run_id IS NULL
  AND lock_task_owner_rows(p.agent_id, p.issue_id, p.runtime_id)
RETURNING *;
