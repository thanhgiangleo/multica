-- Frozen Go StartAgentTaskWithSupplement. $1 task, $2 supplement, $3 cleanup.
WITH candidate AS MATERIALIZED (
    SELECT t.id, t.issue_id, r.workspace_id, r.provider
    FROM agent_task_queue t
    JOIN agent_runtime r ON r.id = t.runtime_id
    WHERE t.id = $1 AND t.status IN ('dispatched', 'waiting_local_directory')
    FOR UPDATE OF t
), capability AS (
    INSERT INTO task_supplement_capability (task_id, workspace_id, issue_id, capability)
    SELECT id, workspace_id, issue_id, 'task-supplement-v1'
    FROM candidate
    WHERE $2::boolean
      AND provider IN ('codex', 'claude', 'grok', 'cursor', 'kiro')
      AND issue_id IS NOT NULL
    ON CONFLICT DO NOTHING
    RETURNING task_id
)
UPDATE agent_task_queue t
SET status = 'running', started_at = now(), wait_reason = NULL,
    prepare_lease_expires_at = NULL,
    result = CASE WHEN $3::boolean AND candidate.provider IN ('pi', 'omp')
                  THEN COALESCE(t.result, '{}'::jsonb) || '{"process_cleanup_capability":"pi-process-cleanup-v1"}'::jsonb
                  ELSE t.result END
FROM candidate
WHERE t.id = candidate.id AND (SELECT count(*) FROM capability) >= 0
RETURNING t.*;
