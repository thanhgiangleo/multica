WITH head AS MATERIALIZED (
  SELECT candidate.id
  FROM agent_task_queue AS candidate
  WHERE candidate.chat_session_id = $1
    AND candidate.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
    AND candidate.regenerate_quick_actions_for IS NULL
  ORDER BY
    CASE
      WHEN candidate.status IN ('dispatched', 'running', 'waiting_local_directory') THEN 0
      WHEN candidate.status = 'deferred' THEN 1
      ELSE 2
    END,
    candidate.priority DESC,
    candidate.created_at ASC,
    candidate.id ASC
  LIMIT 1
)
UPDATE agent_task_queue AS queued
SET status = 'cancelled', completed_at = now(), prepare_lease_expires_at = NULL,
    cancelled_by_type = 'system', cancelled_by_id = NULL, cancelled_by_name = NULL
WHERE queued.chat_session_id = $1
  AND queued.status = 'queued'
  AND queued.id IS DISTINCT FROM (SELECT id FROM head)
RETURNING queued.*;