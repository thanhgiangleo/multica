WITH target AS MATERIALIZED (
  SELECT candidate.id
  FROM agent_task_queue AS candidate
  WHERE candidate.id = $1 AND candidate.chat_session_id = $2
    AND candidate.status = 'queued'
    AND EXISTS (
      SELECT 1 FROM agent_task_queue AS active
      WHERE active.chat_session_id = $2
        AND active.status IN ('dispatched', 'running', 'waiting_local_directory')
        AND active.regenerate_quick_actions_for IS NULL
    )
  FOR UPDATE
), demoted AS (
  UPDATE agent_task_queue AS queued SET priority = 3
  WHERE queued.chat_session_id = $2 AND queued.id <> $1
    AND queued.status = 'queued' AND queued.priority >= 4
    AND EXISTS (SELECT 1 FROM target)
), prioritized AS (
  UPDATE agent_task_queue AS selected SET priority = 4
  FROM target WHERE selected.id = target.id RETURNING selected.id
)
SELECT prioritized.id AS task_id,
  (SELECT active.id FROM agent_task_queue AS active
   WHERE active.chat_session_id = $2
     AND active.status IN ('dispatched', 'running', 'waiting_local_directory')
     AND active.regenerate_quick_actions_for IS NULL
   ORDER BY active.created_at ASC, active.id ASC LIMIT 1)::uuid AS active_task_id
FROM prioritized
