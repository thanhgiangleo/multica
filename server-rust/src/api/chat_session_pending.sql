-- Returns a claimed task first, then a deferred retry, followed by prioritized
-- then FIFO queued work. See the shared visible-head invariant above
-- ListChatMessages; changing this order requires changing every selector named
-- there in the same patch.
-- The message lateral join reads only the immutable input owned by each task;
-- it avoids loading the session's complete message history just to render a
-- one-line queue preview. GetPendingChatTask remains for legacy callers that
-- only need an existence check.
SELECT
    task.id,
    task.status,
    task.created_at,
    -- Only meaningful while status is waiting_local_directory: the daemon
    -- stamps which directory this task is parked on and who holds it, so the
    -- StatusPill can say why it is not moving. Stale on any other status; the
    -- renderer reads it only for the waiting one.
    task.wait_reason,
    message.id AS message_id,
    COALESCE(message.content, '')::text AS content
FROM agent_task_queue AS task
LEFT JOIN LATERAL (
    SELECT input.id, input.content
    FROM chat_message AS input
    WHERE input.task_id = COALESCE(task.chat_input_task_id, task.id)
      AND input.role = 'user'
    ORDER BY input.created_at ASC, input.id ASC
    LIMIT 1
) AS message ON TRUE
WHERE task.chat_session_id = $1
  AND task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
  AND task.regenerate_quick_actions_for IS NULL
ORDER BY
    CASE
      WHEN task.status IN ('dispatched', 'running', 'waiting_local_directory') THEN 0
      WHEN task.status = 'deferred' THEN 1
      ELSE 2
    END,
    task.priority DESC,
    task.created_at ASC,
    task.id ASC
