-- Go ListChatMessages / ListChatMessagesPage predicates (frozen 2026-10-31).
-- Both queries share the head visibility rule; changes must remain in sync.
-- IMPORTANT: the visible-head selector below is also used by
-- ListChatMessagesForLegacyTask, ListChatMessagesPage,
-- ReanchorClaimedDirectChatInput, ReanchorNextQueuedDirectChatInput,
-- ListPendingChatTasksForSession, and CancelQueuedAgentTasksForSession in
-- agent.sql. Keep the eligible statuses and ordering identical: a claimed task
-- is current, a deferred retry precedes still-queued work, and queued peers use
-- claim priority/FIFO order. Background quick-action regeneration is invisible.
-- This is a presentation/visibility order, not a scheduling guarantee:
-- deferred rows are not claimable before promotion, so a queued row may be
-- claimed during the backoff and then becomes the visible claimed head.
SELECT message.* FROM chat_message AS message
WHERE message.chat_session_id = $1
  AND message.message_kind != 'channel_command'
  AND NOT (
    message.role = 'user'
    AND EXISTS (
      SELECT 1
      FROM agent_task_queue AS task
      WHERE task.chat_session_id = message.chat_session_id
        AND task.status = 'queued'
        AND task.id = message.task_id
        -- "Queued follow-up" is positional, not the row's transient status:
        -- the first pending task is the current turn even before claim.
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
ORDER BY message.created_at ASC, message.id ASC;

-- PAGE
SELECT message.* FROM chat_message AS message
WHERE message.chat_session_id = $1
  AND message.message_kind != 'channel_command'
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
  AND (
    $3::timestamptz IS NULL
    OR (message.created_at, message.id) < ($3::timestamptz, $4::uuid)
  )
ORDER BY message.created_at DESC, message.id DESC
LIMIT $2;
