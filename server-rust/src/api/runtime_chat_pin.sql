-- Frozen Go AdvanceCancelledChatSessionPointer. Caller holds chat_session then task.
UPDATE chat_session cs
SET session_id = t.session_id,
    runtime_id = t.runtime_id,
    work_dir   = COALESCE(t.work_dir, cs.work_dir),
    updated_at = now()
FROM agent_task_queue t
WHERE t.id = $1
  AND t.chat_session_id = cs.id
  AND t.status = 'cancelled'
  AND t.session_id IS NOT NULL
  AND t.runtime_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM agent_task_queue newer
      WHERE newer.chat_session_id = t.chat_session_id
        AND newer.id <> t.id
        AND newer.session_id IS NOT NULL
        AND newer.created_at > t.created_at
  );
