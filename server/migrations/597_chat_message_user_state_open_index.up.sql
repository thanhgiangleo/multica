-- The hot read: "outstanding replies in this session" — hit by the session
-- badge, the chat header chip and the needs-action list. Partial, so the index
-- holds only open items instead of the whole decided history, and matches the
-- writer's WHERE clause exactly (flagged set, handled unset).
CREATE INDEX CONCURRENTLY idx_chat_message_user_state_open
    ON chat_message_user_state (user_id, chat_session_id)
    WHERE flagged_at IS NOT NULL AND handled_at IS NULL;
