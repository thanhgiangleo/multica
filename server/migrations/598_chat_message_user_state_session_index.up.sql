-- Per-session hydration of decided rows when a transcript is fetched (the
-- reader needs to see which replies are already marked). Separate from the
-- partial open index because it must also cover handled rows.
CREATE INDEX CONCURRENTLY idx_chat_message_user_state_session
    ON chat_message_user_state (user_id, chat_session_id);
