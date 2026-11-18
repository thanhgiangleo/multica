-- Backfill zbs_has_guest_history for sessions that already hold guest content.
-- Guests exist only on owner-visible channel sessions, so the scan is limited
-- to Telegram and Zalo sessions (served by the partial indexes from 002/003)
-- and each probe uses an existing chat_session_id index. Runs in its own file
-- so it takes only row locks on matching sessions, never the table lock 035
-- needs. Idempotent: only false rows are rewritten.
UPDATE chat_session AS cs SET zbs_has_guest_history = true
WHERE cs.zbs_has_guest_history = false
  AND cs.zbs_source_channel_type IN ('telegram', 'zalo')
  -- Group sessions keep their existing readers; see MarkChatSessionGuestHistory.
  AND cs.zbs_source_channel_chat_type IS DISTINCT FROM 'group'
  AND (
    EXISTS (
      SELECT 1 FROM chat_message AS m
      JOIN "user" AS sender ON sender.id = m.zbs_sender_user_id
      WHERE m.chat_session_id = cs.id AND sender.zbs_is_shadow = true
    )
    OR EXISTS (
      SELECT 1 FROM channel_chat_context_generation AS generation
      WHERE generation.chat_session_id = cs.id AND generation.zbs_guest_shadow_user_id IS NOT NULL
    )
    OR EXISTS (
      SELECT 1 FROM attachment AS a
      WHERE a.chat_session_id = cs.id AND a.uploader_type = 'guest'
    )
  );
