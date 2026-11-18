-- Durable Telegram provenance keeps owner-visible audit history available even
-- after an installation is revoked or its route binding is reclaimed.
-- Sender attribution is intentionally soft-linked: repository policy forbids
-- new foreign keys, and historical/external identities must remain renderable
-- after membership changes.
ALTER TABLE chat_session
    ADD COLUMN zbs_source_channel_type TEXT,
    ADD COLUMN zbs_source_channel_chat_type TEXT;

ALTER TABLE chat_message
    ADD COLUMN zbs_sender_user_id UUID,
    ADD COLUMN zbs_sender_external_id TEXT,
    ADD COLUMN zbs_sender_display_name TEXT;

-- Best-effort backfill while the channel route history is still present.
UPDATE chat_session AS session
SET zbs_source_channel_type = binding.channel_type,
    zbs_source_channel_chat_type = binding.chat_type
FROM channel_chat_session_binding AS binding
WHERE binding.chat_session_id = session.id
  AND session.zbs_source_channel_type IS NULL;

-- Private historical sessions had exactly one human creator. Group sender
-- identity was not persisted before this migration and must remain unknown.
UPDATE chat_message AS message
SET zbs_sender_user_id = session.creator_id
FROM chat_session AS session
WHERE message.chat_session_id = session.id
  AND message.role = 'user'
  AND message.channel_ingested
  AND session.zbs_source_channel_chat_type = 'p2p'
  AND message.zbs_sender_user_id IS NULL;
