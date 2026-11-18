ALTER TABLE chat_message
    DROP COLUMN IF EXISTS zbs_sender_display_name,
    DROP COLUMN IF EXISTS zbs_sender_external_id,
    DROP COLUMN IF EXISTS zbs_sender_user_id;

ALTER TABLE chat_session
    DROP COLUMN IF EXISTS zbs_source_channel_chat_type,
    DROP COLUMN IF EXISTS zbs_source_channel_type;
