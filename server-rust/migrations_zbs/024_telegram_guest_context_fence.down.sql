ALTER TABLE channel_chat_context_generation DROP CONSTRAINT IF EXISTS zbs_channel_chat_context_generation_guest_pair_check;
ALTER TABLE channel_chat_context_generation
    DROP COLUMN IF EXISTS zbs_guest_grant_generation,
    DROP COLUMN IF EXISTS zbs_guest_shadow_user_id;
