-- The debounce recovery path must retain the generation of the last
-- authenticated guest sender, not infer it from a later re-pair.
ALTER TABLE channel_chat_context_generation
    ADD COLUMN IF NOT EXISTS zbs_guest_shadow_user_id UUID,
    ADD COLUMN IF NOT EXISTS zbs_guest_grant_generation BIGINT;
ALTER TABLE channel_chat_context_generation DROP CONSTRAINT IF EXISTS zbs_channel_chat_context_generation_guest_pair_check;
ALTER TABLE channel_chat_context_generation ADD CONSTRAINT zbs_channel_chat_context_generation_guest_pair_check
    CHECK ((zbs_guest_shadow_user_id IS NULL) = (zbs_guest_grant_generation IS NULL)) NOT VALID;
