-- External task-channel extensions use one generic origin. Provider-specific
-- names (Zalo, Matrix, Mattermost, ...) remain outside the Multica schema.
-- Drop the previous enum-like constraint before rewriting legacy rows: a
-- self-host that already ran the short-lived provider-specific migration
-- enforces `zalo_chat` and would otherwise reject `external_chat` mid-upgrade.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;

UPDATE issue SET origin_type = 'external_chat' WHERE origin_type = 'zalo_chat';

UPDATE channel_installation SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_user_binding SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_chat_session_binding SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_inbound_audit SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_binding_token SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_task_delivery SET channel_type = 'external' WHERE channel_type = 'zalo';
UPDATE channel_outbound_card_message SET channel_type = 'external' WHERE channel_type = 'zalo';

ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat', 'wecom_chat', 'telegram_chat', 'external_chat'))
    NOT VALID;
