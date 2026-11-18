-- Refuse rollback before touching the live constraint while Zalo issues exist.
-- The migration runner does not wrap files in a transaction, so checking only
-- during VALIDATE would leave a partially replaced NOT VALID constraint.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM issue WHERE origin_type = 'zalo_chat' LIMIT 1) THEN
        RAISE EXCEPTION 'cannot roll back Zalo issue origin while Zalo issues remain';
    END IF;
END;
$$;
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat', 'wecom_chat', 'telegram_chat'))
    NOT VALID;
ALTER TABLE issue VALIDATE CONSTRAINT issue_origin_type_check;
