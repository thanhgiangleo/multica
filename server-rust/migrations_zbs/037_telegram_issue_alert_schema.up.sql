SET LOCAL lock_timeout = '5s';

-- Opt-in notifications, independent of the bot's conversation agent binding.
CREATE TABLE IF NOT EXISTS zbs_telegram_issue_alert_rule (
    id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    installation_id UUID NOT NULL,
    recipient_user_id UUID NOT NULL,
    telegram_bot_id TEXT NOT NULL,
    telegram_chat_id TEXT NOT NULL,
    enabled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    enabled BOOLEAN NOT NULL DEFAULT false,
    test_requested BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS zbs_telegram_issue_alert_delivery (
    rule_id UUID NOT NULL,
    activity_key TEXT NOT NULL,
    issue_id UUID,
    occurred_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'sent', 'failed', 'unknown')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_id BIGINT,
    error_kind TEXT NOT NULL DEFAULT ''
);
