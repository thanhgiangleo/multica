CREATE TABLE IF NOT EXISTS zbs_gateway_daily_telegram_rule (
    workspace_id uuid NOT NULL,
    owner_user_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    installation_id uuid NOT NULL,
    telegram_bot_id text NOT NULL,
    telegram_chat_id text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS zbs_gateway_daily_telegram_delivery (
    workspace_id uuid NOT NULL,
    request_id uuid NOT NULL,
    issue_id uuid NOT NULL,
    cutoff date NOT NULL,
    content_sha256 text NOT NULL,
    status text NOT NULL,
    message_id bigint,
    updated_at timestamptz NOT NULL DEFAULT now()
);
