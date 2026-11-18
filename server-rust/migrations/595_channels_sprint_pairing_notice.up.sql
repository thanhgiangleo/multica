-- Static pairing reply directive, atomically committed with inbound redemption.
-- An attempted send is never retried: Telegram sendMessage has no dedup key.
CREATE TABLE IF NOT EXISTS channel_pairing_notice (
    workspace_id UUID NOT NULL,
    installation_id UUID NOT NULL,
    generation UUID NOT NULL,
    message_id TEXT NOT NULL,
    chat_id BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('paired','invalid','unavailable')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','attempted','delivered','refused','denied','unknown')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
