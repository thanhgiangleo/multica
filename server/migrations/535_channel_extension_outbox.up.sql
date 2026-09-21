-- Durable hand-off from Multica to an out-of-process task-channel extension.
-- Relationships stay application-owned: repository policy forbids foreign keys.
CREATE TABLE IF NOT EXISTS channel_extension_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    installation_id UUID NOT NULL,
    channel_chat_id TEXT NOT NULL,
    text TEXT NOT NULL CHECK (octet_length(text) > 0),
    dedupe_key TEXT NOT NULL CHECK (char_length(dedupe_key) BETWEEN 1 AND 512),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until TIMESTAMPTZ,
    claim_token UUID,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
