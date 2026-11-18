-- Fetched Zalo updates must survive process restarts before router dispatch.
-- A terminal failure is retained for investigation and blocks later turns in its chat.
CREATE TABLE IF NOT EXISTS zbs_zalo_update_inbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    workspace_id UUID NOT NULL,
    installation_id UUID NOT NULL,
    generation TEXT NOT NULL,
    message_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'delivered', 'failed', 'obsolete')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    owner_token UUID,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
