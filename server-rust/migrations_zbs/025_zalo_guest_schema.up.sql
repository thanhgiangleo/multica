CREATE TABLE IF NOT EXISTS zbs_zalo_guest_invite (
    id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    installation_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    code_hash TEXT NOT NULL,
    issued_by_user_id UUID NOT NULL,
    alias TEXT NOT NULL CHECK (char_length(alias) BETWEEN 1 AND 80),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_by_user_id UUID,
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '24 hours'),
    CHECK ((revoked_at IS NULL) = (revoked_by_user_id IS NULL))
);

CREATE TABLE IF NOT EXISTS zbs_zalo_guest_grant (
    installation_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    zalo_user_id TEXT NOT NULL CHECK (zalo_user_id <> ''),
    shadow_user_id UUID NOT NULL,
    alias TEXT NOT NULL CHECK (char_length(alias) BETWEEN 1 AND 80),
    zalo_display_name TEXT NOT NULL DEFAULT '',
    invite_id UUID NOT NULL,
    grant_generation BIGINT NOT NULL DEFAULT 1 CHECK (grant_generation > 0),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    revoked_by_user_id UUID,
    CHECK ((revoked_at IS NULL) = (revoked_by_user_id IS NULL))
);

