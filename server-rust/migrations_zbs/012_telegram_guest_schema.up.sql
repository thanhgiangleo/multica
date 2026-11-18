-- The runner sends this file as one implicit transaction, so the "user" lock
-- is held while the attachment lock is acquired. Bound the wait: a busy table
-- fails the migration for a retry instead of stalling every auth lookup.
SET LOCAL lock_timeout = '5s';

-- Shadow identities carry attribution, never workspace membership or login.
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS zbs_is_shadow BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS zbs_telegram_guest_invite (
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

CREATE TABLE IF NOT EXISTS zbs_telegram_guest_grant (
    installation_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    telegram_user_id TEXT NOT NULL CHECK (telegram_user_id <> ''),
    shadow_user_id UUID NOT NULL,
    alias TEXT NOT NULL CHECK (char_length(alias) BETWEEN 1 AND 80),
    telegram_display_name TEXT NOT NULL DEFAULT '',
    invite_id UUID NOT NULL,
    grant_generation BIGINT NOT NULL DEFAULT 1 CHECK (grant_generation > 0),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    revoked_by_user_id UUID,
    CHECK ((revoked_at IS NULL) = (revoked_by_user_id IS NULL))
);

-- Widen first; validate separately to avoid holding the DDL lock during scan.
ALTER TABLE attachment DROP CONSTRAINT IF EXISTS attachment_uploader_type_check;
ALTER TABLE attachment ADD CONSTRAINT attachment_uploader_type_check
    CHECK (uploader_type IN ('member', 'agent', 'guest')) NOT VALID;
