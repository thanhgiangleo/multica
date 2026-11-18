-- Integrator assigns a numbered migration after the original user asset ledger.
ALTER TABLE user_asset_object ADD COLUMN IF NOT EXISTS deleting BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE user_asset_object ADD COLUMN IF NOT EXISTS cleanup_checked_at TIMESTAMPTZ;
