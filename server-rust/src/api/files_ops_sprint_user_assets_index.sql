-- Integrator assigns this concurrent index its own single-statement migration.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS user_asset_object_id_key ON user_asset_object (id);
