CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_channel_extension_outbox_dedupe ON channel_extension_outbox (installation_id, dedupe_key);
