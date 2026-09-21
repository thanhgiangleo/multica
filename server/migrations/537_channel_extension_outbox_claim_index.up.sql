CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_channel_extension_outbox_claim ON channel_extension_outbox (available_at, created_at) WHERE delivered_at IS NULL;
