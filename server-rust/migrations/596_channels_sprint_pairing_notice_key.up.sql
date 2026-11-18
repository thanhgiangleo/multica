CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_channel_pairing_notice_key ON channel_pairing_notice (installation_id, message_id);
