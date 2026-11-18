CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_channel_pairing_notice_pending ON channel_pairing_notice (installation_id, created_at) WHERE status='pending';
