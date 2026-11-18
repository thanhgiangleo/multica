CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_idx_zalo_update_inbox_claim ON zbs_zalo_update_inbox (status, next_attempt_at, id) WHERE status IN ('pending', 'processing');
