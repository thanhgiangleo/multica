CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_idx_zalo_update_inbox_message ON zbs_zalo_update_inbox (installation_id, generation, message_id);
