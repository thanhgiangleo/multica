CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_idx_zalo_update_inbox_chat ON zbs_zalo_update_inbox (workspace_id, installation_id, generation, chat_id, id);
