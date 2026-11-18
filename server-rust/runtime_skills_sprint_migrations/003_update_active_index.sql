CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS runtime_rpc_update_active_idx ON runtime_rpc_update (runtime_id) WHERE status IN ('pending','running');
