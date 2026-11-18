CREATE INDEX CONCURRENTLY IF NOT EXISTS runtime_rpc_model_list_pending_idx ON runtime_rpc_model_list (runtime_id,created_at,id) WHERE status='pending';
