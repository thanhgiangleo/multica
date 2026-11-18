CREATE INDEX CONCURRENTLY zbs_idx_chat_session_zalo_agent ON chat_session(workspace_id, agent_id, updated_at DESC) WHERE zbs_source_channel_type = 'zalo';
