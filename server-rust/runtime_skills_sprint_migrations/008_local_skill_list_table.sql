CREATE TABLE IF NOT EXISTS runtime_rpc_local_skill_list (
    id UUID NOT NULL,
    runtime_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout')),
    skills JSONB,
    supported BOOLEAN NOT NULL DEFAULT TRUE,
    mcp_servers JSONB,
    mcp_supported BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '5 minutes')
);
