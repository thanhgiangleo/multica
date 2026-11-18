-- Integrator assigns a numbered migration. No foreign keys: upload intents must
-- survive user deletion and be reclaimable after a crashed writer. A concurrent
-- index build is required for every index; see the separate index script.
CREATE TABLE IF NOT EXISTS user_asset_object (
    id UUID NOT NULL,
    user_id UUID NOT NULL,
    object_url TEXT NOT NULL,
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    ready BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
