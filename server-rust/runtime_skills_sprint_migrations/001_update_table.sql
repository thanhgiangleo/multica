-- Proposed Rust control-plane schema. Integrator assigns migration stream/version.
-- No foreign keys: runtime/initiator relationships are checked in application code.
CREATE TABLE IF NOT EXISTS runtime_rpc_update (
    id UUID NOT NULL,
    runtime_id UUID NOT NULL,
    initiator_user_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed','timeout')),
    target_version TEXT NOT NULL,
    output TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ
);
