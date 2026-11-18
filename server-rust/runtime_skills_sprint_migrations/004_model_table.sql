-- Integrator assigns migration stream/version; no FKs, no indexes in this file.
CREATE TABLE IF NOT EXISTS runtime_rpc_model_list (
    id UUID NOT NULL,
    runtime_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','running','completed','failed','timeout')),
    models JSONB,
    unavailable_models JSONB,
    supported BOOLEAN NOT NULL DEFAULT TRUE,
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS runtime_rpc_model_catalog (
    runtime_id UUID NOT NULL,
    models JSONB NOT NULL,
    unavailable_models JSONB NOT NULL,
    supported BOOLEAN NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
