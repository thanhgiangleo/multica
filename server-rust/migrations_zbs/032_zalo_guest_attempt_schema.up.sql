-- A durable fixed-window gate shared by every server replica. Keys are SHA-256
-- hashes of dimension + installation + external sender; no code or sender ID
-- is stored. Expired windows can be pruned independently of guest history.
CREATE TABLE IF NOT EXISTS zbs_zalo_guest_attempt (
    scope_hash TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts > 0)
);
