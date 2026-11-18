CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_zalo_guest_attempt_key_uidx ON zbs_zalo_guest_attempt(scope_hash, window_start);
