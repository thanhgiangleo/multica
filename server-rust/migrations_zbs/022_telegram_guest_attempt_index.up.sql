CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_guest_attempt_key_uidx ON zbs_telegram_guest_attempt(scope_hash, window_start);
