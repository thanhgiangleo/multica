CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_guest_invite_code_uidx
    ON zbs_telegram_guest_invite (installation_id, code_hash);
