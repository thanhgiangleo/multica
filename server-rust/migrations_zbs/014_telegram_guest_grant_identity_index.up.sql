CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_guest_grant_identity_uidx
    ON zbs_telegram_guest_grant (installation_id, telegram_user_id);
