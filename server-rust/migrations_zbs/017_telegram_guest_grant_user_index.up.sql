CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_guest_grant_user_uidx
    ON zbs_telegram_guest_grant (shadow_user_id);
