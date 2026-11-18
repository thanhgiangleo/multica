CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_guest_grant_invite_idx
    ON zbs_telegram_guest_grant (invite_id);
