CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_zalo_guest_grant_identity_uidx
    ON zbs_zalo_guest_grant (installation_id, zalo_user_id);
