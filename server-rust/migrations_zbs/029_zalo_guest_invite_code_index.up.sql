CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_zalo_guest_invite_code_uidx
    ON zbs_zalo_guest_invite (installation_id, code_hash);
