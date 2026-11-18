-- Refuse while Zalo guest data remains; see 012's down.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM zbs_zalo_guest_invite LIMIT 1)
        OR EXISTS (SELECT 1 FROM zbs_zalo_guest_grant LIMIT 1) THEN
        RAISE EXCEPTION 'cannot roll back Zalo guest schema while guest data remains';
    END IF;
END;
$$;
DROP TABLE IF EXISTS zbs_zalo_guest_grant;
DROP TABLE IF EXISTS zbs_zalo_guest_invite;
