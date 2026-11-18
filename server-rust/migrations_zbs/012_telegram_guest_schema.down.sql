-- Refuse while any guest data remains. Shadow users are never deleted by the
-- application, and dropping zbs_is_shadow would silently turn them into ordinary
-- users. Zalo guest tables are dropped by 025's down, which runs first.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM "user" WHERE zbs_is_shadow LIMIT 1)
        OR EXISTS (SELECT 1 FROM zbs_telegram_guest_invite LIMIT 1)
        OR EXISTS (SELECT 1 FROM zbs_telegram_guest_grant LIMIT 1)
        OR EXISTS (SELECT 1 FROM attachment WHERE uploader_type = 'guest' LIMIT 1) THEN
        RAISE EXCEPTION 'cannot roll back guest schema while guest data remains';
    END IF;
END;
$$;
ALTER TABLE attachment DROP CONSTRAINT IF EXISTS attachment_uploader_type_check;
ALTER TABLE attachment ADD CONSTRAINT attachment_uploader_type_check
    CHECK (uploader_type IN ('member', 'agent')) NOT VALID;
DROP TABLE IF EXISTS zbs_telegram_guest_grant;
DROP TABLE IF EXISTS zbs_telegram_guest_invite;
ALTER TABLE "user" DROP COLUMN IF EXISTS zbs_is_shadow;
