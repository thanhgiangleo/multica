-- Guest visibility is a permanent property of a session, not of its creator,
-- current route, grant, status, or most recent sender. Sessions that ever held
-- guest content are readable only by the current agent owner. The flag is
-- sticky; archive, revocation and later member messages never clear it.
--
-- This file only adds the column. A constant default makes it a catalog-only
-- change, so the ACCESS EXCLUSIVE lock is brief; the backfill lives in 036 so
-- it does not run while that lock is held.
ALTER TABLE chat_session ADD COLUMN IF NOT EXISTS zbs_has_guest_history BOOLEAN NOT NULL DEFAULT false;
