-- The flag is sticky by design and 035's down drops the column; clearing the
-- backfilled values here would expose guest sessions to their creator.
SELECT 1;
