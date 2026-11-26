-- Reverses 596/597/598. The table holds only per-user triage decisions and is
-- referenced by no other table, so dropping it returns the product to
-- "read cursor only" with no dangling references. Its indexes go with it.
DROP TABLE IF EXISTS chat_message_user_state;
