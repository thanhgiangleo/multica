-- Cross-chat search (MUL chat search): make LOWER(chat_message.content) LIKE
-- '%pattern%' indexable so searching every session's transcript does not fall
-- back to a sequential scan over the whole workspace history.
--
-- Mirrors 036_search_index_lower: the CJK-friendly pg_bigm GIN index is created
-- inside an exception handler because self-hosted deployments without the
-- pg_bigm extension still have to migrate. Those deployments keep the slower
-- sequential path and the handler's statement_timeout (SQLSTATE 57014 -> 503)
-- remains the guard rail. pg_trgm already exists from 137, so nothing here
-- creates extensions.
--
-- Follows 371's lesson: this migration is the only thing that creates the
-- index. A later migration will not recreate it, so do not drop it without
-- reading server/cmd/migrate/README.md's index-recovery procedure.
DROP INDEX IF EXISTS idx_chat_message_content_bigm;

DO $$
BEGIN
  CREATE INDEX idx_chat_message_content_bigm ON chat_message USING gin (LOWER(content) gin_bigm_ops);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'skipping bigram index on chat_message (pg_bigm not installed)';
END
$$;