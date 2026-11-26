-- Per-(user, message) triage state for chat replies: FLAGGED ("I read this, I
-- still owe an answer") is stored separately from HANDLED ("done with this").
--
-- The problem: chat's read cursor (`chat_session.last_read_at`) is one bit for a
-- whole conversation, so "I saw it" and "I still owe an answer" are the same
-- state. A reader who opens a conversation, reads three replies that each ask
-- for something and is pulled away has no way back to which ones they did not
-- answer — all of them look read.
--
-- Two timestamps rather than one, because the product distinguishes the
-- reader's DECISION from the obligation being DISCHARGED:
--
--   flagged_at IS NOT NULL, handled_at IS NULL  -> needs attention now
--   handled_at IS NOT NULL                      -> dealt with
--   neither                                     -> nothing decided yet
--
-- A single handled_at would collapse "read but undecided" into "needs
-- attention", so a reader who scrolled past ten replies without deciding
-- anything would light up ten alarms and the signal would stop meaning
-- anything. KEEPING THAT THIRD STATE is the whole point of the second column.
--
-- Rows are created lazily on the first decision. No row means "nothing
-- decided" — the state of nearly every message — so the table grows with
-- deliberate action rather than message volume, and needs no backfill.
--
-- No foreign keys (MUL-3515): relationships are enforced in the application
-- layer. DeleteChatSession prunes this table inside its existing transaction,
-- alongside the other chat_session-keyed tables it already prunes there.
--
-- Every index on this table is built CONCURRENTLY in its own single-statement
-- migration (597, 598) per the repository's migration rules.
CREATE TABLE chat_message_user_state (
    chat_message_id UUID NOT NULL,
    user_id         UUID NOT NULL,
    workspace_id    UUID NOT NULL,
    chat_session_id UUID NOT NULL,
    flagged_at      TIMESTAMPTZ,
    handled_at      TIMESTAMPTZ,
    -- Own revision counter, deliberately not chat_message.revision: that one
    -- tracks content edits and drives client content sync. A triage decision
    -- changes no content and must never make a reader's client think the reply
    -- it is displaying was rewritten.
    revision        BIGINT NOT NULL DEFAULT 1,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_message_id, user_id),
    -- A message cannot be simultaneously outstanding and discharged. Enforcing
    -- it here means the two writers (flag, handle) cannot drift: every path
    -- clears the opposite timestamp in the same statement.
    CONSTRAINT chat_message_user_state_not_both
        CHECK (NOT (flagged_at IS NOT NULL AND handled_at IS NOT NULL))
);