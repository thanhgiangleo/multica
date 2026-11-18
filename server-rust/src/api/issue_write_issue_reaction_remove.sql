WITH deleted AS (
    DELETE FROM issue_reaction
    WHERE issue_id = $1 AND actor_type = $2 AND actor_id = $3 AND emoji = $4
    RETURNING issue_id
), bumped AS (
    UPDATE issue
    SET revision = revision + 1
    WHERE id IN (SELECT issue_id FROM deleted)
    RETURNING revision
)
SELECT EXISTS(SELECT 1 FROM deleted) AS changed,
       COALESCE((SELECT revision FROM bumped), 0)::bigint AS issue_revision;
