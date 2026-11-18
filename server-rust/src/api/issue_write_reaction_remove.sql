WITH deleted AS (
    DELETE FROM comment_reaction
    WHERE comment_id = $1 AND actor_type = $2 AND actor_id = $3 AND emoji = $4
    RETURNING comment_id
), bumped AS (
    UPDATE comment SET revision = revision + 1
    WHERE id IN (SELECT comment_id FROM deleted) RETURNING revision
)
SELECT EXISTS(SELECT 1 FROM deleted) AS changed,
       COALESCE((SELECT revision FROM bumped), 0)::bigint AS comment_revision
