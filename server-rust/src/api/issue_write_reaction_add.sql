WITH inserted AS (
    INSERT INTO comment_reaction (comment_id, workspace_id, actor_type, actor_id, emoji)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (comment_id, actor_type, actor_id, emoji) DO NOTHING
    RETURNING *
), bumped AS (
    UPDATE comment SET revision = revision + 1
    WHERE id IN (SELECT comment_id FROM inserted) RETURNING revision
)
SELECT reaction.*, COALESCE((SELECT revision FROM bumped), 0)::bigint AS comment_revision
FROM inserted reaction
UNION ALL
SELECT reaction.*, 0::bigint AS comment_revision
FROM comment_reaction reaction
WHERE reaction.comment_id = $1 AND reaction.actor_type = $3
  AND reaction.actor_id = $4 AND reaction.emoji = $5
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1
