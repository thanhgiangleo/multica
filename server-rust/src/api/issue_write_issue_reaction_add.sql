WITH inserted AS (
    INSERT INTO issue_reaction (issue_id, workspace_id, actor_type, actor_id, emoji)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (issue_id, actor_type, actor_id, emoji) DO NOTHING
    RETURNING *
), bumped AS (
    UPDATE issue
    SET revision = revision + 1
    WHERE id IN (SELECT issue_id FROM inserted)
    RETURNING revision
)
SELECT reaction.*, COALESCE((SELECT revision FROM bumped), 0)::bigint AS issue_revision
FROM inserted reaction
UNION ALL
SELECT reaction.*, 0::bigint AS issue_revision
FROM issue_reaction reaction
WHERE reaction.issue_id = $1
  AND reaction.actor_type = $3
  AND reaction.actor_id = $4
  AND reaction.emoji = $5
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1;
