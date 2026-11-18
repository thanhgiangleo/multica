WITH RECURSIVE root_of AS (
    -- Walk up from the target to its thread root.
    SELECT c.id, c.parent_id
    FROM comment c
    WHERE c.id = $1 AND c.issue_id = $2 AND c.workspace_id = $3
    UNION ALL
    SELECT p.id, p.parent_id
    FROM comment p
    JOIN root_of r ON p.id = r.parent_id
),
thread_root AS (
    SELECT id FROM root_of WHERE parent_id IS NULL LIMIT 1
),
descendants AS (
    -- Expand back down from the root over the whole subtree. Cycle-safe under
    -- the PK constraint (a comment cannot be its own ancestor).
    SELECT c.id
    FROM comment c
    JOIN thread_root tr ON c.id = tr.id
    UNION
    SELECT c.id
    FROM comment c
    JOIN descendants d ON c.parent_id = d.id
    WHERE c.issue_id = $2 AND c.workspace_id = $3
)
UPDATE comment SET
    resolved_at = NULL,
    resolved_by_type = NULL,
    resolved_by_id = NULL,
    revision = revision + 1,
    updated_at = now()
WHERE comment.id IN (SELECT id FROM descendants)
  AND comment.id <> $1
  AND comment.resolved_at IS NOT NULL
RETURNING *;
