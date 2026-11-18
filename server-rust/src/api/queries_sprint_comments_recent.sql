-- Frozen Go recent-window projection: N most recently active threads, each
-- returned whole with its root. Tenant predicate is repeated on every recursion
-- step so a cross-workspace parent link can never pull another tenant's row in.
WITH RECURSIVE membership(id, root_id, comment_created_at) AS (
    SELECT c.id, c.id, c.created_at
    FROM comment c
    WHERE c.issue_id=$1 AND c.workspace_id=$2 AND c.parent_id IS NULL
    UNION ALL
    SELECT c.id, m.root_id, c.created_at
    FROM comment c JOIN membership m ON c.parent_id=m.id
    WHERE c.issue_id=$1 AND c.workspace_id=$2
), thread_stats AS (
    SELECT root_id, MAX(comment_created_at)::timestamptz AS last_activity_at
    FROM membership GROUP BY root_id
), picked AS (
    SELECT ts.root_id, ts.last_activity_at
    FROM thread_stats ts
    WHERE (
        $3::boolean = FALSE
        OR (ts.last_activity_at, ts.root_id) < ($4::timestamptz, $5::uuid)
    )
    ORDER BY ts.last_activity_at DESC, ts.root_id DESC
    LIMIT $6::int4
)
SELECT c.*, p.root_id AS thread_root_id,
       p.last_activity_at AS thread_last_activity_at
FROM picked p
JOIN membership m ON m.root_id=p.root_id
JOIN comment c ON c.id=m.id AND c.issue_id=$1 AND c.workspace_id=$2
ORDER BY p.last_activity_at ASC, p.root_id ASC, c.created_at ASC, c.id ASC;
