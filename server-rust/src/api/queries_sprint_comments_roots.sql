-- Frozen Go roots projection; since selects oldest eligible roots, otherwise newest.
WITH RECURSIVE selected_roots AS (
    SELECT c.id, c.created_at
    FROM comment c
    WHERE c.issue_id=$1 AND c.workspace_id=$2 AND c.parent_id IS NULL
      AND ($3::timestamptz IS NULL OR c.created_at>$3)
    ORDER BY
      CASE WHEN $3::timestamptz IS NOT NULL THEN c.created_at END ASC,
      CASE WHEN $3::timestamptz IS NOT NULL THEN c.id END ASC,
      c.created_at DESC, c.id DESC
    LIMIT $4::int4
), membership(id, root_id, comment_created_at) AS (
    SELECT sr.id, sr.id, sr.created_at FROM selected_roots sr
    UNION ALL
    SELECT c.id, m.root_id, c.created_at
    FROM comment c JOIN membership m ON c.parent_id=m.id
    WHERE c.issue_id=$1 AND c.workspace_id=$2
), thread_stats AS (
    SELECT root_id, (COUNT(*)-1)::int4 AS reply_count,
           MAX(comment_created_at)::timestamptz AS last_activity_at
    FROM membership GROUP BY root_id
)
SELECT c.*, ts.reply_count, ts.last_activity_at
FROM selected_roots sr
JOIN comment c ON c.id=sr.id AND c.issue_id=$1 AND c.workspace_id=$2
JOIN thread_stats ts ON ts.root_id=sr.id
ORDER BY c.created_at ASC, c.id ASC;
