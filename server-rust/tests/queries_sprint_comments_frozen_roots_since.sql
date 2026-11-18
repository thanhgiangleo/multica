-- name: ListRootCommentsSinceForIssue :many
WITH RECURSIVE selected_roots AS (
    SELECT c.id, c.created_at
    FROM comment c
    WHERE c.issue_id = $1
      AND c.workspace_id = $2
      AND c.parent_id IS NULL
      AND c.created_at > $3
    ORDER BY c.created_at ASC, c.id ASC
    LIMIT $4
),
membership(id, root_id, comment_created_at) AS (
    SELECT sr.id, sr.id AS root_id, sr.created_at
    FROM selected_roots sr
    UNION ALL
    SELECT c.id, m.root_id, c.created_at
    FROM comment c
    JOIN membership m ON c.parent_id = m.id
    WHERE c.issue_id = $1
      AND c.workspace_id = $2
),
thread_stats AS (
    SELECT root_id,
           (COUNT(*) - 1)::int AS reply_count,
           MAX(comment_created_at)::timestamptz AS last_activity_at
    FROM membership
    GROUP BY root_id
)
SELECT c.id, c.issue_id, c.author_type, c.author_id, c.content, c.type,
       c.created_at, c.updated_at, c.parent_id, c.workspace_id,
       c.resolved_at, c.resolved_by_type, c.resolved_by_id,
       c.source_task_id, c.quick_action_id, c.revision, c.deleted_at,
       ts.reply_count AS reply_count,
       ts.last_activity_at AS last_activity_at
FROM selected_roots sr
JOIN comment c ON c.id = sr.id
JOIN thread_stats ts ON ts.root_id = sr.id
ORDER BY c.created_at ASC, c.id ASC
