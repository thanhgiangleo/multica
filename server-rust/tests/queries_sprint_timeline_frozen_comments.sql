-- Frozen Go server/pkg/db/queries/comment.sql, ListCommentsForIssue (2026-10-31).
SELECT * FROM (
    SELECT * FROM comment
    WHERE issue_id = $1 AND workspace_id = $2
    ORDER BY created_at DESC, id DESC
    LIMIT $3
) AS recent
ORDER BY created_at ASC, id ASC;
