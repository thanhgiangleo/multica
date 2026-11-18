-- Frozen Go server/pkg/db/queries/activity.sql, ListActivitiesForIssue (2026-10-31).
SELECT * FROM (
    SELECT * FROM activity_log
    WHERE issue_id = $1
    ORDER BY created_at DESC, id DESC
    LIMIT $2
) AS recent
ORDER BY created_at ASC, id ASC;
