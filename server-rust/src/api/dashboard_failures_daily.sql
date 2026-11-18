-- Frozen staged Go task_usage.sql aggregate, with sqlc arguments mapped to $1..$4.
SELECT
    DATE(atq.completed_at AT TIME ZONE $2::text) AS date,
    CASE
        WHEN atq.status = 'failed'
            THEN COALESCE(NULLIF(atq.failure_reason, ''), 'unclassified')
        ELSE ''
    END AS failure_reason,
    COUNT(*)::int AS task_count
FROM agent_task_queue atq
JOIN agent a ON a.id = atq.agent_id
LEFT JOIN issue i ON i.id = atq.issue_id
WHERE a.workspace_id = $1
  AND atq.status IN ('completed', 'failed')
  AND atq.completed_at IS NOT NULL
  AND atq.completed_at >= $3::timestamptz
  AND ($4::uuid IS NULL OR i.project_id = $4::uuid)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
