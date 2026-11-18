-- ListDashboardFailuresByAgent from frozen server/pkg/db/queries/task_usage.sql.
SELECT atq.agent_id,
    CASE WHEN atq.status = 'failed' THEN COALESCE(NULLIF(atq.failure_reason, ''), 'unclassified') ELSE '' END AS failure_reason,
    COUNT(*)::int AS task_count
FROM agent_task_queue atq
JOIN agent a ON a.id = atq.agent_id
LEFT JOIN issue i ON i.id = atq.issue_id
WHERE a.workspace_id = $1
    AND atq.status IN ('completed', 'failed')
    AND atq.completed_at IS NOT NULL
    AND atq.completed_at >= $2::timestamptz
    AND ($3::uuid IS NULL OR i.project_id = $3::uuid)
GROUP BY atq.agent_id, 2
ORDER BY atq.agent_id, 2
