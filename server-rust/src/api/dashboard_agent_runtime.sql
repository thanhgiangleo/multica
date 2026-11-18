-- ListDashboardAgentRunTime from frozen server/pkg/db/queries/task_usage.sql.
SELECT atq.agent_id,
    COALESCE(SUM(EXTRACT(EPOCH FROM (atq.completed_at - atq.started_at)))::bigint, 0)::bigint AS total_seconds,
    COUNT(*)::int AS task_count,
    COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM task_usage tu WHERE tu.task_id = atq.id))::int AS metered_task_count,
    COUNT(*) FILTER (WHERE atq.status = 'failed')::int AS failed_count,
    COUNT(*) FILTER (WHERE atq.status = 'cancelled')::int AS cancelled_count
FROM agent_task_queue atq
JOIN agent a ON a.id = atq.agent_id
LEFT JOIN issue i ON i.id = atq.issue_id
WHERE a.workspace_id = $1
    AND atq.status IN ('completed', 'failed', 'cancelled')
    AND atq.started_at IS NOT NULL
    AND atq.completed_at IS NOT NULL
    AND atq.completed_at >= $2::timestamptz
    AND ($3::uuid IS NULL OR i.project_id = $3::uuid)
GROUP BY atq.agent_id
ORDER BY total_seconds DESC
