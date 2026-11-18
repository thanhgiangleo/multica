-- Frozen Go vcs.sql ListVCSPullRequestsByIssue; sqlc.arg('issue_id') becomes $1.
WITH checks AS (
    SELECT
        pr.id AS pr_id,
        COUNT(*)::bigint AS total,
        SUM(CASE WHEN cs.state = 'failed'  THEN 1 ELSE 0 END)::bigint AS failed,
        SUM(CASE WHEN cs.state = 'passed'  THEN 1 ELSE 0 END)::bigint AS passed,
        SUM(CASE WHEN cs.state = 'pending' THEN 1 ELSE 0 END)::bigint AS pending
    FROM vcs_pull_request pr
    JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
    JOIN vcs_commit_status cs
        ON cs.connection_id = pr.connection_id
       AND cs.sha = pr.head_sha
       AND pr.head_sha <> ''
    WHERE ipr.issue_id = $1
    GROUP BY pr.id
)
SELECT
    pr.*,
    COALESCE(ipr.linked_by_type, 'system')::text AS linked_by_type,
    COALESCE(c.total, 0)::bigint   AS checks_total,
    COALESCE(c.passed, 0)::bigint  AS checks_passed,
    COALESCE(c.failed, 0)::bigint  AS checks_failed,
    COALESCE(c.pending, 0)::bigint AS checks_pending
FROM vcs_pull_request pr
JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
LEFT JOIN checks c ON c.pr_id = pr.id
WHERE ipr.issue_id = $1
ORDER BY pr.pr_created_at DESC
