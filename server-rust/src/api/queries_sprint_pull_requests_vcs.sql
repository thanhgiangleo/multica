WITH issue_prs AS (
    SELECT pr.*, COALESCE(ipr.linked_by_type, 'system')::text AS linked_by_type
    FROM vcs_pull_request pr
    JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
    LEFT JOIN vcs_connection vc ON vc.id = pr.connection_id AND vc.workspace_id = pr.workspace_id AND vc.provider = pr.provider
    WHERE ipr.issue_id = $1 AND pr.workspace_id = $2
      AND (vc.id IS NOT NULL OR NOT EXISTS (SELECT 1 FROM vcs_connection c WHERE c.id = pr.connection_id))
      AND ($3::boolean = FALSE OR (pr.pr_created_at, pr.id) < ($4::timestamptz, $5::uuid))
    ORDER BY pr.pr_created_at DESC, pr.id DESC
    LIMIT 512
)
SELECT pr.*, c.* FROM issue_prs pr
CROSS JOIN LATERAL (
    SELECT COUNT(*)::bigint AS checks_total,
        COUNT(*) FILTER (WHERE cs.state = 'failed')::bigint AS checks_failed,
        COUNT(*) FILTER (WHERE cs.state = 'passed')::bigint AS checks_passed,
        COUNT(*) FILTER (WHERE cs.state = 'pending')::bigint AS checks_running
    FROM vcs_commit_status cs
    WHERE cs.connection_id = pr.connection_id AND cs.sha = pr.head_sha AND pr.head_sha <> ''
) c
ORDER BY pr.pr_created_at DESC, pr.id DESC
