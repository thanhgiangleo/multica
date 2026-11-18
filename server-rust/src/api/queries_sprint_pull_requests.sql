WITH issue_prs AS (
    SELECT pr.*, COALESCE(ipr.linked_by_type, 'system')::text AS linked_by_type
    FROM github_pull_request pr
    JOIN issue_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1 AND pr.workspace_id = $2
      AND ($3::boolean = FALSE OR (pr.pr_created_at, pr.id) < ($4::timestamptz, $5::uuid))
    ORDER BY pr.pr_created_at DESC, pr.id DESC
    LIMIT 512
)
SELECT pr.*, c.* FROM issue_prs pr
CROSS JOIN LATERAL (
    SELECT COUNT(*)::bigint AS checks_total,
        COUNT(*) FILTER (WHERE cr.status = 'completed' AND cr.conclusion IN
            ('failure','cancelled','timed_out','action_required','startup_failure','stale','error'))::bigint AS checks_failed,
        COUNT(*) FILTER (WHERE cr.status = 'completed' AND cr.conclusion IN
            ('success','neutral','skipped'))::bigint AS checks_passed,
        COUNT(*) FILTER (WHERE cr.status <> 'completed' OR cr.conclusion IS NULL)::bigint AS checks_running,
        COALESCE(array_agg(cr.name) FILTER (WHERE cr.status = 'completed' AND cr.conclusion IN
            ('failure','cancelled','timed_out','action_required','startup_failure','stale','error')), '{}')::text[] AS failed_check_names
    FROM github_pull_request_check_run cr
    WHERE cr.pr_id = pr.id AND cr.head_sha = pr.snapshot_head_sha AND pr.snapshot_head_sha <> ''
) c
ORDER BY pr.pr_created_at DESC, pr.id DESC
