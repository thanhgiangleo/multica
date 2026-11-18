-- Frozen Go github.sql ListPullRequestsByIssue; sqlc.arg('issue_id') becomes $1.
WITH issue_prs AS (
    SELECT pr.id, pr.snapshot_head_sha
    FROM github_pull_request pr
    JOIN issue_pull_request ipr ON ipr.pull_request_id = pr.id
    WHERE ipr.issue_id = $1
),
checks AS (
    SELECT
        cr.pr_id,
        COUNT(*)::bigint AS total,
        SUM(CASE WHEN cr.status = 'completed' AND cr.conclusion IN
                ('failure','cancelled','timed_out','action_required','startup_failure','stale','error')
            THEN 1 ELSE 0 END)::bigint AS failed,
        SUM(CASE WHEN cr.status = 'completed' AND cr.conclusion IN
                ('success','neutral','skipped')
            THEN 1 ELSE 0 END)::bigint AS passed,
        SUM(CASE WHEN cr.status <> 'completed' OR cr.conclusion IS NULL
            THEN 1 ELSE 0 END)::bigint AS running,
        COALESCE(
            array_agg(cr.name) FILTER (WHERE cr.status = 'completed' AND cr.conclusion IN
                ('failure','cancelled','timed_out','action_required','startup_failure','stale','error')),
            '{}'
        )::text[] AS failed_names
    FROM github_pull_request_check_run cr
    JOIN issue_prs ip ON ip.id = cr.pr_id
    WHERE cr.head_sha = ip.snapshot_head_sha AND ip.snapshot_head_sha <> ''
    GROUP BY cr.pr_id
)
SELECT
    pr.id, pr.workspace_id, pr.installation_id, pr.repo_owner, pr.repo_name,
    pr.pr_number, pr.title, pr.state, pr.html_url, pr.branch, pr.author_login,
    pr.author_avatar_url, pr.merged_at, pr.closed_at, pr.pr_created_at,
    pr.pr_updated_at, pr.head_sha, pr.mergeable_state,
    pr.additions, pr.deletions, pr.changed_files,
    pr.api_mergeable, pr.api_merge_state_status, pr.checks_rollup_state,
    pr.snapshot_head_sha, pr.snapshot_fetched_at,
    pr.created_at, pr.updated_at,
    COALESCE(ipr.linked_by_type, 'system')::text AS linked_by_type,
    COALESCE(c.total, 0)::bigint AS checks_total,
    COALESCE(c.passed, 0)::bigint AS checks_passed,
    COALESCE(c.failed, 0)::bigint AS checks_failed,
    COALESCE(c.running, 0)::bigint AS checks_running,
    COALESCE(c.failed_names, '{}')::text[] AS failed_check_names
FROM github_pull_request pr
JOIN issue_pull_request ipr ON ipr.pull_request_id = pr.id
LEFT JOIN checks c ON c.pr_id = pr.id
WHERE ipr.issue_id = $1
ORDER BY pr.pr_created_at DESC
