-- A linked PR may be hidden from cards by a connection that currently
-- belongs to another workspace/provider. Its workspace-local state still
-- participates in the frozen Go merge decision. Page inside the same snapshot
-- so a large hidden set cannot bypass the read-model paging boundary.
SELECT pr.id, pr.pr_number, pr.state, pr.pr_created_at
FROM vcs_pull_request pr
JOIN issue_vcs_pull_request ipr ON ipr.pull_request_id = pr.id
WHERE ipr.issue_id = $1 AND pr.workspace_id = $2
  AND EXISTS (
    SELECT 1 FROM vcs_connection c WHERE c.id = pr.connection_id
  )
  AND NOT EXISTS (
    SELECT 1 FROM vcs_connection c
    WHERE c.id = pr.connection_id AND c.workspace_id = pr.workspace_id
      AND c.provider = pr.provider
  )
  AND ($3::boolean = FALSE OR (pr.pr_created_at, pr.id) < ($4::timestamptz, $5::uuid))
ORDER BY pr.pr_created_at DESC, pr.id DESC
LIMIT 512
