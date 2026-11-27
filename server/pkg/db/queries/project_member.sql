-- name: ListProjectMemberProjectIDs :many
SELECT project_id FROM project_member
WHERE user_id = $1 AND workspace_id = $2;

-- name: ListProjectMembers :many
SELECT * FROM project_member
WHERE project_id = $1 AND workspace_id = $2
ORDER BY created_at;

-- name: AddProjectMember :one
INSERT INTO project_member (project_id, user_id, workspace_id, created_by)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project_id, user_id) DO UPDATE SET project_id = EXCLUDED.project_id
RETURNING *;

-- name: RemoveProjectMember :exec
DELETE FROM project_member
WHERE project_id = $1 AND user_id = $2 AND workspace_id = $3;

-- name: DeleteProjectMembersByProject :exec
DELETE FROM project_member
WHERE project_id = $1 AND workspace_id = $2;

-- name: DeleteProjectMembersByUser :exec
DELETE FROM project_member
WHERE user_id = $1 AND workspace_id = $2;

-- name: SetMemberAccessScope :one
UPDATE member SET access_scope = $3
WHERE user_id = $1 AND workspace_id = $2
RETURNING *;

-- name: GetIssueProjectID :one
SELECT project_id FROM issue
WHERE id = $1 AND workspace_id = $2;

-- name: ListProjectMateUserIDs :many
SELECT DISTINCT user_id FROM project_member
WHERE workspace_id = $1 AND project_id = ANY($2::uuid[]);

-- name: ChildIssueProgressInProjects :many
SELECT parent_issue_id,
       COUNT(*)::bigint AS total,
       COUNT(*) FILTER (WHERE status = ANY(sqlc.arg('terminal_status_keys')::text[]))::bigint AS done
FROM issue
WHERE workspace_id = $1
  AND parent_issue_id IS NOT NULL
  AND project_id = ANY(sqlc.arg('project_ids')::uuid[])
GROUP BY parent_issue_id;

-- name: DeleteProjectMembersByWorkspace :exec
-- No foreign keys by project rule, so workspace teardown cleans up here.
DELETE FROM project_member WHERE workspace_id = $1;
