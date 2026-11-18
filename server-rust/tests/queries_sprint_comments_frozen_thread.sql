-- name: ListThreadCommentsForIssuePaged :many
WITH RECURSIVE root_of AS (
    SELECT c.id, c.parent_id
    FROM comment c
    WHERE c.id = $1 AND c.issue_id = $2 AND c.workspace_id = $3
    UNION ALL
    SELECT p.id, p.parent_id
    FROM comment p
    JOIN root_of r ON p.id = r.parent_id
    WHERE p.issue_id = $2 AND p.workspace_id = $3
),
thread_root AS (
    SELECT id FROM root_of WHERE parent_id IS NULL LIMIT 1
),
descendants AS (
    SELECT c.id, c.issue_id, c.author_type, c.author_id, c.content, c.type,
           c.created_at, c.updated_at, c.parent_id, c.workspace_id,
           c.resolved_at, c.resolved_by_type, c.resolved_by_id,
           c.source_task_id, c.quick_action_id, c.revision, c.deleted_at
    FROM comment c
    JOIN thread_root tr ON c.id = tr.id
    UNION
    SELECT c.id, c.issue_id, c.author_type, c.author_id, c.content, c.type,
           c.created_at, c.updated_at, c.parent_id, c.workspace_id,
           c.resolved_at, c.resolved_by_type, c.resolved_by_id,
           c.source_task_id, c.quick_action_id, c.revision, c.deleted_at
    FROM comment c
    JOIN descendants d ON c.parent_id = d.id
    WHERE c.issue_id = $2 AND c.workspace_id = $3
),
reply_page AS (
    SELECT d.id, d.issue_id, d.author_type, d.author_id, d.content, d.type,
           d.created_at, d.updated_at, d.parent_id, d.workspace_id,
           d.resolved_at, d.resolved_by_type, d.resolved_by_id,
           d.source_task_id, d.quick_action_id, d.revision, d.deleted_at
    FROM descendants d
    WHERE d.id NOT IN (SELECT id FROM thread_root)
      AND (
          $4::boolean = FALSE
          OR (d.created_at, d.id) < ($5::timestamptz, $6::uuid)
      )
    ORDER BY d.created_at DESC, d.id DESC
    LIMIT $7
)
SELECT id, issue_id, author_type, author_id, content, type,
       created_at, updated_at, parent_id, workspace_id,
       resolved_at, resolved_by_type, resolved_by_id,
       source_task_id, quick_action_id, revision, deleted_at
FROM (
    SELECT d.id, d.issue_id, d.author_type, d.author_id, d.content, d.type,
           d.created_at, d.updated_at, d.parent_id, d.workspace_id,
           d.resolved_at, d.resolved_by_type, d.resolved_by_id,
           d.source_task_id, d.quick_action_id, d.revision, d.deleted_at
    FROM descendants d
    JOIN thread_root tr ON d.id = tr.id
    UNION ALL
    SELECT id, issue_id, author_type, author_id, content, type,
           created_at, updated_at, parent_id, workspace_id,
           resolved_at, resolved_by_type, resolved_by_id,
           source_task_id, quick_action_id, revision, deleted_at
    FROM reply_page
) combined
ORDER BY created_at ASC, id ASC
