WITH eligible_archived AS MATERIALIZED (
    SELECT i.id,
           COALESCE(i.issue_id, i.id) AS group_id,
           i.created_at,
           i.details
    FROM inbox_item i
    WHERE i.workspace_id = $1
      AND i.recipient_type = $2
      AND i.recipient_id = $3
      AND i.archived = true
      AND (i.issue_id IS NULL OR NOT EXISTS (
          SELECT 1
          FROM inbox_item active
          WHERE active.workspace_id = i.workspace_id
            AND active.recipient_type = i.recipient_type
            AND active.recipient_id = i.recipient_id
            AND active.issue_id = i.issue_id
            AND active.archived = false
      ))
), newest_groups AS (
    SELECT DISTINCT ON (group_id)
           group_id,
           id AS newest_id,
           created_at AS newest_created_at
    FROM eligible_archived
    ORDER BY group_id, created_at DESC, id DESC
), limited_groups AS (
    SELECT group_id, newest_id
    FROM newest_groups
    ORDER BY newest_created_at DESC, newest_id DESC
    LIMIT 200
), comment_anchors AS (
    SELECT DISTINCT ON (archived.group_id)
           archived.group_id,
           archived.id
    FROM eligible_archived archived
    JOIN limited_groups selected USING (group_id)
    WHERE NULLIF(archived.details->>'comment_id', '') IS NOT NULL
    ORDER BY archived.group_id, archived.created_at DESC, archived.id DESC
), selected_ids AS (
    SELECT newest_id AS id FROM limited_groups
    UNION
    SELECT id FROM comment_anchors
)
SELECT i.*,
       iss.status AS issue_status,
       iss.priority AS issue_priority
FROM inbox_item i
JOIN selected_ids selected ON selected.id = i.id
LEFT JOIN issue iss ON iss.id = i.issue_id
ORDER BY i.created_at DESC, i.id DESC;
