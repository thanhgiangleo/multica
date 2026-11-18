-- Frozen Go ListArchivedInboxPage; positional parameters preserve generated SQL order.
-- Select narrow group representatives before loading bodies or comment anchors.
WITH newest AS MATERIALIZED (
    SELECT DISTINCT ON (COALESCE(i.issue_id, i.id))
           i.id, i.issue_id, i.created_at, i.read,
           CASE WHEN i.actor_type = 'system' THEN 'system'
                ELSE i.actor_type || ':' || i.actor_id::text END AS actor
    FROM inbox_item i
    WHERE i.workspace_id = $1::uuid
      AND i.recipient_type = 'member'
      AND i.recipient_id = $2::uuid
      AND i.archived = true
      AND ($3::uuid IS NULL OR i.issue_id = $3::uuid
           OR (i.issue_id IS NULL AND i.id = $3::uuid))
      AND (i.issue_id IS NULL OR NOT EXISTS (
          SELECT 1 FROM inbox_item active
          WHERE active.workspace_id = i.workspace_id
            AND active.recipient_type = i.recipient_type
            AND active.recipient_id = i.recipient_id
            AND active.issue_id = i.issue_id AND active.archived = false
      ))
    ORDER BY COALESCE(i.issue_id, i.id), i.created_at DESC, i.id DESC
), projected AS (
    SELECT newest.*, iss.status AS issue_status, iss.priority AS issue_priority
    FROM newest
    LEFT JOIN issue iss ON iss.id = newest.issue_id
      AND iss.workspace_id = $1::uuid
), matched AS (
    SELECT projected.*,
           (cardinality($4::text[]) = 0 OR issue_status = ANY($4::text[])) AS status_match,
           (cardinality($5::text[]) = 0 OR issue_priority = ANY($5::text[])) AS priority_match,
           (cardinality($6::text[]) = 0 OR actor = ANY($6::text[])) AS actor_match,
           (NOT $7::boolean OR NOT read) AS read_match
    FROM projected
)
, selected AS (
    SELECT * FROM matched
    WHERE status_match AND priority_match AND actor_match AND read_match
      AND ($8::timestamptz IS NULL OR
           (created_at, id) < ($8::timestamptz, $9::uuid))
    ORDER BY created_at DESC, id DESC
    LIMIT $10::int
)
SELECT i.*, selected.issue_status, selected.issue_priority,
       COALESCE(anchor.comment_id, '')::text AS comment_id
FROM selected
JOIN inbox_item i ON i.id = selected.id
LEFT JOIN LATERAL (
    SELECT a.details->>'comment_id' AS comment_id
    FROM inbox_item a
    WHERE i.issue_id IS NOT NULL AND a.issue_id = i.issue_id
      AND a.workspace_id = i.workspace_id AND a.recipient_type = i.recipient_type
      AND a.recipient_id = i.recipient_id AND a.archived = true
      AND NULLIF(a.details->>'comment_id', '') IS NOT NULL
    ORDER BY a.created_at DESC, a.id DESC LIMIT 1
) anchor ON true
ORDER BY i.created_at DESC, i.id DESC;

