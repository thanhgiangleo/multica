-- Frozen Go ArchivedInboxFacets, with sqlc named parameters mapped to positional parameters.
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
           (cardinality($3::text[]) = 0 OR issue_status = ANY($3::text[])) AS status_match,
           (cardinality($4::text[]) = 0 OR issue_priority = ANY($4::text[])) AS priority_match,
           (cardinality($5::text[]) = 0 OR actor = ANY($5::text[])) AS actor_match,
           (NOT $6::boolean OR NOT read) AS read_match
    FROM projected
)
SELECT 'statuses'::text AS dimension, issue_status::text AS key,
       count(*) FILTER (WHERE priority_match AND actor_match AND read_match)::bigint AS count
FROM matched WHERE issue_status IS NOT NULL GROUP BY issue_status
UNION ALL
SELECT 'priorities'::text, issue_priority::text,
       count(*) FILTER (WHERE status_match AND actor_match AND read_match)::bigint
FROM matched WHERE issue_priority IS NOT NULL GROUP BY issue_priority
UNION ALL
SELECT 'actors'::text, actor::text,
       count(*) FILTER (WHERE status_match AND priority_match AND read_match)::bigint
FROM matched WHERE actor IS NOT NULL GROUP BY actor
UNION ALL
SELECT 'unread'::text, 'unread'::text,
       count(*) FILTER (WHERE status_match AND priority_match AND actor_match AND NOT read)::bigint
FROM matched;
