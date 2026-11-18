SELECT i.* FROM issue i
WHERE i.workspace_id=$1
  AND i.status <> ALL(ARRAY['done','cancelled']::text[] || ARRAY(
      SELECT key FROM issue_status WHERE workspace_id=$1 AND category IN ('done','closed')
  ))
  AND i.triage_state IS NULL
  AND i.project_id IS NOT DISTINCT FROM $2::uuid
  AND i.parent_issue_id IS NOT DISTINCT FROM $3::uuid
  AND lower(btrim(regexp_replace(i.title,'[[:space:]]+',' ','g')))=$4
ORDER BY i.created_at ASC
LIMIT 1
