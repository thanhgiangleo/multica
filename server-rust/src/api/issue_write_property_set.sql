-- Frozen SetIssuePropertyValue, positional parameters instead of sqlc names.
UPDATE issue
SET properties = jsonb_set(properties, ARRAY[$3::text], $4::jsonb, true),
    revision = revision + CASE WHEN properties -> $3::text IS DISTINCT FROM $4::jsonb THEN 1 ELSE 0 END,
    last_activity_at = CASE
        WHEN properties -> $3::text IS DISTINCT FROM $4::jsonb
        THEN GREATEST(COALESCE(last_activity_at, updated_at), now())
        ELSE last_activity_at
    END,
    updated_at = CASE WHEN properties -> $3::text IS DISTINCT FROM $4::jsonb THEN now() ELSE updated_at END
WHERE id = $1 AND workspace_id = $2
RETURNING properties, revision;
