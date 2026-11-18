-- Frozen DeleteIssuePropertyValue.
UPDATE issue
SET properties = properties - $3::text,
    revision = revision + CASE WHEN properties ? $3::text THEN 1 ELSE 0 END,
    last_activity_at = CASE
        WHEN properties ? $3::text
        THEN GREATEST(COALESCE(last_activity_at, updated_at), now())
        ELSE last_activity_at
    END,
    updated_at = CASE WHEN properties ? $3::text THEN now() ELSE updated_at END
WHERE id = $1 AND workspace_id = $2
RETURNING properties, revision;
