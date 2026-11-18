-- Frozen Go anchor-to-root and newest reply keyset, scoped on every recursion.
WITH RECURSIVE root_of AS (
    SELECT c.id, c.parent_id FROM comment c
    WHERE c.id=$1 AND c.issue_id=$2 AND c.workspace_id=$3
    UNION
    SELECT p.id, p.parent_id FROM comment p JOIN root_of r ON p.id=r.parent_id
    WHERE p.issue_id=$2 AND p.workspace_id=$3
), thread_root AS (
    SELECT id FROM root_of WHERE parent_id IS NULL LIMIT 1
), descendants AS (
    SELECT c.* FROM comment c JOIN thread_root tr ON c.id=tr.id
    WHERE c.issue_id=$2 AND c.workspace_id=$3
    UNION
    SELECT c.* FROM comment c JOIN descendants d ON c.parent_id=d.id
    WHERE c.issue_id=$2 AND c.workspace_id=$3
), reply_page AS (
    SELECT d.* FROM descendants d
    WHERE d.id NOT IN (SELECT id FROM thread_root)
      AND ($4::timestamptz IS NULL OR (d.created_at,d.id)<($4,$5::uuid))
    ORDER BY d.created_at DESC, d.id DESC LIMIT $6::int4
)
SELECT * FROM (
    SELECT d.* FROM descendants d JOIN thread_root tr ON d.id=tr.id
    UNION ALL
    SELECT * FROM reply_page
) combined ORDER BY created_at ASC, id ASC;
