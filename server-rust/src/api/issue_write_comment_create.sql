WITH touched_issue AS (
    UPDATE issue SET updated_at=now(), revision=revision+1,
        last_activity_at=GREATEST(COALESCE(last_activity_at,updated_at),now())
    WHERE id=$1 AND workspace_id=$2
    RETURNING id,workspace_id,revision
), inserted_comment AS (
    INSERT INTO comment(id,issue_id,workspace_id,author_type,author_id,content,type,parent_id,source_task_id,suppressed_agent_ids)
    SELECT $3,ti.id,ti.workspace_id,$4,$5,$6,$7,$8,$9,$10
    FROM touched_issue ti
    RETURNING *
)
SELECT c.*,ti.revision AS issue_revision
FROM inserted_comment c JOIN touched_issue ti ON ti.id=c.issue_id
