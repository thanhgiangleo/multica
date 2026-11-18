WITH locked_task AS MATERIALIZED (
    SELECT t.id, t.issue_id, t.agent_id, t.trigger_comment_id
    FROM agent_task_queue t
    JOIN agent_runtime r ON r.id = t.runtime_id
    JOIN task_supplement_capability cap ON cap.task_id = t.id
    WHERE t.id = $1
      AND t.issue_id = $2
      AND r.workspace_id = $3
      AND t.status = 'running'
      AND cap.capability = 'task-supplement-v1'
      AND r.provider IN ('codex', 'claude', 'grok', 'cursor', 'kiro')
    FOR UPDATE OF t
), touched_issue AS (
    UPDATE issue i SET
        updated_at = now(),
        revision = revision + 1,
        last_activity_at = GREATEST(COALESCE(last_activity_at, updated_at), now())
    FROM locked_task t
    WHERE i.id = t.issue_id AND i.workspace_id = $3
    RETURNING i.id, i.workspace_id, i.revision
), inserted_comment AS (
    INSERT INTO comment (
        issue_id, workspace_id, author_type, author_id, content, type, parent_id
    )
    SELECT i.id, i.workspace_id, 'member', $4, $5, 'comment', t.trigger_comment_id
    FROM touched_issue i
    JOIN locked_task t ON t.issue_id = i.id
    RETURNING *
), inserted_supplement AS (
    INSERT INTO task_supplement (
        task_id, workspace_id, issue_id, comment_id, author_id,
        client_request_id, status
    )
    SELECT t.id, i.workspace_id, i.id, c.id, $4, $6, 'pending'
    FROM locked_task t
    JOIN touched_issue i ON i.id = t.issue_id
    JOIN inserted_comment c ON c.issue_id = i.id
    RETURNING *
)
SELECT c.id, i.revision AS issue_revision
FROM inserted_comment c
JOIN touched_issue i ON i.id = c.issue_id
JOIN inserted_supplement s ON s.comment_id = c.id
