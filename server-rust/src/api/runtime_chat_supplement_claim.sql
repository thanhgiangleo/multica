-- Frozen Go ClaimNextTaskSupplement. $1 task, $2 author-aware daemon.
WITH next AS MATERIALIZED (
    SELECT s.comment_id, s.task_id
    FROM task_supplement s
    JOIN agent_task_queue t ON t.id = s.task_id
    JOIN task_supplement_capability cap ON cap.task_id = t.id
    WHERE s.task_id = $1
      AND s.status = 'pending'
      AND t.status = 'running'
      AND cap.capability = 'task-supplement-v1'
      -- A daemon that cannot present an agent's message as an agent's never
      -- receives one: it stays pending until the turn ends, and completion
      -- replays it as an ordinary mention.
      AND ($2::bool OR NOT EXISTS (
          SELECT 1 FROM comment ac WHERE ac.id = s.comment_id AND ac.author_type = 'agent'
      ))
    ORDER BY s.created_at, s.comment_id
    FOR UPDATE OF s SKIP LOCKED
    LIMIT 1
), claimed AS (
    UPDATE task_supplement s
    SET status = 'delivering',
        attempt_count = attempt_count + 1,
        failure_reason = NULL,
        updated_at = now()
    FROM next
    WHERE s.comment_id = next.comment_id
      AND s.task_id = next.task_id
    RETURNING s.*
)
SELECT claimed.comment_id, claimed.attempt_count, c.content, c.author_type,
       COALESCE(NULLIF(btrim(COALESCE(u.name, a.name)), ''),
                CASE WHEN c.author_type = 'agent' THEN 'an agent' ELSE 'a user' END)::text AS author_name
FROM claimed
JOIN comment c ON c.id = claimed.comment_id
LEFT JOIN "user" u ON u.id = claimed.author_id AND c.author_type = 'member'
LEFT JOIN agent a ON a.id = claimed.author_id AND c.author_type = 'agent';
