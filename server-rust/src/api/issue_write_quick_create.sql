INSERT INTO agent_task_queue (
    agent_id, runtime_id, issue_id, status, priority, context,
    originator_user_id, accountable_user_id, runtime_mcp_overlay,
    runtime_connected_apps, originator_source,
    trigger_evidence_kind, trigger_evidence_ref_id, id, delegated_from_task_id
)
SELECT $1, $2, NULL, 'queued', 3, $3, $4, $4, $5, $6,
       CASE WHEN $8::uuid IS NULL THEN 'direct_human' ELSE 'delegation' END,
       NULL, NULL, $7, $8
WHERE lock_task_owner_rows($1, NULL, $2)
RETURNING id
