-- Frozen Go ListIssueWakeups with named binds replaced by positional binds.
SELECT w.id,w.workspace_id,w.issue_id,w.agent_id,w.created_by,w.source_task_id,w.parent_comment_id,w.instruction,
 w.kind,w.mode,w.event_types,w.filter_actor_type,
 (CASE WHEN actor_agent.id IS NOT NULL OR actor_member.user_id IS NOT NULL THEN w.filter_actor_id END)::uuid AS filter_actor_id,
 COALESCE(actor_agent.name,actor_user.name,'')::text AS filter_actor_name,
 (CASE WHEN source.id IS NOT NULL THEN w.filter_agent_id END)::uuid AS filter_agent_id,
 (CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue ft JOIN agent fa ON fa.id=ft.agent_id AND fa.workspace_id=w.workspace_id
  WHERE ft.id=w.filter_task_id AND ft.issue_id=w.issue_id AND fa.id=ANY($1::uuid[])) THEN w.filter_task_id END)::uuid AS filter_task_id,
 w.interval_seconds,w.cron_expression,w.timezone,w.next_fire_at,w.enabled,w.disabled_at,w.revision,
 w.last_task_id,w.last_error,w.created_at,w.updated_at,a.name AS agent_name,source.name AS filter_agent_name,t.status AS last_task_status,
 w.expires_at,w.expiry_seconds,w.on_timeout,w.timed_out_at,
 w.condition,w.max_fires,w.fire_count,w.paused_reason,
 (w.source_task_id IS NOT NULL)::bool AS created_by_agent,creator.name AS created_by_name,
 (CASE WHEN creator_agent.id IS NOT NULL THEN creator_agent.id END)::uuid AS source_agent_id,creator_agent.name AS source_agent_name
FROM issue_wakeup w JOIN agent a ON a.id=w.agent_id AND a.workspace_id=w.workspace_id
LEFT JOIN "user" creator ON creator.id=w.created_by
LEFT JOIN agent_task_queue creator_task ON creator_task.id=w.source_task_id
LEFT JOIN agent creator_agent ON creator_agent.id=creator_task.agent_id AND creator_agent.workspace_id=w.workspace_id AND creator_agent.id=ANY($1::uuid[])
LEFT JOIN agent actor_agent ON w.filter_actor_type='agent' AND actor_agent.id=w.filter_actor_id AND actor_agent.workspace_id=w.workspace_id AND actor_agent.id=ANY($1::uuid[])
LEFT JOIN member actor_member ON w.filter_actor_type='member' AND actor_member.user_id=w.filter_actor_id AND actor_member.workspace_id=w.workspace_id
LEFT JOIN "user" actor_user ON actor_user.id=actor_member.user_id
LEFT JOIN agent source ON source.id=w.filter_agent_id AND source.workspace_id=w.workspace_id AND source.id=ANY($1::uuid[])
LEFT JOIN agent_task_queue t ON t.id=w.last_task_id AND t.issue_id=w.issue_id AND t.agent_id=w.agent_id
WHERE w.workspace_id= $2 AND w.issue_id= $3 ORDER BY w.created_at,w.id;
