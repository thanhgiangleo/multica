-- name: ListWorkspaceWakeupSummaryRows :many
WITH ranked AS (
 SELECT w.issue_id,w.id,w.agent_id,a.name AS agent_name,w.kind,w.mode,w.event_types,w.filter_actor_type,
 (CASE WHEN actor_agent.id IS NOT NULL OR actor_member.user_id IS NOT NULL THEN w.filter_actor_id END)::uuid AS filter_actor_id,
 COALESCE(actor_agent.name,actor_user.name,'')::text AS filter_actor_name,
  (CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue ft JOIN agent fa ON fa.id=ft.agent_id AND fa.workspace_id=w.workspace_id
   WHERE ft.id=w.filter_task_id AND ft.issue_id=w.issue_id AND fa.id=ANY($1::uuid[])) THEN w.filter_task_id END)::uuid AS filter_task_id,
  source.name AS filter_agent_name,w.interval_seconds,w.cron_expression,w.timezone,w.next_fire_at,w.condition,
  count(*) OVER(PARTITION BY w.issue_id) AS active_count,
  count(*) FILTER(WHERE w.kind='event') OVER(PARTITION BY w.issue_id) AS event_count,
  row_number() OVER(PARTITION BY w.issue_id ORDER BY w.next_fire_at NULLS LAST,w.created_at,w.id) AS rank
 FROM issue_wakeup w
 JOIN issue i ON i.id=w.issue_id AND i.workspace_id=w.workspace_id
 JOIN agent a ON a.id=w.agent_id AND a.workspace_id=w.workspace_id
 LEFT JOIN agent actor_agent ON w.filter_actor_type='agent' AND actor_agent.id=w.filter_actor_id AND actor_agent.workspace_id=w.workspace_id AND actor_agent.id=ANY($1::uuid[])
LEFT JOIN member actor_member ON w.filter_actor_type='member' AND actor_member.user_id=w.filter_actor_id AND actor_member.workspace_id=w.workspace_id
LEFT JOIN "user" actor_user ON actor_user.id=actor_member.user_id
LEFT JOIN agent source ON source.id=w.filter_agent_id AND source.workspace_id=w.workspace_id AND source.id=ANY($1::uuid[])
 WHERE w.workspace_id= $2 AND w.enabled
  AND i.status NOT IN ('done','cancelled')
  AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=i.status AND s.category IN ('done','closed'))
)
SELECT issue_id,id,agent_id,agent_name,kind,mode,event_types,filter_actor_type,filter_actor_id,filter_actor_name,filter_task_id,filter_agent_name,interval_seconds,cron_expression,timezone,next_fire_at,condition,active_count,event_count
FROM ranked WHERE rank<=3 ORDER BY issue_id,rank
