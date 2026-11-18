SELECT t.id,t.status,t.created_at,t.started_at,t.completed_at,
 COALESCE(t.context->'wakeup_checkin'->>'note','')::text AS checkin_note,
 COALESCE((SELECT array_agg(DISTINCT f->>'event_type') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(t.context->'wakeup_evidence'->'facts')='array' THEN t.context->'wakeup_evidence'->'facts' ELSE '[]'::jsonb END) f),'{}')::text[] AS triggers,
 EXISTS(SELECT 1 FROM comment c WHERE c.issue_id=t.issue_id AND c.source_task_id=t.id)::bool AS commented
FROM agent_task_queue t WHERE t.context->>'wakeup_id'= $1::text AND t.issue_id= $2
ORDER BY t.created_at DESC,t.id DESC LIMIT 10;

