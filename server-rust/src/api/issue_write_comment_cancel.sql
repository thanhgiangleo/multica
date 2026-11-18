UPDATE agent_task_queue AS task
SET status='cancelled', completed_at=now(), prepare_lease_expires_at=NULL,
    cancelled_by_type='system',cancelled_by_id=NULL,cancelled_by_name=NULL,
    context=COALESCE(task.context,'{}'::jsonb)||jsonb_build_object('comment_change_cancelled_task_id',task.id::text)
FROM comment c JOIN issue i ON i.id=c.issue_id AND i.workspace_id=c.workspace_id
WHERE c.id=$1 AND c.workspace_id=$2 AND task.issue_id=i.id
  AND (task.trigger_comment_id=c.id OR c.id=ANY(task.coalesced_comment_ids))
  AND task.status IN ('queued','dispatched','running','waiting_local_directory','deferred')
RETURNING task.id,task.agent_id,task.trigger_comment_id,task.coalesced_comment_ids
