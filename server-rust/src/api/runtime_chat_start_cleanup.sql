-- Frozen Go TaskWaitingForProcessCleanup and TaskBlockedByProcessCleanup.
SELECT EXISTS (
    SELECT 1 FROM agent_task_queue candidate
    JOIN agent_task_queue stopping ON stopping.agent_id=candidate.agent_id AND stopping.runtime_id=candidate.runtime_id
    WHERE candidate.id=$1 AND candidate.status IN ('dispatched','waiting_local_directory')
      AND stopping.status='cancelled' AND stopping.result->>'process_cleanup_pending'='true'
      AND stopping.completed_at > now()-interval '30 seconds'
) AS waiting,
EXISTS (
    SELECT 1 FROM agent_task_queue candidate
    JOIN LATERAL (
        SELECT CASE WHEN unsafe.status='cancelled' AND unsafe.result->>'process_cleanup_pending'='true'
                    THEN unsafe.completed_at+interval '30 seconds'
                    ELSE COALESCE(unsafe.completed_at,unsafe.created_at) END AS completed_at
        FROM agent_task_queue unsafe
        WHERE unsafe.agent_id=candidate.agent_id AND unsafe.runtime_id=candidate.runtime_id
          AND (unsafe.failure_reason='cleanup_unconfirmed'
            OR (unsafe.status='cancelled' AND unsafe.result->>'process_cleanup_pending'='true'
                AND unsafe.completed_at <= now()-interval '30 seconds'))
        ORDER BY unsafe.completed_at DESC,unsafe.id DESC LIMIT 1
    ) unsafe ON true
    WHERE candidate.id=$1 AND candidate.status IN ('queued','dispatched','waiting_local_directory')
      AND NOT EXISTS (
          SELECT 1 FROM agent_task_queue manual
          WHERE manual.agent_id=candidate.agent_id AND manual.runtime_id=candidate.runtime_id
            AND manual.rerun_of_task_id IS NOT NULL AND manual.retry_of_task_id IS NULL
            AND manual.created_at > unsafe.completed_at
            AND (manual.id=candidate.id OR manual.status='completed')
      )
) AS blocked;
