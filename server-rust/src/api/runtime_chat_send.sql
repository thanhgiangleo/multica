INSERT INTO agent_task_queue(id,agent_id,runtime_id,issue_id,status,priority,chat_session_id,
    initiator_user_id,originator_user_id,accountable_user_id,force_fresh_session,
    originator_source,trigger_evidence_kind,trigger_evidence_ref_id,chat_input_task_id)
SELECT $1,$2,$3,NULL,'queued',2,$4,$5,$5,$5,false,'direct_human','chat',$4,$1
WHERE lock_task_owner_rows($2,NULL,$3)
RETURNING created_at
