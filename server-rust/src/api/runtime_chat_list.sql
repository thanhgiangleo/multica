SELECT cs.*, (cs.creator_id <> $2)::boolean AS read_only,
       CASE WHEN cs.status='archived' OR cs.creator_id <> $2 THEN 0
            ELSE (SELECT count(*) FROM chat_message m WHERE m.chat_session_id=cs.id
                  AND m.role='assistant' AND m.created_at>cs.last_read_at) END::int AS unread_count,
       COALESCE(lm.content,'') AS last_message_content,
       COALESCE(lm.role,'') AS last_message_role,
       lm.created_at AS last_message_at,
       lm.failure_reason AS last_message_failure_reason,
       COALESCE(lm.message_kind,'') AS last_message_kind
FROM chat_session cs
JOIN agent a ON a.id=cs.agent_id AND a.workspace_id=cs.workspace_id AND a.kind='user'
LEFT JOIN LATERAL (
    SELECT content,role,created_at,failure_reason,message_kind FROM chat_message m
    WHERE m.chat_session_id=cs.id AND m.message_kind!='channel_command'
    ORDER BY m.created_at DESC LIMIT 1
) lm ON true
WHERE cs.workspace_id=$1
  AND ((cs.creator_id=$2 AND (NOT cs.zbs_has_guest_history OR a.owner_id=$2))
       OR (cs.zbs_source_channel_type IN ('telegram','zalo') AND a.owner_id=$2 AND $4::bool))
  AND ($3::bool OR cs.status='active')
  AND (cs.explicitly_created_at IS NOT NULL OR lm.created_at IS NOT NULL)
  AND ($4::bool OR (cs.creator_id=$2 AND NOT cs.zbs_has_guest_history AND cs.id=$5))
  AND ($6::bool OR a.owner_id=$2 OR (a.permission_mode='public_to' AND EXISTS (
       SELECT 1 FROM agent_invocation_target t WHERE t.agent_id=a.id
         AND ((t.target_type='workspace' AND t.target_id=$1) OR
              (t.target_type='member' AND t.target_id=$2)))))
ORDER BY (cs.pinned_at IS NOT NULL) DESC, cs.pinned_at DESC,
         COALESCE(lm.created_at,cs.updated_at) DESC
