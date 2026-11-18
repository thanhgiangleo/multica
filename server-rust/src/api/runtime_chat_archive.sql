WITH target AS (
    SELECT id FROM channel_chat_session_binding WHERE chat_session_id=$1
), cleared_deliveries AS (
    DELETE FROM channel_task_delivery WHERE binding_id IN (SELECT id FROM target)
), cleared_outbound AS (
    DELETE FROM channel_outbound_message WHERE binding_id IN (SELECT id FROM target)
), cleared_reply_deliveries AS (
    DELETE FROM channel_reply_delivery WHERE binding_id IN (SELECT id FROM target)
), deleted_binding AS (
    DELETE FROM channel_chat_session_binding WHERE id IN (SELECT id FROM target)
)
DELETE FROM channel_chat_context_generation WHERE chat_session_id=$1
