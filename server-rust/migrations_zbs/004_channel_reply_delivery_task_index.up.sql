-- The Zalo outbound recovery sweep asks whether the attempt holding a reply
-- turn has settled it (ListPendingChannelReplyTasks).
CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_idx_channel_reply_delivery_task ON channel_reply_delivery (task_id);
