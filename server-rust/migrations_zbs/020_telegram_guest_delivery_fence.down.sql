ALTER TABLE channel_task_delivery DROP CONSTRAINT IF EXISTS zbs_channel_task_delivery_guest_pair_check;
ALTER TABLE channel_task_delivery
    DROP COLUMN IF EXISTS zbs_guest_grant_generation,
    DROP COLUMN IF EXISTS zbs_guest_shadow_user_id;
