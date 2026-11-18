-- Guest runs freeze the grant they were admitted under beside the delivery
-- route. A shadow user can be revoked and re-paired (same UUID, new
-- generation) while an old run is still queued; delivery and claim compare
-- this pair against the live grant and fail closed on any mismatch. Both
-- columns are NULL for member runs. No index: reads go through task_id.
ALTER TABLE channel_task_delivery
    ADD COLUMN IF NOT EXISTS zbs_guest_shadow_user_id UUID,
    ADD COLUMN IF NOT EXISTS zbs_guest_grant_generation BIGINT;

ALTER TABLE channel_task_delivery DROP CONSTRAINT IF EXISTS zbs_channel_task_delivery_guest_pair_check;
ALTER TABLE channel_task_delivery ADD CONSTRAINT zbs_channel_task_delivery_guest_pair_check
    CHECK ((zbs_guest_shadow_user_id IS NULL) = (zbs_guest_grant_generation IS NULL)) NOT VALID;
