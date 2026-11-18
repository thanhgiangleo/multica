-- Frozen GetTaskTokenByHash, including both channel guest generation fences.
SELECT t.* FROM task_token AS t
WHERE t.token_hash = $1 AND t.expires_at > now()
  AND EXISTS (
    SELECT 1 FROM "user" AS u
    WHERE u.id = t.user_id
      AND (NOT u.zbs_is_shadow OR EXISTS (
        SELECT 1 FROM channel_task_delivery AS d
        WHERE d.task_id = t.task_id AND d.zbs_guest_shadow_user_id = u.id
      ))
  )
  AND (
    NOT EXISTS (
      SELECT 1 FROM channel_task_delivery AS d
      WHERE d.task_id = t.task_id
        AND (d.zbs_guest_shadow_user_id IS NOT NULL OR d.zbs_guest_grant_generation IS NOT NULL)
    )
    OR EXISTS (
      SELECT 1 FROM channel_task_delivery AS d
      JOIN zbs_telegram_guest_grant AS g ON g.installation_id = d.installation_id
        AND g.shadow_user_id = d.zbs_guest_shadow_user_id
        AND g.grant_generation = d.zbs_guest_grant_generation
      JOIN channel_installation AS i ON i.id = g.installation_id
      JOIN "user" AS u ON u.id = g.shadow_user_id
      WHERE d.task_id = t.task_id
        AND d.channel_type = 'telegram'
        AND g.revoked_at IS NULL
        AND i.status = 'active' AND i.channel_type = 'telegram'
        AND i.workspace_id = g.workspace_id AND i.agent_id = g.agent_id
        AND u.zbs_is_shadow
    )
    OR EXISTS (
      SELECT 1 FROM channel_task_delivery AS d
      JOIN zbs_zalo_guest_grant AS g ON g.installation_id = d.installation_id
        AND g.shadow_user_id = d.zbs_guest_shadow_user_id
        AND g.grant_generation = d.zbs_guest_grant_generation
      JOIN channel_installation AS i ON i.id = g.installation_id
      JOIN "user" AS u ON u.id = g.shadow_user_id
      WHERE d.task_id = t.task_id
        AND d.channel_type = 'zalo'
        AND g.revoked_at IS NULL
        AND i.status = 'active' AND i.channel_type = 'zalo'
        AND i.workspace_id = g.workspace_id AND i.agent_id = g.agent_id
        AND u.zbs_is_shadow
    )
  );
