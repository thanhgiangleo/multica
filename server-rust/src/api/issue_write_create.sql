INSERT INTO issue(
  id, workspace_id, title, description, status, priority,
  assignee_type, assignee_id, creator_type, creator_id, parent_issue_id,
  position, start_date, due_date, number, project_id, stage,
  properties, last_activity_at, origin_type, origin_id
) VALUES (
  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
  $20::jsonb,now(),$18,$19
) RETURNING *
