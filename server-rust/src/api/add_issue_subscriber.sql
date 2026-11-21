-- name: AddIssueSubscriber :execrows
INSERT INTO issue_subscriber (issue_id, user_type, user_id, reason)
VALUES ($1, $2, $3, $4)
ON CONFLICT (issue_id, user_type, user_id) DO UPDATE
SET reason = EXCLUDED.reason
WHERE issue_subscriber.unsubscribed_at IS NULL
  AND issue_subscriber.reason = 'delegated'
  AND EXCLUDED.reason <> 'delegated'
