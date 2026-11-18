CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_issue_alert_activity_idx ON activity_log (workspace_id, created_at, id) WHERE action = 'status_changed' AND details->>'to' = 'blocked';
