CREATE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_issue_alert_pending_idx ON zbs_telegram_issue_alert_delivery (next_attempt_at) WHERE status = 'pending';
