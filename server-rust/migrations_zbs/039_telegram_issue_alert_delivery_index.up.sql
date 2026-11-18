CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS zbs_telegram_issue_alert_delivery_key_idx ON zbs_telegram_issue_alert_delivery (rule_id, activity_key);
