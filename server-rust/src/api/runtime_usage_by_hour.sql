SELECT EXTRACT(HOUR FROM tu.created_at AT TIME ZONE $2::text)::int AS hour,
       tu.model,
       SUM(tu.input_tokens)::bigint AS input_tokens,
       SUM(tu.output_tokens)::bigint AS output_tokens,
       SUM(tu.cache_read_tokens)::bigint AS cache_read_tokens,
       SUM(tu.cache_write_tokens)::bigint AS cache_write_tokens,
       COALESCE(SUM(tu.cost_usd_ticks), 0)::bigint AS cost_usd_ticks,
       COALESCE(SUM(tu.input_tokens) FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_input_tokens,
       COALESCE(SUM(tu.output_tokens) FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_output_tokens,
       COALESCE(SUM(tu.cache_read_tokens) FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_cache_read_tokens,
       COALESCE(SUM(tu.cache_write_tokens) FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_cache_write_tokens,
       COUNT(DISTINCT tu.task_id)::int AS task_count
FROM task_usage tu
JOIN agent_task_queue atq ON atq.id = tu.task_id
WHERE atq.runtime_id = $1 AND tu.created_at >= $3::timestamptz
GROUP BY EXTRACT(HOUR FROM tu.created_at AT TIME ZONE $2::text), tu.model
ORDER BY hour, tu.model
