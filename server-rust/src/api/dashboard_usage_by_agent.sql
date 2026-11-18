-- ListDashboardUsageByAgent from frozen server/pkg/db/queries/task_usage.sql.
SELECT agent_id, LOWER(provider) AS provider, model,
    SUM(input_tokens)::bigint AS input_tokens,
    SUM(output_tokens)::bigint AS output_tokens,
    SUM(cache_read_tokens)::bigint AS cache_read_tokens,
    SUM(cache_write_tokens)::bigint AS cache_write_tokens,
    SUM(cost_usd_ticks)::bigint AS cost_usd_ticks,
    SUM(COALESCE(uncosted_input_tokens, input_tokens))::bigint AS uncosted_input_tokens,
    SUM(COALESCE(uncosted_output_tokens, output_tokens))::bigint AS uncosted_output_tokens,
    SUM(COALESCE(uncosted_cache_read_tokens, cache_read_tokens))::bigint AS uncosted_cache_read_tokens,
    SUM(COALESCE(uncosted_cache_write_tokens, cache_write_tokens))::bigint AS uncosted_cache_write_tokens,
    SUM(task_count)::int AS task_count
FROM task_usage_hourly
WHERE workspace_id = $1
    AND bucket_hour >= $2::timestamptz
    AND ($3::uuid IS NULL OR project_id = $3::uuid)
GROUP BY agent_id, LOWER(provider), model
ORDER BY agent_id, LOWER(provider), model
