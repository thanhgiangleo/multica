SELECT DATE(bucket_hour AT TIME ZONE $2::text) AS date,
       LOWER(provider) AS provider, model,
       SUM(input_tokens)::bigint AS input_tokens,
       SUM(output_tokens)::bigint AS output_tokens,
       SUM(cache_read_tokens)::bigint AS cache_read_tokens,
       SUM(cache_write_tokens)::bigint AS cache_write_tokens,
       SUM(cost_usd_ticks)::bigint AS cost_usd_ticks,
       SUM(COALESCE(uncosted_input_tokens, input_tokens))::bigint AS uncosted_input_tokens,
       SUM(COALESCE(uncosted_output_tokens, output_tokens))::bigint AS uncosted_output_tokens,
       SUM(COALESCE(uncosted_cache_read_tokens, cache_read_tokens))::bigint AS uncosted_cache_read_tokens,
       SUM(COALESCE(uncosted_cache_write_tokens, cache_write_tokens))::bigint AS uncosted_cache_write_tokens
FROM task_usage_hourly
WHERE runtime_id = $1 AND bucket_hour >= $3::timestamptz
GROUP BY DATE(bucket_hour AT TIME ZONE $2::text), LOWER(provider), model
ORDER BY DATE(bucket_hour AT TIME ZONE $2::text) DESC, LOWER(provider), model
