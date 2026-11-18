WITH usage AS (SELECT
        COALESCE(SUM(tu.input_tokens), 0)::bigint AS total_input_tokens,
        COALESCE(SUM(tu.output_tokens), 0)::bigint AS total_output_tokens,
        COALESCE(SUM(tu.cache_read_tokens), 0)::bigint AS total_cache_read_tokens,
        COALESCE(SUM(tu.cache_write_tokens), 0)::bigint AS total_cache_write_tokens,
        COALESCE(SUM(tu.cost_usd_ticks), 0)::bigint AS total_cost_usd_ticks,
        COALESCE(SUM(tu.input_tokens)       FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_input_tokens,
        COALESCE(SUM(tu.output_tokens)      FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_output_tokens,
        COALESCE(SUM(tu.cache_read_tokens)  FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_cache_read_tokens,
        COALESCE(SUM(tu.cache_write_tokens) FILTER (WHERE tu.cost_usd_ticks IS NULL), 0)::bigint AS uncosted_cache_write_tokens,
        COUNT(DISTINCT tu.task_id)::int AS task_count
    FROM task_usage tu
    JOIN agent_task_queue atq ON atq.id = tu.task_id
    WHERE atq.issue_id = $1
), terminal_runs AS (
    SELECT
        COUNT(*)::int AS terminal_task_count,
        COUNT(*) FILTER (WHERE EXISTS (
            SELECT 1 FROM task_usage tu WHERE tu.task_id = atq.id
        ))::int AS metered_task_count
    FROM agent_task_queue atq
    WHERE atq.issue_id = $1
      AND atq.status IN ('completed', 'failed', 'cancelled')
      AND atq.started_at IS NOT NULL
      AND atq.completed_at IS NOT NULL
)
SELECT
    usage.*,
    terminal_runs.terminal_task_count,
    terminal_runs.metered_task_count,
    (terminal_runs.terminal_task_count - terminal_runs.metered_task_count)::int AS unreported_task_count
FROM usage
CROSS JOIN terminal_runs;
