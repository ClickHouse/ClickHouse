-- Every replica keeps its own `n` groups in the `GROUP BY` top-K heap, so each one ships about as much as the
-- single-node aggregation outputs. The cost model must not divide that output between the replicas, the same as
-- for `LIMIT n`.

DROP TABLE IF EXISTS t_05446;
CREATE TABLE t_05446 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05446 SELECT intHash64(number) % 100000, number FROM numbers(1000000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1, query_plan_enable_optimizations = 1, enable_group_by_top_k_optimization = 1,
    max_rows_to_group_by = 0, exact_rows_before_limit = 0, query_plan_max_limit_for_top_k_optimization = 1000;
-- With a single thread the plans of the `ORDER BY` query do not match.
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;

-- Twice each: the first run only collects statistics, the second is priced.
SELECT k, count() FROM t_05446 GROUP BY k LIMIT 10 FORMAT Null SETTINGS log_comment = '05446_no_order_by';
SELECT k, count() FROM t_05446 GROUP BY k LIMIT 10 FORMAT Null SETTINGS log_comment = '05446_no_order_by';
SELECT k, count() FROM t_05446 GROUP BY k ORDER BY k LIMIT 10 FORMAT Null SETTINGS log_comment = '05446_order_by';
SELECT k, count() FROM t_05446 GROUP BY k ORDER BY k LIMIT 10 FORMAT Null SETTINGS log_comment = '05446_order_by';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log, text_log;

-- The divisor of the output bytes in `The applied formula: in / threads ? (in / threads + out / divisor)`.
SELECT q.log_comment, extract(t.message, '\\+ \\d+ / (\\d+)\\)') AS output_divisor
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND startsWith(log_comment, '05446_') AND (type = 'QueryFinish')
) AS q ON t.query_id = q.query_id
WHERE (t.event_date >= yesterday()) AND (t.event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (t.logger_name = 'AutoParallelReplicas') AND (t.message LIKE 'The applied formula%')
ORDER BY q.log_comment
SETTINGS max_rows_to_read = 0; -- system.text_log can be really big

DROP TABLE t_05446;
