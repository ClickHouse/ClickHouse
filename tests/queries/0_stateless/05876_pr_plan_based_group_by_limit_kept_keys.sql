-- The kept-keys cutoff of a trivial `GROUP BY ... LIMIT` with aggregate functions is sound only when one node runs
-- both stages of the aggregation. Plan-based parallel replicas split the aggregation after planning, so the local
-- replica must not apply the cutoff: it would drop its rows of the keys it did not keep, and the query could return
-- such a key with an incomplete count. https://github.com/ClickHouse/ClickHouse/issues/134583

DROP TABLE IF EXISTS t_kept_keys;

CREATE TABLE t_kept_keys (a UInt32) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;
INSERT INTO t_kept_keys SELECT number % 1000 FROM numbers(100000);

-- The `GROUP BY` top-K heap takes precedence over the cutoff, so it is disabled.
SET enable_group_by_top_k_optimization = 0, optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 0;
SET max_threads = 2;

-- Without parallel replicas the cutoff applies.
SELECT count(), min(c), max(c) FROM (SELECT a, count() AS c FROM t_kept_keys GROUP BY a LIMIT 3)
SETTINGS enable_parallel_replicas = 0, log_comment = '05876_no_parallel_replicas';

SELECT count(), min(c), max(c) FROM (SELECT a, count() AS c FROM t_kept_keys GROUP BY a LIMIT 3)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0,
    parallel_replicas_local_plan = 1, log_comment = '05876_plan_based';

SYSTEM FLUSH LOGS query_log;

-- The local replica runs in the process of the initiator, so its aggregation is counted in the query of the initiator.
SELECT log_comment, ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
    AND type = 'QueryFinish' AND is_initial_query AND log_comment IN ('05876_no_parallel_replicas', '05876_plan_based')
ORDER BY log_comment;

DROP TABLE t_kept_keys;
