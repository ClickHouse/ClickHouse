-- A plan fragment carries scalar subquery values as constants, so only the shard scalars are sent to replicas.

DROP TABLE IF EXISTS t_pr_scalars;
DROP TABLE IF EXISTS t_pr_scalars_d;

CREATE TABLE t_pr_scalars (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_scalars SELECT number FROM numbers(100000);
CREATE TABLE t_pr_scalars_d AS t_pr_scalars ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_pr_scalars);

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_prefer_local_replica = 0;

SELECT sum(x) FROM t_pr_scalars WHERE x < length((SELECT groupArray(number) FROM numbers(1000000)))
SETTINGS log_comment = '05871_big_scalar';

SELECT sum(x) FROM t_pr_scalars WHERE has((SELECT groupArray(number) FROM numbers(10)), x);

SELECT shardNum() AS s, count() FROM t_pr_scalars_d GROUP BY s ORDER BY s SETTINGS prefer_localhost_replica = 0;

SYSTEM FLUSH LOGS query_log;

-- The 1M-element array is not sent to the replicas.
SELECT ProfileEvents['NetworkSendBytes'] < 1000000 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND is_initial_query AND log_comment = '05871_big_scalar';

DROP TABLE t_pr_scalars_d;
DROP TABLE t_pr_scalars;
