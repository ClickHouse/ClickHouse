-- Random settings limits: optimize_use_projections=(1, None); optimize_use_implicit_projections=(1, None); parallel_replicas_local_plan=(1, None); automatic_parallel_replicas_mode=(0, 0)

-- https://github.com/ClickHouse/ClickHouse/issues/123958
-- With parallel_replicas_prefer_local_replica = 0 the initiator builds no local plan, so the replicas must not
-- leave rows answered by a projection (exact count, minmax, a projection beside parts without it) to it.

DROP TABLE IF EXISTS t_pr_no_local_plan;
CREATE TABLE t_pr_no_local_plan (x UInt64, b UInt64) ENGINE = MergeTree ORDER BY x;
SYSTEM STOP MERGES t_pr_no_local_plan;
INSERT INTO t_pr_no_local_plan SELECT number, number % 100 FROM numbers(200000) SETTINGS max_insert_threads = 1;

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_prefer_local_replica = 0;

-- Without projections in the table the replicas analyze the primary key themselves instead of reading the whole part.
SELECT count() FROM t_pr_no_local_plan WHERE x < 70000 SETTINGS max_parallel_replicas = 3, log_comment = '05401_rows' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'rows read below the table size', read_rows < 200000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05401_rows' AND type = 'QueryFinish' AND is_initial_query
ORDER BY event_time_microseconds DESC LIMIT 1;

ALTER TABLE t_pr_no_local_plan ADD PROJECTION agg (SELECT b, sum(x) GROUP BY b), ADD PROJECTION p (SELECT * ORDER BY b);
INSERT INTO t_pr_no_local_plan SELECT 200000 + number, number % 100 FROM numbers(100000);

SELECT 'exact count', count() FROM t_pr_no_local_plan WHERE x < 70000 SETTINGS max_parallel_replicas = 3, log_comment = '05401_exact_count';
SELECT 'exact count, one replica', count() FROM t_pr_no_local_plan WHERE x < 70000 SETTINGS max_parallel_replicas = 1, log_comment = '05401_exact_count_one';
SELECT 'minmax', min(x), max(x), count() FROM t_pr_no_local_plan SETTINGS max_parallel_replicas = 3, log_comment = '05401_minmax';
SELECT 'projection beside parts without it', b, sum(x) FROM t_pr_no_local_plan WHERE b < 3 GROUP BY b ORDER BY b SETTINGS max_parallel_replicas = 3, log_comment = '05401_agg_proj';
SELECT 'normal projection beside parts without it', count(), sum(x) FROM t_pr_no_local_plan WHERE b = 7 SETTINGS max_parallel_replicas = 3, log_comment = '05401_normal_proj';

SYSTEM FLUSH LOGS query_log;
SELECT 'parallel replicas used', log_comment, min(ProfileEvents['ParallelReplicasUsedCount'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('05401_rows', '05401_exact_count', '05401_exact_count_one', '05401_minmax', '05401_agg_proj', '05401_normal_proj')
GROUP BY log_comment
ORDER BY log_comment;

-- With a local plan the exact count is still answered from the primary key.
SELECT 'exact count with a local plan', count() > 0
FROM (EXPLAIN SELECT count() FROM t_pr_no_local_plan WHERE x < 70000 SETTINGS parallel_replicas_prefer_local_replica = 1, max_parallel_replicas = 3)
WHERE explain LIKE '%_exact_count_projection%';

DROP TABLE t_pr_no_local_plan;
