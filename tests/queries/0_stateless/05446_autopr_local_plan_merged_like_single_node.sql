-- A join runtime filter pushed into an aggregated subquery splits the expression it passes, and once
-- PREWHERE takes the filter, the split part is left as an `Expression` of its own. The plan merges it
-- back after the runtime filters are added. The parallel replicas local branch has to get that merge too,
-- or its steps hash differently from the single-node plan's, the two plans do not match and no statistics
-- are collected.

DROP TABLE IF EXISTS t_facts;
DROP TABLE IF EXISTS t_dim;

CREATE TABLE t_facts (k UInt64, flag UInt8, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_facts SELECT number % 20000, number % 3, number, number % 7 FROM numbers(200000);
INSERT INTO t_dim SELECT number, toString(number % 10) FROM numbers(100);

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 1;
-- Both plans have to add the runtime filter. The single-node plan can estimate the probe side below the
-- threshold, while the replicas plan cannot see through `MergingAggregated`.
SET join_runtime_filter_min_probe_rows = 0;
-- Without merging the filters, the runtime filter stays a step of its own in the local branch.
SET query_plan_merge_filters = 1;
-- The local branch is optimized on its own only in this implementation.
SET parallel_replicas_plan_based = 0;
SET enable_parallel_replicas = 1;
-- Mode 2 always collects the statistics, without consulting the cost model.
SET automatic_parallel_replicas_mode = 2;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- `v * (1 + w)` cannot go below the runtime filter on `k`, so pushing the filter splits the expression.
SELECT d.name, sum(f.s)
FROM (SELECT k, sum(v * (1 + w)) AS s FROM t_facts WHERE flag = 1 GROUP BY k) AS f, t_dim AS d
WHERE f.k = d.k
GROUP BY d.name
FORMAT Null
SETTINGS log_comment = '05446_local_plan_merged';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS collected_statistics
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday() AND log_comment = '05446_local_plan_merged'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_facts;
DROP TABLE t_dim;
