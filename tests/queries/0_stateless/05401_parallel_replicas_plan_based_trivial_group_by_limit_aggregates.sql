-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the fail point below is server-wide.

-- The local replica runs its partial aggregation without serializing the plan. The kept-keys cutoff of the trivial
-- `GROUP BY ... LIMIT` optimization must not survive there, or a key it drops comes out of the merge with only the
-- remote replicas' share.

SET enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;
SET parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 1;
-- Arm the cutoff: no user cap, no spill (a spill abandons the cutoff), no exact row count.
SET optimize_trivial_group_by_limit_query = 1, max_rows_to_group_by = 0, exact_rows_before_limit = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET distributed_aggregation_memory_efficient = 1;

DROP TABLE IF EXISTS t_pr_kept_keys;
CREATE TABLE t_pr_kept_keys (k UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Large enough for the remote replicas to get a share: a table that one replica reads alone gives exact counts.
INSERT INTO t_pr_kept_keys SELECT number % 4999 FROM numbers(9998000);

SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT k, count() FROM t_pr_kept_keys GROUP BY k LIMIT 2000);

-- Delay the local replica's read requests so that the remote replicas get a share too.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- LIMIT above `query_plan_max_limit_for_top_k_optimization`: the cutoff, not the top-K heap, takes the query.
SELECT count(), countIf(c != 2000) FROM (SELECT k, count() AS c FROM t_pr_kept_keys GROUP BY k LIMIT 2000);
-- With the heap disabled the cutoff takes any LIMIT.
SELECT count(), countIf(c != 2000) FROM (SELECT k, count() AS c FROM t_pr_kept_keys GROUP BY k LIMIT 10)
SETTINGS enable_group_by_top_k_optimization = 0;
-- The ordinary merge applies its own cap to the keys it receives and must still return exact values.
SELECT count(), countIf(c != 2000) FROM (SELECT k, count() AS c FROM t_pr_kept_keys GROUP BY k LIMIT 2000)
SETTINGS distributed_aggregation_memory_efficient = 0;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_kept_keys;
