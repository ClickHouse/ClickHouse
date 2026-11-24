-- Automatic parallel replicas hand each read of the replicas plan the index analysis of the same read in the
-- single-node plan. A table read more than once needs its occurrences told apart, and the analyzer's `__tableN`
-- names cannot do it: the replicas plan's tree is rebuilt by `buildQueryTreeForShard`, which renumbers them over
-- a join tree `CrossToInnerJoinPass` may have reordered. Here `t AS a` and `t AS b` swap names that way, each
-- name still finds one counterpart, and pairing by name gave each read the other's mark ranges - wrong rows.
-- Occurrences are paired by where they were written in the query, and only when that is certain. Each read of
-- `t_occ_t` below selects different ranges, so a swap shows in the results.

DROP TABLE IF EXISTS t_occ_d;
DROP TABLE IF EXISTS t_occ_t;

-- The layout is pinned: the cost model works off the estimated bytes to read. So are the statistics: without the
-- basic ones the single-node plan of the subquery shape is built differently and the replicas plan never matches it.
CREATE TABLE t_occ_d (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_occ_t (id UInt64, k UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_occ_d SELECT number, number FROM numbers(300000);
INSERT INTO t_occ_t SELECT number, 10000 + number % 20000, number FROM numbers(300000);
OPTIMIZE TABLE t_occ_d FINAL;
OPTIMIZE TABLE t_occ_t FINAL;

SET enable_analyzer = 1;
-- As in `05261_autopr_transplant_analysis_table_order`: the reads are pruned by their own key conditions, the
-- join side and order are pinned, and the cost model is made to favour replicas on tables of this size.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET max_threads = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET automatic_parallel_replicas_max_duplicated_read_ratio = 1;
-- Without `PREWHERE` the plans of the shape with two subqueries written alike do not match, and it is skipped
-- before the cost model, where its reads are to be declined.
SET optimize_move_to_prewhere = 1;

SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The first run of each shape only collects statistics; the next one can act on them.
SELECT count(), sum(d.v), sum(b.v), sum(a.v), sum(a.k)
FROM t_occ_d AS d, t_occ_t AS a, t_occ_t AS b
WHERE d.id = b.id AND a.id = b.k AND b.id < 20000
SETTINGS log_comment = '05628_warmup_aliases';
SELECT count(), sum(d.v), sum(b.v), sum(a.v), sum(a.k)
FROM t_occ_d AS d, t_occ_t AS a, t_occ_t AS b
WHERE d.id = b.id AND a.id = b.k AND b.id < 20000
SETTINGS log_comment = '05628_replicas_aliases';

-- Neither read has an alias, and the two subqueries differ only in `WHERE` against `PREWHERE`.
SELECT count(), sum(d.v), sum(b.v), sum(a.v)
FROM t_occ_d AS d, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS a, (SELECT id, k, v FROM t_occ_t PREWHERE id < 20000) AS b
WHERE d.id = b.id AND a.id = b.k
SETTINGS log_comment = '05628_warmup_subqueries';
SELECT count(), sum(d.v), sum(b.v), sum(a.v)
FROM t_occ_d AS d, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS a, (SELECT id, k, v FROM t_occ_t PREWHERE id < 20000) AS b
WHERE d.id = b.id AND a.id = b.k
SETTINGS log_comment = '05628_replicas_subqueries';

-- The two subqueries are written alike, so their reads cannot be told apart, although the join pushes different
-- conditions into them. The pairing has to decline, and only after the cost model has favoured replicas: an earlier
-- skip would not test it.
SELECT count(), sum(d.v), sum(b.v), sum(a.v)
FROM t_occ_d AS d, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS a, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS b
WHERE d.id = b.id AND a.id = b.k AND d.id < 20000
SETTINGS log_comment = '05628_warmup_ambiguous';
SELECT count(), sum(d.v), sum(b.v), sum(a.v)
FROM t_occ_d AS d, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS a, (SELECT id, k, v FROM t_occ_t WHERE id < 20000) AS b
WHERE d.id = b.id AND a.id = b.k AND d.id < 20000
SETTINGS log_comment = '05628_replicas_ambiguous';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Every run above prints its result, and the reference holds the correct one. What it cannot show is whether
-- replicas were used; that is this.
SELECT
    replaceOne(log_comment, '05628_replicas_', '') AS shape,
    count() = 1 AND min(ProfileEvents['ParallelReplicasUsedCount'] > 0) AS replicas_were_used
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday()
  AND log_comment IN ('05628_replicas_aliases', '05628_replicas_subqueries')
GROUP BY shape
ORDER BY shape
FORMAT TSVWithNames;

SELECT
    count() = 1
        AND min(ProfileEvents['AutoParallelReplicasCostModelEvaluated'] > 0)
        AND min(ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0)
        AND max(ProfileEvents['AutoParallelReplicasApplied']) = 0
        AND max(ProfileEvents['ParallelReplicasUsedCount']) = 0 AS ambiguous_reads_declined_after_the_cost_model
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday() AND log_comment = '05628_replicas_ambiguous'
FORMAT TSVWithNames;

DROP TABLE t_occ_d;
DROP TABLE t_occ_t;
