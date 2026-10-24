-- The lazy FINAL rewrite must decline on a bucketed distributed read, otherwise every reader task
-- returns the whole deduplicated result.

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0, distributed_plan_default_reader_bucket_count = 4, distributed_plan_fallback_to_local_execution = 0;
-- `optimize_aggregation_in_order` makes the lazy FINAL rewrite orders of magnitude slower on this shape
-- without changing the result.
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS t_lazy;
CREATE TABLE t_lazy (k UInt64, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 256;
SYSTEM STOP MERGES t_lazy;
-- 40000 rows is above `distributed_plan_max_rows_to_broadcast`, so the read is bucketed.
INSERT INTO t_lazy SELECT number, number, 1 FROM numbers(20000);
INSERT INTO t_lazy SELECT number, number + 5, 2 FROM numbers(20000);

SELECT 'lazy WHERE local', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0 SETTINGS make_distributed_plan = 0;
SELECT 'lazy WHERE distributed buckets 2', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 2, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE distributed buckets 4', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 4, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE distributed buckets 8', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, distributed_plan_default_reader_bucket_count = 8, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy WHERE read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN PLAN SELECT k, v FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);

SELECT 'lazy PREWHERE distributed', count(), sum(v) FROM t_lazy FINAL PREWHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;
SELECT 'lazy PREWHERE read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN PLAN SELECT k, v FROM t_lazy FINAL PREWHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);

-- Without a filter the rewrite declines on its own, so this arm must be unaffected by the guard.
SELECT 'lazy no filter local', count(), sum(v) FROM t_lazy FINAL SETTINGS make_distributed_plan = 0;
SELECT 'lazy no filter distributed', count(), sum(v) FROM t_lazy FINAL
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;

-- The non-lazy distributed path must keep working: bucketing itself is correct.
SELECT 'lazy off distributed', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 1, query_plan_optimize_lazy_final = 0;

-- A local FINAL must still be rewritten by lazy FINAL.
SELECT 'lazy local still rewrites', countIf(explain LIKE '%LazyFinalKeyAnalysis%' OR explain LIKE '%LazyReadReplacingFinal%') > 0
FROM (EXPLAIN PLAN SELECT count() FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0);
SELECT 'lazy local result', count(), sum(v) FROM t_lazy FINAL WHERE k % 3 = 0
SETTINGS make_distributed_plan = 0, query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0.0;

DROP TABLE t_lazy;
