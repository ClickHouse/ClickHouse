-- Push a scatter above a UNION ALL into its gathered branches, preserving keys, bucket counts,
-- and hash cast types. Negative cases must retain the scatter; every case also compares
-- distributed and local results, ordered ones the delivered rows directly.

DROP TABLE IF EXISTS t1, t2, t3, t_small;

CREATE TABLE t1 (k UInt64, v UInt64, n Nullable(UInt64), lc LowCardinality(String))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t2 (k UInt64, v UInt64, n Nullable(UInt64), lc LowCardinality(String))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t3 (k UInt32, v UInt64, n Nullable(UInt64), lc LowCardinality(String))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t1; SYSTEM STOP MERGES t2; SYSTEM STOP MERGES t3; SYSTEM STOP MERGES t_small;

-- A few wide-part granules are enough for the plan shape. The flaky check runs many copies of
-- this test at once and kills a run after 180s, so the old 100000-row tables did not finish.
INSERT INTO t1 SELECT number, number, if(number % 10 = 0, NULL, number), toString(number % 100) FROM numbers(4096);
INSERT INTO t2 SELECT number * 2, number, if(number % 7 = 0, NULL, number * 2), toString(number % 50) FROM numbers(4096);
INSERT INTO t3 SELECT number, number, number, toString(number % 100) FROM numbers(4096);
INSERT INTO t_small SELECT number, number FROM numbers(10);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_optimize_exchanges = 1, distributed_plan_max_rows_to_broadcast = 0,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0, query_plan_join_swap_table = 0,
    optimize_read_in_order = 0, max_threads = 4,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4;

-- P1: full plan snapshot and hash-cast/result consistency (t3.k is UInt32).
EXPLAIN actions = 0 SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k SETTINGS make_distributed_plan = 0;

-- P3: forced shuffle aggregation.
SELECT 'P3', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k SETTINGS distributed_plan_force_shuffle_aggregation = 1, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS distributed_plan_force_shuffle_aggregation = 1;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS make_distributed_plan = 0, distributed_plan_force_shuffle_aggregation = 1;

-- P4a/P4b: empty-key 4-to-4 shuffles are redundant.
SELECT 'P4a', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT 'P4b', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count(), sum(v) FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count(), sum(v) FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2);
SELECT count(), sum(v) FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) SETTINGS make_distributed_plan = 0;

-- P4c: reads, union, and partial aggregation share one four-task fragment.
EXPLAIN fragments = 1, optimize = 1 SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k;

-- P5: a 3-to-4 empty-key shuffle is retained.
SELECT 'P5', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k SETTINGS distributed_plan_default_reader_bucket_count = 3, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS distributed_plan_default_reader_bucket_count = 3;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS make_distributed_plan = 0, distributed_plan_default_reader_bucket_count = 3;

-- P6a/P6b: global order is unchanged.
SELECT 'P6a', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k LIMIT 5 SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k LIMIT 5;
SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k LIMIT 5 SETTINGS make_distributed_plan = 0;
SELECT 'P6b', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k LIMIT 20;
SELECT k FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY k LIMIT 20 SETTINGS make_distributed_plan = 0;

-- P7: window partitioning and delivered order.
SELECT 'P7', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, v, sum(v) OVER (PARTITION BY v ORDER BY k) AS s FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY v, k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT k, v, sum(v) OVER (PARTITION BY v ORDER BY k) AS s FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY v, k LIMIT 20;
SELECT k, v, sum(v) OVER (PARTITION BY v ORDER BY k) AS s FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) ORDER BY v, k LIMIT 20 SETTINGS make_distributed_plan = 0;

-- P8: the broadcast side is unchanged while the union's any scatter is removed.
SELECT 'P8', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t_small ON u.k = t_small.k SETTINGS distributed_plan_max_rows_to_broadcast = 100, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t_small ON u.k = t_small.k SETTINGS distributed_plan_max_rows_to_broadcast = 100;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t_small ON u.k = t_small.k SETTINGS make_distributed_plan = 0, distributed_plan_max_rows_to_broadcast = 100;

-- P9a/P9b: runtime filter builds remain above the union.
SELECT 'P9a', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS enable_join_runtime_filters = 1, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS enable_join_runtime_filters = 1;
SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS make_distributed_plan = 0, enable_join_runtime_filters = 1;
SELECT 'P9b', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS enable_join_runtime_filters = 1, distributed_plan_join_runtime_filters = 1, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS enable_join_runtime_filters = 1, distributed_plan_join_runtime_filters = 1;
SELECT count() FROM t3 JOIN (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u ON u.k = t3.k SETTINGS make_distributed_plan = 0, enable_join_runtime_filters = 1, distributed_plan_join_runtime_filters = 1;

-- P10/P11: flattened unions, constants, Nullable, and LowCardinality keys.
SELECT 'P10', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION ALL SELECT k FROM t3)) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION ALL SELECT k FROM t3)) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION ALL SELECT k FROM t3)) GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT 'P11a', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT c, count() FROM (SELECT 1 AS c, k FROM t1 UNION ALL SELECT 1 AS c, k FROM t2) GROUP BY c SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT c, count() FROM (SELECT 1 AS c, k FROM t1 UNION ALL SELECT 1 AS c, k FROM t2) GROUP BY c);
SELECT sum(cityHash64(*)) FROM (SELECT c, count() FROM (SELECT 1 AS c, k FROM t1 UNION ALL SELECT 1 AS c, k FROM t2) GROUP BY c) SETTINGS make_distributed_plan = 0;
SELECT 'P11b', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT n, count() FROM (SELECT n FROM t1 UNION ALL SELECT n FROM t2) GROUP BY n SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT n, count() FROM (SELECT n FROM t1 UNION ALL SELECT n FROM t2) GROUP BY n);
SELECT sum(cityHash64(*)) FROM (SELECT n, count() FROM (SELECT n FROM t1 UNION ALL SELECT n FROM t2) GROUP BY n) SETTINGS make_distributed_plan = 0;
SELECT 'P11c', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT lc, count() FROM (SELECT lc FROM t1 UNION ALL SELECT lc FROM t2) GROUP BY lc SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT lc, count() FROM (SELECT lc FROM t1 UNION ALL SELECT lc FROM t2) GROUP BY lc);
SELECT sum(cityHash64(*)) FROM (SELECT lc, count() FROM (SELECT lc FROM t1 UNION ALL SELECT lc FROM t2) GROUP BY lc) SETTINGS make_distributed_plan = 0;
SELECT 'P11d', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT lc, v FROM t1 UNION ALL SELECT lc, v FROM t2) u JOIN t3 ON u.lc = t3.lc SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT lc, v FROM t1 UNION ALL SELECT lc, v FROM t2) u JOIN t3 ON u.lc = t3.lc;
SELECT count() FROM (SELECT lc, v FROM t1 UNION ALL SELECT lc, v FROM t2) u JOIN t3 ON u.lc = t3.lc SETTINGS make_distributed_plan = 0;

-- P12/P12b/P13: aliases, materialize, and deterministic expression/filter chains.
SELECT 'P12', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT k AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT k AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS make_distributed_plan = 0, query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT 'P12b', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT materialize(k) AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT materialize(k) AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT materialize(k) AS kk, v * 2 AS w FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.kk = t3.k SETTINGS make_distributed_plan = 0, query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT 'P13', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT k, v FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) WHERE v % 3 = 0) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, v FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) WHERE v % 3 = 0) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT k, v FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) WHERE v % 3 = 0) u JOIN t3 ON u.k = t3.k SETTINGS make_distributed_plan = 0, query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;

-- P14: branches with different source bucket counts rewrite together: no scatter stays above the
-- union and no gather below it; each branch keeps (3-to-4) or drops (4-to-4) its any shuffle on its own.
SELECT 'P14', countIf(explain LIKE '%ScatterExchange%'), countIf(explain LIKE '%GatherExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM (SELECT k FROM t2 GROUP BY k SETTINGS distributed_plan_force_shuffle_aggregation = 1)) GROUP BY k SETTINGS distributed_plan_default_reader_bucket_count = 3, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM (SELECT k FROM t2 GROUP BY k)) GROUP BY k) SETTINGS distributed_plan_default_reader_bucket_count = 3;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM (SELECT k FROM t2 GROUP BY k)) GROUP BY k) SETTINGS make_distributed_plan = 0, distributed_plan_default_reader_bucket_count = 3;

-- P15: a self union puts two reads of the same table below the union with no exchange.
SELECT 'P15', countIf(explain LIKE '%ShuffleExchange%'), countIf(explain LIKE '%GatherExchange%'), countIf(explain LIKE '%ScatterExchange%')
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t1) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t1) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t1) GROUP BY k) SETTINGS make_distributed_plan = 0;

-- N1-N3: a branch without a root gather blocks the rewrite.
SELECT 'N1', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t_small) GROUP BY k SETTINGS distributed_plan_max_rows_to_broadcast = 100, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t_small) GROUP BY k) SETTINGS distributed_plan_max_rows_to_broadcast = 100;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t_small) GROUP BY k) SETTINGS make_distributed_plan = 0, distributed_plan_max_rows_to_broadcast = 100;
SELECT 'N2', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT toUInt64(1) AS k) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT toUInt64(1) AS k) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT toUInt64(1) AS k) GROUP BY k) SETTINGS make_distributed_plan = 0;
SELECT 'N3', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t2 WHERE 0) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t2 WHERE 0) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL SELECT k FROM t2 WHERE 0) GROUP BY k) SETTINGS make_distributed_plan = 0;

-- N4/N5: computed join keys cannot be translated below the expression.
SELECT 'N4', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k + 1 = t3.k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k + 1 = t3.k;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k + 1 = t3.k SETTINGS make_distributed_plan = 0;
SELECT 'N5', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k AND u.v + 1 = t3.v SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k AND u.v + 1 = t3.v;
SELECT count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) u JOIN t3 ON u.k = t3.k AND u.v + 1 = t3.v SETTINGS make_distributed_plan = 0;

-- N6/N7: a nondeterministic expression between the scatter and the union blocks the rewrite; the
-- deterministic `v AS r` variant rewrites. EXPLAIN folds it into the aggregate, so compare row counts.
SELECT 'N6', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT sum(u.r) FROM (SELECT k, v + rand() % 2 AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, v + rand() % 2 AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT k, v + rand() % 2 AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS make_distributed_plan = 0, query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT 'N7', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT sum(u.r) FROM (SELECT k, rowNumberInAllBlocks() AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT count() FROM (SELECT k, rowNumberInAllBlocks() AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;
SELECT count() FROM (SELECT k, rowNumberInAllBlocks() AS r FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2)) u JOIN t3 ON u.k = t3.k SETTINGS make_distributed_plan = 0, query_plan_lift_up_union = 0, query_plan_filter_push_down = 0;

-- N8: a nested union with DISTINCT leaves a non-gather outer branch.
SELECT 'N8', countIf(explain LIKE '%ScatterExchange%') FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION DISTINCT SELECT k FROM t3)) GROUP BY k SETTINGS make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION DISTINCT SELECT k FROM t3)) GROUP BY k);
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k FROM t1 UNION ALL (SELECT k FROM t2 UNION DISTINCT SELECT k FROM t3)) GROUP BY k) SETTINGS make_distributed_plan = 0;

-- N9: the existing optimization setting disables the rule and retains branch gathers.
SELECT 'N9', countIf(explain LIKE '%ScatterExchange%') >= 1, countIf(explain LIKE '%GatherExchange%') >= 2
FROM (EXPLAIN actions = 0 SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k SETTINGS distributed_plan_optimize_exchanges = 0, make_distributed_plan = 1) SETTINGS make_distributed_plan = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS distributed_plan_optimize_exchanges = 0;
SELECT sum(cityHash64(*)) FROM (SELECT k, count() FROM (SELECT k, v FROM t1 UNION ALL SELECT k, v FROM t2) GROUP BY k) SETTINGS make_distributed_plan = 0, distributed_plan_optimize_exchanges = 0;

DROP TABLE t1, t2, t3, t_small;
