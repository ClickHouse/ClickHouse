-- Push a scatter above a UNION ALL into its gathered branches, preserving keys, bucket counts,
-- and hash cast types. This file checks the join, aggregation, empty-key shuffle, and order cases.
-- Windows, broadcasts, key types, and the cases that must keep the scatter are in
-- `05459_distributed_plan_union_exchange_pushdown_rest`. Every case compares distributed and
-- local results; ordered ones compare the delivered rows directly.

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
-- one test against one server and fails a run after 180s. This used to be one file and the
-- debug copies reached about 170s, so the cases are split across two tests. `max_threads = 1`
-- keeps those copies from oversubscribing the server.
INSERT INTO t1 SELECT number, number, if(number % 10 = 0, NULL, number), toString(number % 100) FROM numbers(4096);
INSERT INTO t2 SELECT number * 2, number, if(number % 7 = 0, NULL, number * 2), toString(number % 50) FROM numbers(4096);
INSERT INTO t3 SELECT number, number, number, toString(number % 100) FROM numbers(4096);
INSERT INTO t_small SELECT number, number FROM numbers(10);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_fallback_to_local_execution = 0,
    distributed_plan_optimize_exchanges = 1, distributed_plan_max_rows_to_broadcast = 0,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0, query_plan_join_swap_table = 0,
    optimize_read_in_order = 0, max_threads = 1,
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

DROP TABLE t1, t2, t3, t_small;
