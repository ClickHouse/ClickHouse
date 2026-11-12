-- A window sorting that reuses the storage order (`query_plan_reuse_storage_ordering_for_window_functions`) merges
-- the streams read in order, then scatters the merged stream by the `PARTITION BY` columns, so that the window
-- functions run in parallel instead of in a single stream.

SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET query_plan_reuse_storage_ordering_for_window_functions = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS t_window_read_in_order;
CREATE TABLE t_window_read_in_order (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1000;
SYSTEM STOP MERGES t_window_read_in_order;
INSERT INTO t_window_read_in_order SELECT number % 100, number, number % 7 FROM numbers(0, 100000);
INSERT INTO t_window_read_in_order SELECT number % 100, number, number % 7 FROM numbers(100000, 100000);
INSERT INTO t_window_read_in_order SELECT number % 100, number, number % 7 FROM numbers(200000, 100000);
INSERT INTO t_window_read_in_order SELECT number % 100, number, number % 7 FROM numbers(300000, 100000);

SELECT 'pipeline';
-- The window runs in four streams after the merge of the streams read in order.
SELECT countIf(explain LIKE '%MergingSortedTransform%') > 0, countIf(explain LIKE '%ScatterByPartitionTransform%'), countIf(explain LIKE '%WindowTransform × 4%')
FROM (EXPLAIN PIPELINE SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) FROM t_window_read_in_order);
-- Only a prefix of the window sorting comes from the storage: the suffix is sorted in every stream after the scatter.
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%'), countIf(explain LIKE '%FinishSortingTransform × 4%'), countIf(explain LIKE '%WindowTransform × 4%')
FROM (EXPLAIN PIPELINE SELECT a, c, row_number() OVER (PARTITION BY a ORDER BY c, b) FROM t_window_read_in_order);

SELECT 'results';
SELECT
    (SELECT sum(cityHash64(a, b, rn)) FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t_window_read_in_order) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0)
  = (SELECT sum(cityHash64(a, b, rn)) FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t_window_read_in_order));
SELECT
    (SELECT sum(cityHash64(a, b, s)) FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY c, b ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM t_window_read_in_order) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0)
  = (SELECT sum(cityHash64(a, b, s)) FROM (SELECT a, b, sum(b) OVER (PARTITION BY a ORDER BY c, b ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM t_window_read_in_order));
-- The second window reuses the sorting and the distribution of the first one.
SELECT
    (SELECT sum(cityHash64(a, b, rn, s)) FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b, c) AS rn, sum(c) OVER (PARTITION BY a ORDER BY b) AS s FROM t_window_read_in_order) SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0)
  = (SELECT sum(cityHash64(a, b, rn, s)) FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b, c) AS rn, sum(c) OVER (PARTITION BY a ORDER BY b) AS s FROM t_window_read_in_order));

SELECT 'order by over the window';
-- The `ORDER BY` above stays a full sort: a merge of the scattered streams could deadlock against the scatter.
SELECT a, b, rn FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t_window_read_in_order) ORDER BY a, b LIMIT 3 OFFSET 3998
SETTINGS query_plan_enable_multithreading_after_window_functions = 0;
SELECT a, b, rn FROM (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t_window_read_in_order) ORDER BY a, b LIMIT 3 OFFSET 3998;

DROP TABLE t_window_read_in_order;
