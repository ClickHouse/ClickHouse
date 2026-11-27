-- Tags: no-parallel-replicas
-- (parallel replicas plan the read differently; this test asserts the local plan)

DROP TABLE IF EXISTS t_dvf;
DROP TABLE IF EXISTS t_dvf_right;
CREATE TABLE t_dvf (a UInt64, b UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY (a, b);
SYSTEM STOP MERGES t_dvf;
INSERT INTO t_dvf SELECT number % 10, number, 1 FROM numbers(100);
INSERT INTO t_dvf SELECT number % 10, number, 2 FROM numbers(100);
CREATE TABLE t_dvf_right (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_dvf_right SELECT number FROM numbers(5);

SET optimize_distinct_in_order = 1, optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_swap_table = 'false', join_algorithm = 'hash';

-- Without a `LIMIT`, `DISTINCT` does not read in order from a vertical `FINAL` read, also through a join.
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL SETTINGS enable_vertical_final = 0);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT l.a, l.b FROM t_dvf AS l FINAL INNER JOIN t_dvf_right AS r ON l.a = r.a SETTINGS enable_vertical_final = 1, query_plan_read_in_order_through_join = 1);

-- `ORDER BY` reads in order anyway.
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL ORDER BY a, b SETTINGS enable_vertical_final = 1);

-- A `LIMIT` that stops reading early keeps the in-order read: own, outer, above `ARRAY JOIN`, a join's probe side, a window, `LIMIT AFTER`.
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL LIMIT 5 SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT a, b FROM t_dvf FINAL) LIMIT 5 SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT a, b FROM t_dvf FINAL) ARRAY JOIN [1, 2] AS x LIMIT 5 SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT * FROM (SELECT DISTINCT a, b FROM t_dvf FINAL) AS l LEFT ANY JOIN t_dvf_right AS r ON l.a = r.a LIMIT 5 SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT a, b, row_number() OVER () FROM (SELECT DISTINCT a, b FROM t_dvf FINAL) LIMIT 5 SETTINGS enable_vertical_final = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL LIMIT 5 AFTER a >= 0 SETTINGS enable_vertical_final = 1);

-- A `LIMIT` that reads till the end does not, nor one above the input a join builds its table from.
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_dvf FINAL LIMIT 5 SETTINGS enable_vertical_final = 1, exact_rows_before_limit = 1);
SELECT countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%SelectByIndicesTransform%') > 0 FROM (EXPLAIN PIPELINE SELECT * FROM t_dvf_right AS r INNER JOIN (SELECT DISTINCT a, b FROM t_dvf FINAL) AS l ON l.a = r.a LIMIT 5 SETTINGS enable_vertical_final = 1);

DROP TABLE t_dvf;
DROP TABLE t_dvf_right;
