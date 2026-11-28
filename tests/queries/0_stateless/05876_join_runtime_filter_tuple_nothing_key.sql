-- A `Nothing` element makes a tuple comparison `NULL`, so a JOIN runtime filter with one key
-- on the build side must not answer by `equals`.

SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (n UInt32, e UInt8) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t2 (n UInt32, e UInt8) ENGINE = MergeTree ORDER BY n;
INSERT INTO t1 SELECT number, number % 2 FROM numbers(10);
INSERT INTO t2 VALUES (1, 1);

SELECT count() FROM t1 JOIN t2 ON tuple(t1.e, [][materialize(t1.n)]) = tuple(t2.e, [][materialize(t2.n)]) SETTINGS query_plan_join_swap_table = 0;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t1 JOIN t2 ON tuple(t1.e, [][materialize(t1.n)]) = tuple(t2.e, [][materialize(t2.n)]) SETTINGS query_plan_join_swap_table = 0) WHERE explain ILIKE '%BuildRuntimeFilter%';
SELECT count() FROM t1 JOIN t2 ON tuple(tuple(t1.e, [][materialize(t1.n)])) = tuple(tuple(t2.e, [][materialize(t2.n)])) SETTINGS query_plan_join_swap_table = 0;
SELECT count() FROM t1 JOIN t2 ON t1.e = t2.e AND tuple(t1.e, [][materialize(t1.n)]) = tuple(t2.e, [][materialize(t2.n)]) SETTINGS query_plan_join_swap_table = 0;
SELECT count() FROM t1 LEFT ANTI JOIN t2 ON tuple(t1.e, [][materialize(t1.n)]) = tuple(t2.e, [][materialize(t2.n)]) SETTINGS query_plan_join_swap_table = 0;
SELECT count() FROM t1 LEFT ANTI JOIN t2 ON t1.e = t2.e AND tuple(t1.e, [][materialize(t1.n)]) = tuple(t2.e, [][materialize(t2.n)]) SETTINGS query_plan_join_swap_table = 0;

DROP TABLE t1;
DROP TABLE t2;

-- The decorrelated scalar subquery joins on `x`.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (n UInt8, e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory;
INSERT INTO tab VALUES (1, 'b'), (3, 'b');
SELECT n, (SELECT x.1) FROM (SELECT n, tuple(e, [][materialize(1)]) AS x FROM tab) ORDER BY n;
DROP TABLE tab;
