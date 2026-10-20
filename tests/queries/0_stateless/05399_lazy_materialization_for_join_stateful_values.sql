-- Lazy materialization over joins with values that do not answer the same twice: a stateful function, and a lambda
-- calling a random function. They are computed where the plan computes them and cross the LIMIT as columns, even where
-- nothing below the LIMIT needs them: a join repeats the value of a row of its side, and a stateful function counts the
-- rows before the LIMIT. The results are compared with the optimization disabled, or checked where they are random, and
-- every query is followed by the number of lazy reads in its plan.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number, [number, number + 1, number + 2] FROM numbers(50);

SELECT '-- a stateful function in a subquery, under a join that repeats its rows';
SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k, t.n LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k, t.n LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k, t.n LIMIT 6);

SELECT '-- the same, with the value in the result only';
-- The five rows with the smallest key are all the ones it has, so which of the tied rows the LIMIT keeps does not matter.
SELECT * FROM (
    SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k LIMIT 5)
ORDER BY n SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT * FROM (
    SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k LIMIT 5)
ORDER BY n SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.k, l.heavy, t.n FROM l JOIN (SELECT k % 10 AS k, rowNumberInAllBlocks() AS n FROM r) AS t ON l.k % 10 = t.k ORDER BY l.k LIMIT 5);

SELECT '-- a lambda calling a random function, in a filter and in the result, which agree';
SELECT min(has(m, 0)) FROM (
    SELECT l.heavy, t.m FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 2, arr) AS m FROM r) AS t ON l.k = t.k WHERE has(t.m, 0) ORDER BY l.k LIMIT 20);
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.heavy, t.m FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 2, arr) AS m FROM r) AS t ON l.k = t.k WHERE has(t.m, 0) ORDER BY l.k LIMIT 20);

DROP TABLE l;
DROP TABLE r;
