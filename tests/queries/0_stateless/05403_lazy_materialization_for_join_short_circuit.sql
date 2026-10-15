-- Lazy materialization over joins with a short-circuit function whose branch cannot be recomputed above the LIMIT, and
-- with a function that has observable side effects. A function in a branch that can throw on the rows the branch skips
-- would throw computed for every row, so the short-circuit function is computed where the plan computes it, as a whole.
-- A function with side effects runs for the rows the plan runs it for, not for every row a join repeats. Every query is
-- followed by the number of lazy reads in its plan, and the results are compared with the optimization disabled.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS one;
DROP TABLE IF EXISTS many;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE one (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE many (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(20);
INSERT INTO r SELECT number, concat('r', toString(number)) FROM numbers(10);
INSERT INTO one VALUES (1, 'x');
INSERT INTO many SELECT 1, toString(number) FROM numbers(100);

SELECT '-- a branch that throws where its condition skips it, on the side of an outer join';
SELECT l.k, l.heavy, t.v, t.rheavy FROM l LEFT JOIN (SELECT k, if(k = 0, toUInt64(0), intDiv(toUInt64(1), k)) AS v, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k LIMIT 12 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.v, t.rheavy FROM l LEFT JOIN (SELECT k, if(k = 0, toUInt64(0), intDiv(toUInt64(1), k)) AS v, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k LIMIT 12 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, t.v, t.rheavy FROM l LEFT JOIN (SELECT k, if(k = 0, toUInt64(0), intDiv(toUInt64(1), k)) AS v, rheavy FROM r) AS t
    ON l.k = t.k ORDER BY l.k LIMIT 12);

SELECT '-- sleepEachRow of a row of one side, which the join repeats 100 times';
SELECT t.heavy, t.z, many.rheavy FROM (SELECT k, heavy, sleepEachRow(0.00002) AS z FROM one) AS t JOIN many ON t.k = many.k ORDER BY many.rheavy LIMIT 3
SETTINGS function_sleep_max_microseconds_per_block = 100, query_plan_lazy_materialization_for_join = 1;
SELECT t.heavy, t.z, many.rheavy FROM (SELECT k, heavy, sleepEachRow(0.00002) AS z FROM one) AS t JOIN many ON t.k = many.k ORDER BY many.rheavy LIMIT 3
SETTINGS function_sleep_max_microseconds_per_block = 100, query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT t.heavy, t.z, many.rheavy FROM (SELECT k, heavy, sleepEachRow(0.00002) AS z FROM one) AS t JOIN many ON t.k = many.k
    ORDER BY many.rheavy LIMIT 3);

SELECT '-- the same in a lambda';
SELECT t.heavy, t.z, many.rheavy FROM (SELECT k, heavy, arrayMap(x -> sleepEachRow(0.00002), [1]) AS z FROM one) AS t JOIN many ON t.k = many.k
ORDER BY many.rheavy LIMIT 3 SETTINGS function_sleep_max_microseconds_per_block = 100, query_plan_lazy_materialization_for_join = 1;
SELECT t.heavy, t.z, many.rheavy FROM (SELECT k, heavy, arrayMap(x -> sleepEachRow(0.00002), [1]) AS z FROM one) AS t JOIN many ON t.k = many.k
ORDER BY many.rheavy LIMIT 3 SETTINGS function_sleep_max_microseconds_per_block = 100, query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
DROP TABLE one;
DROP TABLE many;
