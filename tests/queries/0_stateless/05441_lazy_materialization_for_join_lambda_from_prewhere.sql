-- A non-deterministic lambda shared by a PREWHERE condition and a WHERE condition is output by the read.
-- The higher-order function that reads it must cross the LIMIT as a column, not be recomputed above it.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET optimize_move_to_prewhere = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number, [number, number + 1, number + 2] FROM numbers(50);

SELECT count() > 0, min(has(m, 0)), min(c <= k), min(heavy = concat('l', toString(k))) FROM (
    SELECT l.heavy AS heavy, t.m AS m, t.c AS c, t.k AS k
    FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 3, arr) AS m, arrayCount(x -> rand64(x) % 3, arr) AS c FROM r) AS t ON l.k = t.k
    WHERE has(t.m, 0) AND t.c <= t.k ORDER BY l.k LIMIT 20);

SELECT count() > 0, min(has(m, 0)), min(length(f) <= k), min(heavy = concat('l', toString(k))) FROM (
    SELECT l.heavy AS heavy, t.m AS m, t.f AS f, t.k AS k
    FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 3, arr) AS m, arrayFilter(x -> rand64(x) % 3, arr) AS f FROM r) AS t ON l.k = t.k
    WHERE has(t.m, 0) AND length(t.f) <= t.k ORDER BY l.k LIMIT 20);

SELECT count() > 0, min(has(m, 0)), min(e <= tk), min(heavy = concat('l', toString(k))) FROM (
    SELECT l.heavy AS heavy, l.k AS k, t.m AS m, t.e AS e, t.k AS tk
    FROM l LEFT JOIN (SELECT k, arrayMap(x -> rand64(x) % 3, arr) AS m, arrayExists(x -> rand64(x) % 3, arr) AS e FROM r) AS t ON l.k = t.k
    WHERE has(t.m, 0) AND t.e <= t.k ORDER BY l.k LIMIT 20);

-- The pass applies, the read outputs the lambda, and no lambda crosses the LIMIT.
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.heavy, t.m, t.c
    FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 3, arr) AS m, arrayCount(x -> rand64(x) % 3, arr) AS c FROM r) AS t ON l.k = t.k
    WHERE has(t.m, 0) AND t.c <= t.k ORDER BY l.k LIMIT 20);

SELECT countIf(explain LIKE '%Function(UInt64 -> UInt8)%') > 0, countIf(match(explain, '__lazy_carried_[0-9]+ Function\\(')) FROM (
    EXPLAIN header = 1 SELECT l.heavy, t.m, t.c
    FROM l JOIN (SELECT k, arrayMap(x -> rand64(x) % 3, arr) AS m, arrayCount(x -> rand64(x) % 3, arr) AS c FROM r) AS t ON l.k = t.k
    WHERE has(t.m, 0) AND t.c <= t.k ORDER BY l.k LIMIT 20);

DROP TABLE l;
DROP TABLE r;
