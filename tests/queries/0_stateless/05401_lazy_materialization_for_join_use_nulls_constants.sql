-- Lazy materialization over joins with constants of a side an outer join can leave unmatched: under `join_use_nulls`
-- the join wraps them in `toNullable`, and they are NULL at the rows it pads, although they read no column of a
-- table. Every query is followed by the number of lazy reads in its plan, and the results are compared with the
-- optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number * 2, concat('r', toString(number)) FROM numbers(50);

SELECT '-- LEFT JOIN, join_use_nulls = 1';
SELECT l.k, l.heavy, t.c, t.s, t.rheavy FROM l LEFT JOIN (SELECT k, 42 AS c, 'const' AS s, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.c, t.s, t.rheavy FROM l LEFT JOIN (SELECT k, 42 AS c, 'const' AS s, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, t.c, t.s, t.rheavy FROM l LEFT JOIN (SELECT k, 42 AS c, 'const' AS s, rheavy FROM r) AS t ON l.k = t.k
    ORDER BY l.k DESC LIMIT 4 SETTINGS join_use_nulls = 1);

SELECT '-- RIGHT JOIN, join_use_nulls = 1';
SELECT t.k, t.c, t.s, t.heavy, r.k, r.rheavy FROM (SELECT k, 7 AS c, 'left' AS s, heavy FROM l WHERE k % 3 = 0) AS t RIGHT JOIN r ON t.k = r.k
ORDER BY r.k DESC LIMIT 4 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.c, t.s, t.heavy, r.k, r.rheavy FROM (SELECT k, 7 AS c, 'left' AS s, heavy FROM l WHERE k % 3 = 0) AS t RIGHT JOIN r ON t.k = r.k
ORDER BY r.k DESC LIMIT 4 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT t.k, t.c, t.s, t.heavy, r.k, r.rheavy FROM (SELECT k, 7 AS c, 'left' AS s, heavy FROM l WHERE k % 3 = 0) AS t RIGHT JOIN r ON t.k = r.k
    ORDER BY r.k DESC LIMIT 4 SETTINGS join_use_nulls = 1);

SELECT '-- LEFT JOIN, join_use_nulls = 0';
SELECT l.k, l.heavy, t.c, t.s, t.rheavy FROM l LEFT JOIN (SELECT k, 42 AS c, 'const' AS s, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS join_use_nulls = 0, query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.c, t.s, t.rheavy FROM l LEFT JOIN (SELECT k, 42 AS c, 'const' AS s, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS join_use_nulls = 0, query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
