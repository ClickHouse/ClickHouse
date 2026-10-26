-- Lazy materialization over joins at the rows an outer join leaves unmatched: a column read lazily, and a value
-- recomputed under the mask of the join, have the default the join stands there, the one of the type. For an `Enum`
-- that is its first element, and for a `Date32` the epoch, unlike the default of the column. Every query is followed
-- by the number of lazy reads in its plan, and the results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, e Enum8('a' = 1, 'b' = 2), d Date32, t Tuple(e Enum8('x' = 5, 'y' = 6), d Date32), n Nullable(Enum8('p' = 3)), rheavy String)
ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number * 2, 'b', toDate32('2020-01-01') + number, ('y', toDate32('2021-01-01')), 'p', concat('r', toString(number)) FROM numbers(50);

SELECT '-- columns read lazily';
SELECT l.k, r.e, r.d, r.t, r.n, r.rheavy FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 4
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, r.e, r.d, r.t, r.n, r.rheavy FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 4
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.k, r.e, r.d, r.t, r.n, r.rheavy FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 4);

SELECT '-- constants of the right side, recomputed under the mask';
SELECT l.k, l.heavy, s.d, s.e, s.rheavy FROM l LEFT JOIN (SELECT k, toDate32('2020-01-01') AS d, CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e, rheavy FROM r) AS s
ON l.k = s.k ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, s.d, s.e, s.rheavy FROM l LEFT JOIN (SELECT k, toDate32('2020-01-01') AS d, CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e, rheavy FROM r) AS s
ON l.k = s.k ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 0;
-- The left side is read lazily as well where the sorting is not pushed below the join, which depends on the settings.
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, s.d, s.e, s.rheavy FROM l LEFT JOIN (SELECT k, toDate32('2020-01-01') AS d, CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e, rheavy FROM r) AS s
    ON l.k = s.k ORDER BY l.k DESC LIMIT 4);

DROP TABLE l;
DROP TABLE r;
