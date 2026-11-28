-- Lazy materialization over joins with values of a side an outer join can leave unmatched, recomputed above the LIMIT:
-- only what is read as the join left it is masked - an output, or what is read above the join - while what is computed
-- from it under the same join reads it unmasked. A constant argument stays a constant, as `formatDateTime` requires
-- of its format. Every query is followed by the number of lazy reads in its plan, and the results are compared with
-- the optimization disabled.

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
DROP TABLE IF EXISTS m;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, dt DateTime('UTC'), rheavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE m (k UInt64, mheavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number * 2, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 86400 * 40, concat('r', toString(number)) FROM numbers(50);
INSERT INTO m SELECT number * 4, concat('m', toString(number)) FROM numbers(25);

SELECT '-- a constant argument of a function of the right side';
SELECT l.k, l.heavy, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y, rheavy FROM r) AS t ON l.k = t.k
    ORDER BY l.k DESC LIMIT 4);

SELECT '-- the constant is in the result as well';
SELECT l.k, l.heavy, t.fmt, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, '%Y' AS fmt, formatDateTime(dt, fmt) AS y, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.fmt, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, '%Y' AS fmt, formatDateTime(dt, fmt) AS y, rheavy FROM r) AS t ON l.k = t.k
ORDER BY l.k DESC LIMIT 4 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, t.fmt, t.y, t.rheavy FROM l LEFT JOIN (SELECT k, '%Y' AS fmt, formatDateTime(dt, fmt) AS y, rheavy FROM r) AS t
    ON l.k = t.k ORDER BY l.k DESC LIMIT 4);

SELECT '-- under two outer joins';
SELECT l.k, l.heavy, u.y, u.mheavy FROM l LEFT JOIN (
    SELECT m.k AS k, m.mheavy AS mheavy, t.y AS y FROM m LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y FROM r) AS t ON m.k = t.k) AS u ON l.k = u.k
ORDER BY l.k DESC LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, u.y, u.mheavy FROM l LEFT JOIN (
    SELECT m.k AS k, m.mheavy AS mheavy, t.y AS y FROM m LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y FROM r) AS t ON m.k = t.k) AS u ON l.k = u.k
ORDER BY l.k DESC LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0 FROM (
    EXPLAIN SELECT l.k, l.heavy, u.y, u.mheavy FROM l LEFT JOIN (
        SELECT m.k AS k, m.mheavy AS mheavy, t.y AS y FROM m LEFT JOIN (SELECT k, formatDateTime(dt, '%Y') AS y FROM r) AS t ON m.k = t.k) AS u
    ON l.k = u.k ORDER BY l.k DESC LIMIT 6);

DROP TABLE l;
DROP TABLE r;
DROP TABLE m;
