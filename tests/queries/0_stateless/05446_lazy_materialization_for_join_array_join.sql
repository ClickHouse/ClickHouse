-- Lazy materialization over joins goes through an `ARRAY JOIN`: the columns of a table below it are read for the rows the
-- `LIMIT` returns only. The row index is repeated with the rows, as a join repeats them; the elements are computed below
-- the `LIMIT`. Each query is compared with the feature off, and the plan shows which columns are read lazily.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_unaligned_array_join = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS f;

CREATE TABLE l (k UInt64, arr Array(UInt64), brr Array(String), heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;
-- Every third row has an empty array.
INSERT INTO l SELECT number, if(number % 3 = 0, [], range(number % 5)), if(number % 3 = 0, [], arrayMap(x -> toString(x), range(number % 5))),
    concat('heavy', toString(number)) FROM numbers(1000);
INSERT INTO r SELECT number, concat('r', toString(number)) FROM numbers(10);

SELECT '-- ARRAY JOIN below a join';
SELECT l.k, e, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, e, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, e, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- LEFT ARRAY JOIN, the whole array as well';
-- An inner join: a LEFT JOIN would have the sorting and the LIMIT pushed down to the table, below the ARRAY JOIN.
SELECT l.k, e, l.arr, l.heavy, r.rheavy FROM l LEFT ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, e, l.arr, l.heavy, r.rheavy FROM l LEFT ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, e, l.arr, l.heavy, r.rheavy FROM l LEFT ARRAY JOIN l.arr AS e JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- two arrays';
SELECT l.k, e, s, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e, l.brr AS s JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, e, s, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e, l.brr AS s JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- ARRAY JOIN on the right side of a join';
SELECT r.k, x.k, x.e, x.heavy FROM r JOIN (SELECT k, e, heavy FROM l ARRAY JOIN arr AS e) AS x ON r.k = x.e
ORDER BY x.k DESC, r.k LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT r.k, x.k, x.e, x.heavy FROM r JOIN (SELECT k, e, heavy FROM l ARRAY JOIN arr AS e) AS x ON r.k = x.e
ORDER BY x.k DESC, r.k LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT r.k, x.k, x.e, x.heavy FROM r JOIN (SELECT k, e, heavy FROM l ARRAY JOIN arr AS e) AS x ON r.k = x.e
    ORDER BY x.k DESC, r.k LIMIT 6
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- FINAL';
CREATE TABLE f (k UInt64, v UInt64, arr Array(UInt64), heavy String) ENGINE = ReplacingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES f;
INSERT INTO f SELECT number, 1, range(number % 4), concat('old', toString(number)) FROM numbers(1000);
INSERT INTO f SELECT number, 2, range(number % 4), concat('new', toString(number)) FROM numbers(0, 1000, 2);
SELECT f.k, e, f.heavy, r.rheavy FROM f FINAL ARRAY JOIN f.arr AS e JOIN r ON e = r.k ORDER BY f.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT f.k, e, f.heavy, r.rheavy FROM f FINAL ARRAY JOIN f.arr AS e JOIN r ON e = r.k ORDER BY f.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT f.k, e, f.heavy, r.rheavy FROM f FINAL ARRAY JOIN f.arr AS e JOIN r ON e = r.k ORDER BY f.k DESC, e LIMIT 6
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- unaligned ARRAY JOIN is not looked through';
SELECT l.k, e, s, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e, l.brr AS s JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
SETTINGS query_plan_lazy_materialization_for_join = 1, enable_unaligned_array_join = 1;
SELECT count() FROM (EXPLAIN actions = 1
    SELECT l.k, e, s, l.heavy, r.rheavy FROM l ARRAY JOIN l.arr AS e, l.brr AS s JOIN r ON e = r.k ORDER BY l.k DESC, e LIMIT 6
    SETTINGS query_plan_lazy_materialization_for_join = 1, enable_unaligned_array_join = 1)
WHERE explain LIKE '%Lazily read columns: %heavy%' AND explain NOT LIKE '%rheavy%';

DROP TABLE l;
DROP TABLE r;
DROP TABLE f;
