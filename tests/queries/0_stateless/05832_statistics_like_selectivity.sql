-- PREWHERE order follows the statistics estimate of `LIKE`: a pattern without wildcards is estimated as an equality,
-- a pattern of only `%` as matching every non-NULL value, and `startsWith`/`endsWith`, which `LIKE 'p%'`/`LIKE '%s'`
-- are rewritten to, as `LIKE`.

SET allow_statistics = 1, use_statistics = 1, materialize_statistics_on_insert = 1;
SET min_marks_per_part_to_load_statistics = 0;
SET optimize_move_to_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

-- Compact parts: PREWHERE conditions are ordered by estimated rows alone.
CREATE TABLE tab (x UInt64, s String, n Nullable(String), a Array(UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

-- `s` has 10000 distinct values, `n` is NULL in 99% of the rows.
INSERT INTO tab SELECT number, toString(number), if(number % 100 = 0, toString(number), NULL), [number % 10] FROM numbers(10000);

-- Each query prints 1 when PREWHERE evaluates the pattern condition before `less`.
SELECT 'exact LIKE', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND s LIKE '1234'));

SELECT 'LIKE %', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND n LIKE '%'));

SELECT 'NOT LIKE %', position(p, 'notLike(') > 0 AND position(p, 'notLike(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND n NOT LIKE '%'));

SELECT 'prefix LIKE', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND s LIKE '12%' SETTINGS optimize_rewrite_like_perfect_affix = 1));

SELECT 'suffix LIKE', position(p, 'endsWith(') > 0 AND position(p, 'endsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND s LIKE '%12' SETTINGS optimize_rewrite_like_perfect_affix = 1));

SELECT 'startsWith empty', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND startsWith(n, '')));

-- The `Array` overload is not a LIKE and stays an unknown condition.
SELECT 'startsWith Array', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND startsWith(a, [1])));

DROP TABLE tab;
