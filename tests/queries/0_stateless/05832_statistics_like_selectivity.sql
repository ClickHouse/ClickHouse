-- PREWHERE order follows the statistics estimate of `LIKE`: a pattern without wildcards on a `String` (also with
-- `ESCAPE`) is estimated as an equality, a pattern of only `%` (any haystack type) as matching every non-NULL value,
-- and `startsWith`/`endsWith`, which `LIKE 'p%'`/`LIKE '%s'` are rewritten to, as `LIKE`.

SET allow_statistics = 1, use_statistics = 1, materialize_statistics_on_insert = 1;
SET min_marks_per_part_to_load_statistics = 0;
SET optimize_move_to_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab;

-- Compact parts: PREWHERE conditions are ordered by estimated rows alone.
CREATE TABLE tab (x UInt64, s String, n LowCardinality(Nullable(String)), f Nullable(FixedString(4)), e Nullable(Enum8('a' = 1, 'b' = 2)), a Array(UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2', min_bytes_for_wide_part = 1000000000000, min_rows_for_wide_part = 1000000000000;

-- `s` has 10000 distinct values, `n`, `f` and `e` are NULL in 99% of the rows (the same rows).
INSERT INTO tab SELECT number, toString(number), if(number % 100 = 0, toString(number), NULL),
    if(number % 100 = 0, toFixedString(toString(number), 4), NULL), if(number % 100 = 0, 'a', NULL), [number % 10] FROM numbers(10000);

-- Each query prints 1 when PREWHERE evaluates the pattern condition before `less`.
SELECT 'exact LIKE', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 50 AND s LIKE '1234'));

SELECT 'exact LIKE ESCAPE', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 50 AND s LIKE '12#%' ESCAPE '#'));

SELECT 'exact NOT LIKE', position(p, 'less(') > 0 AND position(p, 'less(') < position(p, 'notLike(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 9950 AND s NOT LIKE '1234'));

SELECT 'LIKE %', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND n LIKE '%'));

SELECT 'ILIKE %', position(p, 'ilike(') > 0 AND position(p, 'ilike(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND n ILIKE '%'));

SELECT 'NOT ILIKE %', position(p, 'notILike(') > 0 AND position(p, 'notILike(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND n NOT ILIKE '%'));

SELECT 'prefix LIKE', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND s LIKE '12%' SETTINGS optimize_rewrite_like_perfect_affix = 1));

SELECT 'suffix LIKE', position(p, 'endsWith(') > 0 AND position(p, 'endsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND s LIKE '%12' SETTINGS optimize_rewrite_like_perfect_affix = 1));

SELECT 'startsWith empty', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND startsWith(n, '')));

SELECT 'FixedString LIKE %', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND f LIKE '%'));

SELECT 'FixedString startsWith empty', position(p, 'startsWith(') > 0 AND position(p, 'startsWith(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND startsWith(f, '')));

SELECT 'Enum LIKE %', position(p, 'like(') > 0 AND position(p, 'like(') < position(p, 'less(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 500 AND e LIKE '%'));

-- The `Array` overload is not a LIKE and stays an unknown condition, after `less`.
SELECT 'startsWith Array', position(p, 'less(') > 0 AND position(p, 'less(') < position(p, 'startsWith(') FROM (
    SELECT arrayStringConcat(extractAll(arrayStringConcat(groupArray(explain), '\n'), 'Prewhere filter column: ([^\n]+)'), ' | ') AS p
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE x < 2000 AND startsWith(a, [1])));

DROP TABLE tab;
