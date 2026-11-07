-- Tags: no-fasttest
-- no-fasttest: requires dictionaries
-- Random settings limits: optimize_dictget_tuple_element=(1, 1); optimize_rewrite_array_filter_length_to_array_count=(1, 1); optimize_qbit_distance_function_reads=(1, 1)

-- `length(arrayFilter(...))`, `dictGet(..., (attrs), key).N` and `*DistanceTransposed(qbit, ...)` rewrites keep a `GROUP BY` key read
-- from the aggregation result: with `ROLLUP`, `CUBE`, `GROUPING SETS` and `group_by_use_nulls`, and when the key is the rewritten
-- function's argument, the result is the one with the rewrite disabled.

SET group_by_use_nulls = 1;

DROP DICTIONARY IF EXISTS d;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS q;
DROP TABLE IF EXISTS qn;

CREATE TABLE src (id UInt64, a UInt64, b String, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (0, 10, 'x', [1]), (1, 11, 'y', [2, 3]), (2, 12, 'z', []);
CREATE DICTIONARY d (id UInt64, a UInt64, b String, arr Array(UInt64)) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);

CREATE TABLE q (id UInt64, v QBit(Float32, 4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO q VALUES (1, [1, 2, 3, 4]), (2, [1, 2, 3, 5]), (3, [0, 0, 0, 0]);
CREATE TABLE qn (id UInt64, v Nullable(QBit(Float32, 4))) ENGINE = MergeTree ORDER BY id;
INSERT INTO qn VALUES (1, [1, 2, 3, 4]), (2, NULL), (3, [0, 0, 0, 0]);

-- `length(arrayFilter(...))` -> `arrayCount(...)`.
SELECT 'ac', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL;
SELECT 'ac cube', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY k WITH CUBE ORDER BY ALL;
SELECT 'ac grouping sets', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL;
SELECT 'ac having', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP HAVING k IS NULL OR k > 0 ORDER BY ALL;
SELECT 'ac expression', toString(length(arrayFilter(x -> x > 0, [number]))) AS s FROM numbers(3) GROUP BY length(arrayFilter(x -> x > 0, [number])) WITH ROLLUP ORDER BY ALL;
SELECT 'ac key of a key', length(arrayFilter(x -> x > 0, [number])) AS k, toString(k) AS s, count() FROM numbers(3) GROUP BY ROLLUP(k, s) ORDER BY ALL;
SELECT 'ac array key', a, length(arrayFilter(x -> x > 0, a)) AS k, count() FROM (SELECT [number] AS a FROM numbers(3)) GROUP BY ROLLUP(a, k) ORDER BY ALL;
SELECT 'ac legacy', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL SETTINGS array_count_legacy_uint32_result = 1;
SELECT 'ac remote', length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY k WITH ROLLUP ORDER BY ALL;
-- The `arrayFilter` is the key.
SELECT 'ac filter key', length(f) AS k, count() FROM (SELECT [number] AS a FROM numbers(3)) GROUP BY arrayFilter(x -> x > 0, a) AS f ORDER BY ALL;
SELECT 'ac filter key rollup', length(f) AS k, count() FROM (SELECT [number] AS a FROM numbers(3)) GROUP BY ROLLUP(arrayFilter(x -> x > 0, a) AS f) ORDER BY ALL;

-- `dictGet(..., (attrs), key).N` -> `dictGet(..., attr, key)`.
SELECT 'dg', dictGet(currentDatabase() || '.d', ('a', 'b'), number).1 AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL;
SELECT 'dg named', dictGet(currentDatabase() || '.d', ('a', 'b'), number).b AS k, count() FROM numbers(3) GROUP BY k WITH CUBE ORDER BY ALL;
SELECT 'dg default', dictGetOrDefault(currentDatabase() || '.d', ('a', 'b'), number, (toUInt64(0), '')).1 AS k, count() FROM numbers(5) GROUP BY k WITH ROLLUP ORDER BY ALL;
SELECT 'dg key of a key', dictGet(currentDatabase() || '.d', ('a', 'b'), number) AS t, t.1 AS k, count() FROM numbers(3) GROUP BY ROLLUP(t, k) ORDER BY ALL;
SELECT 'dg array', dictGet(currentDatabase() || '.d', ('arr', 'b'), number).1 AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL;
SELECT 'dg low cardinality', dictGet(currentDatabase() || '.d', ('a', 'b'), toLowCardinality(number)).2 AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL;
-- The tuple `dictGet` is the key.
SELECT 'dg tuple key', dictGet(currentDatabase() || '.d', ('a', 'b'), number) AS t, t.1 AS k, count() FROM numbers(3) GROUP BY t ORDER BY ALL;
SELECT 'dg tuple key rollup', dictGet(currentDatabase() || '.d', ('a', 'b'), number) AS t, t.1 AS k, count() FROM numbers(3) GROUP BY ROLLUP(t) ORDER BY ALL;

-- `*DistanceTransposed(qbit, ...)` -> reads of the bit planes of `qbit`.
-- The distances are rounded because the low bits depend on the distance kernel.
SELECT 'qb', round(L2DistanceTransposed(v, [1., 2., 3., 4.], 32), 6) AS k, count() FROM q GROUP BY L2DistanceTransposed(v, [1., 2., 3., 4.], 32) WITH ROLLUP ORDER BY ALL;
SELECT 'qb cosine', round(cosineDistanceTransposed(v, [1., 2., 3., 4.], 16), 6) AS k, count() FROM q WHERE id < 3 GROUP BY cosineDistanceTransposed(v, [1., 2., 3., 4.], 16) WITH CUBE ORDER BY ALL;
SELECT 'qb other key', id, round(L2DistanceTransposed(v, [1., 2., 3., 4.], 32), 6) AS k, count() FROM q GROUP BY ROLLUP(id, L2DistanceTransposed(v, [1., 2., 3., 4.], 32)) ORDER BY ALL;
SELECT 'qb nullable', round(L2DistanceTransposed(v, [1., 2., 3., 4.], 32), 6) AS k, count() FROM qn GROUP BY L2DistanceTransposed(v, [1., 2., 3., 4.], 32) WITH ROLLUP ORDER BY ALL;
-- The `QBit` column is the key.
SELECT 'qb column key', round(L2DistanceTransposed(v, [1., 2., 3., 4.], 32), 6) AS k, count() FROM q GROUP BY v ORDER BY ALL;
-- A `QBit` column of an outer query, read by a correlated subquery.
SELECT 'qb correlated', id, (SELECT round(L2DistanceTransposed(q.v, [1., 2., 3., 4.], 32), 6)) AS k FROM q ORDER BY id;
SELECT 'qb correlated key', toString(v) AS s, (SELECT round(L2DistanceTransposed(q.v, [1., 2., 3., 4.], 32), 6)) AS k, count() FROM q GROUP BY v ORDER BY s;

-- The rewrites still apply to the key and to its `Nullable` copies.
SELECT 'ac explain',
    countIf(explain ILIKE '%function_name: arrayCount, function_type: ordinary, result_type: Nullable(UInt64)%'),
    countIf(explain ILIKE '%function_name: arrayCount, function_type: ordinary, result_type: UInt64%'),
    countIf(explain ILIKE '%function_name: length,%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(arrayFilter(x -> x > 0, [number])) AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL);
SELECT 'dg explain',
    countIf(explain ILIKE '%function_name: tupleElement,%'),
    countIf(explain ILIKE '%function_name: dictGet, function_type: ordinary, result_type: Nullable(UInt64)%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT dictGet(currentDatabase() || '.d', ('a', 'b'), number).1 AS k, count() FROM numbers(3) GROUP BY k WITH ROLLUP ORDER BY ALL);
SELECT 'qb explain',
    countIf(explain ILIKE '%constant_value_type: Nullable(UInt8)%'),
    countIf(explain ILIKE '%column_name: v.1,%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT L2DistanceTransposed(v, [1., 2., 3., 4.], 32) AS k, count() FROM q GROUP BY k WITH ROLLUP ORDER BY ALL);

DROP DICTIONARY d;
DROP TABLE src;
DROP TABLE q;
DROP TABLE qn;
