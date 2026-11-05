-- Different `JSON` and `Dynamic` values must not hash the same in `uniq*`, `DISTINCT`, `arrayDistinct` and hash `JOIN`.

-- `arrayElement` rejects `JSON` keys, only the key subcolumn reads them.
SET optimize_functions_to_subcolumns = 1;
-- It rewrites a lone `uniqExact` into a `GROUP BY`, which serializes the values instead of hashing them.
SET count_distinct_optimization = 0;

-- The paths of one object moved to the next object of an array (#129458).
WITH data AS (SELECT arrayJoin(['{"x":[{"a":1},{"b":2,"c":3}]}', '{"x":[{"a":1,"b":2},{"c":3}]}'])::JSON AS j)
SELECT count(DISTINCT j), uniqExact(j), uniq(j) FROM data;
WITH data AS (SELECT arrayJoin(['{"x":[{"a":1},{"b":2,"c":3}]}', '{"x":[{"a":1,"b":2},{"c":3}]}'])::JSON(max_dynamic_paths = 0) AS j)
SELECT uniqExact(j) FROM data;
SELECT count() FROM (SELECT DISTINCT j FROM (SELECT arrayJoin(['{"x":[{"a":1},{"b":2,"c":3}]}', '{"x":[{"a":1,"b":2},{"c":3}]}'])::JSON AS j));
SELECT length(arrayDistinct(['{"x":[{"a":1},{"b":2,"c":3}]}'::JSON, '{"x":[{"a":1,"b":2},{"c":3}]}'::JSON]));
-- Other join algorithms compare the values.
SELECT count() FROM (SELECT '{"x":[{"a":1},{"b":2,"c":3}]}'::JSON AS j) AS l
    INNER JOIN (SELECT '{"x":[{"a":1,"b":2},{"c":3}]}'::JSON AS j) AS r USING (j) SETTINGS join_algorithm = 'hash';
SELECT uniqExact(t) FROM (SELECT arrayJoin([tuple('{"a":1}'::JSON, '{"b":2,"c":3}'::JSON), tuple('{"a":1,"b":2}'::JSON, '{"c":3}'::JSON)]) AS t);

-- A path that is a prefix of another path: `x` + `UInt64` and `xU` + `Int64` with the same bits.
SELECT uniqExact(j) FROM (SELECT arrayJoin(['{"x":9223372036854775808}', '{"xU":-9223372036854775808}'])::JSON AS j);

-- A type name that is a prefix of another type name: `Date` + bytes `32` and `Date32`.
SELECT uniqExact(a), count(DISTINCT a) FROM
(
    SELECT [toDate(12851)::Dynamic, NULL, NULL, NULL, NULL, toDate32(-1)::Dynamic] AS a
    UNION ALL
    SELECT [toDate32(-1)::Dynamic, toDate(12851)::Dynamic, NULL, NULL, NULL, NULL]
);
-- The same in the shared variant, where `Dynamic(max_types=0)` stores every value.
SELECT uniqExact(a), count(DISTINCT a), min(arrayCount(x -> isDynamicElementInSharedData(x), a)) FROM
(
    SELECT [toDate(12851)::Dynamic, NULL, NULL, NULL, NULL, toDate32(-1)::Dynamic]::Array(Dynamic(max_types=0)) AS a
    UNION ALL
    SELECT [toDate32(-1)::Dynamic, toDate(12851)::Dynamic, NULL, NULL, NULL, NULL]::Array(Dynamic(max_types=0))
);

-- A bucketed `Map` with `JSON` keys is written in one bucket, and its keys are found.
DROP TABLE IF EXISTS t_map_json_key_buckets;
-- Only `with_types` serialization info writes buckets, and a wide part lists every bucket stream.
CREATE TABLE t_map_json_key_buckets (id UInt64, m Map(JSON, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0, max_buckets_in_map = 16,
    serialization_info_version = 'with_types', min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;
INSERT INTO t_map_json_key_buckets SELECT number, mapFromArrays(arrayMap(i -> ('{"a":' || toString(i) || '}')::JSON, range(32)), arrayMap(i -> number + i, range(32))) FROM numbers(100);
SELECT has(substreams, 'm.buckets_info'), has(substreams, 'm.bucket_indexes')
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_map_json_key_buckets' AND column = 'm' AND active;
SELECT sum(m['{"a":7}'::JSON]), sum(m['{"a":31}'::JSON]), sum(m['{"a":32}'::JSON]) FROM t_map_json_key_buckets;
DROP TABLE t_map_json_key_buckets;
