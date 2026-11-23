-- The JSON shared data of a part uses `object_shared_data_serialization_version` only if the part reaches the
-- minimal size and is not created by an insert (with `use_object_shared_data_serialization_version_for_small_parts_on_insert`),
-- otherwise `object_shared_data_serialization_version_for_small_parts`.
-- Each case uses its own partition, so background merges cannot combine parts of different cases.
-- Every row holds about 2000 bytes of shared data, so 8 rows are below the minimal size of 21000 bytes and 12 rows are above it
-- under any randomized settings.

DROP TABLE IF EXISTS test;

CREATE TABLE test (p UInt32, id UInt64, json JSON(max_dynamic_paths = 0))
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_small_parts = 'map_with_buckets',
    min_bytes_for_object_shared_data_main_serialization_version = 21000,
    use_object_shared_data_serialization_version_for_small_parts_on_insert = 1;

-- Inserted parts are small regardless of their size.
INSERT INTO test SELECT 1, number, toJSONString(map('a', repeat('a', 2000))) FROM numbers(4);
INSERT INTO test SELECT 1, number, toJSONString(map('b', repeat('b', 2000))) FROM numbers(4);
INSERT INTO test SELECT 2, number, toJSONString(map('a', repeat('a', 2000))) FROM numbers(4);
INSERT INTO test SELECT 2, number, toJSONString(map('b', repeat('b', 2000))) FROM numbers(4);
INSERT INTO test SELECT 2, number, toJSONString(map('c', repeat('c', 2000))) FROM numbers(4);
INSERT INTO test SELECT 3, number, toJSONString(map('a', repeat('a', 2000))) FROM numbers(12);

SELECT 'inserts';
SELECT partition, rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json'
ORDER BY partition, name;

-- Merged parts are decided by the size: 8 rows are below the minimal size, 12 rows reach it, including a merge of a single part.
OPTIMIZE TABLE test PARTITION 1 FINAL;
OPTIMIZE TABLE test PARTITION 2 FINAL;
OPTIMIZE TABLE test PARTITION 3 FINAL;

SELECT 'merges';
SELECT partition, rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json'
ORDER BY partition, name;

-- A mutation that rewrites the JSON column decides by the size of the source part.
ALTER TABLE test MODIFY SETTING min_bytes_for_object_shared_data_main_serialization_version = 1000000;
ALTER TABLE test UPDATE json = toJSONString(map('d', repeat('d', 2000))) WHERE p = 2 SETTINGS mutations_sync = 2;

SELECT 'mutation';
SELECT partition, rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json'
ORDER BY partition, name;

-- Without `use_object_shared_data_serialization_version_for_small_parts_on_insert`, inserted parts are decided by the size.
ALTER TABLE test MODIFY SETTING min_bytes_for_object_shared_data_main_serialization_version = 21000, use_object_shared_data_serialization_version_for_small_parts_on_insert = 0;
INSERT INTO test SELECT 4, number, toJSONString(map('a', repeat('a', 2000))) FROM numbers(4);
INSERT INTO test SELECT 5, number, toJSONString(map('a', repeat('a', 2000))) FROM numbers(12);

SELECT 'inserts decided by size';
SELECT partition, rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json' AND partition IN ('4', '5')
ORDER BY partition, name;

SELECT 'data';
SELECT p, count(), arraySort(groupUniqArray(JSONAllPaths(json))), sum(length(toJSONString(json))) FROM test GROUP BY p ORDER BY p;

DROP TABLE test;

-- `optimize_on_insert` gives a part inserted into a non-ordinary engine the level 1, but it is still an inserted part.
-- A merged projection part follows the same rules as its parent part.
SET optimize_on_insert = 1;

CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths = 0), PROJECTION pr (SELECT json, id ORDER BY id))
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    deduplicate_merge_projection_mode = 'rebuild',
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_small_parts = 'map_with_buckets',
    min_bytes_for_object_shared_data_main_serialization_version = 0,
    use_object_shared_data_serialization_version_for_small_parts_on_insert = 1;

INSERT INTO test SELECT number, '{"a" : 1}' FROM numbers(4);
INSERT INTO test SELECT number + 4, '{"b" : 2}' FROM numbers(4);

SELECT 'optimize_on_insert';
SELECT level, rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json'
ORDER BY name;

OPTIMIZE TABLE test FINAL;
SELECT rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json';

SELECT 'projection';
SELECT count(), arraySort(groupUniqArray(JSONAllPaths(json))), sum(json.a::UInt64), sum(json.b::UInt64) FROM test
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE test;

-- The old name of `object_shared_data_serialization_version_for_small_parts` is an alias.
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths = 0))
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map_with_buckets',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    use_object_shared_data_serialization_version_for_small_parts_on_insert = 1;

INSERT INTO test SELECT number, '{"a" : 1}' FROM numbers(4);

SELECT 'alias';
SELECT rows, arrayExists(s -> s LIKE '%.object_shared_data.%.paths_marks', substreams) AS advanced
FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test' AND active AND column = 'json';

DROP TABLE test;
