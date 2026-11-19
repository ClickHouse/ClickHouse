-- Tags: no-fasttest
-- no-fasttest: needs the S3 disks.

-- The prefetched read pool prefetches a subcolumn whose streams depend on prefixes from the streams the part cached when
-- it listed them for the subcolumn's size. Such subcolumns match a Memory table, also under PREWHERE steps and with a
-- patch part.

DROP TABLE IF EXISTS t_prefixes_s3;
DROP TABLE IF EXISTS t_prefixes_memory;

CREATE TABLE t_prefixes_s3 (id UInt64, json JSON(SHARED a UInt64, SHARED b String), d Dynamic, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    index_granularity = 64, object_typed_shared_data_serialization_version = 'v2',
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_prefixes_memory (id UInt64, json JSON(a UInt64, b String), d Dynamic, m Map(String, UInt64)) ENGINE = Memory;

INSERT INTO t_prefixes_s3
SELECT
    number,
    concat('{"a":', toString(number), ',"b":"s', toString(number % 100), '","c":', toString(number % 7), '}'),
    if(number % 3 = 0, number::Dynamic, concat('s', toString(number))::Dynamic),
    map(concat('k', toString(number % 10)), number, 'z', number % 5)
FROM numbers(3000);

-- A patch part.
UPDATE t_prefixes_s3 SET d = 'upd', m = map('k1', 1000000, 'z', 0) WHERE id % 11 = 0;

INSERT INTO t_prefixes_memory
SELECT
    number,
    concat('{"a":', toString(number), ',"b":"s', toString(number % 100), '","c":', toString(number % 7), '}'),
    if(number % 11 = 0, 'upd'::Dynamic, if(number % 3 = 0, number::Dynamic, concat('s', toString(number))::Dynamic)),
    if(number % 11 = 0, map('k1', 1000000, 'z', 0), map(concat('k', toString(number % 10)), number, 'z', number % 5))
FROM numbers(3000);

-- Many tasks per part, read by the prefetched pool, with PREWHERE in several steps.
SET max_threads = 8, merge_tree_min_rows_for_concurrent_read = 128, merge_tree_min_bytes_for_concurrent_read = 1,
    remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1, filesystem_prefetch_min_bytes_for_single_read_task = 1,
    filesystem_prefetch_step_marks = 0, filesystem_prefetch_step_bytes = 0,
    optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1;

SELECT 'whole columns',
    (SELECT count() FROM (SELECT id, toString(json), toString(d), toString(m) FROM t_prefixes_s3
        EXCEPT SELECT id, toString(json), toString(d), toString(m) FROM t_prefixes_memory)),
    (SELECT count() FROM (SELECT id, toString(json), toString(d), toString(m) FROM t_prefixes_memory
        EXCEPT SELECT id, toString(json), toString(d), toString(m) FROM t_prefixes_s3));

SELECT 'subcolumns',
    (SELECT count() FROM (SELECT id, json.a, json.b, toString(json.c), toString(d), m['k1'] FROM t_prefixes_s3
        EXCEPT SELECT id, json.a, json.b, toString(json.c), toString(d), m['k1'] FROM t_prefixes_memory)),
    (SELECT count() FROM (SELECT id, json.a, json.b, toString(json.c), toString(d), m['k1'] FROM t_prefixes_memory
        EXCEPT SELECT id, json.a, json.b, toString(json.c), toString(d), m['k1'] FROM t_prefixes_s3));

SELECT 'prewhere steps',
    (SELECT count() FROM (SELECT id, json.b, toString(d), m['k1'] FROM t_prefixes_s3 PREWHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3
        EXCEPT SELECT id, json.b, toString(d), m['k1'] FROM t_prefixes_memory WHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3)),
    (SELECT count() FROM (SELECT id, json.b, toString(d), m['k1'] FROM t_prefixes_memory WHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3
        EXCEPT SELECT id, json.b, toString(d), m['k1'] FROM t_prefixes_s3 PREWHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3)),
    (SELECT count() FROM t_prefixes_s3 PREWHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3)
        = (SELECT count() FROM t_prefixes_memory WHERE json.a % 3 = 0 AND d.UInt64 % 2 = 0 AND m['z'] < 3);

SELECT 'prewhere on a patched column',
    (SELECT count() FROM (SELECT id, json.a FROM t_prefixes_s3 PREWHERE toString(d) = 'upd' AND m['k1'] = 1000000
        EXCEPT SELECT id, json.a FROM t_prefixes_memory WHERE toString(d) = 'upd' AND m['k1'] = 1000000)),
    (SELECT count() FROM t_prefixes_s3 PREWHERE toString(d) = 'upd' AND m['k1'] = 1000000);

DROP TABLE t_prefixes_s3;
DROP TABLE t_prefixes_memory;
