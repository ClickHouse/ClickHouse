-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- `PartsSplitter` fault injection also reads the sorting key, which changes the compressed bytes of only the query it fires on.

-- A key of a bucketed `Map` reads only its bucket, but a key with a float zero reads all buckets,
-- because the other zero can be in another bucket. Compare the compressed bytes the two lookups read.

SET use_uncompressed_cache = 0, use_columns_cache = 0, optimize_functions_to_subcolumns = 1, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_map_buckets_zero_key_read_volume;
CREATE TABLE t_map_buckets_zero_key_read_volume (id UInt64, m Map(Float64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0, max_buckets_in_map = 4, min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_types';

-- Every map has the keys 1 to 16 except 8, which is a zero: `-0.0` in the even rows and `0.0` in the odd rows.
INSERT INTO t_map_buckets_zero_key_read_volume
SELECT number, mapFromArrays(
    arrayMap(i -> if(i = 7, if(number % 2 = 0, -0.0, 0.0), toFloat64(i + 1)), range(16)),
    arrayMap(i -> number + i, range(16)))
FROM numbers(10000);

SELECT sum(m[3.0]) FROM t_map_buckets_zero_key_read_volume SETTINGS log_comment = '05407_one_bucket';
SELECT sum(m[0.0]) FROM t_map_buckets_zero_key_read_volume SETTINGS log_comment = '05407_all_buckets';

SYSTEM FLUSH LOGS query_log;

SELECT maxIf(ProfileEvents['ReadCompressedBytes'], log_comment = '05407_all_buckets')
    > 2 * maxIf(ProfileEvents['ReadCompressedBytes'], log_comment = '05407_one_bucket')
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05407_one_bucket', '05407_all_buckets');

DROP TABLE t_map_buckets_zero_key_read_volume;
