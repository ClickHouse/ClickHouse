-- Tags: no-fasttest, no-parallel-replicas, no-distributed-cache
-- no-fasttest: needs the s3_cache storage policy, a cache disk over MinIO.
-- no-parallel-replicas: the replicas would size and read the parts, so the requests are not counted on the initiator.
-- no-distributed-cache: the distributed cache would serve the reads instead of the filesystem cache.

-- Sizing a JSON path or a key of a bucketed Map for PREWHERE reads the serialization prefixes of every part:
-- the filesystem cache must serve them when it holds them.

SET enable_filesystem_cache = 1;
SET read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0;
SET use_page_cache_for_disks_without_file_cache = 0;
SET allow_calculating_subcolumns_sizes_for_merge_tree_reading = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET use_query_cache = 0;
SET use_statistics = 0;

DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_map;

CREATE TABLE t_json (d Date, id UInt64, j JSON)
ENGINE = MergeTree PARTITION BY d ORDER BY id
SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_map (d Date, id UInt64, m Map(String, String))
ENGINE = MergeTree PARTITION BY d ORDER BY id
SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    serialization_info_version = 'with_types';

INSERT INTO t_json SELECT toDate('2026-01-01') + number % 50, number, concat('{"k1":', toString(number), ',"k2":', toString(number), '}')
FROM numbers(10000) SETTINGS enable_filesystem_cache_on_write_operations = 1;
INSERT INTO t_map SELECT toDate('2026-01-01') + number % 50, number, map('k' || toString(number % 10), toString(number))
FROM numbers(10000) SETTINGS enable_filesystem_cache_on_write_operations = 1;

-- Every file is in the filesystem cache now.
SELECT j FROM t_json FORMAT Null;
SELECT m FROM t_map FORMAT Null;

SELECT count() FROM t_json WHERE j.k2::Int64 > 0 SETTINGS log_comment = '05839_json';
SELECT count() FROM t_json WHERE j.k1::Int64 > 0 SETTINGS log_comment = '05839_json_sizes_off', allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;
SELECT count() FROM t_map WHERE m['k1'] != '' SETTINGS log_comment = '05839_map';
SELECT count() FROM t_map WHERE m['k2'] != '' SETTINGS log_comment = '05839_map_sizes_off', allow_calculating_subcolumns_sizes_for_merge_tree_reading = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['S3GetObject'] > 0, ProfileEvents['CachedReadBufferReadFromSourceBytes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05839_json', '05839_json_sizes_off', '05839_map', '05839_map_sizes_off')
ORDER BY event_time_microseconds;

DROP TABLE t_json;
DROP TABLE t_map;
