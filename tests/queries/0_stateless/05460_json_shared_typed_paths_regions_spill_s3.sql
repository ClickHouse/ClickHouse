-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs S3
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- Spilling of the per-path buffers of a merge of v2 parts to an S3 disk, into a temporary directory of the table. With
-- `serialization_spill_to_part_disk_only` every segment is spilled there; with a cap of the temporary storage of a few
-- KiB some segments go to the temporary storage and the rest there. The spilled segments read back into the merged
-- part, which reads like the plain type (a Memory table) and passes `CHECK TABLE`, and no spill file is left.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_s3_part_disk;
DROP TABLE IF EXISTS t_s3_cap;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}') FROM numbers(100);

CREATE TABLE t_s3_part_disk (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3,
         index_granularity_bytes = 10485760, merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2',
         serialization_spill_max_bytes_to_buffer = 1, serialization_spill_to_part_disk_only = 1;
INSERT INTO t_s3_part_disk SELECT number, concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}') FROM numbers(50);
INSERT INTO t_s3_part_disk SELECT number, concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}') FROM numbers(50, 50);
OPTIMIZE TABLE t_s3_part_disk FINAL;

CREATE TABLE t_s3_cap (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3,
         index_granularity_bytes = 10485760, merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2',
         serialization_spill_max_bytes_to_buffer = 1, serialization_spill_max_temporary_data_size = 2048;
INSERT INTO t_s3_cap SELECT * FROM t_s3_part_disk WHERE x < 50;
INSERT INTO t_s3_cap SELECT * FROM t_s3_part_disk WHERE x >= 50;
OPTIMIZE TABLE t_s3_cap FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT '-- the part disk only';
SELECT 'spilled', ProfileEvents['SerializationSpilledSegments'] > 0, ProfileEvents['SerializationSpilledToPartDisk'] = ProfileEvents['SerializationSpilledSegments']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_s3_part_disk' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_s3_part_disk' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_s3_part_disk' AND active;
CHECK TABLE t_s3_part_disk;
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_s3_part_disk) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.c', (SELECT arraySort(groupArray((x, json.c))) FROM t_s3_part_disk) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);
SELECT json.b FROM t_s3_part_disk FORMAT Null SETTINGS log_comment = '05460_part_disk_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

SELECT '-- the cap: the temporary storage and the part disk';
SELECT 'spilled', ProfileEvents['SerializationSpilledToPartDisk'] > 0, ProfileEvents['SerializationSpilledSegments'] > ProfileEvents['SerializationSpilledToPartDisk']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_s3_cap' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_s3_cap' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_s3_cap' AND active;
CHECK TABLE t_s3_cap;
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_s3_cap) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a', (SELECT arraySort(groupArray((x, json.a))) FROM t_s3_cap) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT json.a FROM t_s3_cap FORMAT Null SETTINGS log_comment = '05460_cap_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

-- No spill directory is left on the disk in the directories of the two tables.
SELECT 'spill files left', count()
FROM system.remote_data_paths
WHERE disk_name = 's3_disk' AND local_path LIKE '%tmp\\_data\\_%'
    AND (local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_s3_part_disk'), '%')
        OR local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_s3_cap'), '%'));

-- The reads of one path went through the regions of the merged parts.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05460_part_disk_region_read', '05460_cap_region_read')
ORDER BY log_comment;

DROP TABLE t_mem;
DROP TABLE t_s3_part_disk;
DROP TABLE t_s3_cap;
