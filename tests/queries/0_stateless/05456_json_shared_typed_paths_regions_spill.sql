-- Tags: no-distributed-cache
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- Spilling of the per-path buffers of a merge of v2 parts. With a buffer budget of one byte every granule is spilled. A
-- cap of the temporary storage of a few KiB takes some of the regions and sends the rest to the part's disk; with
-- `serialization_spill_to_part_disk_only` all of them go there. The merged part has its regions, reads like the plain
-- type (a Memory table), and passes `CHECK TABLE`.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_spill_cap;
DROP TABLE IF EXISTS t_spill_part_disk;

CREATE TABLE t_mem (x UInt64, j1 JSON(a UInt64, b String, c Array(UInt8)), j2 JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
INSERT INTO t_mem SELECT number,
    concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}'),
    concat('{"a":', toString(number * 7), ',"b":"', repeat('c', number % 13), '","c":', toString(range(number % 3)), '}')
FROM numbers(100);

-- Two JSON columns: the cap is shared by the spills of the part write.
CREATE TABLE t_spill_cap (x UInt64, j1 JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)), j2 JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_max_temporary_data_size = 4096;
INSERT INTO t_spill_cap SELECT number,
    concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}'),
    concat('{"a":', toString(number * 7), ',"b":"', repeat('c', number % 13), '","c":', toString(range(number % 3)), '}')
FROM numbers(50);
INSERT INTO t_spill_cap SELECT number,
    concat('{"a":', toString(number), ',"b":"', repeat('b', number % 11), '","c":', toString(range(number % 5)), '}'),
    concat('{"a":', toString(number * 7), ',"b":"', repeat('c', number % 13), '","c":', toString(range(number % 3)), '}')
FROM numbers(50, 50);
OPTIMIZE TABLE t_spill_cap FINAL;

CREATE TABLE t_spill_part_disk (x UInt64, j1 JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)), j2 JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_to_part_disk_only = 1;
INSERT INTO t_spill_part_disk SELECT * FROM t_spill_cap WHERE x < 50;
INSERT INTO t_spill_part_disk SELECT * FROM t_spill_cap WHERE x >= 50;
OPTIMIZE TABLE t_spill_part_disk FINAL;

SYSTEM FLUSH LOGS part_log;

-- An insert writes a part from one block, which holds the data already: nothing is spilled, whatever the budget.
SELECT 'inserts spilled', countIf(ProfileEvents['SerializationSpilledSegments'] > 0), count()
FROM system.part_log WHERE database = currentDatabase() AND table IN ('t_spill_cap', 't_spill_part_disk') AND event_type = 'NewPart';

SELECT '-- cap: the temporary storage and the part disk';
SELECT 'spilled', ProfileEvents['SerializationSpilledToPartDisk'] > 0, ProfileEvents['SerializationSpilledSegments'] > ProfileEvents['SerializationSpilledToPartDisk']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_spill_cap' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_cap' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_cap' AND active;
CHECK TABLE t_spill_cap;
SELECT 'j1',   (SELECT arraySort(groupArray((x, toString(j1)))) FROM t_spill_cap) = (SELECT arraySort(groupArray((x, toString(j1)))) FROM t_mem);
SELECT 'j2.b', (SELECT arraySort(groupArray((x, j2.b))) FROM t_spill_cap) = (SELECT arraySort(groupArray((x, j2.b))) FROM t_mem);
SELECT 'j1.c', (SELECT arraySort(groupArray((x, j1.c))) FROM t_spill_cap) = (SELECT arraySort(groupArray((x, j1.c))) FROM t_mem);
SELECT j1.c FROM t_spill_cap FORMAT Null SETTINGS log_comment = '05456_cap_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

SELECT '-- the part disk only';
SELECT 'spilled', ProfileEvents['SerializationSpilledSegments'] > 0, ProfileEvents['SerializationSpilledToPartDisk'] = ProfileEvents['SerializationSpilledSegments']
FROM system.part_log WHERE database = currentDatabase() AND table = 't_spill_part_disk' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_part_disk' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_part_disk' AND active;
CHECK TABLE t_spill_part_disk;
SELECT 'j1',   (SELECT arraySort(groupArray((x, toString(j1)))) FROM t_spill_part_disk) = (SELECT arraySort(groupArray((x, toString(j1)))) FROM t_mem);
SELECT 'j2.a', (SELECT arraySort(groupArray((x, j2.a))) FROM t_spill_part_disk) = (SELECT arraySort(groupArray((x, j2.a))) FROM t_mem);
SELECT j2.a FROM t_spill_part_disk FORMAT Null SETTINGS log_comment = '05456_part_disk_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

-- The reads of one path went through the regions of the merged parts (read from the parts, not the columns cache, in
-- the query itself, not in parallel replicas).
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05456_cap_region_read', '05456_part_disk_region_read')
ORDER BY log_comment;

DROP TABLE t_mem;
DROP TABLE t_spill_cap;
DROP TABLE t_spill_part_disk;
