-- The spill files of a merge of v2 parts are counted as they are written, compressed and uncompressed, both in the
-- temporary storage and on the part's disk. A horizontal merge writes no other temporary data, so the counts equal
-- the totals of all temporary data.

DROP TABLE IF EXISTS t_spill_bytes_tmp;
DROP TABLE IF EXISTS t_spill_bytes_part_disk;

CREATE TABLE t_spill_bytes_tmp (x UInt64, json JSON(SHARED a UInt64, SHARED b String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1048576;

CREATE TABLE t_spill_bytes_part_disk AS t_spill_bytes_tmp
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0, merge_max_block_size = 8192,
         object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1048576,
         serialization_spill_to_part_disk_only = 1;

INSERT INTO t_spill_bytes_tmp SELECT number, concat('{"a":', toString(number), ',"b":"', repeat('b', 100), toString(number % 10), '"}') FROM numbers(20000);
INSERT INTO t_spill_bytes_tmp SELECT number, concat('{"a":', toString(number), ',"b":"', repeat('b', 100), toString(number % 10), '"}') FROM numbers(20000, 20000);
INSERT INTO t_spill_bytes_part_disk SELECT * FROM t_spill_bytes_tmp WHERE x < 20000;
INSERT INTO t_spill_bytes_part_disk SELECT * FROM t_spill_bytes_tmp WHERE x >= 20000;
OPTIMIZE TABLE t_spill_bytes_tmp FINAL;
OPTIMIZE TABLE t_spill_bytes_part_disk FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT
    table,
    ProfileEvents['SerializationSpilledSegments'] > 0,
    ProfileEvents['SerializationSpilledBytes'] > 0,
    ProfileEvents['SerializationSpilledUncompressedBytes'] > ProfileEvents['SerializationSpilledBytes'],
    ProfileEvents['SerializationSpilledBytes'] = ProfileEvents['ExternalProcessingCompressedBytesTotal'],
    ProfileEvents['SerializationSpilledUncompressedBytes'] = ProfileEvents['ExternalProcessingUncompressedBytesTotal']
FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_spill_bytes_tmp', 't_spill_bytes_part_disk') AND event_type = 'MergeParts'
    AND (table, part_name) IN (SELECT table, name FROM system.parts WHERE database = currentDatabase() AND active)
ORDER BY table;

DROP TABLE t_spill_bytes_tmp;
DROP TABLE t_spill_bytes_part_disk;
