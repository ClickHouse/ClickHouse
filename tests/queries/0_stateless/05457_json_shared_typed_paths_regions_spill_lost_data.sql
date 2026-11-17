-- Tags: no-parallel, no-fasttest, no-distributed-cache
-- no-parallel: the failpoints are global and would break the spills of the other tests
-- no-fasttest: tests with failpoints run alone, and those are kept out of the fast test
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- Errors of the spilling of the per-path buffers of a merge of v2 parts. With a buffer budget of one byte every append
-- spills: the first spill opens a temporary file, and the second write to it hits `serialization_spill_fail_write`,
-- which loses spilled data. With `skip` the regions are skipped, while the merge still moves the first block's
-- granules into the buffers, and the part keeps the plain layout only; with `throw` the merge is aborted. An error
-- writing to the part's disk always aborts the merge. The data reads like the plain type (a Memory table) in
-- every case.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_spill_skip;
DROP TABLE IF EXISTS t_spill_throw;
DROP TABLE IF EXISTS t_spill_part_disk;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8), d UInt64)) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"d":', toString(number % 5), '}') FROM numbers(26);

CREATE TABLE t_spill_skip (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8), SHARED d UInt64))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_error_mode = 'skip';
INSERT INTO t_spill_skip SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"d":', toString(number % 5), '}') FROM numbers(0, 13);
INSERT INTO t_spill_skip SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"d":', toString(number % 5), '}') FROM numbers(13, 13);

CREATE TABLE t_spill_throw AS t_spill_skip
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_error_mode = 'throw';
INSERT INTO t_spill_throw SELECT * FROM t_spill_skip WHERE x < 13;
INSERT INTO t_spill_throw SELECT * FROM t_spill_skip WHERE x >= 13;

CREATE TABLE t_spill_part_disk AS t_spill_skip
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_to_part_disk_only = 1;
INSERT INTO t_spill_part_disk SELECT * FROM t_spill_skip WHERE x < 13;
INSERT INTO t_spill_part_disk SELECT * FROM t_spill_skip WHERE x >= 13;

SYSTEM ENABLE FAILPOINT serialization_spill_fail_write;
OPTIMIZE TABLE t_spill_skip FINAL;
OPTIMIZE TABLE t_spill_throw FINAL; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT serialization_spill_fail_write;

SYSTEM ENABLE FAILPOINT serialization_spill_fail_part_disk;
OPTIMIZE TABLE t_spill_part_disk FINAL; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT serialization_spill_fail_part_disk;

SELECT '-- skip';
SYSTEM FLUSH LOGS part_log;
SELECT 'spill error', ProfileEvents['SerializationSpillErrors'] > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_spill_skip' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_skip' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_spill_skip' AND active;
SELECT 'json',     (SELECT arraySort(groupArray((x, toString(json)))) FROM t_spill_skip) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a',   (SELECT arraySort(groupArray((x, json.a))) FROM t_spill_skip) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.d',   (SELECT arraySort(groupArray((x, json.d))) FROM t_spill_skip) = (SELECT arraySort(groupArray((x, json.d))) FROM t_mem);
SELECT 'json.a mbs=2', (SELECT arraySort(groupArray((x, json.a))) FROM t_spill_skip SETTINGS max_block_size = 2) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);

SELECT '-- throw';
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_spill_throw) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.c', (SELECT arraySort(groupArray((x, json.c))) FROM t_spill_throw) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);

SELECT '-- part disk';
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_spill_part_disk) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.b', (SELECT arraySort(groupArray((x, json.b))) FROM t_spill_part_disk) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);

DROP TABLE t_mem;
DROP TABLE t_spill_skip;
DROP TABLE t_spill_throw;
DROP TABLE t_spill_part_disk;
