-- Tags: no-parallel, no-distributed-cache
-- no-parallel: the failpoints are global and would break the spills of the other tests
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- A spill file that ends before a segment it holds, as an empty or truncated one does, is the spilled data lost: with
-- `skip` the regions are skipped and the part keeps the plain layout only, with `throw` the merge is aborted.
-- `serialization_spill_read_eof` makes every read of a spilled segment start at the end of the file. The data reads
-- like the plain type (a Memory table) in both cases.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_eof_skip;
DROP TABLE IF EXISTS t_eof_throw;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), '}') FROM numbers(26);

CREATE TABLE t_eof_skip (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_error_mode = 'skip';
INSERT INTO t_eof_skip SELECT * FROM t_mem WHERE x < 13;
INSERT INTO t_eof_skip SELECT * FROM t_mem WHERE x >= 13;

CREATE TABLE t_eof_throw AS t_eof_skip
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         merge_max_block_size = 5, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1,
         serialization_spill_error_mode = 'throw';
INSERT INTO t_eof_throw SELECT * FROM t_mem WHERE x < 13;
INSERT INTO t_eof_throw SELECT * FROM t_mem WHERE x >= 13;

SYSTEM ENABLE FAILPOINT serialization_spill_read_eof;
OPTIMIZE TABLE t_eof_skip FINAL;
OPTIMIZE TABLE t_eof_throw FINAL; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SYSTEM DISABLE FAILPOINT serialization_spill_read_eof;

SYSTEM FLUSH LOGS part_log;

SELECT '-- skip';
SELECT 'spill error', ProfileEvents['SerializationSpillErrors'] > 0 FROM system.part_log
WHERE database = currentDatabase() AND table = 't_eof_skip' AND event_type = 'MergeParts'
    AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_eof_skip' AND active);
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_eof_skip' AND active;
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_eof_skip) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.b', (SELECT arraySort(groupArray((x, json.b))) FROM t_eof_skip) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);

SELECT '-- throw';
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_eof_throw' AND active;
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_eof_throw) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);

DROP TABLE t_mem;
DROP TABLE t_eof_skip;
DROP TABLE t_eof_throw;
