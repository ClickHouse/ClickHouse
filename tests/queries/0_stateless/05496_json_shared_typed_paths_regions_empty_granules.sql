-- Tags: no-distributed-cache
-- no-distributed-cache: the temporary storage is the distributed cache, which the spiller does not use

-- Substreams of SHARED paths that have no bytes in most granules: the elements of an array that is mostly empty, and
-- the variants of a Dynamic path, each with values in a few granules only. Their marks at equal offsets are counted
-- instead of stored one by one, both in memory (a large budget) and in the spill files (a budget of one byte). The
-- merged parts read like the plain type (a Memory table) through their regions and pass `CHECK TABLE`.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_in_memory;
DROP TABLE IF EXISTS t_spilled;

CREATE TABLE t_mem (x UInt64, json JSON(a Array(UInt64), d Dynamic)) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{',
    if(number % 100 < 10, concat('"a":', toString(range(number % 4 + 1)), ','), ''),
    multiIf(number < 100, concat('"d":', toString(number)),
            number < 110, concat('"d":"s', toString(number), '"'),
            number >= 500 AND number < 505, concat('"d":["x', toString(number), '"]'),
            '"d":null'),
    '}')
FROM numbers(1000);

CREATE TABLE t_in_memory (x UInt64, json JSON(SHARED a Array(UInt64), SHARED d Dynamic))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 10, index_granularity_bytes = 10485760,
         merge_max_block_size = 7, object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_in_memory SELECT * FROM t_mem WHERE x < 500;
INSERT INTO t_in_memory SELECT * FROM t_mem WHERE x >= 500;
OPTIMIZE TABLE t_in_memory FINAL;

CREATE TABLE t_spilled (x UInt64, json JSON(SHARED a Array(UInt64), SHARED d Dynamic))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 10, index_granularity_bytes = 10485760,
         merge_max_block_size = 7, object_typed_shared_data_serialization_version = 'v2', serialization_spill_max_bytes_to_buffer = 1;
INSERT INTO t_spilled SELECT * FROM t_mem WHERE x < 500;
INSERT INTO t_spilled SELECT * FROM t_mem WHERE x >= 500;
OPTIMIZE TABLE t_spilled FINAL;

SYSTEM FLUSH LOGS part_log;
SELECT table, ProfileEvents['SerializationSpilledSegments'] > 0
FROM system.part_log
WHERE database = currentDatabase() AND table IN ('t_in_memory', 't_spilled') AND event_type = 'MergeParts'
    AND (table, part_name) IN (SELECT table, name FROM system.parts WHERE database = currentDatabase() AND active)
ORDER BY table;

SELECT '-- in memory';
CHECK TABLE t_in_memory;
SELECT 'json',     (SELECT arraySort(groupArray((x, toString(json)))) FROM t_in_memory) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a',   (SELECT arraySort(groupArray((x, json.a))) FROM t_in_memory) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.d',   (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_in_memory) = (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_mem);
SELECT 'json.d.String', (SELECT arraySort(groupArray((x, json.d.String))) FROM t_in_memory) = (SELECT arraySort(groupArray((x, json.d.String))) FROM t_mem);
SELECT json.a, json.d FROM t_in_memory FORMAT Null SETTINGS log_comment = '05496_in_memory_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

SELECT '-- spilled';
CHECK TABLE t_spilled;
SELECT 'json',     (SELECT arraySort(groupArray((x, toString(json)))) FROM t_spilled) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a',   (SELECT arraySort(groupArray((x, json.a))) FROM t_spilled) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.d',   (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_spilled) = (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_mem);
SELECT 'json.d.String', (SELECT arraySort(groupArray((x, json.d.String))) FROM t_spilled) = (SELECT arraySort(groupArray((x, json.d.String))) FROM t_mem);
SELECT json.a, json.d FROM t_spilled FORMAT Null SETTINGS log_comment = '05496_spilled_region_read', enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;

-- The reads of the paths went through the regions of the merged parts.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05496_in_memory_region_read', '05496_spilled_region_read')
ORDER BY log_comment;

DROP TABLE t_mem;
DROP TABLE t_in_memory;
DROP TABLE t_spilled;
