-- Region reads of the SHARED typed paths of JSON (v2 layout). A mutation reading a path reads the whole part without
-- marks, through the region's view from its start. An `Array(JSON)` whose arrays are empty in whole granules has
-- granules without rows of the nested JSON, which write nothing to the regions; a read range starting at such a granule
-- makes a call without rows, which must still position the region stream for the calls that continue it.

SET mutations_sync = 1;

DROP TABLE IF EXISTS t_mutation;
CREATE TABLE t_mutation (x UInt64, y UInt64, json JSON(SHARED a UInt64, SHARED b String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760,
         object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_mutation SELECT number, 0, concat('{"a":', toString(number * 10), ',"b":"b', toString(number % 3), '"}') FROM numbers(30);

SELECT '-- mutations';
ALTER TABLE t_mutation UPDATE y = json.a WHERE 1;
SELECT 'update', count(), countIf(y = x * 10) FROM t_mutation;
ALTER TABLE t_mutation DELETE WHERE json.b = 'b1';
SELECT 'delete', count(), countIf(json.a = x * 10), countIf(json.b = 'b1') FROM t_mutation;

DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS t_arr_mem;
CREATE TABLE t_arr_mem (x UInt64, arr Array(JSON(a UInt64, b String))) ENGINE = Memory;
CREATE TABLE t_arr (x UInt64, arr Array(JSON(SHARED a UInt64, SHARED b String)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760,
         object_typed_shared_data_serialization_version = 'v2';
-- Rows 8 to 15, granules 2 and 3, have empty arrays.
INSERT INTO t_arr_mem SELECT number, if(number >= 8 AND number < 16, [], [concat('{"a":', toString(number), ',"b":"s', toString(number), '"}'), concat('{"a":', toString(number + 100), '}')])::Array(JSON(a UInt64, b String)) FROM numbers(24);
INSERT INTO t_arr SELECT number, if(number >= 8 AND number < 16, [], [concat('{"a":', toString(number), ',"b":"s', toString(number), '"}'), concat('{"a":', toString(number + 100), '}')])::Array(JSON(SHARED a UInt64, SHARED b String)) FROM numbers(24);

SELECT '-- granules without rows';
SELECT 'arr.a',               (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr_mem);
SELECT 'arr.b',               (SELECT arraySort(groupArray((x, arr.b))) FROM t_arr SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, arr.b))) FROM t_arr_mem);
SELECT 'arr.a from granule 2',(SELECT arraySort(groupArray((x, arr.a))) FROM t_arr WHERE x >= 8 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr_mem WHERE x >= 8);
SELECT 'arr.a from granule 3',(SELECT arraySort(groupArray((x, arr.a))) FROM t_arr WHERE x >= 12 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr_mem WHERE x >= 12);
SELECT 'arr.a mbs=3',         (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr WHERE x >= 8 SETTINGS max_threads = 1, max_block_size = 3) = (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr_mem WHERE x >= 8);
SELECT 'arr.a mbs=8',         (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr WHERE x >= 8 SETTINGS max_threads = 1, max_block_size = 8) = (SELECT arraySort(groupArray((x, arr.a))) FROM t_arr_mem WHERE x >= 8);
SELECT 'arr',                 (SELECT arraySort(groupArray((x, toString(arr)))) FROM t_arr SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, toString(arr)))) FROM t_arr_mem);

DROP TABLE t_mutation;
DROP TABLE t_arr;
DROP TABLE t_arr_mem;
