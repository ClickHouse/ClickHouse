-- Reads of the SHARED typed paths of JSON from their regions in v2 parts, compared with the plain type (a Memory
-- table): a part without adaptive granularity, a subcolumn of a path, the uncompressed cache and the synchronous load of
-- marks, v1 and v2 parts of one table read together and merged, `CHECK TABLE`, and a table detached and attached again.

SET check_query_single_value_result = 1, max_threads = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_non_adaptive;
DROP TABLE IF EXISTS t_mixed;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8))) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), '}') FROM numbers(40);

CREATE TABLE t_non_adaptive (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 0,
         object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_non_adaptive SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), '}') FROM numbers(40);

SELECT '-- without adaptive granularity';
SELECT 'json.a',       (SELECT arraySort(groupArray((x, json.a))) FROM t_non_adaptive) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.c.size0', (SELECT arraySort(groupArray((x, json.c.size0))) FROM t_non_adaptive) = (SELECT arraySort(groupArray((x, json.c.size0))) FROM t_mem);
SELECT 'json.a mbs=1', (SELECT arraySort(groupArray((x, json.a))) FROM t_non_adaptive SETTINGS max_block_size = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.b WHERE', (SELECT arraySort(groupArray((x, json.b))) FROM t_non_adaptive WHERE x % 9 < 2) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem WHERE x % 9 < 2);
CHECK TABLE t_non_adaptive;

SELECT '-- caches and marks';
SELECT 'uncompressed cache', (SELECT arraySort(groupArray((x, json.b))) FROM t_non_adaptive SETTINGS use_uncompressed_cache = 1) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'uncompressed cache again', (SELECT arraySort(groupArray((x, json.b))) FROM t_non_adaptive SETTINGS use_uncompressed_cache = 1) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'no uncompressed cache', (SELECT arraySort(groupArray((x, json.b))) FROM t_non_adaptive SETTINGS use_uncompressed_cache = 0) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'synchronous marks', (SELECT arraySort(groupArray((x, json.c))) FROM t_non_adaptive SETTINGS load_marks_asynchronously = 0) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);
SELECT 'asynchronous marks', (SELECT arraySort(groupArray((x, json.c))) FROM t_non_adaptive SETTINGS load_marks_asynchronously = 1) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);

-- One part written with v1 and one with v2, read together, then merged into a v2 part.
CREATE TABLE t_mixed (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, index_granularity_bytes = 10485760,
         object_typed_shared_data_serialization_version = 'v1';
INSERT INTO t_mixed SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), '}') FROM numbers(20);
ALTER TABLE t_mixed MODIFY SETTING object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_mixed SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), '}') FROM numbers(20, 20);

SELECT '-- v1 and v2 parts';
SELECT 'json.a', (SELECT arraySort(groupArray((x, json.a))) FROM t_mixed) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mixed) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
OPTIMIZE TABLE t_mixed FINAL;
SELECT 'merged parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_mixed' AND active;
SELECT 'merged json.a', (SELECT arraySort(groupArray((x, json.a))) FROM t_mixed) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'merged json.c', (SELECT arraySort(groupArray((x, json.c))) FROM t_mixed SETTINGS max_block_size = 2) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);
CHECK TABLE t_mixed;

SELECT '-- detached and attached';
DETACH TABLE t_mixed;
ATTACH TABLE t_mixed;
SELECT 'json.b', (SELECT arraySort(groupArray((x, json.b))) FROM t_mixed) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'json',   (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mixed) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);

DROP TABLE t_mem;
DROP TABLE t_non_adaptive;
DROP TABLE t_mixed;
