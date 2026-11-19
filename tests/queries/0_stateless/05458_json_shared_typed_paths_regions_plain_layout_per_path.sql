-- The readers of the SHARED typed paths of a v2 part choose per path between its region and the plain layout. A
-- sub-object with more paths than `object_shared_data_max_sub_object_paths_to_read_separately`, and the whole column,
-- read their paths from the plain layout; every other reader of such a path reads it from there too, and the other
-- paths keep their regions. A granule cut across read calls (`max_block_size` below the granule) must continue each
-- path where its own reading stopped. Results are compared with the plain type (a Memory table); the read-side profile
-- events show which layouts were read.

SET max_threads = 1, max_block_size = 3;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_v2;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, x.p1 UInt64, x.p2 String, x.y.q1 UInt64, x.y.q2 String)) ENGINE = Memory;
CREATE TABLE t_v2 (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED x.p1 UInt64, SHARED x.p2 String, SHARED x.y.q1 UInt64, SHARED x.y.q2 String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 10485760,
         object_typed_shared_data_serialization_version = 'v2';

INSERT INTO t_mem SELECT number, concat('{"a":', toString(number), ',"b":"b', toString(number % 5),
    '","x":{"p1":', toString(number * 2), ',"p2":"', repeat('p', number % 4), '","y":{"q1":', toString(number * 3), ',"q2":"q', toString(number % 6), '"}}}') FROM numbers(40);
INSERT INTO t_v2 SELECT number, concat('{"a":', toString(number), ',"b":"b', toString(number % 5),
    '","x":{"p1":', toString(number * 2), ',"p2":"', repeat('p', number % 4), '","y":{"q1":', toString(number * 3), ',"q2":"q', toString(number % 6), '"}}}') FROM numbers(40);

SELECT '-- every sub-object over regions';
SELECT 'json.a, json.^x', (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_v2) = (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_mem);
SELECT 'json.^x, json.^x.y', (SELECT arraySort(groupArray((x, toString(json.^x), toString(json.^x.y)))) FROM t_v2) = (SELECT arraySort(groupArray((x, toString(json.^x), toString(json.^x.y)))) FROM t_mem);

SELECT '-- a wide sub-object reads the plain layout, the other paths their regions';
SET object_shared_data_max_sub_object_paths_to_read_separately = 0;
SELECT 'json.a, json.^x', (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_v2) = (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_mem);
SELECT 'json.x.p1, json.^x, json.b', (SELECT arraySort(groupArray((x, json.x.p1, toString(json.^x), json.b))) FROM t_v2) = (SELECT arraySort(groupArray((x, json.x.p1, toString(json.^x), json.b))) FROM t_mem);
SELECT 'json.a, json.^x WHERE', (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_v2 WHERE x % 7 < 3) = (SELECT arraySort(groupArray((x, json.a, toString(json.^x)))) FROM t_mem WHERE x % 7 < 3);

SELECT '-- a narrow sub-object inside a wide one';
SET object_shared_data_max_sub_object_paths_to_read_separately = 2;
SELECT 'json.^x, json.^x.y', (SELECT arraySort(groupArray((x, toString(json.^x), toString(json.^x.y)))) FROM t_v2) = (SELECT arraySort(groupArray((x, toString(json.^x), toString(json.^x.y)))) FROM t_mem);
SELECT 'json.^x.y, json.a', (SELECT arraySort(groupArray((x, toString(json.^x.y), json.a))) FROM t_v2) = (SELECT arraySort(groupArray((x, toString(json.^x.y), json.a))) FROM t_mem);
SET object_shared_data_max_sub_object_paths_to_read_separately = 32;

SELECT '-- the whole column with a path';
SELECT 'json, json.a', (SELECT arraySort(groupArray((x, toString(json), json.a))) FROM t_v2) = (SELECT arraySort(groupArray((x, toString(json), json.a))) FROM t_mem);

SELECT '-- the layouts read';
-- Read from the part, not the columns cache, in the query itself, not in parallel replicas.
SET enable_reads_from_columns_cache = 0, enable_parallel_replicas = 0;
SELECT json.a FROM t_v2 FORMAT Null SETTINGS log_comment = '05458_path';
SELECT json.a, json.^x FROM t_v2 FORMAT Null SETTINGS log_comment = '05458_narrow_sub_object';
SELECT json.a, json.^x FROM t_v2 FORMAT Null SETTINGS log_comment = '05458_wide_sub_object', object_shared_data_max_sub_object_paths_to_read_separately = 0;
SELECT json, json.a FROM t_v2 FORMAT Null SETTINGS log_comment = '05458_whole_column';
-- The tiny blocks are for the reads above; the query log is scanned with normal ones.
SET max_block_size = DEFAULT;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05458_path', '05458_narrow_sub_object', '05458_wide_sub_object', '05458_whole_column')
ORDER BY log_comment;

DROP TABLE t_mem;
DROP TABLE t_v2;
