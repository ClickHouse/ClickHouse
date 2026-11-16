-- Reads of SHARED typed paths of JSON type over the regions of v2 parts, which seek inside their own shared data by the
-- internal marks, compared with a Memory table: objects nested in the shared data of the path, sub-objects of it, a JSON
-- typed path inside the JSON of the path, the advanced_chunked shared data with every substream spilled, and the shared
-- data in several buckets.

SET max_threads = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_chunked_spilled;
DROP TABLE IF EXISTS t_buckets;

CREATE TABLE t_mem (x UInt64, json JSON(h JSON(max_dynamic_paths = 1), g JSON(i JSON(max_dynamic_paths = 1), c UInt32)))
ENGINE = Memory;

-- Merged from blocks of 7 rows, so a granule of 16 rows takes several serialize calls and chunks of 3 rows cross granules.
CREATE TABLE t_chunked_spilled (x UInt64, json JSON(SHARED h JSON(max_dynamic_paths = 1), SHARED g JSON(i JSON(max_dynamic_paths = 1), c UInt32)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced_chunked',
         object_shared_data_target_chunk_rows = 3, merge_max_block_size = 7, serialization_spill_max_bytes_to_buffer = 1;

CREATE TABLE t_buckets AS t_chunked_spilled ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced',
         object_shared_data_buckets_for_wide_part = 4, merge_max_block_size = 7;

-- Only `h.k0` and `g.i.a` are dynamic paths; the rest of `h` and `g.i`, nested objects included, is in their shared data.
CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"h":{"k0":', toString(number), ',"o":{"k":', toString(number), ',"s":"o', toString(number), '"',
        if(number % 3 = 0, concat(',"m":{"z":', toString(number % 7), '}'), ''), '},"w":"w', toString(number), '"}',
    ',"g":{"i":{"a":', toString(number), ',"b":"b', toString(number), '","d":{"e":', toString(number % 5), '}},"c":', toString(number), '}',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_chunked_spilled SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_chunked_spilled SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_chunked_spilled SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_chunked_spilled SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_chunked_spilled FINAL;
INSERT INTO t_buckets SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_buckets SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_buckets SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_buckets SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_buckets FINAL;

SELECT 'parts', table, count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_chunked_spilled', 't_buckets') AND active GROUP BY table ORDER BY table;

SELECT '-- advanced_chunked, spilled';
SELECT 'json.h.o.k',   (SELECT arraySort(groupArray((x, toString(json.h.o.k)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.h.o.k)))) FROM t_mem);
SELECT 'json.h.o.m.z', (SELECT arraySort(groupArray((x, toString(json.h.o.m.z)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.h.o.m.z)))) FROM t_mem);
SELECT 'json.h.w',     (SELECT arraySort(groupArray((x, toString(json.h.w)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.h.w)))) FROM t_mem);
SELECT 'json.h.^o',    (SELECT arraySort(groupArray((x, toString(json.h.^o)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.h.^o)))) FROM t_mem);
SELECT 'json.h.o.^m',  (SELECT arraySort(groupArray((x, toString(json.h.o.^m)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.h.o.^m)))) FROM t_mem);
SELECT 'json.g.i.b',   (SELECT arraySort(groupArray((x, toString(json.g.i.b)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.g.i.b)))) FROM t_mem);
SELECT 'json.g.i.d.e', (SELECT arraySort(groupArray((x, toString(json.g.i.d.e)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.g.i.d.e)))) FROM t_mem);
SELECT 'json.g.i.^d',  (SELECT arraySort(groupArray((x, toString(json.g.i.^d)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.g.i.^d)))) FROM t_mem);
SELECT 'json.g.i',     (SELECT arraySort(groupArray((x, toString(json.g.i)))) FROM t_chunked_spilled) = (SELECT arraySort(groupArray((x, toString(json.g.i)))) FROM t_mem);

SELECT '-- advanced, buckets';
SELECT 'json.h.o.k',   (SELECT arraySort(groupArray((x, toString(json.h.o.k)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.h.o.k)))) FROM t_mem);
SELECT 'json.h.o.m.z', (SELECT arraySort(groupArray((x, toString(json.h.o.m.z)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.h.o.m.z)))) FROM t_mem);
SELECT 'json.h.^o',    (SELECT arraySort(groupArray((x, toString(json.h.^o)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.h.^o)))) FROM t_mem);
SELECT 'json.h.o.^m',  (SELECT arraySort(groupArray((x, toString(json.h.o.^m)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.h.o.^m)))) FROM t_mem);
SELECT 'json.g.i.d.e', (SELECT arraySort(groupArray((x, toString(json.g.i.d.e)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.g.i.d.e)))) FROM t_mem);
SELECT 'json.g.i.^d',  (SELECT arraySort(groupArray((x, toString(json.g.i.^d)))) FROM t_buckets) = (SELECT arraySort(groupArray((x, toString(json.g.i.^d)))) FROM t_mem);

SELECT '-- several readers, cut granules, filters';
SELECT 'together, advanced_chunked', (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.h.o.m.z), toString(json.g.i.b), toString(json.g.i.^d), toString(json)))) FROM t_chunked_spilled)
    = (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.h.o.m.z), toString(json.g.i.b), toString(json.g.i.^d), toString(json)))) FROM t_mem);
SELECT 'together, buckets', (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.h.o.m.z), toString(json.g.i.b), toString(json.g.i.^d), toString(json)))) FROM t_buckets)
    = (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.h.o.m.z), toString(json.g.i.b), toString(json.g.i.^d), toString(json)))) FROM t_mem);
SELECT 'max_block_size = 5', (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.g.i.d.e)))) FROM t_chunked_spilled SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.g.i.d.e)))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.h.o.^m), toString(json.g.i.b)))) FROM t_buckets WHERE x % 37 < 3)
    = (SELECT arraySort(groupArray((x, toString(json.h.o.^m), toString(json.g.i.b)))) FROM t_mem WHERE x % 37 < 3);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.g.i.^d)))) FROM t_chunked_spilled PREWHERE json.h.k0 % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.h.^o), toString(json.g.i.^d)))) FROM t_mem WHERE json.h.k0 % 11 = 0);

SELECT '-- layouts read';
-- Without the columns cache, which would serve the columns the queries above read.
SELECT toString(json.h.^o), toString(json.g.i.d.e) FROM t_chunked_spilled SETTINGS log_comment = '05491_regions', use_columns_cache = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05491_regions';

CHECK TABLE t_chunked_spilled;
CHECK TABLE t_buckets;

DROP TABLE t_mem;
DROP TABLE t_chunked_spilled;
DROP TABLE t_buckets;
