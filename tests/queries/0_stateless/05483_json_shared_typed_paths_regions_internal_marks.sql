-- Reads of SHARED typed paths whose types contain JSON, over the regions of v2 parts, compared with a Memory table. The
-- shared data of a JSON and its own SHARED typed paths store marks of their data, which inside a region refer to a
-- granule, and narrow reads seek by them: JSON at the top level of a path, inside Array and inside Variant, with the
-- advanced shared data in one chunk per serialize call and in several, every substream spilled to temporary files,
-- chunks cut across reads, granules skipped by filters, and a mutation reading the whole part without marks.

SET max_threads = 1, mutations_sync = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_advanced;
DROP TABLE IF EXISTS t_chunked;
DROP TABLE IF EXISTS t_spilled;

CREATE TABLE t_mem (x UInt64, y UInt64, json JSON(
    h JSON(max_dynamic_paths = 1), a Array(JSON(max_dynamic_paths = 1)), v Variant(JSON(max_dynamic_paths = 1), UInt32),
    n JSON(SHARED p UInt32, SHARED q Array(String))))
ENGINE = Memory;

-- Merged from blocks of 7 rows, so a granule of 16 rows takes several serialize calls, each writing its own chunks.
CREATE TABLE t_advanced (x UInt64, y UInt64, json JSON(
    SHARED h JSON(max_dynamic_paths = 1), SHARED a Array(JSON(max_dynamic_paths = 1)),
    SHARED v Variant(JSON(max_dynamic_paths = 1), UInt32), SHARED n JSON(SHARED p UInt32, SHARED q Array(String))))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced',
         merge_max_block_size = 7;

CREATE TABLE t_chunked AS t_advanced ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced_chunked',
         object_shared_data_target_chunk_rows = 3, merge_max_block_size = 7;

-- The first spilled segment of a substream carries its prefix; the internal marks count from the granule's start over the
-- spilled segments and the bytes in memory.
CREATE TABLE t_spilled AS t_advanced ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced',
         merge_max_block_size = 7, serialization_spill_max_bytes_to_buffer = 1;

-- `h.k1` to `h.k3` and `v.k1` are in the shared data; the arrays of `a` are empty in a whole granule.
CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"h":{"k0":', toString(number), ',"k1":"s', toString(number), '","k2":[', toString(number), ',1]',
        if(number % 5 = 0, concat(',"k3":', toString(number)), ''), '}',
    ',"a":', if(number >= 32 AND number < 48, '[]',
        concat('[{"k0":', toString(number), ',"k1":"x"},{"k1":"y', toString(number), '","k2":', toString(number), '}]')),
    ',"v":', if(number % 2 = 1, concat('{"k0":', toString(number), ',"k1":"v', toString(number), '"}'), toString(number)),
    ',"n":{"p":', toString(number), ',"q":["q', toString(number), '"],"r":"r', toString(number % 7), '"}',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, 0, json FROM src;
INSERT INTO t_advanced SELECT x, 0, json FROM src WHERE x % 4 = 0;
INSERT INTO t_advanced SELECT x, 0, json FROM src WHERE x % 4 = 1;
INSERT INTO t_advanced SELECT x, 0, json FROM src WHERE x % 4 = 2;
INSERT INTO t_advanced SELECT x, 0, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_advanced FINAL;
INSERT INTO t_chunked SELECT x, 0, json FROM src WHERE x % 4 = 0;
INSERT INTO t_chunked SELECT x, 0, json FROM src WHERE x % 4 = 1;
INSERT INTO t_chunked SELECT x, 0, json FROM src WHERE x % 4 = 2;
INSERT INTO t_chunked SELECT x, 0, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_chunked FINAL;
INSERT INTO t_spilled SELECT x, 0, json FROM src WHERE x % 4 = 0;
INSERT INTO t_spilled SELECT x, 0, json FROM src WHERE x % 4 = 1;
INSERT INTO t_spilled SELECT x, 0, json FROM src WHERE x % 4 = 2;
INSERT INTO t_spilled SELECT x, 0, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_spilled FINAL;

SELECT 'parts', table, count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_advanced', 't_chunked', 't_spilled') AND active GROUP BY table ORDER BY table;

SELECT '-- one path or subcolumn per query, advanced';
SELECT 'json.h.k1',         (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'json.h.k1.:String', (SELECT arraySort(groupArray((x, json.h.k1.:String))) FROM t_advanced) = (SELECT arraySort(groupArray((x, json.h.k1.:String))) FROM t_mem);
SELECT 'json.h.k2',         (SELECT arraySort(groupArray((x, toString(json.h.k2)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.h.k2)))) FROM t_mem);
SELECT 'json.h.k3',         (SELECT arraySort(groupArray((x, toString(json.h.k3)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.h.k3)))) FROM t_mem);
SELECT 'json.a.k1',         (SELECT arraySort(groupArray((x, toString(json.a.k1)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.a.k1)))) FROM t_mem);
SELECT 'json.a.k2',         (SELECT arraySort(groupArray((x, toString(json.a.k2)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.a.k2)))) FROM t_mem);
SELECT 'json.v.JSON.k1',    (SELECT arraySort(groupArray((x, toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_advanced)
    = (SELECT arraySort(groupArray((x, toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_mem);
SELECT 'json.n.p',          (SELECT arraySort(groupArray((x, json.n.p))) FROM t_advanced) = (SELECT arraySort(groupArray((x, json.n.p))) FROM t_mem);
SELECT 'json.n.q.size0',    (SELECT arraySort(groupArray((x, json.n.q.size0))) FROM t_advanced) = (SELECT arraySort(groupArray((x, json.n.q.size0))) FROM t_mem);
SELECT 'json.n.r',          (SELECT arraySort(groupArray((x, toString(json.n.r)))) FROM t_advanced) = (SELECT arraySort(groupArray((x, toString(json.n.r)))) FROM t_mem);

SELECT '-- one path or subcolumn per query, advanced_chunked';
SELECT 'json.h.k1',         (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_chunked) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'json.h.k1.:String', (SELECT arraySort(groupArray((x, json.h.k1.:String))) FROM t_chunked) = (SELECT arraySort(groupArray((x, json.h.k1.:String))) FROM t_mem);
SELECT 'json.h.k3',         (SELECT arraySort(groupArray((x, toString(json.h.k3)))) FROM t_chunked) = (SELECT arraySort(groupArray((x, toString(json.h.k3)))) FROM t_mem);
SELECT 'json.a.k2',         (SELECT arraySort(groupArray((x, toString(json.a.k2)))) FROM t_chunked) = (SELECT arraySort(groupArray((x, toString(json.a.k2)))) FROM t_mem);
SELECT 'json.v.JSON.k1',    (SELECT arraySort(groupArray((x, toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_chunked)
    = (SELECT arraySort(groupArray((x, toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_mem);

SELECT '-- several readers of a path, the whole column';
SELECT 'advanced', (SELECT arraySort(groupArray((x, toString(json.h), toString(json.h.k1), json.h.k1.:String, toString(json.a), toString(json.a.k1),
        toString(json.v), json.n.p, toString(json.n.r), toString(json)))) FROM t_advanced)
    = (SELECT arraySort(groupArray((x, toString(json.h), toString(json.h.k1), json.h.k1.:String, toString(json.a), toString(json.a.k1),
        toString(json.v), json.n.p, toString(json.n.r), toString(json)))) FROM t_mem);
SELECT 'advanced_chunked', (SELECT arraySort(groupArray((x, toString(json.h), toString(json.h.k1), json.h.k1.:String, toString(json.a), toString(json.a.k1),
        toString(json.v), json.n.p, toString(json.n.r), toString(json)))) FROM t_chunked)
    = (SELECT arraySort(groupArray((x, toString(json.h), toString(json.h.k1), json.h.k1.:String, toString(json.a), toString(json.a.k1),
        toString(json.v), json.n.p, toString(json.n.r), toString(json)))) FROM t_mem);

SELECT 'spilled', (SELECT arraySort(groupArray((x, toString(json.h.k1), json.h.k1.:String, toString(json.a.k2), toString(json.v.`JSON(max_dynamic_paths=1)`.k1),
        json.n.p, toString(json.n.r), toString(json)))) FROM t_spilled SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), json.h.k1.:String, toString(json.a.k2), toString(json.v.`JSON(max_dynamic_paths=1)`.k1),
        json.n.p, toString(json.n.r), toString(json)))) FROM t_mem);

SELECT '-- chunks cut across reads, filters, PREWHERE';
SELECT 'max_block_size = 5, advanced', (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k1), json.n.p))) FROM t_advanced SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k1), json.n.p))) FROM t_mem);
SELECT 'max_block_size = 5, advanced_chunked', (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k1), json.n.p))) FROM t_chunked SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k1), json.n.p))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_chunked WHERE x % 37 < 3)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.v.`JSON(max_dynamic_paths=1)`.k1)))) FROM t_mem WHERE x % 37 < 3);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k2)))) FROM t_advanced PREWHERE json.h.k0 % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), toString(json.a.k2)))) FROM t_mem WHERE json.h.k0 % 11 = 0);

SELECT '-- a mutation reads the whole part without marks';
ALTER TABLE t_advanced UPDATE y = cityHash64(toString(json.h.k1), toString(json.a.k1), json.n.p) WHERE 1;
SELECT 'update', (SELECT arraySort(groupArray((x, y))) FROM t_advanced)
    = (SELECT arraySort(groupArray((x, cityHash64(toString(json.h.k1), toString(json.a.k1), json.n.p)))) FROM t_mem);

SELECT '-- layouts read';
-- Without the columns cache, which would serve the column the queries above read.
SELECT toString(json.h.k1) FROM t_advanced SETTINGS log_comment = '05483_region', use_columns_cache = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05483_region';

CHECK TABLE t_advanced;
CHECK TABLE t_chunked;
CHECK TABLE t_spilled;

DROP TABLE t_mem;
DROP TABLE t_advanced;
DROP TABLE t_chunked;
DROP TABLE t_spilled;
