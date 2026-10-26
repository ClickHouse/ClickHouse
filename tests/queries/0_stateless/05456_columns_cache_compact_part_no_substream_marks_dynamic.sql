-- Tags: no-parallel
-- no-parallel: the test needs its primed entries, and `SYSTEM DROP COLUMNS CACHE` and the admission control of the cache are shared with other tests.

-- In a compact part without substream marks, a column read together with its subcolumn is copied
-- from the column decoded for the subcolumn. When the subcolumn of a later granule is served from
-- the columns cache, that granule is decoded straight into the same result column, which therefore
-- needs the dynamic structure of the part. Each table caches granule 2 (rows 8..11) of the subcolumn
-- only, then reads the subcolumn and the full column over all granules.

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, enable_parallel_replicas = 0;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_json_mem;
DROP TABLE IF EXISTS t_dynamic;
DROP TABLE IF EXISTS t_dynamic_mem;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_tuple_mem;

CREATE TABLE t_json (x UInt64, json JSON(max_dynamic_paths = 2)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, index_granularity_bytes = '10Mi',
    write_marks_for_substreams_in_compact_parts = 0, object_serialization_version = 'v3', dynamic_serialization_version = 'v3';
CREATE TABLE t_json_mem (x UInt64, json JSON(max_dynamic_paths = 2)) ENGINE = Memory;
INSERT INTO t_json SELECT number, concat('{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"k":', toString(number), '}') FROM numbers(20);
INSERT INTO t_json_mem SELECT number, concat('{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"k":', toString(number), '}') FROM numbers(20);

SELECT json.k FROM t_json WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'json cached', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_json' AND column = 'json.k';
SELECT 'json', (SELECT arraySort(groupArray((x, toString((json.k, json))))) FROM t_json) = (SELECT arraySort(groupArray((x, toString((json.k, json))))) FROM t_json_mem);

CREATE TABLE t_dynamic (x UInt64, d Dynamic) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, index_granularity_bytes = '10Mi',
    write_marks_for_substreams_in_compact_parts = 0, dynamic_serialization_version = 'v3';
CREATE TABLE t_dynamic_mem (x UInt64, d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic SELECT number, multiIf(number % 3 = 0, number::Int64::Dynamic, number % 3 = 1, concat('d', toString(number))::Dynamic, [number::Int64]::Dynamic) FROM numbers(20);
INSERT INTO t_dynamic_mem SELECT number, multiIf(number % 3 = 0, number::Int64::Dynamic, number % 3 = 1, concat('d', toString(number))::Dynamic, [number::Int64]::Dynamic) FROM numbers(20);

SELECT d.Int64 FROM t_dynamic WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'dynamic cached', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_dynamic' AND column = 'd.Int64';
SELECT 'dynamic', (SELECT arraySort(groupArray((x, toString((d.Int64, d))))) FROM t_dynamic) = (SELECT arraySort(groupArray((x, toString((d.Int64, d))))) FROM t_dynamic_mem);

CREATE TABLE t_tuple (x UInt64, t Tuple(a Dynamic, b UInt8)) ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, index_granularity_bytes = '10Mi',
    write_marks_for_substreams_in_compact_parts = 0, dynamic_serialization_version = 'v3';
CREATE TABLE t_tuple_mem (x UInt64, t Tuple(a Dynamic, b UInt8)) ENGINE = Memory;
INSERT INTO t_tuple SELECT number, (multiIf(number % 3 = 0, number::Int64::Dynamic, number % 3 = 1, concat('d', toString(number))::Dynamic, [number::Int64]::Dynamic), number % 7) FROM numbers(20);
INSERT INTO t_tuple_mem SELECT number, (multiIf(number % 3 = 0, number::Int64::Dynamic, number % 3 = 1, concat('d', toString(number))::Dynamic, [number::Int64]::Dynamic), number % 7) FROM numbers(20);

SELECT t.b FROM t_tuple WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'tuple cached', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_tuple' AND column = 't.b';
SELECT 'tuple', (SELECT arraySort(groupArray((x, toString((t.b, t))))) FROM t_tuple) = (SELECT arraySort(groupArray((x, toString((t.b, t))))) FROM t_tuple_mem);

DROP TABLE t_json;
DROP TABLE t_json_mem;
DROP TABLE t_dynamic;
DROP TABLE t_dynamic_mem;
DROP TABLE t_tuple;
DROP TABLE t_tuple_mem;
