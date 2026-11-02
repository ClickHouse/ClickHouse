-- Tags: no-parallel, long
-- The test drops server-wide caches.

-- The readers of the SHARED paths of one JSON column share the decode of a call: the first of them decodes every
-- requested path and the others take the columns from the substreams cache. With the columns cache the readers of one
-- call can start differently, one continuing and one entering the call after being served from the cache, so the
-- remainders and the stream positions have to hold whichever of them decodes.

SET use_columns_cache = 1, max_threads = 1, merge_tree_use_deserialization_prefixes_cache = 1;

DROP TABLE IF EXISTS t_shared_typed;

CREATE TABLE t_shared_typed (id UInt64, json JSON(SHARED a String, SHARED b String, SHARED c UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_shared_typed SELECT number, format('{{"a":"a{0}","b":"b{0}","c":{0}}}', number)::JSON FROM numbers(200000)
SETTINGS max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;

-- Granule g holds ids [8192 * g, 8192 * (g + 1)); a key range also covers the granule after its upper bound.

-- `a` cached in granules 7..14 and `b` in 17..22, then the whole table: after granule 22 `b` enters a call while `a` continues.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_typed WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(json.a) FORMAT Null;
SELECT count() FROM t_shared_typed WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(json.b) FORMAT Null;
SELECT 'whole table', count(), countIf(json.a != concat('a', toString(id))), countIf(json.b != concat('b', toString(id))), countIf(json.c != id)
FROM t_shared_typed;

-- `a` cached in granules 7..14 and 17..22, one granule per block, granule 12, a part of 13, then 15 .. a part of 18.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_typed WHERE id BETWEEN 57344 AND 114687 AND NOT ignore(json.a) FORMAT Null;
SELECT count() FROM t_shared_typed WHERE id BETWEEN 139264 AND 180223 AND NOT ignore(json.a) FORMAT Null;
SELECT 'prewhere', count(), countIf(json.a != concat('a', toString(id))), countIf(json.b != concat('b', toString(id))), countIf(json.c != id)
FROM t_shared_typed PREWHERE (id BETWEEN 98304 AND 109999) OR (id BETWEEN 122880 AND 149999)
SETTINGS max_block_size = 8192;

DROP TABLE t_shared_typed;

-- A sub-object read together with a subcolumn of one of its paths: when the sub-object is served from the cache, the
-- subcolumn reader is the first reader of the call and decodes the paths from the streams the reader positioned.
DROP TABLE IF EXISTS t_shared_typed_sub;
DROP TABLE IF EXISTS t_shared_typed_sub_mem;

CREATE TABLE t_shared_typed_sub (x UInt64, json JSON(SHARED a.x UInt64, SHARED a.y String, SHARED a.p.q UInt64, SHARED a.p.r String, SHARED b UInt64, SHARED c.z Array(UInt8)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 0, min_compress_block_size = 1;
CREATE TABLE t_shared_typed_sub_mem (x UInt64, json JSON(a.x UInt64, a.y String, a.p.q UInt64, a.p.r String, b UInt64, c.z Array(UInt8))) ENGINE = Memory;

INSERT INTO t_shared_typed_sub SELECT number, concat('{"a":{"x":', toString(number), ',"y":"', repeat('y', number % 5), '","p":{"q":', toString(number * 3), ',"r":"r', toString(number % 2), '"}},"b":', toString(number * 2), ',"c":{"z":', toString(range(number % 3)), '}}') FROM numbers(60);
INSERT INTO t_shared_typed_sub_mem SELECT number, concat('{"a":{"x":', toString(number), ',"y":"', repeat('y', number % 5), '","p":{"q":', toString(number * 3), ',"r":"r', toString(number % 2), '"}},"b":', toString(number * 2), ',"c":{"z":', toString(range(number % 3)), '}}') FROM numbers(60);

-- Granule g holds x in [4 * g, 4 * (g + 1)); the sub-object is cached for granules 2..4 and 8..11, the read covers
-- granule 1, one row of 2, then 4 .. 8, one granule per block.
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_shared_typed_sub WHERE x BETWEEN 8 AND 15 AND NOT ignore(json.^a.p) FORMAT Null;
SELECT count() FROM t_shared_typed_sub WHERE x BETWEEN 32 AND 43 AND NOT ignore(json.^a.p) FORMAT Null;
SELECT 'sub-object cached', (SELECT arraySort(groupArray((x, json.b, toString(json.^a.p), json.a.p.q))) FROM t_shared_typed_sub PREWHERE (x BETWEEN 4 AND 8) OR (x BETWEEN 16 AND 35) SETTINGS max_block_size = 4)
    = (SELECT arraySort(groupArray((x, json.b, toString(json.^a.p), json.a.p.q))) FROM t_shared_typed_sub_mem WHERE (x BETWEEN 4 AND 8) OR (x BETWEEN 16 AND 35));

DROP TABLE t_shared_typed_sub;
DROP TABLE t_shared_typed_sub_mem;

-- SHARED paths whose types have a dynamic structure: a reader that takes a path from the substreams cache must give its
-- column the structure the part gives the path, because a later call decodes into that column when the other reader is
-- served from the cache.
SET enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_shared_typed_dyn;
DROP TABLE IF EXISTS t_shared_typed_dyn_compact;
DROP TABLE IF EXISTS t_shared_typed_dyn_mem;

CREATE TABLE t_shared_typed_dyn (x UInt64, json JSON(SHARED d Dynamic(max_types = 1), SHARED j JSON(max_dynamic_paths = 1)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 0, min_compress_block_size = 1;
CREATE TABLE t_shared_typed_dyn_compact (x UInt64, json JSON(SHARED d Dynamic(max_types = 1), SHARED j JSON(max_dynamic_paths = 1)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, write_marks_for_substreams_in_compact_parts = 1;
CREATE TABLE t_shared_typed_dyn_mem (x UInt64, json JSON(d Dynamic(max_types = 1), j JSON(max_dynamic_paths = 1))) ENGINE = Memory;

INSERT INTO t_shared_typed_dyn SELECT number, concat('{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"j":{"k":', toString(number), ',"p":{"a":', toString(number * 2), ',"b":"b', toString(number % 3), '"}}}') FROM numbers(20);
INSERT INTO t_shared_typed_dyn_compact SELECT number, concat('{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"j":{"k":', toString(number), ',"p":{"a":', toString(number * 2), ',"b":"b', toString(number % 3), '"}}}') FROM numbers(20);
INSERT INTO t_shared_typed_dyn_mem SELECT number, concat('{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"j":{"k":', toString(number), ',"p":{"a":', toString(number * 2), ',"b":"b', toString(number % 3), '"}}}') FROM numbers(20);

-- Granule g holds x in [4 * g, 4 * (g + 1)). A subcolumn cached for granule 2, then the whole column read with it.
SYSTEM DROP COLUMNS CACHE;
SELECT json.j.k FROM t_shared_typed_dyn WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'cached', column, row_begin, row_end FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_shared_typed_dyn' ORDER BY column;
SELECT 'whole column after a cached subcolumn', (SELECT arraySort(groupArray((x, toString((json.j.k, json))))) FROM t_shared_typed_dyn)
    = (SELECT arraySort(groupArray((x, toString((json.j.k, json))))) FROM t_shared_typed_dyn_mem);

-- The whole column cached for granule 2, then a path read as a subcolumn with it.
SYSTEM DROP COLUMNS CACHE;
SELECT json FROM t_shared_typed_dyn WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'cached', column, row_begin, row_end FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_shared_typed_dyn' ORDER BY column;
SELECT 'path after the cached whole column', (SELECT arraySort(groupArray((x, toString((json, json.j))))) FROM t_shared_typed_dyn)
    = (SELECT arraySort(groupArray((x, toString((json, json.j))))) FROM t_shared_typed_dyn_mem);

-- The same in a Compact part, where only the subcolumns of a column share the decode.
SYSTEM DROP COLUMNS CACHE;
SELECT json.j.k FROM t_shared_typed_dyn_compact WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'cached', column, row_begin, row_end FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_shared_typed_dyn_compact' ORDER BY column;
SELECT 'compact path after a cached subcolumn', (SELECT arraySort(groupArray((x, toString((json.j.k, json.d))))) FROM t_shared_typed_dyn_compact)
    = (SELECT arraySort(groupArray((x, toString((json.j.k, json.d))))) FROM t_shared_typed_dyn_mem);

DROP TABLE t_shared_typed_dyn;
DROP TABLE t_shared_typed_dyn_compact;
DROP TABLE t_shared_typed_dyn_mem;

-- A sub-object with a Dynamic path, read with a path cached for granule 2.
DROP TABLE IF EXISTS t_shared_typed_dyn_sub;
DROP TABLE IF EXISTS t_shared_typed_dyn_sub_mem;

CREATE TABLE t_shared_typed_dyn_sub (x UInt64, json JSON(SHARED a.d Dynamic(max_types = 1), SHARED a.e UInt64, SHARED b UInt64))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, index_granularity_bytes = 0, min_compress_block_size = 1;
CREATE TABLE t_shared_typed_dyn_sub_mem (x UInt64, json JSON(a.d Dynamic(max_types = 1), a.e UInt64, b UInt64)) ENGINE = Memory;

INSERT INTO t_shared_typed_dyn_sub SELECT number, concat('{"a":{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"e":', toString(number * 2), '},"b":', toString(number * 3), '}') FROM numbers(20);
INSERT INTO t_shared_typed_dyn_sub_mem SELECT number, concat('{"a":{"d":', multiIf(number % 3 = 0, toString(number), number % 3 = 1, concat('"d', toString(number), '"'), concat('[', toString(number), ']')), ',"e":', toString(number * 2), '},"b":', toString(number * 3), '}') FROM numbers(20);

SYSTEM DROP COLUMNS CACHE;
SELECT json.b FROM t_shared_typed_dyn_sub WHERE x = 10 FORMAT Null SETTINGS optimize_move_to_prewhere = 0;
SELECT 'cached', column, row_begin, row_end FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_shared_typed_dyn_sub' ORDER BY column;
SELECT 'sub-object after a cached path', (SELECT arraySort(groupArray((x, json.b, toString(json.^a)))) FROM t_shared_typed_dyn_sub)
    = (SELECT arraySort(groupArray((x, json.b, toString(json.^a)))) FROM t_shared_typed_dyn_sub_mem);

DROP TABLE t_shared_typed_dyn_sub;
DROP TABLE t_shared_typed_dyn_sub_mem;
