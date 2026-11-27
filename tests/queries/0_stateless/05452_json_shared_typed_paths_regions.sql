-- Region layout (v2) of the SHARED typed paths of JSON: a narrow read of a path or a sub-object decodes only that
-- path's regions through file views, and must give the same result as the plain type (a Memory table). Covers direct
-- mode (one insert, regions from retained columns), buffered mode (a merge, regions from per-path buffers), several
-- granularities and block sizes, nested JSON, and a Compact part (which has no regions and keeps the plain layout).

DROP TABLE IF EXISTS t_v2;
DROP TABLE IF EXISTS t_v2_merge;
DROP TABLE IF EXISTS t_v2_compact;
DROP TABLE IF EXISTS t_mem;

CREATE TABLE t_mem (x UInt64, json JSON(a UInt64, b String, c Array(UInt8), j JSON(p String))) ENGINE = Memory;
INSERT INTO t_mem SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(40);

-- Direct mode: one INSERT is one block, so each granule is kept as references to that block and the regions are written from them.
CREATE TABLE t_v2 (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8), SHARED j JSON(SHARED p String)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1,
         object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_v2 SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(40);

SELECT '-- direct mode (Wide, one block)';
SELECT 'json',        (SELECT arraySort(groupArray((x, toString(json)))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a',      (SELECT arraySort(groupArray((x, json.a))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.b',      (SELECT arraySort(groupArray((x, json.b))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'json.c',      (SELECT arraySort(groupArray((x, json.c))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);
SELECT 'json.a,json.b', (SELECT arraySort(groupArray((x, json.a, json.b))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a, json.b))) FROM t_mem);
SELECT 'json.j',      (SELECT arraySort(groupArray((x, toString(json.j)))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, toString(json.j)))) FROM t_mem);
SELECT 'json.j.p',    (SELECT arraySort(groupArray((x, json.j.p))) FROM t_v2 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.j.p))) FROM t_mem);
SELECT 'json.a mbs=1',(SELECT arraySort(groupArray((x, json.a))) FROM t_v2 SETTINGS max_threads = 1, max_block_size = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.a mbs=3',(SELECT arraySort(groupArray((x, json.a))) FROM t_v2 SETTINGS max_threads = 1, max_block_size = 3) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.a WHERE',(SELECT arraySort(groupArray((x, json.a))) FROM t_v2 WHERE x % 3 = 0 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem WHERE x % 3 = 0);
SELECT 'json.a PREWHERE',(SELECT arraySort(groupArray((x, json.a))) FROM t_v2 PREWHERE json.b = '' SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem WHERE json.b = '');
SELECT 'json.a LIMIT', (SELECT groupArray(a) FROM (SELECT json.a AS a FROM t_v2 ORDER BY x LIMIT 5 SETTINGS max_threads = 1)) = (SELECT groupArray(a) FROM (SELECT json.a AS a FROM t_mem ORDER BY x LIMIT 5));

-- Buffered mode: several parts merged into one; the merge reads its sources block by block, so after the first block the
-- regions switch to the per-path buffers. A granularity that does not divide the merge block size makes a granule span blocks.
CREATE TABLE t_v2_merge (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8), SHARED j JSON(SHARED p String)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 3, min_compress_block_size = 1,
         object_typed_shared_data_serialization_version = 'v2', merge_max_block_size = 5;
INSERT INTO t_v2_merge SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(0, 13);
INSERT INTO t_v2_merge SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(13, 14);
INSERT INTO t_v2_merge SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(27, 13);
OPTIMIZE TABLE t_v2_merge FINAL;

SELECT '-- buffered mode (Wide, merged, granule spans blocks)';
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_v2_merge' AND active;
SELECT 'json',     (SELECT arraySort(groupArray((x, toString(json)))) FROM t_v2_merge SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_mem);
SELECT 'json.a',   (SELECT arraySort(groupArray((x, json.a))) FROM t_v2_merge SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.b',   (SELECT arraySort(groupArray((x, json.b))) FROM t_v2_merge SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.b))) FROM t_mem);
SELECT 'json.c',   (SELECT arraySort(groupArray((x, json.c))) FROM t_v2_merge SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.c))) FROM t_mem);
SELECT 'json.j.p', (SELECT arraySort(groupArray((x, json.j.p))) FROM t_v2_merge SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.j.p))) FROM t_mem);
SELECT 'json.a mbs=1', (SELECT arraySort(groupArray((x, json.a))) FROM t_v2_merge SETTINGS max_threads = 1, max_block_size = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);

-- Compact part: no regions; with the v2 setting it keeps the v1 plain layout, and reads still match.
CREATE TABLE t_v2_compact (x UInt64, json JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8), SHARED j JSON(SHARED p String)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4,
         object_typed_shared_data_serialization_version = 'v2';
INSERT INTO t_v2_compact SELECT number, concat('{"a":', toString(number * 10), ',"b":"', repeat('x', number % 7), '","c":', toString(range(number % 4)), ',"j":{"p":"p', toString(number), '"}}') FROM numbers(40);

SELECT '-- Compact part (v2 setting, the plain layout)';
SELECT 'part_type', any(part_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_v2_compact' AND active;
SELECT 'json.a',   (SELECT arraySort(groupArray((x, json.a))) FROM t_v2_compact SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.j.p', (SELECT arraySort(groupArray((x, json.j.p))) FROM t_v2_compact SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((x, json.j.p))) FROM t_mem);

DROP TABLE t_v2;
DROP TABLE t_v2_merge;
DROP TABLE t_v2_compact;
DROP TABLE t_mem;
