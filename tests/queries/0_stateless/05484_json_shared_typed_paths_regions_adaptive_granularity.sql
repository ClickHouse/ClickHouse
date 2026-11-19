-- Reads of SHARED typed paths over the regions of v2 parts with adaptive granularity, compared with a Memory table. The
-- granules of a part differ in rows, so the regions' marks and the marks a JSON path stores in its data, which refer to
-- a granule by its index, must follow the part's granules exactly.

SET max_threads = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_adaptive;

CREATE TABLE t_mem (x UInt64, json JSON(
    a Nullable(String), b Array(UInt32), h JSON(max_dynamic_paths = 1), n JSON(SHARED p UInt32, SHARED q String)))
ENGINE = Memory;

-- The rows grow with x, so a granule of 1 KiB holds fewer rows as the part goes on.
CREATE TABLE t_adaptive (x UInt64, json JSON(
    SHARED a Nullable(String), SHARED b Array(UInt32), SHARED h JSON(max_dynamic_paths = 1),
    SHARED n JSON(SHARED p UInt32, SHARED q String)))
ENGINE = MergeTree ORDER BY x
-- The granule sizes depend on the JSON bytes, which only a horizontal merge counts: a vertical merge sizes granules
-- from the key columns alone (the gathered columns reuse that granularity), so it would make them uniform.
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = 1024,
         use_const_adaptive_granularity = 0, object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced',
         merge_max_block_size = 7, enable_vertical_merge_algorithm = 0;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"a":', if(number % 3 = 0, 'null', concat('"', repeat('a', number % 40), '"')),
    ',"b":', toJSONString(range(number % 9)),
    ',"h":{"k0":', toString(number), ',"k1":"', repeat('s', number % 30), '","k2":', toString(number * 2), '}',
    ',"n":{"p":', toString(number), ',"q":"', repeat('q', number % 20), '","r":', toString(number % 7), '}',
    '}') AS json FROM numbers(300);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_adaptive SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_adaptive SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_adaptive SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_adaptive SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_adaptive FINAL;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_adaptive' AND active;
SELECT 'granule sizes vary', uniqExact(rows_in_granule) > 2 FROM mergeTreeIndex(currentDatabase(), t_adaptive);

SELECT '-- one path or subcolumn per query';
SELECT 'json.a',            (SELECT arraySort(groupArray((x, toString(json.a)))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, toString(json.a)))) FROM t_mem);
SELECT 'json.a.null',       (SELECT arraySort(groupArray((x, json.a.null))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, json.a.null))) FROM t_mem);
SELECT 'json.b.size0',      (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_mem);
SELECT 'json.h.k1',         (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'json.h.k2.:Int64',  (SELECT arraySort(groupArray((x, json.h.k2.:Int64))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, json.h.k2.:Int64))) FROM t_mem);
SELECT 'json.n.q',          (SELECT arraySort(groupArray((x, json.n.q))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, json.n.q))) FROM t_mem);
SELECT 'json.n.r',          (SELECT arraySort(groupArray((x, toString(json.n.r)))) FROM t_adaptive) = (SELECT arraySort(groupArray((x, toString(json.n.r)))) FROM t_mem);

SELECT '-- the whole column, cut granules, filters, PREWHERE';
SELECT 'everything', (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.h.k1), json.n.p, toString(json)))) FROM t_adaptive)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.b.size0, toString(json.h.k1), json.n.p, toString(json)))) FROM t_mem);
SELECT 'max_block_size = 5', (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1), json.n.q))) FROM t_adaptive SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1), json.n.q))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.a), toString(json.h.k1), toString(json.n.r)))) FROM t_adaptive WHERE x % 53 < 4)
    = (SELECT arraySort(groupArray((x, toString(json.a), toString(json.h.k1), toString(json.n.r)))) FROM t_mem WHERE x % 53 < 4);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.h.k1), json.n.q))) FROM t_adaptive PREWHERE json.n.p % 13 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.h.k1), json.n.q))) FROM t_mem WHERE json.n.p % 13 = 0);

CHECK TABLE t_adaptive;

DROP TABLE t_mem;
DROP TABLE t_adaptive;
