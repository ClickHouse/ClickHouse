-- Reads of SHARED typed paths over the regions of their substreams in v2 parts, compared with a Memory table: a part
-- written from one block, a part merged from several blocks with granules spanning them, and the same merge spilling
-- every substream to temporary files. Each path type has substreams of its own. Paths and subcolumns are read alone
-- and together, with cut granules, filters and PREWHERE.

SET max_threads = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_one_block;
DROP TABLE IF EXISTS t_merged;
DROP TABLE IF EXISTS t_spilled;

CREATE TABLE t_mem (x UInt64, json JSON(
    a Nullable(String), b Array(Nullable(UInt32)), c LowCardinality(String), d Tuple(x UInt32, y Array(String)),
    e Dynamic(max_types = 2), f Variant(UInt32, String), g Map(String, UInt32), h JSON(i UInt32), `s.p1` UInt32, `s.p2` String))
ENGINE = Memory;

CREATE TABLE t_one_block (x UInt64, json JSON(
    SHARED a Nullable(String), SHARED b Array(Nullable(UInt32)), SHARED c LowCardinality(String), SHARED d Tuple(x UInt32, y Array(String)),
    SHARED e Dynamic(max_types = 2), SHARED f Variant(UInt32, String), SHARED g Map(String, UInt32), SHARED h JSON(i UInt32),
    SHARED `s.p1` UInt32, SHARED `s.p2` String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2';

CREATE TABLE t_merged AS t_one_block ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', merge_max_block_size = 7;

CREATE TABLE t_spilled AS t_one_block ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', merge_max_block_size = 7, serialization_spill_max_bytes_to_buffer = 1;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"a":', if(number % 3 = 0, 'null', concat('"', repeat('a', number % 5), '"')),
    ',"b":', toJSONString(arrayMap(i -> if(i % 2 = 1, NULL, i), range(number % 4))),
    ',"c":"v', toString(number % 3), '"',
    ',"d":{"x":', toString(number), ',"y":', toJSONString(arrayMap(i -> toString(i), range(number % 3))), '}',
    ',"e":', multiIf(number % 4 = 0, toString(number), number % 4 = 1, concat('"s', toString(number), '"'), number % 4 = 2, '[1,2]', 'true'),
    ',"f":', if(number % 2 = 1, toString(number), concat('"t', toString(number), '"')),
    ',"g":{"k', toString(number % 3), '":', toString(number), '}',
    ',"h":{"i":', toString(number), ',"k":"', toString(number % 5), '"}',
    ',"s":{"p1":', toString(number), ',"p2":"', toString(number * 2), '"}',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_one_block SELECT x, json FROM src;
INSERT INTO t_merged SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_merged SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_merged SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_merged SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_merged FINAL;
INSERT INTO t_spilled SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_spilled SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_spilled SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_spilled SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_spilled FINAL;

SELECT 'parts', table, count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t_one_block', 't_merged', 't_spilled') AND active GROUP BY table ORDER BY table;

SELECT '-- one path or subcolumn per query, merged part';
SELECT 'json.a',         (SELECT arraySort(groupArray((x, toString(json.a)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.a)))) FROM t_mem);
SELECT 'json.a.null',    (SELECT arraySort(groupArray((x, json.a.null))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.a.null))) FROM t_mem);
SELECT 'json.b',         (SELECT arraySort(groupArray((x, toString(json.b)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.b)))) FROM t_mem);
SELECT 'json.b.size0',   (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_mem);
SELECT 'json.b.null',    (SELECT arraySort(groupArray((x, toString(json.b.null)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.b.null)))) FROM t_mem);
SELECT 'json.c',         (SELECT arraySort(groupArray((x, toString(json.c)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.c)))) FROM t_mem);
SELECT 'json.d',         (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.d)))) FROM t_mem);
SELECT 'json.d.x',       (SELECT arraySort(groupArray((x, json.d.x))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.d.x))) FROM t_mem);
SELECT 'json.d.y.size0', (SELECT arraySort(groupArray((x, json.d.y.size0))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.d.y.size0))) FROM t_mem);
SELECT 'json.e',         (SELECT arraySort(groupArray((x, toString(json.e)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.e)))) FROM t_mem);
SELECT 'json.e.Int64',   (SELECT arraySort(groupArray((x, toString(json.e.Int64)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.e.Int64)))) FROM t_mem);
SELECT 'json.e.String',  (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_mem);
SELECT 'json.f',         (SELECT arraySort(groupArray((x, toString(json.f)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.f)))) FROM t_mem);
SELECT 'json.f.String',  (SELECT arraySort(groupArray((x, toString(json.f.String)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.f.String)))) FROM t_mem);
SELECT 'json.g',         (SELECT arraySort(groupArray((x, toString(json.g)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.g)))) FROM t_mem);
SELECT 'json.g.keys',    (SELECT arraySort(groupArray((x, json.g.keys))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.g.keys))) FROM t_mem);
SELECT 'json.h',         (SELECT arraySort(groupArray((x, toString(json.h)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.h)))) FROM t_mem);
SELECT 'json.h.i',       (SELECT arraySort(groupArray((x, json.h.i))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.h.i))) FROM t_mem);
SELECT 'json.h.k',       (SELECT arraySort(groupArray((x, toString(json.h.k)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.h.k)))) FROM t_mem);
SELECT 'json.s.p2',      (SELECT arraySort(groupArray((x, json.s.p2))) FROM t_merged) = (SELECT arraySort(groupArray((x, json.s.p2))) FROM t_mem);
SELECT 'json.^s',        (SELECT arraySort(groupArray((x, toString(json.^s)))) FROM t_merged) = (SELECT arraySort(groupArray((x, toString(json.^s)))) FROM t_mem);

SELECT '-- everything in one query, every part';
SELECT 't_merged', (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_merged)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_mem);
SELECT 't_one_block', (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_one_block)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_mem);
SELECT 't_spilled', (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_spilled)
    = (SELECT arraySort(groupArray((x, toString(json.a), json.a.null, toString(json.b), json.b.size0, toString(json.c), json.d.x,
        toString(json.d), toString(json.e), toString(json.e.String), toString(json.f), toString(json.g), json.g.keys, json.h.i,
        toString(json.h), json.s.p1, toString(json.^s), toString(json)))) FROM t_mem);

SELECT '-- cut granules, filters, PREWHERE';
SELECT 'max_block_size = 5', (SELECT arraySort(groupArray((x, toString(json.b), json.b.size0, toString(json.e), json.d.x))) FROM t_spilled SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.b), json.b.size0, toString(json.e), json.d.x))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.a), toString(json.c), toString(json.g)))) FROM t_merged WHERE x % 37 < 3)
    = (SELECT arraySort(groupArray((x, toString(json.a), toString(json.c), toString(json.g)))) FROM t_mem WHERE x % 37 < 3);
SELECT 'PREWHERE path', (SELECT arraySort(groupArray((x, toString(json.b), toString(json.f)))) FROM t_one_block PREWHERE json.d.x % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.b), toString(json.f)))) FROM t_mem WHERE json.d.x % 11 = 0);
SELECT 'PREWHERE subcolumn', (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_spilled PREWHERE json.a.null)
    = (SELECT arraySort(groupArray((x, toString(json.e.String)))) FROM t_mem WHERE json.a.null);

SELECT '-- layouts read';
-- Without the columns cache, which would serve the columns the queries above read.
SELECT sum(json.d.x) FROM t_merged SETTINGS log_comment = '05482_region', use_columns_cache = 0 FORMAT Null;
SELECT sum(json.h.i) FROM t_merged SETTINGS log_comment = '05482_json_path', use_columns_cache = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05482_region', '05482_json_path')
ORDER BY log_comment;

CHECK TABLE t_one_block;
CHECK TABLE t_merged;
CHECK TABLE t_spilled;

DROP TABLE t_mem;
DROP TABLE t_one_block;
DROP TABLE t_merged;
DROP TABLE t_spilled;
