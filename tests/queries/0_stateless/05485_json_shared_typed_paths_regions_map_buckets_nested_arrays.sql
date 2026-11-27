-- Reads of SHARED typed paths over the regions of v2 parts, compared with a Memory table: a Map path with bucketed
-- serialization, whose buckets are substreams and whose buckets info is a metadata stream, and nested arrays, whose
-- inner sizes are named at array level 1 inside the path.

SET max_threads = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_regions;

CREATE TABLE t_mem (x UInt64, json JSON(m Map(String, UInt32), aa Array(Array(UInt32)), at Array(Tuple(s String, l Array(UInt8)))))
ENGINE = Memory;

CREATE TABLE t_regions (x UInt64, json JSON(SHARED m Map(String, UInt32), SHARED aa Array(Array(UInt32)),
    SHARED at Array(Tuple(s String, l Array(UInt8)))))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', merge_max_block_size = 7,
         map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
         map_buckets_strategy = 'constant', max_buckets_in_map = 4, map_buckets_min_avg_size = 0;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"m":{', arrayStringConcat(arrayMap(i -> concat('"k', toString((number + i) % 11), '":', toString(number * 10 + i)), range(number % 6)), ','), '}',
    ',"aa":', toJSONString(arrayMap(i -> range(i % 4), range(number % 5))),
    ',"at":[', arrayStringConcat(arrayMap(i -> concat('{"s":"s', toString(i), '","l":', toJSONString(range(i)), '}'), range(number % 3)), ','), ']',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_regions FINAL;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_regions' AND active;

SELECT '-- one path or subcolumn per query';
SELECT 'json.m',          (SELECT arraySort(groupArray((x, toString(json.m)))) FROM t_regions) = (SELECT arraySort(groupArray((x, toString(json.m)))) FROM t_mem);
SELECT 'json.m.keys',     (SELECT arraySort(groupArray((x, json.m.keys))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.m.keys))) FROM t_mem);
SELECT 'json.m.values',   (SELECT arraySort(groupArray((x, json.m.values))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.m.values))) FROM t_mem);
SELECT 'json.m.size0',    (SELECT arraySort(groupArray((x, json.m.size0))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.m.size0))) FROM t_mem);
SELECT 'json.m[k3]',      (SELECT arraySort(groupArray((x, json.m['k3']))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.m['k3']))) FROM t_mem);
SELECT 'json.aa',         (SELECT arraySort(groupArray((x, toString(json.aa)))) FROM t_regions) = (SELECT arraySort(groupArray((x, toString(json.aa)))) FROM t_mem);
SELECT 'json.aa.size0',   (SELECT arraySort(groupArray((x, json.aa.size0))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.aa.size0))) FROM t_mem);
SELECT 'json.aa.size1',   (SELECT arraySort(groupArray((x, json.aa.size1))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.aa.size1))) FROM t_mem);
SELECT 'json.at.s',       (SELECT arraySort(groupArray((x, json.at.s))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.at.s))) FROM t_mem);
SELECT 'json.at.l.size1', (SELECT arraySort(groupArray((x, json.at.l.size1))) FROM t_regions) = (SELECT arraySort(groupArray((x, json.at.l.size1))) FROM t_mem);

SELECT '-- together, cut granules, filters';
SELECT 'everything', (SELECT arraySort(groupArray((x, toString(json.m), json.m['k3'], json.m.keys, toString(json.aa), json.aa.size1, json.at.s, toString(json)))) FROM t_regions)
    = (SELECT arraySort(groupArray((x, toString(json.m), json.m['k3'], json.m.keys, toString(json.aa), json.aa.size1, json.at.s, toString(json)))) FROM t_mem);
SELECT 'max_block_size = 5', (SELECT arraySort(groupArray((x, json.m['k5'], json.aa.size1, json.at.l.size1))) FROM t_regions SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, json.m['k5'], json.aa.size1, json.at.l.size1))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.m), json.aa.size1))) FROM t_regions WHERE x % 37 < 3)
    = (SELECT arraySort(groupArray((x, toString(json.m), json.aa.size1))) FROM t_mem WHERE x % 37 < 3);

CHECK TABLE t_regions;

DROP TABLE t_mem;
DROP TABLE t_regions;
