-- Sub-objects of SHARED typed paths read over the regions of v2 parts, compared with a Memory table: a sub-object
-- holding a JSON path, whose shared data seeks by the marks inside its region, read alone, with the paths it holds,
-- with cut granules and filters.

SET max_threads = 1, check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_regions;

CREATE TABLE t_mem (x UInt64, json JSON(`g.j` JSON(max_dynamic_paths = 1), `g.k` UInt32, `g.l` Array(String), `o.p` String))
ENGINE = Memory;

CREATE TABLE t_regions (x UInt64, json JSON(SHARED `g.j` JSON(max_dynamic_paths = 1), SHARED `g.k` UInt32,
    SHARED `g.l` Array(String), SHARED `o.p` String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', object_shared_data_serialization_version = 'advanced',
         merge_max_block_size = 7;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"g":{"j":{"k0":', toString(number), ',"k1":"j', toString(number), '","k2":', toString(number % 5), '}',
    ',"k":', toString(number), ',"l":', toJSONString(arrayMap(i -> toString(i), range(number % 4))), '}',
    ',"o":{"p":"p', toString(number), '"}',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, json FROM src;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 0;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 1;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 2;
INSERT INTO t_regions SELECT x, json FROM src WHERE x % 4 = 3;
OPTIMIZE TABLE t_regions FINAL;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_regions' AND active;

SELECT 'json.^g',         (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_regions) = (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_mem);
SELECT 'json.^g.j',       (SELECT arraySort(groupArray((x, toString(json.^`g.j`)))) FROM t_regions) = (SELECT arraySort(groupArray((x, toString(json.^`g.j`)))) FROM t_mem);
SELECT 'json.^g, paths',  (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.g.j.k1), json.g.k, json.g.l.size0))) FROM t_regions)
    = (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.g.j.k1), json.g.k, json.g.l.size0))) FROM t_mem);
SELECT 'two sub-objects', (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.^o)))) FROM t_regions)
    = (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.^o)))) FROM t_mem);
SELECT 'max_block_size = 5', (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.g.j.k2)))) FROM t_regions SETTINGS max_block_size = 5)
    = (SELECT arraySort(groupArray((x, toString(json.^g), toString(json.g.j.k2)))) FROM t_mem);
SELECT 'WHERE x', (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_regions WHERE x % 37 < 3)
    = (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_mem WHERE x % 37 < 3);
SELECT 'PREWHERE', (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_regions PREWHERE json.g.k % 11 = 0)
    = (SELECT arraySort(groupArray((x, toString(json.^g)))) FROM t_mem WHERE json.g.k % 11 = 0);

-- Without the columns cache, which would serve the column the queries above read.
SELECT toString(json.^g) FROM t_regions SETTINGS log_comment = '05486_sub_object', use_columns_cache = 0 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT 'layouts read', ProfileEvents['ObjectTypedSharedDataRegionReads'] > 0, ProfileEvents['ObjectTypedSharedDataPlainLayoutReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05486_sub_object';

CHECK TABLE t_regions;

DROP TABLE t_mem;
DROP TABLE t_regions;
