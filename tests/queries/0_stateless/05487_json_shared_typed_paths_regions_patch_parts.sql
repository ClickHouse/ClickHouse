-- SHARED typed paths of a v2 part read over its regions with patch parts of lightweight updates applied, compared with a
-- Memory table updated by mutations: the patches update the JSON column itself and another column.

SET max_threads = 1, mutations_sync = 1, enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_patched;

CREATE TABLE t_mem (x UInt64, y UInt64, json JSON(a UInt32, b Array(String), h JSON(max_dynamic_paths = 1)))
ENGINE = Memory;

CREATE TABLE t_patched (x UInt64, y UInt64, json JSON(SHARED a UInt32, SHARED b Array(String), SHARED h JSON(max_dynamic_paths = 1)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 16, index_granularity_bytes = '10Mi',
         object_typed_shared_data_serialization_version = 'v2', enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0;

CREATE TEMPORARY TABLE src AS SELECT number AS x, concat('{',
    '"a":', toString(number),
    ',"b":', toJSONString(arrayMap(i -> concat('b', toString(i)), range(number % 4))),
    ',"h":{"k0":', toString(number), ',"k1":"h', toString(number), '"}',
    '}') AS json FROM numbers(200);

INSERT INTO t_mem SELECT x, 0, json FROM src;
INSERT INTO t_patched SELECT x, 0, json FROM src;

UPDATE t_patched SET json = concat('{"a":', toString(x + 1000), ',"b":["u"],"h":{"k1":"u', toString(x), '","k2":1}}') WHERE x % 10 = 0;
UPDATE t_patched SET y = x * 3 WHERE x % 7 = 0;
ALTER TABLE t_mem UPDATE json = concat('{"a":', toString(x + 1000), ',"b":["u"],"h":{"k1":"u', toString(x), '","k2":1}}') WHERE x % 10 = 0;
ALTER TABLE t_mem UPDATE y = x * 3 WHERE x % 7 = 0;

SELECT 'parts', countIf(NOT startsWith(name, 'patch')) > 0, countIf(startsWith(name, 'patch')) > 0
FROM system.parts WHERE database = currentDatabase() AND table = 't_patched' AND active;

SELECT 'json.a',         (SELECT arraySort(groupArray((x, json.a))) FROM t_patched) = (SELECT arraySort(groupArray((x, json.a))) FROM t_mem);
SELECT 'json.b.size0',   (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_patched) = (SELECT arraySort(groupArray((x, json.b.size0))) FROM t_mem);
SELECT 'json.h.k1',      (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_patched) = (SELECT arraySort(groupArray((x, toString(json.h.k1)))) FROM t_mem);
SELECT 'json.h.k2',      (SELECT arraySort(groupArray((x, toString(json.h.k2)))) FROM t_patched) = (SELECT arraySort(groupArray((x, toString(json.h.k2)))) FROM t_mem);
SELECT 'y, json.a',      (SELECT arraySort(groupArray((x, y, json.a))) FROM t_patched) = (SELECT arraySort(groupArray((x, y, json.a))) FROM t_mem);
SELECT 'everything',     (SELECT arraySort(groupArray((x, y, json.a, toString(json.b), toString(json.h.k1), toString(json)))) FROM t_patched)
    = (SELECT arraySort(groupArray((x, y, json.a, toString(json.b), toString(json.h.k1), toString(json)))) FROM t_mem);
SELECT 'WHERE x',        (SELECT arraySort(groupArray((x, json.a, toString(json.h.k1)))) FROM t_patched WHERE x % 20 < 3)
    = (SELECT arraySort(groupArray((x, json.a, toString(json.h.k1)))) FROM t_mem WHERE x % 20 < 3);
SELECT 'PREWHERE',       (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1)))) FROM t_patched PREWHERE json.a > 1000)
    = (SELECT arraySort(groupArray((x, toString(json.b), toString(json.h.k1)))) FROM t_mem WHERE json.a > 1000);

SELECT '-- patches applied by a merge';
ALTER TABLE t_patched MODIFY SETTING apply_patches_on_merge = 1;
OPTIMIZE TABLE t_patched FINAL;
SELECT 'merged', (SELECT arraySort(groupArray((x, y, json.a, json.b.size0, toString(json.h.k1), toString(json)))) FROM t_patched)
    = (SELECT arraySort(groupArray((x, y, json.a, json.b.size0, toString(json.h.k1), toString(json)))) FROM t_mem);

DROP TABLE t_mem;
DROP TABLE t_patched;
