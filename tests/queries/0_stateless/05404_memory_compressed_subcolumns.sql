-- Tags: memory-engine
-- Subcolumns of a `Memory` table with `compress = 1` are decompressed without the rest of the column.
-- They are compared with the same data in a table with `compress = 0`. The first block of the compressed
-- table is compressed, the second one is below `min_size_to_compress` and stays uncompressed.

DROP TABLE IF EXISTS t_compressed;
DROP TABLE IF EXISTS t_uncompressed;

CREATE TABLE t_uncompressed (id UInt64, s String, ns Nullable(String), a Array(UInt32), n Nullable(UInt64), t Tuple(x UInt64, y String), tu Tuple(UInt64, String), m Map(String, UInt64))
ENGINE = Memory SETTINGS compress = 0;
CREATE TABLE t_compressed AS t_uncompressed ENGINE = Memory SETTINGS compress = 1;

INSERT INTO t_uncompressed
SELECT number, repeat(toString(number % 10), number % 50), if(number % 7 = 0, NULL, repeat('a', number % 13)), range(number % 5),
    if(number % 3 = 0, NULL, number), (number * 2, toString(number % 100)), (number % 1000, repeat('z', number % 7)),
    mapFromArrays(arrayMap(x -> toString(x), range(number % 4)), range(number % 4))
FROM numbers(30010) SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

INSERT INTO t_compressed SELECT * FROM t_uncompressed WHERE id < 30000 SETTINGS min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO t_compressed SELECT * FROM t_uncompressed WHERE id >= 30000;

SELECT blockSize() AS rows, count() FROM t_compressed GROUP BY rows ORDER BY rows;
SELECT (SELECT total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 't_compressed')
    < (SELECT total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 't_uncompressed') / 2;

SELECT 'subcolumns';
SELECT id, s.size, a.size0, n.null, ns.null, t.x, t.y, tu.1, tu.2, m.keys, m.values, m.size0 FROM t_compressed WHERE id IN (6, 7, 29999, 30003) ORDER BY id;
SELECT (SELECT sum(cityHash64(id, s.size, a.size0, n.null, ns.null, t.x, t.y, tu.1, tu.2, m.keys, m.values, m.size0)) FROM t_compressed)
    = (SELECT sum(cityHash64(id, s.size, a.size0, n.null, ns.null, t.x, t.y, tu.1, tu.2, m.keys, m.values, m.size0)) FROM t_uncompressed);

SELECT 'functions';
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), sum(length(a)), countIf(empty(a)), countIf(isNull(n)), countIf(isNotNull(n)), count(n),
    sum(cityHash64(id, mapKeys(m), mapValues(m), length(m), tupleElement(t, 'y')))
FROM t_compressed SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)), sum(length(a)), countIf(empty(a)), countIf(isNull(n)), countIf(isNotNull(n)), count(n),
    sum(cityHash64(id, mapKeys(m), mapValues(m), length(m), tupleElement(t, 'y')))
FROM t_uncompressed SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'fallbacks';
SELECT (SELECT sum(cityHash64(id, ifNull(ns.size, 100), t.y.size, m['2'])) FROM t_compressed)
    = (SELECT sum(cityHash64(id, ifNull(ns.size, 100), t.y.size, m['2'])) FROM t_uncompressed);

SELECT 'columns with their subcolumns';
SELECT (SELECT sum(cityHash64(id, s, s.size, a, a.size0, ifNull(n, 0), n.null, t, t.x, m, m.keys, m.size0)) FROM t_compressed)
    = (SELECT sum(cityHash64(id, s, s.size, a, a.size0, ifNull(n, 0), n.null, t, t.x, m, m.keys, m.size0)) FROM t_uncompressed);

SELECT 'prewhere';
SELECT (SELECT sum(cityHash64(id, s, s.size)) FROM t_compressed PREWHERE s.size > 20) = (SELECT sum(cityHash64(id, s, s.size)) FROM t_uncompressed PREWHERE s.size > 20);
SELECT (SELECT sum(cityHash64(id, n)) FROM t_compressed PREWHERE NOT n.null) = (SELECT sum(cityHash64(id, n)) FROM t_uncompressed PREWHERE NOT n.null);
SELECT (SELECT sum(cityHash64(id, t)) FROM t_compressed PREWHERE t.x % 3 = 0) = (SELECT sum(cityHash64(id, t)) FROM t_uncompressed PREWHERE t.x % 3 = 0);
SELECT (SELECT sum(cityHash64(id, m.values)) FROM t_compressed PREWHERE m.size0 = 2) = (SELECT sum(cityHash64(id, m.values)) FROM t_uncompressed PREWHERE m.size0 = 2);

DROP TABLE t_compressed;
DROP TABLE t_uncompressed;

SELECT 'nested';
DROP TABLE IF EXISTS t_nested_compressed;
DROP TABLE IF EXISTS t_nested_uncompressed;

CREATE TABLE t_nested_uncompressed (id UInt64, nst Nested(a UInt32)) ENGINE = Memory SETTINGS compress = 0;
CREATE TABLE t_nested_compressed AS t_nested_uncompressed ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_nested_uncompressed SELECT number, range(number % 5) FROM numbers(30010)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO t_nested_compressed SELECT * FROM t_nested_uncompressed WHERE id < 30000 SETTINGS min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
INSERT INTO t_nested_compressed SELECT * FROM t_nested_uncompressed WHERE id >= 30000;
ALTER TABLE t_nested_uncompressed ADD COLUMN `nst.b` Array(String);
ALTER TABLE t_nested_compressed ADD COLUMN `nst.b` Array(String);

SELECT id, nst.b, nst.b.size0 FROM t_nested_compressed WHERE id IN (3, 4, 30004) ORDER BY id;
SELECT (SELECT sum(cityHash64(id, nst.b, nst.b.size0, length(nst.b))) FROM t_nested_compressed)
    = (SELECT sum(cityHash64(id, nst.b, nst.b.size0, length(nst.b))) FROM t_nested_uncompressed);

DROP TABLE t_nested_compressed;
DROP TABLE t_nested_uncompressed;

SELECT 'null subcolumn of the nested type';
DROP TABLE IF EXISTS t_json_compressed;
DROP TABLE IF EXISTS t_json_uncompressed;

CREATE TABLE t_json_uncompressed (id UInt64, j Nullable(JSON)) ENGINE = Memory SETTINGS compress = 0;
CREATE TABLE t_json_compressed AS t_json_uncompressed ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_json_uncompressed VALUES (1, '{"null": 5, "a": 1}'), (2, NULL), (3, '{"a": 2}');
INSERT INTO t_json_compressed SELECT * FROM t_json_uncompressed;

SELECT id, j.null, toTypeName(j.null) FROM t_json_compressed ORDER BY id;
SELECT id, j, j.null FROM t_json_compressed ORDER BY id;
SELECT (SELECT arraySort(groupArray((id, toString(j.null), isNull(j)))) FROM t_json_compressed)
    = (SELECT arraySort(groupArray((id, toString(j.null), isNull(j)))) FROM t_json_uncompressed);

DROP TABLE t_json_compressed;
DROP TABLE t_json_uncompressed;
