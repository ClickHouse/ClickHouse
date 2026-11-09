-- The sign, version and is_deleted subcolumns are read from the column that owns the name, even when a JSON
-- column has a path with the same full name. Here the column declared first (`j.m` / `j.t`) owns it.

DROP TABLE IF EXISTS t_ver;
DROP TABLE IF EXISTS t_ver_merge;
DROP TABLE IF EXISTS t_sign;
DROP TABLE IF EXISTS t_del;
DROP TABLE IF EXISTS t_rev;

SELECT '-- version';
CREATE TABLE t_ver (id UInt64, `j.m` Map(String, UInt64), j JSON(`m.size0` UInt64)) ENGINE = ReplacingMergeTree(j.m.size0) ORDER BY id;
INSERT INTO t_ver SETTINGS optimize_on_insert = 1 VALUES (1, {'a': 1, 'b': 2}, '{"m": {"size0": 1}}'), (1, {'c': 3}, '{"m": {"size0": 5}}');
SELECT length(j.m), j.m.size0 FROM t_ver;

-- The same rows in two parts, merged.
CREATE TABLE t_ver_merge (id UInt64, `j.m` Map(String, UInt64), j JSON(`m.size0` UInt64)) ENGINE = ReplacingMergeTree(j.m.size0) ORDER BY id;
INSERT INTO t_ver_merge SETTINGS optimize_on_insert = 0 VALUES (1, {'a': 1, 'b': 2}, '{"m": {"size0": 1}}');
INSERT INTO t_ver_merge SETTINGS optimize_on_insert = 0 VALUES (1, {'c': 3}, '{"m": {"size0": 5}}');
OPTIMIZE TABLE t_ver_merge FINAL;
SELECT length(j.m), j.m.size0 FROM t_ver_merge;

SELECT '-- sign';
CREATE TABLE t_sign (id UInt64, `j.t` Tuple(s Int8), j JSON(`t.s` String)) ENGINE = CollapsingMergeTree(j.t.s) ORDER BY id;
INSERT INTO t_sign SETTINGS optimize_on_insert = 1 VALUES (1, tuple(1), '{"t": {"s": "a"}}'), (1, tuple(-1), '{"t": {"s": "b"}}'), (2, tuple(1), '{"t": {"s": "c"}}');
SELECT id FROM t_sign;

SELECT '-- is_deleted';
CREATE TABLE t_del (id UInt64, ver UInt64, `j.t` Tuple(d UInt8), j JSON(`t.d` String)) ENGINE = ReplacingMergeTree(ver, j.t.d) ORDER BY id;
INSERT INTO t_del SETTINGS optimize_on_insert = 1 VALUES (1, 1, tuple(0), '{"t": {"d": "x"}}'), (1, 2, tuple(1), '{"t": {"d": "y"}}');
SELECT ver, j.t.d FROM t_del;
SELECT count() FROM t_del FINAL;

SELECT '-- JSON declared first owns the name';
CREATE TABLE t_rev (id UInt64, j JSON(`m.size0` UInt64), `j.m` Map(String, UInt64)) ENGINE = ReplacingMergeTree(j.m.size0) ORDER BY id;
INSERT INTO t_rev SETTINGS optimize_on_insert = 1 VALUES (1, '{"m": {"size0": 1}}', {'a': 1, 'b': 2}), (1, '{"m": {"size0": 5}}', {'c': 3});
SELECT j.m.size0, length(j.m) FROM t_rev;

DROP TABLE t_ver;
DROP TABLE t_ver_merge;
DROP TABLE t_sign;
DROP TABLE t_del;
DROP TABLE t_rev;
