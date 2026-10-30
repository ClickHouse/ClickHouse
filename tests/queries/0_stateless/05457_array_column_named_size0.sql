-- An `Array` column named like `a.size0` is a column of its own, not the array sizes of a `Nested` column `a`.

DROP TABLE IF EXISTS t_size0_wide;
DROP TABLE IF EXISTS t_size0_missing;
DROP TABLE IF EXISTS t_size0_alter;
DROP TABLE IF EXISTS t_size0_log;

CREATE TABLE t_size0_wide (k UInt8, `a.size0` Array(UInt8)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_size0_wide VALUES (1, [1, 2]), (2, []);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_size0_wide' AND active;
SELECT k, `a.size0` FROM t_size0_wide ORDER BY k;
SELECT k, length(`a.size0`) FROM t_size0_wide ORDER BY k;
SELECT a FROM t_size0_wide ORDER BY k;

CREATE TABLE t_size0_missing (a LowCardinality(String), `a.size0` Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_size0_missing (a) VALUES ('x');
INSERT INTO t_size0_missing VALUES ('y', [1]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_size0_missing' AND active;
SELECT a, `a.size0` FROM t_size0_missing ORDER BY a;

CREATE TABLE t_size0_alter (k UInt8) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_size0_alter VALUES (1);
ALTER TABLE t_size0_alter ADD COLUMN `a.size0` Array(UInt8);
INSERT INTO t_size0_alter VALUES (2, [3]);
SELECT k, `a.size0` FROM t_size0_alter ORDER BY k;
OPTIMIZE TABLE t_size0_alter FINAL;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_size0_alter' AND active;
SELECT k, `a.size0` FROM t_size0_alter ORDER BY k;

CREATE TABLE t_size0_log (k UInt8, `a.size0` Array(UInt8)) ENGINE = Log;
INSERT INTO t_size0_log VALUES (1, [1, 2]), (2, []);
SELECT k, `a.size0`, length(`a.size0`) FROM t_size0_log ORDER BY k;

DROP TABLE t_size0_wide;
DROP TABLE t_size0_missing;
DROP TABLE t_size0_alter;
DROP TABLE t_size0_log;
