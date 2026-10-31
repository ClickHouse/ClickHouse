-- A column added under the name of a subcolumn must not change what a TTL expression reads: the parts written before
-- keep TTL infos computed from the subcolumn.

-- A rows TTL reads the sizes of `a`.
DROP TABLE IF EXISTS t_rows_ttl;
CREATE TABLE t_rows_ttl (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`);
INSERT INTO t_rows_ttl VALUES ([1]);
ALTER TABLE t_rows_ttl ADD COLUMN `a.size0` UInt64 DEFAULT 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_rows_ttl ADD COLUMN `b.size0` UInt64 DEFAULT 0;
SELECT a, `a.size0` FROM t_rows_ttl;
DROP TABLE t_rows_ttl;

-- A column TTL reads the sizes of `a`.
DROP TABLE IF EXISTS t_column_ttl;
CREATE TABLE t_column_ttl (a Array(UInt64), c UInt64 TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_column_ttl ADD COLUMN `a.size0` UInt64 DEFAULT 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_column_ttl;

-- A TTL that starts reading the column in the same `ALTER`.
DROP TABLE IF EXISTS t_same_alter;
CREATE TABLE t_same_alter (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_same_alter ADD COLUMN `a.size0` UInt64 DEFAULT 0, MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_same_alter;

-- A TTL that starts reading the column later, while a part still misses it.
DROP TABLE IF EXISTS t_modify_ttl;
CREATE TABLE t_modify_ttl (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_modify_ttl VALUES ([1]);
ALTER TABLE t_modify_ttl ADD COLUMN `a.size0` UInt64 DEFAULT 0;
INSERT INTO t_modify_ttl VALUES ([2], 1);
ALTER TABLE t_modify_ttl MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, `a.size0` FROM t_modify_ttl ORDER BY a;
OPTIMIZE TABLE t_modify_ttl FINAL;
ALTER TABLE t_modify_ttl MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_modify_ttl FINAL;
SELECT a, `a.size0` FROM t_modify_ttl ORDER BY a;
DROP TABLE t_modify_ttl;

-- A table that declares both columns from the start: every part holds the column.
DROP TABLE IF EXISTS t_declared;
CREATE TABLE t_declared (a Array(UInt64), `a.size0` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_declared VALUES ([1], 0), ([2], 1);
ALTER TABLE t_declared MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.size0`) SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_declared FINAL;
SELECT a, `a.size0` FROM t_declared ORDER BY a;
DROP TABLE t_declared;
