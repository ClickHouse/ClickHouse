-- A TTL expression must not start reading a column named like a subcolumn of another column (`a.size0` of an `Array`
-- column `a`) under a new name either, while some part misses it: the part still reads the column under its old name,
-- as the sizes of `a`, for a TTL input.

-- The rename and the TTL in the same `ALTER`.
DROP TABLE IF EXISTS t_same_alter;
CREATE TABLE t_same_alter (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_same_alter;
INSERT INTO t_same_alter VALUES ([]);
ALTER TABLE t_same_alter ADD COLUMN `a.size0` UInt64 DEFAULT 1;
SELECT a, `a.size0` FROM t_same_alter;
ALTER TABLE t_same_alter
    RENAME COLUMN `a.size0` TO `a.sz`,
    MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.sz`)
    SETTINGS materialize_ttl_after_modify = 0, alter_sync = 0, mutations_sync = 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, `a.size0` FROM t_same_alter;
SYSTEM START MERGES t_same_alter;
OPTIMIZE TABLE t_same_alter FINAL;
SELECT count() FROM t_same_alter;
DROP TABLE t_same_alter;

-- The TTL in a later `ALTER`, while the rename is not yet applied to the part.
DROP TABLE IF EXISTS t_pending_rename;
CREATE TABLE t_pending_rename (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_pending_rename;
INSERT INTO t_pending_rename VALUES ([]);
ALTER TABLE t_pending_rename ADD COLUMN `a.size0` UInt64 DEFAULT 1;
ALTER TABLE t_pending_rename RENAME COLUMN `a.size0` TO `a.sz` SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT a, `a.sz` FROM t_pending_rename;
ALTER TABLE t_pending_rename MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.sz`)
    SETTINGS materialize_ttl_after_modify = 0, alter_sync = 0, mutations_sync = 0; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, `a.sz` FROM t_pending_rename;
DROP TABLE t_pending_rename;

-- A rename of a column the part holds does not matter.
DROP TABLE IF EXISTS t_held;
CREATE TABLE t_held (a Array(UInt64), `a.size0` UInt64) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_held;
INSERT INTO t_held VALUES ([], 1);
ALTER TABLE t_held
    RENAME COLUMN `a.size0` TO `a.sz`,
    MODIFY TTL addDays(toDate('2000-01-01'), 36500 * `a.sz`)
    SETTINGS materialize_ttl_after_modify = 0, alter_sync = 0, mutations_sync = 0;
SELECT a, `a.sz` FROM t_held;
DROP TABLE t_held;
