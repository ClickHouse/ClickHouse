-- Dropping the last stored column preserves the read values of untouched missing columns.
DROP TABLE IF EXISTS t_memory_untouched_missing;
CREATE TABLE t_memory_untouched_missing (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_untouched_missing VALUES (7);
ALTER TABLE t_memory_untouched_missing ADD COLUMN y UInt64 DEFAULT 257;
SELECT y FROM t_memory_untouched_missing;
ALTER TABLE t_memory_untouched_missing DROP COLUMN x;
SELECT y FROM t_memory_untouched_missing;
DROP TABLE t_memory_untouched_missing;

-- A replacement default reads the same untouched missing value and retains the row count.
DROP TABLE IF EXISTS t_memory_readd_missing;
CREATE TABLE t_memory_readd_missing (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_readd_missing VALUES (7);
ALTER TABLE t_memory_readd_missing ADD COLUMN y UInt64 DEFAULT 257;
ALTER TABLE t_memory_readd_missing DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT y + 1;
SELECT x, y FROM t_memory_readd_missing;
DROP TABLE t_memory_readd_missing;

-- Added columns keep their defaults after a later rename or Nested expansion.
DROP TABLE IF EXISTS t_memory_added_rename;
CREATE TABLE t_memory_added_rename (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_added_rename VALUES (7);
ALTER TABLE t_memory_added_rename DROP COLUMN x, ADD COLUMN a UInt64 DEFAULT 257, RENAME COLUMN a TO b;
SELECT b FROM t_memory_added_rename;
DROP TABLE t_memory_added_rename;

DROP TABLE IF EXISTS t_memory_added_nested;
CREATE TABLE t_memory_added_nested (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_added_nested VALUES (7);
ALTER TABLE t_memory_added_nested DROP COLUMN x, ADD COLUMN n Nested(a UInt64), MODIFY COLUMN `n.a` DEFAULT [257];
SELECT `n.a` FROM t_memory_added_nested;
DROP TABLE t_memory_added_nested;

-- The saved row count is available while evaluating a constant tuple alias parent.
DROP TABLE IF EXISTS t_memory_constant_alias;
CREATE TABLE t_memory_constant_alias (id UInt8, a Tuple(v UInt64) ALIAS tuple(9), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_constant_alias (id, x) VALUES (1, 0);
ALTER TABLE t_memory_constant_alias DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.v + 1;
SELECT id, a.v, x FROM t_memory_constant_alias;
DROP TABLE t_memory_constant_alias;
