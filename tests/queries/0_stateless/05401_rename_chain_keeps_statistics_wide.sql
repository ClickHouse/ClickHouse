-- A chain of `RENAME COLUMN` onto names the same `ALTER` frees keeps each column's statistics on a wide part.
-- Each column's values lie outside every other column's range, so statistics of another column prune the probe to 0 rows.

SET materialize_statistics_on_insert = 1, use_statistics_for_part_pruning = 1;

DROP TABLE IF EXISTS t_rename_chain3;
CREATE TABLE t_rename_chain3 (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_chain3 VALUES (1, 50, 100), (2, 60, 1000);
ALTER TABLE t_rename_chain3 RENAME COLUMN a TO tmp, RENAME COLUMN b TO a, RENAME COLUMN c TO b SETTINGS mutations_sync = 2;
SELECT 'chain3', column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_chain3' AND active AND column IN ('tmp', 'a', 'b') ORDER BY column;
SELECT 'chain3', count() FROM t_rename_chain3 WHERE tmp = 1;
SELECT 'chain3', count() FROM t_rename_chain3 WHERE a = 50;
SELECT 'chain3', count() FROM t_rename_chain3 WHERE b = 1000;
DROP TABLE t_rename_chain3;

DROP TABLE IF EXISTS t_rename_chain2;
CREATE TABLE t_rename_chain2 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_chain2 VALUES (1, 50), (2, 60);
ALTER TABLE t_rename_chain2 RENAME COLUMN a TO tmp, RENAME COLUMN b TO a SETTINGS mutations_sync = 2;
SELECT 'chain2', column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_chain2' AND active AND column IN ('tmp', 'a') ORDER BY column;
SELECT 'chain2', count() FROM t_rename_chain2 WHERE tmp = 1;
SELECT 'chain2', count() FROM t_rename_chain2 WHERE a = 50;
DROP TABLE t_rename_chain2;

DROP TABLE IF EXISTS t_rename_drop_chain;
CREATE TABLE t_rename_drop_chain (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_drop_chain VALUES (1, 50, 100, 5000), (2, 60, 1000, 6000);
ALTER TABLE t_rename_drop_chain DROP COLUMN a, RENAME COLUMN b TO a, RENAME COLUMN c TO b, RENAME COLUMN d TO c SETTINGS mutations_sync = 2;
SELECT 'drop_chain', column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_rename_drop_chain' AND active AND column IN ('a', 'b', 'c') ORDER BY column;
SELECT 'drop_chain', count() FROM t_rename_drop_chain WHERE a = 50;
SELECT 'drop_chain', count() FROM t_rename_drop_chain WHERE b = 1000;
SELECT 'drop_chain', count() FROM t_rename_drop_chain WHERE c = 5000;
DROP TABLE t_rename_drop_chain;
