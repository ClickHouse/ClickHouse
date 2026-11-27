-- After KILL MUTATION drops a pending RENAME COLUMN, a column name reads another column of the part:
-- a read with the columns cache must return what a read without it does.

DROP TABLE IF EXISTS t_cc_killed_rename;
CREATE TABLE t_cc_killed_rename (p UInt8, id UInt64, a UInt64, b UInt64, j JSON, other JSON(a UInt64))
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 100;

SYSTEM STOP MERGES t_cc_killed_rename;
INSERT INTO t_cc_killed_rename SELECT 0, number, 1, 2, '{"a":3}', '{"a":4}' FROM numbers(1000);
INSERT INTO t_cc_killed_rename SELECT 1, number, 1, 2, '{"a":3}', '{"a":4}' FROM numbers(10);
SELECT partition, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_killed_rename' AND active ORDER BY partition;

ALTER TABLE t_cc_killed_rename RENAME COLUMN a TO old_a, RENAME COLUMN b TO a, RENAME COLUMN j TO old_j, RENAME COLUMN other TO j
SETTINGS alter_sync = 0, mutations_sync = 0;

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;

-- The rename is pending: `a` and `j` read the part's `b` and `other`.
SELECT p, countIf(a = 2), countIf(j.a = 4) FROM t_cc_killed_rename GROUP BY p ORDER BY p;

KILL MUTATION WHERE database = currentDatabase() AND table = 't_cc_killed_rename' SYNC FORMAT Null;

SELECT p, countIf(a = 2), countIf(j.a = 4) FROM t_cc_killed_rename GROUP BY p ORDER BY p;
SELECT p, countIf(a = 2), countIf(j.a = 4) FROM t_cc_killed_rename GROUP BY p ORDER BY p SETTINGS use_columns_cache = 0;
SELECT count() FROM t_cc_killed_rename WHERE j.a = 4;

DROP TABLE t_cc_killed_rename;
