-- Tags: zookeeper

-- The column TTL info of a part is keyed by column name, so a mutation that renames a column has to re-key it:
-- a column renamed onto the name of a dropped or renamed-away column must not take over that column's TTL bounds.

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_swap;
DROP TABLE IF EXISTS t_replicated;
DROP TABLE IF EXISTS t_no_info;

CREATE TABLE t_wide (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32 TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0;
SYSTEM STOP TTL MERGES t_wide;
INSERT INTO t_wide VALUES (1, now() - 100, 42, 100);
ALTER TABLE t_wide DROP COLUMN a, RENAME COLUMN c TO a SETTINGS mutations_sync = 2;
SELECT 'wide', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wide' AND active;
SELECT 'wide ttl of a is due', column_ttl_min <= now() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wide' AND active AND column = 'a';
-- Reload the part from disk.
DETACH TABLE t_wide;
ATTACH TABLE t_wide;
SYSTEM START TTL MERGES t_wide;
OPTIMIZE TABLE t_wide FINAL;
SELECT 'wide', id, a FROM t_wide;

CREATE TABLE t_compact (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32 TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0;
SYSTEM STOP TTL MERGES t_compact;
INSERT INTO t_compact VALUES (1, now() - 100, 42, 100);
ALTER TABLE t_compact DROP COLUMN a, RENAME COLUMN c TO a SETTINGS mutations_sync = 2;
SELECT 'compact', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
DETACH TABLE t_compact;
ATTACH TABLE t_compact;
SYSTEM START TTL MERGES t_compact;
OPTIMIZE TABLE t_compact FINAL;
SELECT 'compact', id, a FROM t_compact;

-- Swap two columns with three ALTERs.
CREATE TABLE t_swap (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32 TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0;
SYSTEM STOP TTL MERGES t_swap;
INSERT INTO t_swap VALUES (1, now() - 100, 42, 100);
ALTER TABLE t_swap RENAME COLUMN a TO tmp SETTINGS mutations_sync = 2;
ALTER TABLE t_swap RENAME COLUMN c TO a SETTINGS mutations_sync = 2;
ALTER TABLE t_swap RENAME COLUMN tmp TO c SETTINGS mutations_sync = 2;
SYSTEM START TTL MERGES t_swap;
OPTIMIZE TABLE t_swap FINAL;
SELECT 'swap', id, a, c FROM t_swap;

CREATE TABLE t_replicated (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32 TTL d + INTERVAL 1 SECOND)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', '1') ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0;
SYSTEM STOP TTL MERGES t_replicated;
INSERT INTO t_replicated VALUES (1, now() - 100, 42, 100);
ALTER TABLE t_replicated DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 2, mutations_sync = 2;
SYSTEM START TTL MERGES t_replicated;
OPTIMIZE TABLE t_replicated FINAL;
SELECT 'replicated', id, a FROM t_replicated;

-- The renamed column is stored without TTL info.
CREATE TABLE t_no_info (id UInt32, d DateTime, a UInt32 TTL d + INTERVAL 1 YEAR, c UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_with_ttl_timeout = 0;
SYSTEM STOP TTL MERGES t_no_info;
INSERT INTO t_no_info VALUES (1, now() - 100, 42, 100);
ALTER TABLE t_no_info MODIFY COLUMN c UInt32 TTL d + INTERVAL 1 SECOND SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_no_info DROP COLUMN a, RENAME COLUMN c TO a SETTINGS mutations_sync = 2;
SELECT 'no info', column_ttl_min IS NULL FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_no_info' AND active AND column = 'a';
SYSTEM START TTL MERGES t_no_info;
OPTIMIZE TABLE t_no_info FINAL;
SELECT 'no info', id, a FROM t_no_info;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_swap;
DROP TABLE t_replicated;
DROP TABLE t_no_info;
