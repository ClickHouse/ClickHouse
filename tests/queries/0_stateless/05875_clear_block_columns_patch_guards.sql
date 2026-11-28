-- Tags: zookeeper

-- `CLEAR COLUMN _block_number | _block_offset` changes the values by which patch parts find their rows, so it is
-- rejected while patches are not applied, and lightweight updates are rejected while it is not finished.

SET mutations_sync = 2;
SET enable_lightweight_update = 1;

-- `CLEAR COLUMN` with unapplied patches.

DROP TABLE IF EXISTS t_clear_block_guard;

CREATE TABLE t_clear_block_guard (p UInt8, id UInt64, s String) ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_clear_block_guard VALUES (0, 1, 'a'), (0, 3, 'c'), (1, 1, 'a'), (1, 3, 'c');
INSERT INTO t_clear_block_guard VALUES (0, 2, 'b'), (0, 4, 'd'), (1, 2, 'b'), (1, 4, 'd');
OPTIMIZE TABLE t_clear_block_guard FINAL;

UPDATE t_clear_block_guard SET s = 'x' WHERE p = 1 AND id = 3;

ALTER TABLE t_clear_block_guard CLEAR COLUMN _block_number; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_clear_block_guard CLEAR COLUMN _block_offset IN PARTITION 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_clear_block_guard CLEAR COLUMN _block_number IN PARTITION 0;

ALTER TABLE t_clear_block_guard APPLY PATCHES;
ALTER TABLE t_clear_block_guard CLEAR COLUMN _block_number;

SELECT 'clear after patches';
SELECT p, id, s, _block_number = toUInt64(splitByChar('_', _part)[2]), _block_offset FROM t_clear_block_guard ORDER BY p, id;

DROP TABLE t_clear_block_guard;

-- Lightweight updates while `CLEAR COLUMN` is not finished, with both versions of patch parts.

DROP TABLE IF EXISTS t_clear_block_guard_v1;
DROP TABLE IF EXISTS t_clear_block_guard_v2;

CREATE TABLE t_clear_block_guard_v1 (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
CREATE TABLE t_clear_block_guard_v2 (id UInt64, s String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_clear_block_guard_v1 VALUES (1, 'a'), (3, 'c');
INSERT INTO t_clear_block_guard_v1 VALUES (2, 'b'), (4, 'd');
INSERT INTO t_clear_block_guard_v2 VALUES (1, 'a'), (3, 'c');
INSERT INTO t_clear_block_guard_v2 VALUES (2, 'b'), (4, 'd');
OPTIMIZE TABLE t_clear_block_guard_v1 FINAL;
OPTIMIZE TABLE t_clear_block_guard_v2 FINAL;

SYSTEM STOP MERGES t_clear_block_guard_v1;
SYSTEM STOP MERGES t_clear_block_guard_v2;

ALTER TABLE t_clear_block_guard_v1 CLEAR COLUMN _block_number SETTINGS alter_sync = 0;
ALTER TABLE t_clear_block_guard_v2 CLEAR COLUMN _block_number SETTINGS alter_sync = 0;

UPDATE t_clear_block_guard_v1 SET s = 'x' WHERE id = 3; -- { serverError SUPPORT_IS_DISABLED }
UPDATE t_clear_block_guard_v2 SET s = 'x' WHERE id = 3; -- { serverError SUPPORT_IS_DISABLED }
DELETE FROM t_clear_block_guard_v1 WHERE id = 4 SETTINGS lightweight_delete_mode = 'lightweight_update_force'; -- { serverError SUPPORT_IS_DISABLED }

SYSTEM START MERGES t_clear_block_guard_v1;
SYSTEM START MERGES t_clear_block_guard_v2;

-- Waits for the previous mutations too.
ALTER TABLE t_clear_block_guard_v1 UPDATE s = s WHERE 0;
ALTER TABLE t_clear_block_guard_v2 UPDATE s = s WHERE 0;

UPDATE t_clear_block_guard_v1 SET s = 'x' WHERE id = 3;
UPDATE t_clear_block_guard_v2 SET s = 'x' WHERE id = 3;

SELECT 'v1';
SELECT id, s, _block_number, _block_offset FROM t_clear_block_guard_v1 ORDER BY id;
SELECT 'v2';
SELECT id, s, _block_number, _block_offset FROM t_clear_block_guard_v2 ORDER BY id;

DROP TABLE t_clear_block_guard_v1;
DROP TABLE t_clear_block_guard_v2;

-- The same for a replicated table, on both replicas.

DROP TABLE IF EXISTS t_clear_block_guard_r1;
DROP TABLE IF EXISTS t_clear_block_guard_r2;

CREATE TABLE t_clear_block_guard_r1 (id UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_block_guard', 'r1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE t_clear_block_guard_r2 (id UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_block_guard', 'r2') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_clear_block_guard_r1 VALUES (1, 'a'), (3, 'c');
INSERT INTO t_clear_block_guard_r1 VALUES (2, 'b'), (4, 'd');
OPTIMIZE TABLE t_clear_block_guard_r1 FINAL;
SYSTEM SYNC REPLICA t_clear_block_guard_r2;

UPDATE t_clear_block_guard_r1 SET s = 'y' WHERE id = 1;
SYSTEM SYNC REPLICA t_clear_block_guard_r2;

ALTER TABLE t_clear_block_guard_r1 CLEAR COLUMN _block_number; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_clear_block_guard_r1 APPLY PATCHES;
SYSTEM SYNC REPLICA t_clear_block_guard_r2;

SYSTEM STOP MERGES t_clear_block_guard_r1;
SYSTEM STOP MERGES t_clear_block_guard_r2;

ALTER TABLE t_clear_block_guard_r1 CLEAR COLUMN _block_number SETTINGS alter_sync = 0;

UPDATE t_clear_block_guard_r1 SET s = 'x' WHERE id = 3; -- { serverError SUPPORT_IS_DISABLED }
UPDATE t_clear_block_guard_r2 SET s = 'x' WHERE id = 3; -- { serverError SUPPORT_IS_DISABLED }
UPDATE t_clear_block_guard_r2 SET s = 'x' WHERE id = 3 SETTINGS update_sequential_consistency = 0; -- { serverError SUPPORT_IS_DISABLED }

SYSTEM START MERGES t_clear_block_guard_r1;
SYSTEM START MERGES t_clear_block_guard_r2;

-- Waits for the previous mutations too, on both replicas.
ALTER TABLE t_clear_block_guard_r1 UPDATE s = s WHERE 0;

UPDATE t_clear_block_guard_r2 SET s = 'x' WHERE id = 3;
SYSTEM SYNC REPLICA t_clear_block_guard_r1;

-- The first block number of a replicated table depends on the environment, so it is compared with the one of the part.
SELECT 'replicated';
SELECT 'r1', id, s, _block_number = (SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_guard_r1' AND active AND NOT startsWith(name, 'patch')), _block_offset
FROM t_clear_block_guard_r1 ORDER BY id;
SELECT 'r2', id, s, _block_number = (SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = 't_clear_block_guard_r2' AND active AND NOT startsWith(name, 'patch')), _block_offset
FROM t_clear_block_guard_r2 ORDER BY id;

DROP TABLE t_clear_block_guard_r1;
DROP TABLE t_clear_block_guard_r2;
