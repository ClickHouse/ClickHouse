-- A `DEFAULT` column evaluated again after a lightweight `UPDATE` of the column it reads is evaluated only
-- in the rows that still hold its `DEFAULT`. In a row a patch overwrote, the expression may throw on values
-- its result is never used for: here `intDiv(100, a)` with `a` patched to 0 in the row where `z` is patched.
-- The result must not depend on which rows are read together in one block.

SET alter_sync = 0, mutations_sync = 0;

DROP TABLE IF EXISTS t_lwu_patched_row_v1;
DROP TABLE IF EXISTS t_lwu_patched_row_v2;
DROP TABLE IF EXISTS t_lwu_patched_row_pending;

CREATE TABLE t_lwu_patched_row_v1 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v1';

CREATE TABLE t_lwu_patched_row_v2 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_patched_row_v1 VALUES (0), (1);
INSERT INTO t_lwu_patched_row_v2 VALUES (0), (1);

SYSTEM STOP MERGES t_lwu_patched_row_v1;
SYSTEM STOP MERGES t_lwu_patched_row_v2;

ALTER TABLE t_lwu_patched_row_v1 ADD COLUMN a UInt32 DEFAULT 1;
ALTER TABLE t_lwu_patched_row_v1 ADD COLUMN z UInt32 DEFAULT intDiv(100, a);
ALTER TABLE t_lwu_patched_row_v1 ADD COLUMN y UInt32 DEFAULT z + 1;

ALTER TABLE t_lwu_patched_row_v2 ADD COLUMN a UInt32 DEFAULT 1;
ALTER TABLE t_lwu_patched_row_v2 ADD COLUMN z UInt32 DEFAULT intDiv(100, a);
ALTER TABLE t_lwu_patched_row_v2 ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_patched_row_v1 SET z = 999 WHERE id = 0;
UPDATE t_lwu_patched_row_v1 SET a = 0 WHERE id = 0;

UPDATE t_lwu_patched_row_v2 SET z = 999 WHERE id = 0;
UPDATE t_lwu_patched_row_v2 SET a = 0 WHERE id = 0;

SELECT 'v1';
SELECT id, a, z, y FROM t_lwu_patched_row_v1 ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_patched_row_v1 ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;
SELECT id, z FROM t_lwu_patched_row_v1 ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;

SELECT 'v2';
SELECT id, a, z, y FROM t_lwu_patched_row_v2 ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_patched_row_v2 ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;
SELECT id, z FROM t_lwu_patched_row_v2 ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;

-- A pending mutation is applied on the fly by a reader step of its own, and the later patches are applied
-- at the boundary of that step, where the `DEFAULT` columns are evaluated again.

CREATE TABLE t_lwu_patched_row_pending (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1;

INSERT INTO t_lwu_patched_row_pending VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_patched_row_pending;

ALTER TABLE t_lwu_patched_row_pending ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_patched_row_pending ADD COLUMN a UInt32 DEFAULT 1;
ALTER TABLE t_lwu_patched_row_pending ADD COLUMN z UInt32 DEFAULT intDiv(100, a);
ALTER TABLE t_lwu_patched_row_pending ADD COLUMN y UInt32 DEFAULT z + 1;

-- Stays pending.
ALTER TABLE t_lwu_patched_row_pending UPDATE w = 8 WHERE 1;

UPDATE t_lwu_patched_row_pending SET z = 999 WHERE id = 0;
UPDATE t_lwu_patched_row_pending SET a = 0 WHERE id = 0;

SELECT 'pending mutation applied on the fly';
SELECT id, a, z, y, w FROM t_lwu_patched_row_pending ORDER BY id SETTINGS max_threads = 1, max_block_size = 100, apply_mutations_on_fly = 1;
SELECT id, a, z, y, w FROM t_lwu_patched_row_pending ORDER BY id SETTINGS max_threads = 1, max_block_size = 1, apply_mutations_on_fly = 1;
SELECT id, z, w FROM t_lwu_patched_row_pending ORDER BY id SETTINGS max_threads = 1, max_block_size = 100, apply_mutations_on_fly = 1;

DROP TABLE t_lwu_patched_row_v1;
DROP TABLE t_lwu_patched_row_v2;
DROP TABLE t_lwu_patched_row_pending;
