-- The same as `05615_lwu_default_reads_sorting_key_column`, but the sorting key reads a subcolumn `k.x` of the tuple
-- column `k`, which the part does not store. `z` must read the value the `DEFAULT` of `k` gave before patches, whether
-- `k` is selected or not.

DROP TABLE IF EXISTS t_lwu_default_reads_key_subcolumn;

CREATE TABLE t_lwu_default_reads_key_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_reads_key_subcolumn VALUES (0);
SYSTEM STOP MERGES t_lwu_default_reads_key_subcolumn;

ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN k Tuple(x UInt32, y UInt32), MODIFY ORDER BY (id, k.x);
ALTER TABLE t_lwu_default_reads_key_subcolumn MODIFY COLUMN k Tuple(x UInt32, y UInt32) DEFAULT tuple(a + 100, a + 200);
ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN z UInt32 DEFAULT a + tupleElement(k, 1);

UPDATE t_lwu_default_reads_key_subcolumn SET a = 10 WHERE 1;

SELECT 'patched';
SELECT a, k, z FROM t_lwu_default_reads_key_subcolumn;
SELECT z FROM t_lwu_default_reads_key_subcolumn;
SELECT a, z FROM t_lwu_default_reads_key_subcolumn;
SELECT k.x, z FROM t_lwu_default_reads_key_subcolumn;

SELECT 'merged';
ALTER TABLE t_lwu_default_reads_key_subcolumn MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_default_reads_key_subcolumn;
OPTIMIZE TABLE t_lwu_default_reads_key_subcolumn FINAL;
SELECT a, k, z FROM t_lwu_default_reads_key_subcolumn;
SELECT z FROM t_lwu_default_reads_key_subcolumn;

DROP TABLE t_lwu_default_reads_key_subcolumn;

-- The same with a pending mutation of an unrelated column applied on the fly before the patch.

CREATE TABLE t_lwu_default_reads_key_subcolumn (id UInt32, u UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_reads_key_subcolumn VALUES (0, 0);
SYSTEM STOP MERGES t_lwu_default_reads_key_subcolumn;

ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN k Tuple(x UInt32, y UInt32), MODIFY ORDER BY (id, k.x);
ALTER TABLE t_lwu_default_reads_key_subcolumn MODIFY COLUMN k Tuple(x UInt32, y UInt32) DEFAULT tuple(a + 100, a + 200);
ALTER TABLE t_lwu_default_reads_key_subcolumn ADD COLUMN z UInt32 DEFAULT a + tupleElement(k, 1);

ALTER TABLE t_lwu_default_reads_key_subcolumn UPDATE u = 1 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_default_reads_key_subcolumn SET a = 10 WHERE 1;

SELECT 'pending mutation';
SET apply_mutations_on_fly = 1;
SELECT u, a, k, z FROM t_lwu_default_reads_key_subcolumn;
SELECT z FROM t_lwu_default_reads_key_subcolumn;
SELECT u, z FROM t_lwu_default_reads_key_subcolumn;

DROP TABLE t_lwu_default_reads_key_subcolumn;
