-- The sort-key column `k` is not stored in the part, and `z` reads it. A lightweight `UPDATE` of `a`, which the `DEFAULT`
-- of `k` reads too, makes `z` evaluated again. `k` keeps the value its `DEFAULT` gave, as the rows of the part are sorted
-- by it, and `z` must read that value whether `k` is selected or not.

DROP TABLE IF EXISTS t_lwu_default_reads_key;

CREATE TABLE t_lwu_default_reads_key (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_reads_key VALUES (0);
SYSTEM STOP MERGES t_lwu_default_reads_key;

ALTER TABLE t_lwu_default_reads_key ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_reads_key ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_reads_key MODIFY COLUMN k UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_default_reads_key ADD COLUMN z UInt32 DEFAULT a + k;

UPDATE t_lwu_default_reads_key SET a = 10 WHERE 1;

SELECT 'patched';
SELECT a, k, z FROM t_lwu_default_reads_key;
SELECT z FROM t_lwu_default_reads_key;
SELECT a, z FROM t_lwu_default_reads_key;
SELECT z FROM t_lwu_default_reads_key WHERE z > 0;

SELECT 'merged';
ALTER TABLE t_lwu_default_reads_key MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_default_reads_key;
OPTIMIZE TABLE t_lwu_default_reads_key FINAL;
SELECT a, k, z FROM t_lwu_default_reads_key;
SELECT z FROM t_lwu_default_reads_key;

DROP TABLE t_lwu_default_reads_key;

-- The same with a pending mutation of an unrelated column applied on the fly before the patch.

CREATE TABLE t_lwu_default_reads_key (id UInt32, u UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_reads_key VALUES (0, 0);
SYSTEM STOP MERGES t_lwu_default_reads_key;

ALTER TABLE t_lwu_default_reads_key ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_reads_key ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_reads_key MODIFY COLUMN k UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_default_reads_key ADD COLUMN z UInt32 DEFAULT a + k;

ALTER TABLE t_lwu_default_reads_key UPDATE u = 1 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_default_reads_key SET a = 10 WHERE 1;

SELECT 'pending mutation';
SET apply_mutations_on_fly = 1;
SELECT u, a, k, z FROM t_lwu_default_reads_key;
SELECT z FROM t_lwu_default_reads_key;
SELECT u, z FROM t_lwu_default_reads_key;

DROP TABLE t_lwu_default_reads_key;
