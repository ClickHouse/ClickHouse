DROP TABLE IF EXISTS t_lwu_sorting_key_dependent;
DROP TABLE IF EXISTS t_lwu_shared_intermediate;
DROP TABLE IF EXISTS t_lwu_shared_intermediate_pending_mutation;

-- A missing sort-key column is filled with its `DEFAULT` before any patch is applied and is evaluated
-- again after the patches. A `DEFAULT` column that reads it must be evaluated again from its new value,
-- both on read and when a merge materializes it.

CREATE TABLE t_lwu_sorting_key_dependent (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_sorting_key_dependent VALUES (0, 0), (1, 0);
SYSTEM STOP MERGES t_lwu_sorting_key_dependent;

ALTER TABLE t_lwu_sorting_key_dependent ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_sorting_key_dependent MODIFY COLUMN k UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_sorting_key_dependent ADD COLUMN z UInt32 DEFAULT k + 1;

UPDATE t_lwu_sorting_key_dependent SET a = 5 WHERE 1;

SELECT 'sorting key, read';
SELECT id, a, k, z FROM t_lwu_sorting_key_dependent ORDER BY id;
SELECT z FROM t_lwu_sorting_key_dependent ORDER BY id;

ALTER TABLE t_lwu_sorting_key_dependent MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_sorting_key_dependent;
OPTIMIZE TABLE t_lwu_sorting_key_dependent FINAL;

SELECT 'sorting key, merged';
SELECT id, a, k, z FROM t_lwu_sorting_key_dependent ORDER BY id SETTINGS apply_patch_parts = 0;

-- Two `DEFAULT` columns read the same missing intermediate with a non-deterministic `DEFAULT` that is not
-- requested. After a patch to the column the intermediate reads, they must still agree in each row.

CREATE TABLE t_lwu_shared_intermediate (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_shared_intermediate VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_shared_intermediate;

ALTER TABLE t_lwu_shared_intermediate ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_shared_intermediate ADD COLUMN b UInt64 DEFAULT a + rand64();
ALTER TABLE t_lwu_shared_intermediate ADD COLUMN z UInt64 DEFAULT b;
ALTER TABLE t_lwu_shared_intermediate ADD COLUMN y UInt64 DEFAULT b;

UPDATE t_lwu_shared_intermediate SET a = 5 WHERE 1;

SELECT 'shared intermediate';
SELECT id, z = y FROM t_lwu_shared_intermediate ORDER BY id;
SELECT id, b = z, z = y FROM t_lwu_shared_intermediate ORDER BY id;

-- The same, when the patch is applied at the boundary of the step that applies a pending mutation on the fly.

CREATE TABLE t_lwu_shared_intermediate_pending_mutation (x UInt32) ENGINE = MergeTree ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_shared_intermediate_pending_mutation SELECT number FROM numbers(4);

ALTER TABLE t_lwu_shared_intermediate_pending_mutation ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_shared_intermediate_pending_mutation ADD COLUMN b UInt64 DEFAULT a + rand64();
ALTER TABLE t_lwu_shared_intermediate_pending_mutation ADD COLUMN z UInt64 DEFAULT b, ADD COLUMN y UInt64 DEFAULT b;

SYSTEM STOP MERGES t_lwu_shared_intermediate_pending_mutation;

-- Stays pending.
ALTER TABLE t_lwu_shared_intermediate_pending_mutation UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
-- A later patch.
UPDATE t_lwu_shared_intermediate_pending_mutation SET a = x + 5 WHERE x % 2 = 0;

SELECT 'shared intermediate, pending mutation applied on the fly';
SELECT x, a, w, z = y FROM t_lwu_shared_intermediate_pending_mutation ORDER BY x SETTINGS apply_mutations_on_fly = 1;
SELECT z = y, w FROM t_lwu_shared_intermediate_pending_mutation ORDER BY x SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_sorting_key_dependent;
DROP TABLE t_lwu_shared_intermediate;
DROP TABLE t_lwu_shared_intermediate_pending_mutation;
