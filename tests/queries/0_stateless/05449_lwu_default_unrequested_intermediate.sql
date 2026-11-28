DROP TABLE IF EXISTS t_lwu_intermediate;
DROP TABLE IF EXISTS t_lwu_intermediate_mutation;

-- `b` is not requested, but `z` reads it, so `b` is evaluated as an intermediate when `z` is evaluated
-- again after the patches. In row 0, `z` keeps its patched value, so `b` must not be evaluated there:
-- `intDiv(100, a)` with `a` patched to 0 would throw. The result must not depend on the block size.

CREATE TABLE t_lwu_intermediate (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, index_granularity = 1;

INSERT INTO t_lwu_intermediate VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_intermediate;

ALTER TABLE t_lwu_intermediate ADD COLUMN a UInt32 DEFAULT 1;
ALTER TABLE t_lwu_intermediate ADD COLUMN b UInt32 DEFAULT intDiv(100, a);
ALTER TABLE t_lwu_intermediate ADD COLUMN z UInt32 DEFAULT b;

UPDATE t_lwu_intermediate SET z = 999 WHERE id = 0;
UPDATE t_lwu_intermediate SET a = 0 WHERE id = 0;

SELECT 'patches, one row per block';
SELECT id, a, z FROM t_lwu_intermediate ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;
SELECT 'patches, one block';
SELECT id, a, z FROM t_lwu_intermediate ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;

-- The same when the patches are applied at the boundary of the step applying a pending mutation on the fly.

CREATE TABLE t_lwu_intermediate_mutation (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, index_granularity = 1;

INSERT INTO t_lwu_intermediate_mutation VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_intermediate_mutation;

ALTER TABLE t_lwu_intermediate_mutation ADD COLUMN a UInt32 DEFAULT 1, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_intermediate_mutation ADD COLUMN b UInt32 DEFAULT intDiv(100, a);
ALTER TABLE t_lwu_intermediate_mutation ADD COLUMN z UInt32 DEFAULT b;

ALTER TABLE t_lwu_intermediate_mutation UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_intermediate_mutation SET z = 999 WHERE id = 0;
UPDATE t_lwu_intermediate_mutation SET a = 0 WHERE id = 0;

SELECT 'pending mutation, one row per block';
SELECT id, a, z, w FROM t_lwu_intermediate_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1, max_threads = 1, max_block_size = 1;
SELECT 'pending mutation, one block';
SELECT id, a, z, w FROM t_lwu_intermediate_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1, max_threads = 1, max_block_size = 100;

DROP TABLE t_lwu_intermediate;
DROP TABLE t_lwu_intermediate_mutation;
