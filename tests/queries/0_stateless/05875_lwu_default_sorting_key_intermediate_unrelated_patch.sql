DROP TABLE IF EXISTS t_lwu_default_key_unrelated;

-- The sort-key column `k` is not stored in the part, and its `DEFAULT` reads the missing non-deterministic `b`, as `z` does.
-- The lightweight `UPDATE` patches `a`, which no `DEFAULT` reads, so no `DEFAULT` column is evaluated again after the patch.
-- Still, `k` is evaluated early, to match the rows with the patch, and `z` later: both must read the same value of `b`,
-- whether or not the patched column is requested, and the merge that applies the patch must store equal values.

SET allow_experimental_lightweight_update = 1;

CREATE TABLE t_lwu_default_key_unrelated (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_key_unrelated VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_default_key_unrelated;

ALTER TABLE t_lwu_default_key_unrelated ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_default_key_unrelated ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_default_key_unrelated ADD COLUMN k UInt64, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key_unrelated MODIFY COLUMN k UInt64 DEFAULT b;
ALTER TABLE t_lwu_default_key_unrelated ADD COLUMN z UInt64 DEFAULT b;

UPDATE t_lwu_default_key_unrelated SET a = 5 WHERE 1;

SELECT id, z = k FROM t_lwu_default_key_unrelated ORDER BY id;
SELECT id, a, z = k FROM t_lwu_default_key_unrelated ORDER BY id;
SELECT id, a, z = k, b = k FROM t_lwu_default_key_unrelated ORDER BY id;
SELECT id, a, z = k FROM t_lwu_default_key_unrelated PREWHERE a > 0 ORDER BY id;

ALTER TABLE t_lwu_default_key_unrelated MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_default_key_unrelated;
OPTIMIZE TABLE t_lwu_default_key_unrelated FINAL;

SELECT id, a, z = k, b = k FROM t_lwu_default_key_unrelated ORDER BY id SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_default_key_unrelated;
