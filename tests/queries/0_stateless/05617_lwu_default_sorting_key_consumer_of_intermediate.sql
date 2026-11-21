-- `k` is a sort-key column and keeps the value its `DEFAULT` gave. `z` reads the patched `a` and is evaluated again.
-- Both read the missing `b DEFAULT rand64()`, so `z` must read the value of `b` that `k` was evaluated from.

SET allow_experimental_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_sort_key_consumer_v1;
DROP TABLE IF EXISTS t_lwu_sort_key_consumer_v2;
DROP TABLE IF EXISTS t_lwu_sort_key_consumer_mutation;

CREATE TABLE t_lwu_sort_key_consumer_v1 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_sort_key_consumer_v1 VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_sort_key_consumer_v1;

ALTER TABLE t_lwu_sort_key_consumer_v1 ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_sort_key_consumer_v1 ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_sort_key_consumer_v1 ADD COLUMN k UInt64, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_sort_key_consumer_v1 MODIFY COLUMN k UInt64 DEFAULT b;
ALTER TABLE t_lwu_sort_key_consumer_v1 ADD COLUMN z UInt64 DEFAULT a + b;

SELECT 'v1';
SELECT id, z = k + a FROM t_lwu_sort_key_consumer_v1 ORDER BY id;

UPDATE t_lwu_sort_key_consumer_v1 SET a = 5 WHERE 1;
SELECT id, a, z = k + a FROM t_lwu_sort_key_consumer_v1 ORDER BY id;
SELECT id, a, z = k + a, b = k FROM t_lwu_sort_key_consumer_v1 ORDER BY id;

UPDATE t_lwu_sort_key_consumer_v1 SET a = 7 WHERE id = 0;
SELECT id, a, z = k + a FROM t_lwu_sort_key_consumer_v1 ORDER BY id;
SELECT id, a, z = k + a, b = k FROM t_lwu_sort_key_consumer_v1 ORDER BY id;

CREATE TABLE t_lwu_sort_key_consumer_v2 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_sort_key_consumer_v2 VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_sort_key_consumer_v2;

ALTER TABLE t_lwu_sort_key_consumer_v2 ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_sort_key_consumer_v2 ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_sort_key_consumer_v2 ADD COLUMN k UInt64, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_sort_key_consumer_v2 MODIFY COLUMN k UInt64 DEFAULT b;
ALTER TABLE t_lwu_sort_key_consumer_v2 ADD COLUMN z UInt64 DEFAULT a + b;

-- A patch of format `v2` is applied by the sorting key, which takes a new value of `b` in every query, so only
-- a patch to all rows is applied. The sort-key columns are evaluated before the patches are applied there.
SELECT 'v2';
SELECT id, z = k + a FROM t_lwu_sort_key_consumer_v2 ORDER BY id;

UPDATE t_lwu_sort_key_consumer_v2 SET a = 5 WHERE 1;
SELECT id, a, z = k + a FROM t_lwu_sort_key_consumer_v2 ORDER BY id;
SELECT id, a, z = k + a, b = k FROM t_lwu_sort_key_consumer_v2 ORDER BY id;


-- The same when the patch is applied at the boundary of the step applying a pending mutation on the fly.

CREATE TABLE t_lwu_sort_key_consumer_mutation (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_sort_key_consumer_mutation VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_sort_key_consumer_mutation;

ALTER TABLE t_lwu_sort_key_consumer_mutation ADD COLUMN a UInt64 DEFAULT 5, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_sort_key_consumer_mutation ADD COLUMN b UInt64 DEFAULT rand64();
ALTER TABLE t_lwu_sort_key_consumer_mutation ADD COLUMN k UInt64, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_sort_key_consumer_mutation MODIFY COLUMN k UInt64 DEFAULT b;
ALTER TABLE t_lwu_sort_key_consumer_mutation ADD COLUMN z UInt64 DEFAULT a + b;

ALTER TABLE t_lwu_sort_key_consumer_mutation UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_sort_key_consumer_mutation SET a = 7 WHERE id = 0;

SELECT 'pending mutation';
SELECT id, a, w, z = k + a FROM t_lwu_sort_key_consumer_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, a, w, z = k + a, b = k FROM t_lwu_sort_key_consumer_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_sort_key_consumer_v1;
DROP TABLE t_lwu_sort_key_consumer_v2;
DROP TABLE t_lwu_sort_key_consumer_mutation;
