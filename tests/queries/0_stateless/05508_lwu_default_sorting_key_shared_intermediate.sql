DROP TABLE IF EXISTS t_lwu_default_key_shared;

-- The sort-key column `k` is not stored in the part, and its `DEFAULT` reads the missing `b`, which reads the missing `a`.
-- `z` reads both `a` and `b`, so after a lightweight `UPDATE` of `a` it is evaluated again, and with it `b`.
-- Another requested column reading `b` is evaluated again too, but not the sort-key column `k`: the rows of the part are
-- sorted by the value its `DEFAULT` gave, so it keeps it whether `z` is requested or not, and ordered reads keep the key order.

CREATE TABLE t_lwu_default_key_shared (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_key_shared VALUES (0, 0), (0, 1);
SYSTEM STOP MERGES t_lwu_default_key_shared;

ALTER TABLE t_lwu_default_key_shared ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_key_shared ADD COLUMN b UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_default_key_shared ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key_shared MODIFY COLUMN k UInt32 DEFAULT b;
ALTER TABLE t_lwu_default_key_shared ADD COLUMN z UInt32 DEFAULT a + b;

UPDATE t_lwu_default_key_shared SET a = 10 WHERE x = 0;

SELECT id, x, a, k, z FROM t_lwu_default_key_shared ORDER BY id, k, x SETTINGS optimize_read_in_order = 1;
SELECT id, x, a, k, z FROM t_lwu_default_key_shared ORDER BY id, k, x SETTINGS optimize_read_in_order = 0;
SELECT id, x, a, k FROM t_lwu_default_key_shared ORDER BY id, k, x;
SELECT id, x, a, b, k, z FROM t_lwu_default_key_shared ORDER BY id, k, x;
SELECT x FROM t_lwu_default_key_shared ORDER BY id, k LIMIT 1 SETTINGS optimize_read_in_order = 1;

DROP TABLE t_lwu_default_key_shared;
