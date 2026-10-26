DROP TABLE IF EXISTS t_lwu_shared_parent_v1;
DROP TABLE IF EXISTS t_lwu_shared_parent_v2;
DROP TABLE IF EXISTS t_lwu_shared_parent_mutation;

-- Only the subcolumns `b.x` and `b.y` are requested. They are taken from one value of their parent `b`,
-- which is evaluated once when they are evaluated again after the patch to `a`, so they still agree in a row.

CREATE TABLE t_lwu_shared_parent_v1 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v1';

CREATE TABLE t_lwu_shared_parent_v2 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_shared_parent_v1 VALUES (0), (1);
INSERT INTO t_lwu_shared_parent_v2 VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_shared_parent_v1;
SYSTEM STOP MERGES t_lwu_shared_parent_v2;

ALTER TABLE t_lwu_shared_parent_v1 ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_shared_parent_v1 ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(a + rand64(), a + rand64());
ALTER TABLE t_lwu_shared_parent_v2 ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_shared_parent_v2 ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(a + rand64(), a + rand64());

UPDATE t_lwu_shared_parent_v1 SET a = 5 WHERE 1;
UPDATE t_lwu_shared_parent_v2 SET a = 5 WHERE 1;

SELECT 'v1';
SELECT id, a, b.x = b.y FROM t_lwu_shared_parent_v1 ORDER BY id;
SELECT 'v2';
SELECT id, a, b.x = b.y FROM t_lwu_shared_parent_v2 ORDER BY id;

-- The same when the patch is applied at the boundary of the step applying a pending mutation on the fly.

CREATE TABLE t_lwu_shared_parent_mutation (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_shared_parent_mutation VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_shared_parent_mutation;

ALTER TABLE t_lwu_shared_parent_mutation ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_shared_parent_mutation ADD COLUMN b Tuple(x UInt64, y UInt64) DEFAULT tuple(a + rand64(), a + rand64());

ALTER TABLE t_lwu_shared_parent_mutation UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_shared_parent_mutation SET a = 5 WHERE 1;

SELECT 'pending mutation';
SELECT id, a, w, b.x = b.y FROM t_lwu_shared_parent_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_shared_parent_v1;
DROP TABLE t_lwu_shared_parent_v2;
DROP TABLE t_lwu_shared_parent_mutation;
