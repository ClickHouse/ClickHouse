-- A `DEFAULT` column evaluated again after a lightweight `UPDATE` of the column it reads gives its subcolumns
-- from the final value, also when the subcolumn is requested before the full column.

DROP TABLE IF EXISTS t_lwu_default_subcolumn;

SELECT 'v1';

CREATE TABLE t_lwu_default_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_default_subcolumn;

ALTER TABLE t_lwu_default_subcolumn ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_subcolumn ADD COLUMN z Tuple(x UInt32, y UInt32) DEFAULT tuple(a + 100, a + 200);

UPDATE t_lwu_default_subcolumn SET a = 5 WHERE 1;

SELECT id, z.x, z FROM t_lwu_default_subcolumn ORDER BY id;
SELECT id, z, z.x FROM t_lwu_default_subcolumn ORDER BY id;
SELECT id, z.y FROM t_lwu_default_subcolumn ORDER BY id;

DROP TABLE t_lwu_default_subcolumn;

SELECT 'v2';

CREATE TABLE t_lwu_default_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_default_subcolumn;

ALTER TABLE t_lwu_default_subcolumn ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_default_subcolumn ADD COLUMN z Tuple(x UInt32, y UInt32) DEFAULT tuple(a + 100, a + 200);

UPDATE t_lwu_default_subcolumn SET a = 5 WHERE 1;

SELECT id, z.x, z FROM t_lwu_default_subcolumn ORDER BY id;
SELECT id, z, z.x FROM t_lwu_default_subcolumn ORDER BY id;
SELECT id, z.y FROM t_lwu_default_subcolumn ORDER BY id;

DROP TABLE t_lwu_default_subcolumn;
