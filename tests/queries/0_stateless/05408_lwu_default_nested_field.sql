DROP TABLE IF EXISTS t_lwu_default_nested;

-- A flattened `Nested` field whose `DEFAULT` reads a column a lightweight `UPDATE` patched is evaluated
-- again under its own name, also in a wide part, which reads it as a subcolumn of the `Nested` column.

CREATE TABLE t_lwu_default_nested (id UInt32, n Nested(x UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_lwu_default_nested VALUES (0, [1, 2]), (1, [3, 4]);

SYSTEM STOP MERGES t_lwu_default_nested;

ALTER TABLE t_lwu_default_nested ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_default_nested ADD COLUMN n.z Array(UInt32) DEFAULT arrayMap(x -> x + a, n.x);

UPDATE t_lwu_default_nested SET a = 5 WHERE 1;

SELECT 'patched';
SELECT id, a, n.x, n.z FROM t_lwu_default_nested ORDER BY id;
SELECT id, n.z FROM t_lwu_default_nested ORDER BY id;

-- Stays pending, so a later patch is applied at the boundary of the step that applies it on the fly.
ALTER TABLE t_lwu_default_nested UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_default_nested SET a = 10 WHERE id = 1;

SELECT 'pending mutation applied on the fly';
SELECT id, a, w, n.x, n.z FROM t_lwu_default_nested ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'pending mutation not applied';
SELECT id, a, w, n.x, n.z FROM t_lwu_default_nested ORDER BY id SETTINGS apply_mutations_on_fly = 0;

DROP TABLE t_lwu_default_nested;
