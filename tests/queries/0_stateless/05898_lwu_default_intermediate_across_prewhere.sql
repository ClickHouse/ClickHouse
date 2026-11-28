-- The unrequested missing column `b` is read by `z`, evaluated by the `PREWHERE` reader, and by `y`, evaluated by the next one.
-- The lightweight `UPDATE` patches `a`, which the non-deterministic `DEFAULT` of `b` reads, so `z` is evaluated again
-- from a new value of `b` after the patch is applied. `y` must read that value of `b`, not yet another one:
-- the two columns agree in a row whether or not they are read by one reader.

SET allow_experimental_lightweight_update = 1;
SET enable_multiple_prewhere_read_steps = 0;

DROP TABLE IF EXISTS t_lwu_default_prewhere_v1;

CREATE TABLE t_lwu_default_prewhere_v1 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_prewhere_v1 VALUES (0), (1), (2), (3);
SYSTEM STOP MERGES t_lwu_default_prewhere_v1;

ALTER TABLE t_lwu_default_prewhere_v1 ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_default_prewhere_v1 ADD COLUMN b UInt64 DEFAULT a + rand64();
ALTER TABLE t_lwu_default_prewhere_v1 ADD COLUMN z UInt64 DEFAULT b;
ALTER TABLE t_lwu_default_prewhere_v1 ADD COLUMN y UInt64 DEFAULT b;

UPDATE t_lwu_default_prewhere_v1 SET a = 6 WHERE 1;

SELECT 'v1, both columns in one reader';
SELECT id, z = y FROM t_lwu_default_prewhere_v1 ORDER BY id;
SELECT 'v1, columns on the opposite sides of PREWHERE';
SELECT id, z = y FROM t_lwu_default_prewhere_v1 PREWHERE z != 0 ORDER BY id;
SELECT id, y = z FROM t_lwu_default_prewhere_v1 PREWHERE y != 0 ORDER BY id;
SELECT 'v1, the intermediate is requested after PREWHERE';
SELECT id, z = y, b = z FROM t_lwu_default_prewhere_v1 PREWHERE z != 0 ORDER BY id;

-- A patch overwrites `z` in some rows: `z` is evaluated again, and `b` with it, only in the other rows.
-- `y` reads the same `b` in those rows, and `b` is evaluated for it in the rows `z` did not need.
UPDATE t_lwu_default_prewhere_v1 SET z = 100 WHERE id < 2;

SELECT 'v1, partially patched column before PREWHERE';
SELECT id, z = 100, z = y FROM t_lwu_default_prewhere_v1 PREWHERE z != 0 ORDER BY id;
SELECT id, z = 100, z = y, b = y FROM t_lwu_default_prewhere_v1 PREWHERE z != 0 ORDER BY id;
SELECT 'v1, partially patched column after PREWHERE';
SELECT id, z = 100, z = y FROM t_lwu_default_prewhere_v1 PREWHERE y != 0 ORDER BY id;

DROP TABLE t_lwu_default_prewhere_v1;

DROP TABLE IF EXISTS t_lwu_default_prewhere_v2;

CREATE TABLE t_lwu_default_prewhere_v2 (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_prewhere_v2 VALUES (0), (1), (2), (3);
SYSTEM STOP MERGES t_lwu_default_prewhere_v2;

ALTER TABLE t_lwu_default_prewhere_v2 ADD COLUMN a UInt64 DEFAULT 5;
ALTER TABLE t_lwu_default_prewhere_v2 ADD COLUMN b UInt64 DEFAULT a + rand64();
ALTER TABLE t_lwu_default_prewhere_v2 ADD COLUMN z UInt64 DEFAULT b;
ALTER TABLE t_lwu_default_prewhere_v2 ADD COLUMN y UInt64 DEFAULT b;

UPDATE t_lwu_default_prewhere_v2 SET a = 6 WHERE 1;

SELECT 'v2, both columns in one reader';
SELECT id, z = y FROM t_lwu_default_prewhere_v2 ORDER BY id;
SELECT 'v2, columns on the opposite sides of PREWHERE';
SELECT id, z = y FROM t_lwu_default_prewhere_v2 PREWHERE z != 0 ORDER BY id;
SELECT id, y = z FROM t_lwu_default_prewhere_v2 PREWHERE y != 0 ORDER BY id;
SELECT 'v2, the intermediate is requested after PREWHERE';
SELECT id, z = y, b = z FROM t_lwu_default_prewhere_v2 PREWHERE z != 0 ORDER BY id;

-- A patch overwrites `z` in some rows: `z` is evaluated again, and `b` with it, only in the other rows.
-- `y` reads the same `b` in those rows, and `b` is evaluated for it in the rows `z` did not need.
UPDATE t_lwu_default_prewhere_v2 SET z = 100 WHERE id < 2;

SELECT 'v2, partially patched column before PREWHERE';
SELECT id, z = 100, z = y FROM t_lwu_default_prewhere_v2 PREWHERE z != 0 ORDER BY id;
SELECT id, z = 100, z = y, b = y FROM t_lwu_default_prewhere_v2 PREWHERE z != 0 ORDER BY id;
SELECT 'v2, partially patched column after PREWHERE';
SELECT id, z = 100, z = y FROM t_lwu_default_prewhere_v2 PREWHERE y != 0 ORDER BY id;

DROP TABLE t_lwu_default_prewhere_v2;
