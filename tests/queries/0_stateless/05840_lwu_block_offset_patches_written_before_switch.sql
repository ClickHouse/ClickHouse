-- A part written by older versions with `enable_block_offset_column = 1` and `enable_block_number_column = 0` stores
-- `_block_offset` without `_block_number`. Its `_block_offset` reads as the stored value with
-- `use_block_offset_only_with_block_number = 0`, and as the row position with `use_block_offset_only_with_block_number = 1`.
-- A patch is applied to such a part only if it was written with the same value of the setting as the part is read with.
-- Otherwise it would change other rows, so it is rejected. Patches of other parts are not affected.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET mutations_sync = 2;
SET alter_sync = 2;

DROP TABLE IF EXISTS t_lwu_before_v2;
DROP TABLE IF EXISTS t_lwu_after_v2;
DROP TABLE IF EXISTS t_lwu_before_v1_merge;
DROP TABLE IF EXISTS t_lwu_before_v1_join;
DROP TABLE IF EXISTS t_lwu_before_both_columns;
DROP TABLE IF EXISTS t_lwu_after_r1;
DROP TABLE IF EXISTS t_lwu_after_r2;

-- 1. A v2 patch written before the switch.

SELECT 'v2, written before';

CREATE TABLE t_lwu_before_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

-- Rows `a`, `c` and `d` are in block 1, row `b` is in block 2. The merged part stores `_block_offset` = [0, 0, 1, 2] for `a`, `b`, `c`, `d`.
INSERT INTO t_lwu_before_v2 VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_before_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_v2 FINAL;

ALTER TABLE t_lwu_before_v2 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_before_v2 SET v = 'X' WHERE v = 'd';
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, _block_offset, v;

-- `_block_offset` reads as [0, 1, 2, 3], so the patch would update `c`.
ALTER TABLE t_lwu_before_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, _block_offset, v; -- { serverError INCORRECT_DATA }
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, _block_offset, v SETTINGS apply_patch_parts = 0;

-- A lightweight update reads the part with its patches. A merge writes the row positions to the new part,
-- so it applies the patches even with `apply_patches_on_merge = 0`.
UPDATE t_lwu_before_v2 SET v = 'Y' WHERE v = 'b'; -- { serverError INCORRECT_DATA }
OPTIMIZE TABLE t_lwu_before_v2 FINAL; -- { serverError INCORRECT_DATA }

-- The patch is applied again if the setting is disabled.
DETACH TABLE t_lwu_before_v2;
ATTACH TABLE t_lwu_before_v2;
ALTER TABLE t_lwu_before_v2 MODIFY SETTING use_block_offset_only_with_block_number = 0;
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, _block_offset, v;

-- 2. A v2 patch written after the switch.

SELECT 'v2, written after';

CREATE TABLE t_lwu_after_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_after_v2 VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_after_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_after_v2 FINAL;

ALTER TABLE t_lwu_after_v2 MODIFY SETTING enable_block_number_column = 1, use_block_offset_only_with_block_number = 1;
UPDATE t_lwu_after_v2 SET v = 'X' WHERE v = 'd';
SELECT uid, v, _block_offset FROM t_lwu_after_v2 ORDER BY uid, _block_offset, v;

-- The patch stores the row positions, which is kept when it is loaded again.
DETACH TABLE t_lwu_after_v2;
ATTACH TABLE t_lwu_after_v2;
SELECT uid, v, _block_offset FROM t_lwu_after_v2 ORDER BY uid, _block_offset, v;

-- The stored `_block_offset` of `c` is 1 and of `d` is 2, so the patch would update nothing.
ALTER TABLE t_lwu_after_v2 MODIFY SETTING use_block_offset_only_with_block_number = 0;
SELECT uid, v, _block_offset FROM t_lwu_after_v2 ORDER BY uid, _block_offset, v; -- { serverError INCORRECT_DATA }
ALTER TABLE t_lwu_after_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1;

-- The merge applies the patch even with `apply_patches_on_merge = 0`.
OPTIMIZE TABLE t_lwu_after_v2 FINAL;
SELECT uid, v, _block_offset FROM t_lwu_after_v2 ORDER BY uid, _block_offset, v SETTINGS apply_patch_parts = 0;
SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_after_v2;

-- 3. A v1 patch applied to its source part is matched by `_part_offset` (`Merge` mode), whenever it was written.

SELECT 'v1, Merge';

CREATE TABLE t_lwu_before_v1_merge (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v1', apply_patches_on_merge = 0;

INSERT INTO t_lwu_before_v1_merge VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_before_v1_merge VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_v1_merge FINAL;

ALTER TABLE t_lwu_before_v1_merge MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_before_v1_merge SET v = 'X' WHERE v = 'd';

ALTER TABLE t_lwu_before_v1_merge MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v1_merge ORDER BY uid, _block_offset, v;

OPTIMIZE TABLE t_lwu_before_v1_merge FINAL;
SELECT uid, v, _block_offset FROM t_lwu_before_v1_merge ORDER BY uid, _block_offset, v SETTINGS apply_patch_parts = 0;

-- 4. A v1 patch applied in `Join` mode, written for a part of one block before an older version merged it
-- into a part that stores `_block_offset` without `_block_number`.

SELECT 'v1, Join';

CREATE TABLE t_lwu_before_v1_join (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v1', apply_patches_on_merge = 0;

-- Rows `a` and `c` are in block 1, row `b` is in block 2. The patch stores `(1, 1)` for `c`.
INSERT INTO t_lwu_before_v1_join VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_before_v1_join VALUES (0, 'b');
UPDATE t_lwu_before_v1_join SET v = 'X' WHERE v = 'c';

-- The merged part stores `_block_offset` = [0, 0, 1] for `a`, `b`, `c`, and the patch stays.
ALTER TABLE t_lwu_before_v1_join MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_lwu_before_v1_join FINAL;
ALTER TABLE t_lwu_before_v1_join MODIFY SETTING enable_block_number_column = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v1_join ORDER BY uid, _block_offset, v;

-- `_block_offset` reads as [0, 1, 2], so the patch would update `b`.
ALTER TABLE t_lwu_before_v1_join MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v1_join ORDER BY uid, _block_offset, v; -- { serverError INCORRECT_DATA }

-- 5. A patch written before the switch for a part that stores both columns is applied as before.

SELECT 'both columns';

CREATE TABLE t_lwu_before_both_columns (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_before_both_columns VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_before_both_columns VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_both_columns FINAL;
UPDATE t_lwu_before_both_columns SET v = 'X' WHERE v = 'd';

ALTER TABLE t_lwu_before_both_columns MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_number, _block_offset FROM t_lwu_before_both_columns ORDER BY uid, _block_number, _block_offset;

-- 6. Another replica fetches a patch written after the switch as it is.

SELECT 'replicated';

CREATE TABLE t_lwu_after_r1 (uid Int16, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_after_replicated', 'r1') ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

CREATE TABLE t_lwu_after_r2 (uid Int16, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_after_replicated', 'r2') ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_after_r1 VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_after_r1 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_after_r1 FINAL;

-- The settings are changed on each replica separately.
ALTER TABLE t_lwu_after_r1 MODIFY SETTING enable_block_number_column = 1, use_block_offset_only_with_block_number = 1;
ALTER TABLE t_lwu_after_r2 MODIFY SETTING enable_block_number_column = 1, use_block_offset_only_with_block_number = 1;
UPDATE t_lwu_after_r1 SET v = 'X' WHERE v = 'd';

SYSTEM SYNC REPLICA t_lwu_after_r2;
SELECT uid, v FROM t_lwu_after_r2 ORDER BY uid, v;

ALTER TABLE t_lwu_after_r2 MODIFY SETTING use_block_offset_only_with_block_number = 0;
SELECT uid, v FROM t_lwu_after_r2 ORDER BY uid, v; -- { serverError INCORRECT_DATA }

DROP TABLE t_lwu_before_v2;
DROP TABLE t_lwu_after_v2;
DROP TABLE t_lwu_before_v1_merge;
DROP TABLE t_lwu_before_v1_join;
DROP TABLE t_lwu_before_both_columns;
DROP TABLE t_lwu_after_r1;
DROP TABLE t_lwu_after_r2;
