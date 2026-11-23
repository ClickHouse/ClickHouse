-- A part written by older versions with `enable_block_offset_column = 1` and `enable_block_number_column = 0` stores
-- `_block_offset` without `_block_number`. With `use_block_offset_only_with_block_number = 1` its stored `_block_offset`
-- reads as the row position. Patches written before the setting was enabled store the stored `_block_offset`, so they
-- are matched with it, and patches written after are matched with the row position, whatever the current value is.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET mutations_sync = 2;
SET alter_sync = 2;

DROP TABLE IF EXISTS t_lwu_before_v2;
DROP TABLE IF EXISTS t_lwu_before_v1;
DROP TABLE IF EXISTS t_lwu_before_ambiguous;
DROP TABLE IF EXISTS t_lwu_before_r1;
DROP TABLE IF EXISTS t_lwu_before_r2;

-- 1. A v2 patch written before the switch.

SELECT 'v2';

CREATE TABLE t_lwu_before_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

-- Rows `a`, `c` and `d` are in block 1, row `b` is in block 2. The merged part stores `_block_offset` = [0, 0, 1, 2] for `a`, `b`, `c`, `d`.
INSERT INTO t_lwu_before_v2 VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_before_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_v2 FINAL;

ALTER TABLE t_lwu_before_v2 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_before_v2 SET v = 'X' WHERE v = 'd';

-- `_block_offset` reads as [0, 1, 2, 3], so the patch would update `c` if it were matched with the row position.
ALTER TABLE t_lwu_before_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, _block_offset;

-- A patch written after the switch is matched with the row position.
UPDATE t_lwu_before_v2 SET v = 'Y' WHERE v = 'b';
SELECT uid, v FROM t_lwu_before_v2 ORDER BY uid, v;

-- Both patches keep updating their rows after they are loaded again, and when the setting is disabled.
DETACH TABLE t_lwu_before_v2;
ATTACH TABLE t_lwu_before_v2;
ALTER TABLE t_lwu_before_v2 MODIFY SETTING use_block_offset_only_with_block_number = 0;
SELECT uid, v FROM t_lwu_before_v2 ORDER BY uid, v;
ALTER TABLE t_lwu_before_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1;

-- The two patches are in different partitions, so they are never merged with each other.
SELECT count(DISTINCT partition_id) FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_before_v2' AND active AND startsWith(name, 'patch-');

-- A merge rewrites `_block_offset` of the part, so it applies the patches even with `apply_patches_on_merge = 0`.
OPTIMIZE TABLE t_lwu_before_v2 FINAL;
SELECT uid, v, _block_offset FROM t_lwu_before_v2 ORDER BY uid, v SETTINGS apply_patch_parts = 0;
SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_before_v2;

-- 2. A v1 patch applied in `Join` mode, written for a part of one block before an older version merged it
-- into a part that stores `_block_offset` without `_block_number`.

SELECT 'v1, Join';

CREATE TABLE t_lwu_before_v1 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v1', apply_patches_on_merge = 0;

-- Rows `a` and `c` are in block 1, row `b` is in block 2. The patch stores `(1, 1)` for `c`.
INSERT INTO t_lwu_before_v1 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_before_v1 VALUES (0, 'b');
UPDATE t_lwu_before_v1 SET v = 'X' WHERE v = 'c';

-- The merged part stores `_block_offset` = [0, 0, 1] for `a`, `b`, `c`, and the patch stays.
ALTER TABLE t_lwu_before_v1 MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_lwu_before_v1 FINAL;
ALTER TABLE t_lwu_before_v1 MODIFY SETTING enable_block_number_column = 1;
SELECT v FROM t_lwu_before_v1 ORDER BY uid, v;

-- `_block_offset` reads as [0, 1, 2], so the patch would update `b` if it were matched with the row position.
ALTER TABLE t_lwu_before_v1 MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT uid, v, _block_offset FROM t_lwu_before_v1 ORDER BY uid, _block_offset;

-- 3. A patch written before the switch for a row whose stored pair is not unique matches more than one row.

SELECT 'ambiguous';

CREATE TABLE t_lwu_before_ambiguous (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_before_ambiguous VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_before_ambiguous VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_ambiguous FINAL;

ALTER TABLE t_lwu_before_ambiguous MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_before_ambiguous SET v = 'X' WHERE v = 'a';

ALTER TABLE t_lwu_before_ambiguous MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT v FROM t_lwu_before_ambiguous ORDER BY v; -- { serverError INCORRECT_DATA }
SELECT v FROM t_lwu_before_ambiguous ORDER BY v SETTINGS apply_patch_parts = 0;

-- 4. The same as 1 for `ReplicatedMergeTree`: the other replica fetches the patches and matches them in the same way.

SELECT 'replicated';

CREATE TABLE t_lwu_before_r1 (uid Int16, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_before_replicated', 'r1') ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

CREATE TABLE t_lwu_before_r2 (uid Int16, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_before_replicated', 'r2') ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_before_r1 VALUES (0, 'a'), (1, 'c'), (1, 'd');
INSERT INTO t_lwu_before_r1 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_before_r1 FINAL;

ALTER TABLE t_lwu_before_r1 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_before_r1 SET v = 'X' WHERE v = 'd';

ALTER TABLE t_lwu_before_r1 MODIFY SETTING use_block_offset_only_with_block_number = 1;
UPDATE t_lwu_before_r1 SET v = 'Y' WHERE v = 'b';

SYSTEM SYNC REPLICA t_lwu_before_r2;
SELECT uid, v FROM t_lwu_before_r2 ORDER BY uid, v;

DROP TABLE t_lwu_before_v2;
DROP TABLE t_lwu_before_v1;
DROP TABLE t_lwu_before_ambiguous;
DROP TABLE t_lwu_before_r1;
DROP TABLE t_lwu_before_r2;
