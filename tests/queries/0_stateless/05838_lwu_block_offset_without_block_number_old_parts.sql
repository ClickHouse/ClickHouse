-- Parts written by older versions with `enable_block_offset_column = 1` and `enable_block_number_column = 0`
-- store `_block_offset` without `_block_number`. `_block_number` of such a part reads as the constant `min_block`,
-- so a part merged from several insert blocks has duplicate `(_block_number, _block_offset)` pairs.
-- `use_block_offset_only_with_block_number = 0` keeps the old behavior, so it is used to write such parts.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET mutations_sync = 2;
SET alter_update_mode = 'heavy';

DROP TABLE IF EXISTS t_lwu_old_v1;
DROP TABLE IF EXISTS t_lwu_old_v2;
DROP TABLE IF EXISTS t_lwu_old_mutation;
DROP TABLE IF EXISTS t_lwu_old_one_block;
DROP TABLE IF EXISTS t_lwu_old_dup_v1;
DROP TABLE IF EXISTS t_lwu_old_dup_v2;
DROP TABLE IF EXISTS t_lwu_old_delete;
DROP TABLE IF EXISTS t_lwu_old_apply_on_merge;
DROP TABLE IF EXISTS t_lwu_old_replicated;

-- 1. The stored `_block_offset` of a part of several blocks is ignored and reads as the row position,
-- so the pairs are unique, and a lightweight update changes only the matched rows.

SELECT 'old part, v1';

CREATE TABLE t_lwu_old_v1 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v1', apply_patches_on_merge = 0;

-- Rows `a` and `c` are in block 1, row `b` is in block 2. The merged part stores `_block_offset` = [0, 0, 1].
INSERT INTO t_lwu_old_v1 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_v1 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_v1 FINAL;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_old_v1' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT _block_number, _block_offset, v FROM t_lwu_old_v1 ORDER BY v;

ALTER TABLE t_lwu_old_v1 MODIFY SETTING use_block_offset_only_with_block_number = 1;
SELECT _block_number, _block_offset, v FROM t_lwu_old_v1 ORDER BY v;

ALTER TABLE t_lwu_old_v1 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_old_v1 SET v = 'X' WHERE v = 'a';

-- The patch is applied in `Merge` mode.
SELECT v FROM t_lwu_old_v1 ORDER BY v;

-- The part is rewritten with both columns, the patch stays and is applied in `Join` mode.
OPTIMIZE TABLE t_lwu_old_v1 FINAL;
SELECT v FROM t_lwu_old_v1 ORDER BY v;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_old_v1' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_old_v1;

SELECT 'old part, v2';

CREATE TABLE t_lwu_old_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_v2 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_v2 FINAL;

ALTER TABLE t_lwu_old_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1, enable_block_number_column = 1;
SELECT _block_number, _block_offset, v FROM t_lwu_old_v2 ORDER BY v;

UPDATE t_lwu_old_v2 SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_old_v2 ORDER BY v;

OPTIMIZE TABLE t_lwu_old_v2 FINAL;
SELECT v FROM t_lwu_old_v2 ORDER BY v;
SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_old_v2;

-- 2. A mutation that materializes `_block_number` rewrites `_block_offset` too, even with `enable_block_offset_column = 0`,
-- otherwise the ignored stored offsets would become valid again.

SELECT 'mutation';

CREATE TABLE t_lwu_old_mutation (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_mutation VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_mutation VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_mutation FINAL;

ALTER TABLE t_lwu_old_mutation MODIFY SETTING use_block_offset_only_with_block_number = 1, enable_block_number_column = 1, enable_block_offset_column = 0;
ALTER TABLE t_lwu_old_mutation UPDATE v = concat(v, '') WHERE 1;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_old_mutation' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT _block_number, _block_offset, v FROM t_lwu_old_mutation ORDER BY v;

ALTER TABLE t_lwu_old_mutation MODIFY SETTING enable_block_offset_column = 1;
UPDATE t_lwu_old_mutation SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_old_mutation ORDER BY v;

-- 3. A part of one block keeps its stored `_block_offset`: the stored offsets are unique, and after a mutation
-- that drops rows they differ from the row positions.

SELECT 'one block';

CREATE TABLE t_lwu_old_one_block (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0;

INSERT INTO t_lwu_old_one_block VALUES (0, 'a'), (1, 'b'), (2, 'c');
ALTER TABLE t_lwu_old_one_block DELETE WHERE uid = 0;

ALTER TABLE t_lwu_old_one_block MODIFY SETTING use_block_offset_only_with_block_number = 1;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_old_one_block' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT _block_number, _block_offset, v FROM t_lwu_old_one_block ORDER BY v;

-- 4. A part that stores both columns with duplicate pairs can't be detected by its columns.
-- A patch that matches more than one row of it throws instead of changing all of them.

SELECT 'both columns stored with duplicate pairs, v1';

CREATE TABLE t_lwu_old_dup_v1 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v1', apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_dup_v1 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_dup_v1 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_dup_v1 FINAL;

-- An older version stores the constant `_block_number` and the duplicate offsets on the next merge.
ALTER TABLE t_lwu_old_dup_v1 MODIFY SETTING enable_block_number_column = 1;
OPTIMIZE TABLE t_lwu_old_dup_v1 FINAL;
ALTER TABLE t_lwu_old_dup_v1 MODIFY SETTING use_block_offset_only_with_block_number = 1;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_old_dup_v1' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT _block_number, _block_offset, v FROM t_lwu_old_dup_v1 ORDER BY v;

-- The row `c` has a unique pair.
UPDATE t_lwu_old_dup_v1 SET v = 'Y' WHERE v = 'c';
SELECT v FROM t_lwu_old_dup_v1 ORDER BY v;

-- The patch for the row `a` is applied in `Merge` mode and is exact until the part is rewritten. Then `Join` mode throws.
UPDATE t_lwu_old_dup_v1 SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_old_dup_v1 ORDER BY v;
OPTIMIZE TABLE t_lwu_old_dup_v1 FINAL;
SELECT v FROM t_lwu_old_dup_v1 ORDER BY v; -- { serverError INCORRECT_DATA }

-- The data can still be read without patches.
SELECT v FROM t_lwu_old_dup_v1 ORDER BY v SETTINGS apply_patch_parts = 0;

SELECT 'both columns stored with duplicate pairs, v2';

CREATE TABLE t_lwu_old_dup_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_dup_v2 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_dup_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_dup_v2 FINAL;

ALTER TABLE t_lwu_old_dup_v2 MODIFY SETTING enable_block_number_column = 1;
OPTIMIZE TABLE t_lwu_old_dup_v2 FINAL;
ALTER TABLE t_lwu_old_dup_v2 MODIFY SETTING use_block_offset_only_with_block_number = 1;

UPDATE t_lwu_old_dup_v2 SET v = 'Y' WHERE v = 'c';
SELECT v FROM t_lwu_old_dup_v2 ORDER BY v;

-- The rows `a` and `b` have the same sorting key and the same pair.
UPDATE t_lwu_old_dup_v2 SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_old_dup_v2 ORDER BY v; -- { serverError INCORRECT_DATA }
SELECT v FROM t_lwu_old_dup_v2 ORDER BY v SETTINGS apply_patch_parts = 0;

-- 5. A lightweight delete that writes patch parts deletes only the matched rows.

SELECT 'lightweight delete';

CREATE TABLE t_lwu_old_delete (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_delete VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_delete VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_delete FINAL;

ALTER TABLE t_lwu_old_delete MODIFY SETTING use_block_offset_only_with_block_number = 1, enable_block_number_column = 1;
DELETE FROM t_lwu_old_delete WHERE v = 'b' SETTINGS lightweight_delete_mode = 'lightweight_update_force';
SELECT v FROM t_lwu_old_delete ORDER BY v;

OPTIMIZE TABLE t_lwu_old_delete FINAL;
SELECT v FROM t_lwu_old_delete ORDER BY v;

-- 6. A merge that applies the patch changes only the matched rows.

SELECT 'apply patches on merge';

CREATE TABLE t_lwu_old_apply_on_merge (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    apply_patches_on_merge = 1;

INSERT INTO t_lwu_old_apply_on_merge VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_apply_on_merge VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_apply_on_merge FINAL;

ALTER TABLE t_lwu_old_apply_on_merge MODIFY SETTING use_block_offset_only_with_block_number = 1, enable_block_number_column = 1;
UPDATE t_lwu_old_apply_on_merge SET v = 'X' WHERE v = 'a';
OPTIMIZE TABLE t_lwu_old_apply_on_merge FINAL;
SELECT v FROM t_lwu_old_apply_on_merge ORDER BY v SETTINGS apply_patch_parts = 0;

-- 7. The same for `ReplicatedMergeTree`.

SELECT 'replicated';

CREATE TABLE t_lwu_old_replicated (uid Int16, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_old_replicated', 'r1') ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    apply_patches_on_merge = 0;

INSERT INTO t_lwu_old_replicated VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_old_replicated VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_old_replicated FINAL;

ALTER TABLE t_lwu_old_replicated MODIFY SETTING use_block_offset_only_with_block_number = 1, enable_block_number_column = 1;
-- The first block number of `ReplicatedMergeTree` depends on the environment, so it is not printed.
SELECT _block_offset, v FROM t_lwu_old_replicated ORDER BY v;
SELECT uniqExact(_block_number) FROM t_lwu_old_replicated;

UPDATE t_lwu_old_replicated SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_old_replicated ORDER BY v;

OPTIMIZE TABLE t_lwu_old_replicated FINAL;
SELECT v FROM t_lwu_old_replicated ORDER BY v;
SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_old_replicated;

DROP TABLE t_lwu_old_v1;
DROP TABLE t_lwu_old_v2;
DROP TABLE t_lwu_old_mutation;
DROP TABLE t_lwu_old_one_block;
DROP TABLE t_lwu_old_dup_v1;
DROP TABLE t_lwu_old_dup_v2;
DROP TABLE t_lwu_old_delete;
DROP TABLE t_lwu_old_apply_on_merge;
DROP TABLE t_lwu_old_replicated;
