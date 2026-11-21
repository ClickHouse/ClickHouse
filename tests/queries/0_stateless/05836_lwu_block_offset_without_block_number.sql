-- With `enable_block_offset_column = 1` and `enable_block_number_column = 0`, merges must store `_block_number`
-- together with `_block_offset`. Before the fix, merges stored `_block_offset` without `_block_number`.
-- `_block_number` then read as the constant `min_block`, so a part merged from several insert blocks had
-- duplicate `(_block_number, _block_offset)` pairs. After `enable_block_number_column` was enabled,
-- a lightweight update changed every row with the same pair, not only the rows it matched.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_lwu_offset_v1;
DROP TABLE IF EXISTS t_lwu_offset_v2;
DROP TABLE IF EXISTS t_lwu_offset_both;

SELECT 'v1';

CREATE TABLE t_lwu_offset_v1 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, patch_parts_version = 'v1', apply_patches_on_merge = 0;

INSERT INTO t_lwu_offset_v1 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_offset_v1 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_offset_v1 FINAL;

-- The merged part stores both columns.
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_offset_v1' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_offset_v1;

ALTER TABLE t_lwu_offset_v1 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_offset_v1 SET v = 'X' WHERE v = 'a';

-- The patch is applied in `Merge` mode.
SELECT v FROM t_lwu_offset_v1 ORDER BY v;

-- The part is rewritten, the patch stays and is applied in `Join` mode.
OPTIMIZE TABLE t_lwu_offset_v1 FINAL;
SELECT v FROM t_lwu_offset_v1 ORDER BY v;

SELECT 'v2';

CREATE TABLE t_lwu_offset_v2 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, patch_parts_version = 'v2', apply_patches_on_merge = 0;

INSERT INTO t_lwu_offset_v2 VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_offset_v2 VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_offset_v2 FINAL;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_offset_v2' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_offset_v2;

ALTER TABLE t_lwu_offset_v2 MODIFY SETTING enable_block_number_column = 1;
UPDATE t_lwu_offset_v2 SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_offset_v2 ORDER BY v;

OPTIMIZE TABLE t_lwu_offset_v2 FINAL;
SELECT v FROM t_lwu_offset_v2 ORDER BY v;

-- With both settings on from the start, merges store both columns, and the pairs are unique.
SELECT 'both';

CREATE TABLE t_lwu_offset_both (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_lwu_offset_both VALUES (0, 'a'), (1, 'c');
INSERT INTO t_lwu_offset_both VALUES (0, 'b');
OPTIMIZE TABLE t_lwu_offset_both FINAL;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_offset_both' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT _block_number, _block_offset, v FROM t_lwu_offset_both ORDER BY v;

UPDATE t_lwu_offset_both SET v = 'X' WHERE v = 'a';
SELECT v FROM t_lwu_offset_both ORDER BY v;

OPTIMIZE TABLE t_lwu_offset_both FINAL;
SELECT v FROM t_lwu_offset_both ORDER BY v;

DROP TABLE t_lwu_offset_v1;
DROP TABLE t_lwu_offset_v2;
DROP TABLE t_lwu_offset_both;
