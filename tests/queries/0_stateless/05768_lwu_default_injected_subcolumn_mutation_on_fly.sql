-- A patched subcolumn `b.x` read to evaluate `z DEFAULT b.x + 1` is also injected into an on-fly mutation step, which
-- projects it out and keeps only its parent `b`. The query that selects `b.x` must still get it.

SET allow_experimental_lightweight_update = 1;

DROP TABLE IF EXISTS t_lwu_default_injected_subcolumn;

CREATE TABLE t_lwu_default_injected_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v1';

INSERT INTO t_lwu_default_injected_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_default_injected_subcolumn;

ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN b Tuple(x UInt32, y UInt32) DEFAULT tuple(100, 200);
ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN z UInt32 DEFAULT b.x + 1;
ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN c UInt32 DEFAULT 4;

UPDATE t_lwu_default_injected_subcolumn SET b = tuple(900, 901) WHERE id = 0;
ALTER TABLE t_lwu_default_injected_subcolumn UPDATE c = 8 WHERE 1 SETTINGS mutations_sync = 0;

SELECT id, b.x, z, c FROM t_lwu_default_injected_subcolumn ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, b.x, z, c FROM t_lwu_default_injected_subcolumn PREWHERE c > 0 ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, b, b.x, z, c FROM t_lwu_default_injected_subcolumn ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, z FROM t_lwu_default_injected_subcolumn ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, b.x, z, c FROM t_lwu_default_injected_subcolumn ORDER BY id SETTINGS apply_mutations_on_fly = 0;

DROP TABLE t_lwu_default_injected_subcolumn;

-- The same with the patch format `v2`.

CREATE TABLE t_lwu_default_injected_subcolumn (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_injected_subcolumn VALUES (0), (1);
SYSTEM STOP MERGES t_lwu_default_injected_subcolumn;

ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN b Tuple(x UInt32, y UInt32) DEFAULT tuple(100, 200);
ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN z UInt32 DEFAULT b.x + 1;
ALTER TABLE t_lwu_default_injected_subcolumn ADD COLUMN c UInt32 DEFAULT 4;

UPDATE t_lwu_default_injected_subcolumn SET b = tuple(900, 901) WHERE id = 0;
ALTER TABLE t_lwu_default_injected_subcolumn UPDATE c = 8 WHERE 1 SETTINGS mutations_sync = 0;

SELECT id, b.x, z, c FROM t_lwu_default_injected_subcolumn ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, b.x, z, c FROM t_lwu_default_injected_subcolumn PREWHERE c > 0 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_lwu_default_injected_subcolumn;
