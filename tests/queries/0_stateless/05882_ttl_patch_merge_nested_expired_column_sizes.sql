-- A patch merge with stopped TTL merges fills a `Nested` subcolumn that no part stores with arrays
-- sized like its siblings, as a read does, so the projection it rebuilds stays readable.

SET allow_experimental_lightweight_update = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_patch_nested;

CREATE TABLE t_ttl_patch_nested (id UInt64, ts DateTime, v UInt64, n Nested(a String, c String))
ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 DAY WHERE id = 100
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1, apply_patches_on_merge = 1,
    deduplicate_merge_projection_mode = 'rebuild', enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_ttl_patch_nested VALUES (1, '2020-01-01 00:00:00', 1, ['a', 'b'], ['x', 'y']);

-- No part stores `n.d`, so the merge fills it as an expired column.
ALTER TABLE t_ttl_patch_nested ADD COLUMN `n.d` Array(String);
ALTER TABLE t_ttl_patch_nested ADD PROJECTION p (SELECT id, v, `n.a`, `n.d` ORDER BY v);

SYSTEM STOP TTL MERGES t_ttl_patch_nested;
UPDATE t_ttl_patch_nested SET v = 2 WHERE id = 1;
OPTIMIZE TABLE t_ttl_patch_nested FINAL;

SELECT 'table', id, v, `n.a`, `n.d` FROM t_ttl_patch_nested;
SELECT 'projection', id, v, `n.a`, `n.d` FROM mergeTreeProjection(currentDatabase(), t_ttl_patch_nested, p);

DROP TABLE t_ttl_patch_nested;
