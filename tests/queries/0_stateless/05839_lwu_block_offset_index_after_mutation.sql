-- Tags: no-random-merge-tree-settings

-- A skip index on `_block_offset` of a part that stores `_block_offset` without `_block_number` is built from the stored offsets.
-- They are ignored with `use_block_offset_only_with_block_number = 1`, so the index is refused for such a part.
-- A mutation that materializes `_block_number` rewrites `_block_offset` and must rebuild the index,
-- otherwise the new part stores both columns, the stale index is used and prunes granules with matching rows.

SET mutations_sync = 2;
SET alter_update_mode = 'heavy';
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_lwu_block_offset_index;

CREATE TABLE t_lwu_block_offset_index (uid UInt64, v UInt64) ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    add_minmax_index_for_block_offset_column = 1, index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Three blocks of 4 rows. The merged part stores `_block_offset` = [0..3, 0..3, 0..3], and each granule is indexed as [0, 3].
INSERT INTO t_lwu_block_offset_index SELECT number, number FROM numbers(4);
INSERT INTO t_lwu_block_offset_index SELECT number + 4, number FROM numbers(4);
INSERT INTO t_lwu_block_offset_index SELECT number + 8, number FROM numbers(4);
OPTIMIZE TABLE t_lwu_block_offset_index FINAL;

ALTER TABLE t_lwu_block_offset_index MODIFY SETTING use_block_offset_only_with_block_number = 1;

SELECT groupArray(_block_offset) FROM (SELECT _block_offset FROM t_lwu_block_offset_index ORDER BY uid);
SELECT uid FROM t_lwu_block_offset_index WHERE _block_offset = 10;

-- The mutation of `v` materializes `_block_number` and rewrites `_block_offset`. The files of `uid` are hardlinked.
ALTER TABLE t_lwu_block_offset_index UPDATE v = v + 1 WHERE uid = 0;

SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_lwu_block_offset_index' AND active AND column IN ('_block_number', '_block_offset')
ORDER BY column;

SELECT groupArray(_block_offset) FROM (SELECT _block_offset FROM t_lwu_block_offset_index ORDER BY uid);
SELECT uid FROM t_lwu_block_offset_index WHERE _block_offset = 10;
SELECT uid FROM t_lwu_block_offset_index WHERE _block_offset = 10 SETTINGS use_skip_indexes = 0;

DROP TABLE t_lwu_block_offset_index;

-- The same for a projection that stores `_block_offset`. Such a projection is not loaded for a part whose stored `_block_offset`
-- is ignored, so after a restart (here `DETACH` and `ATTACH`) the mutation must still rebuild it instead of hardlinking its stale copy.
-- The mutation changes only `w`, which the projection does not read. The projection is built by the merge, as a projection
-- with `_block_offset` and without `_block_number` cannot be calculated on insert.

DROP TABLE IF EXISTS t_lwu_block_offset_projection;

CREATE TABLE t_lwu_block_offset_projection (uid UInt64, v UInt64, w UInt64, PROJECTION p (SELECT uid, v, _block_offset ORDER BY v))
ENGINE = MergeTree ORDER BY uid
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1, use_block_offset_only_with_block_number = 0,
    allow_commit_order_projection = 1, materialize_projections_on_insert = 0, index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_lwu_block_offset_projection SELECT number, number, 0 FROM numbers(4);
INSERT INTO t_lwu_block_offset_projection SELECT number + 4, number + 4, 0 FROM numbers(4);
INSERT INTO t_lwu_block_offset_projection SELECT number + 8, number + 8, 0 FROM numbers(4);
OPTIMIZE TABLE t_lwu_block_offset_projection FINAL;

ALTER TABLE t_lwu_block_offset_projection MODIFY SETTING use_block_offset_only_with_block_number = 1;
DETACH TABLE t_lwu_block_offset_projection;
ATTACH TABLE t_lwu_block_offset_projection;

ALTER TABLE t_lwu_block_offset_projection UPDATE w = 1 WHERE uid = 0;

SELECT uid, _block_offset FROM t_lwu_block_offset_projection WHERE v IN (5, 10) ORDER BY uid SETTINGS optimize_use_projections = 0;
SELECT uid, _block_offset FROM t_lwu_block_offset_projection WHERE v IN (5, 10) ORDER BY uid SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_lwu_block_offset_projection;
