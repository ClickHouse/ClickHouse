-- Tags: long
-- 1.2M-row parts, too heavy for the 180 s flaky-check limit under sanitizers.

SET mutations_sync = 2, lightweight_deletes_sync = 2;

-- Projection storing the parent `_part_offset` (with_parent_part_offset path), on Compact
-- (MutateAllPartColumns) and Wide (MutateSomePartColumns) parts. The deletion mask must filter the
-- generated offset column too, so surviving rows keep their original parent offsets. Mutations and
-- merges squash source blocks to `min_insert_block_size_rows` (1048449) rows before calculating a
-- projection, so 1.2M rows are needed for a batch to start at a non-zero offset. The base side of each
-- check reads with optimize_use_projections = 0, otherwise its `_part_offset` comes from the same projection.
DROP TABLE IF EXISTS t_lwd_offset_compact;
CREATE TABLE t_lwd_offset_compact (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = '1G',
         min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
         merge_max_block_size = 8192;

INSERT INTO t_lwd_offset_compact SELECT number, number % 13 FROM numbers(1200000)
SETTINGS max_block_size = 1200000, max_insert_block_size = 1200000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'offset compact storage', part_type, part_storage_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_offset_compact' AND active;

DELETE FROM t_lwd_offset_compact WHERE a % 101 = 0;

SELECT 'offset compact first delete rows',
    (SELECT count() FROM t_lwd_offset_compact SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_compact' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) WHERE a % 101 = 0);
SELECT 'offset compact first delete parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_compact l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

DELETE FROM t_lwd_offset_compact WHERE a % 103 = 1;

SELECT 'offset compact second delete rows',
    (SELECT count() FROM t_lwd_offset_compact SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_compact' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) WHERE a % 101 = 0 OR a % 103 = 1);
SELECT 'offset compact second delete parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_compact l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

-- A merge rebuilds the projection from the surviving rows (MergeTask::calculateProjections).
INSERT INTO t_lwd_offset_compact SELECT number + 1200000, number % 13 FROM numbers(1000);
OPTIMIZE TABLE t_lwd_offset_compact FINAL;

SELECT 'offset compact merged rows',
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_offset_compact' AND active),
    (SELECT count() FROM t_lwd_offset_compact SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_compact' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) WHERE a < 1200000 AND (a % 101 = 0 OR a % 103 = 1));
SELECT 'offset compact merged parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_compact l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_compact, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

DROP TABLE t_lwd_offset_compact;

DROP TABLE IF EXISTS t_lwd_offset_wide;
CREATE TABLE t_lwd_offset_wide (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS lightweight_mutation_projection_mode = 'rebuild', min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
         merge_max_block_size = 8192;

INSERT INTO t_lwd_offset_wide SELECT number, number % 13 FROM numbers(1200000)
SETTINGS max_block_size = 1200000, max_insert_block_size = 1200000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'offset wide storage', part_type, part_storage_type, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_offset_wide' AND active;

DELETE FROM t_lwd_offset_wide WHERE a % 101 = 0;

SELECT 'offset wide first delete rows',
    (SELECT count() FROM t_lwd_offset_wide SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_wide' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) WHERE a % 101 = 0);
SELECT 'offset wide first delete parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_wide l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

DELETE FROM t_lwd_offset_wide WHERE a % 103 = 1;

SELECT 'offset wide second delete rows',
    (SELECT count() FROM t_lwd_offset_wide SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_wide' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) WHERE a % 101 = 0 OR a % 103 = 1);
SELECT 'offset wide second delete parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_wide l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

-- A merge rebuilds the projection from the surviving rows (MergeTask::calculateProjections).
INSERT INTO t_lwd_offset_wide SELECT number + 1200000, number % 13 FROM numbers(1000);
OPTIMIZE TABLE t_lwd_offset_wide FINAL;

SELECT 'offset wide merged rows',
    (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_lwd_offset_wide' AND active),
    (SELECT count() FROM t_lwd_offset_wide SETTINGS optimize_use_projections = 0),
    (SELECT sum(rows) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_offset_wide' AND name = 'p' AND active),
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) WHERE a < 1200000 AND (a % 101 = 0 OR a % 103 = 1));
SELECT 'offset wide merged parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_offset_wide l JOIN mergeTreeProjection(currentDatabase(), t_lwd_offset_wide, p) r USING (a)
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

DROP TABLE t_lwd_offset_wide;
