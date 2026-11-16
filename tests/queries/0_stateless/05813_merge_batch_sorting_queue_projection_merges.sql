-- Projection merges give the same projection parts with `merge_use_batch_sorting_queue` = 1 as with the default queue:
-- a normal projection with another sorting key (equal keys across parts), a `_parent_part_offset` projection and a
-- commit-order projection, in horizontal and vertical merges, and the merge of the temporary parts that
-- `MATERIALIZE PROJECTION` writes for a part larger than `min_insert_block_size_rows`.

SET mutations_sync = 2;
SET max_insert_threads = 1;

CREATE TABLE src (id UInt64, k UInt8, payload String) ENGINE = Memory;
INSERT INTO src SELECT number, number % 5, concat('payload-', toString(number % 50)) FROM numbers(256);

CREATE TABLE h_default
(
    id UInt64,
    k UInt8,
    payload String,
    PROJECTION p_normal (SELECT * ORDER BY k),
    PROJECTION p_offset (SELECT id, payload, _part_offset ORDER BY payload),
    PROJECTION p_commit (SELECT *, _block_number, _block_offset ORDER BY _block_number, _block_offset)
)
ENGINE = MergeTree ORDER BY id
SETTINGS merge_use_batch_sorting_queue = 0, enable_vertical_merge_algorithm = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1, merge_max_block_size = 16;

CREATE TABLE h_batch AS h_default;
ALTER TABLE h_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;
CREATE TABLE v_default AS h_default;
ALTER TABLE v_default MODIFY SETTING enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE v_batch AS v_default;
ALTER TABLE v_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;

INSERT INTO h_default SELECT * FROM src WHERE id % 3 = 0;
INSERT INTO h_default SELECT * FROM src WHERE id % 3 = 1;
INSERT INTO h_default SELECT * FROM src WHERE id % 3 = 2;
INSERT INTO h_batch SELECT * FROM src WHERE id % 3 = 0;
INSERT INTO h_batch SELECT * FROM src WHERE id % 3 = 1;
INSERT INTO h_batch SELECT * FROM src WHERE id % 3 = 2;
INSERT INTO v_default SELECT * FROM src WHERE id % 3 = 0;
INSERT INTO v_default SELECT * FROM src WHERE id % 3 = 1;
INSERT INTO v_default SELECT * FROM src WHERE id % 3 = 2;
INSERT INTO v_batch SELECT * FROM src WHERE id % 3 = 0;
INSERT INTO v_batch SELECT * FROM src WHERE id % 3 = 1;
INSERT INTO v_batch SELECT * FROM src WHERE id % 3 = 2;

-- Commit-order projections are not built on insert, and parts with different projection sets are not merged.
ALTER TABLE h_default MATERIALIZE PROJECTION p_commit;
ALTER TABLE h_batch MATERIALIZE PROJECTION p_commit;
ALTER TABLE v_default MATERIALIZE PROJECTION p_commit;
ALTER TABLE v_batch MATERIALIZE PROJECTION p_commit;

OPTIMIZE TABLE h_default FINAL;
OPTIMIZE TABLE h_batch FINAL;
OPTIMIZE TABLE v_default FINAL;
OPTIMIZE TABLE v_batch FINAL;

SELECT 'horizontal', 'p_normal',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_normal')),
    (SELECT sum(cityHash64(id, k, payload, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_default', 'p_normal'))
        = (SELECT sum(cityHash64(id, k, payload, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_normal'));
SELECT 'horizontal', 'p_offset',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_offset')),
    (SELECT sum(cityHash64(id, payload, _parent_part_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_default', 'p_offset'))
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_offset'));
SELECT 'horizontal', 'p_commit',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_commit')),
    (SELECT sum(cityHash64(id, k, payload, _block_number, _block_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_default', 'p_commit'))
        = (SELECT sum(cityHash64(id, k, payload, _block_number, _block_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_commit'));
SELECT 'vertical', 'p_normal',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_normal')),
    (SELECT sum(cityHash64(id, k, payload, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_default', 'p_normal'))
        = (SELECT sum(cityHash64(id, k, payload, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_normal'));
SELECT 'vertical', 'p_offset',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_offset')),
    (SELECT sum(cityHash64(id, payload, _parent_part_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_default', 'p_offset'))
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_offset'));
SELECT 'vertical', 'p_commit',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_commit')),
    (SELECT sum(cityHash64(id, k, payload, _block_number, _block_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_default', 'p_commit'))
        = (SELECT sum(cityHash64(id, k, payload, _block_number, _block_offset, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_commit'));

-- `_parent_part_offset` points at the right parent row, and the commit-order projection matches the parent's virtual columns.
SELECT 'h_default',
    (SELECT sum(cityHash64(id, payload, _part_offset)) FROM h_default)
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_default', 'p_offset')),
    (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM h_default)
        = (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_default', 'p_commit'));
SELECT 'h_batch',
    (SELECT sum(cityHash64(id, payload, _part_offset)) FROM h_batch)
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_offset')),
    (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM h_batch)
        = (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM mergeTreeProjection(currentDatabase(), 'h_batch', 'p_commit'));
SELECT 'v_default',
    (SELECT sum(cityHash64(id, payload, _part_offset)) FROM v_default)
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_default', 'p_offset')),
    (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM v_default)
        = (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_default', 'p_commit'));
SELECT 'v_batch',
    (SELECT sum(cityHash64(id, payload, _part_offset)) FROM v_batch)
        = (SELECT sum(cityHash64(id, payload, _parent_part_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_offset')),
    (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM v_batch)
        = (SELECT sum(cityHash64(id, _block_number, _block_offset)) FROM mergeTreeProjection(currentDatabase(), 'v_batch', 'p_commit'));

-- The merges above merged the projection parts instead of rebuilding them.
SYSTEM FLUSH LOGS part_log;
SELECT table, ProfileEvents['MergedProjections'], ProfileEvents['RebuiltProjections']
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND table IN ('h_default', 'h_batch', 'v_default', 'v_batch')
ORDER BY table;

CREATE TABLE big_default (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS merge_use_batch_sorting_queue = 0;
CREATE TABLE big_batch AS big_default;
ALTER TABLE big_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;
INSERT INTO big_default SELECT number, intHash64(number) % 1000 FROM numbers(1200000);
INSERT INTO big_batch SELECT number, intHash64(number) % 1000 FROM numbers(1200000);
OPTIMIZE TABLE big_default FINAL;
OPTIMIZE TABLE big_batch FINAL;
ALTER TABLE big_default ADD PROJECTION p (SELECT * ORDER BY b);
ALTER TABLE big_batch ADD PROJECTION p (SELECT * ORDER BY b);
ALTER TABLE big_default MATERIALIZE PROJECTION p;
ALTER TABLE big_batch MATERIALIZE PROJECTION p;

SELECT 'temporary parts',
    (SELECT count() FROM mergeTreeProjection(currentDatabase(), 'big_batch', 'p')),
    (SELECT sum(cityHash64(a, b, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'big_default', 'p'))
        = (SELECT sum(cityHash64(a, b, _part_offset)) FROM mergeTreeProjection(currentDatabase(), 'big_batch', 'p'));
