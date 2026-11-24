-- Merges that drop or rewrite rows give the same part with `merge_use_batch_sorting_queue` = 1 as with the default queue:
-- lightweight delete together with patch parts in horizontal and vertical merges, and `OPTIMIZE ... DEDUPLICATE`.
-- Parts 1-3 alternate in runs of 7 rows, then part 3 continues alone; whole runs are deleted and updated.

SET lightweight_delete_mode = 'alter_update';
SET max_insert_threads = 1;
SET optimize_throw_if_noop = 1;

CREATE TABLE src ENGINE = Memory AS
SELECT number AS id, concat('payload-', toString(number)) AS payload,
    number % 20 = 0 OR number BETWEEN 21 AND 27 AS removed,
    if(number >= 63, 2, intDiv(number, 7) % 3) AS part
FROM numbers(100);

CREATE TABLE seed (id UInt64, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS merge_use_batch_sorting_queue = 0, enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_lightweight_delete = 1,
    enable_block_number_column = 1, enable_block_offset_column = 1, max_bytes_to_merge_at_max_space_in_pool = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    index_granularity = 8, merge_max_block_size = 16;

INSERT INTO seed SELECT id, payload FROM src WHERE part = 0;
INSERT INTO seed SELECT id, payload FROM src WHERE part = 1;
INSERT INTO seed SELECT id, payload FROM src WHERE part = 2;

CREATE TABLE lwd_h_default AS seed;
CREATE TABLE lwd_h_batch AS seed SETTINGS merge_use_batch_sorting_queue = 1;
CREATE TABLE lwd_v_default AS seed SETTINGS enable_vertical_merge_algorithm = 1;
CREATE TABLE lwd_v_batch AS seed SETTINGS enable_vertical_merge_algorithm = 1, merge_use_batch_sorting_queue = 1;
CREATE TABLE dedup_default AS seed;
CREATE TABLE dedup_batch AS seed SETTINGS merge_use_batch_sorting_queue = 1;

ALTER TABLE dedup_default ATTACH PARTITION tuple() FROM seed;
INSERT INTO dedup_default SELECT id, payload FROM src WHERE removed;
ALTER TABLE dedup_batch ATTACH PARTITION tuple() FROM dedup_default;

DELETE FROM seed WHERE id % 20 = 0 OR id BETWEEN 21 AND 27;
ALTER TABLE lwd_h_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_h_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_v_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_v_batch ATTACH PARTITION tuple() FROM seed;
UPDATE lwd_h_default SET payload = concat(payload, '-u') WHERE id % 20 = 10 OR id BETWEEN 49 AND 55;
UPDATE lwd_h_batch SET payload = concat(payload, '-u') WHERE id % 20 = 10 OR id BETWEEN 49 AND 55;
UPDATE lwd_v_default SET payload = concat(payload, '-u') WHERE id % 20 = 10 OR id BETWEEN 49 AND 55;
UPDATE lwd_v_batch SET payload = concat(payload, '-u') WHERE id % 20 = 10 OR id BETWEEN 49 AND 55;

OPTIMIZE TABLE lwd_h_default FINAL;
OPTIMIZE TABLE lwd_h_batch FINAL;
OPTIMIZE TABLE lwd_v_default FINAL;
OPTIMIZE TABLE lwd_v_batch FINAL;
OPTIMIZE TABLE dedup_default FINAL DEDUPLICATE;
OPTIMIZE TABLE dedup_batch FINAL DEDUPLICATE;

SELECT 'horizontal', (SELECT count() FROM lwd_h_batch), (SELECT countIf(endsWith(payload, '-u')) FROM lwd_h_batch),
    (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM lwd_h_default)
    = (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM lwd_h_batch)
SETTINGS apply_deleted_mask = 0, apply_patch_parts = 0;
SELECT 'vertical', (SELECT count() FROM lwd_v_batch), (SELECT countIf(endsWith(payload, '-u')) FROM lwd_v_batch),
    (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM lwd_v_default)
    = (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM lwd_v_batch)
SETTINGS apply_deleted_mask = 0, apply_patch_parts = 0;
SELECT 'dedup', (SELECT count() FROM dedup_batch),
    (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM dedup_default)
    = (SELECT sum(cityHash64(id, payload, _part_offset, _block_number, _block_offset)) FROM dedup_batch);

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND NOT startsWith(part_name, 'patch')
ORDER BY table;
