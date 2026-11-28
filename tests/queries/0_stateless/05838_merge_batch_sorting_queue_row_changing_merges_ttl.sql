-- TTL delete merges give the same part with `merge_use_batch_sorting_queue` = 1 as with the default queue,
-- in horizontal and vertical merges.
-- Parts 1-3 alternate in runs of 7 rows, then part 3 continues alone; the expired rows include a whole run.

SET max_insert_threads = 1;
SET optimize_throw_if_noop = 1;

CREATE TABLE src ENGINE = Memory AS
SELECT number AS id, concat('payload-', toString(number)) AS payload,
    if(number % 20 = 0 OR number BETWEEN 21 AND 27, toDateTime('2000-01-01'), toDateTime('2100-01-01')) AS d,
    if(number >= 63, 2, intDiv(number, 7) % 3) AS part
FROM numbers(100);

CREATE TABLE seed (id UInt64, payload String, d DateTime)
ENGINE = MergeTree ORDER BY id TTL d
SETTINGS merge_use_batch_sorting_queue = 0, enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_ttl_delete = 1,
    enable_block_number_column = 1, enable_block_offset_column = 1, max_bytes_to_merge_at_max_space_in_pool = 0,
    merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, index_granularity = 8, merge_max_block_size = 16;

INSERT INTO seed SELECT id, payload, d FROM src WHERE part = 0;
INSERT INTO seed SELECT id, payload, d FROM src WHERE part = 1;
INSERT INTO seed SELECT id, payload, d FROM src WHERE part = 2;

CREATE TABLE ttl_h_default AS seed;
CREATE TABLE ttl_h_batch AS seed SETTINGS merge_use_batch_sorting_queue = 1;
CREATE TABLE ttl_v_default AS seed SETTINGS enable_vertical_merge_algorithm = 1;
CREATE TABLE ttl_v_batch AS seed SETTINGS enable_vertical_merge_algorithm = 1, merge_use_batch_sorting_queue = 1;

ALTER TABLE ttl_h_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE ttl_h_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE ttl_v_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE ttl_v_batch ATTACH PARTITION tuple() FROM seed;

OPTIMIZE TABLE ttl_h_default FINAL;
OPTIMIZE TABLE ttl_h_batch FINAL;
OPTIMIZE TABLE ttl_v_default FINAL;
OPTIMIZE TABLE ttl_v_batch FINAL;

SELECT 'horizontal', (SELECT count() FROM ttl_h_batch),
    (SELECT sum(cityHash64(id, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_h_default)
    = (SELECT sum(cityHash64(id, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_h_batch);
SELECT 'vertical', (SELECT count() FROM ttl_v_batch),
    (SELECT sum(cityHash64(id, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_v_default)
    = (SELECT sum(cityHash64(id, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_v_batch);

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts'
ORDER BY table;
