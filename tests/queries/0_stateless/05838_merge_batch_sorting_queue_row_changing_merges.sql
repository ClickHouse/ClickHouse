-- Merges that drop or rewrite rows give the same part with `merge_use_batch_sorting_queue` = 1 as with the default queue:
-- lightweight delete, TTL delete and patch parts in horizontal and vertical merges, and `OPTIMIZE ... DEDUPLICATE`.
-- Parts 1-3 alternate in runs of 7 rows and part 4 follows them; the removed rows include a whole run.

SET lightweight_delete_mode = 'alter_update';
SET max_insert_threads = 1;
SET optimize_throw_if_noop = 1;

CREATE TABLE src ENGINE = Memory AS
SELECT number AS id, toUInt8(number % 5) AS k, concat('payload-', toString(number)) AS payload,
    number % 20 = 0 OR number BETWEEN 140 AND 146 AS removed,
    if(removed, toDateTime('2000-01-01'), toDateTime('2100-01-01')) AS d,
    if(number >= 300, 3, intDiv(number, 7) % 3) AS part
FROM numbers(400);

CREATE TABLE seed (id UInt64, k UInt8, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS merge_use_batch_sorting_queue = 0, enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1, vertical_merge_optimize_lightweight_delete = 1,
    vertical_merge_optimize_ttl_delete = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
    max_bytes_to_merge_at_max_space_in_pool = 0, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, index_granularity = 8, merge_max_block_size = 16;
CREATE TABLE seed_ttl AS seed;
ALTER TABLE seed_ttl ADD COLUMN d DateTime, MODIFY TTL d;

INSERT INTO seed SELECT id, k, payload FROM src WHERE part = 0;
INSERT INTO seed SELECT id, k, payload FROM src WHERE part = 1;
INSERT INTO seed SELECT id, k, payload FROM src WHERE part = 2;
INSERT INTO seed SELECT id, k, payload FROM src WHERE part = 3;
INSERT INTO seed_ttl SELECT id, k, payload, d FROM src WHERE part = 0;
INSERT INTO seed_ttl SELECT id, k, payload, d FROM src WHERE part = 1;
INSERT INTO seed_ttl SELECT id, k, payload, d FROM src WHERE part = 2;
INSERT INTO seed_ttl SELECT id, k, payload, d FROM src WHERE part = 3;

CREATE TABLE lwd_h_default AS seed;
CREATE TABLE lwd_h_batch AS seed;
ALTER TABLE lwd_h_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;
CREATE TABLE lwd_v_default AS seed;
ALTER TABLE lwd_v_default MODIFY SETTING enable_vertical_merge_algorithm = 1;
CREATE TABLE lwd_v_batch AS seed;
ALTER TABLE lwd_v_batch MODIFY SETTING enable_vertical_merge_algorithm = 1, merge_use_batch_sorting_queue = 1;

CREATE TABLE patch_h_default AS seed;
CREATE TABLE patch_h_batch AS seed;
ALTER TABLE patch_h_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;
CREATE TABLE patch_v_default AS seed;
ALTER TABLE patch_v_default MODIFY SETTING enable_vertical_merge_algorithm = 1;
CREATE TABLE patch_v_batch AS seed;
ALTER TABLE patch_v_batch MODIFY SETTING enable_vertical_merge_algorithm = 1, merge_use_batch_sorting_queue = 1;

CREATE TABLE ttl_h_default AS seed_ttl;
CREATE TABLE ttl_h_batch AS seed_ttl;
ALTER TABLE ttl_h_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;
CREATE TABLE ttl_v_default AS seed_ttl;
ALTER TABLE ttl_v_default MODIFY SETTING enable_vertical_merge_algorithm = 1;
CREATE TABLE ttl_v_batch AS seed_ttl;
ALTER TABLE ttl_v_batch MODIFY SETTING enable_vertical_merge_algorithm = 1, merge_use_batch_sorting_queue = 1;

CREATE TABLE dedup_default AS seed;
CREATE TABLE dedup_batch AS seed;
ALTER TABLE dedup_batch MODIFY SETTING merge_use_batch_sorting_queue = 1;

ALTER TABLE lwd_h_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_h_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_v_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE lwd_v_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE patch_h_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE patch_h_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE patch_v_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE patch_v_batch ATTACH PARTITION tuple() FROM seed;
ALTER TABLE ttl_h_default ATTACH PARTITION tuple() FROM seed_ttl;
ALTER TABLE ttl_h_batch ATTACH PARTITION tuple() FROM seed_ttl;
ALTER TABLE ttl_v_default ATTACH PARTITION tuple() FROM seed_ttl;
ALTER TABLE ttl_v_batch ATTACH PARTITION tuple() FROM seed_ttl;
ALTER TABLE dedup_default ATTACH PARTITION tuple() FROM seed;
ALTER TABLE dedup_batch ATTACH PARTITION tuple() FROM seed;

DELETE FROM lwd_h_default WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
DELETE FROM lwd_h_batch WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
DELETE FROM lwd_v_default WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
DELETE FROM lwd_v_batch WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
UPDATE patch_h_default SET payload = concat(payload, '-u') WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
UPDATE patch_h_batch SET payload = concat(payload, '-u') WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
UPDATE patch_v_default SET payload = concat(payload, '-u') WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
UPDATE patch_v_batch SET payload = concat(payload, '-u') WHERE id % 20 = 0 OR id BETWEEN 140 AND 146;
INSERT INTO dedup_default SELECT id, k, payload FROM src WHERE removed;
INSERT INTO dedup_batch SELECT id, k, payload FROM src WHERE removed;

OPTIMIZE TABLE lwd_h_default FINAL;
OPTIMIZE TABLE lwd_h_batch FINAL;
OPTIMIZE TABLE lwd_v_default FINAL;
OPTIMIZE TABLE lwd_v_batch FINAL;
OPTIMIZE TABLE patch_h_default FINAL;
OPTIMIZE TABLE patch_h_batch FINAL;
OPTIMIZE TABLE patch_v_default FINAL;
OPTIMIZE TABLE patch_v_batch FINAL;
OPTIMIZE TABLE ttl_h_default FINAL;
OPTIMIZE TABLE ttl_h_batch FINAL;
OPTIMIZE TABLE ttl_v_default FINAL;
OPTIMIZE TABLE ttl_v_batch FINAL;
OPTIMIZE TABLE dedup_default FINAL DEDUPLICATE;
OPTIMIZE TABLE dedup_batch FINAL DEDUPLICATE;

SELECT 'lwd horizontal', (SELECT count() FROM lwd_h_batch),
    (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM lwd_h_default)
    = (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM lwd_h_batch)
SETTINGS apply_deleted_mask = 0;
SELECT 'lwd vertical', (SELECT count() FROM lwd_v_batch),
    (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM lwd_v_default)
    = (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM lwd_v_batch)
SETTINGS apply_deleted_mask = 0;
SELECT 'ttl horizontal', (SELECT count() FROM ttl_h_batch),
    (SELECT sum(cityHash64(id, k, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_h_default)
    = (SELECT sum(cityHash64(id, k, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_h_batch);
SELECT 'ttl vertical', (SELECT count() FROM ttl_v_batch),
    (SELECT sum(cityHash64(id, k, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_v_default)
    = (SELECT sum(cityHash64(id, k, payload, d, _part_offset, _block_number, _block_offset)) FROM ttl_v_batch);
SELECT 'patch horizontal', (SELECT count() FROM patch_h_batch), (SELECT countIf(endsWith(payload, '-u')) FROM patch_h_batch),
    (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM patch_h_default)
    = (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM patch_h_batch)
SETTINGS apply_patch_parts = 0;
SELECT 'patch vertical', (SELECT count() FROM patch_v_batch), (SELECT countIf(endsWith(payload, '-u')) FROM patch_v_batch),
    (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM patch_v_default)
    = (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM patch_v_batch)
SETTINGS apply_patch_parts = 0;
SELECT 'dedup', (SELECT count() FROM dedup_batch),
    (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM dedup_default)
    = (SELECT sum(cityHash64(id, k, payload, _part_offset, _block_number, _block_offset)) FROM dedup_batch);

SYSTEM FLUSH LOGS part_log;
SELECT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND NOT startsWith(part_name, 'patch')
ORDER BY table;
