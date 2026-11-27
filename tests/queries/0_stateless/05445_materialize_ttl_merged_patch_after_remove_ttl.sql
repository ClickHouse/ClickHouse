-- A merge that applies a lightweight `UPDATE` (a patch part) to the column a previous rows TTL read must drop
-- the stored rows-TTL fingerprint even when the table has no rows TTL at the time of the merge. Otherwise a
-- later `MODIFY TTL` back to a shift of the old expression trusts bounds computed from the old values and
-- drops rows that, after the update, expire far in the future.

SET alter_sync = 2, mutations_sync = 2, enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_ttl_merged_patch_removed;
CREATE TABLE t_ttl_merged_patch_removed (id UInt32, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_ttl_merged_patch_removed;
INSERT INTO t_ttl_merged_patch_removed SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(10);
ALTER TABLE t_ttl_merged_patch_removed REMOVE TTL;
UPDATE t_ttl_merged_patch_removed SET d = now('UTC') + INTERVAL 1000 DAY WHERE 1;
SYSTEM START MERGES t_ttl_merged_patch_removed;
OPTIMIZE TABLE t_ttl_merged_patch_removed FINAL;
SELECT count(), min(d) > now('UTC') FROM t_ttl_merged_patch_removed SETTINGS apply_patch_parts = 0;

ALTER TABLE t_ttl_merged_patch_removed MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_merged_patch_removed;

DROP TABLE t_ttl_merged_patch_removed;
