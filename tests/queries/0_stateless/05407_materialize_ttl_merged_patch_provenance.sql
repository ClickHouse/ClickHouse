-- A merge that applies a lightweight `UPDATE` (a patch part) to the column the rows TTL reads must not
-- carry over the stored rows-TTL bounds of its source parts as if they still described the merged rows.
-- Otherwise the fast `MODIFY TTL` path shifts bounds computed from the old values and drops a part whose
-- rows, after the update, expire far in the future.

SET alter_sync = 2, enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_ttl_merged_patch;
CREATE TABLE t_ttl_merged_patch (id UInt32, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_ttl_merged_patch;
INSERT INTO t_ttl_merged_patch SELECT number, now('UTC') - INTERVAL 100 DAY FROM numbers(10);
UPDATE t_ttl_merged_patch SET d = now('UTC') + INTERVAL 1000 DAY WHERE 1;
SYSTEM START MERGES t_ttl_merged_patch;
OPTIMIZE TABLE t_ttl_merged_patch FINAL;

ALTER TABLE t_ttl_merged_patch MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_merged_patch;

DROP TABLE t_ttl_merged_patch;
