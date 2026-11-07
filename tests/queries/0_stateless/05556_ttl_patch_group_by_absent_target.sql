-- A patch that moves the TTL input into the past forces the TTL step of the merge.
-- A GROUP BY SET target that no part stores must still get the rolled-up values.

SET allow_experimental_lightweight_update = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_patch_rollup SYNC;

CREATE TABLE t_ttl_patch_rollup (k UInt64, x UInt64, v UInt64, d DateTime)
ENGINE = MergeTree ORDER BY k
TTL d + INTERVAL 1 DAY GROUP BY k SET v = sum(x)
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_ttl_patch_rollup SELECT number % 10, 1, 0, now() + INTERVAL 1 HOUR FROM numbers(100);

-- The rollup target is no longer stored in the part.
ALTER TABLE t_ttl_patch_rollup CLEAR COLUMN v IN PARTITION tuple();

UPDATE t_ttl_patch_rollup SET d = now() - INTERVAL 2 DAY WHERE 1;

OPTIMIZE TABLE t_ttl_patch_rollup FINAL;

SELECT count(), sum(v) FROM t_ttl_patch_rollup;

DROP TABLE t_ttl_patch_rollup;
