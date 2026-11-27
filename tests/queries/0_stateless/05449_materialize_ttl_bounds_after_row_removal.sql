-- The fast path of `MATERIALIZE TTL` shifts the stored rows-TTL bounds of a part. After an operation that
-- removes rows without recalculating the rows TTL, these bounds may belong to a row that no longer exists.
-- Such bounds must not be shifted: the result must match the regular recalculation, otherwise
-- `TTLPartDropMergeSelector` would keep an entirely expired part until the bound of the removed row.

SET alter_sync = 2, mutations_sync = 2;

-- A partial `ALTER TABLE ... DELETE` removes the row with the maximum TTL.
DROP TABLE IF EXISTS t_ttl_bounds_delete;
CREATE TABLE t_ttl_bounds_delete (id UInt64, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;

SYSTEM STOP TTL MERGES t_ttl_bounds_delete;
INSERT INTO t_ttl_bounds_delete
SELECT 1, toDateTime('2100-01-01', 'UTC')
UNION ALL SELECT 2, now('UTC') - INTERVAL 100 DAY;
ALTER TABLE t_ttl_bounds_delete DELETE WHERE id = 1;
ALTER TABLE t_ttl_bounds_delete MODIFY TTL d + INTERVAL 10 DAY;
SELECT 'delete', rows, delete_ttl_info_max < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_bounds_delete' AND active;

DROP TABLE t_ttl_bounds_delete;

-- A merge removes the row with the maximum TTL hidden by a lightweight `DELETE`.
DROP TABLE IF EXISTS t_ttl_bounds_lightweight_delete;
CREATE TABLE t_ttl_bounds_lightweight_delete (id UInt64, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;

SYSTEM STOP TTL MERGES t_ttl_bounds_lightweight_delete;
INSERT INTO t_ttl_bounds_lightweight_delete
SELECT 1, toDateTime('2100-01-01', 'UTC')
UNION ALL SELECT 2, now('UTC') - INTERVAL 100 DAY;
DELETE FROM t_ttl_bounds_lightweight_delete WHERE id = 1;
OPTIMIZE TABLE t_ttl_bounds_lightweight_delete FINAL;
ALTER TABLE t_ttl_bounds_lightweight_delete MODIFY TTL d + INTERVAL 10 DAY;
SELECT 'lightweight delete', rows, delete_ttl_info_max < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_bounds_lightweight_delete' AND active;

DROP TABLE t_ttl_bounds_lightweight_delete;

-- A `ReplacingMergeTree` merge replaces the old version holding the maximum TTL.
DROP TABLE IF EXISTS t_ttl_bounds_replacing;
CREATE TABLE t_ttl_bounds_replacing (id UInt64, v UInt64, d DateTime('UTC'))
ENGINE = ReplacingMergeTree(v) ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;

SYSTEM STOP TTL MERGES t_ttl_bounds_replacing;
INSERT INTO t_ttl_bounds_replacing VALUES (1, 1, '2100-01-01');
INSERT INTO t_ttl_bounds_replacing SELECT 1, 2, now('UTC') - INTERVAL 100 DAY;
OPTIMIZE TABLE t_ttl_bounds_replacing FINAL;
ALTER TABLE t_ttl_bounds_replacing MODIFY TTL d + INTERVAL 10 DAY;
SELECT 'replacing', rows, delete_ttl_info_max < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_bounds_replacing' AND active;

DROP TABLE t_ttl_bounds_replacing;
