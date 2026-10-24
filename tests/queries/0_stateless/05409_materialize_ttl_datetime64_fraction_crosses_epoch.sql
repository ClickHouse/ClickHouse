-- The fast `MODIFY TTL` path shifts the stored whole-second rows-TTL bounds by the difference of the literal
-- offsets. A fractional `DateTime64` is truncated toward zero when it is converted to those bounds, so the
-- shift is not exact when the value crosses the epoch: an expiry of `-1.5` seconds is stored as `-1`, but
-- adding two seconds gives `0.5`, which is stored as `0` (the non-expiring sentinel), not `1`.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_dt64_fraction;
CREATE TABLE t_ttl_dt64_fraction (d DateTime64(3, 'UTC'))
ENGINE = MergeTree ORDER BY tuple() TTL d
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_dt64_fraction;
INSERT INTO t_ttl_dt64_fraction VALUES ('1969-12-31 23:59:58.500');
ALTER TABLE t_ttl_dt64_fraction MODIFY TTL d + INTERVAL 2 SECOND;
SELECT count() FROM t_ttl_dt64_fraction;

DROP TABLE t_ttl_dt64_fraction;

-- With `materialize_ttl_recalculate_only = 1` the row is kept either way, but the recomputed bounds must be
-- the ones the new expression actually produces.
CREATE TABLE t_ttl_dt64_fraction (d DateTime64(3, 'UTC'))
ENGINE = MergeTree ORDER BY tuple() TTL d
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1;

SYSTEM STOP TTL MERGES t_ttl_dt64_fraction;
INSERT INTO t_ttl_dt64_fraction VALUES ('1969-12-31 23:59:58.500');
ALTER TABLE t_ttl_dt64_fraction MODIFY TTL d + INTERVAL 2 SECOND;
SELECT count() FROM t_ttl_dt64_fraction;
SELECT toUnixTimestamp(delete_ttl_info_min), toUnixTimestamp(delete_ttl_info_max)
FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_dt64_fraction' AND active;

DROP TABLE t_ttl_dt64_fraction;
