-- The fast `MODIFY TTL` path may only shift the stored rows-TTL bounds by the difference of the literal
-- offsets when both expressions are computed exactly. Here the old expression saturates at the end of the
-- representable calendar, so the stored bound is not `d + 10000000 days`, and subtracting that offset
-- would make the shifted bound negative and drop the part. The part must take the regular rewrite.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_saturated;
CREATE TABLE t_ttl_saturated (d DateTime64(0, 'UTC'))
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 10000000 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

INSERT INTO t_ttl_saturated VALUES ('2100-01-01 00:00:00');
ALTER TABLE t_ttl_saturated MODIFY TTL d + INTERVAL 1 SECOND;
SELECT count() FROM t_ttl_saturated;

DROP TABLE t_ttl_saturated;

