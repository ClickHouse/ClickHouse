-- The fast `MODIFY TTL` path must not shift valid positive TTL bounds into a range entirely before the
-- epoch: a full recalculation stores `max = 0` for such a range, while the shifted bounds would keep a
-- negative `max`, which makes a subsequent TTL extension drop the whole part before evaluating the new TTL.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_negative_destination;

CREATE TABLE t_ttl_negative_destination (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 25000 DAY
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1;
SYSTEM STOP TTL MERGES t_ttl_negative_destination;
INSERT INTO t_ttl_negative_destination VALUES ('1960-01-01 00:00:00');
ALTER TABLE t_ttl_negative_destination MODIFY TTL d;

SELECT toUnixTimestamp(delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_negative_destination' AND active;

ALTER TABLE t_ttl_negative_destination MODIFY SETTING materialize_ttl_recalculate_only = 0;
ALTER TABLE t_ttl_negative_destination MODIFY TTL d + INTERVAL 30000 DAY;

SELECT count() FROM t_ttl_negative_destination;

DROP TABLE t_ttl_negative_destination;
