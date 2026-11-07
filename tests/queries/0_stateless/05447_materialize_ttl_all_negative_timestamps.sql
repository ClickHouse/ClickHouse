-- A part whose TTL values are all before the epoch stores `max = 0`: `MergeTreeDataPartTTLInfo::update`
-- starts `max` at zero, so it is a sentinel rather than the real maximum. The fast `MODIFY TTL` path must
-- not shift it into a bogus upper bound; the stored bounds must match a full recalculation.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_negative_altered;
DROP TABLE IF EXISTS t_ttl_negative_created;

CREATE TABLE t_ttl_negative_altered (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;
SYSTEM STOP TTL MERGES t_ttl_negative_altered;
INSERT INTO t_ttl_negative_altered VALUES ('1960-01-01 00:00:00');
ALTER TABLE t_ttl_negative_altered MODIFY TTL d + INTERVAL 22500 DAY;

CREATE TABLE t_ttl_negative_created (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;
SYSTEM STOP TTL MERGES t_ttl_negative_created;
INSERT INTO t_ttl_negative_created VALUES ('1960-01-01 00:00:00');
ALTER TABLE t_ttl_negative_created MODIFY TTL addDays(d, 22500);

SELECT
    (SELECT (delete_ttl_info_min, delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_negative_altered' AND active)
  = (SELECT (delete_ttl_info_min, delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_negative_created' AND active);

SELECT toDate(delete_ttl_info_min) = toDate(delete_ttl_info_max) FROM system.parts WHERE database = currentDatabase() AND table = 't_ttl_negative_altered' AND active;

DROP TABLE t_ttl_negative_altered;
DROP TABLE t_ttl_negative_created;
