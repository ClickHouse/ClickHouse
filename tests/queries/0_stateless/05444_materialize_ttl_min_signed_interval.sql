-- `d - toIntervalSecond(-9223372036854775808)` is a constant shift of `d`, but negating the minimum
-- signed interval overflows `Int64`. The shift analysis (used both when a part records its rows-TTL
-- fingerprint and by the fast `MODIFY TTL` path) must reject it instead of negating it unchecked
-- (undefined behavior, caught by UBSan). The part then takes the regular rewrite, which gives the
-- same result as a table created with this TTL.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_min_interval_altered;
DROP TABLE IF EXISTS t_ttl_min_interval_created;

CREATE TABLE t_ttl_min_interval_altered (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d + INTERVAL 1 SECOND
    SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;
INSERT INTO t_ttl_min_interval_altered VALUES ('2100-01-01 00:00:02');
ALTER TABLE t_ttl_min_interval_altered MODIFY TTL d - toIntervalSecond(-9223372036854775808);

CREATE TABLE t_ttl_min_interval_created (d DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple()
    TTL d - toIntervalSecond(-9223372036854775808)
    SETTINGS min_bytes_for_full_part_storage = 0;
INSERT INTO t_ttl_min_interval_created VALUES ('2100-01-01 00:00:02');
OPTIMIZE TABLE t_ttl_min_interval_created FINAL;

SELECT (SELECT count() FROM t_ttl_min_interval_altered) = (SELECT count() FROM t_ttl_min_interval_created);

DROP TABLE t_ttl_min_interval_altered;
DROP TABLE t_ttl_min_interval_created;
