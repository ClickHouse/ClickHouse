-- A physical column named as the rows-TTL expression shadows it: the TTL values are read from that
-- column instead of being evaluated. Such bounds must not be certified with the expression's
-- fingerprint, or the fast `MODIFY TTL` path shifts them as if they were computed from `d` and drops
-- a part whose rows are live under the new TTL.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_shadowing;
CREATE TABLE t_ttl_shadowing
(
    d DateTime('UTC'),
    `d + toIntervalDay(300)` DateTime('UTC')
)
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_shadowing;
INSERT INTO t_ttl_shadowing VALUES ('2100-01-01 00:00:00', now('UTC') + INTERVAL 1 DAY);
-- The true new expiry is in 2100.
ALTER TABLE t_ttl_shadowing MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_shadowing;
DROP TABLE t_ttl_shadowing;

-- The same with a physical column named as the new expression: the regular rewrite reads it, so the
-- fast path must not shift the bounds either. The column says the row expired long ago, so the rewrite
-- drops it, while shifting the bounds computed from `d` would keep it.
CREATE TABLE t_ttl_shadowing
(
    d DateTime('UTC'),
    `d + toIntervalDay(10)` DateTime('UTC')
)
ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES t_ttl_shadowing;
INSERT INTO t_ttl_shadowing VALUES ('2100-01-01 00:00:00', '2000-01-01 00:00:00');
ALTER TABLE t_ttl_shadowing MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_shadowing;
DROP TABLE t_ttl_shadowing;
