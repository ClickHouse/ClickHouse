-- The fast path of `MATERIALIZE TTL` shifts the stored rows-TTL bounds of a part. A part whose rows were all
-- removed by a merge may still carry the rows-TTL bounds of the removed rows. When such an empty part is
-- merged with a live part, its stale bounds must not be attached to the live part's bounds and fingerprint,
-- otherwise the shifted maximum would keep an entirely expired part with `ttl_only_drop_parts`.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_ttl_empty_source;
CREATE TABLE t_ttl_empty_source (id UInt64, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, materialize_ttl_recalculate_only = 1, ttl_only_drop_parts = 1;

SYSTEM STOP TTL MERGES t_ttl_empty_source;
INSERT INTO t_ttl_empty_source VALUES (1, '2100-01-01');
DELETE FROM t_ttl_empty_source WHERE id = 1;
OPTIMIZE TABLE t_ttl_empty_source FINAL;
INSERT INTO t_ttl_empty_source SELECT 2, now('UTC') - INTERVAL 100 DAY;
OPTIMIZE TABLE t_ttl_empty_source FINAL;
ALTER TABLE t_ttl_empty_source MODIFY TTL d + INTERVAL 10 DAY;
SELECT rows, delete_ttl_info_max < now() FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_empty_source' AND active;

DROP TABLE t_ttl_empty_source;
