-- A column added by `ALTER TABLE ... ADD COLUMN` that no part stores yet made a merge apply the TTL although
-- `SYSTEM STOP TTL MERGES` was in effect, so expired rows were deleted:
-- https://github.com/ClickHouse/ClickHouse/issues/127530

DROP TABLE IF EXISTS t_stop_ttl;
DROP TABLE IF EXISTS t_stop_ttl_column;

CREATE TABLE t_stop_ttl (id UInt64, d DateTime) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS enable_vertical_merge_algorithm = 0;
SYSTEM STOP TTL MERGES t_stop_ttl;
INSERT INTO t_stop_ttl SELECT number, '2000-01-01' FROM numbers(10);
INSERT INTO t_stop_ttl SELECT number + 10, '2000-01-01' FROM numbers(10);
ALTER TABLE t_stop_ttl ADD COLUMN x UInt64;
ALTER TABLE t_stop_ttl ADD COLUMN y UInt64 MATERIALIZED x + 5;

OPTIMIZE TABLE t_stop_ttl;
SELECT 'stopped', count(), sum(x), sum(y) FROM t_stop_ttl;
SELECT 'ttl info kept', toUnixTimestamp(delete_ttl_info_min) > 0, toUnixTimestamp(delete_ttl_info_max) > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_stop_ttl' AND active;

SYSTEM START TTL MERGES t_stop_ttl;
OPTIMIZE TABLE t_stop_ttl FINAL;
SELECT 'started', count() FROM t_stop_ttl;

-- A column TTL and `TTL ... WHERE` are not applied either.
CREATE TABLE t_stop_ttl_column (id UInt64, d DateTime, v UInt64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 1 DAY DELETE WHERE id % 2 = 0
SETTINGS enable_vertical_merge_algorithm = 0;
SYSTEM STOP TTL MERGES t_stop_ttl_column;
INSERT INTO t_stop_ttl_column SELECT number, '2000-01-01', 7 FROM numbers(10);
INSERT INTO t_stop_ttl_column SELECT number + 10, '2000-01-01', 7 FROM numbers(10);
ALTER TABLE t_stop_ttl_column ADD COLUMN x UInt64;

OPTIMIZE TABLE t_stop_ttl_column FINAL;
SELECT 'stopped', count(), sum(v) FROM t_stop_ttl_column;

SYSTEM START TTL MERGES t_stop_ttl_column;
OPTIMIZE TABLE t_stop_ttl_column FINAL;
SELECT 'started', count(), sum(v) FROM t_stop_ttl_column;

DROP TABLE t_stop_ttl;
DROP TABLE t_stop_ttl_column;
