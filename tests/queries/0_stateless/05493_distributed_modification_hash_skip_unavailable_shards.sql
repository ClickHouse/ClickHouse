-- With `skip_unavailable_shards`, a read of a `Distributed` table can silently drop a shard that became unavailable
-- only during the read, while the modification-hash probes before and after it reach every shard. No table
-- changes, so the hash cannot tell a partial result from a full one: `modification_hash` must be NULL whenever
-- skipping is enabled, either by the query setting or by the table setting.

DROP TABLE IF EXISTS dist_skip_src;
DROP TABLE IF EXISTS dist_skip;
DROP TABLE IF EXISTS dist_skip_table_setting;

CREATE TABLE dist_skip_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dist_skip_src VALUES (1);

CREATE TABLE dist_skip (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_skip_src);
CREATE TABLE dist_skip_table_setting (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_skip_src)
    SETTINGS skip_unavailable_shards = 1;

SELECT 'no skipping', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_skip'
SETTINGS skip_unavailable_shards = 0, prefer_localhost_replica = 1;

SELECT 'query setting', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_skip'
SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1;

SELECT 'table setting', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_skip_table_setting'
SETTINGS skip_unavailable_shards = 0, prefer_localhost_replica = 1;

DROP TABLE dist_skip_table_setting;
DROP TABLE dist_skip;
DROP TABLE dist_skip_src;
