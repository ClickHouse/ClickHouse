-- Tags: shard

-- The modification hash of a `Distributed` table must follow how a read actually reaches its shards.
--
-- A non-deterministic sharding key (e.g. `dictGet`, which can route a key to another shard after a dictionary
-- reload) decides what a read returns once `optimize_skip_unused_shards` may use it. None of the hashed inputs
-- sees such a change, so the hash fails closed in that case.
--
-- With `prefer_localhost_replica = 0` a local shard is read over TCP as the configured user of the cluster, so
-- it is probed over TCP as well, which also folds that user and its settings.

DROP TABLE IF EXISTS dist_routing_src;
DROP TABLE IF EXISTS dist_routing;
DROP TABLE IF EXISTS dist_routing_hashes;

CREATE TABLE dist_routing_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO dist_routing_src VALUES (1);

CREATE TABLE dist_routing (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_routing_src, rand());

SELECT 'non-deterministic key, not used', modification_hash IS NOT NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_routing'
SETTINGS optimize_skip_unused_shards = 1, allow_nondeterministic_optimize_skip_unused_shards = 0;

SELECT 'non-deterministic key, used for routing', modification_hash IS NULL
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_routing'
SETTINGS optimize_skip_unused_shards = 1, allow_nondeterministic_optimize_skip_unused_shards = 1;

DROP TABLE dist_routing;
CREATE TABLE dist_routing (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_routing_src, x);

CREATE TABLE dist_routing_hashes (k String, v Nullable(UInt128)) ENGINE = Memory;
INSERT INTO dist_routing_hashes SELECT 'in-process', modification_hash
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_routing' SETTINGS prefer_localhost_replica = 1;
INSERT INTO dist_routing_hashes SELECT 'tcp', modification_hash
FROM system.tables WHERE database = currentDatabase() AND name = 'dist_routing' SETTINGS prefer_localhost_replica = 0;

SELECT 'both known', countIf(v IS NOT NULL) FROM dist_routing_hashes;
SELECT 'local shard probed over TCP', (SELECT v FROM dist_routing_hashes WHERE k = 'in-process') != (SELECT v FROM dist_routing_hashes WHERE k = 'tcp');

DROP TABLE dist_routing;
DROP TABLE dist_routing_src;
DROP TABLE dist_routing_hashes;
