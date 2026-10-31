-- With parallel replicas, a direct read of a `MergeTree` table can be served by the servers of
-- `cluster_for_parallel_replicas`, which read their own parts. The modification hash covers only the local
-- parts, so it fails closed when a replica of that cluster is another server. A cluster whose replicas
-- are all this server reads the same parts, and the hash stays known.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1);

SELECT 'without parallel replicas', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't'
SETTINGS allow_experimental_parallel_reading_from_replicas = 0;

SELECT 'local replicas', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't'
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas = 'test_shard_localhost';

SELECT 'remote replica', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't'
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas = 'test_unavailable_shard';

SELECT 'unknown cluster', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 't'
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, cluster_for_parallel_replicas = 'no_such_cluster_05474';

DROP TABLE t;
