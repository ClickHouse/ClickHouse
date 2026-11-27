-- The conversion of two-level aggregated data to chunks in several threads returned the buckets in the order the threads
-- finished them. A remote `GROUP BY` over a view over a `Distributed` table merges the partial results of the view
-- on the remote server and sends them out of order, and the memory-efficient merging of the initiator, which relies
-- on the order of buckets, threw "pushed bucket ... twice" (or silently returned duplicate keys before).

DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS l;

CREATE TABLE l (id UInt32, val UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO l SELECT number, number FROM numbers(200000);
CREATE TABLE d AS l ENGINE = Distributed(test_shard_localhost, currentDatabase(), l);
CREATE VIEW v AS SELECT id, val FROM d;

SET optimize_trivial_view_pushdown_to_distributed = 1, prefer_localhost_replica = 0, max_threads = 8,
    group_by_two_level_threshold = 1000, distributed_aggregation_memory_efficient = 1;

SELECT count(), uniqExact(id) FROM (SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), v) GROUP BY id);
SELECT count(), uniqExact(id) FROM (SELECT id FROM clusterAllReplicas('test_cluster_one_shard_two_replicas', currentDatabase(), v) GROUP BY id);

DROP TABLE v;
DROP TABLE d;
DROP TABLE l;
