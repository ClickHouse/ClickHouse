-- Tags: distributed, no-fasttest
-- A parallel replica of an `INSERT SELECT` runs as a secondary query and, with `serialize_query_plan = 1`, builds
-- the plans of the shards that its `IN` subquery reads. The converting `materialize` it puts on top of each such plan
-- must not get a marshalled column on the shard, which used to abort with `Logical error: 'from_blob_task'`.
-- `optimize_distinct_in_order = 1` (the default) is pinned: its transform makes the constant `5` a full column, so the
-- shard marshals it.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst SYNC;

CREATE TABLE t_src (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_src SELECT number % 4 FROM numbers(100);
CREATE TABLE t_dst (k Int64, c UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dst', 'r1') ORDER BY k;

INSERT INTO t_dst SELECT k, 3 FROM t_src
WHERE k IN (SELECT k FROM (SELECT DISTINCT 5, k FROM cluster('test_cluster_two_shards', currentDatabase(), 't_src') ORDER BY ALL LIMIT 30))
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_distributed_insert_select = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    max_parallel_replicas = 3, serialize_query_plan = 1, enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0,
    optimize_read_in_order = 0, optimize_distinct_in_order = 1;

SELECT k, c, count() FROM t_dst GROUP BY ALL ORDER BY ALL;

DROP TABLE t_src;
DROP TABLE t_dst SYNC;
