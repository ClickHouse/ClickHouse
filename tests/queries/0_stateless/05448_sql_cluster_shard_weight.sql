-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- `Cluster` looks up shard-with-REPLICA properties as `cluster.<name>.shardN..weight`
-- (a trailing dot on the shard prefix plus another dot in `.weight`). A map
-- configuration must write that double-dot key or every such shard silently
-- gets `weight = 1` and `internal_replication = 0`.

DROP CLUSTER IF EXISTS test_sql_cluster_05448;

CREATE CLUSTER test_sql_cluster_05448 (
    user = 'default',
    SHARD (
        weight = 0,
        internal_replication = 1,
        REPLICA (host = '127.0.0.1', port = 9000)
    ),
    SHARD (
        weight = 2,
        internal_replication = 0,
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);

SELECT shard_num, shard_weight, internal_replication, host_name
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05448'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05448;

CREATE CLUSTER test_sql_cluster_05448 (
    user = 'default',
    SHARD (
        weight = 5,
        host = '127.0.0.1',
        port = 9000
    )
);

SELECT shard_num, shard_weight, host_name
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05448'
ORDER BY shard_num
FORMAT TSV;

DROP CLUSTER IF EXISTS test_sql_cluster_05448;
