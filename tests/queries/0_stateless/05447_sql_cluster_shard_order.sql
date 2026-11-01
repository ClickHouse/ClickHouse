-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- Mixed SHARD forms and more than nine shards/replicas must keep SQL definition order.
-- Cluster numbers shards from configuration key order; lexicographic keys would
-- put node before shard and 10 before 2.

DROP CLUSTER IF EXISTS test_sql_cluster_05447;

CREATE CLUSTER test_sql_cluster_05447 (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    ),
    SHARD (
        host = '127.0.0.2',
        port = 9000
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05447'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05447;

CREATE CLUSTER test_sql_cluster_05447 (
    user = 'default',
    SHARD (
        host = '127.0.0.1',
        port = 9000
    ),
    SHARD (
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);

SELECT shard_num, replica_num, host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05447'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05447;

CREATE CLUSTER test_sql_cluster_05447 (
    user = 'default',
    SHARD (host = '127.0.0.1', port = 9000),
    SHARD (host = '127.0.0.2', port = 9000),
    SHARD (host = '127.0.0.3', port = 9000),
    SHARD (host = '127.0.0.4', port = 9000),
    SHARD (host = '127.0.0.5', port = 9000),
    SHARD (host = '127.0.0.6', port = 9000),
    SHARD (host = '127.0.0.7', port = 9000),
    SHARD (host = '127.0.0.8', port = 9000),
    SHARD (host = '127.0.0.9', port = 9000),
    SHARD (host = '127.0.0.10', port = 9000)
);

SELECT shard_num, replica_num, host_name
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05447'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05447;

CREATE CLUSTER test_sql_cluster_05447 (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000),
        REPLICA (host = '127.0.0.3', port = 9000),
        REPLICA (host = '127.0.0.4', port = 9000),
        REPLICA (host = '127.0.0.5', port = 9000),
        REPLICA (host = '127.0.0.6', port = 9000),
        REPLICA (host = '127.0.0.7', port = 9000),
        REPLICA (host = '127.0.0.8', port = 9000),
        REPLICA (host = '127.0.0.9', port = 9000),
        REPLICA (host = '127.0.0.10', port = 9000)
    )
);

SELECT shard_num, replica_num, host_name
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05447'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER IF EXISTS test_sql_cluster_05447;
