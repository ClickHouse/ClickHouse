-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- The cluster `secret` and passwords at the cluster, shard and replica scope are hidden in `system.clusters.create_query` by default.

DROP CLUSTER IF EXISTS test_sql_cluster_05453;

CREATE CLUSTER test_sql_cluster_05453 (
    secret = 'cluster_secret_05453',
    user = 'default',
    password = 'cluster_password_05453',
    SHARD (host = '127.0.0.1', port = 9000, password = 'shard_password_05453'),
    SHARD (
        password = 'shard_with_replicas_password_05453',
        REPLICA (host = '127.0.0.1', port = 9000, password = 'replica_password_05453')
    )
);

SELECT DISTINCT create_query FROM system.clusters WHERE cluster = 'test_sql_cluster_05453';
SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05453' AND create_query LIKE '%password_05453%';
SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05453' AND create_query LIKE '%secret_05453%';

DROP CLUSTER test_sql_cluster_05453;
