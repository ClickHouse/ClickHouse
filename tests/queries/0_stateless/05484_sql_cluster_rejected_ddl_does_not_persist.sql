-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- A CREATE/ALTER that fails in `materializeCluster` must not persist metadata, otherwise a retry
-- gets `CLUSTER_ALREADY_EXISTS` and a shared-storage replica cannot start.

DROP CLUSTER IF EXISTS test_sql_cluster_05484;

CREATE CLUSTER test_sql_cluster_05484 (SHARD (port = 9000)); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05484 (SHARD (host = '127.0.0.1', port = 9000));

SELECT host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05484'
ORDER BY shard_num, replica_num
FORMAT TSV;

ALTER CLUSTER test_sql_cluster_05484 (SHARD (port = 9000)); -- { serverError BAD_ARGUMENTS }

SELECT host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05484'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05484;
