-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- Out-of-range `port` used to wrap to `UInt16` (`70000` → `4464`) while `create_query`
-- kept the original value. Reject it before the definition is materialized or stored.

DROP CLUSTER IF EXISTS test_sql_cluster_05570;

CREATE CLUSTER test_sql_cluster_05570 (SHARD (host = '127.0.0.1', port = 70000)); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05570 (SHARD (REPLICA (host = '127.0.0.1', port = 70000))); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05570 (SHARD (host = '127.0.0.1', port = 0)); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05570';

CREATE CLUSTER test_sql_cluster_05570 (SHARD (host = '127.0.0.1', port = 9000));

SELECT host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05570'
ORDER BY shard_num, replica_num
FORMAT TSV;

ALTER CLUSTER test_sql_cluster_05570 (SHARD (host = '127.0.0.1', port = 70000)); -- { serverError BAD_ARGUMENTS }

ALTER CLUSTER test_sql_cluster_05570 (SHARD (REPLICA (host = '127.0.0.1', port = 65536))); -- { serverError BAD_ARGUMENTS }

SELECT host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05570'
ORDER BY shard_num, replica_num
FORMAT TSV;

DROP CLUSTER test_sql_cluster_05570;
