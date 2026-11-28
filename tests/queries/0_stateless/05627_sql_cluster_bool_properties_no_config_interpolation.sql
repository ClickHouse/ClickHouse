-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster
-- Boolean cluster properties must be parsed from the raw SQL value. A configuration
-- reference such as `${tcp_port}` or `${tcp_port_secure}` must not be expanded and
-- stored: a peer without that key would then fail to convert the value to a Boolean.
-- Rejected CREATE / ALTER must leave metadata unchanged.

DROP CLUSTER IF EXISTS test_sql_cluster_05627;

CREATE CLUSTER test_sql_cluster_05627 (
    SHARD (host = '127.0.0.1', port = 9440, secure = '${tcp_port_secure}')
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05627 (
    SHARD (host = '127.0.0.1', port = 9000, compression = '${tcp_port}')
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05627 (
    SHARD (
        internal_replication = '${tcp_port}',
        REPLICA (host = '127.0.0.1', port = 9000)
    )
); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05627 (
    allow_distributed_ddl_queries = '${tcp_port}',
    SHARD (host = '127.0.0.1', port = 9000)
); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05627';

CREATE CLUSTER test_sql_cluster_05627 (
    allow_distributed_ddl_queries = 1,
    SHARD (
        internal_replication = 0,
        host = '127.0.0.1',
        port = 9000,
        secure = 0,
        compression = 1
    )
);

SELECT countIf(create_query LIKE '%${%')
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05627';

SELECT host_name, port, internal_replication
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05627'
ORDER BY shard_num, replica_num
FORMAT TSV;

ALTER CLUSTER test_sql_cluster_05627 (
    SHARD (host = '127.0.0.1', port = 9440, secure = '${tcp_port_secure}')
); -- { serverError BAD_ARGUMENTS }

ALTER CLUSTER test_sql_cluster_05627 (
    SHARD (host = '127.0.0.1', port = 9000, compression = '${tcp_port}')
); -- { serverError BAD_ARGUMENTS }

ALTER CLUSTER test_sql_cluster_05627 (
    allow_distributed_ddl_queries = '${tcp_port}',
    SHARD (host = '127.0.0.1', port = 9000)
); -- { serverError BAD_ARGUMENTS }

SELECT host_name, port, internal_replication
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05627'
ORDER BY shard_num, replica_num
FORMAT TSV;

SELECT countIf(create_query LIKE '%${%')
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05627';

DROP CLUSTER test_sql_cluster_05627;

CREATE CLUSTER test_sql_cluster_05627 (
    SHARD (host = '127.0.0.1', port = 9000, secure = 9440)
); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_05627';
