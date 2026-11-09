-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- An omitted `port` is resolved from `tcp_port` of the server running the DDL and stored
-- explicitly, so that servers sharing the metadata connect to the same port.
--
-- `secure = 1` without `port` would default to `tcp_port_secure`. Fast test servers usually do
-- not listen on that port, and `getServerPort('tcp_port_secure')` then throws
-- `CLUSTER_DOESNT_EXIST`, so this test does not cover the secure default.

DROP CLUSTER IF EXISTS test_sql_cluster_05574;

CREATE CLUSTER test_sql_cluster_05574 (
    SHARD (host = '127.0.0.1'),
    SHARD (host = '127.0.0.2'),
    SHARD (REPLICA (host = '127.0.0.3'), REPLICA (host = '127.0.0.4'), REPLICA (host = '127.0.0.5', port = 1234))
);

SELECT replace(DISTINCT_query, toString(getServerPort('tcp_port')), '<tcp_port>')
FROM (SELECT DISTINCT create_query AS DISTINCT_query FROM system.clusters WHERE cluster = 'test_sql_cluster_05574');

SELECT shard_num, replica_num, host_name, port = getServerPort('tcp_port')
FROM system.clusters WHERE cluster = 'test_sql_cluster_05574' ORDER BY shard_num, replica_num;

ALTER CLUSTER test_sql_cluster_05574 (SHARD (REPLICA (host = '127.0.0.6')));

SELECT replace(DISTINCT_query, toString(getServerPort('tcp_port')), '<tcp_port>')
FROM (SELECT DISTINCT create_query AS DISTINCT_query FROM system.clusters WHERE cluster = 'test_sql_cluster_05574');

DROP CLUSTER test_sql_cluster_05574;
