-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- A later scope replaces an earlier property of the same name even when the spelling differs.
-- Replica overrides shard, and shard overrides cluster. The stored statement keeps the original spelling.

DROP CLUSTER IF EXISTS test_sql_cluster_05838;

CREATE CLUSTER test_sql_cluster_05838 (
    user = 'cluster_user',
    password = 'cluster_password',
    SHARD (
        USER = 'shard_user',
        PASSWORD = 'shard_password',
        REPLICA (host = '127.0.0.1', port = 9000, user = 'replica_user', Password = 'replica_password'),
        REPLICA (host = '127.0.0.2', port = 9000)
    ),
    SHARD (HOST = '127.0.0.3', PORT = 9000, user = 'inline_user', password = 'inline_password')
);

SELECT shard_num, replica_num, host_name, user
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05838'
ORDER BY shard_num, replica_num;

SELECT min(create_query LIKE '%user = \'cluster_user\'%'
    AND create_query LIKE '%USER = \'shard_user\'%'
    AND create_query LIKE '%user = \'replica_user\'%'
    AND create_query LIKE '%Password = \'[HIDDEN]\'%')
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05838';

ALTER CLUSTER test_sql_cluster_05838 (
    USER = 'cluster_user_2',
    SHARD (
        user = 'shard_user_2',
        REPLICA (HOST = '127.0.0.4', PORT = 9000, USER = 'replica_user_2')
    )
);

SELECT shard_num, replica_num, host_name, user
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05838'
ORDER BY shard_num, replica_num;

SELECT min(create_query LIKE '%USER = \'cluster_user_2\'%'
    AND create_query LIKE '%user = \'shard_user_2\'%'
    AND create_query LIKE '%USER = \'replica_user_2\'%')
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05838';

DROP CLUSTER test_sql_cluster_05838;
