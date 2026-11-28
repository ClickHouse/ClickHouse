-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- The cluster `secret` and passwords at the cluster, shard and replica scope are hidden in `system.clusters.create_query` by default.
-- The property names keep the spelling from the query, so mixed-case `SECRET` and `Password` are hidden too.

DROP CLUSTER IF EXISTS test_sql_cluster_05453;
DROP CLUSTER IF EXISTS test_sql_cluster_05453_case;

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

CREATE CLUSTER test_sql_cluster_05453_case (
    SECRET = 'cluster_secret_05453_case',
    user = 'default',
    Password = 'cluster_password_05453_case',
    SHARD (HOST = '127.0.0.1', PORT = 9000, Password = 'shard_password_05453_case'),
    SHARD (
        Password = 'shard_with_replicas_password_05453_case',
        REPLICA (HOST = '127.0.0.1', PORT = 9000, Password = 'replica_password_05453_case')
    )
);

SELECT count() FROM system.clusters
WHERE cluster = 'test_sql_cluster_05453_case'
  AND (create_query LIKE '%cluster_secret_05453_case%'
    OR create_query LIKE '%cluster_password_05453_case%'
    OR create_query LIKE '%shard_password_05453_case%'
    OR create_query LIKE '%shard_with_replicas_password_05453_case%'
    OR create_query LIKE '%replica_password_05453_case%');

SELECT min(create_query LIKE '%SECRET = \'[HIDDEN]\'%' AND create_query LIKE '%Password = \'[HIDDEN]\'%')
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05453_case';

SELECT host_name, port
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05453_case'
ORDER BY shard_num, replica_num;

DROP CLUSTER test_sql_cluster_05453;
DROP CLUSTER test_sql_cluster_05453_case;
