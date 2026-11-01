-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster and a user

-- String literals of `CREATE CLUSTER` must be used verbatim: `${...}` must not be substituted
-- with values from the server configuration (e.g. `tcp_port`).

DROP CLUSTER IF EXISTS test_sql_cluster_05473;
DROP USER IF EXISTS test_user_05473;

CREATE USER test_user_05473 IDENTIFIED WITH plaintext_password BY '${tcp_port}';
GRANT SELECT ON system.one TO test_user_05473;

CREATE CLUSTER test_sql_cluster_05473 (
    secret = '${tcp_port}',
    SHARD (host = '127.0.0.1', port = 9000, user = 'test_user_05473', password = '${tcp_port}', default_database = '${tcp_port}')
);

SELECT user, default_database FROM system.clusters WHERE cluster = 'test_sql_cluster_05473';

ALTER CLUSTER test_sql_cluster_05473 (
    SHARD (host = '127.0.0.1', port = 9000, user = 'test_user_05473', password = '${tcp_port}')
);

-- Connect remotely, so the password is checked.
SELECT currentUser() FROM cluster(test_sql_cluster_05473, system.one) SETTINGS prefer_localhost_replica = 0;

DROP CLUSTER test_sql_cluster_05473;
DROP USER test_user_05473;
