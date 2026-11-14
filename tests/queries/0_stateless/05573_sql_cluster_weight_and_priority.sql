-- Tags: no-parallel
-- no-parallel: creates and drops a global SQL-managed cluster

-- `setConfigValue` used to store every non-string as `UInt64`, so `weight = -1` and
-- `priority = -5` became `18446744073709551615` and failed later with `POCO_EXCEPTION`.
-- `weight` is a non-negative slot count; `priority` is signed and must round-trip.

DROP CLUSTER IF EXISTS test_sql_cluster_05573;

CREATE CLUSTER test_sql_cluster_05573 (SHARD (host = '127.0.0.1', port = 9000, weight = -1)); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05573 (SHARD (weight = -1, REPLICA (host = '127.0.0.1', port = 9000))); -- { serverError BAD_ARGUMENTS }

CREATE CLUSTER test_sql_cluster_05573 (SHARD (host = '127.0.0.1', port = 9000, priority = -5));

SELECT shard_weight
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05573'
ORDER BY shard_num, replica_num
LIMIT 1;

DROP CLUSTER test_sql_cluster_05573;

CREATE CLUSTER test_sql_cluster_05573 (
    SHARD (
        weight = 0,
        REPLICA (host = '127.0.0.1', port = 9000, priority = -5)
    ),
    SHARD (
        weight = 1,
        REPLICA (host = '127.0.0.2', port = 9000)
    )
);

SELECT shard_weight
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05573'
ORDER BY shard_num, replica_num
LIMIT 1;

ALTER CLUSTER test_sql_cluster_05573 (SHARD (host = '127.0.0.1', port = 9000, weight = -1)); -- { serverError BAD_ARGUMENTS }

SELECT shard_weight
FROM system.clusters
WHERE cluster = 'test_sql_cluster_05573'
ORDER BY shard_num, replica_num
LIMIT 1;

DROP CLUSTER test_sql_cluster_05573;
