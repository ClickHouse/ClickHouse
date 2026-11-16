-- Tags: shard

-- A query-wide constant (`randConstant`, `now64`) must keep one value for the whole query, even when
-- identical calls are resolved both in a local scope and in a scope over `clusterAllReplicas`.
-- Only server constants (`hostName`, ...) are separated by the scope's `isDistributed` flag.

SET enable_analyzer = 1;

SELECT uniqExact(x)
FROM
(
    SELECT randConstant() AS x
    UNION ALL
    SELECT randConstant() AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
);

SELECT uniqExact(x)
FROM
(
    SELECT randConstant() AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
    UNION ALL
    SELECT randConstant() AS x
);

SELECT uniqExact(x)
FROM
(
    SELECT randConstant() AS x
    UNION ALL
    SELECT randConstant() AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
)
SETTINGS prefer_localhost_replica = 0;

SELECT uniqExact(x)
FROM
(
    SELECT now64(9) AS x
    UNION ALL
    SELECT now64(9) AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
);

SELECT uniqExact(x)
FROM
(
    SELECT now64(9) AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
    UNION ALL
    SELECT now64(9) AS x
)
SETTINGS prefer_localhost_replica = 0;

WITH t AS (SELECT randConstant() AS x)
SELECT uniqExact(x)
FROM
(
    SELECT x FROM t
    UNION ALL
    SELECT (SELECT x FROM t) AS x
    FROM clusterAllReplicas('test_cluster_two_shards', system.one)
);
