-- Tags: shard

-- A server-constant function (`hostName`, `tcpPort`, ...) used both in the outer query and in a
-- sub-SELECT over `clusterAllReplicas` must not share one `FunctionBase` between these scopes:
-- the base captures `isDistributed` of the context it is built in, which decides constant folding.
-- Previously this failed with `THERE_IS_NO_COLUMN` (`NOT_FOUND_COLUMN_IN_BLOCK` on the remote path).

SET enable_analyzer = 1;

SELECT count(), sum(x)
FROM
(
    SELECT host, sum(x) AS x
    FROM
    (
        SELECT hostName() AS host, 0 AS x
        UNION ALL
        SELECT hostName() AS host, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY host
    )
    GROUP BY host
);

SELECT count(), sum(x)
FROM
(
    SELECT host, sum(x) AS x
    FROM
    (
        SELECT hostName() AS host, 0 AS x
        UNION ALL
        SELECT hostName() AS host, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY host
    )
    GROUP BY host
)
SETTINGS prefer_localhost_replica = 0;

SELECT count(), sum(x)
FROM
(
    SELECT port, sum(x) AS x
    FROM
    (
        SELECT tcpPort() AS port, 0 AS x
        UNION ALL
        SELECT tcpPort() AS port, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY port
    )
    GROUP BY port
);

SELECT count(), sum(x)
FROM
(
    SELECT port, sum(x) AS x
    FROM
    (
        SELECT tcpPort() AS port, 0 AS x
        UNION ALL
        SELECT tcpPort() AS port, count() AS x
        FROM clusterAllReplicas('test_cluster_two_shards', system.one)
        GROUP BY port
    )
    GROUP BY port
)
SETTINGS prefer_localhost_replica = 0;

-- A query-wide constant (`randConstant`, `now64`) must keep one value for the whole query, even when
-- identical calls are resolved both in a local scope and in a scope over `clusterAllReplicas`.
-- Only server constants are separated by the scope's `isDistributed` flag.

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
