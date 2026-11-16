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
