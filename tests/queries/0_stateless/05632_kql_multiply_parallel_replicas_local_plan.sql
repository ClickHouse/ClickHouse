-- `kqlMultiply` (the `*` operator of the Kusto dialect) used to build `multiply` during execution, from
-- the context it was created with. The local plan of parallel replicas is built with a temporary
-- context that is gone by then, so the query failed with the logical error `Context has expired`.

DROP TABLE IF EXISTS t_kql_multiply;
CREATE TABLE t_kql_multiply (k String, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_kql_multiply SELECT toString(number % 10), number FROM numbers(100000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1;

SELECT sum(p) FROM (SELECT kqlMultiply(100, v) AS p FROM t_kql_multiply);
SELECT k, sum(p) FROM (SELECT k, kqlMultiply(v, 2.5) AS p FROM t_kql_multiply) GROUP BY k ORDER BY k LIMIT 3;
SELECT sum(toInt64(p)) FROM (SELECT kqlMultiply(v, toIntervalSecond(2)) AS p FROM t_kql_multiply);

SET enable_parallel_replicas = 0;

SELECT kqlMultiply(toNullable(2), toIntervalSecond(3)) AS x, toTypeName(x);
SELECT kqlMultiply(toLowCardinality(2), 3) AS x, toTypeName(x);
SELECT kqlMultiply(NULL, 3) AS x, toTypeName(x);
SELECT kqlMultiply(materialize(toLowCardinality(toNullable(2))), toIntervalHour(1)) AS x, toTypeName(x);

DROP TABLE t_kql_multiply;
