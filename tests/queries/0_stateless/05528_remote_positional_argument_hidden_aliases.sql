-- Tags: shard
-- The other aliases of a positional argument are not sent to a remote server, but query tree passes still see them.

SET enable_positional_arguments = 1;

SELECT 'different definitions of an alias in a lambda function';
SELECT countSubstrings(extract(explain, 'ORDER BY .*'), ' AS q')
FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 WITH x -> (x + 1 AS q) * q AS f SELECT (number IN (SELECT 1)) + 1 AS k, f(k) FROM numbers(2) ORDER BY 2);

SELECT 'grouping';
SELECT abs(number % 2 AS a) AS x FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY 1 WITH ROLLUP HAVING grouping(x) = 0 ORDER BY x;

SELECT 'a GROUP BY key under an aggregate function';
SELECT toYear(d) AS y, any(y = 2024) FROM remote('127.0.0.{1,2}', view(SELECT toDate('2023-12-31') + number AS d FROM numbers(2))) GROUP BY 1 ORDER BY 1;
SELECT number + 1 AS x, any(arrayJoin([x, x])) FROM remote('127.0.0.{1,2}', numbers(1)) GROUP BY 1;
SELECT length(arrayFilter(v -> v > number, [1])) AS x, any(arrayJoin([x, x])) FROM remote('127.0.0.{1,2}', numbers(1)) GROUP BY 1;
SELECT uniqExact(x) FROM (SELECT number + 1 AS x FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY 1) SETTINGS distributed_group_by_no_merge = 1;

SELECT 'custom key';
DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t SELECT number FROM numbers(3);
-- The custom key refers to the column, not to the alias.
SELECT t.n % 2 AS x, max(intDiv(t.n, 2) AS n) AS y, count() FROM cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), t) AS t GROUP BY x ORDER BY 2, 1
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'n % 2', prefer_localhost_replica = 0;
DROP TABLE t;
