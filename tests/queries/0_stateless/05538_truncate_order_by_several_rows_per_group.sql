-- Tags: shard
-- `ORDER BY` cannot be truncated if there can be several rows with equal values of `GROUP BY` keys.

SET optimize_truncate_order_by_after_group_by_keys = 1;

SELECT 'arrayJoin';
SELECT number % 3 AS x, row_number() OVER (PARTITION BY x) AS r, count() AS c FROM numbers(6) GROUP BY x HAVING arrayJoin([1, 1]) = count() - 1 ORDER BY x, r DESC;

SELECT '-';
SELECT countIf(explain LIKE '%count() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY x QUALIFY arrayJoin([1, 1]) = row_number() OVER (PARTITION BY x) ORDER BY x, c DESC);
SELECT '-';
SELECT number % 2 AS k, arrayJoin([1, 2]) AS j FROM numbers(4) GROUP BY k ORDER BY k, arrayMap(y -> -y, [j]);
SELECT '-';
SELECT toYear(toDate(number * 400)) AS k, arrayJoin([1, 2]) AS j FROM numbers(2) GROUP BY k ORDER BY k, j DESC, k = 1970;

SELECT 'equal values of different keys';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY x ORDER BY x, c DESC;
SELECT '-';
SELECT f, reinterpretAsUInt64(f) AS bits FROM values('f Float64', 0., -0.) GROUP BY f ORDER BY f, bits SETTINGS optimize_redundant_functions_in_order_by = 0;
SELECT f, reinterpretAsUInt64(f) AS bits FROM values('f Float64', 0., -0.) GROUP BY f ORDER BY f, bits DESC SETTINGS optimize_redundant_functions_in_order_by = 0;

SELECT '-';
SELECT arrayJoin([0., -0., 0.]) AS x, count() AS c FROM numbers(2) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT arrayJoin([0., -0., 0.]) AS x, count() AS c FROM numbers(2) GROUP BY x ORDER BY x, c DESC;

SELECT '-';
SELECT number = 0 AS x, number = 1 AS y, number = 2 AS z, toFloat64(number) AS f FROM numbers(4) GROUP BY x, y, z, f ORDER BY x, y, z, f, if(x OR y OR z, count(), 0) SETTINGS optimize_min_equality_disjunction_chain_length = 3;

SELECT 'stateful functions';
SELECT number % 3 AS x, rowNumberInAllBlocks() AS r, count() AS c FROM numbers(6) GROUP BY x ORDER BY x, r DESC FORMAT Null;
SELECT countIf(explain LIKE '%rowNumberInAllBlocks() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, rowNumberInAllBlocks() AS r, count() AS c FROM numbers(6) GROUP BY x ORDER BY x, r DESC);
SELECT countIf(explain LIKE '%count() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY x ORDER BY x, c DESC);

SELECT 'distributed_group_by_no_merge';
SET distributed_group_by_no_merge = 2;

SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c DESC;
SELECT '-';
SELECT number % 2 AS x, shardNum() AS s FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY x ORDER BY x, shardNum();
SELECT '-';
SELECT number % 2 AS x, shardNum() AS s FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY x ORDER BY x, shardNum() DESC;
SELECT number AS x FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY x ORDER BY x, count() OVER (); -- { serverError NOT_IMPLEMENTED }
SELECT '-';
SELECT f, count() AS c FROM values('f Float64', 0., -0., -0.) GROUP BY f ORDER BY f, row_number() OVER (ORDER BY count()) DESC;
SELECT f, count() AS c FROM values('f Float64', 0., -0., -0.) GROUP BY f ORDER BY f, row_number() OVER (ORDER BY count()) DESC LIMIT 1;

SELECT 'positional arguments with aliases';
SET enable_positional_arguments = 1, distributed_group_by_no_merge = 0;
SELECT (SELECT groupArray(k) FROM (SELECT toFloat64(number) AS f, sum(number * 7) AS m, m + 1 AS k FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY f ORDER BY f, 2, 3 LIMIT 2));
SELECT (SELECT groupArray(m) FROM (SELECT toFloat64(number) AS f, negate(count() AS c) AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY f ORDER BY f, 2 LIMIT 2));
SELECT (SELECT groupArray(x + m) FROM (SELECT toFloat64(number) + 1 AS x, max(x) + count() AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY x ORDER BY x, 2 LIMIT 2));
SELECT 'self join';
SELECT a.f AS f, min(a.n) AS x, min(b.n) AS y FROM values('n Int32, p Int32, f Float64', (0, 1, 0.), (1, 2, -0.), (2, 0, -0.)) AS a JOIN values('n Int32, p Int32, f Float64', (0, 1, 0.), (1, 2, -0.), (2, 0, -0.)) AS b ON a.n = b.p GROUP BY f ORDER BY f, y DESC;
SELECT 'time zones';
SELECT f, toHour(toDateTime(0, 'UTC') + sum(n) * 21600) AS h, toHour(toDateTime(0, 'Asia/Tokyo') + sum(n) * 21600) AS k FROM values('n Int32, p Int32, f Float64', (0, 1, 0.), (1, 2, -0.), (2, 0, -0.)) GROUP BY f ORDER BY f, k;
SELECT 'lambda functions, correlated subqueries';
SELECT f FROM remote('127.0.0.2', values('n Int32, f Float64', (0, 0.), (1, -0.), (2, -0.))) GROUP BY f ORDER BY f, max(arrayMap(l -> formatRowNoNewline('TSKV', l + 1 AS z), [n]))[1] = 'z=3' DESC LIMIT 1 SETTINGS serialize_query_plan = 0;
SELECT f, (SELECT count() FROM values('f Float64', 1, 2, 2) AS i WHERE i.f = o.f) + count() AS r FROM values('f Float64', 1, 2, 2) AS o GROUP BY f ORDER BY f, r SETTINGS allow_experimental_correlated_subqueries = 1;

SELECT 'parallel replicas with a custom key';
DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n AS SELECT number FROM numbers(6);
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'n', serialize_query_plan = 0;
SELECT n % 2 AS x, min(n) AS m FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t) GROUP BY x ORDER BY x, m DESC;
SELECT f, count() AS c FROM values('n UInt64, f Float64', (0, 0.), (1, -0.), (2, -0.), (3, -0.), (4, -0.), (5, -0.)) GROUP BY f ORDER BY f, row_number() OVER (ORDER BY count()) DESC;
DROP TABLE t;
