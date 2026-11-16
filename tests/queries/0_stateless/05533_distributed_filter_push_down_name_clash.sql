-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/130604
-- A filter pushed down to the shards must mean the same columns as on the initiator,
-- whatever aliases and lambda arguments the subquery has.

SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
-- The shards fail unless they get a condition on `k`.
SET force_primary_key = 1;

DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS d_k;
DROP TABLE IF EXISTS t;
CREATE TABLE t (number UInt64, k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t SELECT number, number + 1 FROM numbers(3);

SELECT 'alias';
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY k) WHERE y = 3;
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t) WHERE y = 3;
SELECT k FROM (SELECT k + 1 AS k FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k) WHERE k = 2;
SELECT k FROM (SELECT DISTINCT t.number AS k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t) WHERE y GLOBAL IN (SELECT 3);
SELECT k FROM (SELECT t.k AS y, sum(number) AS k FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t GROUP BY y) WHERE y = 3
SETTINGS distributed_group_by_no_merge = 2;
SELECT k FROM (SELECT k, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ARRAY JOIN [10] AS k ORDER BY k) WHERE y = 3;
SELECT y FROM (SELECT DISTINCT number AS `__table1.k`, k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t)) WHERE y = 3;

SELECT 'lambda argument';
SELECT count() FROM (SELECT arrayMap(k -> k + t.k, [number]) AS x, t.k AS y FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t ORDER BY x)
WHERE x = [3] AND y > 1;

SELECT 'table function';
SELECT * FROM (SELECT t.number + 1 AS number, t.number AS y FROM remote('127.0.0.{2,3}', numbers(5)) AS t ORDER BY y) WHERE y = 3;

SELECT 'prefer_column_name_to_alias';
SELECT number FROM (SELECT t.k AS number FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS t) WHERE number = 3
SETTINGS prefer_column_name_to_alias = 1;

CREATE TABLE d AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
CREATE VIEW v AS SELECT d.k AS a, d.number AS b FROM d;
SELECT b FROM (SELECT q.a AS b FROM v AS q) WHERE b = 3
SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, optimize_trivial_view_pushdown_to_distributed = 1;
-- The table on the shards has a column that the `Distributed` table does not have.
CREATE TABLE d_k (k UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
SELECT number FROM (SELECT k AS number FROM d_k) WHERE number = 3 SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0;
SELECT number FROM (SELECT k + 1 AS number FROM d_k) WHERE number = 4 SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0;
SELECT k FROM (SELECT k, max(k) AS number FROM d_k GROUP BY k) WHERE number = 3 AND k > 1
SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, distributed_group_by_no_merge = 1;
-- The expression inside the filter would exceed the limit on the shards.
SELECT number FROM (SELECT k + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS number FROM d_k) WHERE number + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 = 19
SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, max_ast_depth = 32;
SELECT number FROM (SELECT k + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS number FROM d_k) WHERE number + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 = 19
SETTINGS prefer_column_name_to_alias = 0, prefer_localhost_replica = 0, max_ast_depth = 32;
-- The repeated expression would exceed the limit on the shards.
SELECT count() FROM (SELECT k, (k, [k]) IN ((1, [1]), (2, [2]), (3, [3]), (4, [4]), (5, [5]), (6, [6]), (7, [7]), (8, [8]), (9, [9])) AS x FROM d_k)
WHERE k > 1 AND x SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, max_ast_elements = 130;
SELECT count() FROM (SELECT k, (number + 1) * (number + 2) * (number + 3) * (number + 4) * (number + 5) AS x FROM remote('127.0.0.{2,3}', currentDatabase(), t))
WHERE k > 1 AND x != 1 AND x > 2 AND x % 2 = 0 AND x % 3 = 0 AND x % 4 = 0 AND x % 5 = 0 AND x % 6 = 0 AND x % 8 = 0
SETTINGS prefer_column_name_to_alias = 1, max_ast_elements = 200;
SELECT x FROM (SELECT k, arrayMap(v -> formatRowNoNewline('TSKV', v + number AS a), [1]) AS x FROM remote('127.0.0.{2,3}', currentDatabase(), t))
WHERE x = ['a=3'] AND k > 1 SETTINGS prefer_column_name_to_alias = 1;
-- A long literal is one element.
SELECT count() FROM (SELECT k IN (3, 1000000001, 1000000002, 1000000003, 1000000004, 1000000005, 1000000006, 1000000007, 1000000008) AS number FROM d_k)
WHERE number SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, max_ast_elements = 130;
-- The text for the shards is too deep for the parser, but it is not sent.
SET max_parser_depth = 100;
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT x FROM
    (WITH k + 1 + 1 + 1 AS a, a + 1 + 1 + 1 AS b, b + 1 + 1 + 1 AS c, c + 1 + 1 + 1 AS d, d + 1 + 1 + 1 AS e, e + 1 + 1 + 1 AS f, f + 1 + 1 + 1 AS g, g + 1 + 1 + 1 AS h, h + 1 + 1 + 1 AS i, i + 1 + 1 + 1 AS j SELECT j + 1 AS x FROM d_k)
    WHERE x = 34 SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0);
SET max_parser_depth = DEFAULT;
-- A copy of `randConstant` has another value on old servers.
SELECT countIf(explain LIKE '%Condition: (k in%') FROM (EXPLAIN indexes = 1, distributed = 1
    SELECT k FROM (SELECT k, randConstant(shardNum()) AS x FROM d_k) WHERE k = x
    SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0, force_primary_key = 0);

SELECT 'functions that use names of arguments';
SELECT k FROM (SELECT k FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k) WHERE formatRowNoNewline('TSKV', k) = 'k=3' AND k > 1;
SELECT k FROM (SELECT k FROM remote('127.0.0.{2,3}', currentDatabase(), t)) WHERE formatRowNoNewline('TSKV', k AS z) = 'z=3' AND k > 1;
SELECT x FROM (SELECT DISTINCT k, arrayMap(v -> formatRowNoNewline('TSKV', v + number AS a), [1]) AS x FROM remote('127.0.0.{2,3}', currentDatabase(), t))
WHERE x = ['a=3'] AND k > 1;
SET enable_named_columns_in_function_tuple = 1;
SELECT k FROM (SELECT k, number FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k)
WHERE CAST((k, number), 'Tuple(number UInt64, k UInt64)').k = 3 AND k > 1;
SELECT k FROM (SELECT k, number FROM remote('127.0.0.{2,3}', currentDatabase(), t)) WHERE toJSONString([(k AS a, number AS b)]) LIKE '[{"a":%3%' AND k > 1;
SELECT k FROM (SELECT k, number FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k)
WHERE (k, number) IN (SELECT CAST((2, 3), 'Tuple(number UInt64, k UInt64)')) AND k > 1;
SELECT k FROM (SELECT k, number FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k) WHERE (k, number) IN ((3, 2), (9, 9));
SELECT k FROM (SELECT k, number FROM remote('127.0.0.{2,3}', currentDatabase(), t)) WHERE has([(3, 2)], (k, number));
SELECT number FROM (SELECT k, (k AS a, k + 1 AS b) AS number FROM d_k) WHERE number.a = 3 AND k > 1
SETTINGS prefer_column_name_to_alias = 1, prefer_localhost_replica = 0;
SELECT x FROM (SELECT k, (k AS a, k + 1 AS b) AS x FROM d_k) WHERE toJSONString(x) LIKE '%"b":%4%' AND k > 1
SETTINGS prefer_column_name_to_alias = 0, prefer_localhost_replica = 0;
SET enable_named_columns_in_function_tuple = 0;

SELECT 'expressions that are rewritten for the shards';
WITH c AS (SELECT 2) SELECT k FROM (SELECT k, number IN c AS f FROM remote('127.0.0.{2,3}', currentDatabase(), t) ORDER BY k) WHERE f AND k > 1;
SELECT g FROM (SELECT k, grouping(k) AS g FROM remote('127.0.0.{2,3}', currentDatabase(), t) WHERE k > 1 GROUP BY GROUPING SETS ((k), ())) WHERE g = 1
SETTINGS distributed_group_by_no_merge = 2;

SELECT 'parallel replicas';
SELECT k FROM (SELECT t.number AS k, t.k AS y FROM t ORDER BY k) WHERE y = 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 0, parallel_replicas_filter_pushdown = 1, force_primary_key = 0;

DROP VIEW v;
DROP TABLE d;
DROP TABLE d_k;
DROP TABLE t;
