-- Tags: shard
-- Cases where `uniq` over a subquery with `GROUP BY` or `DISTINCT` cannot be replaced with `count`.

SET optimize_uniq_to_count = 1;

SELECT 'repeated argument';
SELECT uniqExact(x, x) FROM (SELECT number % 3 AS x, number + 0 AS y FROM numbers(6) GROUP BY x, y) WHERE y < 10;
SELECT uniqExact(x, x) FROM (SELECT DISTINCT number % 3 AS x, number AS y FROM numbers(6));

SELECT 'correlated column';
SELECT (SELECT uniqExact(t.x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) AS s) FROM (SELECT toUInt8(7) AS x) AS t SETTINGS allow_experimental_correlated_subqueries = 1;

SELECT 'other functions';
SELECT uniqCombined(15)(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x);
SELECT uniqCombined(x) FROM (SELECT toNullable(number) AS x FROM numbers(0) GROUP BY x);
SELECT uniqExact(x) OVER () AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) GROUP BY x WITH ROLLUP LIMIT 1;

SELECT 'distributed_group_by_no_merge';
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x SETTINGS distributed_group_by_no_merge = 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x SETTINGS distributed_group_by_no_merge = 2);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) SETTINGS distributed_group_by_no_merge = 1);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) SETTINGS distributed_group_by_no_merge = 2);

SELECT 'replaced';
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(y, x) AS u FROM (SELECT number % 3 AS x, number % 5 AS y FROM numbers(6) GROUP BY x, y));
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(x) AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x));
