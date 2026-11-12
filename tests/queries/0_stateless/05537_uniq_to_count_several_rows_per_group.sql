-- Tags: shard
-- `uniq` cannot be replaced with `count` if the subquery can have several rows with the same values of the keys, or equal rows after `DISTINCT`.

SET enable_positional_arguments = 1;
SET optimize_uniq_to_count = 1, count_distinct_optimization = 0;

SELECT 'WITH FILL';
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY 1 ORDER BY 2 WITH FILL FROM 0 TO 6);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY x ORDER BY c WITH FILL FROM 0 TO 6);

SELECT 'arrayJoin';
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, arrayJoin([1, 2]) AS y FROM numbers(6) GROUP BY 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, arrayJoin([1, 2]) AS y FROM numbers(6) GROUP BY x);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, arrayJoin(groupArray(number)) AS y FROM numbers(6) GROUP BY 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x, arrayJoin(groupArray(number)) AS y FROM numbers(6) GROUP BY x);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1 ORDER BY arrayJoin([1, 2]));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x ORDER BY arrayJoin([1, 2]));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1 HAVING arrayJoin([1, 1]) = 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x HAVING arrayJoin([1, 1]) = 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1 QUALIFY arrayJoin([1, 1]) <= row_number() OVER (PARTITION BY x));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x QUALIFY arrayJoin([1, 1]) <= row_number() OVER (PARTITION BY x));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1 ORDER BY 1 LIMIT 1 BY arrayJoin([1, 2]));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x ORDER BY x LIMIT 1 BY arrayJoin([1, 2]));
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1) WHERE arrayJoin([1, 1]) = 1;
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) WHERE arrayJoin([1, 1]) = 1;
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1) GROUP BY arrayJoin([1, 1]);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) GROUP BY arrayJoin([1, 1]);

SELECT 'arrayJoin with other constants';
SELECT uniqExact(x, y) FROM (SELECT arrayJoin(splitByChar('1', toString(number))) AS x, number AS y FROM numbers(3) GROUP BY 1, 2 ORDER BY arrayJoin(splitByChar(concat('1', ''), toString(y))));
SELECT uniqExact(x, y) FROM (SELECT arrayJoin(splitByChar('1', toString(number))) AS x, number AS y FROM numbers(3) GROUP BY x, y ORDER BY arrayJoin(splitByChar(concat('1', ''), toString(y))));

SELECT 'other arguments';
SELECT uniqExact(x, x) FROM (SELECT number % 3 AS x, number + 0 AS y FROM numbers(6) GROUP BY 1, 2) WHERE y < 10;
SELECT uniqExact(x, x) FROM (SELECT number % 3 AS x, number + 0 AS y FROM numbers(6) GROUP BY x, y) WHERE y < 10;
SELECT (SELECT uniqExact(t.x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1) AS s) FROM (SELECT toUInt8(7) AS x) AS t SETTINGS allow_experimental_correlated_subqueries = 1;
SELECT (SELECT uniqExact(t.x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) AS s) FROM (SELECT toUInt8(7) AS x) AS t SETTINGS allow_experimental_correlated_subqueries = 1;

SELECT 'other functions';
SELECT uniqCombined(15)(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1);
SELECT uniqCombined(15)(x) FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x);
SELECT uniqCombined(x) FROM (SELECT toNullable(number) AS x FROM numbers(0) GROUP BY 1);
SELECT uniqCombined(x) FROM (SELECT toNullable(number) AS x FROM numbers(0) GROUP BY x);
SELECT uniqExact(x) OVER (PARTITION BY arrayJoin([1, 1])) AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1) LIMIT 1;
SELECT uniqExact(x) OVER (PARTITION BY arrayJoin([1, 1])) AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) LIMIT 1;
SELECT uniqExact(x) OVER () AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1) GROUP BY x WITH ROLLUP LIMIT 1;
SELECT uniqExact(x) OVER () AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY x) GROUP BY x WITH ROLLUP LIMIT 1;

SELECT 'distributed_group_by_no_merge';
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 SETTINGS distributed_group_by_no_merge = 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x SETTINGS distributed_group_by_no_merge = 1);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 SETTINGS distributed_group_by_no_merge = 2);
SELECT uniqExact(x) FROM (SELECT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x SETTINGS distributed_group_by_no_merge = 2);

SELECT 'DISTINCT';
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) GROUP BY 1 ORDER BY count() WITH FILL FROM 0 TO 6);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) GROUP BY x ORDER BY count() WITH FILL FROM 0 TO 6);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) ORDER BY x + 1 WITH FILL FROM 0 TO 10);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) ORDER BY 1 LIMIT 1 BY arrayJoin([1, 2]), 1);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) ORDER BY x LIMIT 1 BY arrayJoin([1, 2]), x);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM numbers(6) ORDER BY arrayJoin([1, 2]));
SELECT uniqExact(x, y) FROM (SELECT DISTINCT arrayJoin(splitByChar('1', toString(number))) AS x, number AS y FROM numbers(3) ORDER BY 1 LIMIT 5 BY arrayJoin(splitByChar(concat('1', ''), toString(number))));
SELECT uniqExact(x, y) FROM (SELECT DISTINCT arrayJoin(splitByChar('1', toString(number))) AS x, number AS y FROM numbers(3) ORDER BY x LIMIT 5 BY arrayJoin(splitByChar(concat('1', ''), toString(number))));
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) SETTINGS distributed_group_by_no_merge = 1);
SELECT uniqExact(x) FROM (SELECT DISTINCT number % 3 AS x FROM remote('127.0.0.{2,3}', numbers(6)) SETTINGS distributed_group_by_no_merge = 2);

SELECT 'replaced';
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(y, x) AS u FROM (SELECT number % 3 AS x, number % 5 AS y FROM numbers(6) GROUP BY 1, 2));
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(x) AS u FROM (SELECT number % 3 AS x FROM numbers(6) GROUP BY 1));
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(x) AS u FROM (SELECT arrayJoin([number, 1]) AS x FROM numbers(6) GROUP BY x ORDER BY x));
SELECT countIf(explain LIKE '%uniqExact(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT uniqExact(x) AS u FROM (SELECT DISTINCT arrayJoin([number, 1]) AS x FROM numbers(6) ORDER BY x LIMIT 1 BY x));
