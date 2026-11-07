-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_analyzer = 1, enable_positional_arguments = 1;

SELECT 'ORDER BY';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY 1;
SELECT negate(number + 1 AS a) AS x FROM numbers(3) ORDER BY 1;
SELECT DISTINCT negate(number + 1 AS a) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1 DESC;

SELECT 'GROUP BY';
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.{2,3}', numbers(5)) GROUP BY 1 ORDER BY x;

SELECT 'GROUPING SETS';
SELECT negate(number % 2 + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(5)) GROUP BY GROUPING SETS ((1), ()) ORDER BY x, s;

SELECT 'LIMIT BY';
SELECT intDiv(number + 1 AS a, 2) AS x FROM remote('127.0.0.2', numbers(5)) ORDER BY x LIMIT 1 BY 1;
