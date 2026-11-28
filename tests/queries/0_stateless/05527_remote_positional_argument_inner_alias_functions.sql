-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

SELECT 'functions';
SELECT arraySum(l -> l + a, [number + 1 AS a]) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT arrayMap(l -> (l + 1 AS b) * b, [number + 1 AS a]) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT negate(number + toUInt16(1) AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT (number IN (SELECT number FROM numbers(1)) AS a) + 1 AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT (number + (1 AS a)) GLOBAL IN (SELECT 1) AS x, number FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT number + (1 GLOBAL IN (SELECT 1) AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;

SELECT 'WITH FILL';
SELECT toInt32((number % 3 AS a) * 2) AS x FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 1 WITH FILL TO 6;
SELECT (number + 0 AS a) * 2 AS x, number AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 WITH FILL TO 4 INTERPOLATE (y AS a + 10);

SELECT 'ARRAY JOIN';
SELECT negate(n + 1 AS a) AS x FROM remote('127.0.0.{2,3}', view(SELECT [1, 2] AS arr)) ARRAY JOIN arr AS n ORDER BY 1;

SELECT 'WITH';
WITH negate(number + 1 AS a) AS x SELECT x, (SELECT max(a) FROM numbers(2)) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2 SETTINGS enable_scopes_for_with_statement = 0;
WITH number + 1 AS a SELECT negate(a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS enable_scopes_for_with_statement = 0;
