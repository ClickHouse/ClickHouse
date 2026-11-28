-- A single tuple GROUP BY key must stay one key in the query sent to the shards.

SELECT (number % 2, number % 3) AS k, count() FROM remote('127.0.0.1,localhost', numbers(6)) GROUP BY tuple(k) ORDER BY k SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT (number % 2, number % 3) AS k, count() FROM remote('127.0.0.1,localhost', numbers(6)) GROUP BY tuple(k) WITH TOTALS ORDER BY k;

SELECT (number % 2, number % 3) AS bc, number % 4 AS m, count() FROM remote('127.0.0.1,localhost', numbers(6)) GROUP BY (bc, m) WITH ROLLUP ORDER BY ALL;

SELECT (number % 2, number % 3) AS k, count() FROM remote('127.0.0.1,localhost', numbers(6)) GROUP BY GROUPING SETS ((tuple(k)), ()) ORDER BY ALL;

SELECT if(1, tuple(a), tuple(0)) AS k, count() FROM remote('127.0.0.1,localhost', view(SELECT number % 3 AS a FROM numbers(10))) GROUP BY k ORDER BY k;
