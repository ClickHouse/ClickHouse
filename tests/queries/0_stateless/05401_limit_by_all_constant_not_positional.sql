-- `LIMIT n BY ALL` must not read an integer constant of the SELECT list as a positional argument.

SET enable_positional_arguments = 1;

SELECT 5, number FROM numbers(3) ORDER BY number LIMIT 1 BY ALL;

-- `3` is not position 3 (`count()`, which `ALL` excludes).
SELECT number % 2 AS k, 3, count() AS c FROM numbers(10) GROUP BY k, number % 3 ORDER BY k, c LIMIT 1 BY ALL;

SELECT 5, number FROM remote('127.0.0.{1,2}', numbers(3)) ORDER BY number LIMIT 1 BY ALL;
