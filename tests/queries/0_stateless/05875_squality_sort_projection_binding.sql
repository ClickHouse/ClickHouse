SELECT (number, number + 1) IN
    (SELECT number AS a, number + 1 AS b FROM numbers(3) ORDER BY 2)
FROM numbers(3)
SETTINGS rewrite_in_to_join = 1, allow_correlated_subqueries = 1,
    transform_null_in = 0, enable_add_distinct_to_in_subqueries = 0;

SELECT * EXCEPT (number), number
FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY -1
SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;

SELECT untuple((number, number + 1)), number + 2 AS last_value
FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY -1
SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 1;

SELECT (SELECT o.number) AS k
FROM numbers(3) AS o
GROUP BY number WITH ROLLUP
ORDER BY 1
SETTINGS group_by_use_nulls = 1, allow_correlated_subqueries = 1,
    enable_positional_arguments = 1;

SELECT number + (SELECT 1) AS k
FROM remote('127.0.0.1', numbers(3))
ORDER BY 1 DESC LIMIT 2
SETTINGS enable_positional_arguments = 1,
    serialize_query_plan = 0, prefer_localhost_replica = 0;

SELECT number + (SELECT 1) AS k
FROM remote('127.0.0.1', numbers(3))
ORDER BY k DESC LIMIT 2
SETTINGS
    serialize_query_plan = 0, prefer_localhost_replica = 0;
