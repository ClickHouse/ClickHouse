-- The keys of a chunk with skipped (NULL) rows of a hashed composite join key are moved to their rows
-- from the last one on, and the reverse loop must not wrap its unsigned index below zero.

SELECT count()
FROM values('a Nullable(String), b UInt64', (NULL, 1), ('x', 1)) AS l
FULL OUTER JOIN values('a Nullable(String), b UInt64', (NULL, 1), ('x', 1)) AS r
ON l.a = r.a AND l.b = r.b
SETTINGS join_algorithm = 'hash', max_threads = 1, query_plan_join_swap_table = 'false';

SELECT count()
FROM (SELECT if(number % 3 = 0, NULL, toString(number)) AS a, number AS b FROM numbers(1000)) AS l
FULL OUTER JOIN (SELECT if(number % 3 = 0, NULL, toString(number)) AS a, number AS b FROM numbers(1000)) AS r
ON l.a = r.a AND l.b = r.b
SETTINGS join_algorithm = 'hash', max_threads = 1, query_plan_join_swap_table = 'false';
