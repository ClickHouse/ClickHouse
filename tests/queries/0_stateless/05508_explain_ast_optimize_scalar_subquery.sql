-- EXPLAIN AST optimize = 1 must fold the real value of a scalar subquery, not a placeholder (#127032).
SELECT count() > 0 FROM (EXPLAIN AST optimize = 1 SELECT intDiv(1, (SELECT max(number) FROM numbers(5))));
SELECT countIf(explain LIKE '%Function plus%') FROM (EXPLAIN AST optimize = 1 SELECT if((SELECT max(number) FROM numbers(5)) = 4, number, number + 100) AS r FROM numbers(3));
