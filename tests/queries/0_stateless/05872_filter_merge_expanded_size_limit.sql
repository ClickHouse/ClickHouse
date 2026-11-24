-- A filter is not merged with the expressions below it when its condition would then exceed max_expanded_ast_elements.
-- The condition of this nested `a + a` chain expands to 2^(depth + 2) + 1 nodes: 1048577 at depth 18, 16385 at depth 12.

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a17 + a17 AS a18 FROM (SELECT a16 + a16 AS a17 FROM (SELECT a15 + a15 AS a16 FROM (SELECT a14 + a14 AS a15 FROM (SELECT a13 + a13 AS a14 FROM (SELECT a12 + a12 AS a13 FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))))))))) WHERE a18 <= 0;

SELECT count() FROM (SELECT a17 + a17 AS a18 FROM (SELECT a16 + a16 AS a17 FROM (SELECT a15 + a15 AS a16 FROM (SELECT a14 + a14 AS a15 FROM (SELECT a13 + a13 AS a14 FROM (SELECT a12 + a12 AS a13 FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))))))))) WHERE a18 <= 0;

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))) WHERE a12 <= 0;

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))) WHERE a12 <= 0 SETTINGS max_expanded_ast_elements = 1000;
