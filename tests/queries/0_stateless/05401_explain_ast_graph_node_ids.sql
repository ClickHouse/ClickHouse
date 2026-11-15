-- The graph of EXPLAIN AST numbers its nodes sequentially, so the output is the same on every run.
EXPLAIN AST graph = 1 SELECT number AS x FROM numbers(2) WHERE x > 0;
