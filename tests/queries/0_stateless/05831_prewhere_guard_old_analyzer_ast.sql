-- The AST-based move to PREWHERE (the old analyzer with `query_plan_optimize_prewhere` disabled)
-- has no DAG nodes, so the check for a throwing condition must not dereference them.

SET enable_analyzer = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 0;

DROP TABLE IF EXISTS t_prewhere_ast;
CREATE TABLE t_prewhere_ast (id UInt64, s String, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_prewhere_ast SELECT number, if(number % 3 = 0, '', toString(number)), number % 100 FROM numbers(10000);

SELECT count() FROM t_prewhere_ast WHERE s != '' AND v > 50;

SET query_plan_enable_optimizations = 0, query_plan_optimize_prewhere = 1;
SELECT count() FROM t_prewhere_ast WHERE s != '' AND v > 50;

DROP TABLE t_prewhere_ast;
