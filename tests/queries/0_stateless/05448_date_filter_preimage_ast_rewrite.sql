-- Expected behaviour: the `optimize_time_filter_with_preimage` setting, documented in
-- docs/reference/settings/session-settings/optimize.mdx, replaces a monotonic converter
-- applied to a Date/DateTime column with a plain range comparison on the column. The exact
-- rewrite table is the doc comment of `generateOptimizedDateFilterAST` in
-- src/Interpreters/OptimizeDateOrDateTimeConverterWithPreimageVisitor.cpp:
--   f(x) == c -> x >= b AND x <  e        f(x) != c -> x <  b OR  x >= e
--   f(x) >  c -> x >= e                   f(x) >= c -> x >= b
--   f(x) <  c -> x <  b                   f(x) <= c -> x <  e
-- `EXPLAIN AST optimize = 1` is the SQL surface that runs the AST-level `TreeOptimizer`, so it
-- shows the rewritten predicate; a plain SELECT is rewritten by the analyzer pass instead.

SET optimize_time_filter_with_preimage = 1;
SET convert_query_to_cnf = 0;
SET optimize_using_constraints = 0;
SET optimize_append_index = 0;
SET optimize_substitute_columns = 0;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_preimage_ast_rewrite;

CREATE TABLE t_preimage_ast_rewrite
(
    id UInt32,
    d Date,
    t32 DateTime('UTC'),
    t64 DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY id;

SELECT 'rewritten WHERE, Date column';
SELECT 'equals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) = 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'notEquals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) != 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'greater' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) > 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'greaterOrEquals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) >= 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'less' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) < 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'lessOrEquals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) <= 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'constant on the left, relation swapped' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE 1993 > toYear(d)) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';

SELECT 'rewritten WHERE, DateTime and DateTime64 columns';
SELECT 'DateTime equals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(t32) = 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'DateTime lessOrEquals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(t32) <= 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'DateTime64 notEquals' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(t64) != 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'DateTime64 greater' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(t64) > 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';

SELECT 'not rewritten';
SELECT 'setting off, converter kept' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(d) = 1993 SETTINGS optimize_time_filter_with_preimage = 0) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';
SELECT 'explicit time zone argument, converter kept' AS comparator, arrayStringConcat(groupArray(replaceAll(trimLeft(explain), ' (children 1)', '')), ' ') AS rewritten FROM (EXPLAIN AST optimize = 1 SELECT id FROM t_preimage_ast_rewrite WHERE toYear(t32, 'UTC') = 1993) WHERE explain LIKE '%Function %' OR explain LIKE '%Literal %';

INSERT INTO t_preimage_ast_rewrite VALUES
    (1, '1992-12-31', '1992-12-31 00:00:00', '1992-12-31 00:00:00.000'),
    (2, '1993-01-01', '1993-01-01 00:00:00', '1993-01-01 00:00:00.000'),
    (3, '1993-06-15', '1993-06-15 12:34:56', '1993-06-15 12:34:56.789'),
    (4, '1993-12-31', '1993-12-31 23:59:59', '1993-12-31 23:59:59.999'),
    (5, '1994-01-01', '1994-01-01 00:00:00', '1994-01-01 00:00:00.000'),
    (6, '1995-03-03', '1995-03-03 00:00:00', '1995-03-03 00:00:00.000');

SELECT 'row count with the rewrite';
SELECT 'equals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) = 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'notEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) != 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'greater', count() FROM t_preimage_ast_rewrite WHERE toYear(d) > 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'greaterOrEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) >= 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'less', count() FROM t_preimage_ast_rewrite WHERE toYear(d) < 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'lessOrEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) <= 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'DateTime equals', count() FROM t_preimage_ast_rewrite WHERE toYear(t32) = 1993 SETTINGS optimize_time_filter_with_preimage = 1;
SELECT 'DateTime64 equals', count() FROM t_preimage_ast_rewrite WHERE toYear(t64) = 1993 SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'row count without the rewrite';
SELECT 'equals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) = 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'notEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) != 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'greater', count() FROM t_preimage_ast_rewrite WHERE toYear(d) > 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'greaterOrEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) >= 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'less', count() FROM t_preimage_ast_rewrite WHERE toYear(d) < 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'lessOrEquals', count() FROM t_preimage_ast_rewrite WHERE toYear(d) <= 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'DateTime equals', count() FROM t_preimage_ast_rewrite WHERE toYear(t32) = 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT 'DateTime64 equals', count() FROM t_preimage_ast_rewrite WHERE toYear(t64) = 1993 SETTINGS optimize_time_filter_with_preimage = 0;

DROP TABLE t_preimage_ast_rewrite;
