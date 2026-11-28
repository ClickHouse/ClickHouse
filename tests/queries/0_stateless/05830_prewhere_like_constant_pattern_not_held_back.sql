-- `LIKE` with a constant pattern cannot throw, so it is not kept in WHERE behind the conditions written before it,
-- unlike a condition that can throw (see 05497_prewhere_keeps_guard_before_throwing_condition).

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_all_conditions_to_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET query_plan_merge_filters = 1;
SET optimize_functions_to_subcolumns = 0; -- keeps the conditions on the columns themselves
SET explain_query_plan_default = 'legacy';
SET short_circuit_function_evaluation = 'enable';
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_like_prewhere;

CREATE TABLE t_like_prewhere (a String, b String, s String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_like_prewhere VALUES ('one', '1', '2025-05-16 07:20:20'), ('two', '2', 'invaliddatetime');

-- The first condition reads every queried column, so it stays in WHERE. The `LIKE` after it is still moved.
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(length(a)) FROM t_like_prewhere WHERE sipHash64(a, b, s) != 0 AND s LIKE '%05-16%')
WHERE explain ILIKE '%Prewhere filter column: like(%s%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(length(a)) FROM t_like_prewhere WHERE sipHash64(a, b, s) != 0 AND s NOT LIKE '%valid%')
WHERE explain ILIKE '%Prewhere filter column: notLike(%s%';

-- A pattern that is not a constant is compiled for every row and can throw, so this `LIKE` waits behind the first condition.
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(length(a)) FROM t_like_prewhere WHERE sipHash64(a, b, s) != 0 AND s LIKE b)
WHERE explain ILIKE '%Prewhere filter column:%';

-- `LIKE` still guards a condition that can throw.
SELECT a FROM t_like_prewhere WHERE s LIKE '%05-16%' AND toDateTime64(s, 3) > '2020-01-01';

DROP TABLE t_like_prewhere;
