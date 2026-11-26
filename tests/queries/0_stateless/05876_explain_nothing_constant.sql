-- EXPLAIN and log_query_plans print a constant holding a `Nothing` value by its name: it has no value to read (issue #134718).
SET explain_query_plan_default = 'pretty';
EXPLAIN SELECT tuple([][1]);
EXPLAIN SELECT [[][1]];
EXPLAIN SELECT map('a', [][1]);
EXPLAIN SELECT [CAST(NULL, 'Nullable(Tuple(Nothing))'), tuple([][1])];
SELECT explain FROM (EXPLAIN ANALYZE SELECT tuple([][1])) WHERE explain ILIKE '%Output:%';

-- Constants that hold a value are still printed by it, NULLs of a type reaching `Nothing` included.
EXPLAIN SELECT tuple(NULL), [], [[]];
EXPLAIN SELECT CAST(NULL, 'Nullable(Tuple(Nothing))'), [CAST(NULL, 'Nullable(Tuple(Nothing))')];

SELECT tuple([][1]) FORMAT Null SETTINGS log_query_plans = 1, log_comment = '05876_tuple_nothing';
SYSTEM FLUSH LOGS query_log;
SELECT count(), anyLast(toJSONString(query_plan)) != '{}' FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05876_tuple_nothing';
