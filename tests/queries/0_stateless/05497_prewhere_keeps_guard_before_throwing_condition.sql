-- A condition that short-circuit `and` evaluates only on the rows passing the conditions written before it
-- (here a parse that throws on the 'two' row) must not be moved to PREWHERE in front of them.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_primary_key_columns_to_end_of_prewhere = 1; -- ranks the primary key guard `a = 'one'` last
SET enable_multiple_prewhere_read_steps = 1; -- one read step is checked at the end
SET query_plan_merge_filters = 1; -- merges the CTE filter with the outer one
SET optimize_functions_to_subcolumns = 0; -- keeps `usage_timestamp != ''` on the column itself
SET optimize_empty_string_comparisons = 0; -- keeps the `notEquals` name that the EXPLAIN check matches
SET explain_query_plan_default = 'legacy'; -- prints the PREWHERE conditions in their evaluation order
SET short_circuit_function_evaluation = 'enable';
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS t_force;
DROP TABLE IF EXISTS t_memory;

-- Both rows in one granule, so the primary key does not skip the bad row.
CREATE TABLE tbl (a String, b String, usage_timestamp String) ENGINE = MergeTree ORDER BY (a, b, usage_timestamp)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO tbl VALUES ('one', '1', '1747380020000'), ('two', '2', 'invaliddatetime');

-- The guard is in the CTE, the throwing condition in the outer query.
WITH cte AS (SELECT b, toDateTime64(usage_timestamp, 3) AS usage_timestamp FROM tbl WHERE a = 'one')
SELECT b, usage_timestamp FROM cte WHERE (b, usage_timestamp) NOT IN ('1', '2025-01-02 12:34:45.789');

-- The same in one WHERE.
SELECT b FROM tbl WHERE a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01';
SELECT count() FROM (EXPLAIN actions = 1 SELECT b FROM tbl WHERE a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01')
WHERE explain ILIKE '%Prewhere filter column: and(equals(%a%), greater(toDateTime64(%';

-- The guard reads every queried column, so it stays in WHERE, and so does the throwing condition.
SELECT a FROM tbl WHERE concat(a, usage_timestamp) LIKE 'one%' AND toDateTime64(usage_timestamp, 3) > '2020-01-01';
SELECT a FROM tbl WHERE concat(a, usage_timestamp) LIKE 'one%' AND toDateTime64(usage_timestamp, 3) > '2020-01-01'
SETTINGS allow_reorder_prewhere_conditions = 0;

-- The throwing condition is not grouped with the first condition on its column across the guard.
SELECT b FROM tbl WHERE usage_timestamp != '' AND a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01';

-- `timestamp` is executed lazily only with `force_enable`.
CREATE TABLE t_force (a String, s String) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_force VALUES ('one', '2025-05-16 07:20:20'), ('two', 'invaliddatetime');
SELECT a FROM t_force WHERE a = 'one' AND timestamp(s) > '2020-01-01' SETTINGS short_circuit_function_evaluation = 'force_enable';
-- Then the step with the guard filters its rows also when most of them pass.
INSERT INTO t_force SELECT 'one', '2025-05-16 07:20:20' FROM numbers(8);
OPTIMIZE TABLE t_force FINAL;
SELECT count() FROM t_force WHERE a = 'one' AND timestamp(s) > '2020-01-01'
SETTINGS short_circuit_function_evaluation = 'force_enable', query_plan_merge_filters = 0;

-- Memory evaluates PREWHERE in one step when it has a non-deterministic condition.
CREATE TABLE t_memory (a String, s String, x UInt64) ENGINE = Memory;
INSERT INTO t_memory VALUES ('one', '2025-05-16 07:20:20', 1), ('two', 'invaliddatetime', 2);
SELECT toDateTime64(s, 3), x FROM t_memory WHERE a = 'one' AND toDateTime64(s, 3) > '2020-01-01' AND rand(x) >= 0;

-- A condition that cannot throw is still moved in front of the primary key guard.
SELECT count() FROM (EXPLAIN actions = 1 SELECT b FROM tbl WHERE a = 'one' AND usage_timestamp != '')
WHERE explain ILIKE '%Prewhere filter column: and(notEquals(%usage_timestamp%), equals(%a%';

-- With `disable`, `and` evaluates every condition on all rows, so a later condition is still moved alone.
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(length(a)) FROM tbl WHERE sipHash64(a, b, usage_timestamp) != 0 AND usage_timestamp LIKE '17%'
SETTINGS short_circuit_function_evaluation = 'disable')
WHERE explain ILIKE '%Prewhere filter column:%usage_timestamp%';

-- One read step computes the columns that PREWHERE passes on (here the conversion the projection needs) for all rows.
SET enable_multiple_prewhere_read_steps = 0;
WITH cte AS (SELECT b, toDateTime64(usage_timestamp, 3) AS usage_timestamp FROM tbl WHERE a = 'one')
SELECT b, usage_timestamp FROM cte WHERE (b, usage_timestamp) NOT IN ('1', '2025-01-02 12:34:45.789');
SELECT toDateTime64(usage_timestamp, 3) FROM tbl WHERE a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01';
SELECT a, toDateTime64(usage_timestamp, 3) FROM tbl WHERE usage_timestamp != 'invaliddatetime' AND toDateTime64(usage_timestamp, 3) > '2020-01-01';
SELECT b, toDateTime64(usage_timestamp, 3) FROM tbl PREWHERE a = 'one' WHERE toDateTime64(usage_timestamp, 3) > '2020-01-01'
SETTINGS optimize_prewhere_after_pushdown = 1;
-- The other conditions are still moved, and so is a conversion that is not passed on.
SELECT count() FROM (EXPLAIN actions = 1 SELECT toDateTime64(usage_timestamp, 3) FROM tbl WHERE a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01')
WHERE explain ILIKE '%Prewhere filter column: equals(%a%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT toDateTime64(usage_timestamp, 3) FROM tbl PREWHERE a = 'one' WHERE b != '' AND toDateTime64(usage_timestamp, 3) > '2020-01-01'
SETTINGS optimize_prewhere_after_pushdown = 1)
WHERE explain ILIKE '%Prewhere filter column: and(equals(%a%), notEquals(%b%' AND explain NOT ILIKE '%toDateTime64%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT b FROM tbl WHERE a = 'one' AND toDateTime64(usage_timestamp, 3) > '2020-01-01')
WHERE explain ILIKE '%Prewhere filter column: and(equals(%a%), greater(toDateTime64(%';

DROP TABLE tbl;
DROP TABLE t_force;
DROP TABLE t_memory;
