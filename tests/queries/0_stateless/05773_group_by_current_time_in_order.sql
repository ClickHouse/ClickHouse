-- `GROUP BY` the current time computed for every block uses aggregation in order and emits every group as soon as the time changes.

SET enable_analyzer = 1;
SET optimize_aggregation_in_order_by_current_time = 1;

SELECT '-- plain';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT nowInBlock() AS t, count() FROM numbers(10) GROUP BY t)
WHERE explain ILIKE '%Order:%' OR explain ILIKE '%Flush on order key change%';

SELECT '-- monotonic function';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT toStartOfMinute(nowInBlock()) AS t, count() FROM numbers(10) GROUP BY t)
WHERE explain ILIKE '%Order:%';

SELECT '-- decreasing function';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT -toInt64(nowInBlock()) AS t, count() FROM numbers(10) GROUP BY t SETTINGS optimize_injective_functions_in_group_by = 0)
WHERE explain ILIKE '%Order:%';

SELECT '-- nowInBlock64';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT nowInBlock64(6, 'UTC') AS t, count() FROM numbers(10) GROUP BY t)
WHERE explain ILIKE '%Order:%';

SELECT '-- the time and other keys';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT number % 3 AS k, toStartOfSecond(nowInBlock64()) AS t, count() FROM numbers(10) GROUP BY k, t)
WHERE explain ILIKE '%Order:%';

SELECT '-- the time from a subquery';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1 SELECT t, count() FROM (SELECT number, nowInBlock() AS t FROM numbers(10) WHERE number % 2) GROUP BY t)
WHERE explain ILIKE '%Order:%';

SELECT '-- not monotonic';
SELECT count() FROM (EXPLAIN actions = 1 SELECT toSecond(nowInBlock()) AS t, count() FROM numbers(10) GROUP BY t)
WHERE explain ILIKE '%Order:%';

SELECT '-- disabled';
SELECT count() FROM (EXPLAIN actions = 1 SELECT nowInBlock() AS t, count() FROM numbers(10) GROUP BY t SETTINGS optimize_aggregation_in_order_by_current_time = 0)
WHERE explain ILIKE '%Order:%';

SELECT '-- results';
SELECT sum(c), count() = uniqExact(t)
FROM (SELECT nowInBlock() AS t, count() AS c FROM numbers_mt(100) WHERE NOT ignore(sleepEachRow(0.02)) GROUP BY t)
SETTINGS max_block_size = 1, max_threads = 4;

SELECT sum(c), count() = uniqExact(k, t)
FROM (SELECT number % 3 AS k, nowInBlock() AS t, count() AS c FROM numbers_mt(100) WHERE NOT ignore(sleepEachRow(0.02)) GROUP BY k, t)
SETTINGS max_block_size = 1, max_threads = 4;

SELECT '-- infinite input';
-- These queries finish only if a group is emitted as soon as the time changes.
-- The limits on read rows are disabled, because the input is read for up to a second before the time changes.
SELECT count() FROM (SELECT nowInBlock() AS t, count() FROM system.numbers GROUP BY t LIMIT 1)
SETTINGS max_threads = 1, max_execution_time = 300, max_rows_to_read = 0, max_bytes_to_read = 0;
SELECT count() FROM (SELECT nowInBlock() AS t, count() FROM system.numbers_mt GROUP BY t LIMIT 1)
SETTINGS max_threads = 4, max_execution_time = 300, max_rows_to_read = 0, max_bytes_to_read = 0;
SELECT count() FROM (SELECT nowInBlock() AS t, number % 10 AS k, count() FROM system.numbers_mt GROUP BY t, k LIMIT 10)
SETTINGS max_threads = 4, max_execution_time = 300, max_rows_to_read = 0, max_bytes_to_read = 0;
