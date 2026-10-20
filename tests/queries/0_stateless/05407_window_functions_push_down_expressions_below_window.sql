-- The expressions of a `String` column with a small fixed-size result are computed before the window functions,
-- so the window step buffers the result instead of the column.

SET enable_analyzer = 1;
SET query_plan_push_down_expressions_below_window = 1;

SELECT 'memory';
SELECT count(), sum(v), any(n) FROM
(
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM (SELECT concat(repeat('x', 200), '@', toString(number)) AS s FROM numbers(2000000))
)
SETTINGS max_memory_usage = '150M', max_threads = 2;

-- Prints the `Window` steps and the expression in the order they appear in the plan, from the top.
SELECT 'plan';
SELECT if(explain LIKE '%Window (%', 'Window', 'toUInt64OrZero') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM (SELECT concat('p@', toString(number)) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toUInt64OrZero%';

SELECT 'between the windows and their lowest sorting';
SELECT if(explain LIKE '%Window (%', 'Window', 'toUInt64OrZero') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER (PARTITION BY k) AS n, sum(k) OVER (ORDER BY k) AS m
    FROM (SELECT number % 3 AS k, concat('p@', toString(number)) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toUInt64OrZero%';
SELECT k, v, n, m FROM
(
    SELECT k, toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER (PARTITION BY k) AS n, sum(k) OVER (ORDER BY k) AS m
    FROM (SELECT number % 3 AS k, concat('p@', toString(number)) AS s FROM numbers(10))
)
ORDER BY v;

SELECT 'a sparse column materialized by the sorting for the window';
DROP TABLE IF EXISTS t_window_push_down_byte_size;
CREATE TABLE t_window_push_down_byte_size (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO t_window_push_down_byte_size SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(10000);
SELECT b, count(), sum(n) FROM (SELECT byteSize(s) AS b, count() OVER (ORDER BY k % 31) AS n FROM t_window_push_down_byte_size)
GROUP BY b ORDER BY b SETTINGS max_threads = 1, optimize_read_in_order = 0;
SELECT b, count(), sum(n) FROM (SELECT byteSize(s) AS b, count() OVER (ORDER BY k % 31) AS n FROM t_window_push_down_byte_size)
GROUP BY b ORDER BY b SETTINGS max_threads = 1, optimize_read_in_order = 0, query_plan_push_down_expressions_below_window = 0;
DROP TABLE t_window_push_down_byte_size;

SELECT 'disabled';
SELECT if(explain LIKE '%Window (%', 'Window', 'toUInt64OrZero') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM (SELECT concat('p@', toString(number)) AS s FROM numbers(10))
    SETTINGS query_plan_push_down_expressions_below_window = 0
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toUInt64OrZero%';

-- The cases where the expression stays after the window functions.

SELECT 'the column is read after the window functions';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'length') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s, length(s) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION length%';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'length') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT length(s) AS l, count() OVER (ORDER BY s) AS n FROM (SELECT toString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION length%';

SELECT 'several results larger than the smallest string';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT length(s) AS l, cityHash64(s) AS h, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';

SELECT 'results of unbounded size';
SELECT repeat(s, 1000) AS r, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT toFixedString(s, 2048) AS r, count() OVER () AS n FROM (SELECT toString(number % 10) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';

SELECT 'results whose size of value does not bound their storage';
SELECT if(explain LIKE '%Window (%', 'Window', 'CAST') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT CAST(reinterpretAsUInt64(s), 'Variant(FixedString(1), UInt64)'), count() OVER () FROM (SELECT reinterpretAsFixedString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION CAST(%';

SELECT 'arrayJoin below the window functions';
SELECT toUInt64(s), count() OVER ()
FROM (SELECT if(number = 0, 'x', '1') AS s, arrayJoin(if(number = 0, [], [1])) AS a FROM numbers(2))
SETTINGS query_plan_lower_array_join_function = 0, max_threads = 1;

SELECT 'QUALIFY';
SELECT number, throwIf(length(s) = 3) AS t, row_number() OVER (ORDER BY number) AS rn
FROM (SELECT number, if(number = 3, 'abc', 'a') AS s FROM numbers(5)) QUALIFY rn != 4 ORDER BY number;

SELECT 'an AND condition of a filter';
SELECT count() OVER () AS n FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2))
QUALIFY s != 'x' AND toUInt64(s) > 0
SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1;
SELECT count() OVER () AS n FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2))
QUALIFY s != 'x' AND toUInt64(s) > 0
SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1, query_plan_push_down_expressions_below_window = 0;
SELECT count() OVER () AS n FROM (SELECT if(number = 0, '', 'true') AS s FROM numbers(2))
QUALIFY s != '' AND toBool(s)
SETTINGS short_circuit_function_evaluation = 'enable', query_plan_merge_filters = 1;
SELECT count() OVER () AS n FROM (SELECT if(number = 0, '', 'true') AS s FROM numbers(2))
QUALIFY s != '' AND toBool(s)
SETTINGS short_circuit_function_evaluation = 'enable', query_plan_merge_filters = 1, query_plan_push_down_expressions_below_window = 0;
SELECT v FROM (SELECT toUInt64(s) AS v, row_number() OVER () AS rn FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2)))
WHERE rn != 1 AND rn != 3
SETTINGS query_plan_split_filter = 0, query_plan_merge_filters = 1, max_threads = 1;
SELECT v FROM (SELECT toUInt64(s) AS v, row_number() OVER () AS rn FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2)))
WHERE rn != 1 AND rn != 3
SETTINGS query_plan_split_filter = 0, query_plan_merge_filters = 1, max_threads = 1, query_plan_push_down_expressions_below_window = 0;

SELECT 'lazily executed arguments of short-circuit functions';
SELECT if(count() OVER () = 0, toUInt64(s), 0) AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT multiIf(count() OVER () = 0, toUInt64(s), count() OVER () = 1, toUInt64(s) + 1, 0) AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count() OVER () = 0 AND toUInt64(s) = 1 AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count() OVER () > 0 OR toUInt64(s) = 1 AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'block-dependent functions and side effects';
SELECT number, length(s) + blockSize() AS b, count() OVER (ORDER BY number DESC) AS n
FROM (SELECT number, toString(number) AS s FROM numbers(5))
SETTINGS max_block_size = 2, max_threads = 1;
SELECT number, rowNumberInAllBlocks() + length(s) AS r, row_number() OVER (ORDER BY number DESC) AS rn
FROM (SELECT number, toString(number) AS s FROM numbers(5)) SETTINGS max_threads = 1;
SELECT length(s) + sleepEachRow(0.0001) AS b, count() OVER (ORDER BY number DESC) AS n
FROM (SELECT number, toString(number) AS s FROM (SELECT arrayJoin(range(100)) AS number))
FORMAT Null SETTINGS max_block_size = 2, max_threads = 1, function_sleep_max_microseconds_per_block = 1000;
SELECT 'ok';

SELECT 'arrayJoin';
SELECT arrayJoin([1, 2]) AS a, length(s) AS l, count() OVER () AS c FROM (SELECT toString(number) AS s FROM numbers(3)) ORDER BY a, l;

SELECT 'remote';
DROP TABLE IF EXISTS t_window_push_down;
CREATE TABLE t_window_push_down (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_push_down SELECT concat('p@', toString(number)) FROM numbers(3);
SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
FROM remote('127.0.0.{1,2}', currentDatabase(), t_window_push_down) ORDER BY v;
-- The window functions and the expressions are computed on the initiator, so the shards send the column.
SELECT trimLeft(multiIf(
    explain LIKE '%Window (%', 'Window',
    explain LIKE '%FUNCTION toUInt64OrZero%', 'toUInt64OrZero',
    explain LIKE '%ReadFromRemote%', 'ReadFromRemote',
    explain))
FROM
(
    EXPLAIN header = 1, actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_window_push_down) ORDER BY v
    SETTINGS prefer_localhost_replica = 0
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toUInt64OrZero%'
    OR explain LIKE '%ReadFromRemote%' OR explain LIKE '%Header: \\_\\_table1.s String%';
DROP TABLE t_window_push_down;
