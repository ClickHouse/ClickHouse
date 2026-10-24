-- The projection expressions that do not depend on window functions are computed before the window step,
-- so the window step buffers their results instead of the columns they read.

SET enable_analyzer = 1;

SELECT count(), sum(v), any(n) FROM
(
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM (SELECT concat(repeat('x', 200), '@', toString(number)) AS s FROM numbers(2000000))
)
SETTINGS max_memory_usage = '150M', max_threads = 2;

SELECT 'plan';
SELECT if(explain LIKE '%Window (%', 'Window', 'toUInt64OrZero') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM (SELECT concat('p@', toString(number)) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toUInt64OrZero%';

SELECT 'mixed with window functions';
SELECT number, number * 2 + count() OVER () AS a, sum(number) OVER (ORDER BY number) - number AS b
FROM numbers(5) ORDER BY number;

SELECT 'arrayJoin multiplies rows after window functions';
SELECT arrayJoin([1, 2]) AS a, number * 10 AS b, count() OVER () AS c FROM numbers(3) ORDER BY a, b;
SELECT arrayJoin(range(number)) + number AS a, count() OVER () AS c FROM numbers(4) ORDER BY a;

SELECT 'stateful functions are computed after window functions';
SELECT number, rowNumberInAllBlocks() AS r, row_number() OVER (ORDER BY number DESC) AS rn
FROM numbers(5) SETTINGS max_threads = 1;

SELECT 'block-dependent functions are computed after window functions';
SELECT number, length(s) + blockSize() AS b, count() OVER (ORDER BY number DESC) AS n
FROM (SELECT number, toString(number) AS s FROM numbers(5))
SETTINGS max_block_size = 2, max_threads = 1, query_plan_enable_optimizations = 0;

SELECT 'lazily executed arguments of short-circuit functions';
SELECT if(count() OVER () = 0, toUInt64(s), 0) AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT multiIf(count() OVER () = 0, toUInt64(s), count() OVER () = 1, toUInt64(s) + 1, 0) AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count() OVER () = 0 AND toUInt64(s) = 1 AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT count() OVER () > 0 OR toUInt64(s) = 1 AS a FROM (SELECT concat('x', toString(number)) AS s FROM numbers(2)) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT 'expressions with results that can be larger than their input are computed after window functions';
SELECT repeat(s, 1000) AS r, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT toFixedString(s, 2048) AS r, count() OVER () AS n FROM (SELECT toString(number % 10) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';

SELECT 'expressions are not computed before window functions if the columns they read are not freed';
SELECT s, toUInt64OrZero(s), count() OVER () AS n FROM (SELECT toString(number % 10) AS s FROM numbers(2000000))
FORMAT Null SETTINGS max_memory_usage = '80M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT length(s), sipHash64(s), cityHash64(s), xxHash64(s), count() OVER () AS n FROM (SELECT toString(number % 10) AS s FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT s, length(t) AS h, count() OVER () AS n FROM (SELECT toString(number % 10) AS s, s AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '40M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT reinterpretAsUInt128(concat(s, t)), count() OVER () FROM (SELECT toString(number % 10) AS s, 'a' AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '40M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT s, length(t) AS h, count() OVER () AS n FROM (SELECT toString(number % 10) AS s, CAST(s AS String) AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '48M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT s, length(t) AS h, count() OVER () AS n FROM (SELECT toString(number % 10) AS s, toNullable(s) AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '48M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';
SELECT s, length(t) AS h, count() OVER () AS n FROM (SELECT toNullable(toString(number % 10)) AS s, assumeNotNull(s) AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '48M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';

SELECT 'functions with side effects are computed after window functions';
SELECT length(s) + sleepEachRow(0.0001) AS b, count() OVER (ORDER BY number DESC) AS n
FROM (SELECT number, toString(number) AS s FROM (SELECT arrayJoin(range(100)) AS number))
FORMAT Null SETTINGS max_block_size = 2, max_threads = 1, function_sleep_max_microseconds_per_block = 1000, query_plan_enable_optimizations = 0;
SELECT 'ok';

SELECT 'LIMIT';
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 10, '1', 'x') AS s FROM numbers(100))
LIMIT 1 SETTINGS max_block_size = 10, max_threads = 1;
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT number, if(number < 10, '1', 'x') AS s FROM numbers(100))
LIMIT UNTIL number = 1 SETTINGS max_block_size = 10, max_threads = 1;

SELECT 'QUALIFY';
SELECT number, throwIf(number = 3) AS t, row_number() OVER (ORDER BY number) AS rn FROM numbers(5) QUALIFY rn != 4 ORDER BY number;

SELECT 'a grouping key is an input of its own type';
-- The window step reads the `UInt32` key, not `s`, so the `UInt64` hash of it is computed after the window step.
SELECT if(explain LIKE '%Window (%', 'Window', 'sipHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT sipHash64(toUInt32(s)) AS h, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY toUInt32(s)
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION sipHash64%';
-- A `String` key is credited as the key itself.
SELECT if(explain LIKE '%Window (%', 'Window', 'length') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT length(concat(s, 'x')) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY concat(s, 'x')
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION length%';
SELECT sum(h), any(n) FROM (SELECT sipHash64(toUInt32(s)) AS h, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY toUInt32(s));
SELECT sum(l), any(n) FROM (SELECT length(concat(s, 'x')) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY concat(s, 'x'));

SELECT 'GROUP BY';
SELECT number % 3 AS k, sum(number) * 2 AS s, sum(sum(number)) OVER () AS total, toString(k) || '_' AS str
FROM numbers(10) GROUP BY k ORDER BY k;

SELECT 'lambda';
SELECT arrayMap(x -> x + number, [1, 2]) AS arr, count() OVER () AS c FROM numbers(3) ORDER BY arr;

SELECT 'IN';
SELECT number, number IN (SELECT number * 2 FROM numbers(3)) AS i, number IN (1, 3) AS j, count() OVER () AS c FROM numbers(5) ORDER BY number;

SELECT 'DISTINCT';
SELECT count() FROM (SELECT DISTINCT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n FROM (SELECT concat('p@', toString(number % 7)) AS s FROM numbers(100))) WHERE v > 0;

SELECT 'remote';
DROP TABLE IF EXISTS t_window_projection;
CREATE TABLE t_window_projection (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_projection SELECT concat('p@', toString(number)) FROM numbers(3);
SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, length(s) AS l, count() OVER () AS n, row_number() OVER (ORDER BY s) AS rn
FROM remote('127.0.0.{1,2}', currentDatabase(), t_window_projection) ORDER BY rn;
-- The expression is computed on the initiator in the step before the window step, while the shards send the column.
SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
FROM remote('127.0.0.{1,2}', currentDatabase(), t_window_projection) ORDER BY v;
SELECT trimLeft(multiIf(
    explain LIKE '%Window (%', 'Window',
    explain LIKE '%Projection before window functions%', 'Projection before window functions',
    explain LIKE '%FUNCTION toUInt64OrZero%', 'toUInt64OrZero',
    explain LIKE '%ReadFromRemote%', 'ReadFromRemote',
    explain))
FROM
(
    EXPLAIN header = 1, actions = 1, compact = 0, pretty = 0
    SELECT toUInt64OrZero(splitByString('@', s)[2]) AS v, count() OVER () AS n
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_window_projection) ORDER BY v
    SETTINGS prefer_localhost_replica = 0
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%Projection before window functions%' OR explain LIKE '%FUNCTION toUInt64OrZero%'
    OR explain LIKE '%ReadFromRemote%' OR explain LIKE '%Header: \\_\\_table1.s String%';
DROP TABLE t_window_projection;
