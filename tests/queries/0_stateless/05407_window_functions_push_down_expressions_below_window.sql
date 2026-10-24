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

SELECT 'below the sorting for the window';
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

SELECT 'a column that may share memory with a column read after the window functions';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'length') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s, length(t) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s, CAST(s AS String) AS t FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION length%';
SELECT s, length(t) AS h, count() OVER () AS n FROM (SELECT toString(number % 10) AS s, CAST(s AS String) AS t FROM numbers(1000000))
FORMAT Null SETTINGS max_memory_usage = '48M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';

SELECT 'results of unbounded size';
SELECT repeat(s, 1000) AS r, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT toFixedString(s, 2048) AS r, count() OVER () AS n FROM (SELECT toString(number % 10) AS s FROM numbers(100000))
FORMAT Null SETTINGS max_memory_usage = '50M', max_block_size = 1024, max_threads = 1;
SELECT 'ok';

SELECT 'a fixed-size grouping key';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'sipHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT sipHash64(toUInt32(s)) AS h, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY toUInt32(s)
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION sipHash64%';
SELECT 'a String grouping key';
SELECT multiIf(explain LIKE '%Window (%', 'Window', 'length') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT length(concat(s, 'x')) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY concat(s, 'x')
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION length%';
SELECT sum(l), any(n) FROM (SELECT length(concat(s, 'x')) AS l, count() OVER () AS n FROM (SELECT toString(number) AS s FROM numbers(10)) GROUP BY concat(s, 'x'));

SELECT 'LIMIT';
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 10, '1', 'x') AS s FROM numbers(100))
LIMIT 1 SETTINGS max_block_size = 10, max_threads = 1;
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT number, if(number < 10, '1', 'x') AS s FROM numbers(100))
LIMIT UNTIL number = 1 SETTINGS max_block_size = 10, max_threads = 1;
SELECT * FROM (SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 10, '1', 'x') AS s FROM numbers(100)))
LIMIT 1 SETTINGS max_block_size = 10, max_threads = 1;

SELECT 'LIMIT BY with a constant key';
SELECT toUInt64(s), count() OVER () FROM (SELECT if(number < 1000, '1', 'x') AS s FROM numbers(10000))
LIMIT 1 BY 'group' SETTINGS max_block_size = 10, max_threads = 1;

SELECT 'INTERSECT with an empty side';
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, '1', 'x') AS s FROM numbers(10000))
INTERSECT ALL SELECT number AS v, number AS n FROM numbers(0)
SETTINGS max_threads = 1, max_block_size = 10;
SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, '1', 'x') AS s FROM numbers(10000))
INTERSECT DISTINCT SELECT number AS v, number AS n FROM numbers(0)
SETTINGS max_threads = 1, max_block_size = 10;
SELECT 'ok';

SELECT 'a column that may share memory with a default value of another column';
DROP TABLE IF EXISTS t_window_push_down_default;
CREATE TABLE t_window_push_down_default (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_window_push_down_default SELECT toString(number % 10) FROM numbers(10);
ALTER TABLE t_window_push_down_default ADD COLUMN t String DEFAULT s;
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s, cityHash64(t), count() OVER () FROM t_window_push_down_default
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_default;
CREATE TABLE t_window_push_down_default (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_window_push_down_default SELECT toString(number % 10) FROM numbers(10);
ALTER TABLE t_window_push_down_default ADD COLUMN u String DEFAULT s;
ALTER TABLE t_window_push_down_default ADD COLUMN v String DEFAULT u;
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT v, cityHash64(s), count() OVER () FROM t_window_push_down_default
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_default;

SELECT 'overflow mode break';
SELECT v, any(n) FROM (SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, toString(number), 'x') AS s FROM numbers(10000)))
GROUP BY v FORMAT Null
SETTINGS max_threads = 1, max_block_size = 10, max_rows_to_group_by = 1, group_by_overflow_mode = 'break';
SELECT DISTINCT v, n FROM (SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, toString(number), 'x') AS s FROM numbers(10000)))
FORMAT Null
SETTINGS max_threads = 1, max_block_size = 10, max_rows_in_distinct = 1, distinct_overflow_mode = 'break';
SELECT 'ok';

SELECT 'overflow mode break of a subquery';
SELECT sum(v), sum(n) FROM
(
    SELECT v, any(n) AS n FROM (SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, toString(number), 'x') AS s FROM numbers(10000)))
    GROUP BY v
    SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break'
)
FORMAT Null
SETTINGS max_threads = 1, max_block_size = 10;
SELECT count() FROM
(
    SELECT DISTINCT v, n FROM (SELECT toUInt64(s) AS v, count() OVER () AS n FROM (SELECT if(number < 1000, toString(number), 'x') AS s FROM numbers(10000)))
    SETTINGS max_rows_in_distinct = 1, distinct_overflow_mode = 'break'
)
FORMAT Null
SETTINGS max_threads = 1, max_block_size = 10;
SELECT 'ok';

SELECT 'results whose size of value does not bound their storage';
SELECT if(explain LIKE '%Window (%', 'Window', 'toLowCardinality') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toLowCardinality(s), count() OVER () FROM (SELECT reinterpretAsFixedString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toLowCardinality%';
SELECT if(explain LIKE '%Window (%', 'Window', 'CAST') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT CAST(reinterpretAsUInt64(s), 'Variant(FixedString(1), UInt64)'), count() OVER () FROM (SELECT reinterpretAsFixedString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION CAST(%';

SELECT if(explain LIKE '%Window (%', 'Window', 'toLowCardinality') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT toLowCardinality(reinterpretAsUInt64(s)), count() OVER () FROM (SELECT reinterpretAsFixedString(number) AS s FROM numbers(10))
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION toLowCardinality%';

SELECT 'a sparse column';
DROP TABLE IF EXISTS t_window_push_down_sparse;
CREATE TABLE t_window_push_down_sparse (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO t_window_push_down_sparse SELECT if(number % 1000 = 0, toString(number), '') FROM numbers(10000);
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(s), count() OVER () FROM t_window_push_down_sparse
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(t), count() OVER () FROM (SELECT toFixedString(s, 8) AS t FROM t_window_push_down_sparse)
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_sparse;

SELECT 'a sparse subcolumn';
DROP TABLE IF EXISTS t_window_push_down_sparse_tuple;
CREATE TABLE t_window_push_down_sparse_tuple (t Tuple(s String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO t_window_push_down_sparse_tuple SELECT tuple(if(number % 1000 = 0, toString(number), '')) FROM numbers(10000);
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(t.s), count() OVER () FROM t_window_push_down_sparse_tuple
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_sparse_tuple;

SELECT 'a sparse column with a pending rename';
DROP TABLE IF EXISTS t_window_push_down_sparse_rename;
CREATE TABLE t_window_push_down_sparse_rename (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
INSERT INTO t_window_push_down_sparse_rename SELECT if(number % 1000 = 0, toString(number), '') FROM numbers(10000);
SYSTEM STOP MERGES t_window_push_down_sparse_rename;
ALTER TABLE t_window_push_down_sparse_rename RENAME COLUMN s TO t SETTINGS alter_sync = 0;
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(t), count() OVER () FROM t_window_push_down_sparse_rename
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_sparse_rename;

SELECT 'a column changed by a mutation on the fly';
DROP TABLE IF EXISTS t_window_push_down_mutation;
CREATE TABLE t_window_push_down_mutation (s String, t String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_window_push_down_mutation SELECT toString(number % 10), '0' FROM numbers(10);
SYSTEM STOP MERGES t_window_push_down_mutation;
ALTER TABLE t_window_push_down_mutation UPDATE t = s WHERE 1 SETTINGS mutations_sync = 0;
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT s, cityHash64(t), count() OVER () FROM t_window_push_down_mutation SETTINGS apply_mutations_on_fly = 1
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_mutation;

SELECT 'arrayJoin below the window functions';
SELECT toUInt64(s), count() OVER ()
FROM (SELECT if(number = 0, 'x', '1') AS s, arrayJoin(if(number = 0, [], [1])) AS a FROM numbers(2))
SETTINGS query_plan_lower_array_join_function = 0, max_threads = 1;

SELECT 'a column replicated lazily by arrayJoin';
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(s), count() OVER () FROM (SELECT toString(number) AS s, arrayJoin(range(10000)) AS a FROM numbers(500))
    SETTINGS query_plan_lower_array_join_function = 0
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';

SELECT 'a table that keeps its columns in memory';
DROP TABLE IF EXISTS t_window_push_down_memory;
CREATE TABLE t_window_push_down_memory (s String) ENGINE = Memory;
INSERT INTO t_window_push_down_memory SELECT toString(number) FROM numbers(10);
SELECT if(explain LIKE '%Window (%', 'Window', 'cityHash64') FROM
(
    EXPLAIN actions = 1, compact = 0, pretty = 0
    SELECT cityHash64(s), count() OVER () FROM t_window_push_down_memory
)
WHERE explain LIKE '%Window (%' OR explain LIKE '%FUNCTION cityHash64%';
DROP TABLE t_window_push_down_memory;

SELECT 'QUALIFY';
SELECT number, throwIf(length(s) = 3) AS t, row_number() OVER (ORDER BY number) AS rn
FROM (SELECT number, if(number = 3, 'abc', 'a') AS s FROM numbers(5)) QUALIFY rn != 4 ORDER BY number;

SELECT 'an AND condition of a filter with short-circuit evaluation disabled';
SELECT count() OVER () AS n FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2))
QUALIFY s != 'x' AND toUInt64(s) > 0
SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1;
SELECT count() OVER () AS n FROM (SELECT if(number = 0, 'x', '1') AS s FROM numbers(2))
QUALIFY s != 'x' AND toUInt64(s) > 0
SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1, query_plan_push_down_expressions_below_window = 0;

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
-- The columns received from other servers may be sparse, so the expression stays after the window functions,
-- and the shards send the column.
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
