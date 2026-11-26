-- `arrayElement` of a constant array with a non-constant index gathers the rows with `index`, from the elements of the
-- constant array, or from the elements followed by a default value when some indices are out of range.

SELECT 'small array, mixed';
SELECT [[1], [2, 3], []][number % 5 - 1], arrayElementOrNull([[1], [2, 3], []], number % 5 - 1) FROM numbers(6);
SELECT ['a', 'bb'][-number], [map('k', 1), map()][number + 1], [(1, 'x'), (2, 'y')][number] FROM numbers(4);

SELECT 'small array, all in range and all out of range';
SELECT [10, 20, 30][number % 3 + 1], [[1], [2]][number % 2 + 1] FROM numbers(4);
SELECT [[1], [2]][number + 3], ['a', 'b'][number + 3], arrayElementOrNull([[1], [2]], number + 3) FROM numbers(2);

-- A constant array larger than the positions of a block: only the selected elements are copied.
SELECT 'large array';
SELECT arrayMap(x -> toString(x), range(100000))[number % 7 - 2] FROM numbers(7) SETTINGS max_block_size = 3;
SELECT sum(cityHash64(arrayMap(x -> toString(x), range(100000))[number % 7 - 2])) FROM numbers(100000) SETTINGS max_block_size = 1000;
SELECT sum(cityHash64(arrayMap(x -> toString(x), range(100000))[number % 7 + 1])) FROM numbers(100000) SETTINGS max_block_size = 1000;
SELECT arrayElementOrNull(arrayMap(x -> [x], range(50000)), toInt64(number) - 2) FROM numbers(5) SETTINGS max_block_size = 2;

-- `replicate` and `index` of arrays of arrays: the elements of short arrays are gathered with `index`, long arrays are
-- copied row by row without an index of every element.
SELECT 'replicate and index';
SELECT materialize([[1, 2], [3]]) FROM numbers(3);
SELECT x, a FROM (SELECT [range(number), [number]] AS a, range(number % 3) AS r FROM numbers(5)) ARRAY JOIN r AS x ORDER BY x, a;
SELECT sum(cityHash64(toString(a))) FROM (SELECT [range(number % 100), [number]] AS a, range(number % 3) AS r FROM numbers(5000)) ARRAY JOIN r AS x;
SELECT sum(cityHash64(toString(a))) FROM (SELECT [range(number % 40)] AS a FROM numbers(20000) ORDER BY cityHash64(number));
SELECT count() FROM (SELECT materialize([range(100000)::Array(UInt8)]) AS a FROM numbers(128)) WHERE NOT ignore(a)
SETTINGS max_memory_usage = 50000000, max_block_size = 128;

-- More than 65536 nested elements per block, so the gather crosses chunk boundaries; one row is longer than a chunk.
-- Each result is compared with the same value computed without replicating or reordering the rows.
SELECT 'chunks';
SET max_block_size = 1000000, enable_lazy_columns_replication = 0, query_plan_remove_redundant_sorting = 0;
DROP TABLE IF EXISTS t_array_gather_chunks;
CREATE TABLE t_array_gather_chunks (n UInt64, a Array(Array(UInt64)), r Array(UInt8)) ENGINE = Memory;
INSERT INTO t_array_gather_chunks
SELECT number, if(number = 2, arrayMap(x -> [x], range(70000)), [range(number % 5), [number]]), range(number % 3) FROM numbers(100000);
SELECT (SELECT sum(cityHash64(n, toString(a))) FROM t_array_gather_chunks ARRAY JOIN r)
    = (SELECT sum(cityHash64(n, toString(a)) * length(r)) FROM t_array_gather_chunks);
SELECT (SELECT sum(cityHash64(n, toString(a))) FROM (SELECT n, a FROM t_array_gather_chunks ORDER BY cityHash64(n)))
    = (SELECT sum(cityHash64(n, toString(a))) FROM t_array_gather_chunks);
DROP TABLE t_array_gather_chunks;
SELECT sum(cityHash64(toString(materialize([[1, 2], [], [3]])))) = 100000 * cityHash64(toString([[1, 2], [], [3]])) FROM numbers(100000);

-- The total number of gathered elements overflows: empty tuples take no memory, so the arrays can be that large.
SELECT length(arrayFlatten(replicate(b, arrayWithConstant(32768 + number, tuple()))))
FROM
(
    SELECT number, arrayFlatten(replicate(a, arrayWithConstant(1048576 + number, tuple()))) AS b
    FROM (SELECT number, arrayWithConstant(536870912 + number, tuple()) AS a FROM numbers(1))
)
SETTINGS max_threads = 1, max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }

-- The same overflow in the size of an inner array, when the gathered chunks or rows are appended.
SELECT length(z), length(z[1]), length(z[131073])
FROM
(
    SELECT replicate([b], arrayWithConstant(131073 + number, tuple())) AS z
    FROM
    (
        SELECT number, arrayFlatten(replicate(a, arrayWithConstant(524288 + number, tuple()))) AS b
        FROM (SELECT number, arrayWithConstant(268435456 + number, tuple()) AS a FROM numbers(1))
    )
)
SETTINGS max_threads = 1, max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT length(z)
FROM
(
    SELECT replicate(replicate(b, arrayWithConstant(17 + number, tuple())), arrayWithConstant(7711 + number, tuple())) AS z
    FROM
    (
        SELECT number, arrayFlatten(replicate(a, arrayWithConstant(524288 + number, tuple()))) AS b
        FROM (SELECT number, arrayWithConstant(268435456 + number, tuple()) AS a FROM numbers(1))
    )
)
SETTINGS max_threads = 1, max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Replication gathers the rows without an index of every replicated row.
SELECT sum(length(a)) FROM (SELECT materialize([[]::Array(UInt8)]) AS a FROM numbers(2000000))
SETTINGS max_threads = 1, max_block_size = 2000000, max_memory_usage = 70000000;
