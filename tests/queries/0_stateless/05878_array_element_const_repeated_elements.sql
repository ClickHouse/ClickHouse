-- `arrayElement` of a large constant array with some indices out of range inserts the rows one by one, so the selected
-- elements are not copied before the result is built.
SET max_block_size = 64, max_memory_usage = 110000000;

SELECT sum(length([repeat('x', 1000000)][if(number = 0, 2, 1)])) FROM numbers(64);
SELECT sum(length(arrayElementOrNull([repeat('x', 1000000)], if(number = 0, 2, 1)))) FROM numbers(64);
SELECT sum(length([[repeat('x', 1000000)]][if(number = 0, 2, 1)])) FROM numbers(64);

-- The values and their order are kept, with repeated and distinct elements, defaults and negative indices.
SELECT arrayMap(x -> repeat(toString(x), 100), range(10))[toInt64(number % 13) - 3] AS s, length(s), substring(s, 1, 1)
FROM numbers(13);
SELECT arrayElementOrNull(arrayMap(x -> [x], range(100)), toInt64(number % 7) * 30 - 60) FROM numbers(14);

-- The elements of the constant array share one aggregate state: `byteSize` does not count it, and copying all elements
-- to append a default would copy the state for every element.
WITH replicate(arrayReduce('groupArrayState', range(toUInt64(1000))), range(16384)) AS a
SELECT sum(length(finalizeAggregation(a[if(number = 0, 1, 16385)])))
FROM numbers(16384)
SETTINGS max_block_size = 16384, max_memory_usage = 60000000;

-- A constant array of the maximum number of empty tuples: a default cannot be appended to a copy of all elements.
WITH toUInt64(ignore(number)) AS z,
     arrayWithConstant(42007935 + z, tuple()) AS a,
     arrayFlatten(replicate(a, arrayWithConstant(65537 + z, tuple()))) AS b,
     arrayFlatten(replicate(b, arrayWithConstant(6700417 + z, tuple()))) AS c
SELECT c[number] FROM numbers(2) SETTINGS max_memory_usage = 300000000;

-- Distinct positions of large elements with a default: no copy of the selected elements is made before the result.
SELECT sum(length(arrayWithConstant(63, repeat('x', 900000))[number])) FROM numbers(64)
SETTINGS max_threads = 1, max_block_size = 64, max_memory_usage = 160000000;
