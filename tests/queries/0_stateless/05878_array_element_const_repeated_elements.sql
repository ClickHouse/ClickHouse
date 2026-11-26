-- `arrayElement` of a large constant array with some indices out of range gathers each selected element once, so an
-- element repeated in many rows is not copied once for every row before the result is built.
SET max_block_size = 64, max_memory_usage = 100000000;

SELECT sum(length([repeat('x', 1000000)][if(number = 0, 2, 1)])) FROM numbers(64);
SELECT sum(length(arrayElementOrNull([repeat('x', 1000000)], if(number = 0, 2, 1)))) FROM numbers(64);
SELECT sum(length([[repeat('x', 1000000)]][if(number = 0, 2, 1)])) FROM numbers(64);

-- The values and their order are kept, with repeated and distinct elements, defaults and negative indices.
SELECT arrayMap(x -> repeat(toString(x), 100), range(10))[toInt64(number % 13) - 3] AS s, length(s), substring(s, 1, 1)
FROM numbers(13);
SELECT arrayElementOrNull(arrayMap(x -> [x], range(100)), toInt64(number % 7) * 30 - 60) FROM numbers(14);
