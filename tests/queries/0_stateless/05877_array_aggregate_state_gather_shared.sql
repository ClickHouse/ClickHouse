-- Replicating an array of aggregate states gathers its elements in chunks; the states keep their values in every chunk.
SET max_block_size = 100000;

SELECT length(finalizeAggregation(a[1])), arraySum(finalizeAggregation(a[1]))
FROM (SELECT number, materialize([arrayReduce('groupArrayState', range(toUInt64(1000)))]) AS a FROM numbers(100000))
WHERE number IN (0, 70000, 99999);

-- The first chunk has only empty inner arrays, so the states of the following chunks are appended to an empty column.
WITH arrayReduce('groupArrayState', range(toUInt64(1000))) AS st
SELECT sum(arraySum(arrayMap(x -> length(x[1]), z)))
FROM (SELECT replicate([replicate(st, range(number))], range(if(number = 0, 65536, 131072))) AS z FROM numbers(2))
SETTINGS max_block_size = 200000;

-- A row longer than a chunk is copied directly, and the following rows are gathered from views of the same states.
WITH arrayReduce('groupArrayState', range(toUInt64(1000))) AS st
SELECT sum(length(arrayFlatten(z)))
FROM (SELECT replicate(replicate(st, range(if(number = 0, 65537, 1))), range(if(number = 0, 1, 65536))) AS z FROM numbers(2))
SETTINGS max_block_size = 100000;
