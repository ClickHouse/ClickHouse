-- The serialized states of `uniqCombined` must not change between versions: replicas merge parts independently and compare checksums.
-- The value 0 gives the zero key, which a medium set stores separately.

-- A state that is read and written back: medium sets of 40 and 3000 values, and a HyperLogLog counter (20000).
-- `uniqHLL12` reads its HyperLogLog counter the same way.
SELECT n,
    cityHash64(hex(CAST(unhex(hex(arrayReduce('uniqCombinedState', range(toUInt64(n))))), 'AggregateFunction(uniqCombined, UInt64)'))),
    finalizeAggregation(CAST(unhex(hex(arrayReduce('uniqCombinedState', range(toUInt64(n))))), 'AggregateFunction(uniqCombined, UInt64)')),
    cityHash64(hex(CAST(unhex(hex(arrayReduce('uniqCombined64State', range(toUInt64(n))))), 'AggregateFunction(uniqCombined64, UInt64)'))),
    cityHash64(hex(CAST(unhex(hex(arrayReduce('uniqHLL12State', range(toUInt64(n))))), 'AggregateFunction(uniqHLL12, UInt64)')))
FROM (SELECT arrayJoin([40, 3000, 20000]) AS n)
ORDER BY n;

-- Merges into an empty state and then into a medium set: small into medium, medium into small,
-- medium into medium staying medium, and medium into medium crossing over to a HyperLogLog counter (8192 values for K = 17).
WITH arrayReduce('uniqCombinedState', range(toUInt64(a))) AS s1, arrayReduce('uniqCombinedState', range(500, toUInt64(500 + b))) AS s2,
    arrayReduce('uniqCombined64State', range(toUInt64(a))) AS t1, arrayReduce('uniqCombined64State', range(500, toUInt64(500 + b))) AS t2
SELECT a, b,
    cityHash64(hex(arrayReduce('uniqCombinedMergeState', [s1, s2]))),
    arrayReduce('uniqCombinedMerge', [s1, s2]),
    cityHash64(hex(arrayReduce('uniqCombined64MergeState', [t1, t2])))
FROM (SELECT arrayJoin([(10, 30), (30, 10), (40, 3000), (5000, 8000)]) AS t, t.1 AS a, t.2 AS b)
ORDER BY a, b;
