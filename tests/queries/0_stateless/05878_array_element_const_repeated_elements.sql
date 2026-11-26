-- `arrayElement` of a large constant array with some indices out of range.

-- The values and their order are kept, with repeated and distinct elements, defaults and negative indices.
SELECT arrayMap(x -> repeat(toString(x), 100), range(10))[toInt64(number % 13) - 3] AS s, length(s), substring(s, 1, 1)
FROM numbers(13);
SELECT arrayElementOrNull(arrayMap(x -> [x], range(100)), toInt64(number % 7) * 30 - 60) FROM numbers(14);

-- A constant array of the maximum number of empty tuples: a default cannot be appended to a copy of all elements.
WITH toUInt64(ignore(number)) AS z,
     arrayWithConstant(42007935 + z, tuple()) AS a,
     arrayFlatten(replicate(a, arrayWithConstant(65537 + z, tuple()))) AS b,
     arrayFlatten(replicate(b, arrayWithConstant(6700417 + z, tuple()))) AS c
SELECT c[number] FROM numbers(2);
