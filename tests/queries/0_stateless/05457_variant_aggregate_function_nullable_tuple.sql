-- `Variant` inside a `Nullable(Tuple(...))` argument of a combinator that converts the arguments around itself.
-- The rows with NULL tuples must be skipped, and the results must be the same as for the explicit cast.

SET allow_suspicious_variant_types = 1;

SELECT sumMap(t), sumMap(CAST(t AS Nullable(Tuple(Array(String), Array(Nullable(UInt64))))))
FROM (SELECT if(number = 0, NULL, tuple(['a'], [3::Variant(UInt64)])) AS t FROM numbers(2));

SELECT toTypeName(sumMap(t))
FROM (SELECT if(number = 0, NULL, tuple(['a'], [3::Variant(UInt64)])) AS t FROM numbers(2));

SELECT sumMapArgMax(t, k), sumMapArgMax(t, k::Nullable(UInt8))
FROM (SELECT if(number = 0, NULL, tuple(['a'], [3::Variant(UInt64)])) AS t, (2 - number)::UInt8::Variant(UInt8) AS k FROM numbers(2));

SELECT sumMapArgMax(t, k)
FROM (SELECT if(number = 1, NULL, tuple(['a'], [(number + 1)::Variant(UInt64)])) AS t, number::UInt8::Variant(UInt8) AS k FROM numbers(3));

SELECT sumTuple(t), sumTuple(CAST(t AS Nullable(Tuple(Nullable(UInt64)))))
FROM (SELECT if(number % 2 = 0, NULL, tuple(number::Variant(UInt64))) AS t FROM numbers(10));

SELECT sumTupleArgMax(t, k), sumTupleArgMax(CAST(t AS Nullable(Tuple(Nullable(UInt64)))), k::Nullable(UInt64))
FROM (SELECT if(number = 9, NULL, tuple(number::Variant(UInt64))) AS t, number::Variant(UInt64) AS k FROM numbers(10));

-- -Distinct with GROUP BY, also with a `UInt8` key (the lookup table).
SELECT number % 3 AS g, sumDistinct(number::Variant(UInt64)), sumDistinct(number) FROM numbers(30) GROUP BY g ORDER BY g;
SELECT g, sumDistinct(v), sumDistinct(v::Nullable(UInt64))
FROM (SELECT (number % 3)::UInt8 AS g, (number % 7)::UInt64::Variant(UInt64) AS v FROM numbers(100)) GROUP BY g ORDER BY g;
SELECT g, sumDistinctIf(v, v != 1), sumDistinctIf(v::Nullable(UInt64), v != 1)
FROM (SELECT (number % 3)::UInt8 AS g, if(number % 5 = 0, NULL, (number % 7)::UInt64::Variant(UInt64)) AS v FROM numbers(100)) GROUP BY g ORDER BY g;
