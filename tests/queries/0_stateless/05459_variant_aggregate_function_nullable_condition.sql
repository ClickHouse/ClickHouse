-- `Variant` arguments of `-If` with a `Nullable` condition (the null-aware batch methods),
-- and of `-Distinct` with a `Nullable` argument. The results must be the same as for the explicit cast.

SET allow_suspicious_variant_types = 1;

SELECT sumDistinctIf(v, c), sumDistinctIf(v::Nullable(UInt64), c)
FROM (SELECT (number % 7)::UInt64::Variant(UInt64) AS v, if(number % 5 = 0, NULL, number % 2 = 0) AS c FROM numbers(100));

SELECT g, sumIf(v, c), sumIf(v::Nullable(UInt64), c), sumDistinctIf(v, c), sumDistinctIf(v::Nullable(UInt64), c)
FROM
(
    SELECT (number % 3)::UInt8 AS g, if(number % 11 = 0, NULL, number::Variant(UInt64)) AS v, if(number % 5 = 0, NULL, number % 2 = 0) AS c
    FROM numbers(100)
)
GROUP BY g ORDER BY g;

SELECT g, sumIf(v, c), sumIf(v::Nullable(UInt64), c)
FROM (SELECT number % 3 AS g, number::Variant(UInt64) AS v, if(number % 5 = 0, NULL, number % 2 = 0) AS c FROM numbers(100))
GROUP BY g ORDER BY g;

-- -Distinct with a `Nullable` argument, the rows are added through `addBatchSinglePlaceNotNull`.
SELECT sumDistinct(x), sumDistinctIf(x, number % 3 != 0)
FROM (SELECT number, if(number % 4 = 0, NULL, number % 10) AS x FROM numbers(100));

-- Window functions add a few rows at a time.
SELECT number, sum(DISTINCT v) OVER (ORDER BY number), sum(DISTINCT v::Nullable(UInt64)) OVER (ORDER BY number)
FROM (SELECT number, (number % 4)::UInt64::Variant(UInt64) AS v FROM numbers(8));
