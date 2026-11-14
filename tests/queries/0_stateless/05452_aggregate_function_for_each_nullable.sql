-- `-ForEach` over arrays of `Nullable` values: NULLs at the start, at the end, between values and at a position in every row.
SELECT sumForEach(a), avgForEach(a), minForEach(a), maxForEach(a), countForEach(a), groupArrayForEach(a)
FROM values('a Array(Nullable(Float64))',
    ([1, NULL, 3, NULL, NULL]),
    ([NULL, 2, NULL, 4, NULL]),
    ([5, 6, NULL, NULL, NULL, 7]));

-- The same over other nested types, with a result that is not `Nullable`.
SELECT sumForEach(a), maxForEach(d), minForEach(s), uniqExactForEach(a)
FROM values('a Array(Nullable(Int32)), d Array(Nullable(Decimal32(2))), s Array(Nullable(String))',
    ([NULL, -1, 2], [1.5, NULL, NULL], ['b', NULL, 'z']),
    ([3, NULL, 4], [NULL, NULL, 2.25], [NULL, 'y', 'a']));

-- Many rows per key, compared with the same aggregation over the elements with their positions.
DROP TABLE IF EXISTS for_each_nullable_05452;
CREATE TABLE for_each_nullable_05452 (k UInt64, a Array(Nullable(Float64))) ENGINE = Memory;
INSERT INTO for_each_nullable_05452
SELECT number % 3, arrayMap(i -> if((number + i) % 4 = 0, NULL, toFloat64(number * i)), range(number % 7 + 1))
FROM numbers(300);

SELECT
    (SELECT groupArray((k, s, m)) FROM (SELECT k, sumForEach(a) AS s, maxForEach(a) AS m FROM for_each_nullable_05452 GROUP BY k ORDER BY k))
    = (SELECT groupArray((k, s, m)) FROM (
        SELECT k, arrayMap(x -> x.2, arraySort(groupArray((i, s)))) AS s, arrayMap(x -> x.2, arraySort(groupArray((i, m)))) AS m
        FROM (SELECT k, i, sum(v) AS s, max(v) AS m FROM for_each_nullable_05452 ARRAY JOIN a AS v, arrayEnumerate(a) AS i GROUP BY k, i)
        GROUP BY k ORDER BY k));

DROP TABLE for_each_nullable_05452;
