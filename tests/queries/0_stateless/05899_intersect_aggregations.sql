-- Tags: no-fasttest
-- no-fasttest: uniqTheta requires datasketches

SELECT '-- uniqExact';
SELECT finalizeAggregation(intersectAggregations(
    uniqExactStateIf(number, number % 2 = 0),
    uniqExactStateIf(number, number % 3 = 0),
    uniqExactStateIf(number, number % 5 = 0)))
FROM numbers(1000);

-- Large sets are two-level hash tables.
SELECT finalizeAggregation(intersectAggregations(uniqExactStateIf(number, number < 300000), uniqExactStateIf(number, number >= 200000)))
FROM numbers(500000);
SELECT finalizeAggregation(intersectAggregations(uniqExactStateIf(number, number < 300000), uniqExactStateIf(number, number % 1000 = 0)))
FROM numbers(500000);
SELECT finalizeAggregation(intersectAggregations(uniqExactStateIf(number, number % 1000 = 0), uniqExactStateIf(number, number < 300000)))
FROM numbers(500000);

-- Strings and several arguments.
SELECT finalizeAggregation(intersectAggregations(uniqExactStateIf(toString(number), number < 60), uniqExactStateIf(toString(number), number >= 50)))
FROM numbers(100);
SELECT finalizeAggregation(intersectAggregations(uniqExactStateIf(number % 10, number % 3, number < 50), uniqExactStateIf(number % 10, number % 3, number >= 50)))
FROM numbers(100);

-- Nullable arguments, including a state without non-NULL values.
SELECT finalizeAggregation(intersectAggregations(uniqExactState(if(number < 10, NULL, number)), uniqExactState(if(number % 2, NULL, number))))
FROM numbers(30);
SELECT finalizeAggregation(intersectAggregations(uniqExactState(if(number < 10, NULL, number)), uniqExactState(CAST(NULL, 'Nullable(UInt64)'))))
FROM numbers(30);

-- The result is a usual state: it can be merged.
SELECT uniqExactMerge(s) FROM
(
    SELECT intersectAggregations(uniqExactStateIf(number, number < 20 + k * 10), uniqExactStateIf(number, number >= k * 10)) AS s
    FROM numbers(100) AS n, (SELECT arrayJoin([0, 5]) AS k) AS t
    GROUP BY k
);

-- Per row, with a constant argument.
SELECT k, finalizeAggregation(intersectAggregations(s, (SELECT uniqExactState(number) FROM numbers(25))))
FROM (SELECT number % 3 AS k, uniqExactState(number) AS s FROM numbers(100) GROUP BY k)
ORDER BY k;

-- A single argument.
SELECT finalizeAggregation(intersectAggregations(uniqExactState(number))) FROM numbers(10);

SELECT '-- uniq';
SELECT finalizeAggregation(intersectAggregations(uniqStateIf(number, number % 2 = 0), uniqStateIf(number, number % 3 = 0)))
FROM numbers(1000);
SELECT finalizeAggregation(intersectAggregations(uniqStateIf(number, number < 10), uniqStateIf(number, number >= 10)))
FROM numbers(1000);
-- A sampled state: compare with the exact size of the intersection.
SELECT abs(finalizeAggregation(intersectAggregations(uniqStateIf(number, number < 3000000), uniqStateIf(number, number >= 1000000))) - 2000000) < 2000000 * 0.05
FROM numbers(4000000);
-- Different state representations: `uniqIf` and `uniq`.
SELECT finalizeAggregation(intersectAggregations(uniqState(number), uniqIfState(number, number < 7)))
FROM numbers(100);

SELECT '-- uniqTheta';
SELECT finalizeAggregation(intersectAggregations(a, b)) = finalizeAggregation(uniqThetaIntersect(a, b))
FROM (SELECT uniqThetaStateIf(number, number < 30000) AS a, uniqThetaStateIf(number, number >= 10000) AS b FROM numbers(50000));
SELECT finalizeAggregation(intersectAggregations(uniqThetaStateIf(number, number % 2 = 0), uniqThetaStateIf(number, number % 3 = 0), uniqThetaStateIf(number, number % 5 = 0)))
FROM numbers(1000);

SELECT '-- groupBitmap';
SELECT bitmapToArray(intersectAggregations(a, b)) = bitmapToArray(bitmapAnd(a, b)), finalizeAggregation(intersectAggregations(a, b))
FROM (SELECT groupBitmapStateIf(number, number < 30000) AS a, groupBitmapStateIf(number, number % 7 = 0) AS b FROM numbers(50000));
SELECT bitmapToArray(intersectAggregations(groupBitmapStateIf(toUInt32(number), number < 10), groupBitmapStateIf(toUInt32(number), number % 2 = 0), groupBitmapStateIf(toUInt32(number), number % 3 = 0)))
FROM numbers(100);

SELECT '-- groupUniqArray';
SELECT arraySort(finalizeAggregation(intersectAggregations(groupUniqArrayStateIf(number % 10, number < 50), groupUniqArrayStateIf(number % 10, number % 4 = 0))))
FROM numbers(100);
SELECT arraySort(finalizeAggregation(intersectAggregations(groupUniqArrayStateIf(toUInt8(number), number < 50), groupUniqArrayStateIf(toUInt8(number), number % 4 = 0))))
FROM numbers(100);
SELECT arraySort(finalizeAggregation(intersectAggregations(groupUniqArrayStateIf(toString(number), number < 15), groupUniqArrayStateIf(toString(number), number % 4 = 0))))
FROM numbers(100);
SELECT arraySort(finalizeAggregation(intersectAggregations(groupUniqArrayStateIf([number % 3], number < 15), groupUniqArrayStateIf([number % 3], number = 4))))
FROM numbers(100);
SELECT arraySort(finalizeAggregation(intersectAggregations(groupUniqArrayStateIf(toDate(number), number < 5), groupUniqArrayStateIf(toDate(number), number >= 3))))
FROM numbers(10);

SELECT '-- empty intersections';
-- A sampled `uniq` state intersected down to nothing estimates zero, also after a merge.
SELECT finalizeAggregation(intersectAggregations(uniqState(number), uniqStateIf(number, 0))) FROM numbers(1000000);
SELECT uniqMerge(s) FROM (SELECT intersectAggregations(uniqState(number), uniqStateIf(number, 0)) AS s FROM numbers(1000000));
-- `-State` drops the `Nullable` adapter from the state type: an empty intersection finalizes like an empty state.
SELECT
    finalizeAggregation(intersectAggregations(groupBitmapStateIf(toNullable(number), number = 0), groupBitmapStateIf(toNullable(number), number = 1))),
    finalizeAggregation(groupBitmapStateIf(toNullable(number), 0))
FROM numbers(2);
-- A state type with the `Nullable` adapter: disjoint nonempty operands and an already empty operand give `NULL`.
WITH
    arrayReduce('groupBitmapState', [toNullable(toUInt64(1))]) AS a,
    arrayReduce('groupBitmapState', [toNullable(toUInt64(2))]) AS b,
    arrayReduce('groupBitmapState', [toNullable(toUInt64(1)), 2]) AS c,
    arrayReduce('groupBitmapState', [CAST(NULL, 'Nullable(UInt64)')]) AS n
SELECT
    toTypeName(a),
    finalizeAggregation(intersectAggregations(a, b)),
    finalizeAggregation(intersectAggregations(c, n)),
    finalizeAggregation(intersectAggregations(a, c)),
    finalizeAggregation(arrayReduce('groupBitmapMergeState', [intersectAggregations(a, b)])),
    finalizeAggregation(arrayReduce('groupBitmapMergeState', [intersectAggregations(a, b), b]));

SELECT '-- versioned states';
-- The result keeps the state version of its type, so it survives a round trip through a `Field` or an array.
SELECT toTypeName(intersectAggregations(uniqState(number))) FROM numbers(10);
SELECT finalizeAggregation(any(s)), finalizeAggregation(groupArray(s)[1]) FROM (SELECT intersectAggregations(uniqState(number)) AS s FROM numbers(10));

SELECT '-- errors';
SELECT intersectAggregations(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT intersectAggregations(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(uniqState(number), uniqExactState(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(uniqState(number), uniqState(toUInt32(number))) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(uniqState(number), 1) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(sumState(number), sumState(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(uniqCombinedState(number), uniqCombinedState(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT intersectAggregations(groupUniqArrayState(10)(number), groupUniqArrayState(10)(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
