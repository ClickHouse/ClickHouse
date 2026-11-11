-- `arrayReduceInRanges` returns the same as `arrayReduce` over `arraySlice` for every range with a length that fits in `Int64`.
-- The arrays longer than 64 elements also use the pre-aggregated path.
SELECT count()
FROM
(
    SELECT
        range(n) AS arr,
        arrayJoin(range(-260, 260, 7)) AS i,
        arrayJoin([0, 1, 2, 5, 63, 64, 65, 100, 300, 9223372036854775807]) AS l,
        arrayReduceInRanges('sum', [(i, l)], arr)[1] AS res,
        arrayReduce('sum', arraySlice(arr, i, l)) AS expected
    FROM (SELECT arrayJoin([0, 1, 3, 64, 200]) AS n)
)
WHERE res != expected;

-- A range that starts before the array is clipped, as in `arraySlice`.
SELECT arrayReduceInRanges('sum', [(-5, 2), (-5, 3), (-5, 10), (-3, 2)], [1, 2, 3]);
SELECT arrayReduceInRanges('sum', [(-210, 1), (-210, 15)], range(200));
SELECT arrayReduceInRanges('sum', [(-9223372036854775808, 18446744073709551615)], [1, 2, 3]);

-- A huge length does not wrap around and selects the rest of the array.
-- Unlike `arraySlice`, which interprets a length greater than the maximum `Int64` value as a negative length.
SELECT arrayReduceInRanges('sum', [(2, 18446744073709551615), (1, 9223372036854775808)], [1, 2, 3]);
SELECT arrayReduceInRanges('sum', [(-2, 18446744073709551615)], range(200));

-- A range that starts after the array is empty.
SELECT arrayReduceInRanges('sum', [(4, 1), (9223372036854775807, 18446744073709551615)], [1, 2, 3]);
