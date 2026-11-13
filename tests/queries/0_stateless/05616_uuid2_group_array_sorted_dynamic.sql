-- `groupArraySorted` keeps the values of a `Dynamic` in a column and compares them as `ORDER BY` does, so a `UUID2`
-- value keeps its type and its order (by the canonical bytes), both in the regular and in the shared variant.

SELECT toJSONString(groupArraySorted(1)(CAST(u AS Dynamic))), toJSONString(groupArraySorted(2)(CAST(u AS Dynamic)))
FROM
(
    SELECT arrayJoin([
        toUUID2('00000000-0000-0001-0000-000000000000'),
        toUUID2('00000000-0000-0000-0000-000000000001')]) AS u
);

SELECT groupArraySorted(2)(d), arrayMap(x -> dynamicType(x), groupArraySorted(2)(d))
FROM
(
    SELECT CAST(u AS Dynamic(max_types = 0)) AS d
    FROM
    (
        SELECT arrayJoin([
            toUUID2('00000000-0000-0001-0000-000000000000'),
            toUUID2('00000000-0000-0000-0000-000000000001')]) AS u
    )
);

-- The concrete type for comparison.
SELECT groupArraySorted(2)(u)
FROM
(
    SELECT arrayJoin([
        toUUID2('00000000-0000-0001-0000-000000000000'),
        toUUID2('00000000-0000-0000-0000-000000000001')]) AS u
);

-- Merging and serialization of the state, with more values than the limit.
SELECT groupArraySorted(3)(CAST(number % 7 AS Dynamic)) FROM numbers(1000) SETTINGS max_block_size = 13, max_threads = 4;
SELECT finalizeAggregation(groupArraySortedState(3)(CAST(number AS Dynamic))) FROM (SELECT number FROM numbers(10) ORDER BY number DESC);
SELECT groupArraySorted(3)(v) FROM (SELECT CAST(if(number % 3 = 0, NULL, number) AS Variant(UInt64, String)) AS v FROM numbers(10));
