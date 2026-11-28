-- Merging a small state into a large one whose sum of weights is the maximum of `UInt64` is rejected, in both orders,
-- instead of wrapping the counters around.
SELECT arrayReduce('quantileTimingWeightedMerge', [a, b])
FROM (SELECT quantileTimingWeightedState(toUInt64(10), toUInt64(18446744073709551615)) AS a, quantileTimingWeightedState(toUInt64(10), toUInt64(1)) AS b); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arrayReduce('quantileTimingWeightedMerge', [b, a])
FROM (SELECT quantileTimingWeightedState(toUInt64(10), toUInt64(18446744073709551615)) AS a, quantileTimingWeightedState(toUInt64(10), toUInt64(1)) AS b); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Valid mixed merges are unchanged.
SELECT arrayReduce('quantileTimingWeightedMerge', [a, b])
FROM (SELECT quantileTimingWeightedState(toUInt64(10), toUInt64(100)) AS a, quantileTimingWeightedState(toUInt64(20), toUInt64(1)) AS b);
SELECT arrayReduce('quantileTimingMerge', [a, b])
FROM (SELECT quantileTimingState(number) AS a, quantileTimingState(toUInt64(5)) AS b FROM numbers(100));
